import BybitP2PParser from './bybit';
import fs from 'fs';
import path from 'path';

/**
 * Обработка и экспорт данных Bybit транзакций
 * Сохраняет только завершенные транзакции в требуемом формате
 */
async function processCompletedTransactions() {
  // API ключи Bybit
  const apiKey = '2KVut3JZrNzrbSK7bO';
  const apiSecret = 'JAfkHwP7eZrbczPcOgGTvI8fU37YofwvRLuh';
  
  try {
    console.log('Инициализация парсера Bybit P2P...');
    const parser = new BybitP2PParser(apiKey, apiSecret);
    
    // Синхронизация времени с сервером Bybit
    await parser.syncTime();
    
    // Пробуем сначала запрос без параметров времени
    console.log('Получение завершенных P2P транзакций из API Bybit (без параметров времени)...');
    
    // Указываем статус 50 (Completed) для фильтрации при запросе к API
    const completedStatus = [50]; // 50 = Completed в API Bybit
    
    // Объявляем переменную для хранения транзакций
    let completedTransactions: any[] = [];

    try {
      // Стратегия 1: Пробуем без параметров времени
      console.log('Попытка 1: Получение завершенных P2P транзакций без параметров времени...');
      const result = await parser.getAndProcessAllOrders(1, 20, undefined, undefined, completedStatus);
      
      if (result.success && result.data && result.data.transactions.length > 0) {
        // Если успешно получили данные без параметров времени
        completedTransactions = result.data.transactions;
        console.log(`Получено ${completedTransactions.length} завершенных транзакций без параметров времени`);
      } else {
        console.log('Не удалось получить транзакции без параметров времени:', result.message || 'Неизвестная ошибка');
        
        // Стратегия 2: Пробуем с коротким периодом (1 день)
        console.log('Попытка 2: Пробуем с очень коротким периодом (1 день)...');
        const endTime = Date.now();
        const beginTime = Date.now() - 1 * 24 * 60 * 60 * 1000; // 1 день назад
        
        console.log(`Период получения транзакций: ${new Date(beginTime).toLocaleString()} - ${new Date(endTime).toLocaleString()}`);
        
        // Используем меньшее количество записей и очень короткий период
        const resultWithTime = await parser.getAndProcessAllOrders(1, 10, undefined, undefined, completedStatus, beginTime, endTime);
        
        if (resultWithTime.success && resultWithTime.data && resultWithTime.data.transactions.length > 0) {
          // Если успешно получили данные с параметрами времени
          completedTransactions = resultWithTime.data.transactions;
          console.log(`Получено ${completedTransactions.length} завершенных транзакций с параметрами времени`);
        } else {
          // Стратегия 3: Пробуем только с параметром статуса, без других параметров
          console.log('Попытка 3: Только с параметром статуса, без других параметров...');
          
          // Используем минимальные параметры
          const minimalResult = await parser.getAndProcessAllOrders(1, 5, undefined, undefined, completedStatus);
          if (minimalResult.success && minimalResult.data && minimalResult.data.transactions.length > 0) {
            completedTransactions = minimalResult.data.transactions;
            console.log(`Получено ${completedTransactions.length} завершенных транзакций с минимальными параметрами`);
          } else {
            console.error('Не удалось получить транзакции ни с одним из методов. Проблема с API Bybit.');
            return;
          }
        }
      }
    } catch (error: any) {
      console.error('Ошибка при получении транзакций:', error.message);
      return;
    }
    
    // Проверка наличия полученных транзакций
    if (completedTransactions.length === 0) {
      console.log('Завершенных транзакций не найдено. Экспорт не требуется.');
      return;
    }
    
    // Путь к файлу для сохранения
    const outputFilePath = path.join(__dirname, 'BybitTransaction.csv');
    
    // Проверяем существование файла и создаем/добавляем данные
    let existingTransactions: any[] = [];
    let headers = 'id,orderNo,counterparty,status,userId,createdAt,updatedAt,amount,asset,dateTime,originalData,totalPrice,type,unitPrice';
    
    if (fs.existsSync(outputFilePath)) {
      // Если файл существует, читаем его
      console.log('Файл CSV существует, чтение данных...');
      const fileContent = fs.readFileSync(outputFilePath, 'utf8');
      const lines = fileContent.split('\n');
      
      // Получаем заголовки из файла
      headers = lines[0];
      
      // Пропускаем заголовок, парсим только строки с данными
      if (lines.length > 1) {
        existingTransactions = lines.slice(1)
          .filter(line => line.trim() !== '')
          .map(line => {
            const values = line.split(',');
            return {
              id: values[0].replace(/"/g, ''),
              orderNo: values[1].replace(/"/g, ''),
            };
          });
      }
    }
    
    // Форматирование транзакций по требуемому формату
    const formattedRows = completedTransactions.map(tx => {
      // Преобразование даты
      const createDate = new Date(parseInt(tx.createDate));
      const formattedDate = createDate.toISOString().replace('T', ' ').slice(0, 19);
      
      // Вычисление цены
      const amount = parseFloat(tx.amount);
      const unitPrice = parseFloat(tx.price);
      const totalPrice = (amount * unitPrice).toFixed(2);
      
      // Тип операции
      const type = tx.side === 0 ? 'BUY' : 'SELL';
      
      // Создание оригинальных данных в требуемом формате
      const originalData = {
        "Time": formattedDate,
        "Type": type,
        "Price": unitPrice.toString(),
        "Status": "Completed",
        "Currency": "RUB",
        "Order No.": tx.id,
        "Currency_1": "RUB",
        "Coin Amount": amount.toString(),
        "Fiat Amount": totalPrice,
        "p2p-convert": "no",
        "Counterparty": tx.targetNickName,
        "Cryptocurrency": tx.tokenId,
        "Cryptocurrency_1": tx.tokenId,
        "Transaction Fees": "0"
      };
      
      return [
        `"${tx.id}"`,
        `"${tx.id}"`,
        `"${tx.targetNickName}"`,
        `"completed"`,
        `"${tx.userId}"`,
        `"${createDate.toISOString().replace('T', ' ').slice(0, 23)}"`,
        `"${createDate.toISOString().replace('T', ' ').slice(0, 23)}"`,
        `"${amount}"`,
        `"${tx.tokenId}"`,
        `"${createDate.toISOString().replace('T', ' ').slice(0, 23)}"`,
        `"${JSON.stringify(originalData)}"`,
        `"${totalPrice}"`,
        `"${type}"`,
        `"${unitPrice}"`,
      ].join(',');
    });
    
    // Запись данных в файл
    const csvContent = [headers, ...formattedRows].join('\n');
    fs.writeFileSync(outputFilePath, csvContent);
    
    console.log(`Экспорт успешно завершен! Сохранено ${formattedRows.length} транзакций в ${outputFilePath}`);
    
  } catch (error: any) {
    console.error('Ошибка при обработке транзакций:', error.message);
  }
}

// Запуск обработки транзакций
processCompletedTransactions();