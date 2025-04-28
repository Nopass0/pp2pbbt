import { PrismaClient } from '@prisma/client';
import BybitP2PParser from './bybit';
import * as fs from 'fs';
import * as path from 'path';
import axios from 'axios';

/**
 * Сервис синхронизации транзакций Bybit
 * Выполняет периодическую синхронизацию данных для всех пользователей с API ключами
 */
export class BybitSyncService {
    private prisma: PrismaClient;
    private isRunning: boolean = false;
    private syncInterval: NodeJS.Timeout | null = null;
    private dbRetryAttempts: number = 5;
    private dbRetryDelay: number = 5000; // 5 секунд
    private syncIntervalTime: number = 5 * 60 * 1000; // 5 минут
    private logDir: string = path.join(__dirname, 'logs');
    private processingInterval: NodeJS.Timeout | null = null;
    private processingIntervalTime: number = 10 * 60 * 1000; // 10 минут
    
    constructor() {
        // Инициализация клиента Prisma
        this.prisma = new PrismaClient({
            log: [
                { level: 'warn', emit: 'event' },
                { level: 'error', emit: 'event' }
            ]
        });
        
        // Настройка обработчиков ошибок Prisma
        this.prisma.$on('error', (e) => {
            this.logError(`Prisma ошибка: ${e.message}`);
        });
        
        // Создание директории для логов, если её нет
        if (!fs.existsSync(this.logDir)) {
            fs.mkdirSync(this.logDir, { recursive: true });
        }
    }
    
    /**
     * Запуск сервиса синхронизации
     */
    public async start(): Promise<void> {
        if (this.isRunning) {
            console.log('Сервис синхронизации уже запущен');
            return;
        }
        
        this.isRunning = true;
        console.log('Сервис синхронизации Bybit запущен');
        this.log('Сервис синхронизации Bybit запущен');
        
        try {
            // Подключение к базе данных с механизмом повторных попыток
            await this.connectWithRetry();
            
            // Выполняем первую синхронизацию немедленно
            // await this.syncAllUsers();
            await this.syncAllBybitCabinets();
            
            // Устанавливаем интервал для регулярной синхронизации
            this.syncInterval = setInterval(async () => {
                try {
                    // await this.syncAllUsers();
                    await this.syncAllBybitCabinets();
                } catch (error: any) {
                    this.logError(`Ошибка при выполнении регулярной синхронизации: ${error.message}`);
                }
            }, this.syncIntervalTime);
            
            // Устанавливаем интервал для обработки непроцессированных транзакций
            this.processingInterval = setInterval(async () => {
                try {
                    await this.processUnprocessedTransactions();
                    await this.processUnprocessedCabinetTransactions();
                } catch (error: any) {
                    this.logError(`Ошибка при обработке непроцессированных транзакций: ${error.message}`);
                }
            }, this.processingIntervalTime);
            
            console.log(`Синхронизация будет выполняться каждые ${this.syncIntervalTime / 60000} минут`);
            
        } catch (error: any) {
            this.isRunning = false;
            this.logError(`Не удалось запустить сервис синхронизации: ${error.message}`);
            throw error;
        }
    }
    
    /**
     * Остановка сервиса синхронизации
     */
    public async stop(): Promise<void> {
        if (!this.isRunning) {
            console.log('Сервис синхронизации не запущен');
            return;
        }
        
        if (this.syncInterval) {
            clearInterval(this.syncInterval);
            this.syncInterval = null;
        }
        
        if (this.processingInterval) {
            clearInterval(this.processingInterval);
            this.processingInterval = null;
        }
        
        try {
            await this.prisma.$disconnect();
            this.isRunning = false;
            console.log('Сервис синхронизации Bybit остановлен');
            this.log('Сервис синхронизации Bybit остановлен');
        } catch (error: any) {
            this.logError(`Ошибка при остановке сервиса: ${error.message}`);
        }
    }
    
    /**
     * Подключение к базе данных с механизмом повторных попыток
     */
    private async connectWithRetry(): Promise<void> {
        let attempts = 0;
        
        while (attempts < this.dbRetryAttempts) {
            try {
                await this.prisma.$connect();
                console.log('Успешное подключение к базе данных');
                return;
            } catch (error: any) {
                attempts++;
                this.logError(`Попытка ${attempts}/${this.dbRetryAttempts}: Не удалось подключиться к базе данных: ${error.message}`);
                
                if (attempts === this.dbRetryAttempts) {
                    throw new Error(`Не удалось подключиться к базе данных после ${this.dbRetryAttempts} попыток`);
                }
                
                // Ожидание перед повторной попыткой
                console.log(`Повторная попытка через ${this.dbRetryDelay / 1000} секунд...`);
                await this.sleep(this.dbRetryDelay);
            }
        }
    }
    
    /**
     * Синхронизация всех пользователей с API ключами Bybit
     */
    private async syncAllUsers(): Promise<void> {
        const startTime = Date.now();
        this.log('Запуск синхронизации для всех пользователей');
        
        try {
            // Получаем всех активных пользователей с API ключами Bybit
            const users = await this.prisma.user.findMany({
                where: {
                    isActive: true,
                    AND: [
                        { bybitApiToken: { not: null } },
                        { bybitApiToken: { not: '' } },
                        { bybitApiSecret: { not: null } },
                        { bybitApiSecret: { not: '' } }
                    ]
                }
            });
            
            if (users.length === 0) {
                this.log('Нет активных пользователей с API ключами Bybit');
                return;
            }
            
            this.log(`Найдено ${users.length} пользователей для синхронизации`);
            
            // Синхронизация транзакций для каждого пользователя последовательно
            for (const user of users) {
                try {
                    await this.syncUserTransactions(user);
                    // Пауза между синхронизацией разных пользователей для снижения нагрузки
                    await this.sleep(2000);
                } catch (error: any) {
                    this.logError(`Не удалось синхронизировать пользователя ${user.id}: ${error.message}`);
                }
            }
            
            // После синхронизации всех пользователей обрабатываем неиспользованные транзакции
            await this.processUnprocessedTransactions();
            
            this.log('Синхронизация для всех пользователей завершена');
            const endTime = Date.now();
            const duration = (endTime - startTime) / 1000;
            this.log(`Завершение синхронизации для всех пользователей. Длительность: ${duration.toFixed(2)} сек.`);
            
        } catch (error: any) {
            this.logError(`Ошибка при синхронизации пользователей: ${error.message}`);
        }
    }
    
    /**
     * Синхронизация транзакций для одного пользователя
     */
    private async syncUserTransactions(user: any): Promise<void> {
        if (!user.bybitApiToken || !user.bybitApiSecret) {
            this.log(`Пользователь ${user.id} не имеет API ключей Bybit`);
            return;
        }
        
        this.log(`Синхронизация транзакций для пользователя ${user.id}`);
        
        try {
            // Инициализация парсера Bybit с API ключами пользователя
            const parser = new BybitP2PParser(user.bybitApiToken, user.bybitApiSecret);
            await parser.syncTime(); // Важно: синхронизируем время с сервером Bybit
            
            // Указываем статус 50 (Completed) для фильтрации при запросе к API
            const completedStatus = [50]; // 50 = Completed в API Bybit
            
            // Объявляем переменную для хранения транзакций
            let allCompletedTransactions: any[] = [];
            
            // Стратегия 1: Запрос с параметрами времени за последние 3 дня
            this.log(`Пользователь ${user.id}: Стратегия 1 - запрос с параметрами времени`);
            const endTime = Date.now();
            const beginTime = endTime - 3 * 24 * 60 * 60 * 1000; // 3 дня назад
            
            this.log(`Пользователь ${user.id}: Период получения транзакций: ${new Date(beginTime).toLocaleString()} - ${new Date(endTime).toLocaleString()}`);
            
            // Запрашиваем последние 10 страниц транзакций
            let hasSuccess = false;
            
            for (let page = 1; page <= 10; page++) {
                try {
                    const result = await parser.getAndProcessAllOrders(
                        page, 
                        20, 
                        undefined, 
                        undefined, 
                        completedStatus,
                        beginTime,
                        endTime
                    );
                    
                    if (result.success && result.data && result.data.transactions.length > 0) {
                        allCompletedTransactions = [...allCompletedTransactions, ...result.data.transactions];
                        this.log(`Пользователь ${user.id}: Получено ${result.data.transactions.length} транзакций на странице ${page}`);
                        hasSuccess = true;
                        
                        // Небольшая задержка между запросами страниц для не перегрузки API
                        await this.sleep(500);
                    } else {
                        this.log(`Пользователь ${user.id}: Нет транзакций на странице ${page} или ошибка: ${result.message || 'Неизвестная ошибка'}`);
                        break; // Если не получили данные, прекращаем запрашивать следующие страницы
                    }
                } catch (pageError: any) {
                    this.logError(`Пользователь ${user.id}: Ошибка при получении страницы ${page}: ${pageError.message}`);
                    break; // Если ошибка, прекращаем запрашивать следующие страницы
                }
            }
            
            if (!hasSuccess) {
                this.log(`Пользователь ${user.id}: Нет результатов со стратегией 1, пробуем стратегию 2`);
                
                // Стратегия 2: Без параметров времени
                for (let page = 1; page <= 10; page++) {
                    try {
                        const result = await parser.getAndProcessAllOrders(page, 10, undefined, undefined, completedStatus);
                        
                        if (result.success && result.data && result.data.transactions.length > 0) {
                            allCompletedTransactions = [...allCompletedTransactions, ...result.data.transactions];
                            this.log(`Пользователь ${user.id}: Получено ${result.data.transactions.length} транзакций на странице ${page} без параметров времени`);
                            hasSuccess = true;
                            
                            // Небольшая задержка между запросами страниц
                            await this.sleep(500);
                        } else {
                            this.log(`Пользователь ${user.id}: Нет транзакций на странице ${page} или ошибка: ${result.message || 'Неизвестная ошибка'}`);
                            break; // Если не получили данные, прекращаем запрашивать следующие страницы
                        }
                    } catch (pageError: any) {
                        this.logError(`Пользователь ${user.id}: Ошибка при получении страницы ${page} без параметров времени: ${pageError.message}`);
                        break; // Если ошибка, прекращаем запрашивать следующие страницы
                    }
                }
            }
            
            if (!hasSuccess) {
                this.log(`Пользователь ${user.id}: Нет результатов и со стратегией 2`);
                this.log(`Пользователь ${user.id}: Новых транзакций не найдено`);
                await this.updateUserSyncStatus(user.id, 'Новых транзакций не найдено');
                return;
            }
            
            // Проверка наличия полученных транзакций
            if (allCompletedTransactions.length === 0) {
                this.log(`Пользователь ${user.id}: Новых транзакций не найдено`);
                await this.updateUserSyncStatus(user.id, 'Новых транзакций не найдено');
                return;
            }
            
            // Сохранение транзакций в базу данных
            let savedCount = 0;
            for (const transaction of allCompletedTransactions) {
                try {
                    // Проверяем, что это транзакция типа Sell (продажа) и имеет статус Completed (50)
                    if (transaction.side !== 1 || transaction.status !== 50) {
                        // Пропускаем транзакции, которые не являются продажами или не завершены
                        continue;
                    }
                    
                    // Извлекаем идентификатор заказа из API-ответа
                    const orderNo = transaction.id || transaction.orderId || '';
                    
                    // Проверяем, что orderNo не пустой
                    if (!orderNo) {
                        this.logError(`Пользователь ${user.id}: Пропуск транзакции без orderNo: ${JSON.stringify(transaction)}`);
                        continue;
                    }
                    
                    // Проверяем, существует ли уже транзакция с таким orderNo
                    const existingTransaction = await this.prisma.bybitTransaction.findFirst({
                        where: {
                            orderNo: orderNo,
                            userId: user.id
                        }
                    });
                    
                    if (existingTransaction) {
                        // Если транзакция с таким orderNo уже существует, пропускаем её
                        this.log(`Пользователь ${user.id}: Пропуск существующей транзакции с orderNo=${orderNo}`);
                        continue;
                    }
                    
                    // Обработка даты - преобразование из миллисекунд в дату
                    let dateTime;
                    try {
                        // Учитываем разные форматы даты в API
                        const timestamp = parseInt(transaction.createDate || '0');
                        if (timestamp > 0) {
                            dateTime = new Date(timestamp);
                        } else {
                            this.logError(`Пользователь ${user.id}: Недействительная дата для транзакции с orderNo=${orderNo}`);
                            dateTime = new Date(); // используем текущую дату, если не удалось распарсить
                        }
                    } catch (e) {
                        this.logError(`Ошибка при преобразовании даты: ${e.message}`);
                        dateTime = new Date(); // используем текущую дату в случае ошибки
                    }
                    
                    // Всегда устанавливаем тип "Sell" (так как мы уже отфильтровали ранее)
                    const type = 'Sell';
                    
                    // Обработка числовых значений
                    const amount = parseFloat(transaction.amount || '0');
                    const unitPrice = parseFloat(transaction.price || '0');
                    const totalPrice = amount * unitPrice;
                    
                    // Имя контрагента
                    const counterparty = transaction.targetNickName || 'Unknown';
                    
                    // Актив (криптовалюта)
                    const asset = transaction.tokenId || 'USDT';
                    
                    // Всегда устанавливаем статус "Completed" (так как мы уже отфильтровали ранее)
                    const statusStr = "Completed";
                    
                    // Логирование данных для отладки
                    this.log(`Подготовка к сохранению транзакции: orderNo=${orderNo}, date=${dateTime.toISOString()}, type=${type}, amount=${amount}, price=${unitPrice}`);
                    
                    // Создаем новую транзакцию
                    await this.prisma.bybitTransaction.create({
                        data: {
                            orderNo: orderNo,
                            counterparty: counterparty,
                            status: statusStr,
                            userId: user.id,
                            amount: amount,
                            asset: asset,
                            dateTime: dateTime,
                            originalData: transaction,
                            totalPrice: totalPrice,
                            type: type,
                            unitPrice: unitPrice,
                            updatedAt: new Date()
                        }
                    });
                    savedCount++;
                } catch (dbError: any) {
                    this.logError(`Пользователь ${user.id}: Ошибка при сохранении транзакции ${transaction.id || 'неизвестная'}: ${dbError.message}`);
                }
            }
            
            this.log(`Пользователь ${user.id}: Сохранено ${savedCount} новых транзакций`);
            await this.updateUserSyncStatus(user.id, `Успешно. Сохранено ${savedCount} новых транзакций`);
            
        } catch (error: any) {
            this.logError(`Ошибка при синхронизации транзакций пользователя ${user.id}: ${error.message}`);
            await this.updateUserSyncStatus(user.id, `Ошибка: ${error.message}`);
        }
    }
    
    /**
     * Обновление статуса и времени последней синхронизации пользователя
     */
    private async updateUserSyncStatus(userId: number, status: string): Promise<void> {
        try {
            await this.prisma.user.update({
                where: { id: userId },
                data: {
                    lastBybitSyncAt: new Date(),
                    lastBybitSyncStatus: status
                }
            });
        } catch (error: any) {
            this.logError(`Не удалось обновить статус синхронизации для пользователя ${userId}: ${error.message}`);
        }
    }
    
    /**
     * Функция ожидания (sleep)
     */
    private sleep(ms: number): Promise<void> {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
    
    /**
     * Запись логов в файл
     */
    private log(message: string): void {
        const timestamp = new Date().toISOString();
        const logMessage = `[${timestamp}] INFO: ${message}`;
        console.log(logMessage);
        
        const logFile = path.join(this.logDir, `sync-${new Date().toISOString().split('T')[0]}.log`);
        fs.appendFileSync(logFile, logMessage + '\n');
    }
    
    /**
     * Запись ошибок в лог
     */
    private logError(message: string): void {
        const timestamp = new Date().toISOString();
        const errorMessage = `[${timestamp}] ERROR: ${message}`;
        console.error(errorMessage);
        
        const logFile = path.join(this.logDir, `sync-${new Date().toISOString().split('T')[0]}.log`);
        fs.appendFileSync(logFile, errorMessage + '\n');
    }
    
    /**
     * Синхронизация всех аккаунтов BybitCabinet
     */
    private async syncAllBybitCabinets(): Promise<void> {
        try {
            // Получаем все активные кабинеты Bybit
            const cabinets = await this.prisma.bybitCabinet.findMany();
            
            if (cabinets.length === 0) {
                console.log('Не найдено аккаунтов BybitCabinet для синхронизации');
                return;
            }
            
            console.log(`Найдено ${cabinets.length} аккаунтов BybitCabinet для синхронизации`);
            this.log(`Начало синхронизации для ${cabinets.length} аккаунтов BybitCabinet`);
            
            // Синхронизируем каждый кабинет последовательно
            for (const cabinet of cabinets) {
                try {
                    await this.syncCabinetTransactions(cabinet);
                    // Небольшая пауза между синхронизациями разных кабинетов
                    await this.sleep(1000);
                } catch (error: any) {
                    this.logError(`Ошибка при синхронизации кабинета ${cabinet.id}: ${error.message}`);
                }
            }
            
            this.log(`Завершена синхронизация аккаунтов BybitCabinet`);
            console.log('Синхронизация аккаунтов BybitCabinet завершена');
            
        } catch (error: any) {
            this.logError(`Ошибка при синхронизации аккаунтов BybitCabinet: ${error.message}`);
        }
    }
    
    /**
     * Синхронизация транзакций для одного аккаунта BybitCabinet
     */
    private async syncCabinetTransactions(cabinet: any): Promise<void> {
        console.log(`Синхронизация транзакций для кабинета ${cabinet.id} (${cabinet.bybitEmail})`);
        
        try {
            // Проверка наличия API токенов
            if (!cabinet.bybitApiToken || !cabinet.bybitApiSecret) {
                this.logError(`Отсутствуют API ключи для кабинета ${cabinet.id} (${cabinet.bybitEmail})`);
                await this.updateCabinetSyncStatus(cabinet.id, 'ERROR: Отсутствуют API ключи');
                return;
            }
            
            // Создаем экземпляр парсера Bybit
            const parser = new BybitP2PParser(cabinet.bybitApiToken, cabinet.bybitApiSecret);
            
            // Синхронизируем время с сервером Bybit
            await parser.syncTime();
            
            // Используем фиксированный диапазон дат для решения проблемы с временными метками
            // Задаем вручную диапазон за последние 3 дня
            
            // Получаем текущую дату
            const now = new Date();
            const year = now.getFullYear();
            const month = now.getMonth();
            const day = now.getDate();
            
            // Создаем объекты даты для начала и конца периода
            const endDate = new Date(year, month, day); // сегодня в 00:00:00
            const beginDate = new Date(year, month, day - 3); // 3 дня назад в 00:00:00
            
            // Преобразуем в UNIX время в миллисекундах
            const apiBeginTime = beginDate.getTime();
            const apiEndTime = endDate.getTime() + (86400 * 1000); // +86400000 = + 24 часа (до конца дня)
            
            // Логируем реальные даты для отладки
            console.log(`Период синхронизации: ${beginDate.toISOString()} - ${endDate.toISOString()} (+24 часа)`);
            console.log(`Даты в формате UNIX (миллисекунды): beginTime=${apiBeginTime}, endTime=${apiEndTime}`);
            
            // Дополнительная проверка, что даты в адекватном диапазоне (2020-2025 годы)
            const minTimestamp = new Date('2020-01-01').getTime();
            const maxTimestamp = new Date('2025-12-31').getTime();
            
            if (apiBeginTime < minTimestamp || apiBeginTime > maxTimestamp) {
                this.logError(`Недопустимый apiBeginTime: ${apiBeginTime}, выходит за рамки 2020-2025 годов`);
                return; // Прерываем выполнение, чтобы не отправлять некорректные запросы к API
            }
            
            if (apiEndTime < minTimestamp || apiEndTime > maxTimestamp) {
                this.logError(`Недопустимый apiEndTime: ${apiEndTime}, выходит за рамки 2020-2025 годов`);
                return; // Прерываем выполнение
            }
            
            // Эти строки дублируются выше, поэтому удаляем их
            
            // Вывод параметров для отладки
            console.log(`Parameters for P2P request: ${JSON.stringify({page: 1, size: 50, beginTime: apiBeginTime, endTime: apiEndTime})}`);
            
            // Получаем и обрабатываем все заявки P2P для указанного периода
            // Обрабатываем больше страниц для лучшего покрытия данных
            const result = await parser.getAndProcessAllOrders(1, 10, undefined, undefined, undefined, apiBeginTime, apiEndTime);
            
            if (!result.success || !result.data) {
                this.logError(`Ошибка при получении транзакций для кабинета ${cabinet.id}: ${result.message || 'Неизвестная ошибка'}`);
                await this.updateCabinetSyncStatus(cabinet.id, `ERROR: ${result.message || 'Ошибка получения данных'}`);
                return;
            }
            
            // Получаем массив транзакций
            const transactions = result.data.transactions;
            const count = transactions.length;
            console.log(`Получено ${count} транзакций для кабинета ${cabinet.id}`);
            
            // Если нет транзакций, обновляем только статус и время синхронизации
            if (count === 0) {
                await this.updateCabinetSyncStatus(cabinet.id, 'SUCCESS: Нет новых транзакций');
                return;
            }

            // 3 transaction sample
            console.log(`Sample transaction: ${JSON.stringify(transactions[0], null, 2)}\n${JSON.stringify(transactions[1], null, 2)}\n${JSON.stringify(transactions[2], null, 2)}`);
            
            // Фильтруем транзакции, оставляя только завершенные (COMPLETED, статус 50 или 30)
            const completedTransactions = transactions.filter(tx => tx.status === 50).filter(tx => tx.side === 1);
            console.log(`Отфильтровано ${completedTransactions.length} завершенных транзакций из ${transactions.length} общих`);
            
            // Обрабатываем только завершенные транзакции
            for (const tx of completedTransactions) {
                // Преобразуем формат данных для сохранения в БД
                const dbRecord = this.transformCabinetTransactionToDbFormat(tx, cabinet.id);
                
                // Проверяем, существует ли уже такая транзакция
                const existingTransaction = await this.prisma.bybitTransactionFromCabinet.findUnique({
                    where: { orderNo: dbRecord.orderNo }
                });
                
                if (!existingTransaction) {
                    console.log(`Создание новой записи для транзакции ${dbRecord.orderNo}`);
                    // Создаем новую запись транзакции
                    await this.prisma.bybitTransactionFromCabinet.create({
                        data: dbRecord
                    });
                } else {
                    console.log(`Обновление существующей записи для транзакции ${dbRecord.orderNo}`);
                    // Обновляем существующую запись, если статус изменился
                    if (existingTransaction.status !== dbRecord.status) {
                        await this.prisma.bybitTransactionFromCabinet.update({
                            where: { id: existingTransaction.id },
                            data: {
                                status: dbRecord.status,
                                updatedAt: new Date()
                            }
                        });
                    }
                }
            }
            
            // Обновляем статус и время синхронизации
            await this.updateCabinetSyncStatus(cabinet.id, `SUCCESS: Добавлено/обновлено ${count} транзакций`);
            
        } catch (error: any) {
            this.logError(`Ошибка при синхронизации кабинета ${cabinet.id}: ${error.message}`);
            await this.updateCabinetSyncStatus(cabinet.id, `ERROR: ${error.message}`);
        }
    }
    
    /**
     * Обновление статуса и времени последней синхронизации для кабинета Bybit
     */
    private async updateCabinetSyncStatus(cabinetId: number, status: string): Promise<void> {
        try {
            await this.prisma.bybitCabinet.update({
                where: { id: cabinetId },
                data: {
                    lastBybitSyncAt: new Date(),
                    lastBybitSyncStatus: status,
                    updatedAt: new Date()
                }
            });
        } catch (error: any) {
            this.logError(`Ошибка при обновлении статуса синхронизации кабинета ${cabinetId}: ${error.message}`);
        }
    }
    
    /**
     * Преобразование данных транзакции Bybit в формат для базы данных
     */
    private transformCabinetTransactionToDbFormat(transaction: any, cabinetId: number): any {
        // Определяем тип транзакции (BUY или SELL)
        const type = transaction.side === 0 ? 'BUY' : 'SELL';
        
        // Определяем статус
        let status = 'UNKNOWN';
        switch (transaction.status) {
            case 40: status = 'CANCELED'; break;
            case 50: status = 'COMPLETED'; break;
            case 30: status = 'COMPLETED'; break;
            case 20: status = 'PENDING'; break;
            case 0: status = 'PENDING'; break;
            default: status = 'UNKNOWN';
        }
        
        // Форматируем дату и время из поля createDate
        // createDate может быть строкой в формате timestamp в миллисекундах
        let dateTime;
        try {
            // Пробуем преобразовать строку в число и создать дату
            const timestamp = parseInt(transaction.createDate || '0', 10);
            if (isNaN(timestamp) || timestamp === 0) {
                dateTime = new Date();
            } else {
                dateTime = new Date(timestamp); // Предполагаем, что timestamp уже в миллисекундах
            }
        } catch (e) {
            console.log(`Ошибка при преобразовании даты: ${e.message}`);
            dateTime = new Date(); // Устанавливаем текущую дату в случае ошибки
        }
        
        // Логируем основные поля для отладки
        console.log(`ID: ${transaction.id}, Side: ${transaction.side}, Status: ${transaction.status}, CreateDate: ${transaction.createDate}`);
        console.log(`TokenId: ${transaction.tokenId}, Price: ${transaction.price}, Amount: ${transaction.amount}`);
        console.log(`Преобразованная дата: ${dateTime.toISOString()}`);
        
        return {
            orderNo: transaction.id, // Используем id как orderNo
            counterparty: transaction.targetNickName || 'Unknown',
            status,
            amount: parseFloat(transaction.notifyTokenQuantity || '0'),
            asset: transaction.tokenId || 'Unknown',
            dateTime,
            totalPrice: parseFloat(transaction.amount || '0'),
            type,
            unitPrice: parseFloat(transaction.price || '0'),
            originalData: transaction,
            cabinetId,
            processed: false,
            extractedPhones: [],
            createdAt: new Date(),
            updatedAt: new Date()
        };
    }
    
    /**
     * Обработка непроцессированных транзакций от BybitCabinet
     * Получает сообщения из чата для каждой транзакции и ищет номера телефонов
     */
    private async processUnprocessedCabinetTransactions(): Promise<void> {
        try {
            // Получаем непроцессированные транзакции
            const unprocessedTransactions = await this.prisma.bybitTransactionFromCabinet.findMany({
                where: { processed: false },
                include: { BybitCabinet: true }
            });
            
            if (unprocessedTransactions.length === 0) {
                console.log('Нет непроцессированных транзакций от BybitCabinet');
                return;
            }
            
            console.log(`Найдено ${unprocessedTransactions.length} непроцессированных транзакций от BybitCabinet`);
            
            // Обрабатываем каждую транзакцию
            for (const transaction of unprocessedTransactions) {
                try {
                    // Проверяем наличие API токенов
                    const cabinet = transaction.BybitCabinet;
                    if (!cabinet.bybitApiToken || !cabinet.bybitApiSecret) {
                        this.log(`Пропуск транзакции ${transaction.id} из-за отсутствия API ключей в кабинете ${cabinet.id}`);
                        console.log(`Пропуск транзакции ${transaction.id} из-за отсутствия API ключей в кабинете ${cabinet.id}`);
                        continue;
                    }
                    
                    // Получаем сообщения чата для заявки
                    const chatMessages = await this.getChatMessages(transaction.orderNo, { 
                        bybitApiKey: cabinet.bybitApiToken, 
                        bybitApiSecret: cabinet.bybitApiSecret 
                    });
                    
                    // Если сообщения получены успешно, ищем номера телефонов
                    if (chatMessages && chatMessages.length > 0) {
                        const phoneNumbers = this.extractPhoneNumbers(chatMessages);
                        
                        // Обновляем транзакцию с найденными номерами телефонов
                        await this.prisma.bybitTransactionFromCabinet.update({
                            where: { id: transaction.id },
                            data: {
                                extractedPhones: phoneNumbers,
                                processed: true,
                                updatedAt: new Date()
                            }
                        });
                        
                        console.log(`Обработана транзакция ${transaction.id}. Найдено ${phoneNumbers.length} номеров телефонов.`);
                    } else {
                        // Помечаем как обработанную даже без сообщений
                        await this.prisma.bybitTransactionFromCabinet.update({
                            where: { id: transaction.id },
                            data: {
                                processed: true,
                                updatedAt: new Date()
                            }
                        });
                        
                        console.log(`Обработана транзакция ${transaction.id}. Сообщения чата не найдены.`);
                    }
                    
                    // Небольшая пауза между запросами API
                    await this.sleep(500);
                    
                } catch (error: any) {
                    this.logError(`Ошибка при обработке транзакции ${transaction.id}: ${error.message}`);
                    
                    // Обновляем запись с ошибкой, но не помечаем как обработанную
                    await this.prisma.bybitTransactionFromCabinet.update({
                        where: { id: transaction.id },
                        data: {
                            lastAttemptError: error.message,
                            updatedAt: new Date()
                        }
                    });
                }
            }
            
        } catch (error: any) {
            this.logError(`Ошибка при обработке непроцессированных транзакций от BybitCabinet: ${error.message}`);
        }
    }
    
    /**
            for (const transaction of unprocessedTransactions) {
                try {
                    // Проверяем наличие API ключей пользователя
                    if (!transaction.User.bybitApiToken || !transaction.User.bybitApiSecret) {
                        this.log(`Пропускаем транзакцию ${transaction.orderNo} - пользователь ${transaction.userId} не имеет API ключей`);
                        continue;
                    }
                    
                    // Получаем сообщения чата для заявки
                    const chatMessages = await this.getChatMessages(transaction.orderNo, transaction.User);
                    
                    if (!chatMessages || chatMessages.length === 0) {
                        this.log(`Нет сообщений чата для заявки ${transaction.orderNo}`);
                        continue;
                    }
                    
                    // Извлекаем номера телефонов из сообщений
                    const phoneNumbers = this.extractPhoneNumbers(chatMessages);
                    
                    if (phoneNumbers.length === 0) {
                        this.log(`Номера телефонов не найдены в чате заявки ${transaction.orderNo}`);
                        continue;
                    }
                    
                    // Создаем запись в BybitOrderInfo
                    await this.prisma.bybitOrderInfo.create({
                        data: {
                            orderNo: transaction.orderNo,
                            userId: transaction.userId,
                            phoneNumbers: phoneNumbers,
                            bybitTransactionId: transaction.id // Добавляем связь с транзакцией
                        }
                    });
                    
                    this.log(`Создана запись в BybitOrderInfo для заявки ${transaction.orderNo} с ${phoneNumbers.length} номерами телефонов`);
                    
                    // Пауза между запросами API для снижения нагрузки
                    await this.sleep(1000);
                    
                } catch (error: any) {
                    this.logError(`Ошибка при обработке транзакции ${transaction.orderNo}: ${error.message}`);
                }
            }
            
            this.log('Обработка непроцессированных транзакций завершена');
            
        } catch (error: any) {
            this.logError(`Ошибка при обработке непроцессированных транзакций: ${error.message}`);
        }
    }
    
    /**
     * Обработка непроцессированных транзакций Bybit
     * Получает сообщения из чата для каждой транзакции и ищет номера телефонов
     */
    private async processUnprocessedTransactions(): Promise<void> {
        this.log('Запуск обработки непроцессированных транзакций Bybit');
        
        try {
            // Находим все транзакции, которые ещё не обработаны
            const unprocessedTransactions = await this.prisma.bybitTransaction.findMany({
                where: {
                    processed: false
                },
                include: {
                    User: true
                }
            });
            
            if (unprocessedTransactions.length === 0) {
                console.log('Нет непроцессированных транзакций Bybit');
                return;
            }
            
            console.log(`Найдено ${unprocessedTransactions.length} непроцессированных транзакций Bybit`);
            
            // Обрабатываем каждую транзакцию последовательно
            for (const transaction of unprocessedTransactions) {
                try {
                    // Проверяем наличие API ключей пользователя
                    const user = transaction.User;
                    if (!user.bybitApiKey || !user.bybitApiSecret) {
                        console.log(`Пропуск транзакции ${transaction.id} из-за отсутствия API ключей у пользователя ${user.id}`);
                        continue;
                    }
                    
                    // Получаем сообщения чата для заявки
                    const chatMessages = await this.getChatMessages(transaction.orderNo, {
                        bybitApiKey: user.bybitApiKey,
                        bybitApiSecret: user.bybitApiSecret
                    });
                    
                    // Если сообщения получены успешно, ищем номера телефонов
                    if (chatMessages && chatMessages.length > 0) {
                        const phoneNumbers = this.extractPhoneNumbers(chatMessages);
                        
                        // Создаем запись в BybitOrderInfo
                        await this.prisma.bybitOrderInfo.create({
                            data: {
                                phoneNumbers,
                                orderNo: transaction.orderNo,
                                userId: transaction.userId
                            }
                        });
                        
                        // Обновляем транзакцию как обработанную
                        await this.prisma.bybitTransaction.update({
                            where: { id: transaction.id },
                            data: {
                                processed: true,
                                updatedAt: new Date()
                            }
                        });
                        
                        console.log(`Обработана транзакция ${transaction.id}. Найдено ${phoneNumbers.length} номеров телефонов.`);
                    } else {
                        // Помечаем как обработанную, но без телефонов
                        await this.prisma.bybitOrderInfo.create({
                            data: {
                                phoneNumbers: [],
                                orderNo: transaction.orderNo,
                                userId: transaction.userId
                            }
                        });
                        
                        await this.prisma.bybitTransaction.update({
                            where: { id: transaction.id },
                            data: {
                                processed: true,
                                updatedAt: new Date()
                            }
                        });
                        
                        console.log(`Обработана транзакция ${transaction.id}. Сообщения чата не найдены.`);
                    }
                    
                    // Небольшая пауза между обработкой разных транзакций
                    await this.sleep(500);
                    
                } catch (error: any) {
                    this.logError(`Ошибка при обработке транзакции ${transaction.id}: ${error.message}`);
                    
                    // Обновляем запись с ошибкой
                    await this.prisma.bybitTransaction.update({
                        where: { id: transaction.id },
                        data: {
                            lastAttemptError: error.message,
                            updatedAt: new Date()
                        }
                    });
                }
            }
            
        } catch (error: any) {
            this.logError(`Ошибка при обработке непроцессированных транзакций Bybit: ${error.message}`);
        }
    }
    
    /**
     * Получение сообщений чата для указанной заявки через API Bybit
     */
    private async getChatMessages(orderId: string, user: any): Promise<any[]> {
        try {
            const parser = new BybitP2PParser(user.bybitApiToken, user.bybitApiSecret);
            
            // Параметры запроса к API Bybit
            const params = {
                orderId: orderId,
                size: "100" // Максимальное количество сообщений
            };
            
            // Выполняем запрос к API
            const response = await parser.p2pRequest('POST', '/v5/p2p/order/message/listpage', params);
            
            if (response && response.ret_code === 0 && response.result && Array.isArray(response.result)) {
                return response.result;
            }
            
            return [];
            
        } catch (error: any) {
            this.logError(`Ошибка при получении сообщений чата для заявки ${orderId}: ${error.message}`);
            return [];
        }
    }
    
    /**
     * Извлечение номеров телефонов из сообщений чата
     * Поддерживает различные форматы номеров телефонов
     */
    private extractPhoneNumbers(messages: any[]): string[] {
        const phoneNumbers = new Set<string>();
        
        // Регулярные выражения для разных форматов номеров телефонов
        const phonePatterns = [
            /\+7\s?\(?(\d{3})\)?[\s-]?(\d{3})[\s-]?(\d{2})[\s-]?(\d{2})/g, // +7(999)-999-99-99 или +7 999 999 99 99
            /\+7\s?(\d{3})(\d{3})(\d{2})(\d{2})/g, // +79999999999
            /8\s?\(?(\d{3})\)?[\s-]?(\d{3})[\s-]?(\d{2})[\s-]?(\d{2})/g, // 8(999)-999-99-99 или 8 999 999 99 99
            /8\s?(\d{3})(\d{3})(\d{2})(\d{2})/g, // 89999999999
            /(\d{3})[\s-]?(\d{3})[\s-]?(\d{2})[\s-]?(\d{2})/g // 999 999 99 99 или 999-999-99-99
        ];
        
        for (const message of messages) {
            // Проверяем только текстовые сообщения
            if (message.contentType === 'str' && message.message) {
                const messageText = message.message.toString();
                
                // Проверяем каждый шаблон номера телефона
                for (const pattern of phonePatterns) {
                    let match;
                    while ((match = pattern.exec(messageText)) !== null) {
                        // Форматируем номер телефона в стандартный формат +7XXXXXXXXXX
                        let phoneNumber;
                        
                        if (match[0].startsWith('+7')) {
                            // Если номер начинается с +7, извлекаем и форматируем
                            phoneNumber = `+7${match[1]}${match[2]}${match[3]}${match[4]}`;
                        } else if (match[0].startsWith('8')) {
                            // Если номер начинается с 8, заменяем на +7
                            phoneNumber = `+7${match[1]}${match[2]}${match[3]}${match[4]}`;
                        } else {
                            // Если номер без префикса, добавляем +7
                            phoneNumber = `+7${match[1]}${match[2]}${match[3]}${match[4]}`;
                        }
                        
                        // Удаляем все нецифровые символы, кроме +
                        phoneNumber = phoneNumber.replace(/[^\d+]/g, '');
                        
                        // Проверяем, что номер имеет правильную длину (12 символов для +7XXXXXXXXXX)
                        if (phoneNumber.length === 12) {
                            phoneNumbers.add(phoneNumber);
                        }
                    }
                }
            }
        }
        
        return Array.from(phoneNumbers);
    }
}

// Если файл запущен напрямую, а не импортирован как модуль
if (require.main === module) {
    const syncService = new BybitSyncService();
    
    // Обработка остановки процесса
    process.on('SIGINT', async () => {
        console.log('Получен сигнал SIGINT. Остановка сервиса...');
        await syncService.stop();
        process.exit(0);
    });
    
    process.on('SIGTERM', async () => {
        console.log('Получен сигнал SIGTERM. Остановка сервиса...');
        await syncService.stop();
        process.exit(0);
    });
    
    // Перехват необработанных исключений
    process.on('uncaughtException', (error) => {
        console.error('Необработанное исключение:', error);
        // Не завершаем процесс, чтобы сервис продолжал работать
    });
    
    process.on('unhandledRejection', (reason, promise) => {
        console.error('Необработанное отклонение promise:', reason);
        // Не завершаем процесс, чтобы сервис продолжал работать
    });
    
    // Запуск сервиса
    syncService.start().catch(error => {
        console.error('Ошибка при запуске сервиса:', error);
        process.exit(1);
    });
}