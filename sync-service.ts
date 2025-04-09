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
            await this.syncAllUsers();
            
            // Устанавливаем интервал для регулярной синхронизации
            this.syncInterval = setInterval(async () => {
                try {
                    await this.syncAllUsers();
                } catch (error: any) {
                    this.logError(`Ошибка при выполнении регулярной синхронизации: ${error.message}`);
                }
            }, this.syncIntervalTime);
            
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
                    const unitPrice = parseFloat(transaction.price || '0');
                    const amount = parseFloat(transaction.amount || '0');
                    const totalPrice = unitPrice * amount;
                    
                    // Имя контрагента
                    const counterparty = transaction.targetNickName || 'Unknown';
                    
                    // Актив (криптовалюта)
                    const asset = transaction.tokenId || 'USDT';
                    
                    // Всегда устанавливаем статус "Completed" (так как мы уже отфильтровали ранее)
                    const statusStr = "Completed";
                    
                    // Логирование данных для отладки
                    this.log(`Подготовка к сохранению транзакции: orderNo=${orderNo}, date=${dateTime.toISOString()}, type=${type}, amount=${amount}, price=${unitPrice}`);
                    
                    // Форматируем дату для поля Time в originalData
                    const formattedDate = dateTime.toISOString()
                    .replace('T', ' ')
                    .replace(/\.\d+Z$/, '');

                    // Создаем структуру originalData в требуемом формате
                    const originalData = {
                    "Time": formattedDate,
                    "Type": type.toUpperCase(),
                    "Price": unitPrice.toString(),
                    "Status": statusStr,
                    "Currency": "RUB",
                    "Order No.": orderNo,
                    "Currency_1": "RUB",
                    "Coin Amount": amount.toString(),
                    "Fiat Amount": totalPrice.toString(),
                    "p2p-convert": "no",
                    "Counterparty": counterparty,
                    "Cryptocurrency": asset,
                    "Cryptocurrency_1": asset,
                    "Transaction Fees": "0"
                    };

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
                            originalData: originalData,
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
     * Обработка непроцессированных транзакций (которые не имеют записи в BybitOrderInfo)
     * Получает сообщения из чата для каждой транзакции и ищет номера телефонов
     */
    private async processUnprocessedTransactions(): Promise<void> {
        this.log('Запуск обработки непроцессированных транзакций');
        
        try {
            // Находим все транзакции, которые ещё не имеют связанной записи в BybitOrderInfo
            const unprocessedTransactions = await this.prisma.bybitTransaction.findMany({
                where: {
                    BybitOrderInfo: null // Используем null вместо none для отношений один-к-одному
                },
                include: {
                    User: true
                }
            });
            
            if (unprocessedTransactions.length === 0) {
                this.log('Непроцессированных транзакций не найдено');
                return;
            }
            
            this.log(`Найдено ${unprocessedTransactions.length} непроцессированных транзакций`);
            
            // Обрабатываем каждую транзакцию последовательно
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