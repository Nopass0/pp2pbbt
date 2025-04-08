import { PrismaClient } from '@prisma/client';
import BybitP2PParser from './bybit';
import * as fs from 'fs';
import * as path from 'path';

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
        try {
            const startTime = Date.now();
            this.log(`Начало синхронизации для всех пользователей: ${new Date().toLocaleString()}`);
            
            // Получение всех пользователей с API ключами Bybit
            const users = await this.prisma.user.findMany({
                where: {
                    bybitApiToken: { not: null },
                    bybitApiSecret: { not: null }
                }
            });
            
            this.log(`Найдено ${users.length} пользователей с API ключами Bybit`);
            
            // Синхронизация каждого пользователя по очереди
            for (const user of users) {
                try {
                    await this.syncUserTransactions(user);
                } catch (error: any) {
                    this.logError(`Ошибка при синхронизации пользователя ${user.id}: ${error.message}`);
                    // Обновляем статус синхронизации даже в случае ошибки
                    await this.updateUserSyncStatus(user.id, `Ошибка: ${error.message}`);
                }
                
                // Небольшая задержка между запросами к API для разных пользователей
                await this.sleep(1000);
            }
            
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