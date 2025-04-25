import crypto from 'crypto';
import axios from 'axios';
import fs from 'fs';

interface P2POrderListParams {
  page: number;
  size: number;
  beginTime?: number; // timestamp в миллисекундах для начала периода
  endTime?: number; // timestamp в миллисекундах для конца периода
  tokenId?: string;
  side?: number[];
  status?: number[];
}

interface TransactionSummary {
  count: number;
  volume: number;
}

interface TokenSummary {
  count: number;
  volume: number;
  tokenVolume: number;
}

interface ProcessedData {
  totalCount: number;
  summary: {
    buy: TransactionSummary;
    sell: TransactionSummary;
  };
  byCoin: {
    [key: string]: {
      buy: TokenSummary;
      sell: TokenSummary;
    };
  };
  transactions: any[];
}

interface ApiResponse {
  ret_code: number;
  ret_msg?: string;
  result?: {
    count?: number;
    items?: any[];
    [key: string]: any;
  };
  time_now?: string;
  [key: string]: any;
}

interface ProcessResult {
  success: boolean;
  data?: ProcessedData;
  message?: string;
  rawResponse?: any;
  error?: any;
}

/**
 * Bybit P2P Transaction History Parser
 * This class fetches and processes P2P transaction history from Bybit API
 */
class BybitP2PParser {
  apiKey: string;
  apiSecret: string;
  baseUrl: string;
  timeOffset: number = 0;
  recvWindow: number = 5000;
  timeSyncComplete: boolean = false;

  /**
   * Creates a new BybitP2PParser instance
   * @param apiKey - Bybit API key
   * @param apiSecret - Bybit API secret
   * @param testnet - Whether to use testnet (default: false)
   * @param recvWindow - Receive window in milliseconds (default: 5000)
   */
  constructor(apiKey: string, apiSecret: string, testnet: boolean = false, recvWindow: number = 5000) {
    this.apiKey = apiKey;
    this.apiSecret = apiSecret;
    this.baseUrl = testnet 
      ? 'https://api-testnet.bybit.com' 
      : 'https://api.bybit.com';
    this.recvWindow = recvWindow;
  }

  /**
   * Synchronize time with Bybit server
   * This should be called before making authenticated requests
   */
  async syncTime(): Promise<void> {
    try {
      // Call Bybit's time endpoint (this endpoint doesn't require authentication)
      const response = await axios.get(`${this.baseUrl}/v5/market/time`);
      
      if (response.data && response.data.result && response.data.result.timeNano) {
        const serverTime = Math.floor(Number(response.data.result.timeNano) / 1000000);
        const localTime = Date.now();
        this.timeOffset = serverTime - localTime;
        
        console.log(`Time synchronized with Bybit server. Offset: ${this.timeOffset}ms`);
        this.timeSyncComplete = true;
      }
    } catch (error: any) {
      console.error('Failed to synchronize time with Bybit server:', error.message);
      this.timeOffset = 0; // Reset offset on error
    }
  }

  /**
   * Get current timestamp adjusted for server time
   * @returns Adjusted timestamp
   */
  getTimestamp(): number {
    return Date.now() + this.timeOffset;
  }

  /**
   * Generate signature for Bybit API authentication
   * @param timestamp - Request timestamp
   * @param method - HTTP method (GET or POST)
   * @param path - API endpoint path
   * @param queryString - Query string for GET requests
   * @param requestBody - Request body for POST requests
   * @returns Signature string
   */
  generateSignature(timestamp: string, method: string, path: string, queryString: string = '', requestBody: string = ''): string {
    // Bybit V5 API signature pattern is: timestamp + api_key + recv_window + (queryString for GET or requestBody for POST)
    const signString = timestamp + this.apiKey + this.recvWindow + (method === 'GET' ? queryString : requestBody);
    
    return crypto.createHmac('sha256', this.apiSecret)
      .update(signString)
      .digest('hex');
  }

  /**
   * Make authenticated request to Bybit API
   * @param endpoint - API endpoint
   * @param method - HTTP method (GET or POST)
   * @param params - Request parameters
   * @returns API response
   */
  async makeRequest(endpoint: string, method: 'GET' | 'POST' = 'GET', params: Record<string, any> = {}): Promise<any> {
    // Ensure time is synchronized before making authenticated requests
    if (!this.timeSyncComplete) {
      throw new Error('Time synchronization required before making API requests. Call syncTime() first.');
    }
    
    try {
      const path = endpoint;
      const timestamp = this.getTimestamp().toString();
      console.log(`Making ${method} request to: ${this.baseUrl}${endpoint}`);
      
      let response;
      
      if (method === 'GET') {
        // For GET requests, convert params to query string
        let queryString = '';
        if (Object.keys(params).length > 0) {
          const queryParams = new URLSearchParams();
          
          Object.entries(params).forEach(([key, value]) => {
            if (value !== undefined && value !== null) {
              queryParams.append(key, String(value));
            }
          });
          
          queryString = queryParams.toString();
          console.log('Query params:', queryString);
        }
        
        // Generate signature
        const signature = this.generateSignature(timestamp, 'GET', path, queryString);
        
        // Set headers for authentication
        const headers = {
          'X-BAPI-API-KEY': this.apiKey,
          'X-BAPI-SIGN': signature,
          'X-BAPI-TIMESTAMP': timestamp,
          'X-BAPI-RECV-WINDOW': this.recvWindow.toString()
        };
        
        // Make GET request
        const url = queryString ? `${this.baseUrl}${path}?${queryString}` : `${this.baseUrl}${path}`;
        response = await axios.get(url, { headers });
      } else { // POST
        // For POST requests, send params in request body
        const requestBody = JSON.stringify(params);
        console.log('Request body:', requestBody);
        
        // Generate signature
        const signature = this.generateSignature(timestamp, 'POST', path, '', requestBody);
        
        // Set headers for authentication
        const headers = {
          'X-BAPI-API-KEY': this.apiKey,
          'X-BAPI-SIGN': signature,
          'X-BAPI-TIMESTAMP': timestamp,
          'X-BAPI-RECV-WINDOW': this.recvWindow.toString(),
          'Content-Type': 'application/json'
        };
        
        // Make POST request
        response = await axios.post(`${this.baseUrl}${path}`, params, { headers });
      }
      
      // Return the response data
      return response?.data;
    } catch (error: any) {
      console.error('API request error:', error.response ? error.response.data : error.message);
      throw error;
    }
  }

  /**
   * Get all P2P orders
   * @param page - Page number (default: 1)
   * @param size - Items per page (default: 50)
   * @param tokenId - Filter by token (optional)
   * @param side - Filter by side (optional - 0: Buy, 1: Sell)
   * @param status - Filter by status (optional - 50: Completed, etc.)
   * @param beginTime - Filter by begin time (timestamp in ms)
   * @param endTime - Filter by end time (timestamp in ms)
   * @returns API response with P2P order history
   */
  async getAllOrders(
    page: number = 1, 
    size: number = 50, 
    tokenId?: string, 
    side?: number[],
    status?: number[],
    beginTime?: number,
    endTime?: number
  ): Promise<ApiResponse> {
    // Build parameters - only include non-null/undefined values
    const params: P2POrderListParams = {
      page,
      size
    };
    
    // Add optional parameters only if they have values
    if (tokenId) params.tokenId = tokenId;
    if (side) params.side = side;
    if (status) params.status = status;
    if (beginTime) params.beginTime = beginTime;
    if (endTime) params.endTime = endTime;
    
    // Логирование параметров запроса для отладки
    console.log('Parameters for P2P request:', JSON.stringify(params));
    
    // Endpoint for P2P orders
    const endpoint = '/v5/p2p/order/simplifyList';
    
    // Make POST request to get P2P order list
    return await this.makeRequest(endpoint, 'POST', params);
  }

  /**
   * Process and analyze P2P transaction history
   * @param transactions - Array of P2P orders
   * @returns Processed transaction data
   */
  processTransactions(transactions: any[]): ProcessedData {
    if (!transactions || !transactions.length) {
      return {
        totalCount: 0,
        summary: {
          buy: { count: 0, volume: 0 },
          sell: { count: 0, volume: 0 }
        },
        byCoin: {},
        transactions: []
      };
    }

    const summary = {
      buy: { count: 0, volume: 0 },
      sell: { count: 0, volume: 0 }
    };
    
    const byCoin: {
      [key: string]: {
        buy: TokenSummary;
        sell: TokenSummary;
      };
    } = {};
    
    // Process each transaction
    transactions.forEach(tx => {
      // In the P2P API, side: 0 is Buy, 1 is Sell
      const orderType = tx.side === 0 ? 'buy' : 'sell';
      const coin = tx.tokenId || '';
      const amount = parseFloat(tx.amount || 0);
      // Calculate fiat amount (price * amount)
      const price = parseFloat(tx.price || 0);
      const fiatAmount = price * amount;
      
      // Skip invalid transactions
      if (!coin) return;
      
      // Update summary
      summary[orderType].count += 1;
      summary[orderType].volume += fiatAmount;
      
      // Update coin statistics
      if (!byCoin[coin]) {
        byCoin[coin] = {
          buy: { count: 0, volume: 0, tokenVolume: 0 },
          sell: { count: 0, volume: 0, tokenVolume: 0 }
        };
      }
      
      byCoin[coin][orderType].count += 1;
      byCoin[coin][orderType].volume += fiatAmount;
      byCoin[coin][orderType].tokenVolume += amount;
    });
    
    return {
      totalCount: transactions.length,
      summary,
      byCoin,
      transactions
    };
  }

  /**
   * Get and process all P2P orders by fetching multiple pages
   * @param initialPage - Starting page number (default: 1)
   * @param pageSize - Items per page (default: 50)
   * @param tokenId - Filter by token (optional)
   * @param side - Filter by side (optional - 0: Buy, 1: Sell)
   * @param status - Filter by status (optional - 50: Completed, etc.)
   * @param beginTime - Filter by begin time (timestamp in ms)
   * @param endTime - Filter by end time (timestamp in ms)
   * @returns Processed transaction data
   */
  async getAndProcessAllOrders(
    initialPage: number = 1, 
    pageSize: number = 50, 
    tokenId?: string, 
    side?: number[],
    status?: number[],
    beginTime?: number,
    endTime?: number
  ): Promise<ProcessResult> {
    try {
      // Максимальное количество страниц для обработки
      const maxPages = 100;
      let allTransactions: any[] = [];
      let totalItemsFound = 0;
      
      // Получаем первую страницу данных
      console.log(`Запрос страницы ${initialPage} (размер ${pageSize})...`);
      let response = await this.getAllOrders(initialPage, pageSize, tokenId, side, status, beginTime, endTime);
      
      // Проверяем успешность первого запроса
      if (response.ret_code !== 0) {
        return {
          success: false,
          message: response.ret_msg || 'Failed to fetch P2P orders',
          rawResponse: response
        };
      }
      
      // Получаем данные первой страницы
      const firstPageTransactions = response.result?.items || [];
      const totalCount = response.result?.count || 0;
      allTransactions = [...allTransactions, ...firstPageTransactions];
      totalItemsFound += firstPageTransactions.length;
      
      console.log(`Найдено ${firstPageTransactions.length} транзакций на странице ${initialPage} из ${Math.ceil(totalCount / pageSize)} страниц`);
      
      // Если есть дополнительные страницы и общее количество известно
      if (totalCount > pageSize && totalCount > 0) {
        const totalPages = Math.min(Math.ceil(totalCount / pageSize), maxPages);
        console.log(`Всего ${totalCount} транзакций, получаем все страницы (максимум ${maxPages})...`);
        
        // Получаем оставшиеся страницы данных
        for (let currentPage = initialPage + 1; currentPage <= totalPages; currentPage++) {
          console.log(`Запрос страницы ${currentPage} из ${totalPages}...`);
          response = await this.getAllOrders(currentPage, pageSize, tokenId, side, status, beginTime, endTime);
          
          if (response.ret_code === 0) {
            const pageTransactions = response.result?.items || [];
            allTransactions = [...allTransactions, ...pageTransactions];
            totalItemsFound += pageTransactions.length;
            console.log(`Получено ${pageTransactions.length} транзакций на странице ${currentPage}`);
          } else {
            console.error(`Ошибка при получении страницы ${currentPage}: ${response.ret_msg}`);
            break; // Прерываем цикл при ошибке
          }
          
          // Небольшая пауза между запросами, чтобы избежать ограничения API
          await new Promise(resolve => setTimeout(resolve, 300));
        }
      }
      
      console.log(`Всего получено ${totalItemsFound} P2P транзакций из ${totalCount} существующих`);
      
      // Если найдены транзакции, показываем образец полей
      if (allTransactions.length > 0) {
        console.log('Sample transaction fields:', Object.keys(allTransactions[0]).join(', '));
      }
      
      // Обрабатываем все полученные транзакции
      const processedData = this.processTransactions(allTransactions);
      
      return {
        success: true,
        data: processedData
      };
    } catch (error: any) {
      return {
        success: false,
        message: error.message,
        error
      };
    }
  }

  /**
   * Export P2P orders in the same format as BybitTransaction.csv
   * @param transactions - Array of P2P orders
   * @param filepath - Path to save CSV file
   * @returns Success status
   */
  exportToMatchingFormat(transactions: any[], filepath: string): boolean {
    if (!transactions || !transactions.length) {
      console.error('No transactions to export');
      return false;
    }
    
    // Create headers to match the BybitTransaction.csv format
    const headers = [
      'id',
      'orderNo',
      'counterparty',
      'status',
      'userId',
      'createdAt',
      'updatedAt',
      'amount',
      'asset',
      'dateTime',
      'originalData',
      'totalPrice',
      'type',
      'unitPrice'
    ].join(',');
    
    // Format transactions as CSV rows
    const rows = transactions.map(tx => {
      // Convert timestamp to date format
      const createDate = new Date(parseInt(tx.createDate));
      const dateFormatted = createDate.toISOString();
      
      // Calculate total price
      const amount = parseFloat(tx.amount);
      const unitPrice = parseFloat(tx.price);
      const totalPrice = (amount * unitPrice).toFixed(2);
      
      // Determine order type
      const type = tx.side === 0 ? 'Buy' : 'Sell';
      
      // Map status to readable text
      let status = "Unknown";
      switch (tx.status) {
        case 5: status = "Waiting for chain"; break;
        case 10: status = "Waiting for payment"; break;
        case 20: status = "Waiting for release"; break;
        case 30: status = "Appealing"; break;
        case 40: status = "Cancelled"; break;
        case 50: status = "Completed"; break;
        case 60: status = "Paying"; break;
        case 70: status = "Payment failed"; break;
        case 80: status = "Exception cancelled"; break;
        case 90: status = "Waiting selection"; break;
        case 100: status = "Objecting"; break;
        case 110: status = "Waiting objection"; break;
      }
      
      // Create a row that matches the CSV format
      return [
        tx.id,                       // id
        tx.id,                       // orderNo (using id as a fallback since orderNo isn't available)
        tx.targetNickName,           // counterparty
        status,                      // status
        tx.userId,                   // userId
        dateFormatted,               // createdAt
        dateFormatted,               // updatedAt (using create date as a fallback)
        amount,                      // amount
        tx.tokenId,                  // asset
        dateFormatted,               // dateTime
        JSON.stringify(tx),          // originalData (storing full JSON)
        totalPrice,                  // totalPrice
        type,                        // type
        unitPrice                    // unitPrice
      ].join(',');
    });
    
    // Combine headers and rows
    const csvContent = [headers, ...rows].join('\n');
    
    // Write to file
    try {
      fs.writeFileSync(filepath, csvContent);
      console.log(`CSV exported successfully to ${filepath} in matching format`);
      return true;
    } catch (error: any) {
      console.error('Error exporting to CSV:', error.message);
      return false;
    }
  }
  
  /**
   * Filter orders for completed transactions only and export to CSV
   * @param transactions - Array of P2P orders
   * @param filepath - Path to save CSV file
   * @returns Success status
   */
  filterAndExportCompletedOrders(transactions: any[], filepath: string): boolean {
    // Filter for completed orders (status 50)
    const completedOrders = transactions.filter(tx => tx.status === 50).filter(tx => tx.type === 'Sell' || tx.type === 'SELL');
    console.log(`Filtered ${completedOrders.length} completed orders from ${transactions.length} total orders`);
    
    if (completedOrders.length === 0) {
      console.log('No completed orders found');
      return false;
    }
    
    // Export filtered orders
    return this.exportToMatchingFormat(completedOrders, filepath);
  }
  
  /**
   * Make a request to P2P endpoints including chat messages
   * @param method - HTTP method (GET or POST)
   * @param endpoint - API endpoint starting with /
   * @param params - Request parameters
   * @returns API response
   */
  async p2pRequest(method: 'GET' | 'POST', endpoint: string, params: Record<string, any> = {}): Promise<any> {
    try {
      // Ensure time synchronization is complete
      if (!this.timeSyncComplete) {
        await this.syncTime();
      }
      
      const timestamp = this.getTimestamp().toString();
      const recvWindow = this.recvWindow.toString();
      
      // Create request configuration
      const config: any = {
        method: method,
        url: `${this.baseUrl}${endpoint}`,
        headers: {
          'X-BAPI-API-KEY': this.apiKey,
          'X-BAPI-TIMESTAMP': timestamp,
          'X-BAPI-RECV-WINDOW': recvWindow,
          'Content-Type': 'application/json'
        }
      };
      
      // For POST requests, add params to request body
      if (method === 'POST') {
        const requestBody = JSON.stringify(params);
        config.data = requestBody;
        config.headers['X-BAPI-SIGN'] = this.generateSignature(
          timestamp,
          method,
          endpoint,
          '',
          requestBody
        );
      } else {
        // For GET requests, add params to query string
        const queryString = new URLSearchParams(params).toString();
        config.url = `${config.url}?${queryString}`;
        config.headers['X-BAPI-SIGN'] = this.generateSignature(
          timestamp,
          method,
          endpoint,
          queryString
        );
      }
      
      // Make the request
      const response = await axios(config);
      return response.data;
      
    } catch (error: any) {
      console.error(`Error in p2pRequest (${method} ${endpoint}):`, error.message);
      if (error.response) {
        console.error('Response data:', error.response.data);
      }
      throw error;
    }
  }
}

export default BybitP2PParser;