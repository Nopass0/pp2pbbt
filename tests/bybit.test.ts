import { expect, mock, spyOn, describe, it, beforeEach, afterAll, beforeAll } from "bun:test";
import BybitP2PParser from '@/bybit';
import crypto from 'crypto';
import fs from 'fs';

// First, completely mock the axios module
const mockGet = mock(async () => ({ data: { retCode: 0, result: { items: [] } } }));
const mockPost = mock(async () => ({ data: { success: true } }));

// Mock axios module properly
mock.module('axios', () => {
  return {
    get: mockGet,
    post: mockPost,
    default: {
      get: mockGet,
      post: mockPost,
    },
    __esModule: true,
  };
});

// Mock fs.writeFileSync
const originalWriteFileSync = fs.writeFileSync;
fs.writeFileSync = mock(() => {});

// Clean up after tests
afterAll(() => {
  fs.writeFileSync = originalWriteFileSync;
});

describe('BybitP2PParser', () => {
  const apiKey = '2KVut3JZrNzrbSK7bO';
  const apiSecret = 'JAfkHwP7eZrbczPcOgGTvI8fU37YofwvRLuh';
  let parser: BybitP2PParser;

  beforeEach(() => {
    parser = new BybitP2PParser(apiKey, apiSecret);
    
    // Reset mocks
    mockGet.mockClear();
    mockPost.mockClear();
    fs.writeFileSync.mockClear();
  });

  describe('constructor', () => {
    it('should initialize with correct properties', () => {
      expect(parser.apiKey).toBe(apiKey);
      expect(parser.apiSecret).toBe(apiSecret);
      expect(parser.baseUrl).toBe('https://api.bybit.com');
    });

    it('should use testnet URL when testnet is true', () => {
      const testnetParser = new BybitP2PParser(apiKey, apiSecret, true);
      expect(testnetParser.baseUrl).toBe('https://api-testnet.bybit.com');
    });
  });

  describe('generateSignature', () => {
    it('should generate correct signature', () => {
      const parameters = 'param1=value1&param2=value2';
      const timestamp = '1617985776000';
      const expectedSign = crypto
        .createHmac('sha256', apiSecret)
        .update(timestamp + apiKey + parameters)
        .digest('hex');

      const signature = parser.generateSignature(parameters, timestamp);
      expect(signature).toBe(expectedSign);
    });
  });

  describe('makeRequest', () => {
    it('should make GET request with correct parameters', async () => {
      const endpoint = '/v5/asset/exchange/order-record';
      const params = { limit: 20, coin: 'BTC' };
      const mockResponse = { retCode: 0, result: { items: [] } };
      
      mockGet.mockResolvedValue({ data: mockResponse });
      
      const result = await parser.makeRequest(endpoint, 'GET', params);
      
      expect(mockGet).toHaveBeenCalled();
      expect(mockGet.mock.calls.length).toBe(1);
      expect(mockGet.mock.calls[0][0]).toContain(endpoint);
      
      // Test each parameter individually rather than depending on order
      expect(mockGet.mock.calls[0][0]).toContain('coin=BTC');
      expect(mockGet.mock.calls[0][0]).toContain('limit=20');
      
      expect(result).toEqual(mockResponse);
    });

    it('should make POST request with correct parameters', async () => {
      const endpoint = '/test-endpoint';
      const params = { param1: 'value1', param2: 'value2' };
      const mockResponse = { success: true };
      
      mockPost.mockResolvedValue({ data: mockResponse });
      
      const result = await parser.makeRequest(endpoint, 'POST', params);
      
      expect(mockPost).toHaveBeenCalled();
      expect(mockPost.mock.calls.length).toBe(1);
      expect(mockPost.mock.calls[0][0]).toBe('https://api.bybit.com/test-endpoint');
      expect(mockPost.mock.calls[0][1]).toEqual(params);
      expect(result).toEqual(mockResponse);
    });

    it('should handle request errors', async () => {
      const endpoint = '/test-endpoint';
      const error = new Error('Request failed with status code 404');
      error.response = { data: { error: 'API error' } };
      
      mockGet.mockRejectedValue(error);
      
      await expect(parser.makeRequest(endpoint, 'GET', {})).rejects.toThrow('Request failed with status code 404');
    });
  });

  describe('getP2PTradeHistory', () => {
    it('should call makeRequest with correct parameters', async () => {
      const mockResponse = { retCode: 0, result: { items: [] } };
      
      // Spy on makeRequest method
      const makeRequestSpy = spyOn(parser, 'makeRequest');
      makeRequestSpy.mockResolvedValue(mockResponse);
      
      await parser.getP2PTradeHistory({ 
        coin: 'BTC', 
        limit: 50, 
        orderType: 'buy',
        startTime: 1617985776000,
        endTime: 1617985876000
      });
      
      expect(makeRequestSpy).toHaveBeenCalledTimes(1);
      expect(makeRequestSpy).toHaveBeenCalledWith(
        '/v5/asset/exchange/order-record',
        'GET',
        {
          coin: 'BTC',
          limit: 50,
          orderType: 'buy',
          startTime: 1617985776000,
          endTime: 1617985876000
        }
      );
    });

    it('should handle optional parameters', async () => {
      const mockResponse = { retCode: 0, result: { items: [] } };
      
      const makeRequestSpy = spyOn(parser, 'makeRequest');
      makeRequestSpy.mockResolvedValue(mockResponse);
      
      await parser.getP2PTradeHistory();
      
      expect(makeRequestSpy).toHaveBeenCalledWith(
        '/v5/asset/exchange/order-record',
        'GET',
        { limit: 20 }
      );
    });
  });

  describe('processTransactions', () => {
    it('should return empty results for empty transactions', () => {
      const result = parser.processTransactions([]);
      
      expect(result).toEqual({
        totalCount: 0,
        summary: {
          buy: { count: 0, volume: 0 },
          sell: { count: 0, volume: 0 }
        },
        byCoin: {},
        transactions: []
      });
    });

    it('should process transactions correctly', () => {
      const transactions = [
        {
          orderType: 'buy',
          tokenId: 'BTC',
          amount: '1.5',
          fiatAmount: '45000',
          createdTime: 1617985776000
        },
        {
          orderType: 'sell',
          tokenId: 'ETH',
          amount: '10',
          fiatAmount: '20000',
          createdTime: 1617985876000
        },
        {
          orderType: 'buy',
          tokenId: 'BTC',
          amount: '0.5',
          fiatAmount: '15000',
          createdTime: 1617985976000
        }
      ];
      
      const result = parser.processTransactions(transactions);
      
      expect(result.totalCount).toBe(3);
      expect(result.summary.buy.count).toBe(2);
      expect(result.summary.buy.volume).toBe(60000);
      expect(result.summary.sell.count).toBe(1);
      expect(result.summary.sell.volume).toBe(20000);
      expect(result.byCoin.BTC.buy.count).toBe(2);
      expect(result.byCoin.BTC.buy.tokenVolume).toBe(2);
      expect(result.byCoin.ETH.sell.count).toBe(1);
      expect(result.byCoin.ETH.sell.tokenVolume).toBe(10);
    });

    it('should handle different API response structures', () => {
      const transactions = [
        {
          side: 'buy',           // Different field name
          coin: 'BTC',           // Different field name
          quantity: '1.5',       // Different field name
          totalAmount: '45000',  // Different field name
          timestamp: 1617985776000 // Different field name
        },
        {
          orderType: 'sell',
          asset: 'ETH',          // Different field name
          size: '10',            // Different field name
          value: '20000',        // Different field name
          time: 1617985876000    // Different field name
        }
      ];
      
      const result = parser.processTransactions(transactions);
      
      expect(result.totalCount).toBe(2);
      expect(result.summary.buy.count).toBe(1);
      expect(result.summary.buy.volume).toBe(45000);
      expect(result.summary.sell.count).toBe(1);
      expect(result.summary.sell.volume).toBe(20000);
      expect(result.byCoin.BTC.buy.tokenVolume).toBe(1.5);
      expect(result.byCoin.ETH.sell.tokenVolume).toBe(10);
    });
  });

  describe('getAndProcessP2PHistory', () => {
    it('should fetch and process transactions successfully', async () => {
      const mockTransactions = [
        {
          orderType: 'buy',
          tokenId: 'BTC',
          amount: '1.5',
          fiatAmount: '45000',
          createdTime: 1617985776000
        }
      ];
      
      const mockResponse = {
        retCode: 0,
        result: {
          items: mockTransactions
        }
      };
      
      const getP2PTradeHistorySpy = spyOn(parser, 'getP2PTradeHistory');
      getP2PTradeHistorySpy.mockResolvedValue(mockResponse);
      
      const result = await parser.getAndProcessP2PHistory({ limit: 50 });
      
      expect(getP2PTradeHistorySpy).toHaveBeenCalledWith({ limit: 50 });
      expect(result.success).toBe(true);
      expect(result.data.totalCount).toBe(1);
    });

    it('should handle API errors', async () => {
      const mockResponse = {
        retCode: 10001,
        retMsg: 'API key invalid'
      };
      
      const getP2PTradeHistorySpy = spyOn(parser, 'getP2PTradeHistory');
      getP2PTradeHistorySpy.mockResolvedValue(mockResponse);
      
      const result = await parser.getAndProcessP2PHistory();
      
      expect(result.success).toBe(false);
      expect(result.message).toBe('API key invalid');
    });

    it('should handle exceptions', async () => {
      const error = new Error('Network error');
      
      const getP2PTradeHistorySpy = spyOn(parser, 'getP2PTradeHistory');
      getP2PTradeHistorySpy.mockRejectedValue(error);
      
      const result = await parser.getAndProcessP2PHistory();
      
      expect(result.success).toBe(false);
      expect(result.message).toBe('Network error');
    });
  });

  describe('exportToCSV', () => {
    it('should export transactions to CSV file', () => {
      const transactions = [
        {
          orderId: '123456',
          orderType: 'buy',
          tokenId: 'BTC',
          amount: '1.5',
          fiatId: 'USD',
          fiatAmount: '45000',
          price: '30000',
          status: 'completed',
          createdTime: 1617985776000
        }
      ];
      
      const filepath = './export.csv';
      
      parser.exportToCSV(transactions, filepath);
      
      expect(fs.writeFileSync).toHaveBeenCalledTimes(1);
      expect(fs.writeFileSync.mock.calls[0][0]).toBe(filepath);
      
      const csvContent = fs.writeFileSync.mock.calls[0][1];
      expect(csvContent).toContain('Date,Order ID,Type,Coin,Amount,Fiat Currency,Fiat Amount,Price,Status');
      expect(csvContent).toContain('123456,buy,BTC,1.5,USD,45000,30000,completed');
    });

    it('should handle empty transactions', () => {
      const result = parser.exportToCSV([], './export.csv');
      
      expect(result).toBe(false);
      expect(fs.writeFileSync).not.toHaveBeenCalled();
    });

    it('should handle file write errors', () => {
      const transactions = [
        {
          orderId: '123456',
          orderType: 'buy',
          tokenId: 'BTC',
          amount: '1.5',
          fiatId: 'USD',
          fiatAmount: '45000',
          price: '30000',
          status: 'completed',
          createdTime: 1617985776000
        }
      ];
      
      // Mock implementation that throws an error
      const originalMock = fs.writeFileSync;
      fs.writeFileSync = mock(() => {
        throw new Error('Write error');
      });
      
      const result = parser.exportToCSV(transactions, './export.csv');
      
      expect(result).toBe(false);
      
      // Restore the original mock
      fs.writeFileSync = originalMock;
    });
  });
});