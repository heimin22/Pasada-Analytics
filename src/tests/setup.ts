/**
 * Jest Test Setup
 * Global test setup and configuration
 */

// Mock console methods to reduce test noise
global.console = {
  ...console,
  // Keep error and warn for important messages
  log: jest.fn(),
  debug: jest.fn(),
  info: jest.fn(),
};

// Mock environment variables for testing
process.env.NODE_ENV = 'test';

// Set up global test timeout
jest.setTimeout(30000);
