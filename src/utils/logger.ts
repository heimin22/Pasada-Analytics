import winston from 'winston';
import DailyRotateFile from 'winston-daily-rotate-file';
import { env } from '../config/environment';

// Custom log format
const logFormat = winston.format.combine(
  winston.format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
  winston.format.errors({ stack: true }),
  winston.format.printf(({ timestamp, level, message, stack, ...meta }) => {
    let logMessage = `${timestamp} [${level.toUpperCase()}]: ${message}`;
    
    // Add metadata if present
    if (Object.keys(meta).length > 0) {
      logMessage += ` ${JSON.stringify(meta)}`;
    }
    
    // Add stack trace for errors
    if (stack) {
      logMessage += `\n${stack}`;
    }
    
    return logMessage;
  })
);

// Console format for development
const consoleFormat = winston.format.combine(
  winston.format.colorize(),
  winston.format.timestamp({ format: 'HH:mm:ss' }),
  winston.format.printf(({ timestamp, level, message, ...meta }) => {
    let logMessage = `${timestamp} ${level}: ${message}`;
    
    if (Object.keys(meta).length > 0) {
      logMessage += ` ${JSON.stringify(meta, null, 2)}`;
    }
    
    return logMessage;
  })
);

// Create logger instance
const logger = winston.createLogger({
  level: env.logging?.level || 'info',
  format: logFormat,
  defaultMeta: { 
    service: 'pasada-analytics',
    version: process.env.npm_package_version || '1.0.0'
  },
  transports: [
    // File transport for all logs
    new DailyRotateFile({
      filename: 'logs/application-%DATE%.log',
      datePattern: 'YYYY-MM-DD',
      maxSize: env.logging?.fileMaxSize || '10m',
      maxFiles: env.logging?.fileMaxFiles || 7,
      level: 'info',
      createSymlink: true,
      symlinkName: 'application.log'
    }),

    // File transport for errors only
    new DailyRotateFile({
      filename: 'logs/error-%DATE%.log',
      datePattern: 'YYYY-MM-DD',
      maxSize: env.logging?.fileMaxSize || '10m',
      maxFiles: env.logging?.fileMaxFiles || 14, // Keep errors longer
      level: 'error',
      createSymlink: true,
      symlinkName: 'error.log'
    }),

    // Console transport for development
    new winston.transports.Console({
      format: env.nodeEnv === 'production' ? logFormat : consoleFormat,
      level: env.nodeEnv === 'production' ? 'warn' : 'debug'
    })
  ],

  // Handle uncaught exceptions and rejections
  exceptionHandlers: [
    new winston.transports.File({ filename: 'logs/exceptions.log' })
  ],
  rejectionHandlers: [
    new winston.transports.File({ filename: 'logs/rejections.log' })
  ]
});

// Analytics-specific logger methods
export const analyticsLogger = {
  // Traffic data logging
  trafficData: (action: string, count: number, metadata?: object) => {
    logger.info(`Traffic Data: ${action}`, { 
      count, 
      type: 'traffic_data',
      ...metadata 
    });
  },

  // QuestDB operations
  questdb: (operation: string, details?: object) => {
    logger.info(`QuestDB: ${operation}`, { 
      type: 'questdb_operation',
      ...details 
    });
  },

  // Weekly analytics
  weeklyAnalytics: (week: number, rowsProcessed: number, duration: number) => {
    logger.info('Weekly Analytics Processed', {
      week,
      rowsProcessed,
      duration,
      type: 'weekly_analytics'
    });
  },

  // Performance monitoring
  performance: (operation: string, duration: number, metadata?: object) => {
    logger.info(`Performance: ${operation}`, {
      duration,
      type: 'performance',
      ...metadata
    });
  },

  // API requests
  apiRequest: (method: string, path: string, statusCode: number, duration: number) => {
    logger.info('API Request', {
      method,
      path,
      statusCode,
      duration,
      type: 'api_request'
    });
  },

  // Errors with context
  error: (message: string, error: Error, context?: object) => {
    logger.error(message, {
      error: error.message,
      stack: error.stack,
      type: 'application_error',
      ...context
    });
  }
};

// Create logs directory if it doesn't exist
import { existsSync, mkdirSync } from 'fs';
if (!existsSync('logs')) {
  mkdirSync('logs', { recursive: true });
}

export default logger;
