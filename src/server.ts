import express from 'express';
import cors from 'cors';
import helmet from 'helmet';
import compression from 'compression';
import rateLimit from 'express-rate-limit';
import morgan from 'morgan';
import { env } from './config/environment';
import logger, { analyticsLogger } from './utils/logger';
import { QuestDBService } from './services/questdbServices';
import { WeeklyAnalyticsService } from './services/weeklyAnalyticsService';
import { AnalyticsService } from './services/analyticsService';
import cron from 'node-cron';

class AnalyticsServer {
  private app: express.Application;
  private questdbService: QuestDBService;
  private weeklyAnalyticsService: WeeklyAnalyticsService;
  private analyticsService: AnalyticsService;

  constructor() {
    this.app = express();
    
    // Initialize services with graceful error handling
    try {
      this.questdbService = new QuestDBService(env.questdb);
      this.weeklyAnalyticsService = new WeeklyAnalyticsService(env.questdb);
      this.analyticsService = new AnalyticsService(null); // No database service for now
      logger.info('All services initialized successfully');
    } catch (error) {
      logger.warn('Some services failed to initialize:', error);
      // Initialize with null services for graceful degradation
      this.questdbService = new QuestDBService(env.questdb);
      this.weeklyAnalyticsService = new WeeklyAnalyticsService(env.questdb);
      this.analyticsService = new AnalyticsService(null);
    }
    
    this.setupMiddleware();
    this.setupRoutes();
    this.setupCronJobs();
    this.setupErrorHandling();
  }

  private setupMiddleware(): void {
    // Security middleware
    this.app.use(helmet({
      contentSecurityPolicy: {
        directives: {
          defaultSrc: ["'self'"],
          styleSrc: ["'self'", "'unsafe-inline'"],
          scriptSrc: ["'self'"],
          imgSrc: ["'self'", "data:", "https:"],
        },
      },
    }));

    // CORS configuration
    this.app.use(cors({
      origin: env.nodeEnv === 'production' ? [env.apiUrl] : true,
      credentials: true,
      methods: ['GET', 'POST', 'PUT', 'DELETE'],
      allowedHeaders: ['Content-Type', 'Authorization']
    }));

    // Rate limiting
    const limiter = rateLimit({
      windowMs: 15 * 60 * 1000, // 15 minutes
      max: env.nodeEnv === 'production' ? 100 : 1000, // requests per window
      message: 'Too many requests from this IP, please try again later.',
      standardHeaders: true,
      legacyHeaders: false,
    });
    this.app.use('/api/', limiter);

    // General middleware
    this.app.use(compression());
    this.app.use(express.json({ limit: '10mb' }));
    this.app.use(express.urlencoded({ extended: true, limit: '10mb' }));

    // Logging middleware
    this.app.use(morgan('combined', {
      stream: {
        write: (message: string) => {
          logger.info(message.trim(), { type: 'http_request' });
        }
      }
    }));

    // Request timing middleware
    this.app.use((req, res, next) => {
      const startTime = Date.now();
      res.on('finish', () => {
        const duration = Date.now() - startTime;
        analyticsLogger.apiRequest(req.method, req.path, res.statusCode, duration);
      });
      next();
    });
  }

  private setupRoutes(): void {
    // Health check
    this.app.get('/health', (_req, res) => {
      res.json({
        status: 'ok',
        timestamp: new Date().toISOString(),
        service: 'pasada-analytics',
        version: process.env.npm_package_version || '1.0.0'
      });
    });

    // QuestDB status
    this.app.get('/api/status/questdb', async (_req, res): Promise<void> => {
      try {
        // Check if QuestDB is configured
        if (!env.questdb.httpEndpoint || !env.questdb.pgConnectionString) {
          res.status(503).json({ 
            status: 'not_configured', 
            message: 'QuestDB endpoints not configured',
            config: {
              httpEndpoint: !!env.questdb.httpEndpoint,
              pgConnection: !!env.questdb.pgConnectionString,
              ilpEndpoint: !!env.questdb.ilpEndpoint
            },
            timestamp: new Date().toISOString()
          });
          return;
        }

        await this.questdbService.connect();
        const testQuery = 'SELECT 1 as test';
        await this.questdbService.queryAnalytics(testQuery);
        await this.questdbService.disconnect();
        
        res.json({ status: 'connected', timestamp: new Date().toISOString() });
      } catch (error) {
        logger.error('QuestDB health check failed', error);
        res.status(503).json({ 
          status: 'error', 
          message: 'QuestDB connection failed',
          error: env.nodeEnv === 'development' ? (error as Error).message : 'Connection error',
          timestamp: new Date().toISOString()
        });
      }
    });

    // Analytics API endpoints
    this.setupAnalyticsRoutes();
    this.setupDataRoutes();
    this.setupAdminRoutes();
  }

  private setupAnalyticsRoutes(): void {
    const router = express.Router();

    // Get route analytics summary
    router.get('/route/:routeId/summary', async (req, res) => {
      try {
        const routeId = parseInt(req.params.routeId);
        const days = parseInt(req.query.days as string) || 30;

        if (isNaN(routeId)) {
          return res.status(400).json({ error: 'Invalid route ID' });
        }

        const summary = await this.weeklyAnalyticsService.getRouteAnalyticsSummary(routeId, days);
        
        if (!summary) {
          return res.status(404).json({ error: 'No analytics data found for this route' });
        }

        res.json({
          success: true,
          data: summary,
          metadata: {
            routeId,
            days,
            generatedAt: new Date().toISOString()
          }
        });
        return;
      } catch (error) {
        logger.error('Route summary request failed', error, { routeId: req.params.routeId });
        res.status(500).json({ error: 'Failed to fetch route summary' });
        return;
      }
    });

    // Get traffic predictions
    router.get('/route/:routeId/predictions', async (req, res) => {
      try {
        const routeId = parseInt(req.params.routeId);

        if (isNaN(routeId)) {
          return res.status(400).json({ error: 'Invalid route ID' });
        }

        const predictions = await this.analyticsService.getEnhancedPredictions(routeId);
        
        res.json({
          success: true,
          data: predictions,
          metadata: {
            routeId,
            predictionDays: 7,
            generatedAt: new Date().toISOString()
          }
        });
        return;
      } catch (error) {
        logger.error('Predictions request failed', error, { routeId: req.params.routeId });
        res.status(500).json({ error: 'Failed to generate predictions' });
        return;
      }
    });

    // Custom analytics query
    router.post('/query', async (req, res) => {
      try {
        const { query, params = {} } = req.body;

        if (!query) {
          return res.status(400).json({ error: 'Query is required' });
        }

        // Basic SQL injection protection
        // const allowedKeywords = ['SELECT', 'FROM', 'WHERE', 'GROUP BY', 'ORDER BY', 'LIMIT', 'COUNT', 'AVG', 'SUM', 'MIN', 'MAX'];
        const upperQuery = query.toUpperCase();
        const hasDisallowedKeywords = ['DROP', 'DELETE', 'UPDATE', 'INSERT', 'ALTER', 'CREATE'].some(keyword => 
          upperQuery.includes(keyword)
        );

        if (hasDisallowedKeywords) {
          return res.status(403).json({ error: 'Query contains disallowed operations' });
        }

        const startTime = Date.now();
        const result = await this.questdbService.queryAnalytics(query, params);
        const duration = Date.now() - startTime;

        analyticsLogger.performance('Custom analytics query', duration, { 
          query: query.substring(0, 100) + '...',
          resultCount: result.length 
        });

        res.json({
          success: true,
          data: result,
          metadata: {
            executionTime: duration,
            rowCount: result.length,
            generatedAt: new Date().toISOString()
          }
        });
        return;
      } catch (error) {
        logger.error('Custom query failed', error, { query: req.body.query });
        res.status(500).json({ error: 'Query execution failed' });
        return;
      }
    });

    // Traffic Analytics endpoints
    router.post('/traffic/run', async (req, res) => {
      try {
        const { routeIds, includeHistoricalAnalysis = false, generateForecasts = true } = req.body;

        if (!this.analyticsService.isTrafficAnalyticsAvailable()) {
          return res.status(503).json({ 
            error: 'Traffic analytics service not available',
            message: 'Service requires Supabase, Google Maps API, and QuestDB configuration'
          });
        }

        const result = await this.analyticsService.runTrafficAnalytics({
          routeIds,
          includeHistoricalAnalysis,
          generateForecasts
        });

        res.json({
          success: result.success,
          data: result,
          timestamp: new Date().toISOString()
        });
        return;
      } catch (error) {
        logger.error('Traffic analytics execution failed', error);
        res.status(500).json({ error: 'Traffic analytics execution failed' });
        return;
      }
    });

    // Get traffic analytics summary for a route
    router.get('/traffic/route/:routeId/summary', async (req, res) => {
      try {
        const routeId = parseInt(req.params.routeId);
        const days = parseInt(req.query.days as string) || 7;

        if (isNaN(routeId)) {
          return res.status(400).json({ error: 'Invalid route ID' });
        }

        if (!this.analyticsService.isTrafficAnalyticsAvailable()) {
          return res.status(503).json({ 
            error: 'Traffic analytics service not available' 
          });
        }

        const summary = await this.analyticsService.getRouteTrafficSummary(routeId, days);
        
        if (!summary) {
          return res.status(404).json({ error: 'No traffic data found for this route' });
        }

        res.json({
          success: true,
          data: summary,
          metadata: {
            routeId,
            days,
            generatedAt: new Date().toISOString()
          }
        });
        return;
      } catch (error) {
        logger.error('Traffic summary request failed', error, { routeId: req.params.routeId });
        res.status(500).json({ error: 'Failed to fetch traffic summary' });
        return;
      }
    });

    // Get all routes traffic status
    router.get('/traffic/status', async (_req, res) => {
      try {
        if (!this.analyticsService.isTrafficAnalyticsAvailable()) {
          return res.json({
            success: false,
            available: false,
            message: 'Traffic analytics service not configured'
          });
        }

        res.json({
          success: true,
          available: true,
          services: {
            supabase: !!env.supabaseUrl,
            googleMaps: !!env.googleMapsApiKey,
            questdb: !!env.questdb.httpEndpoint
          },
          message: 'Traffic analytics service is ready'
        });
        return;
      } catch (error) {
        logger.error('Traffic status check failed', error);
        res.status(500).json({ error: 'Failed to check traffic analytics status' });
        return;
      }
    });

    this.app.use('/api/analytics', router);
  }

  private setupDataRoutes(): void {
    const router = express.Router();

    // Ingest traffic data
    router.post('/traffic', async (req, res) => {
      try {
        const { trafficData } = req.body;

        if (!Array.isArray(trafficData) || trafficData.length === 0) {
          return res.status(400).json({ error: 'Traffic data array is required' });
        }

        // Validate data structure
        const requiredFields = ['timestamp', 'routeId', 'trafficDensity', 'duration', 'durationInTraffic', 'distance', 'status'];
        const invalidRecords = trafficData.filter(record => 
          !requiredFields.every(field => record.hasOwnProperty(field))
        );

        if (invalidRecords.length > 0) {
          return res.status(400).json({ 
            error: 'Invalid data structure',
            invalidRecords: invalidRecords.length 
          });
        }

        await this.analyticsService.saveTrafficDataDual(trafficData);
        analyticsLogger.trafficData('Ingested via API', trafficData.length);

        res.json({
          success: true,
          message: `Successfully ingested ${trafficData.length} traffic records`,
          timestamp: new Date().toISOString()
        });
        return;
      } catch (error) {
        logger.error('Traffic data ingestion failed', error);
        res.status(500).json({ error: 'Failed to ingest traffic data' });
        return;
      }
    });

    // Get traffic data
    router.get('/traffic', async (req, res) => {
      try {
        const { 
          routeId, 
          startDate, 
          endDate, 
          limit = 1000 
        } = req.query;

        let query = 'SELECT * FROM traffic_analytics WHERE 1=1';
        const params: Record<string, string> = {};

        if (routeId) {
          query += ' AND route_id = $routeId';
          params.routeId = routeId as string;
        }

        if (startDate) {
          query += ' AND timestamp >= $startDate';
          params.startDate = startDate as string;
        }

        if (endDate) {
          query += ' AND timestamp <= $endDate';
          params.endDate = endDate as string;
        }

        query += ' ORDER BY timestamp DESC';
        query += ` LIMIT ${Math.min(parseInt(limit as string), 10000)}`;

        const result = await this.questdbService.queryAnalytics(query, params);

        res.json({
          success: true,
          data: result,
          metadata: {
            count: result.length,
            filters: { routeId, startDate, endDate, limit },
            generatedAt: new Date().toISOString()
          }
        });
      } catch (error) {
        logger.error('Traffic data fetch failed', error);
        res.status(500).json({ error: 'Failed to fetch traffic data' });
      }
    });

    this.app.use('/api/data', router);
  }

  private setupAdminRoutes(): void {
    const router = express.Router();

    // Trigger weekly analytics processing
    router.post('/process-weekly/:weekOffset?', async (req, res) => {
      try {
        const weekOffset = parseInt(req.params.weekOffset || '1') || 1;
        
        const result = await this.weeklyAnalyticsService.processWeeklyAnalytics(weekOffset);
        
        res.json({
          success: result.success,
          message: result.message,
          data: {
            weekOffset,
            rowsProcessed: result.rowsProcessed
          },
          timestamp: new Date().toISOString()
        });
      } catch (error) {
        logger.error('Manual weekly processing failed', error);
        res.status(500).json({ error: 'Failed to process weekly analytics' });
      }
    });

    // Get system metrics
    router.get('/metrics', async (_req, res) => {
      try {
        const metrics = {
          memory: process.memoryUsage(),
          uptime: process.uptime(),
          env: env.nodeEnv,
          timestamp: new Date().toISOString()
        };

        res.json({ success: true, data: metrics });
      } catch (error) {
        logger.error('Metrics request failed', error);
        res.status(500).json({ error: 'Failed to fetch metrics' });
      }
    });

    this.app.use('/api/admin', router);
  }

  private setupCronJobs(): void {
    if (env.nodeEnv === 'production') {
      // Weekly analytics processing - every Monday at 2 AM
      cron.schedule('0 2 * * 1', async () => {
        logger.info('Starting scheduled weekly analytics processing');
        try {
          const result = await this.weeklyAnalyticsService.processWeeklyAnalytics(1);
          analyticsLogger.weeklyAnalytics(1, result.rowsProcessed, 0);
          logger.info('Scheduled weekly analytics completed', { result });
        } catch (error) {
          logger.error('Scheduled weekly analytics failed', error);
        }
      }, {
        timezone: 'Asia/Manila'
      });

      // Traffic Analytics - every 30 minutes during peak hours (6 AM - 10 PM)
      cron.schedule('*/30 6-22 * * *', async () => {
        logger.info('Starting scheduled traffic analytics');
        try {
          if (this.analyticsService.isTrafficAnalyticsAvailable()) {
            const result = await this.analyticsService.runTrafficAnalytics({
              includeHistoricalAnalysis: false,
              generateForecasts: false
            });
            logger.info('Scheduled traffic analytics completed', { result });
          } else {
            logger.warn('Traffic analytics service not available - skipping scheduled run');
          }
        } catch (error) {
          logger.error('Scheduled traffic analytics failed', error);
        }
      }, {
        timezone: 'Asia/Manila'
      });

      // Traffic Analytics with forecasts - every 4 hours
      cron.schedule('0 */4 * * *', async () => {
        logger.info('Starting comprehensive traffic analytics with forecasts');
        try {
          if (this.analyticsService.isTrafficAnalyticsAvailable()) {
            const result = await this.analyticsService.runTrafficAnalytics({
              includeHistoricalAnalysis: true,
              generateForecasts: true
            });
            logger.info('Comprehensive traffic analytics completed', { result });
          } else {
            logger.warn('Traffic analytics service not available - skipping comprehensive run');
          }
        } catch (error) {
          logger.error('Comprehensive traffic analytics failed', error);
        }
      }, {
        timezone: 'Asia/Manila'
      });

      logger.info('Cron jobs scheduled successfully');
    } else {
      logger.info('Cron jobs disabled in development mode');
    }
  }

  private setupErrorHandling(): void {
    // 404 handler
    this.app.use('*', (req, res) => {
      res.status(404).json({
        error: 'Route not found',
        path: req.originalUrl,
        timestamp: new Date().toISOString()
      });
    });

    // Global error handler
    this.app.use((error: Error, req: express.Request, res: express.Response, _next: express.NextFunction) => {
      logger.error('Unhandled error', error, {
        url: req.url,
        method: req.method,
        ip: req.ip
      });

      res.status(500).json({
        error: env.nodeEnv === 'production' ? 'Internal server error' : (error as Error).message,
        timestamp: new Date().toISOString()
      });
    });
  }

  public start(): void {
    const port = env.port;
    
    this.app.listen(port, () => {
      logger.info(`Pasada Analytics Server running on port ${port}`);
      logger.info(`Environment: ${env.nodeEnv}`);
      logger.info(`QuestDB: ${env.questdb.httpEndpoint}`);
      
      console.log(`
Pasada Analytics Server Started!
Port: ${port}
Environment: ${env.nodeEnv}
Health Check: http://localhost:${port}/health
API Base: http://localhost:${port}/api
      `);
    });

    // Graceful shutdown
    process.on('SIGTERM', this.shutdown.bind(this));
    process.on('SIGINT', this.shutdown.bind(this));
  }

  private async shutdown(): Promise<void> {
    logger.info('Shutting down server gracefully...');
    
    try {
      await this.questdbService.disconnect();
      logger.info('Database connections closed');
    } catch (error) {
      logger.error('Error closing database connections', error);
    }
    
    process.exit(0);
  }
}

// Start server if this file is run directly
if (require.main === module) {
  try {
    const server = new AnalyticsServer();
    server.start();
  } catch (error) {
    logger.error('Failed to start server', error);
    process.exit(1);
  }
}

export default AnalyticsServer;
