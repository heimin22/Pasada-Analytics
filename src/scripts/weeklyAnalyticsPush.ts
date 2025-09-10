#!/usr/bin/env ts-node
import { WeeklyAnalyticsService } from '../services/weeklyAnalyticsService';
import { env } from '../config/environment';
import logger, { analyticsLogger } from '../utils/logger';

async function main() {
  try {
    console.log('Starting weekly analytics processing...');
    logger.info('Weekly analytics processing initiated');
    
    const analyticsService = new WeeklyAnalyticsService(env.questdb);

    // Process last week's data
    const result = await analyticsService.processWeeklyAnalytics(1);

    if (result.success) {
      console.log(`${result.message}`);
      console.log(`Processed ${result.rowsProcessed} records`);
      analyticsLogger.weeklyAnalytics(1, result.rowsProcessed, 0);
      logger.info('Weekly analytics processing completed successfully', { 
        rowsProcessed: result.rowsProcessed 
      });
    } else {
      console.error(`${result.message}`);
      logger.error('Weekly analytics processing failed', new Error(result.message));
      process.exit(1);
    }

    // Optional: Process previous weeks if needed
    if (process.argv.includes('--backfill')) {
      console.log('Backfilling previous weeks...');
      for (let week = 2; week <= 4; week++) {
        const backfillResult = await analyticsService.processWeeklyAnalytics(week);
        console.log(`Week -${week}: ${backfillResult.message}`);
        analyticsLogger.weeklyAnalytics(week, backfillResult.rowsProcessed, 0);
        logger.info('Backfill processing completed', { 
          week, 
          rowsProcessed: backfillResult.rowsProcessed 
        });
      }
    }

  } catch (error) {
    console.error('Weekly analytics processing failed:', error);
    logger.error('Weekly analytics processing failed', error as Error);
    process.exit(1);
  }
}

// Handle graceful shutdown
process.on('SIGINT', () => {
  console.log('\nReceived SIGINT, shutting down gracefully...');
  process.exit(0);
});

process.on('SIGTERM', () => {
  console.log('\nReceived SIGTERM, shutting down gracefully...');
  process.exit(0);
});

if (require.main === module) {
  main();
}