#!/usr/bin/env ts-node
import { WeeklyAnalyticsService } from '../services/weeklyAnalyticsService';
import { QuestDBConfig } from '../services/questdbServices';
import * as dotenv from 'dotenv';

dotenv.config();

// Validate environment variables
function validateEnvironment(): QuestDBConfig {
  const config: QuestDBConfig = {
    pgConnectionString: process.env.PG_CONN || process.env.DATABASE_URL || '',
    ilpEndpoint: process.env.QUESTDB_ILP || '',
    httpEndpoint: process.env.QUESTDB_HTTP || '',
    connectionTimeout: 30000,
  };

  if (!config.pgConnectionString) {
    throw new Error('Missing PG_CONN or DATABASE_URL environment variable');
  }
  if (!config.ilpEndpoint) {
    throw new Error('Missing QUESTDB_ILP environment variable');
  }
  if (!config.httpEndpoint) {
    throw new Error('Missing QUESTDB_HTTP environment variable');
  }

  return config;
}

async function main() {
  try {
    console.log('Starting weekly analytics processing...');
    
    const config = validateEnvironment();
    const analyticsService = new WeeklyAnalyticsService(config);

    // Process last week's data
    const result = await analyticsService.processWeeklyAnalytics(1);

    if (result.success) {
      console.log(`${result.message}`);
      console.log(`Processed ${result.rowsProcessed} records`);
    } else {
      console.error(`${result.message}`);
      process.exit(1);
    }

    // Optional: Process previous weeks if needed
    if (process.argv.includes('--backfill')) {
      console.log('Backfilling previous weeks...');
      for (let week = 2; week <= 4; week++) {
        const backfillResult = await analyticsService.processWeeklyAnalytics(week);
        console.log(`Week -${week}: ${backfillResult.message}`);
      }
    }

  } catch (error) {
    console.error('Weekly analytics processing failed:', error);
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