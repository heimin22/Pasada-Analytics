#!/usr/bin/env ts-node
/* eslint-disable no-console */

/**
 * Traffic Analytics Push Script
 * 
 * This script runs comprehensive traffic analytics for all active routes.
 * It can be run manually or as part of automated processes.
 * 
 * Usage:
 * - npm run analytics:traffic (basic analysis)
 * - npm run analytics:traffic -- --with-forecasts (with forecasts)
 * - npm run analytics:traffic -- --route-ids 1,2,3 (specific routes)
 * - npm run analytics:traffic -- --historical (include historical analysis)
 */

import { AnalyticsService } from '../services/analyticsService';
import { env } from '../config/environment';
import logger from '../utils/logger';

interface ScriptOptions {
  routeIds?: number[];
  withForecasts?: boolean;
  withHistorical?: boolean;
  dryRun?: boolean;
}

class TrafficAnalyticsScript {
  private analyticsService: AnalyticsService;

  constructor() {
    this.analyticsService = new AnalyticsService(null);
  }

  async run(options: ScriptOptions = {}): Promise<void> {
    const startTime = Date.now();
    
    console.log('Traffic Analytics Script Starting...');
    console.log('Configuration:');
    console.log(`  - Route IDs: ${options.routeIds ? options.routeIds.join(', ') : 'All active routes'}`);
    console.log(`  - Generate Forecasts: ${options.withForecasts ? 'Yes' : 'No'}`);
    console.log(`  - Historical Analysis: ${options.withHistorical ? 'Yes' : 'No'}`);
    console.log(`  - Dry Run: ${options.dryRun ? 'Yes' : 'No'}`);
    console.log('');

    try {
      // Check if traffic analytics is available
      if (!this.analyticsService.isTrafficAnalyticsAvailable()) {
        console.error('Traffic Analytics Service Not Available');
        console.log('Required configurations:');
        console.log(`  - Supabase URL: ${env.supabaseUrl ? 'YES' : 'NO'}`);
        console.log(`  - Supabase Service Key: ${env.supabaseServiceRoleKey ? 'YES' : 'NO'}`);
        console.log(`  - Google Maps API Key: ${env.googleMapsApiKey ? 'YES' : 'NO'}`);
        console.log(`  - QuestDB HTTP Endpoint: ${env.questdb.httpEndpoint ? 'YES' : 'NO'}`);
        process.exit(1);
      }

      console.log('All required services are configured');
      console.log('');

      if (options.dryRun) {
        console.log('DRY RUN - No actual processing will occur');
        console.log('Configuration validation completed successfully.');
        return;
      }

      // Run traffic analytics
      const result = await this.analyticsService.runTrafficAnalytics({
        ...(options.routeIds && { routeIds: options.routeIds }),
        includeHistoricalAnalysis: options.withHistorical || false,
        generateForecasts: options.withForecasts || false
      });

      const duration = Date.now() - startTime;

      console.log('Traffic Analytics Results:');
      console.log(`  - Success: ${result.success ? 'YES' : 'NO'}`);
      console.log(`  - Routes Analyzed: ${result.routesAnalyzed}`);
      console.log(`  - Forecasts Generated: ${result.forecastsGenerated}`);
      console.log(`  - Execution Time: ${duration}ms`);
      console.log(`  - Message: ${result.message}`);

      if (result.success) {
        console.log('');
        console.log('Traffic Analytics completed successfully!');
        
        // Log to application logger
        logger.info('Traffic analytics script completed', {
          routesAnalyzed: result.routesAnalyzed,
          forecastsGenerated: result.forecastsGenerated,
          duration,
          options
        });
      } else {
        console.error('');
        console.error('Traffic Analytics failed');
        console.error(`Error: ${result.message}`);
        process.exit(1);
      }

    } catch (error) {
      const duration = Date.now() - startTime;
      console.error('');
      console.error('Fatal Error:', (error as Error).message);
      
      logger.error('Traffic analytics script failed', error as Error, {
        duration,
        options
      });
      
      process.exit(1);
    }
  }
}

// Parse command line arguments
function parseArguments(): ScriptOptions {
  const args = process.argv.slice(2);
  const options: ScriptOptions = {};

  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    
    switch (arg) {
      case '--with-forecasts':
        options.withForecasts = true;
        break;
        
      case '--historical':
        options.withHistorical = true;
        break;
        
      case '--dry-run':
        options.dryRun = true;
        break;
        
      case '--route-ids':
        if (i + 1 < args.length) {
          const routeIdsStr = args[i + 1];
          options.routeIds = routeIdsStr.split(',').map(id => parseInt(id.trim())).filter(id => !isNaN(id));
          i++; // Skip next argument
        }
        break;
    }
  }

  return options;
}

// Main execution
if (require.main === module) {
  const options = parseArguments();
  
  const script = new TrafficAnalyticsScript();
  
  script.run(options).catch((error) => {
    console.error('Script execution failed:', error);
    process.exit(1);
  });
}

export { TrafficAnalyticsScript };
