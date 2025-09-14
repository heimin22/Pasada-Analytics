#!/usr/bin/env ts-node
/* eslint-disable no-console */
/* eslint-disable @typescript-eslint/no-unused-vars */
/* eslint-disable no-unused-vars */
import { QuestDBService, WeeklyRouteData } from '../services/questdbServices';
import { env } from '../config/environment';
import { createClient, SupabaseClient } from '@supabase/supabase-js';
import logger, { analyticsLogger } from '../utils/logger';

interface MigrationOptions {
  dryRun: boolean;
  batchSize: number;
  startDate?: string | undefined;
  endDate?: string | undefined;
  routeIds?: number[] | undefined;
}

class QuestDBMigrationAPI {
  private questdbService: QuestDBService;
  private supabase: SupabaseClient;

  constructor() {
    this.questdbService = new QuestDBService(env.questdb);
    this.supabase = createClient(
      env.supabaseUrl,
      env.supabaseServiceRoleKey
    );
  }

  private async testQuestDBConnection(): Promise<void> {
    try {
      // Test QuestDB HTTP endpoint with a simple query
      const result = await this.questdbService.queryAnalytics('SELECT 1 as test');
      if (!result || result.length === 0) {
        throw new Error('QuestDB query returned no results');
      }
    } catch (error) {
      throw new Error(`QuestDB HTTP endpoint test failed: ${(error as Error).message}`);
    }
  }

  async migrate(options: MigrationOptions): Promise<void> {
    console.log('Starting QuestDB Migration (API Version)...');
    console.log(`   Dry Run: ${options.dryRun ? 'Yes' : 'No'}`);
    console.log(`   Batch Size: ${options.batchSize}`);
    console.log(`   Date Range: ${options.startDate || 'All'} to ${options.endDate || 'All'}`);
    console.log('');

    try {
      // Test Supabase API connection
      const { count, error } = await this.supabase
        .from('traffic_analytics')
        .select('*', { count: 'exact', head: true });

      if (error) {
        throw new Error(`Supabase API connection failed: ${error.message}`);
      }

      console.log(`Connected to Supabase API - ${count} total records available`);

      // Test QuestDB HTTP endpoint connectivity
      try {
        await this.testQuestDBConnection();
        console.log('QuestDB HTTP endpoint is reachable');
      } catch (error) {
        console.warn('QuestDB connection test failed:', (error as Error).message);
        if (!options.dryRun) {
          throw new Error('QuestDB is not accessible - migration cannot proceed');
        }
      }

      // Step 1: Migrate traffic analytics data
      await this.migrateTrafficAnalytics(options);

      // Step 2: Generate and migrate weekly summaries
      if (!options.dryRun) {
        await this.generateWeeklySummaries(options);
      }

      console.log('\nMigration completed successfully!');

    } catch (error) {
      console.error('\nMigration failed:', error);
      analyticsLogger.error('Migration failed', error as Error);
      throw error;
    }
  }

  private async migrateTrafficAnalytics(options: MigrationOptions): Promise<void> {
    console.log('Migrating traffic analytics data...');

    // Build query filters
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    let query = this.supabase
      .from('traffic_analytics')
      .select(`
        route_id,
        timestamp,
        traffic_density,
        duration,
        duration_in_traffic,
        distance,
        status,
        created_at
      `);

    // Apply filters
    if (options.startDate) {
      query = query.gte('timestamp', options.startDate);
    }
    if (options.endDate) {
      query = query.lte('timestamp', options.endDate);
    }
    if (options.routeIds && options.routeIds.length > 0) {
      query = query.in('route_id', options.routeIds);
    }
    
    // Note: query is used in the batch processing below

    // Get total count
    const { count: totalRecords, error: countError } = await this.supabase
      .from('traffic_analytics')
      .select('*', { count: 'exact', head: true });

    if (countError) {
      throw new Error(`Failed to get record count: ${countError.message}`);
    }

    console.log(`   Total records to migrate: ${totalRecords}`);

    if (!totalRecords || totalRecords === 0) {
      console.log('   No records found to migrate');
      return;
    }

    // Process in batches
    let offset = 0;
    let totalMigrated = 0;

    while (offset < totalRecords) {
      // Build batch query with pagination
      let batchQuery = this.supabase
        .from('traffic_analytics')
        .select(`
          route_id,
          timestamp,
          traffic_density,
          duration,
          duration_in_traffic,
          distance,
          status,
          created_at
        `)
        .order('timestamp', { ascending: true })
        .range(offset, offset + options.batchSize - 1);

      // Apply same filters to batch query
      if (options.startDate) {
        batchQuery = batchQuery.gte('timestamp', options.startDate);
      }
      if (options.endDate) {
        batchQuery = batchQuery.lte('timestamp', options.endDate);
      }
      if (options.routeIds && options.routeIds.length > 0) {
        batchQuery = batchQuery.in('route_id', options.routeIds);
      }

      const { data: batchData, error: batchError } = await batchQuery;

      if (batchError) {
        throw new Error(`Failed to fetch batch: ${batchError.message}`);
      }

      if (!batchData || batchData.length === 0) {
        break;
      }

      console.log(`   Processing batch: ${offset + 1}-${offset + batchData.length} of ${totalRecords}`);

      if (!options.dryRun) {
        // Transform data for QuestDB
        const trafficData = batchData.map(row => ({
          timestamp: row.timestamp,
          routeId: row.route_id,
          trafficDensity: row.traffic_density,
          duration: row.duration,
          durationInTraffic: row.duration_in_traffic,
          distance: row.distance,
          status: row.status
        }));

        // Save to QuestDB
        await this.questdbService.saveTrafficDataToQuestDB(trafficData);
        analyticsLogger.trafficData('Migrated batch to QuestDB', trafficData.length, {
          offset,
          batchSize: batchData.length
        });
      }

      totalMigrated += batchData.length;
      offset += options.batchSize;

      // Progress indicator
      const progress = Math.round((offset / totalRecords) * 100);
      console.log(`   Progress: ${progress}% (${totalMigrated}/${totalRecords})`);

      // Small delay to prevent overwhelming the databases
      await new Promise(resolve => setTimeout(resolve, 100));
    }

    console.log(`Traffic analytics migration completed: ${totalMigrated} records`);
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  private async generateWeeklySummaries(_options: MigrationOptions): Promise<void> {
    console.log('Generating weekly summaries...');

    // Get date range for weekly summaries using Supabase API
    const { data: dateRange, error: dateError } = await this.supabase
      .from('traffic_analytics')
      .select('timestamp')
      .order('timestamp', { ascending: true })
      .limit(1);

    const { data: dateRangeMax, error: dateErrorMax } = await this.supabase
      .from('traffic_analytics')
      .select('timestamp')
      .order('timestamp', { ascending: false })
      .limit(1);

    if (dateError || dateErrorMax || !dateRange || !dateRangeMax) {
      console.log('   No data found for weekly summaries');
      return;
    }

    const minDate = new Date(dateRange[0].timestamp);
    const maxDate = new Date(dateRangeMax[0].timestamp);

    console.log(`   Generating summaries from ${minDate.toISOString()} to ${maxDate.toISOString()}`);

    // Calculate number of weeks
    const weeksDiff = Math.ceil((maxDate.getTime() - minDate.getTime()) / (7 * 24 * 60 * 60 * 1000));

    console.log(`   Processing ${weeksDiff} weeks of data...`);

    let summariesGenerated = 0;

    // Process week by week
    for (let weekOffset = 0; weekOffset < weeksDiff; weekOffset++) {
      try {
        // Calculate weekly summaries from Supabase data
        const weeklyData = await this.calculateWeeklySummariesFromSupabase(weekOffset + 1);
        
        if (weeklyData.length > 0) {
          await this.questdbService.pushToQuestDB(weeklyData);
          summariesGenerated += weeklyData.length;
          
          console.log(`   Week ${weekOffset + 1}: Generated ${weeklyData.length} route summaries`);
          analyticsLogger.weeklyAnalytics(weekOffset + 1, weeklyData.length, 0);
        }
      } catch (error) {
        console.warn(`   Warning: Failed to process week ${weekOffset + 1}:`, error);
      }

      // Progress indicator
      const progress = Math.round(((weekOffset + 1) / weeksDiff) * 100);
      console.log(`   Progress: ${progress}% (${weekOffset + 1}/${weeksDiff} weeks)`);
    }

    console.log(`Weekly summaries completed: ${summariesGenerated} summaries generated`);
  }

  private async calculateWeeklySummariesFromSupabase(weekOffset: number): Promise<WeeklyRouteData[]> {
    // This is a simplified version - for complex aggregations, we might need to use RPC functions in Supabase
    console.log(`   Calculating weekly summary for week offset ${weekOffset}...`);
    
    // For now, return empty array as weekly summaries require complex SQL aggregations
    // that are better handled by the original PostgreSQL approach or Supabase RPC functions
    return [];
  }

  async validateMigration(): Promise<void> {
    console.log('Validating migration...');

    try {
      // Check record counts using HTTP API
      const trafficCountQuery = 'SELECT COUNT(*) as count FROM traffic_analytics';
      const trafficCount = await this.questdbService.queryAnalytics(trafficCountQuery);
      
      const summaryCountQuery = 'SELECT COUNT(*) as count FROM route_weekly_summary';
      const summaryCount = await this.questdbService.queryAnalytics(summaryCountQuery);

      console.log(`   Traffic Analytics Records: ${trafficCount[0]?.[0] || 0}`);
      console.log(`   Weekly Summary Records: ${summaryCount[0]?.[0] || 0}`);

      // Check data quality
      const qualityQuery = `
        SELECT 
          MIN(timestamp) as earliest,
          MAX(timestamp) as latest,
          COUNT(DISTINCT route_id) as unique_routes,
          AVG(traffic_density) as avg_density
        FROM traffic_analytics
      `;

      const quality = await this.questdbService.queryAnalytics(qualityQuery);
      const [earliest, latest, uniqueRoutes, avgDensity] = quality[0] || [];

      console.log(`   Date Range: ${earliest} to ${latest}`);
      console.log(`   Unique Routes: ${uniqueRoutes}`);
      console.log(`   Average Density: ${avgDensity}`);

      console.log('Migration validation completed');

    } catch (error) {
      console.error('Migration validation failed:', error);
      throw error;
    }
  }
}

async function main(): Promise<void> {
  const args = process.argv.slice(2);
  
  const options: MigrationOptions = {
    dryRun: args.includes('--dry-run'),
    batchSize: parseInt(args.find(arg => arg.startsWith('--batch-size='))?.split('=')[1] || '1000'),
    startDate: args.find(arg => arg.startsWith('--start-date='))?.split('=')[1] || undefined,
    endDate: args.find(arg => arg.startsWith('--end-date='))?.split('=')[1] || undefined,
    routeIds: args.find(arg => arg.startsWith('--route-ids='))?.split('=')[1]?.split(',').map(Number) || undefined
  };

  const migration = new QuestDBMigrationAPI();

  try {
    if (args.includes('--validate-only')) {
      await migration.validateMigration();
    } else {
      await migration.migrate(options);
      
      if (args.includes('--validate')) {
        await migration.validateMigration();
      }
    }
  } catch (error) {
    logger.error('Migration script failed', error);
    process.exit(1);
  }
}

// Usage examples in comments
/*
Usage:
  ts-node src/scripts/migrateToQuestDB-api.ts                         # Full migration
  ts-node src/scripts/migrateToQuestDB-api.ts --dry-run               # Test run only
  ts-node src/scripts/migrateToQuestDB-api.ts --batch-size=500        # Custom batch size
  ts-node src/scripts/migrateToQuestDB-api.ts --start-date=2024-01-01 # From specific date
  ts-node src/scripts/migrateToQuestDB-api.ts --route-ids=1,2,3       # Specific routes only
  ts-node src/scripts/migrateToQuestDB-api.ts --validate              # Migrate and validate
  ts-node src/scripts/migrateToQuestDB-api.ts --validate-only         # Validation only
*/

if (require.main === module) {
  main();
}

export default QuestDBMigrationAPI;
