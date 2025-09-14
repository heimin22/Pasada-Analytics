#!/usr/bin/env ts-node
import { QuestDBService } from '../services/questdbServices';
import { env } from '../config/environment';
import { Client } from 'pg';
import logger, { analyticsLogger } from '../utils/logger';
import { WeeklySummaryData } from '../types/traffic';

interface MigrationOptions {
  dryRun: boolean;
  batchSize: number;
  startDate?: string | undefined;
  endDate?: string | undefined;
  routeIds?: number[] | undefined;
}

class QuestDBMigration {
  private questdbService: QuestDBService;
  private supabaseClient: Client;

  constructor() {
    this.questdbService = new QuestDBService(env.questdb);
    this.supabaseClient = new Client({
      connectionString: env.postgresConnection,
    });
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
    console.log('Starting QuestDB Migration...');
    console.log(`   Dry Run: ${options.dryRun ? 'Yes' : 'No'}`);
    console.log(`   Batch Size: ${options.batchSize}`);
    console.log(`   Date Range: ${options.startDate || 'All'} to ${options.endDate || 'All'}`);
    console.log('');

    try {
      // Connect to Supabase (we'll use HTTP APIs for QuestDB)
      await this.supabaseClient.connect();
      console.log('Connected to Supabase PostgreSQL');

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
    } finally {
      // Only disconnect from services that were connected
      try {
        await this.supabaseClient.end();
        console.log('Disconnected from Supabase');
      } catch (error) {
        console.warn('Error disconnecting from Supabase:', error);
      }
    }
  }

  private async migrateTrafficAnalytics(options: MigrationOptions): Promise<void> {
    console.log('Migrating traffic analytics data...');

    // Build query with filters
    let query = `
      SELECT 
        route_id,
        timestamp,
        traffic_density,
        duration,
        duration_in_traffic,
        distance,
        status,
        created_at
      FROM public.traffic_analytics
      WHERE 1=1
    `;

    const queryParams: unknown[] = [];
    let paramIndex = 1;

    if (options.startDate) {
      query += ` AND timestamp >= $${paramIndex}`;
      queryParams.push(options.startDate);
      paramIndex++;
    }

    if (options.endDate) {
      query += ` AND timestamp <= $${paramIndex}`;
      queryParams.push(options.endDate);
      paramIndex++;
    }

    if (options.routeIds && options.routeIds.length > 0) {
      query += ` AND route_id = ANY($${paramIndex})`;
      queryParams.push(options.routeIds);
      paramIndex++;
    }

    query += ' ORDER BY timestamp ASC';

    // Get total count
    const countQuery = query.replace(/SELECT[\s\S]*?FROM/, 'SELECT COUNT(*) FROM');
    const countResult = await this.supabaseClient.query(countQuery, queryParams);
    const totalRecords = parseInt(countResult.rows[0].count);

    console.log(`   Total records to migrate: ${totalRecords}`);

    if (totalRecords === 0) {
      console.log('   No records found to migrate');
      return;
    }

    // Process in batches
    let offset = 0;
    let totalMigrated = 0;

    while (offset < totalRecords) {
      const batchQuery = `${query} LIMIT ${options.batchSize} OFFSET ${offset}`;
      const result = await this.supabaseClient.query(batchQuery, queryParams);
      
      if (result.rows.length === 0) {
        break;
      }

      console.log(`   Processing batch: ${offset + 1}-${offset + result.rows.length} of ${totalRecords}`);

      if (!options.dryRun) {
        // Transform data for QuestDB
        const trafficData = result.rows.map(row => ({
          timestamp: row.timestamp.toISOString(),
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
          batchSize: result.rows.length
        });
      }

      totalMigrated += result.rows.length;
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

    // Get date range for weekly summaries
    const dateRangeQuery = `
      SELECT 
        MIN(timestamp) as min_date,
        MAX(timestamp) as max_date
      FROM public.traffic_analytics
    `;

    const dateResult = await this.supabaseClient.query(dateRangeQuery);
    const { min_date, max_date } = dateResult.rows[0];

    if (!min_date || !max_date) {
      console.log('   No data found for weekly summaries');
      return;
    }

    console.log(`   Generating summaries from ${min_date} to ${max_date}`);

    // Calculate number of weeks
    const startDate = new Date(min_date);
    const endDate = new Date(max_date);
    const weeksDiff = Math.ceil((endDate.getTime() - startDate.getTime()) / (7 * 24 * 60 * 60 * 1000));

    console.log(`   Processing ${weeksDiff} weeks of data...`);

    let summariesGenerated = 0;

    // Process week by week - using Supabase data directly instead of QuestDB
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

  private async calculateWeeklySummariesFromSupabase(weekOffset: number): Promise<WeeklySummaryData[]> {
    // This method calculates weekly summaries directly from Supabase data
    // Similar to the query in QuestDBService.getWeeklyAnalytics
    const query = `
      WITH week_bounds AS (
        SELECT
          (date_trunc('week', now() AT TIME ZONE 'Asia/Manila') AT TIME ZONE 'Asia/Manila')::date - INTERVAL '${weekOffset * 7} days' AS start_dt
      )
      SELECT
        (wb.start_dt)::date AS week_start,
        t.route_id,
        COUNT(*) AS sample_count,
        AVG(traffic_density)::numeric(6,4) AS avg_traffic_density,
        MIN(traffic_density)::numeric(6,4) AS min_traffic_density,
        MAX(traffic_density)::numeric(6,4) AS max_traffic_density,
        AVG(duration) FILTER (WHERE duration IS NOT NULL)::numeric(10,2) AS avg_duration_seconds,
        AVG(duration_in_traffic) FILTER (WHERE duration_in_traffic IS NOT NULL)::numeric(10,2) AS avg_duration_in_traffic_seconds,
        AVG(
          CASE WHEN duration IS NOT NULL AND duration > 0 AND duration_in_traffic IS NOT NULL 
               THEN (duration_in_traffic - duration)::double precision / duration
               ELSE NULL END
        )::numeric(6,4) AS avg_traffic_penalty_fraction,
        SUM(distance) FILTER (WHERE distance IS NOT NULL)::bigint AS total_distance_meters,
        (CASE 
           WHEN SUM(CASE WHEN duration > 0 THEN distance ELSE 0 END) = 0 THEN NULL
           ELSE (SUM(distance) FILTER (WHERE duration > 0) / NULLIF(SUM(duration) FILTER (WHERE duration > 0),0)) * 3.6
         END)::numeric(10,2) AS avg_speed_kmh,
        (SELECT (array_agg(hour ORDER BY cnt DESC))[1]
         FROM (
           SELECT date_part('hour', timestamp AT TIME ZONE 'Asia/Manila')::int AS hour, COUNT(*) AS cnt
           FROM public.traffic_analytics ta2
           WHERE ta2.route_id = t.route_id
             AND ta2.timestamp >= wb.start_dt
             AND ta2.timestamp < wb.start_dt + INTERVAL '7 days'
           GROUP BY hour
         ) h
        )::int AS peak_hour
      FROM public.traffic_analytics t
      CROSS JOIN week_bounds wb
      WHERE t.timestamp >= wb.start_dt
        AND t.timestamp < wb.start_dt + INTERVAL '7 days'
      GROUP BY week_start, t.route_id
      ORDER BY week_start, t.route_id
    `;

    const result = await this.supabaseClient.query(query);
    return result.rows;
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

async function main() {
  const args = process.argv.slice(2);
  
  const options: MigrationOptions = {
    dryRun: args.includes('--dry-run'),
    batchSize: parseInt(args.find(arg => arg.startsWith('--batch-size='))?.split('=')[1] || '1000'),
    startDate: args.find(arg => arg.startsWith('--start-date='))?.split('=')[1] || undefined,
    endDate: args.find(arg => arg.startsWith('--end-date='))?.split('=')[1] || undefined,
    routeIds: args.find(arg => arg.startsWith('--route-ids='))?.split('=')[1]?.split(',').map(Number) || undefined
  };

  const migration = new QuestDBMigration();

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
  npm run analytics:migrate                                    # Full migration
  npm run analytics:migrate -- --dry-run                      # Test run only
  npm run analytics:migrate -- --batch-size=500               # Custom batch size
  npm run analytics:migrate -- --start-date=2024-01-01        # From specific date
  npm run analytics:migrate -- --route-ids=1,2,3              # Specific routes only
  npm run analytics:migrate -- --validate                     # Migrate and validate
  npm run analytics:migrate -- --validate-only                # Validation only
*/

if (require.main === module) {
  main();
}
