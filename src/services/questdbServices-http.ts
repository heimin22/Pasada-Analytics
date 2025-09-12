import fetch from 'node-fetch';
import dotenv from 'dotenv';

dotenv.config();

export interface QuestDBConfig {
    pgConnectionString: string;
    ilpEndpoint: string;
    httpEndpoint: string;
    maxConnections?: number;
    connectionTimeout?: number;   
}

export interface TrafficData {
    timestamp: string;
    routeId: number;
    trafficDensity: number;
    duration: number;
    durationInTraffic: number;
    distance: number;
    status: string;
}

export interface QuestDBResponse {
    query: string;
    columns: Array<{
        name: string;
        type: string;
    }>;
    dataset: string[][];
    count: number;
}

export class QuestDBServiceHTTP {
    private config: QuestDBConfig;

    constructor(config: QuestDBConfig) {
        this.config = config;
        this.validateConfig();
    }

    private validateConfig() {
        if (!this.config.httpEndpoint) {
            throw new Error('QuestDB HTTP endpoint is required');
        }
    }

    // Create table if it doesn't exist
    async ensureTablesExist(): Promise<void> {
        try {
            // Create traffic_analytics table
            const createTrafficTableQuery = `
                CREATE TABLE IF NOT EXISTS traffic_analytics (
                    timestamp TIMESTAMP,
                    route_id INT,
                    traffic_density DOUBLE,
                    duration INT,
                    duration_in_traffic INT,
                    distance INT,
                    status STRING,
                    created_at TIMESTAMP
                ) timestamp(timestamp) PARTITION BY DAY
            `;

            await this.executeQuery(createTrafficTableQuery);
            console.log('Traffic analytics table ensured');

            // Create route_weekly_summary table
            const createSummaryTableQuery = `
                CREATE TABLE IF NOT EXISTS route_weekly_summary (
                    timestamp TIMESTAMP,
                    route_id INT,
                    week_start STRING,
                    sample_count INT,
                    avg_traffic_density DOUBLE,
                    min_traffic_density DOUBLE,
                    max_traffic_density DOUBLE,
                    avg_duration_seconds INT,
                    avg_duration_in_traffic_seconds INT,
                    avg_traffic_penalty_fraction DOUBLE,
                    total_distance_meters LONG,
                    avg_speed_kmh DOUBLE,
                    peak_hour INT
                ) timestamp(timestamp) PARTITION BY DAY
            `;

            await this.executeQuery(createSummaryTableQuery);
            console.log('Route weekly summary table ensured');

        } catch (error) {
            console.error('Error ensuring tables exist:', error);
            throw error;
        }
    }

    // Execute SQL query via HTTP
    private async executeQuery(query: string): Promise<any> {
        try {
            const url = new URL(this.config.httpEndpoint + '/exec');
            url.searchParams.set('query', query);
            
            const response = await fetch(url.toString(), {
                method: 'GET',
                headers: { 
                    'Accept': 'application/json',
                    'User-Agent': 'Pasada-Analytics/1.0'
                }
            });

            if (!response.ok) {
                const errorText = await response.text();
                throw new Error(`QuestDB query failed: ${response.status} - ${errorText}`);
            }

            const result = await response.json();
            return result;
        } catch (error) {
            throw new Error(`Query execution failed: ${error}`);
        }
    }

    // Insert traffic data using SQL INSERT
    async saveTrafficDataToQuestDB(trafficData: TrafficData[]): Promise<void> {
        if (!trafficData || trafficData.length === 0) return;

        try {
            // Ensure tables exist
            await this.ensureTablesExist();

            // Build batch INSERT statement
            const values = trafficData.map(data => {
                const timestamp = new Date(data.timestamp).toISOString().replace('T', ' ').slice(0, 19);
                const createdAt = new Date().toISOString().replace('T', ' ').slice(0, 19);
                
                return `('${timestamp}', ${data.routeId}, ${data.trafficDensity}, ${data.duration}, ${data.durationInTraffic}, ${data.distance}, '${data.status}', '${createdAt}')`;
            }).join(',\n    ');

            const insertQuery = `
                INSERT INTO traffic_analytics (
                    timestamp,
                    route_id,
                    traffic_density,
                    duration,
                    duration_in_traffic,
                    distance,
                    status,
                    created_at
                ) VALUES
                    ${values}
            `;

            await this.executeQuery(insertQuery);
            console.log(`Saved ${trafficData.length} traffic data points to QuestDB via HTTP`);

        } catch (error) {
            console.error('Failed to save traffic data to QuestDB:', error);
            throw error;
        }
    }

    // Insert weekly summary data
    async pushToQuestDB(data: any[]): Promise<void> {
        if (!data || data.length === 0) {
            console.log('No weekly summary data to push to QuestDB');
            return;
        }

        try {
            await this.ensureTablesExist();

            const values = data.map(row => {
                const timestamp = new Date(row.week_start).toISOString().replace('T', ' ').slice(0, 19);
                
                return `('${timestamp}', ${row.route_id}, '${row.week_start}', ${row.sample_count}, ${row.avg_traffic_density || 0}, ${row.min_traffic_density || 0}, ${row.max_traffic_density || 0}, ${row.avg_duration_seconds || 0}, ${row.avg_duration_in_traffic_seconds || 0}, ${row.avg_traffic_penalty_fraction || 0}, ${row.total_distance_meters || 0}, ${row.avg_speed_kmh || 0}, ${row.peak_hour || 0})`;
            }).join(',\n    ');

            const insertQuery = `
                INSERT INTO route_weekly_summary (
                    timestamp,
                    route_id,
                    week_start,
                    sample_count,
                    avg_traffic_density,
                    min_traffic_density,
                    max_traffic_density,
                    avg_duration_seconds,
                    avg_duration_in_traffic_seconds,
                    avg_traffic_penalty_fraction,
                    total_distance_meters,
                    avg_speed_kmh,
                    peak_hour
                ) VALUES
                    ${values}
            `;

            await this.executeQuery(insertQuery);
            console.log(`Saved ${data.length} weekly summary rows to QuestDB`);

        } catch (error) {
            console.error('Failed to push weekly summaries to QuestDB:', error);
            throw error;
        }
    }

    // Query QuestDB for analytics
    async queryAnalytics(query: string, params: Record<string, string> = {}): Promise<string[][]> {
        try {
            // Build URL with query parameters
            const url = new URL(this.config.httpEndpoint + '/exec');
            url.searchParams.set('query', query);
            
            // Add additional parameters if provided
            Object.entries(params).forEach(([key, value]) => {
                url.searchParams.set(key, value);
            });
            
            const response = await fetch(url.toString(), {
                method: 'GET',
                headers: { 
                    'Accept': 'application/json',
                    'User-Agent': 'Pasada-Analytics/1.0'
                }
            });

            if (!response.ok) {
                const errorText = await response.text();
                throw new Error(`QuestDB query failed: ${response.status} - ${errorText}`);
            }

            const result = await response.json() as QuestDBResponse;
            return result.dataset || [];
        } catch (error) {
            throw new Error(`Analytics query failed: ${error}`);
        }
    }
}
