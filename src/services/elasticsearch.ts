import { Client } from "@elastic/elasticsearch";
import { estypes } from "@elastic/elasticsearch";
import { config } from '../config';
import { LogDocument } from '../types';

// Initialize Elasticsearch Client
const client = new Client({
    cloud: { id: config.elastic.cloudId },
    auth: { apiKey: config.elastic.apiKey },
    requestTimeout: 300000 // 5 minutes
});

// Function to fetch logs using scroll API
export async function* fetchLogs(startDate: string, endDate: string, queryType: string = 'retrieveUserInfo'): AsyncGenerator<LogDocument[], void, undefined> {
    console.log(`Querying index pattern: ${config.elastic.indexPattern}`);
    console.log(`Querying from ${startDate} (>=) to ${endDate} (<)`);
    console.log(`Using query type: ${queryType}`);

    // Get query configuration
    const queryConfig = config.queries[queryType as keyof typeof config.queries];
    if (!queryConfig) {
        throw new Error(`Unknown query type: ${queryType}`);
    }

    // Define fields to retrieve based on query type
    const fieldsToRetrieve = queryType === 'bukopin' 
        ? ["@timestamp", "payload"] 
        : ["@timestamp", "uid", "payload"];

    let scrollId: string | undefined;

    try {
        // Initial search request with scroll parameter
        const response = await client.search<LogDocument>({
            index: config.elastic.indexPattern,
            scroll: '5m', // Keep the search context alive for 2 minutes
            size: 6000,  // Fetch 1000 docs per scroll page
            _source: fieldsToRetrieve, // Specify fields to retrieve
            track_total_hits: true,
            body: { // Use 'body' for query structure
                query: {
                    bool: {
                        must: [
                            // Different query structure based on query type
                            ...((queryConfig as any).module 
                                ? [{ match: { module: (queryConfig as any).module } }]
                                : []),
                            ...((queryConfig as any).type 
                                ? [{ match: { type: (queryConfig as any).type } }]
                                : []),
                            ...((queryConfig as any).payloadSearch 
                                ? [{ match_phrase: { payload: (queryConfig as any).payloadSearch } }]
                                : []),
                            { match: { action: queryConfig.action } }
                        ],
                        filter: [
                            {
                                range: {
                                    "@timestamp": {
                                        gte: startDate,
                                        lt: endDate,
                                        time_zone: "+08:00" // Explicitly set timezone
                                    }
                                }
                            }
                        ]
                    }
                }
            }
        });

        let hits = response.hits.hits;
        scrollId = response._scroll_id;
        const totalHits = typeof response.hits.total === 'number' ? response.hits.total : response.hits.total?.value ?? 0;

        console.log(`Total matching documents found: ${totalHits}`);

        while (hits && hits.length > 0 && scrollId) {
            // Yield the current batch of source documents
            yield hits.map(hit => hit._source as LogDocument);

            // Fetch the next batch
            const scrollResponse = await client.scroll<LogDocument>({
                scroll_id: scrollId,
                scroll: '2m',
            });

            scrollId = scrollResponse._scroll_id; // Update scroll ID
            hits = scrollResponse.hits.hits;
        }

    } catch (error) {
        console.error("Error fetching data from Elasticsearch:", error);
        throw error; // Re-throw the error to be handled by the caller
    } finally {
        // Clear the scroll context if it exists
        if (scrollId) {
            try {
                await client.clearScroll({ scroll_id: [scrollId] });
                console.log("Elasticsearch scroll context cleared.");
            } catch (clearError) {
                console.error("Error clearing Elasticsearch scroll context:", clearError);
            }
        }
    }
}
