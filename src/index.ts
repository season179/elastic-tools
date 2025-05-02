import { Client } from "@elastic/elasticsearch";
import { estypes } from "@elastic/elasticsearch";
import * as dotenv from 'dotenv';
import yargs from 'yargs/yargs'; 
import * as fs from 'fs';
import * as path from 'path';

// Load environment variables from .env file
dotenv.config();

// Define CLI arguments
const argv = yargs(process.argv.slice(2))
  .option('startDate', {
    alias: 's',
    description: 'Start date in YYYY-MM-DD format (inclusive)',
    type: 'string',
    demandOption: true, // Make it required
  })
  .option('endDate', {
    alias: 'e',
    description: 'End date in YYYY-MM-DD format (exclusive)',
    type: 'string',
    demandOption: true, // Make it required
  })
  .help()
  .alias('help', 'h')
  .parseSync(); 

// Check for required environment variables
if (!process.env.ELASTIC_CLOUD_ID || !process.env.ELASTIC_API_KEY) {
    throw new Error(
        "Missing required environment variables: ELASTIC_CLOUD_ID and/or ELASTIC_API_KEY"
    );
}

const client = new Client({
    cloud: { id: process.env.ELASTIC_CLOUD_ID as string },
    auth: { apiKey: process.env.ELASTIC_API_KEY as string },
});

// Alternative configuration using Node URL:
// const client = new Client({
//   node: process.env.ELASTIC_NODE,
//   auth: { apiKey: process.env.ELASTIC_API_KEY }
// })

// Define the structure of the documents we expect to retrieve
interface LogDocument {
    "@timestamp": string; // Timestamp field
    uid: string;         // UID field
    payload: any;       // Payload field (can be complex)
    // Add other relevant fields if known
}

// --- Added Interfaces and Helper Function Start ---
interface ProcessedPayload {
    profile?: {
        email?: string;
        mobile?: string;
        name?: string;
        idType?: string;
        idNo?: string;
    };
    employment?: {
        id?: string;
        eid?: string;
        salaryDetails?: {
            salary?: number;
        };
    };
}

interface ProcessedLogDocument {
    "@timestamp": string;
    uid: string;
    payload: ProcessedPayload;
}

// Helper function to extract desired fields
function extractPayloadFields(source: LogDocument): ProcessedLogDocument | null {
    if (!source || !source.payload) {
        console.warn("Skipping hit with missing source or payload:", source?.uid);
        return null;
    }

    let parsedPayload: any;
    try {
        // Assuming payload is a stringified JSON, potentially double-encoded
        if (typeof source.payload === 'string') {
            try {
                // First attempt: assume single JSON string
                parsedPayload = JSON.parse(source.payload);
            } catch (e) {
                // Second attempt: handle potential double escaping like \""
                try {
                   const correctedString = source.payload.replace(/\\"/g, '"');
                   // Check if it starts/ends with extra quotes after replacement
                   if (correctedString.startsWith('"') && correctedString.endsWith('"')) {
                       parsedPayload = JSON.parse(correctedString.slice(1, -1));
                   } else {
                       parsedPayload = JSON.parse(correctedString);
                   }
                } catch (e2) {
                     console.error(`Error parsing payload JSON for UID ${source.uid}:`, e2, "Original payload:", source.payload);
                     return null; // Skip if parsing fails
                }
            }
        } else {
            // If it's already an object (less likely based on example)
             parsedPayload = source.payload;
        }

        if (typeof parsedPayload !== 'object' || parsedPayload === null) {
             console.warn(`Parsed payload is not an object for UID ${source.uid}. Type: ${typeof parsedPayload}`);
             return null;
        }

    } catch (error) {
        console.error(`Error processing payload for UID ${source.uid}:`, error, "Payload:", source.payload);
        return null; // Skip if any processing error occurs
    }

    const processed: ProcessedPayload = {};

    // Extract profile fields
    if (parsedPayload.profile) {
        processed.profile = {
            email: parsedPayload.profile.email,
            mobile: parsedPayload.profile.mobile,
            name: parsedPayload.profile.name,
            idType: parsedPayload.profile.idType,
            idNo: parsedPayload.profile.idNo,
        };
    }

    // Extract employment fields (assuming we take the first employment entry)
    if (parsedPayload.employment && Array.isArray(parsedPayload.employment) && parsedPayload.employment.length > 0) {
        const firstEmployment = parsedPayload.employment[0];
        processed.employment = {
            id: firstEmployment.id,
            eid: firstEmployment.eid,
            salaryDetails: firstEmployment.salaryDetails ? {
                salary: firstEmployment.salaryDetails.salary
            } : undefined
        };
    }

    return {
        "@timestamp": source["@timestamp"],
        uid: source.uid,
        payload: processed
    };
}
// --- Added Interfaces and Helper Function End ---

async function run() {
    // Use environment variable for the index pattern
    const indexPattern = process.env.ELASTIC_INDEX_PATTERN;
    if (!indexPattern) {
        console.error("Error: ELASTIC_INDEX_PATTERN environment variable is not set.");
        process.exit(1); // Exit if the crucial variable is missing
    }

    console.log(`Querying index pattern: ${indexPattern}`);
    let exitCode = 0; // Default to success
    let scrollId: string | undefined = undefined; // To store scroll ID
    const allHits: ProcessedLogDocument[] = []; // Store processed hits

    try {
        // Initial search request with scroll parameter
        const response = await client.search<LogDocument>({
            index: indexPattern,
            scroll: '1m', // Keep the search context alive for 1 minute
            size: 5000, // Fetch 1000 docs per scroll page
            _source: ["@timestamp", "uid", "payload"],
            track_total_hits: true,
            query: {
                bool: {
                    must: [
                        // Revert to 'match' query for 'module'
                        { match: { module: "RetrieveUserInfo" } },
                        // Use 'match' query for potentially analyzed 'action' field
                        { match: { action: "response" } }
                    ],
                    filter: [
                        {
                            range: {
                                "@timestamp": {
                                    gte: argv.startDate, // Use start date from CLI argument
                                    lt: argv.endDate    // Use end date from CLI argument
                                }
                            }
                        }
                    ]
                },
            },
            sort: [
                { "@timestamp": { order: "desc" } } // Sort by timestamp, newest first
            ]
        });

        let currentHits = response.hits.hits;
        scrollId = response._scroll_id;

        // Accumulate initial batch of hits
        currentHits.forEach(hit => {
            if (hit._source) {
                const processed = extractPayloadFields(hit._source);
                if (processed) {
                    allHits.push(processed);
                }
            }
        });

        console.log(`Initial fetch: ${currentHits.length} hits, processed: ${allHits.length}.`);

        // Loop through subsequent pages using the scroll API
        while (scrollId && currentHits.length > 0) {
            console.log(`Fetching next batch with scroll_id: ${scrollId.substring(0, 10)}...`);
            const scrollResponse = await client.scroll<LogDocument>({
                scroll_id: scrollId,
                scroll: '1m'
            });

            scrollId = scrollResponse._scroll_id; // Update scroll ID for the next iteration
            currentHits = scrollResponse.hits.hits;

            if (currentHits.length > 0) {
                console.log(`Fetched ${currentHits.length} more hits.`);
                let processedCount = 0;
                currentHits.forEach(hit => {
                    if (hit._source) {
                        const processed = extractPayloadFields(hit._source);
                        if (processed) {
                            allHits.push(processed);
                            processedCount++;
                        }
                    }
                });
                console.log(`Processed ${processedCount} hits from this batch.`);
            } else {
                console.log("No more hits to fetch.");
            }
        }

        console.log(`\nTotal documents retrieved: ${allHits.length}`);

        // ---- CSV Writing Logic Start (after collecting all hits) ----
        if (allHits.length > 0) {
            const dataDir = path.join(__dirname, '..', 'data');
            if (!fs.existsSync(dataDir)) {
                fs.mkdirSync(dataDir, { recursive: true });
            }
            
            // Construct filename with date range
            const startDateStr = argv.startDate.split('T')[0]; // Get YYYY-MM-DD
            const endDateStr = argv.endDate.split('T')[0];   // Get YYYY-MM-DD
            const filename = path.join(dataDir, `elastic_data_${startDateStr}_to_${endDateStr}.csv`);

            // Prepare headers - adjust based on ProcessedLogDocument structure
            const headers = ['"@timestamp"', '"uid"', '"profile.email"', '"profile.mobile"', '"profile.name"', '"profile.idType"', '"profile.idNo"', '"employment.id"', '"employment.eid"', '"employment.salaryDetails.salary"'];
            const csvWriter = fs.createWriteStream(filename);
            csvWriter.write(headers.join(',') + '\n');

            // Write data rows
            allHits.forEach(doc => {
                const profile = doc.payload.profile || {};
                const employment = doc.payload.employment || {};
                const salaryDetails = employment.salaryDetails || {};

                const row = [
                    `"${doc['@timestamp']}"`, 
                    `"${doc.uid}"`,
                    `"${profile.email || ''}"`,
                    `"${profile.mobile || ''}"`,
                    `"${profile.name || ''}"`,
                    `"${profile.idType || ''}"`,
                    `"${profile.idNo || ''}"`,
                    `"${employment.id || ''}"`,
                    `"${employment.eid || ''}"`,
                    `"${salaryDetails.salary !== undefined ? salaryDetails.salary : ''}"` // Handle potential undefined salary
                ];
                csvWriter.write(row.join(',') + '\n');
            });

            csvWriter.end();
            console.log(`\nData successfully written to ${filename}`);
        } else {
            console.log("\nNo documents matched the criteria.");
        }
        // ---- CSV Writing Logic End ----
    } catch (error) {
        console.error("Error executing search or scroll:", error);
        const errorBody = (error as any)?.meta?.body;
        if (errorBody) {
            console.error("Elasticsearch error details:", JSON.stringify(errorBody, null, 2));
        }
        exitCode = 1; // Set exit code to error
    } finally {
        // Ensure client connection is closed before exiting
        console.log("Closing Elasticsearch client connection...");
        // Clear the scroll context if it exists
        if (scrollId) {
            try {
                console.log(`Clearing scroll context: ${scrollId.substring(0, 10)}...`);
                await client.clearScroll({
                    scroll_id: scrollId,
                });
                console.log("Scroll context cleared.");
            } catch (clearScrollError) {
                console.error("Error clearing scroll context:", clearScrollError);
                // Don't necessarily exit with error 1 just because clearScroll failed
            }
        }
        try {
            await client.close(); // Attempt to close the client
            console.log("Client closed.");
        } catch (closeError) {
            console.error("Error closing Elasticsearch client:", closeError);
            exitCode = 1; // Ensure we exit with an error code if closing fails
        }
        process.exit(exitCode); // Exit with the appropriate code
    }
}

run().catch(console.log);
