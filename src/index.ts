import { Client } from "@elastic/elasticsearch";
import { estypes } from "@elastic/elasticsearch";
import * as dotenv from 'dotenv';
import yargs from 'yargs/yargs'; 
import * as fs from 'fs';
import * as path from 'path';
import { parse, addDays, format } from 'date-fns'; 
import { PrismaClient, Prisma } from '@prisma/client';

// Load environment variables from .env file
dotenv.config();

// Initialize Prisma Client
const prisma = new PrismaClient();

// Define CLI arguments
const argv = yargs(process.argv.slice(2))
  .option('startDate', {
    alias: 's',
    description: 'Start date in YYYY-MM-DD format (inclusive, interpreted as GMT+08)',
    type: 'string',
    demandOption: true, // Make it required
  })
  .option('endDate', {
    alias: 'e',
    description: 'End date in YYYY-MM-DD format (exclusive, interpreted as GMT+08)',
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
                     return null; // Skip if parsing fails
                }
            }
        } else {
            // If it's already an object (less likely based on example)
             parsedPayload = source.payload;
        }

        if (typeof parsedPayload !== 'object' || parsedPayload === null) {
             return null;
        }

    } catch (error) {
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

    // --- DB Insert Variables ---
    const batchSize = 1000; // Adjust batch size as needed
    let batchToInsert = []; // Let TypeScript infer the type
    let totalProcessedCount = 0;
    let totalInsertedCount = 0;
    let totalSkippedPayloadCount = 0;
    let totalFailedToInsertCount = 0;
    // --- End DB Insert Variables ---

    try {
        // --- Calculate Date Range Start ---
        // Construct timezone-aware ISO strings for Elasticsearch
        // The start date is inclusive (beginning of the day)
        const esStartDate = `${argv.startDate}T00:00:00+08:00`; 
        // The end date is exclusive (beginning of the day)
        const esEndDate = `${argv.endDate}T00:00:00+08:00`; 
        console.log(`Querying from ${esStartDate} (>=) to ${esEndDate} (<)`);

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
                                    gte: esStartDate, // Use calculated start date
                                    lt: esEndDate,    // Use calculated end date
                                    format: "strict_date_optional_time||epoch_millis", // Specify expected format
                                    time_zone: "+08:00" // Explicitly set timezone for parsing robustness
                                },
                            },
                        },
                    ],
                },
            },
            sort: [
                { "@timestamp": { order: "desc" } } // Sort by timestamp, newest first
            ]
        });

        scrollId = response._scroll_id;
        let hits = response.hits.hits;

        if (response.hits.total) {
            console.log(`Total hits found: ${typeof response.hits.total === 'number' ? response.hits.total : response.hits.total.value}`);
        } else {
             console.log("Total hits not available in response.");
        }

        // --- Process and Insert Loop ---
        while (hits && hits.length > 0) {
            console.log(`Processing batch of ${hits.length} hits...`);
            totalProcessedCount += hits.length;

            for (const hit of hits) {
                if (!hit._source) continue; // Skip if no source data

                const processedDoc = extractPayloadFields(hit._source as LogDocument);

                if (processedDoc) {
                     try {
                         // Map to Prisma model
                         const prismaData = { // Type is inferred here
                             login_time: new Date(processedDoc["@timestamp"]), // Convert string timestamp to Date
                             uid:        processedDoc.uid,
                             email:      processedDoc.payload.profile?.email ?? null,
                             mobile:     processedDoc.payload.profile?.mobile ?? null,
                             name:       processedDoc.payload.profile?.name ?? null,
                             id_type:    processedDoc.payload.profile?.idType ?? null,
                             id_no:      processedDoc.payload.profile?.idNo ?? null,
                             ebid:       processedDoc.payload.employment?.id ?? null,
                             eid:        processedDoc.payload.employment?.eid ?? null,
                             salary:     processedDoc.payload.employment?.salaryDetails?.salary ?? null,
                         };

                         // Validate required fields ( Prisma schema handles nullability, but extra check for login_time/uid)
                         if (!prismaData.login_time || isNaN(prismaData.login_time.getTime()) || !prismaData.uid) {
                            totalSkippedPayloadCount++;
                            continue;
                         }

                         batchToInsert.push(prismaData);
                     } catch (mappingError) {
                         totalSkippedPayloadCount++;
                     }
                } else {
                     totalSkippedPayloadCount++; // Count hits skipped due to payload processing issues
                }
            }

            // Insert batch if size reached
            if (batchToInsert.length >= batchSize) {
                 console.log(`Attempting to insert batch of ${batchToInsert.length} records...`);
                try {
                    const result = await prisma.userLogin.createMany({
                        data: batchToInsert,
                        skipDuplicates: true, // Skip records violating the unique constraint
                    });
                    console.log(`Successfully inserted ${result.count} records.`);
                    totalInsertedCount += result.count;
                    if (batchToInsert.length !== result.count) {
                        totalFailedToInsertCount += (batchToInsert.length - result.count);
                    }
                } catch (dbError) {
                    console.error("Error inserting batch into database:", dbError);
                    totalFailedToInsertCount += batchToInsert.length; // Assume all failed if batch insert throws
                    exitCode = 1; // Mark run as failed
                }
                batchToInsert = []; // Reset batch
            }

            // Fetch the next batch of results
            console.log("Fetching next scroll batch...");
            const scrollResponse = await client.scroll<LogDocument>({
                scroll_id: scrollId,
                scroll: '1m',
            });

            // Update scroll ID and hits for the next iteration
            scrollId = scrollResponse._scroll_id;
            hits = scrollResponse.hits.hits;
        }
         // --- End Process and Insert Loop ---

        // Insert any remaining records in the final batch
        if (batchToInsert.length > 0) {
            console.log(`Attempting to insert final batch of ${batchToInsert.length} records...`);
             try {
                 const result = await prisma.userLogin.createMany({
                     data: batchToInsert,
                     skipDuplicates: true,
                 });
                 console.log(`Successfully inserted ${result.count} records.`);
                 totalInsertedCount += result.count;
                 if (batchToInsert.length !== result.count) {
                    totalFailedToInsertCount += (batchToInsert.length - result.count);
                 }
             } catch (dbError) {
                 console.error("Error inserting final batch into database:", dbError);
                 totalFailedToInsertCount += batchToInsert.length;
                 exitCode = 1; // Mark run as failed
             }
        }

    } catch (error) {
        console.error("An error occurred during the process:", error);
        exitCode = 1; // Mark run as failed
    } finally {
        // Clear the scroll context if it exists
        if (scrollId) {
            console.log("Clearing scroll context...");
            try {
                await client.clearScroll({ scroll_id: scrollId });
                console.log("Scroll context cleared.");
            } catch (clearScrollError) {
                 console.error("Failed to clear scroll context:", clearScrollError);
            }
        }

        // Disconnect Prisma Client
         console.log("Disconnecting Prisma Client...");
        await prisma.$disconnect();
         console.log("Prisma Client disconnected.");

         // --- Final Summary --- 
         console.log("\n--- Data Load Summary ---");
         console.log(`Total Elasticsearch hits processed: ${totalProcessedCount}`);
         console.log(`Records skipped due to payload processing/mapping errors: ${totalSkippedPayloadCount}`);
         console.log(`Records successfully inserted into database: ${totalInsertedCount}`);
         console.log(`Records skipped during insert (duplicates or errors): ${totalFailedToInsertCount}`);
         console.log(`-------------------------`);

        console.log(`Script finished with exit code ${exitCode}.`); // Corrected template literal syntax
        process.exit(exitCode); // Exit with appropriate code
    }
}

run().catch(error => {
    console.error("Unhandled error in run function:", error);
    prisma.$disconnect().finally(() => process.exit(1)); // Ensure disconnect on unhandled error
});
