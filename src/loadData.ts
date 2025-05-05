import { PrismaClient } from '@prisma/client';
import fs from 'fs';
import path from 'path';
import { parse } from 'csv-parse';

// Initialize Prisma Client
const prisma = new PrismaClient();

// Define the path to the CSV file relative to the project root
// __dirname is the directory of the current module (src), so go up one and then to data
const csvFilePath = path.resolve(__dirname, '../data/elastic_data_2025-01-01_to_2025-01-02.csv');
// Define batch size for insertions
const batchSize = 1000;

async function loadCsvData() {
    console.log(`Starting data load from ${csvFilePath}...`);
    if (!fs.existsSync(csvFilePath)) {
        console.error(`Error: CSV file not found at ${csvFilePath}`);
        return;
    }

    let batch: any[] = [];
    let processedRecordCount = 0;
    let insertedRecordCount = 0;
    let skippedRecordCount = 0;

    // Create a readable stream for the CSV file
    const fileStream = fs.createReadStream(csvFilePath);

    // Create a CSV parser instance
    const parser = parse({
        columns: true,          // Treat the first row as headers
        skip_empty_lines: true, // Skip empty lines
        trim: true,             // Trim whitespace from values
        // Cast values from CSV strings to appropriate types for Prisma
        cast: (value, context) => {
            // Treat empty strings as null for all potentially nullable fields
            if (value === '') {
                return null;
            }

            const column = context.column as string; // Get header name

            // Cast 'login_time' to Date object
            if (column === 'login_time') {
                try {
                    const date = new Date(value);
                    // Check if the parsed date is valid
                    if (isNaN(date.getTime())) {
                        console.warn(`[Record ${context.lines}] Invalid date format for login_time: "${value}". Record will be skipped.`);
                        return undefined; // Use undefined to signal skipping this record later
                    }
                    return date;
                } catch (e) {
                    console.warn(`[Record ${context.lines}] Error parsing date for login_time: "${value}". Record will be skipped. Error: ${e}`);
                    return undefined; // Use undefined to signal skipping this record later
                }
            }

            // Cast 'salary' to Integer
            if (column === 'salary') {
                const num = parseInt(value, 10);
                 // If parsing fails (NaN), return null, otherwise return the number
                return isNaN(num) ? null : num;
            }

            // Return other columns as strings (or null if handled above)
            return value;
        }
    });

    // Pipe the file stream to the parser
    fileStream.pipe(parser);

    try {
        // Process records using async iteration
        for await (const record of parser) {
            processedRecordCount++;

             // Skip record if casting failed (e.g., invalid date returned undefined)
             if (record.login_time === undefined) {
                 skippedRecordCount++;
                 continue; // Skip this record and move to the next
             }

            // Prepare record for Prisma (ensure field names match the model)
            const prismaData = {
                login_time: record.login_time,
                uid:        record.uid,
                email:      record.email,
                mobile:     record.mobile,
                name:       record.name,
                id_type:    record.id_type,
                id_no:      record.id_no,
                ebid:       record.ebid,
                eid:        record.eid,
                salary:     record.salary,
            };

            batch.push(prismaData);

            // If batch size is reached, insert the batch
            if (batch.length >= batchSize) {
                const result = await prisma.userLogin.createMany({
                    data: batch,
                    skipDuplicates: false, // Set to true if you want to ignore duplicates based on unique constraints
                });
                insertedRecordCount += result.count;
                console.log(`Inserted batch of ${result.count} records. Total processed: ${processedRecordCount}`);
                batch = []; // Reset the batch
            }
        }

        // Insert any remaining records in the final batch
        if (batch.length > 0) {
            const result = await prisma.userLogin.createMany({
                data: batch,
                skipDuplicates: false,
            });
            insertedRecordCount += result.count;
            console.log(`Inserted final batch of ${result.count} records.`);
        }

        console.log(`\n--- Data Load Summary ---`);
        console.log(`Processed ${processedRecordCount} records from CSV.`);
        console.log(`Inserted ${insertedRecordCount} records into the database.`);
        console.log(`Skipped ${skippedRecordCount} records due to parsing errors.`);
        console.log(`-------------------------`);

    } catch (error: any) {
        console.error(`Error during data loading process: ${error.message}`);
         if (error.code === 'P2002') { // Prisma unique constraint violation code
             console.error('Details: A unique constraint violation occurred. Check your data for duplicates or adjust schema constraints.');
         } else {
             console.error('Error Details:', error);
         }
        // Log records in the current batch if an error occurs during insertion
        if (batch.length > 0) {
            console.error(`Data in batch during error (first 5 records): ${JSON.stringify(batch.slice(0, 5), null, 2)}`);
        }
    } finally {
        // Ensure Prisma Client disconnects
        await prisma.$disconnect();
        console.log('Database connection closed.');
    }
}

// Execute the main function and handle potential errors
loadCsvData().catch((e) => {
    console.error('Unhandled error executing loadCsvData:', e);
    prisma.$disconnect().finally(() => process.exit(1));
});
