import { PrismaClient, Prisma } from '@prisma/client';
import { ProcessedLogDocument } from '../types';

// Initialize Prisma Client
const prisma = new PrismaClient();

export async function disconnectDb() {
    await prisma.$disconnect();
    console.log('Disconnected from database.');
}

// Function to batch insert bukopin records into the database
export async function batchInsertBukopinRecords(batch: ProcessedLogDocument[]): Promise<{ inserted: number; skipped: number }> {
    if (batch.length === 0) {
        return { inserted: 0, skipped: 0 };
    }

    const recordsToCreate = batch.map(doc => ({
        timestamp: new Date(doc["@timestamp"]),
        payload: doc.payload.raw, // Store raw payload as JSON
    }));

    let insertedCount = 0;
    let skippedCount = 0;

    try {
        // Attempt to insert the batch
        const result = await (prisma as any).bukopinData.createMany({
            data: recordsToCreate,
            skipDuplicates: true, // Use Prisma's built-in duplicate skipping
        });
        insertedCount = result.count;
        skippedCount = batch.length - result.count; // Records not inserted were skipped
    } catch (e) {
        console.error(`Error during bukopin batch insert:`, e);
        // If createMany fails entirely (not just skipping duplicates), consider all as failed/skipped for this batch
        return { inserted: 0, skipped: batch.length };
    }
    return { inserted: insertedCount, skipped: skippedCount };
}

// Function to batch insert records into the database
export async function batchInsertUserRecords(batch: ProcessedLogDocument[]): Promise<{ inserted: number; skipped: number }> {
    if (batch.length === 0) {
        return { inserted: 0, skipped: 0 };
    }

    const recordsToCreate = batch.map(doc => ({
        login_time: new Date(doc["@timestamp"]),
        uid: doc.uid,
        email: doc.payload.profile?.email,
        mobile: doc.payload.profile?.mobile,
        name: doc.payload.profile?.name,
        id_type: doc.payload.profile?.idType,
        id_no: doc.payload.profile?.idNo,
        ebid: doc.payload.employment?.id,
        eid: doc.payload.employment?.eid,
        salary: doc.payload.employment?.salaryDetails?.salary,
    }));

    let insertedCount = 0;
    let skippedCount = 0;

    try {
        // Attempt to insert the batch
        const result = await prisma.userLogin.createMany({
            data: recordsToCreate,
            skipDuplicates: true, // Use Prisma's built-in duplicate skipping
        });
        insertedCount = result.count;
        skippedCount = batch.length - result.count; // Records not inserted were skipped
        // console.log(`Batch insert: ${insertedCount} inserted, ${skippedCount} skipped (duplicates).`);
    } catch (e) {
        console.error(`Error during batch insert:`, e);
        // If createMany fails entirely (not just skipping duplicates), consider all as failed/skipped for this batch
        return { inserted: 0, skipped: batch.length };
    }
    return { inserted: insertedCount, skipped: skippedCount };
}
