import { LogDocument, ProcessedLogDocument, ProcessedPayload } from '../types';

// Helper function to extract desired fields from a raw log document
export function extractPayloadFields(source: LogDocument, queryType: string = 'retrieveUserInfo'): ProcessedLogDocument | null {
    if (!source || !source.payload) {
        return null;
    }

    // For bukopin queries, return simpler structure with just timestamp and payload
    if (queryType === 'bukopin') {
        return {
            "@timestamp": source["@timestamp"],
            uid: source.uid || '', // Provide empty string if uid is not available
            payload: { raw: source.payload } // Store raw payload for bukopin
        };
    }

    let parsedPayload: any;
    try {
        // Assuming payload is a stringified JSON, potentially double-encoded
        if (typeof source.payload === 'string') {
            try {
                // First attempt: assume single JSON string
                parsedPayload = JSON.parse(source.payload);
            } catch (e) {
                // Second attempt: handle potential double escaping like \"
                try {
                   const correctedString = source.payload.replace(/\\"/g, '"');
                   // Check if it starts/ends with extra quotes after replacement
                   if (correctedString.startsWith('"') && correctedString.endsWith('"')) {
                       parsedPayload = JSON.parse(correctedString.slice(1, -1));
                   } else {
                       parsedPayload = JSON.parse(correctedString);
                   }
                } catch (e2) {
                     console.warn(`Skipping payload due to parsing error (double escape check): ${source.uid}, ${source['@timestamp']}`);
                     return null; // Skip if parsing fails
                }
            }
        } else {
            // If it's already an object
             parsedPayload = source.payload;
        }

        if (typeof parsedPayload !== 'object' || parsedPayload === null) {
            console.warn(`Skipping payload because it's not a valid object after parsing: ${source.uid}, ${source['@timestamp']}`);
             return null;
        }

    } catch (error) {
        console.warn(`Skipping payload due to general processing error: ${source.uid}, ${source['@timestamp']}`, error);
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

    // Return null if both profile and employment are empty/missing after processing
    if (Object.keys(processed).length === 0) {
        // console.log(`Skipping record ${source.uid} as no relevant payload fields were extracted.`);
        return null;
    }

    return {
        "@timestamp": source["@timestamp"],
        uid: source.uid,
        payload: processed
    };
}
