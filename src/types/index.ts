// Define the structure of the raw documents from Elasticsearch
export interface LogDocument {
    "@timestamp": string; // Timestamp field
    uid: string;         // UID field
    payload: any;       // Payload field (can be complex)
    // Add other relevant fields if known
}

// Define the structure of the processed payload
export interface ProcessedPayload {
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
    raw?: any; // For storing raw payload data (e.g., bukopin queries)
}

// Define the structure of the fully processed log document ready for DB insertion
export interface ProcessedLogDocument {
    "@timestamp": string;
    uid: string;
    payload: ProcessedPayload;
}
