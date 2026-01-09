import { v4 as uuidv4 } from "uuid"

/* -- Fake dependencies. Pretend these are real and assume they are correctly implemented. -- */

type ProjectConfiguration = {
    projectId: string;
    enabled: boolean;
    apiKey: string;
    contextWaitMs: number;
    inferenceTimeoutMs: number;
}

type FingerprintRequest = {
    context: Record<string, any>
    metrics: Record<string, any>
}

type FingerprintData = {
    fingerprintId: string;
    createdAt: number;
    data: FingerprintRequest;
}

class Database {
    async getProjectConfiguration(projectId: string): Promise<ProjectConfiguration> {
        console.info("fetching project configuration", { projectId });
        return {
            projectId,
            enabled: true,
            apiKey: 'some-secret',
            contextWaitMs: 50,
            inferenceTimeoutMs: 200,
        }
    }

    async saveFingerprint(projectId: string, requestId: string, fingerprintData: FingerprintData) {
        console.info("saving fingerprint", {
            projectId,
            requestId,
        });
        // no-op
    }
}

class Redis<T> {
    private readonly store: Record<string, T> = {}
    async get(key: string): Promise<T | undefined> {
        return this.store[key]
    } 
}

class InferenceService {
    /* Calculate a fingerprint. This is a call to an external HTTP endpoint.
     * Sometimes fails due to timeout.
     */
    async fingerprint(projectId: string, data: FingerprintRequest, timeoutMs: number): Promise<{ fingerprintId: string }> {
        console.info("calculating fingerprint", { projectId })
        return { fingerprintId: `fp_${projectId}` }
    }
}

class DataQueueingService {
    /*
     * Push arbitrary data attached to a projectId to downstream services which store and index it.
     * This mechanism is backed by a queue. If the queue is unavailable, this will raise an exception.
     */
    async upload(projectId: string, uploadData: Record<string, any>) {
        console.info("Uploading data to downstream services")
        // no-op
    }
}

/* -- End of dependencies -- */

/**
 * Ingest metrics.
 * 
 * This function will be called by a webapp endpoint. 
 * 
 * The steps for metrics ingestion are:
 * 1. Fetch the project configuration to check if ingestion is enabled.
 * 2. Fetch additional context from Redis. This is generated via a separate process and *SHOULD*
 *    be available by the time this function is called.
 * 3. Compute the fingerprint using the fingerprinting service (needs the metrics and the context).
 * 4. Persist metrics and the fingerprint response to the database.
 * 5. Upload the data to a downstream data queueing service.
 * 6. Return a request_id to the caller. 
 *
 * CRITICAL: The request_id must only be returned if the result was successfully saved to the DB.
 * 
 * @param requestBody a string containing JSON. It should have a `project_id` and a large blob of 
 *      data under the `metrics` key.
 */
export default async function ingestMetrics(requestBody: string): Promise<Record<string, any>> {
    const db = new Database()
    const redis = new Redis<string>()
    const inferenceService = new InferenceService()
    const dqs = new DataQueueingService()

    const requestId = uuidv4()
    const receivedAt = Date.now()

    // Parse request
    const body = JSON.parse(requestBody)
    const projectId = body["project_id"]
    const metrics = body["metrics"]
    const traceId = body["trace_id"]
    
    // Load config
    const cfg = await db.getProjectConfiguration(projectId)
    if (!cfg.enabled) {
        return { status: 403, error: "disabled" }
    }

    // Fetch the context from Redis
    let rawData = await redis.get(traceId)
    if (rawData === null) {
        await new Promise((r) => setTimeout(r, cfg.contextWaitMs))
        rawData = await redis.get(traceId)
    }
    const context = JSON.parse(rawData || "{}")

    // Call the fingerprinting service
    let fingerprintId: string;
    try {
        const resp = await inferenceService.fingerprint(projectId, {
            context,
            metrics,
        }, cfg.inferenceTimeoutMs)
        fingerprintId = resp["fingerprintId"]
    } catch (err) {
        console.error("failed to call inference service")
        return {
            status: 200,
            request_id: requestId,
            error: "inference failed"
        }
    }

    // Persist fingerprint
    try {
        await db.saveFingerprint(projectId, requestId, {
            fingerprintId,
            data: {
                metrics,
                context,
            },
            createdAt: receivedAt
        })
    } catch (_) {
        console.error("failed to save fingerprint")
    }

    const payload = {
        requestId,
        projectId,
        receivedAt,
        traceId,
        fingerprintId,
        metrics,
        context,
    }

    await dqs.upload(projectId, payload);

    console.info("metrics ingestion complete")
    return {
        status: 200,
        project_id: projectId,
        request_id: requestId,
        fingerprint_id: fingerprintId,
    }
}
