import { expect, test, describe } from "vitest"
import ingestMetrics from "."

describe("ingestMetrics", () => {

    test("returns expectedly", async () => {
        const result = await ingestMetrics('{"project_id": "abc123", "metrics": {}, "trace_id": "xyz"}')
        expect(result).toStrictEqual({
            fingerprint_id: "fp_abc123",
            project_id: "abc123",
            request_id: expect.any(String),
            status: 200,
        })
    })

})