import { expect, test, describe } from "vitest"
import ingestMetrics from "."

describe("ingestMetrics", () => {

    test("is not implemented", async () => {
        await expect(() => ingestMetrics("")).rejects.toThrowError()
    })

})