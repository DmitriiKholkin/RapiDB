import { describe, expect, it } from "vitest";
import { pMapWithLimit } from "../../src/extension/utils/concurrency";

describe("pMapWithLimit", () => {
  it("preserves input order across concurrent workers", async () => {
    const values = [1, 2, 3, 4, 5];

    const results = await pMapWithLimit(values, 2, async (value) => {
      await Promise.resolve();
      return value * 10;
    });

    expect(results).toEqual([10, 20, 30, 40, 50]);
  });
});
