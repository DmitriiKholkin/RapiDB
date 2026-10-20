import { describe, expect, it, vi } from "vitest";
import { MongoDBDriver } from "../../src/extension/dbDrivers/mongodb";

describe("MongoDB optimistic literal values", () => {
  it.each(
    [
      { $ne: "never" },
      { $exists: true },
      { $gt: 0 },
      { nested: { $eq: "literal" } },
      ["before"],
      /before/i,
    ].map((value) => ({ value })),
  )("treats the original value %j as data, not a query predicate", async ({
    value,
  }) => {
    const driver = new MongoDBDriver({
      id: "literal-values",
      name: "Literal values",
      type: "mongodb",
    });
    const updateOne = vi.fn().mockResolvedValue({
      acknowledged: true,
      matchedCount: 0,
    });
    Object.assign(driver, {
      connected: true,
      client: { db: () => ({ collection: () => ({ updateOne }) }) },
    });
    const update = {
      primaryKeys: { _id: "one" },
      changes: { payload: { replacement: true } },
      originalValues: { payload: value },
    };
    await driver.updateRows({
      database: "test",
      schema: "",
      table: "items",
      updates: [update],
    });
    expect(updateOne.mock.calls[0][0]).toEqual({
      _id: { $eq: "one" },
      payload: { $eq: value },
      $expr: { $and: [{ $eq: ["$payload", { $literal: value }] }] },
    });
    const preview = driver.buildMutationPreviewStatement(
      "update",
      "test",
      "",
      "items",
      update,
    );
    expect(preview).toContain('"payload": { "$eq":');
    await driver.query(preview);
    expect(updateOne.mock.calls[1][0]).toEqual(updateOne.mock.calls[0][0]);
  });
});
