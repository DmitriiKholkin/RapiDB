import { describe, expect, it, vi } from "vitest";
import { DynamoDBDriver } from "../../src/extension/dbDrivers/dynamodb";
import { ElasticsearchDriver } from "../../src/extension/dbDrivers/elasticsearch";
import { MongoDBDriver } from "../../src/extension/dbDrivers/mongodb";
import { RedisDriver } from "../../src/extension/dbDrivers/redis";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";

function mongo() {
  return new MongoDBDriver({
    id: "m",
    name: "m",
    type: "mongodb",
    host: "localhost",
  } as ConnectionConfig);
}

function allDrivers() {
  return [
    mongo(),
    new RedisDriver({
      id: "r",
      name: "r",
      type: "redis",
      host: "h",
    } as ConnectionConfig),
    new ElasticsearchDriver({
      id: "e",
      name: "e",
      type: "elasticsearch",
      host: "h",
    } as ConnectionConfig),
    new DynamoDBDriver({
      id: "d",
      name: "d",
      type: "dynamodb",
      awsRegion: "us-east-1",
    } as ConnectionConfig),
  ];
}

describe("stage 5: nosql transaction + preview", () => {
  it("mongo preview uses updateOne (matches execution)", () => {
    const driver = mongo();
    const preview = driver.buildMutationPreviewStatement?.(
      "update",
      "mydb",
      "",
      "users",
      {
        primaryKeys: { _id: "abc" },
        changes: { name: "Bob" },
      },
    );
    expect(preview).toContain("updateOne(");
    expect(preview).not.toContain("updateMany(");
  });

  it("nosql runTransaction rejects multi-op before any query (no partial apply)", async () => {
    for (const driver of allDrivers()) {
      const querySpy = vi.spyOn(driver, "query").mockResolvedValue({
        columns: [],
        rows: [],
        rowCount: 0,
        executionTimeMs: 0,
      });
      await expect(
        driver.runTransaction([
          { sql: "op1", params: [] },
          { sql: "op2", params: [] },
        ]),
      ).rejects.toThrow("does not support atomic multi-operation");
      expect(querySpy).not.toHaveBeenCalled();
    }
  });

  it("nosql runTransaction executes single-op via query", async () => {
    for (const driver of allDrivers()) {
      const querySpy = vi.spyOn(driver, "query").mockResolvedValue({
        columns: [],
        rows: [],
        rowCount: 0,
        executionTimeMs: 0,
      });
      const sql = driver instanceof MongoDBDriver ? "db.x.find({})" : "op1";
      await driver.runTransaction([{ sql, params: ["a"] }]);
      expect(querySpy).toHaveBeenCalledTimes(1);
      expect(querySpy).toHaveBeenCalledWith(sql, ["a"]);
    }
  });

  it("nosql runTransaction treats empty as no-op", async () => {
    for (const driver of allDrivers()) {
      const querySpy = vi.spyOn(driver, "query").mockResolvedValue({
        columns: [],
        rows: [],
        rowCount: 0,
        executionTimeMs: 0,
      });
      await driver.runTransaction([]);
      expect(querySpy).not.toHaveBeenCalled();
    }
  });

  it("mongo rejects multiple statements inside one transaction item before execution", async () => {
    const driver = mongo();
    const querySpy = vi.spyOn(driver, "query");
    await expect(
      driver.runTransaction([
        {
          sql: "db.x.insertOne({_id:1}); db.x.insertOne({_id:1})",
          params: [],
        },
      ]),
    ).rejects.toThrow("does not support atomic multi-operation");
    expect(querySpy).not.toHaveBeenCalled();
  });
});
