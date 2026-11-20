import { randomUUID } from "node:crypto";
import { Int32, Long, MongoClient } from "mongodb";
import { describe, expect, it, vi } from "vitest";
import { MongoDBDriver } from "../../src/extension/dbDrivers/mongodb";
import { QUERY_LIMIT_POLICY } from "../../src/shared/safetyContracts";
import { startMongoWireFixture } from "../support/mongodbWireFixture";

function harness() {
  const deleteMany = vi.fn().mockResolvedValue({ deletedCount: 1 });
  const find = vi.fn();
  const aggregate = vi.fn();
  const command = vi.fn();
  const driver = new MongoDBDriver({
    id: "preflight",
    name: "Preflight",
    type: "mongodb",
  });
  Object.assign(driver, {
    connected: true,
    client: {
      db: () => ({
        command,
        collection: () => ({ deleteMany, find, aggregate }),
      }),
    },
  });
  return { driver, deleteMany, find, aggregate, command };
}

describe("MongoDB validates the whole script before destructive statements", () => {
  it.each([
    ["db.items.find({}).limit(-1)", /limit/],
    ["db.items.find({}).limit(1.5)", /limit/],
    ["db.items.find({}).limit()", /limit/],
    ["db.items.aggregate([]).limit(0)", /positive/],
    ["db.items.find({}, 42)", /projection/],
    ["db.items.find({}).sort(42)", /sort/],
    ["db.items.aggregate({})", /pipeline/],
    ["db.items.drop(true)", /arguments/],
    ["db.getCollectionNames(42)", /arguments/],
    ["db.items.unsupportedOperation()", /Unsupported/],
  ])("rejects a later local validation error: %s", async (query, reason) => {
    const { driver, deleteMany, find, aggregate } = harness();
    await expect(
      driver.query(`db.items.deleteMany({}); ${query}`),
    ).rejects.toThrow(reason);
    expect(deleteMany).not.toHaveBeenCalled();
    expect(find).not.toHaveBeenCalled();
    expect(aggregate).not.toHaveBeenCalled();
  });

  it("keeps valid scripts sequential and does not claim server errors roll back earlier writes", async () => {
    const { driver, deleteMany, find } = harness();
    const cursor = {
      skip: vi.fn().mockReturnThis(),
      limit: vi.fn().mockReturnThis(),
      toArray: vi.fn(async () => [{ _id: "remaining" }]),
    };
    find.mockImplementation(() => {
      expect(deleteMany).toHaveBeenCalledOnce();
      return cursor;
    });
    const result = await driver.query(
      'db.items.deleteMany({_id:"remove"}); db.items.find({}).limit(1)',
    );
    expect(result.affectedRows).toBe(1);
    expect(cursor.limit).toHaveBeenCalledWith(1);
    find.mockImplementation(() => {
      throw new Error("Server unavailable");
    });
    const partial = await driver.query(
      'db.items.deleteMany({_id:"remove"}); db.items.find({})',
    );
    expect(partial.affectedRows).toBe(1);
    expect(partial.error).toBe(
      "MongoDB statement 2 of 2 failed: Server unavailable. Earlier statements completed; later statements were not run.",
    );
    expect(deleteMany).toHaveBeenCalledTimes(2);
  });

  it.each([
    [{ writeErrors: [{ index: 1, code: 121 }] }, /1 write error/],
    [{ writeConcernError: { code: 64 } }, /write concern error/],
    [{ nErrors: new Int32(2) }, /2 write error/],
  ] as const)("does not continue after an embedded server write failure (%j)", async (failure, reason) => {
    const { driver, command, deleteMany } = harness();
    command.mockResolvedValue({ ok: 1, n: 1, ...failure });
    const single = await driver.query(
      'db.runCommand({insert: "archive", documents: [{id: 1}]})',
    );
    expect(single.error).toMatch(reason);
    expect(single).toMatchObject({
      rowCount: 1,
      columns: expect.arrayContaining(Object.keys(failure)),
    });

    const script = await driver.query(
      'db.runCommand({insert: "archive", documents: [{id: 1}]}); db.items.deleteMany({id: 1})',
    );
    expect(script.error).toMatch(reason);
    expect(script.error).toContain("Some writes may already have been applied");
    expect(script.error).toContain("Later statements were not run");
    expect(script).toMatchObject({
      rowCount: 1,
      columns: expect.arrayContaining(Object.keys(failure)),
    });
    expect(deleteMany).not.toHaveBeenCalled();
  });

  it("retains earlier confirmed writes and stops at a failed command reply", async () => {
    const { driver, command, deleteMany } = harness();
    command.mockResolvedValue({ ok: 1, n: 0, writeErrors: [{ code: 121 }] });
    const result = await driver.query(
      'db.items.deleteMany({expired: true}); db.runCommand({insert: "archive", documents: [{id: 1}]}); db.items.deleteMany({id: 1})',
    );
    expect(result).toMatchObject({
      affectedRows: 1,
      error: expect.stringContaining("statement 2 of 3 failed"),
    });
    expect(deleteMany).toHaveBeenCalledExactlyOnceWith({ expired: true });
  });

  it("continues after a successful raw command with no write errors", async () => {
    const { driver, command, deleteMany } = harness();
    command.mockResolvedValue({ ok: 1, n: 1, writeErrors: [] });
    const result = await driver.query(
      'db.runCommand({insert: "archive", documents: [{id: 1}]}); db.items.deleteMany({id: 1})',
    );
    expect(result.error).toBeUndefined();
    expect(deleteMany).toHaveBeenCalledOnce();
  });

  it("keeps diagnostic details when retained results exhaust the row limit", async () => {
    const { driver, command, find, deleteMany } = harness();
    find.mockReturnValue({
      skip: vi.fn().mockReturnThis(),
      limit: vi.fn().mockReturnThis(),
      toArray: async () =>
        Array.from({ length: QUERY_LIMIT_POLICY.hardCap }, (_, i) => ({
          id: i,
        })),
    });
    command.mockResolvedValue({ ok: 1, n: 1, writeErrors: [{ code: 121 }] });
    const result = await driver.query(
      'db.items.find({}).limit(0); db.runCommand({insert: "archive", documents: [{id: 1}]}); db.items.deleteMany({id: 1})',
    );
    expect(result.rows).toHaveLength(QUERY_LIMIT_POLICY.hardCap);
    expect(result.columns).not.toContain("writeErrors");
    expect(result.error).toMatch(
      /code 121.*n=1.*Later statements were not run/,
    );
    expect(deleteMany).not.toHaveBeenCalled();
  });

  it.each([
    [
      { writeConcernError: { code: 64, errmsg: "write concern failed" } },
      "write concern error.*code 64",
    ],
    [
      { writeConcernError: { code: 50, errmsg: "write concern timed out" } },
      "write concern error.*code 50",
    ],
    [
      { writeErrors: [{ code: 50, index: 1, errmsg: "write timed out" }] },
      "write error.*code 50",
    ],
  ])("preserves embedded write failures thrown by the real MongoDB SDK (%j)", async (failure, reason) => {
    const followUp = vi.fn();
    const fixture = await startMongoWireFixture((command) => {
      if (command.listCollections) {
        return {
          ok: 1,
          cursor: {
            id: Long.ZERO,
            ns: "test.$cmd.listCollections",
            firstBatch: [],
          },
        };
      }
      if (command.insert === "archive") {
        return { ok: 1, n: 1, ...failure };
      }
      if (command.insert === "followUp") followUp();
      return { ok: 1, n: 1 };
    });
    const driver = new MongoDBDriver({
      id: "mongo-write-concern",
      name: "MongoDB",
      type: "mongodb",
      connectionUri: `mongodb://127.0.0.1:${fixture.port}/test`,
    });
    try {
      await driver.connect();
      const result = await driver.query(
        'db.runCommand({insert: "archive", documents: [{id: 1}]}); db.followUp.insertOne({ran: true})',
        undefined,
        { deadline: Date.now() + 10_000 },
      );
      expect(result).toMatchObject({
        error: expect.stringMatching(
          new RegExp(
            `${reason}.*n=1.*Some writes may already have been applied.*Later statements were not run`,
          ),
        ),
        rowCount: 1,
        columns: expect.arrayContaining(Object.keys(failure)),
      });
      expect(result.exportRows).toBeUndefined();
      expect(followUp).not.toHaveBeenCalled();
    } finally {
      await driver.disconnect();
      await fixture.close();
    }
  });
});

it.skipIf(!process.env.RAPIDB_MONGO_PREFLIGHT_AUDIT_URL)(
  "stops a script when a real MongoDB write reply reports a validation error",
  async () => {
    const uri = process.env.RAPIDB_MONGO_PREFLIGHT_AUDIT_URL;
    const database = `rapidb_preflight_${randomUUID()}`;
    const client = new MongoClient(uri ?? "");
    const driver = new MongoDBDriver({
      id: database,
      name: database,
      type: "mongodb",
      connectionUri: uri,
      database,
    });
    try {
      await client.connect();
      await driver.connect();
      const db = client.db(database);
      await db.createCollection("archive", {
        validator: { archived: true },
      });
      const result = await driver.query(
        "db.runCommand({insert: 'archive', documents: [{archived: true}, {archived: false}]}); db.followUp.insertOne({ran: true})",
      );
      expect(result.error).toMatch(
        /1 write error.*Later statements were not run/,
      );
      expect(await db.collection("archive").countDocuments()).toBe(1);
      expect(await db.collection("followUp").countDocuments()).toBe(0);
    } finally {
      await driver.disconnect();
      await client.db(database).dropDatabase();
      await client.close();
    }
  },
);

it.skipIf(!process.env.RAPIDB_MONGO_PREFLIGHT_AUDIT_URL)(
  "preserves real MongoDB documents when a later cursor limit is invalid",
  async () => {
    const uri = process.env.RAPIDB_MONGO_PREFLIGHT_AUDIT_URL;
    const database = `rapidb_preflight_${randomUUID()}`;
    const client = new MongoClient(uri ?? "");
    const driver = new MongoDBDriver({
      id: database,
      name: database,
      type: "mongodb",
      connectionUri: uri,
      database,
    });
    try {
      await client.connect();
      await driver.connect();
      await client.db(database).collection("items").insertOne({ keep: true });
      await expect(
        driver.query("db.items.deleteMany({}); db.items.find({}).limit(-1)"),
      ).rejects.toThrow(/limit/);
      expect(
        await client
          .db(database)
          .collection("items")
          .countDocuments({ keep: true }),
      ).toBe(1);
    } finally {
      await driver.disconnect();
      await client.db(database).dropDatabase();
      await client.close();
    }
  },
);
