import { randomUUID } from "node:crypto";
import { Int32, Long, MongoClient } from "mongodb";
import { createClient } from "redis";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { MongoDBDriver } from "../../src/extension/dbDrivers/mongodb";
import { RedisDriver } from "../../src/extension/dbDrivers/redis";
import type { ColumnTypeMeta } from "../../src/extension/dbDrivers/types";
import { applyChangesTransactional } from "../../src/extension/table/tableMutationExecution";
import { logger } from "../../src/extension/utils/logger";

// Also run by test:db:nosql. Overrides allow reproductions on isolated services.
const live = process.env.RAPIDB_LIVE_NOSQL === "1";
const redisUri =
  process.env.RAPIDB_DATA_SAFETY_REDIS_URI ??
  "redis://:redis_pass123@127.0.0.1:6379/0";
const mongoUri =
  process.env.RAPIDB_DATA_SAFETY_MONGO_URI ??
  "mongodb://mongo_admin:mongo_pass123@127.0.0.1:27017/rapidb_mongo_db?authSource=admin&directConnection=true";

describe.runIf(live)("Redis edit failure data safety (live Lua)", () => {
  let admin: ReturnType<typeof createClient>;
  let drivers: RedisDriver[];
  let prefix: string;
  let key: string;
  let users: string[];
  beforeEach(async () => {
    prefix = `rapidb_safety:${randomUUID()}:`;
    key = `${prefix}source`;
    drivers = [];
    users = [];
    admin = createClient({ url: redisUri });
    admin.on("error", () => undefined);
    await admin.connect();
  });
  afterEach(async () => {
    // The driver logs expected socket AbortErrors while disconnecting.
    vi.spyOn(logger, "error").mockReturnValue(new Error("teardown"));
    await Promise.allSettled(drivers.map((driver) => driver.disconnect()));
    if (admin?.isOpen) {
      const keys = await admin.keys(`${prefix}*`);
      if (keys.length) await admin.del(keys);
      for (const user of users)
        await admin.sendCommand(["ACL", "DELUSER", user]);
      await admin.close();
    }
  });
  async function makeDriver(deniedCommand?: string, selectors?: string[]) {
    const url = new URL(redisUri);
    if (deniedCommand) {
      const user = `rapidb_safety_${randomUUID()}`;
      users.push(user);
      await admin.sendCommand([
        "ACL",
        "SETUSER",
        user,
        "reset",
        "on",
        ">safety-pass",
        `~${prefix}*`,
        "+@all",
        `-${deniedCommand}`,
        ...(selectors ?? []),
      ]);
      url.username = user;
      url.password = "safety-pass";
    }
    const driver = new RedisDriver({
      id: "redis-safety",
      name: "Redis safety",
      type: "redis",
      connectionUri: url.toString(),
    });
    drivers.push(driver);
    await driver.connect();
    return driver;
  }
  async function seed(kind: string) {
    if (kind === "string") await admin.set(key, "original");
    else if (kind === "hash") await admin.hSet(key, { field: "original" });
    else if (kind === "list") await admin.rPush(key, ["original"]);
    else if (kind === "set") await admin.sAdd(key, ["original"]);
    else await admin.zAdd(key, [{ score: 1, value: "original" }]);
    await admin.pExpire(key, 120000);
    return admin.sendCommand(["DUMP", key]);
  }
  function update(
    driver: RedisDriver,
    changes: Record<string, unknown>,
    originalValues?: Record<string, unknown>,
  ) {
    return driver.updateRows({
      database: "db0",
      schema: "",
      table: prefix,
      updates: [{ primaryKeys: { key }, changes, originalValues }],
    });
  }
  async function expectPreserved(original: unknown) {
    expect(await admin.sendCommand(["DUMP", key])).toEqual(original);
    expect(await admin.pTTL(key)).toBeGreaterThan(100000);
    expect(await admin.pTTL(key)).toBeLessThanOrEqual(120000);
    expect(await admin.keys(`${prefix}*`)).toEqual([key]);
  }

  it.each([
    ["string", "\ud800"],
    ["hash", '{"field":"\\ud800"}'],
    ["hash", '{"\\ud800":"value"}'],
    ["list", '["ok","\\ud800"]'],
    ["set", '["ok","\\ud800"]'],
    ["zset", '[{"score":1,"value":"\\ud800"}]'],
  ])("preserves %s and TTL on invalid Unicode %j, including rename", async (kind, value) => {
    const original = await seed(kind);
    const driver = await makeDriver();
    await expect(
      update(driver, { key: `${prefix}target`, value }),
    ).rejects.toThrow(/unicode/i);
    await expectPreserved(original);
  });

  it.each([
    ["string", "set", "next"],
    ["hash", "hset", '{"field":"next"}'],
    ["list", "rpush", '["next"]'],
    ["set", "sadd", '["next"]'],
    ["zset", "zadd", '[{"score":2,"value":"next"}]'],
    ["hash", "pexpire", '{"field":"next"}'],
    ["hash", "rename", '{"field":"next"}'],
    ["hash", "del", '{"field":"next"}'],
  ])("preserves %s when %s is denied and cleans staging keys", async (kind, command, value) => {
    const original = await seed(kind);
    const driver = await makeDriver(command);
    await expect(
      update(driver, { key: `${prefix}target`, value }),
    ).rejects.toThrow(/permission/i);
    await expectPreserved(original);
  });

  it("checks Redis 7 key-specific DEL selectors before committing a rename", async () => {
    const original = await seed("hash");
    const driver = await makeDriver("del", [
      `(+del ~${prefix}*:rapidb-edit:*)`,
    ]);
    await expect(
      update(driver, { key: `${prefix}target`, value: '{"field":"next"}' }),
    ).rejects.toThrow(/permission/i);
    await expectPreserved(original);
  });

  it.each([
    ["string", "next", "GET", "next"],
    ["hash", '{"field":"next"}', "HGETALL", { field: "next" }],
    ["list", '["a","b"]', "LRANGE", ["a", "b"]],
    ["set", '["a"]', "SMEMBERS", ["a"]],
    ["zset", '[{"score":2,"value":"next"}]', "ZRANGE", ["next"]],
  ])("successfully replaces %s and keeps TTL across rename", async (kind, value, readCommand, expected) => {
    await seed(kind);
    const driver = await makeDriver();
    const target = `${prefix}target`;
    expect(await update(driver, { key: target, value })).toMatchObject({
      affectedRows: 1,
    });
    const args =
      readCommand === "LRANGE" || readCommand === "ZRANGE"
        ? [target, "0", "-1"]
        : [target];
    expect(await admin.sendCommand([readCommand, ...args])).toEqual(expected);
    expect(await admin.pTTL(target)).toBeGreaterThan(100000);
    expect(await admin.keys(`${prefix}*`)).toEqual([target]);
  });

  it.each([
    "{}",
    "[]",
  ])("keeps empty-collection deletion semantics for %s", async (value) => {
    await seed(value === "{}" ? "hash" : "list");
    expect(await update(await makeDriver(), { value })).toMatchObject({
      affectedRows: 1,
    });
    expect(await admin.keys(`${prefix}*`)).toEqual([]);
  });

  it("replaces in place, sets/persists TTL and rejects stale values or occupied targets", async () => {
    await seed("hash");
    const driver = await makeDriver();
    await update(driver, { value: '{"field":"next"}', ttl: "60" });
    expect(await admin.hGetAll(key)).toEqual({ field: "next" });
    expect(await admin.pTTL(key)).toBeGreaterThan(50000);
    expect(await admin.pTTL(key)).toBeLessThanOrEqual(60000);
    await expect(
      update(driver, { value: "{}" }, { value: '{"field":"original"}' }),
    ).rejects.toThrow(/changed/);
    await admin.set(`${prefix}target`, "unrelated");
    await expect(
      update(driver, { key: `${prefix}target`, value: "{}" }),
    ).rejects.toThrow(/already exists/);
    expect(await admin.get(`${prefix}target`)).toBe("unrelated");
    expect(await admin.hGetAll(key)).toEqual({ field: "next" });
    await update(driver, { value: '{"field":"persisted"}', ttl: "-1" });
    expect(await admin.pTTL(key)).toBe(-1);
    expect(await admin.keys(`${prefix}*`)).toHaveLength(2);
  });
});

describe.runIf(live)("MongoDB integer mutation data safety (live BSON)", () => {
  let admin: MongoClient;
  let driver: MongoDBDriver;
  let table: string;
  const database = "rapidb_safety";
  const base: ColumnTypeMeta = {
    name: "amount",
    type: "int",
    nativeType: "int",
    category: "integer",
    nullable: true,
    isPrimaryKey: false,
    isForeignKey: false,
    filterable: true,
    filterOperators: [],
    valueSemantics: "plain",
  };
  beforeEach(async () => {
    table = `integers_${randomUUID().replaceAll("-", "")}`;
    admin = new MongoClient(mongoUri, { promoteValues: false });
    await admin.connect();
    driver = new MongoDBDriver({
      id: "mongo-safety",
      name: "Mongo safety",
      type: "mongodb",
      connectionUri: mongoUri,
    });
    await driver.connect();
  });
  afterEach(async () => {
    await driver?.disconnect();
    if (admin) {
      await admin.db(database).collection(table).drop();
      await admin.close();
    }
  });
  it.each([
    ["int", "-2147483648", "2147483647", "2147483648", Int32],
    [
      "long",
      "-9223372036854775808",
      "9223372036854775807",
      "9223372036854775808",
      Long,
    ],
  ] as const)("writes %s boundaries exactly and rejects overflow without changing any row", async (nativeType, min, max, overflow, bsonType) => {
    const collection = admin.db(database).collection(table);
    await collection.insertMany([
      { _id: "one", amount: new bsonType(1) },
      { _id: "two", amount: new bsonType(2) },
    ] as never);
    const config = {
      id: "mongo-safety",
      name: "Mongo safety",
      type: "mongodb",
    } as const;
    const manager = { getDriver: () => driver, getConnection: () => config };
    const columns = [
      {
        ...base,
        name: "_id",
        nativeType: "string",
        category: "text" as const,
        isPrimaryKey: true,
      },
      { ...base, nativeType },
    ];
    const updates = (values: string[]) =>
      values.map((amount, index) => ({
        primaryKeys: {
          _id: {
            $rapidbMongoId: { type: "string", value: index ? "two" : "one" },
          },
        },
        changes: { amount },
      }));
    expect(
      await applyChangesTransactional(
        manager as never,
        config.id,
        database,
        "",
        table,
        updates([min, max]),
        columns,
      ),
    ).toMatchObject({ success: true });
    const saved = await collection.find().sort({ _id: 1 }).toArray();
    expect(saved.map((row) => row.amount.toString())).toEqual([min, max]);
    for (const row of saved) expect(row.amount).toBeInstanceOf(bsonType);
    expect(
      await applyChangesTransactional(
        manager as never,
        config.id,
        database,
        "",
        table,
        updates(["123", overflow]),
        columns,
      ),
    ).toMatchObject({ success: false, failedRows: [1] });
    expect(
      (await collection.find().sort({ _id: 1 }).toArray()).map((row) =>
        row.amount.toString(),
      ),
    ).toEqual([min, max]);
  });
});
