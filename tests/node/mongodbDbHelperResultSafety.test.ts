import { createRequire, isBuiltin } from "node:module";
import { fileURLToPath } from "node:url";
import vm from "node:vm";
import { build } from "esbuild";
import {
  Binary,
  BSONRegExp,
  BSONSymbol,
  Code,
  DBRef,
  Decimal128,
  Double,
  Int32,
  Long,
  MaxKey,
  MinKey,
  ObjectId,
  Timestamp,
} from "mongodb";
import { describe, expect, it, vi } from "vitest";
import { MongoDBDriver } from "../../src/extension/dbDrivers/mongodb";

function harness() {
  const calls: Array<{ method: string; args: unknown[] }> = [];
  const record = <T>(method: string, result: T) =>
    vi.fn((...args: unknown[]) => {
      calls.push({ method, args });
      return result;
    });
  const cursorResult = {
    toArray: record("toArray", Promise.resolve([{ _id: "one" }])),
  };
  const cursor = Object.assign(cursorResult, {
    sort: record("sort", cursorResult),
    skip: record("skip", cursorResult),
    limit: record("limit", cursorResult),
  });
  const collection = {
    find: record("find", cursor),
    countDocuments: record("countDocuments", Promise.resolve(1)),
    aggregate: record("aggregate", cursor),
    insertOne: record(
      "insertOne",
      Promise.resolve({ acknowledged: true, insertedId: "new" }),
    ),
    insertMany: record(
      "insertMany",
      Promise.resolve({ acknowledged: true, insertedCount: 1 }),
    ),
    updateOne: record("updateOne", Promise.resolve({ modifiedCount: 1 })),
    updateMany: record("updateMany", Promise.resolve({ modifiedCount: 1 })),
    deleteOne: record("deleteOne", Promise.resolve({ deletedCount: 1 })),
    deleteMany: record("deleteMany", Promise.resolve({ deletedCount: 1 })),
    createIndex: record("createIndex", Promise.resolve("by_name")),
  };
  const db = {
    collection: record("collection", collection),
    listCollections: record("listCollections", {
      toArray: record(
        "listCollections.toArray",
        Promise.resolve([{ name: "users" }]),
      ),
    }),
    createCollection: record("createCollection", Promise.resolve({})),
    command: record("command", Promise.resolve({ ok: 1 })),
  };
  const driver = new MongoDBDriver({
    id: "db-helper-result-safety",
    name: "db-helper-result-safety",
    type: "mongodb",
    database: "app",
  });
  Object.assign(driver, {
    connected: true,
    client: { db: record("db", db) },
  });
  return { driver, calls, db, collection, cursor };
}

const helpers = [
  "db.getCollectionNames()",
  "db.createCollection('copy')",
  "db.createView('view', 'users', [])",
  "db.getSiblingDB('other').getCollectionNames()",
  "db.getSiblingDB('other').createCollection('copy')",
  "db.getSiblingDB('other').createView('view', 'users', [])",
];

const otherOperations = [
  "db.runCommand({ping: 1})",
  "db.adminCommand({ping: 1})",
  "db.getSiblingDB('other').adminCommand({ping: 1})",
  "db.users.find({}).sort({_id: 1}).skip(0).limit(1).toArray().pretty()",
  "db.users.findOne({})",
  "db.users.countDocuments({})",
  "db.users.aggregate([]).skip(0).limit(1).toArray()",
  "db.users.insertOne({_id: 'new'})",
  "db.users.insertMany([{_id: 'new'}])",
  "db.users.updateOne({}, {$set: {active: true}})",
  "db.users.updateMany({}, {$set: {active: true}})",
  "db.users.deleteOne({})",
  "db.users.deleteMany({})",
  "db.users.createIndex({name: 1})",
  "db.users.drop()",
  "db.getCollection('users').find({})",
];

describe("MongoDB DB helper results fail closed before every SDK call", () => {
  it.each([
    "db.users.deleteMany(!!db.getCollectionNames() ? {} : {_id: 'none'})",
    "db.users.deleteMany(Object.assign({}, Object.create(db.getCollectionNames())))",
    "const saved = db.getCollectionNames(); db.users.deleteMany(saved ? {} : {_id: 'none'})",
    "const saved = db.createCollection('copy'); db.users.deleteMany(Boolean(saved) ? {} : {_id: 'none'})",
    "const saved = db.createView('view', 'users', []); db.users.deleteMany(!!saved ? {} : {_id: 'none'})",
    "const saved = db.users.findOne({}); db.users.deleteMany(saved === saved ? {} : {_id: 'none'})",
  ])("rejects remaining dataflow bypasses, including an earlier write: %s", async (query) => {
    const h = harness();
    const error = await h.driver
      .query(`db.users.insertOne({_id: 'earlier'}); ${query}`)
      .then(
        () => undefined,
        (failure: unknown) => failure,
      );
    expect(h.calls).toEqual([]);
    expect(String(error)).toMatch(/Unsupported mongosh JavaScript/);
  });

  const uses = [
    ["direct", (result: string) => `db.users.insertOne(${result})`],
    ["nested", (result: string) => `db.users.insertOne({copy: ${result}})`],
    ["array", (result: string) => `db.users.insertMany([${result}])`],
    ["deep", (result: string) => `db.users.insertOne({a: [{b: ${result}}]})`],
    [
      "truthiness",
      (result: string) => `db.users.insertOne({copy: !!${result}})`,
    ],
    [
      "conditional",
      (result: string) => `db.users.deleteMany(${result} ? {} : {_id: 'none'})`,
    ],
    [
      "Boolean conditional",
      (result: string) =>
        `db.users.deleteMany(Boolean(${result}) ? {} : {_id: 'none'})`,
    ],
    [
      "transient inherited copy",
      (result: string) =>
        `db.users.deleteMany(Object.assign({}, Object.create(${result})))`,
    ],
    [
      "saved conditional",
      (result: string) =>
        `const saved = ${result}; db.users.deleteMany(saved ? {} : {_id: 'none'})`,
    ],
    ["spread", (result: string) => `db.users.deleteMany({...${result}})`],
    [
      "assign",
      (result: string) => `db.users.deleteMany(Object.assign({}, ${result}))`,
    ],
    [
      "assign target",
      (result: string) => `db.users.deleteMany(Object.assign(${result}, {}))`,
    ],
    [
      "reflective copy",
      (result: string) =>
        `db.users.deleteMany(Object.fromEntries(Object.entries(${result})))`,
    ],
    [
      "descriptors",
      (result: string) =>
        `db.users.deleteMany(Object.getOwnPropertyDescriptors(${result}))`,
    ],
    [
      "getter",
      (result: string) =>
        `db.users.insertOne({get copy() { return ${result}; }})`,
    ],
    [
      "prototype",
      (result: string) => `db.users.deleteMany(Object.create(${result}))`,
    ],
    [
      "deep prototype",
      (result: string) =>
        `db.users.deleteMany(Object.create(Object.create(${result})))`,
    ],
    [
      "method spread",
      (result: string) => `db.users.deleteMany({...${result}.missing})`,
    ],
    [
      "method assign",
      (result: string) =>
        `db.users.deleteMany(Object.assign({}, ${result}.missing))`,
    ],
    [
      "method prototype",
      (result: string) =>
        `db.users.deleteMany(Object.create(${result}.missing))`,
    ],
    [
      "property descriptor",
      (result: string) =>
        `db.users.insertOne({copy: Object.getOwnPropertyDescriptor(${result}, 'missing')})`,
    ],
    [
      "get prototype",
      (result: string) =>
        `db.users.insertOne({copy: Object.getPrototypeOf(${result})})`,
    ],
    [
      "membership",
      (result: string) => `db.users.insertOne({copy: 'missing' in ${result}})`,
    ],
    [
      "mutation",
      (result: string) =>
        `const result = ${result}; result.filter = {}; db.users.deleteMany(result.filter)`,
    ],
    ["array spread", (result: string) => `db.users.insertMany([...${result}])`],
    [
      "string coercion",
      (result: string) => `db.users.insertOne({copy: String(${result})})`,
    ],
    [
      "variable",
      (result: string) =>
        `const result = ${result}; db.users.deleteMany(result)`,
    ],
    [
      "command",
      (result: string) => `db.runCommand({delete: 'users', filter: ${result}})`,
    ],
    [
      "marker",
      (result: string) => `db.users.insertOne({id: ObjectId(${result})})`,
    ],
    ["chain argument", (result: string) => `db.users.find({}).sort(${result})`],
  ] as const;
  for (const helper of [...helpers, ...otherOperations]) {
    it.each(uses)(`${helper}: rejects %s`, async (_name, query) => {
      const h = harness();
      const error = await h.driver
        .query(`db.users.insertOne({_id: 'earlier'}); ${query(helper)}`)
        .then(
          () => undefined,
          (failure: unknown) => failure,
        );
      expect(h.calls, "not even client.db() may run before rejection").toEqual(
        [],
      );
      expect(error).toBeInstanceOf(Error);
      expect(String(error)).toMatch(
        /operation results.*cannot be used as values/i,
      );
    });
  }

  it.each(helpers)("rejects unsupported helper chains: %s", async (helper) => {
    const h = harness();
    await expect(
      h.driver.query(
        `db.users.insertOne({_id: 'earlier'}); ${helper}.limit(1)`,
      ),
    ).rejects.toThrow();
    expect(h.calls).toEqual([]);
  });

  it.each([
    "db.getCollectionNames().toArray()",
    "db.createCollection('copy').pretty()",
    "db.createView('view', 'users', []).sort({_id: 1})",
    "db.runCommand({ping: 1}).limit(1)",
    "db.adminCommand({ping: 1}).toArray()",
    "db.users.find({}).unknown()",
    "db.users.find({}).limit(-1)",
    "db.users.aggregate([]).skip(-1)",
  ])("preflights invalid/unsupported chains before earlier writes: %s", async (query) => {
    const h = harness();
    await expect(
      h.driver.query(`db.users.insertOne({_id: 'earlier'}); ${query}`),
    ).rejects.toThrow();
    expect(h.calls).toEqual([]);
  });

  it.each([
    "db.getSiblingDB(db.getCollectionNames()).users.deleteMany({})",
    "db.getCollection(db.createCollection('copy')).deleteMany({})",
    "db.users[db.getCollectionNames()]({})",
    "db.users.deleteMany({[db.getCollectionNames()]: true})",
    "db.users.deleteMany((db.getCollectionNames(), {}))",
    "db.users.deleteMany(db.getCollectionNames() && {})",
    "db.users.deleteMany(db.getCollectionNames() || {})",
    "db.users.deleteMany(db.getCollectionNames() ?? {})",
    "db.users.deleteMany(({copy: db.getCollectionNames()}).copy)",
    "db.users.deleteMany((function() { return db.getCollectionNames() ? {} : {}; })())",
    "db.users.insertOne(Object.assign({}, {safe: true}))",
    "const filter = {}; db.users.find(filter)",
    "({text: 'safe'})",
    "db.users['delete' + 'Many']({})",
    "db.users.find?.({})",
    "db?.users.find({})",
    "db.users.find({}) as any",
    "db.users.find<string>({})",
  ])("rejects unsupported syntax throughout namespace, method and data expressions: %s", async (query) => {
    const h = harness();
    await expect(
      h.driver.query(`db.users.insertOne({_id: 'earlier'}); ${query}`),
    ).rejects.toThrow(/Unsupported mongosh JavaScript/);
    expect(h.calls).toEqual([]);
  });

  it("retains getCollectionNames on read-only connections", async () => {
    const h = harness();
    const result = await h.driver.query("db.getCollectionNames()", undefined, {
      readOnly: true,
    });
    expect(result.columns).toEqual(["name"]);
    expect(result.rows).toEqual([{ __col_0: "users" }]);
    expect(h.db.listCollections).toHaveBeenCalledOnce();
  });

  it("preserves standalone helpers, explicit documents and database routing", async () => {
    const h = harness();
    const result = await h.driver.query(
      "db.getCollectionNames(); db.createCollection('copy', {capped: true, size: 100}); " +
        "db.getSiblingDB('other').createView('active', 'users', [{$match: {active: true}}]); " +
        "db.users.insertOne({_id: 'new'}); db.users.deleteMany({active: false})",
    );
    expect(result.rows.length).toBeGreaterThan(0);
    expect(h.db.listCollections).toHaveBeenCalledWith(
      {},
      { authorizedCollections: true, nameOnly: true },
    );
    expect(h.db.createCollection).toHaveBeenNthCalledWith(1, "copy", {
      capped: true,
      size: 100,
    });
    expect(h.db.createCollection).toHaveBeenNthCalledWith(2, "active", {
      viewOn: "users",
      pipeline: [{ $match: { active: true } }],
    });
    expect(
      h.calls.filter((call) => call.method === "db").map((call) => call.args),
    ).toEqual([["app"], ["app"], ["other"], ["app"], ["app"]]);
    expect(h.collection.deleteMany).toHaveBeenCalledWith({ active: false });
  });

  it("preserves commands and supported read chains", async () => {
    const h = harness();
    await h.driver.query(
      "db.runCommand({ping: 1}); db.getSiblingDB('other').adminCommand({ping: 1}); " +
        "db.users.find({active: true}).sort({_id: 1}).skip(2).limit(3).toArray().pretty(); " +
        "db.users.aggregate([{$match: {active: true}}]).skip(1).limit(2).toArray()",
    );
    expect(h.db.command).toHaveBeenCalledTimes(2);
    expect(
      h.calls.filter((call) => call.method === "db").map((call) => call.args),
    ).toEqual([["app"], ["admin"], ["app"], ["app"]]);
    expect(h.cursor.sort).toHaveBeenCalledWith({ _id: 1 });
    expect(h.cursor.skip).toHaveBeenCalledWith(2);
    expect(h.cursor.limit).toHaveBeenCalledWith(3);
    expect(h.collection.aggregate).toHaveBeenCalledWith(
      [
        { $match: { active: true } },
        { $skip: 1 },
        { $limit: 2 },
        { $limit: 2 },
      ],
      expect.anything(),
    );
  });

  it.each([
    ["new Date(0)", expect.any(Date)],
    ["ISODate('2024-01-01T00:00:00Z')", expect.any(Date)],
    ["ObjectId('507f1f77bcf86cd799439011')", expect.any(ObjectId)],
    ["BinData(0, 'AQ==')", expect.any(Binary)],
    [
      "DBRef('users', ObjectId('507f1f77bcf86cd799439011'), 'app')",
      expect.any(DBRef),
    ],
    ["BSONSymbol('label')", expect.any(BSONSymbol)],
    ["NumberLong('9007199254740993')", expect.any(Long)],
    ["new NumberInt(-7)", expect.any(Int32)],
    ["NumberDecimal('0.1')", expect.any(Decimal128)],
    ["Timestamp({t: 42, i: 1})", expect.any(Timestamp)],
    ["Code('function() { return x; }', {x: 1})", expect.any(Code)],
    ["MinKey()", expect.any(MinKey)],
    ["new MaxKey()", expect.any(MaxKey)],
    ["Double('-0')", expect.any(Double)],
    ["new RegExp('literal', 'i')", expect.any(BSONRegExp)],
    ["BSONRegExp('literal', 'i')", expect.any(BSONRegExp)],
    ["/db.getCollectionNames\\(\\); process/i", expect.any(BSONRegExp)],
    ["9007199254740993n", 9007199254740993n],
    ["1 + 2 * (8 / 4)", 5],
    ["!!true", true],
    [`\`first\n// literal text \${1 + 2}\``, "first\n// literal text 3"],
    ["[undefined, , null, false, -7]", [undefined, undefined, null, false, -7]],
    [
      "{['static-key']: 'text', process: 'eval', import: 'constructor'}",
      { "static-key": "text", process: "eval", import: "constructor" },
    ],
  ])("preserves safe literal/BSON data: %s", async (literal, expected) => {
    const h = harness();
    await h.driver.query(`((db['users']['insertOne']({value: ${literal}})));`);
    expect(h.collection.insertOne).toHaveBeenCalledWith({ value: expected });
  });

  it.each([
    "db.users.deleteMany(!!db.getCollectionNames() ? {} : {_id: 'none'})",
    "db.users.deleteMany(Object.assign({}, Object.create(db.getCollectionNames())))",
    "const saved = db.getCollectionNames(); db.users.deleteMany(saved ? {} : {_id: 'none'})",
  ])("shares the fail-closed boundary with readonly parsing and transaction planning: %s", async (query) => {
    const h = harness();
    const guard = h.driver.getCapabilities().readOnlyQueryGuard;
    expect(guard?.(query)).toMatchObject({
      allowed: false,
      reason: expect.stringMatching(/Unsupported mongosh JavaScript/),
    });
    await expect(h.driver.runTransaction([{ sql: query }])).rejects.toThrow(
      /Unsupported mongosh JavaScript/,
    );
    await expect(
      h.driver.runTransaction([
        { sql: `db.users.insertOne({_id: 'earlier'}); ${query}` },
      ]),
    ).rejects.toThrow(/Unsupported mongosh JavaScript/);
    expect(h.calls).toEqual([]);
  });

  it("keeps literal readonly parsing and generated BSON transaction previews working", async () => {
    const h = harness();
    const guard = h.driver.getCapabilities().readOnlyQueryGuard;
    const query =
      "db.getSiblingDB('other').getCollection('users').find({name: /literal/i, process: 'import'}).sort({_id: 1}).limit(2)";
    expect(guard?.(query)).toEqual({ allowed: true });
    expect(h.calls).toEqual([]);
    await h.driver.query(query, undefined, { readOnly: true });
    expect(h.collection.find).toHaveBeenCalledOnce();
    const values = {
      _id: new ObjectId("507f1f77bcf86cd799439011"),
      quantity: Long.fromString("9007199254740993"),
    };
    const sql = h.driver.buildMutationPreviewStatement(
      "insert",
      "other",
      "",
      "users",
      { values },
    );
    await h.driver.runTransaction([{ sql }]);
    expect(h.collection.insertOne).toHaveBeenCalledWith(values);
  });

  it.each([
    `db.users.insertOne({value: '${"x".repeat(1_000_000)}'})`,
    `db.users.insertOne({value: ${"[".repeat(140)}0${"]".repeat(140)}})`,
    "db.users.find({value: })",
    "db.users.find({}); const data = {}",
    "db.users.find({value: {...{safe: true}}})",
  ])("rejects unsupported or oversized input before any SDK call (#%#)", async (query) => {
    const h = harness();
    await expect(h.driver.query(query)).rejects.toThrow(
      /Unsupported mongosh JavaScript/,
    );
    expect(h.calls).toEqual([]);
  });

  it("bundles the parser without requiring production TypeScript/package resolution", async () => {
    const bundled = await build({
      entryPoints: [
        fileURLToPath(
          new URL(
            "../../src/extension/dbDrivers/mongoshSyntax.ts",
            import.meta.url,
          ),
        ),
      ],
      bundle: true,
      minify: true,
      platform: "node",
      target: "node20",
      format: "cjs",
      write: false,
      metafile: true,
    });
    expect(
      Object.keys(bundled.metafile.inputs).some((path) =>
        path.endsWith("/typescript/lib/typescript.js"),
      ),
    ).toBe(true);
    const runtime = { exports: {} as Record<string, unknown> };
    const nativeRequire = createRequire(import.meta.url);
    vm.runInNewContext(
      bundled.outputFiles[0].text,
      {
        module: runtime,
        exports: runtime.exports,
        process,
        Buffer,
        setTimeout,
        clearTimeout,
        setImmediate,
        clearImmediate,
        __filename: "/virtual/mongosh-syntax.cjs",
        __dirname: "/virtual",
        require: (name: string) => {
          if (!isBuiltin(name))
            throw new Error(`Production package resolution forbidden: ${name}`);
          return nativeRequire(name);
        },
      },
      { timeout: 10_000 },
    );
    const validate = runtime.exports.assertMongoshSupportedSyntax;
    if (typeof validate !== "function")
      throw new Error("Missing bundled syntax validator");
    expect(() =>
      validate(
        "db.users.find({id: ObjectId('507f1f77bcf86cd799439011')}).limit(2)",
      ),
    ).not.toThrow();
    expect(() =>
      validate("db.users.deleteMany(!!db.getCollectionNames() ? {} : {})"),
    ).toThrow(/Unsupported mongosh JavaScript/);
  }, 15_000);
});
