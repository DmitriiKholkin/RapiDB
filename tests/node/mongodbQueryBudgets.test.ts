import { describe, expect, it, vi } from "vitest";
import { MongoDBDriver } from "../../src/extension/dbDrivers/mongodb";
import type { QueryResult } from "../../src/extension/dbDrivers/types";
import { QUERY_LIMIT_POLICY } from "../../src/shared/safetyContracts";

type MongoDocument = Record<string, unknown>;
type ReadMethod = "find" | "aggregate";

const HARD_CAP = QUERY_LIMIT_POLICY.hardCap;
const READ_METHODS: ReadMethod[] = ["find", "aggregate"];

function makeDocuments(count: number): MongoDocument[] {
  return Array.from({ length: count }, (_, index) => ({
    _id: index,
    value: `value-${index}`,
  }));
}

function sortDocuments(
  documents: MongoDocument[],
  sort: Record<string, number>,
): MongoDocument[] {
  return documents.sort((left, right) => {
    for (const [field, direction] of Object.entries(sort)) {
      const difference = Number(left[field]) - Number(right[field]);
      if (difference !== 0) return difference * direction;
    }
    return 0;
  });
}

// Unlike a fixed mockResolvedValue(), this cursor models MongoDB's dangerous
// limit(0) == unlimited behavior and records the documents actually fetched.
class MemoryFindCursor {
  private limitValue = 0;
  private offset = 0;
  private sortSpec: Record<string, number> = {};

  constructor(
    private readonly documents: MongoDocument[],
    private readonly onRead: (count: number) => void,
  ) {}

  readonly limit = vi.fn((value: number) => {
    this.limitValue = value;
    return this;
  });

  readonly skip = vi.fn((value: number) => {
    this.offset = value;
    return this;
  });

  readonly sort = vi.fn((value: Record<string, number>) => {
    this.sortSpec = value;
    return this;
  });

  readonly toArray = vi.fn(async () => {
    const sorted = sortDocuments([...this.documents], this.sortSpec);
    const rows = sorted.slice(
      this.offset,
      this.limitValue === 0
        ? undefined
        : this.offset + Math.abs(this.limitValue),
    );
    this.onRead(rows.length);
    return rows;
  });
}

function createHarness(documents: MongoDocument[]) {
  const cursors: MemoryFindCursor[] = [];
  const fetched: number[] = [];
  const events: string[] = [];
  const find = vi.fn((_filter: MongoDocument, _options?: unknown) => {
    const cursor = new MemoryFindCursor(documents, (count) => {
      fetched.push(count);
      events.push("find");
    });
    cursors.push(cursor);
    return cursor;
  });
  const aggregate = vi.fn((pipeline: MongoDocument[], _options?: unknown) => ({
    toArray: vi.fn(async () => {
      let rows = [...documents];
      for (const stage of pipeline) {
        if (stage.$sort) {
          rows = sortDocuments(rows, stage.$sort as Record<string, number>);
        }
        if (typeof stage.$skip === "number") rows = rows.slice(stage.$skip);
        if ("$limit" in stage) {
          if (
            typeof stage.$limit !== "number" ||
            !Number.isSafeInteger(stage.$limit) ||
            stage.$limit <= 0
          ) {
            throw new Error("MongoDB $limit must be a positive integer");
          }
          rows = rows.slice(0, stage.$limit);
        }
      }
      fetched.push(rows.length);
      events.push("aggregate");
      return rows;
    }),
  }));
  const insertOne = vi.fn(async () => {
    events.push("insertOne");
    return { acknowledged: true, insertedId: "inserted-after-cap" };
  });
  const updateMany = vi.fn(async () => {
    events.push("updateMany");
    return { matchedCount: 2, modifiedCount: 2 };
  });
  const collection = { find, aggregate, insertOne, updateMany };
  const driver = new MongoDBDriver({
    id: "mongo-query-budgets",
    name: "Mongo query budgets",
    type: "mongodb",
    database: "test",
  });
  Object.assign(driver, {
    connected: true,
    client: { db: () => ({ collection: () => collection }) },
  });
  return {
    driver,
    find,
    aggregate,
    cursors,
    fetched,
    events,
    insertOne,
    updateMany,
  };
}

function readQuery(method: ReadMethod, limit?: number): string {
  const query =
    method === "find" ? "db.items.find({})" : "db.items.aggregate([])";
  return limit === undefined ? query : `${query}.limit(${limit})`;
}

function expectFetchLimit(
  harness: ReturnType<typeof createHarness>,
  method: ReadMethod,
  limit: number,
) {
  expect(Number.isSafeInteger(limit) && limit > 0).toBe(true);
  if (method === "find") {
    expect(harness.cursors[0].limit).toHaveBeenCalledExactlyOnceWith(limit);
  } else {
    expect(harness.aggregate.mock.calls[0][0].at(-1)).toEqual({
      $limit: limit,
    });
  }
}

function valuesFor(result: QueryResult, column: string): unknown[] {
  const index = result.columns.indexOf(column);
  expect(index).toBeGreaterThanOrEqual(0);
  return result.rows.map((row) => row[`__col_${index}`]);
}

describe.each(READ_METHODS)("MongoDB %s query budgets", (method) => {
  it.each([
    0, 99, 100, 101, 12_001,
  ])("probes the default 100-row cap on %i documents", async (count) => {
    const harness = createHarness(makeDocuments(count));
    const result = await harness.driver.query(readQuery(method));

    expectFetchLimit(harness, method, 101);
    expect(harness.fetched).toEqual([Math.min(count, 101)]);
    expect(result.rows).toHaveLength(Math.min(count, 100));
    expect(result.rowCount).toBe(result.rows.length);
    expect(result.truncated === true).toBe(count > 100);
  });

  it.each([
    1,
    100,
    HARD_CAP - 1,
    HARD_CAP,
  ])("does not probe or falsely flag explicit limit %i", async (limit) => {
    const harness = createHarness(makeDocuments(12_001));
    const result = await harness.driver.query(readQuery(method, limit));

    expectFetchLimit(harness, method, limit);
    expect(harness.fetched).toEqual([limit]);
    expect(result.rows).toHaveLength(limit);
    expect(result.truncated === true).toBe(false);
  });

  it.each([
    HARD_CAP - 1,
    HARD_CAP,
    HARD_CAP + 1,
    12_001,
  ])("probes the hard cap for an oversized explicit limit on %i documents", async (count) => {
    const harness = createHarness(makeDocuments(count));
    const result = await harness.driver.query(readQuery(method, 20_000));

    expectFetchLimit(harness, method, HARD_CAP + 1);
    expect(harness.fetched).toEqual([Math.min(count, HARD_CAP + 1)]);
    expect(result.rows).toHaveLength(Math.min(count, HARD_CAP));
    expect(result.rowCount).toBe(result.rows.length);
    expect(result.truncated === true).toBe(count > HARD_CAP);
  });

  it.each([
    undefined,
    20_000,
  ])("removes the probe before display conversion and schema inference (limit %s)", async (limit) => {
    const retained = limit === undefined ? 100 : HARD_CAP;
    const documents = makeDocuments(retained + 1);
    const probeValue = vi.fn(() => {
      throw new Error("Probe must never be converted for display");
    });
    Object.defineProperty(documents[retained], "probe_only", {
      enumerable: true,
      get: probeValue,
    });
    const harness = createHarness(documents);
    const result = await harness.driver.query(readQuery(method, limit));

    expect(result.rows).toHaveLength(retained);
    expect(result.columns).toEqual(["_id", "value"]);
    expect(result.truncated).toBe(true);
    expect(probeValue).not.toHaveBeenCalled();
  });

  it.each([
    "-1",
    "-100",
    "-0.5",
    "0.5",
    "1.5",
    '"0"',
    "null",
    "undefined",
    "true",
    "{}",
    "[]",
    "9007199254740992",
    "NumberInt(1)",
    "",
    "1, 2",
  ])("rejects invalid limit(%s) before reading", async (value) => {
    const harness = createHarness(makeDocuments(12_001));
    await expect(
      harness.driver.query(`${readQuery(method)}.limit(${value})`),
    ).rejects.toThrow(/limit\(\).*non-negative safe integer/);
    expect(harness.find).not.toHaveBeenCalled();
    expect(harness.aggregate).not.toHaveBeenCalled();
    expect(harness.fetched).toEqual([]);
  });

  it.each([
    "NaN",
    "Infinity",
    "-Infinity",
  ])("rejects non-finite limit(%s) before reading", async (value) => {
    const harness = createHarness(makeDocuments(12_001));
    await expect(
      harness.driver.query(`${readQuery(method)}.limit(${value})`),
    ).rejects.toThrow(/Non-finite numbers/);
    expect(harness.find).not.toHaveBeenCalled();
    expect(harness.aggregate).not.toHaveBeenCalled();
  });

  it("uses the last repeated limit modifier", async () => {
    const harness = createHarness(makeDocuments(12_001));
    const result = await harness.driver.query(
      `${readQuery(method)}.limit(0).limit(3).limit(5)`,
    );

    expectFetchLimit(harness, method, 5);
    expect(result.rows).toHaveLength(5);
    expect(result.truncated === true).toBe(false);
  });

  it("validates the effective repeated limit before reading", async () => {
    const harness = createHarness(makeDocuments(12_001));
    await expect(
      harness.driver.query(`${readQuery(method)}.limit(5).limit(-1)`),
    ).rejects.toThrow(/Negative and fractional limits are not supported/);
    expect(harness.fetched).toEqual([]);
  });
});

describe("MongoDB find and aggregate limit semantics", () => {
  it.each([
    "0",
    "-0",
  ])("treats find limit(%s) as unlimited requested, not unlimited fetched", async (value) => {
    const harness = createHarness(makeDocuments(12_001));
    const result = await harness.driver.query(
      `db.items.find({}).limit(${value}).toArray()`,
      undefined,
      { readOnly: true },
    );

    expectFetchLimit(harness, "find", HARD_CAP + 1);
    expect(harness.fetched).toEqual([HARD_CAP + 1]);
    expect(result.rows).toHaveLength(HARD_CAP);
    expect(result.truncated).toBe(true);
  });

  it("does not falsely flag an exact-cap unlimited find", async () => {
    const harness = createHarness(makeDocuments(HARD_CAP));
    const result = await harness.driver.query("db.items.find({}).limit(0)");
    expectFetchLimit(harness, "find", HARD_CAP + 1);
    expect(result.rows).toHaveLength(HARD_CAP);
    expect(result.truncated === true).toBe(false);
  });

  it("lets a final zero limit replace an earlier finite find limit", async () => {
    const harness = createHarness(makeDocuments(12_001));
    const result = await harness.driver.query(
      "db.items.find({}).limit(3).limit(0)",
    );
    expectFetchLimit(harness, "find", HARD_CAP + 1);
    expect(result.rows).toHaveLength(HARD_CAP);
    expect(result.truncated).toBe(true);
  });

  it.each([
    "",
    ".limit(0)",
    ".limit(20_000)",
  ])("always bounds findOne%s to one document without a probe", async (chain) => {
    const harness = createHarness(makeDocuments(12_001));
    const result = await harness.driver.query(`db.items.findOne({})${chain}`);
    expectFetchLimit(harness, "find", 1);
    expect(harness.fetched).toEqual([1]);
    expect(result.rows).toHaveLength(1);
    expect(result.truncated === true).toBe(false);
  });

  it("rejects aggregate limit zero before creating a cursor", async () => {
    const harness = createHarness(makeDocuments(12_001));
    await expect(
      harness.driver.query("db.items.aggregate([]).limit(0)"),
    ).rejects.toThrow(/aggregate\(\).limit\(\).*positive safe integer/);
    expect(harness.aggregate).not.toHaveBeenCalled();
  });

  it.each([
    "0",
    "-1",
    "0.5",
  ])("does not normalize a literal pipeline $limit: %s into a different value", async (value) => {
    const harness = createHarness(makeDocuments(12_001));
    await expect(
      harness.driver.query(`db.items.aggregate([{$limit:${value}}])`),
    ).rejects.toThrow(/\$limit must be a positive integer/);
    expect(harness.aggregate.mock.calls[0][0]).toEqual([
      { $limit: Number(value) },
      { $limit: 101 },
    ]);
    expect(harness.fetched).toEqual([]);
  });

  it("does not flag a complete, explicitly limited pipeline", async () => {
    const harness = createHarness(makeDocuments(12_001));
    const result = await harness.driver.query(
      "db.items.aggregate([{$limit:5}])",
    );
    expect(harness.fetched).toEqual([5]);
    expect(result.rows).toHaveLength(5);
    expect(result.truncated === true).toBe(false);
  });

  it("preserves find projection, sort and skip with an explicit limit", async () => {
    const harness = createHarness(makeDocuments(10).reverse());
    const result = await harness.driver.query(
      "db.items.find({}, {value:1}).limit(3).sort({_id:1}).skip(4)",
    );
    expect(harness.find).toHaveBeenCalledWith(
      {},
      { projection: { value: 1 }, promoteValues: false, bsonRegExp: false },
    );
    expect(harness.cursors[0].sort).toHaveBeenCalledWith({ _id: 1 });
    expect(harness.cursors[0].skip).toHaveBeenCalledWith(4);
    expectFetchLimit(harness, "find", 3);
    expect(valuesFor(result, "_id")).toEqual([4, 5, 6]);
    expect(result.truncated === true).toBe(false);
  });

  it.each(
    READ_METHODS,
  )("probes %s after sort and skip, not before", async (method) => {
    const harness = createHarness(makeDocuments(150));
    const query =
      method === "find"
        ? "db.items.find({}).sort({_id:-1}).skip(45)"
        : "db.items.aggregate([{$sort:{_id:-1}}]).skip(45)";
    const result = await harness.driver.query(query);
    expectFetchLimit(harness, method, 101);
    expect(harness.fetched).toEqual([101]);
    expect(valuesFor(result, "_id")).toEqual(
      Array.from({ length: 100 }, (_, index) => 104 - index),
    );
    expect(result.truncated).toBe(true);
  });
});

describe("MongoDB multistatement result budgets", () => {
  it("propagates statement truncation even below the union cap", async () => {
    const documents = makeDocuments(101);
    documents[100].probe_only = "must not become a column";
    const harness = createHarness(documents);
    const result = await harness.driver.query(
      'db.items.find({}); db.items.insertOne({_id:"later"})',
    );
    expect(result.rows).toHaveLength(101);
    expect(result.truncated).toBe(true);
    expect(result.columns).not.toContain("probe_only");
    expect(result.affectedRows).toBe(1);
    expect(harness.insertOne).toHaveBeenCalledOnce();
  });

  it("shares retained slots across reads and still executes later mutations", async () => {
    const harness = createHarness(makeDocuments(12_001));
    const result = await harness.driver.query(
      'db.items.find({}).limit(6000); db.items.find({}).limit(6000); db.items.find({}).limit(5); db.items.insertOne({_id:"later"}); db.items.updateMany({},{$set:{done:true}})',
    );
    expect(
      harness.cursors.map((cursor) => cursor.limit.mock.calls[0][0]),
    ).toEqual([6000, 4001, 1]);
    expect(harness.fetched).toEqual([6000, 4001, 1]);
    expect(harness.events).toEqual([
      "find",
      "find",
      "find",
      "insertOne",
      "updateMany",
    ]);
    expect(result.rows).toHaveLength(HARD_CAP);
    expect(result.truncated).toBe(true);
    expect(result.affectedRows).toBe(3);
    expect(result.columns).not.toContain("insertedId");
    expect(result.columns).not.toContain("modifiedCount");
  });

  it("uses positive probes for aggregate/findOne after the union fills", async () => {
    const harness = createHarness(makeDocuments(12_001));
    const result = await harness.driver.query(
      'db.items.find({}).limit(10000); db.items.aggregate([]); db.items.findOne({}); db.items.insertOne({_id:"later"})',
    );
    expect(harness.aggregate.mock.calls[0][0]).toEqual([{ $limit: 1 }]);
    expect(harness.cursors[1].limit).toHaveBeenCalledWith(1);
    expect(harness.fetched).toEqual([HARD_CAP, 1, 1]);
    expect(harness.insertOne).toHaveBeenCalledOnce();
    expect(result.rows).toHaveLength(HARD_CAP);
    expect(result.truncated).toBe(true);
  });

  it("flags a dropped mutation result even when no read was truncated", async () => {
    const harness = createHarness(makeDocuments(HARD_CAP));
    const result = await harness.driver.query(
      'db.items.find({}).limit(10000); db.items.insertOne({_id:"later"})',
    );
    expect(harness.fetched).toEqual([HARD_CAP]);
    expect(result.rows).toHaveLength(HARD_CAP);
    expect(result.truncated).toBe(true);
    expect(result.affectedRows).toBe(1);
    expect(harness.insertOne).toHaveBeenCalledOnce();
  });

  it("does not falsely flag an exact-cap union including a mutation result", async () => {
    const harness = createHarness(makeDocuments(5000));
    const result = await harness.driver.query(
      'db.items.find({}).limit(5000); db.items.find({}).limit(4999); db.items.insertOne({_id:"later"})',
    );
    expect(harness.fetched).toEqual([5000, 4999]);
    expect(result.rows).toHaveLength(HARD_CAP);
    expect(result.rowCount).toBe(HARD_CAP);
    expect(result.truncated === true).toBe(false);
    expect(result.affectedRows).toBe(1);
  });

  it("does not flag empty read results after the union fills", async () => {
    const harness = createHarness(makeDocuments(HARD_CAP));
    const result = await harness.driver.query(
      "db.items.find({}).limit(10000); db.items.find({}).skip(10000).limit(0)",
    );
    expect(harness.cursors[1].limit).toHaveBeenCalledWith(1);
    expect(harness.fetched).toEqual([HARD_CAP, 0]);
    expect(result.rows).toHaveLength(HARD_CAP);
    expect(result.truncated === true).toBe(false);
  });
});
