import {
  BSON,
  Decimal128,
  Double,
  Int32,
  MaxKey,
  MinKey,
  ObjectId,
} from "mongodb";
import { describe, expect, it, vi } from "vitest";
import { MongoDBDriver } from "../../src/extension/dbDrivers/mongodb";
import * as nosqlUtils from "../../src/extension/dbDrivers/nosqlUtils";
import type {
  ColumnTypeMeta,
  DriverTablePageRequest,
  FilterExpression,
} from "../../src/extension/dbDrivers/types";

type MongoDocument = Record<string, unknown>;

function column(
  name: string,
  nativeType: string,
  category: ColumnTypeMeta["category"],
): ColumnTypeMeta {
  return {
    name,
    type: nativeType,
    nativeType,
    category,
    nullable: true,
    isPrimaryKey: false,
    isForeignKey: false,
    filterable: true,
    filterOperators: [],
    valueSemantics: "plain",
  } as ColumnTypeMeta;
}

function createHarness(
  pageDocuments: MongoDocument[],
  schemaDocuments: MongoDocument[] = pageDocuments,
) {
  const driver = new MongoDBDriver({
    id: "mongo-edge-cases",
    name: "Mongo edge cases",
    type: "mongodb",
  });
  const cursors: Array<Record<string, ReturnType<typeof vi.fn>>> = [];
  const find = vi.fn((_criteria: unknown, _options?: unknown) => {
    const documents =
      find.mock.calls.length === 1 ? schemaDocuments : pageDocuments;
    const cursor: Record<string, ReturnType<typeof vi.fn>> = {};
    cursor.sort = vi.fn(() => cursor);
    cursor.skip = vi.fn(() => cursor);
    cursor.limit = vi.fn(() => cursor);
    cursor.toArray = vi.fn(async () => documents);
    cursors.push(cursor);
    return cursor;
  });
  const countDocuments = vi.fn(async () => pageDocuments.length);
  const aggregate = vi.fn((_pipeline: unknown[], _options?: unknown) => ({
    toArray: async () => [],
  }));
  const insertOne = vi.fn(async () => ({
    acknowledged: true,
    insertedId: "new",
  }));
  const collection = { find, countDocuments, aggregate, insertOne };
  Object.assign(driver, {
    requireDb: () => ({ collection: () => collection }),
    isView: async () => false,
  });
  return { driver, find, countDocuments, aggregate, insertOne, cursors };
}

function pageRequest(
  overrides: Partial<DriverTablePageRequest> = {},
): DriverTablePageRequest {
  return {
    database: "db",
    schema: "db",
    table: "items",
    page: 1,
    pageSize: 25,
    filters: [],
    sort: null,
    skipCount: true,
    ...overrides,
  };
}

describe("MongoDB double column edits", () => {
  it("keeps integral input in a double column as BSON Double", () => {
    const { driver } = createHarness([]);
    const value = driver.coerceTableEditValue(
      "6",
      column("price", "double", "float"),
    );
    expect(value).toBeInstanceOf(Double);
    const decoded = BSON.deserialize(BSON.serialize({ price: value }), {
      promoteValues: false,
    });
    expect(decoded.price).toBeInstanceOf(Double);
  });

  it("renders Double values in mutation previews and accepts them in queries", async () => {
    const { driver, insertOne } = createHarness([]);
    const preview = driver.buildMutationPreviewStatement(
      "update",
      "db",
      "",
      "items",
      {
        primaryKeys: { _id: "a" },
        changes: { price: new Double(6) },
      },
    );
    expect(preview).toContain("Double(6)");
    expect(preview).not.toContain('"value"');
    await driver.query("db.items.insertOne({ price: Double(6) })");
    const [document] = insertOne.mock.calls[0] as unknown as [MongoDocument];
    expect(document.price).toBeInstanceOf(Double);
  });

  it.each([
    [-0, 'Double("-0")'],
    [Number.NaN, 'Double("NaN")'],
    [Number.POSITIVE_INFINITY, 'Double("Infinity")'],
    [Number.NEGATIVE_INFINITY, 'Double("-Infinity")'],
    [6.5, "Double(6.5)"],
  ])("round-trips Double(%s) through previews and queries", async (value, literal) => {
    const { driver, insertOne } = createHarness([]);
    const preview = driver.buildMutationPreviewStatement(
      "insert",
      "db",
      "",
      "items",
      { values: { price: new Double(value) } },
    );
    expect(preview).toContain(literal);
    await driver.query(preview);
    const [document] = insertOne.mock.calls[0] as unknown as [MongoDocument];
    expect(document.price).toBeInstanceOf(Double);
    expect(Object.is((document.price as Double).value, value)).toBe(true);
  });

  it("preserves negative zero typed directly in a query", async () => {
    const { driver, insertOne } = createHarness([]);
    await driver.query("db.items.insertOne({ price: Double(-0) })");
    const [document] = insertOne.mock.calls[0] as unknown as [MongoDocument];
    expect(Object.is((document.price as Double).value, -0)).toBe(true);
  });

  it("rejects non-numeric Double arguments", async () => {
    const { driver, insertOne } = createHarness([]);
    await expect(
      driver.query('db.items.insertOne({ price: Double("6") })'),
    ).rejects.toThrow(/Double requires/);
    expect(insertOne).not.toHaveBeenCalled();
  });
});

describe("MongoDB aggregate bounding", () => {
  it.each([
    [{ $out: "archive" }],
    [{ $merge: { into: "archive" } }],
  ])("does not append stages after a final %j", async (stage) => {
    const { driver, aggregate } = createHarness([]);
    await driver.query(
      `db.items.aggregate([{ $match: {} }, ${JSON.stringify(stage)}])`,
    );
    expect(aggregate.mock.calls[0][0]).toEqual([{ $match: {} }, stage]);
  });

  it("rejects cursor limits on write pipelines before running them", async () => {
    const { driver, aggregate } = createHarness([]);
    await expect(
      driver.query('db.items.aggregate([{ $out: "archive" }]).limit(5)'),
    ).rejects.toThrow(/\$out or \$merge/);
    expect(aggregate).not.toHaveBeenCalled();
  });

  it("applies repeated limits cumulatively like the Node driver", async () => {
    const { driver, aggregate } = createHarness([]);
    await driver.query("db.items.aggregate([]).limit(5).limit(10)");
    expect(aggregate.mock.calls[0][0]).toEqual([
      { $limit: 5 },
      { $limit: 10 },
      { $limit: 10 },
    ]);
  });

  it("emits chained limit and skip stages in call order", async () => {
    const { driver, aggregate } = createHarness([]);
    await driver.query("db.items.aggregate([]).limit(10).skip(5)");
    expect(aggregate.mock.calls[0][0]).toEqual([
      { $limit: 10 },
      { $skip: 5 },
      { $limit: 10 },
    ]);
  });
});

describe("MongoDB read helper arguments", () => {
  it.each([
    "db.items.countDocuments({}, { limit: 1 })",
    "db.items.countDocuments({}, { allowDiskUse: true })",
    'db.items.aggregate([], { collation: { locale: "fr" } })',
    "db.items.find({}, {}, { limit: 1 })",
    "db.items.find({}, {}, { maxTimeMS: -1 })",
    "db.items.find({}, {}, {}, {})",
  ])("rejects result-changing or extra arguments: %s", async (text) => {
    const { driver, find, countDocuments, aggregate } = createHarness([]);
    await expect(driver.query(text)).rejects.toThrow(/supports only/);
    expect(find).not.toHaveBeenCalled();
    expect(countDocuments).not.toHaveBeenCalled();
    expect(aggregate).not.toHaveBeenCalled();
  });

  it("tightens the query deadline with maxTimeMS instead of forwarding both", async () => {
    const { driver, aggregate, countDocuments } = createHarness([]);
    const deadline = Date.now() + 60_000;
    await driver.query("db.items.aggregate([], { maxTimeMS: 5 })", undefined, {
      deadline,
    });
    expect(aggregate.mock.calls[0][1]).toMatchObject({ timeoutMS: 5 });
    expect(aggregate.mock.calls[0][1]).not.toHaveProperty("maxTimeMS");
    await driver.query(
      "db.items.countDocuments({}, { maxTimeMS: 600000 })",
      undefined,
      { deadline },
    );
    const countOptions = (countDocuments.mock.calls[0] as unknown[])[1] as {
      timeoutMS: number;
    };
    expect(countOptions).not.toHaveProperty("maxTimeMS");
    expect(countOptions.timeoutMS).toBeLessThanOrEqual(60_000);
  });

  it("forwards result-neutral read options", async () => {
    const { driver, find, countDocuments, aggregate } = createHarness([]);
    await driver.query(
      'db.items.aggregate([], { allowDiskUse: true, comment: "audit", maxTimeMS: 5 })',
    );
    expect(aggregate.mock.calls[0][1]).toMatchObject({
      allowDiskUse: true,
      comment: "audit",
      maxTimeMS: 5,
    });
    await driver.query('db.items.countDocuments({}, { comment: "audit" })');
    expect(countDocuments.mock.calls[0]).toEqual([{}, { comment: "audit" }]);
    await driver.query('db.items.find({}, {}, { comment: "audit" })');
    expect(find.mock.calls[0][1]).toMatchObject({ comment: "audit" });
  });
});

describe("MongoDB table page columns", () => {
  it("includes fields that appear only after the schema sample", async () => {
    const { driver } = createHarness(
      [
        { _id: "a", name: "first" },
        { _id: "b", late: "value" },
      ],
      [{ _id: "a", name: "first" }],
    );
    const page = await driver.readTablePage(pageRequest());
    expect(page.columns.map((entry) => entry.name)).toEqual([
      "_id",
      "name",
      "late",
    ]);
    expect(page.columns[2]).toMatchObject({
      nativeType: "mixed",
      isReadOnly: true,
      filterable: false,
    });
    const exported = driver.formatTableExportChunk({
      columns: page.columns.slice(0, 2),
      rows: page.rows,
    });
    expect(exported.columns.map((entry) => entry.name)).toContain("late");
    expect(exported.rows[1]).toEqual({ _id: "b", late: "value" });
  });

  it("keeps selected export columns stable for chunks without that field", async () => {
    const { driver } = createHarness([{ _id: "a", name: "first" }]);
    const page = await driver.readTablePage(pageRequest());
    const exported = driver.formatTableExportChunk(page, ["_id", "late"]);
    expect(exported.columns.map((entry) => entry.name)).toEqual([
      "_id",
      "name",
      "late",
    ]);
    expect(exported.rows[0]).toEqual({ _id: "a" });
  });

  it("exports BSON undefined as canonical $undefined", async () => {
    const { driver } = createHarness([{ _id: "a", gone: undefined }]);
    const page = await driver.readTablePage(pageRequest());
    const exported = driver.formatTableExportChunk(page);
    expect(JSON.parse(JSON.stringify(exported.rows[0]))).toEqual({
      _id: "a",
      gone: { $undefined: true },
    });
  });
});

describe("MongoDB table page server reads", () => {
  it("passes cancellation and the remaining deadline to the server", async () => {
    const { driver, find, countDocuments } = createHarness([{ _id: "a" }]);
    const controller = new AbortController();
    await driver.readTablePage(
      pageRequest({
        signal: controller.signal,
        deadline: Date.now() + 60_000,
        skipCount: false,
      }),
    );
    expect(find.mock.calls[1][1]).toMatchObject({
      signal: controller.signal,
      timeoutMS: expect.any(Number),
    });
    expect(countDocuments.mock.calls[0]).toEqual([
      {},
      { signal: controller.signal, timeoutMS: expect.any(Number) },
    ]);
  });

  it("surfaces server errors instead of silently re-reading client-side", async () => {
    const { driver, find, cursors } = createHarness([{ _id: "a" }]);
    find.mockImplementationOnce((_criteria, _options) => {
      const cursor: Record<string, ReturnType<typeof vi.fn>> = {};
      cursor.limit = vi.fn(() => cursor);
      cursor.toArray = vi.fn(async () => [{ _id: "a" }]);
      return cursor;
    });
    find.mockImplementationOnce(() => {
      throw new Error("operation exceeded time limit");
    });
    await expect(driver.readTablePage(pageRequest())).rejects.toThrow(
      "operation exceeded time limit",
    );
    expect(find).toHaveBeenCalledTimes(2);
    expect(cursors).toHaveLength(0);
  });

  it.each([
    { filters: [{ column: "a.b", operator: "eq", value: "dotted" }] },
    { sort: { column: "a.b", direction: "desc" } },
  ] as Array<
    Partial<DriverTablePageRequest>
  >)("keeps literal dotted fields off server paths: %j", async (overrides) => {
    const { driver, find } = createHarness([
      { _id: "1", "a.b": "dotted", a: { b: "nested" } },
      { _id: "2", "a.b": "other", a: { b: "dotted" } },
    ]);
    const page = await driver.readTablePage(pageRequest(overrides));
    for (const [criteria] of find.mock.calls) {
      expect(JSON.stringify(criteria)).not.toContain("a.b");
    }
    expect(page.rows.map((row) => row._id)).toEqual(
      overrides.sort ? ["2", "1"] : ["1"],
    );
  });

  it("keeps server-compatible filters on the server when sorting by a literal dotted field", async () => {
    const applyFilters = vi.spyOn(nosqlUtils, "applyFilters");
    const documents = [
      { _id: "1", v: "123", "a.b": "a" },
      { _id: "2", v: new Int32(123), "a.b": "c" },
      { _id: "3", v: "124", "a.b": "b" },
    ];
    const schemaDocuments = [documents[0], documents[2]];
    for (const sort of [null, { column: "a.b", direction: "desc" as const }]) {
      const { driver, find } = createHarness(documents, schemaDocuments);
      find.mockImplementation((criteria) => {
        const regex = (criteria as { v?: { $regex?: string } }).v?.$regex;
        // Model MongoDB $regex: BSON numbers do not match a string pattern.
        const rows =
          find.mock.calls.length === 1
            ? schemaDocuments
            : regex === undefined
              ? documents
              : documents.filter(
                  (document) =>
                    typeof document.v === "string" &&
                    new RegExp(regex).test(document.v),
                );
        const cursor: Record<string, ReturnType<typeof vi.fn>> = {};
        cursor.sort = vi.fn(() => cursor);
        cursor.skip = vi.fn(() => cursor);
        cursor.limit = vi.fn(() => cursor);
        cursor.toArray = vi.fn(async () => rows);
        return cursor;
      });
      const page = await driver.readTablePage(
        pageRequest({
          filters: [{ column: "v", operator: "like", value: "12" }],
          sort,
        }),
      );
      expect(find.mock.calls[1][0]).toEqual({ v: { $regex: "12" } });
      expect(page.rows.map((row) => row._id)).toEqual(
        sort ? ["3", "1"] : ["1", "3"],
      );
      expect(applyFilters).not.toHaveBeenCalled();
    }
  });
});

describe("MongoDB mongosh literal scanning", () => {
  it.each([
    'db.items.find({ note: "for sale" })',
    "db.items.find({ note: 'process the import' })",
    "db.items.find({ note: `eval constructor => while` })",
    "db.items.find({ note: /try|catch/ })",
    'db.items.find({ note: "__init__" }) // for while',
    'db.items.find({ note: "a" /* try */ })',
  ])("allows unsafe-looking words inside literals: %s", async (text) => {
    const { driver, find } = createHarness([]);
    await driver.query(text);
    expect(find).toHaveBeenCalledOnce();
  });

  it("explains escapes that cannot be verified after an ambiguous slash", async () => {
    const { driver, find } = createHarness([]);
    await expect(
      driver.query('db.items.find({ a: (1) / 2, b: "x\\ny" })'),
    ).rejects.toThrow(/ambiguous '\/'/);
    expect(find).not.toHaveBeenCalled();
  });

  it.each([
    "db.items.find({}).constructor",
    'db.items["constructor"]',
    "db.items.find({ note: `$" + "{process}` })",
    'db.items.find({ "__proto__": {} })',
    'if (1) /"/; db.constructor; ""',
    'db.items.find({ a: (1) / 2, b: "" + `$' + '{"x"}`, c: process })',
    'db.items.find({ "__proto__"/**/: { x: 1 } })',
    'db.items.find({ a: db["constructor"/**/] })',
    "db.items.find({ a: db['constructor'//x\n] })",
    'db.items.find({ a: db[("constructor")] })',
    'db.items.find({ a: db[0, "constructor"] })',
    "db.items.find({ a: db[`$" + '{"constructor"}`] })',
    "db.items.find({ a: db[/constructor/.source] })",
    "db.items.find({ a: db.\\u0063onstructor })",
  ])("still blocks unsafe code: %s", async (text) => {
    const { driver, find } = createHarness([]);
    await expect(driver.query(text)).rejects.toThrow(/Unsafe mongosh/);
    expect(find).not.toHaveBeenCalled();
  });
});

describe("MongoDB typed table edits", () => {
  it.each([
    ["date", "datetime", "2024-13-45"],
    ["objectId", "text", "not-an-object-id"],
    ["bool", "boolean", "yes"],
    ["decimal", "decimal", "1.2.3"],
    ["double", "float", "6x"],
    ["object", "json", "{bad json"],
  ] as const)("rejects unparseable %s input", (nativeType, category, value) => {
    const { driver } = createHarness([]);
    expect(() =>
      driver.coerceTableEditValue(value, column("f", nativeType, category)),
    ).toThrow(/Invalid MongoDB/);
  });

  it("stores explicitly quoted input as a string without re-coercing it", () => {
    const { driver } = createHarness([]);
    expect(
      driver.coerceTableEditValue('"yes"', column("f", "bool", "boolean")),
    ).toBe("yes");
    expect(
      driver.coerceTableEditValue('"true"', column("f", "bool", "boolean")),
    ).toBe("true");
    expect(
      driver.coerceTableEditValue('"123"', column("f", "int", "integer"), {
        preserveNativeType: true,
      }),
    ).toBe("123");
    expect(
      driver.coerceTableEditValue(
        '"507f1f77bcf86cd799439011"',
        column("f", "objectId", "text"),
      ),
    ).toBe("507f1f77bcf86cd799439011");
    expect(
      driver.coerceTableEditValue('"a\\"b"', column("f", "object", "json")),
    ).toBe('a"b');
  });

  it("reports invalid embedded JSON without internal type names", () => {
    const { driver } = createHarness([]);
    expect(() =>
      driver.coerceTableEditValue("{bad", column("f", "bsonDocument", "json")),
    ).toThrow(/^Invalid MongoDB JSON value/);
  });

  it("parses typed input that is valid", () => {
    const { driver } = createHarness([]);
    expect(
      driver.coerceTableEditValue("1.25", column("f", "decimal", "decimal")),
    ).toBeInstanceOf(Decimal128);
    expect(
      driver.coerceTableEditValue('{"a":1}', column("f", "object", "json")),
    ).toEqual({ a: 1 });
  });

  it("rejects edits of embedded values that contain non-JSON BSON types", async () => {
    const { driver } = createHarness([
      {
        _id: "a",
        lossy: { owner: new ObjectId("507f1f77bcf86cd799439011") },
        whole: [new Double(6)],
        plain: { label: "x", count: new Int32(2), ratio: new Double(0.5) },
      },
    ]);
    const page = await driver.readTablePage(pageRequest());
    const types = page.mongoValueTypes?.[0] ?? {};
    expect(types.lossy.nativeType).toBe("bsonDocument");
    expect(types.whole.nativeType).toBe("bsonArray");
    expect(types.plain.nativeType).toBe("object");
    expect(() =>
      driver.validateTableUpdates([
        {
          changes: { lossy: '{"owner":"x"}' },
          originalValues: { lossy: page.rows[0].lossy },
          originalTypes: { lossy: types.lossy },
        },
      ]),
    ).toThrow(/cannot safely edit field "lossy"/);
    expect(() =>
      driver.validateTableUpdates([
        {
          changes: { plain: '{"label":"y"}' },
          originalValues: { plain: page.rows[0].plain },
          originalTypes: { plain: types.plain },
        },
      ]),
    ).not.toThrow();
  });
});

describe("MongoDB MinKey and MaxKey input", () => {
  it("keeps MinKey()/MaxKey() text in string columns", () => {
    const { driver } = createHarness([]);
    expect(
      driver.coerceInputValue("MinKey()", column("f", "string", "text")),
    ).toBe("MinKey()");
    expect(
      driver.coerceInputValue("MaxKey()", column("f", "string", "text")),
    ).toBe("MaxKey()");
  });

  it("interprets them for minKey and maxKey columns", () => {
    const { driver } = createHarness([]);
    expect(
      driver.coerceInputValue("MinKey()", column("f", "minKey", "other")),
    ).toBeInstanceOf(MinKey);
    expect(
      driver.coerceInputValue("MaxKey()", column("f", "maxKey", "other")),
    ).toBeInstanceOf(MaxKey);
  });
});

describe("MongoDB like filters on non-string BSON columns", () => {
  it.each([
    "regex",
    "dbPointer",
    "minKey",
    "maxKey",
  ])("filters %s columns client-side", (nativeType) => {
    const { driver } = createHarness([]);
    const filters: FilterExpression[] = [
      { column: "f", operator: "like", value: "a" },
    ];
    expect(
      (
        driver as unknown as {
          shouldUseClientSideFiltering: (
            filters: FilterExpression[],
            columns: ColumnTypeMeta[],
          ) => boolean;
        }
      ).shouldUseClientSideFiltering(filters, [
        column("f", nativeType, "other"),
      ]),
    ).toBe(true);
  });
});

describe("MongoDB type inference trusts only native BSON values", () => {
  it("does not treat user fields named _bsontype or sub_type as BSON types", async () => {
    const { driver } = createHarness([
      {
        _id: "a",
        tagged: { _bsontype: "Int32", value: 5 },
        subtype: { sub_type: 4, note: "x" },
      },
    ]);
    const page = await driver.readTablePage(pageRequest());
    const byName = new Map(page.columns.map((entry) => [entry.name, entry]));
    expect(byName.get("tagged")?.nativeType).toBe("object");
    expect(byName.get("subtype")?.nativeType).toBe("object");
    expect(page.rows[0].tagged).toBe('{"_bsontype":"Int32","value":5}');
    expect(page.rows[0].subtype).toBe('{"sub_type":4,"note":"x"}');
  });
});
