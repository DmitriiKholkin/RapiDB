import {
  BSONSymbol,
  Code,
  DBRef,
  MongoOperationTimeoutError,
  ObjectId,
  Timestamp,
} from "mongodb";
import { describe, expect, it, vi } from "vitest";
import { MongoDBDriver } from "../../src/extension/dbDrivers/mongodb";
import type {
  ColumnTypeMeta,
  DriverTablePageRequest,
  FilterExpression,
} from "../../src/extension/dbDrivers/types";
import { PersistedEditValidationError } from "../../src/extension/table/persistedEditValidation";
import { applyChangesTransactional } from "../../src/extension/table/tableMutationExecution";
import { TableMutationService } from "../../src/extension/table/tableMutationService";
import { resolveMongoRemoteTarget } from "../../src/shared/mongoConnection";

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

function setup(
  documents: MongoDocument[],
  dbPointerCells: ReadonlySet<string> = new Set(),
) {
  const config = { id: "g4", name: "G4", type: "mongodb" } as const;
  const driver = new MongoDBDriver(config);
  const limits: number[] = [];
  const findOptions: unknown[] = [];
  const find = vi.fn((_criteria: unknown, options?: unknown) => {
    findOptions.push(options);
    let limit = Number.POSITIVE_INFINITY;
    const cursor = {
      sort: vi.fn(() => cursor),
      skip: vi.fn(() => cursor),
      limit: vi.fn((value: number) => {
        limits.push(value);
        limit = value;
        return cursor;
      }),
      toArray: vi.fn(async () => documents.slice(0, limit)),
    };
    return cursor;
  });
  const aggregateOptions: unknown[] = [];
  const aggregate = vi.fn(
    (
      pipeline: [
        { $match: { _id: { $in: unknown[] } } },
        { $project: Record<string, { $type: string }> },
      ],
      options?: unknown,
    ) => {
      aggregateOptions.push(options);
      const ids = new Set(pipeline[0].$match._id.$in);
      const projection = Object.entries(pipeline[1].$project);
      return {
        toArray: async () =>
          documents
            .filter((document) => ids.has(document._id))
            .map((document) => ({
              _id: document._id,
              ...Object.fromEntries(
                projection.map(([alias, { $type }]) => [
                  alias,
                  dbPointerCells.has(`${document._id}:${$type.slice(1)}`)
                    ? "dbPointer"
                    : "object",
                ]),
              ),
            })),
      };
    },
  );
  let listCollectionsError: unknown;
  let viewType = "collection";
  const listCollectionsOptions: unknown[] = [];
  const updateOne = vi.fn(async () => ({
    acknowledged: true,
    matchedCount: 1,
  }));
  const insertOne = vi.fn(async () => ({ acknowledged: true, insertedId: 1 }));
  const collection = {
    find,
    aggregate,
    updateOne,
    insertOne,
    countDocuments: vi.fn(async () => documents.length),
  };
  Object.assign(driver, {
    connected: true,
    client: {
      db: () => ({
        collection: () => collection,
        listCollections: (_filter: unknown, options?: unknown) => {
          listCollectionsOptions.push(options);
          return {
            toArray: async () => {
              if (typeof listCollectionsError === "function")
                listCollectionsError();
              if (listCollectionsError) throw listCollectionsError;
              return [{ name: "items", type: viewType, options: {} }];
            },
          };
        },
      }),
    },
  });
  const manager = { getDriver: () => driver, getConnection: () => config };
  return {
    driver,
    find,
    aggregate,
    aggregateOptions,
    failListCollections: (error: unknown) => {
      listCollectionsError = error;
    },
    markAsView: () => {
      viewType = "view";
    },
    limits,
    findOptions,
    listCollectionsOptions,
    updateOne,
    insertOne,
    manager,
  };
}

function pageRequest(
  overrides: Partial<DriverTablePageRequest> = {},
): DriverTablePageRequest {
  return {
    database: "db",
    schema: "",
    table: "items",
    page: 1,
    pageSize: 25,
    filters: [],
    sort: null,
    skipCount: true,
    ...overrides,
  };
}

describe("MongoDB string edits keep the typed value exactly", () => {
  it.each([
    "  padded  ",
    "text\n",
    '"quoted"',
    "'single'",
  ])("stores %j unchanged in a string column", (value) => {
    const { driver } = setup([]);
    const text = column("f", "string", "text");
    expect(driver.coerceTableEditValue(value, text)).toBe(value);
    expect(
      driver.coerceTableEditValue(value, text, { preserveNativeType: true }),
    ).toBe(value);
    expect(driver.coerceInputValue(value, text)).toBe(value);
    expect(driver.coerceOriginalValue(value, text)).toBe(value);
  });

  it("keeps symbol and JavaScript code values exact", () => {
    const { driver } = setup([]);
    const symbol = driver.coerceTableEditValue(
      ' "sym" ',
      column("f", "symbol", "text"),
    );
    expect(symbol).toBeInstanceOf(BSONSymbol);
    expect((symbol as BSONSymbol).valueOf()).toBe(' "sym" ');
    const code = driver.coerceTableEditValue(
      "  return 1;\n",
      column("f", "javascript", "other"),
    );
    expect(code).toBeInstanceOf(Code);
    expect((code as Code).code).toBe("  return 1;\n");
    expect(
      (
        driver.coerceOriginalValue(
          " x ",
          column("f", "symbol", "text"),
        ) as BSONSymbol
      ).valueOf(),
    ).toBe(" x ");
  });

  it("still uses quotes to force strings into typed columns", () => {
    const { driver } = setup([]);
    expect(
      driver.coerceTableEditValue('"true"', column("f", "bool", "boolean")),
    ).toBe("true");
  });
});

describe("MongoDB display date input", () => {
  const date = column("d", "date", "datetime");

  it.each([
    ["2024-01-01 10:00", "2024-01-01T10:00:00.000Z"],
    ["2024-01-01T10:00", "2024-01-01T10:00:00.000Z"],
    ["2024-01-01 10:00:05.123456", "2024-01-01T10:00:05.123Z"],
    ["2024-01-01T10:00:00+02:00", "2024-01-01T08:00:00.000Z"],
    ["2024-01-01 10:00:00 -0530", "2024-01-01T15:30:00.000Z"],
    ["2024-01-01T10:00:00z", "2024-01-01T10:00:00.000Z"],
    ["2024-02-29", "2024-02-29T00:00:00.000Z"],
  ])("parses %j as UTC", (input, expected) => {
    const { driver } = setup([]);
    const value = driver.coerceTableEditValue(input, date);
    expect(value).toBeInstanceOf(Date);
    expect((value as Date).toISOString()).toBe(expected);
  });

  it.each([
    "2024-02-31",
    "2023-02-29",
    "2024-01-01 24:00",
    "2024-01-01 10:60",
    "Jan 1 2024",
    "01/02/2024",
    "1704103200000",
  ])("rejects %j instead of parsing it locally", (input) => {
    const { driver } = setup([]);
    expect(() => driver.coerceTableEditValue(input, date)).toThrow(
      /Invalid MongoDB date/,
    );
  });

  it("uses the same UTC parser for timestamps", () => {
    const { driver } = setup([]);
    const value = driver.coerceTableEditValue(
      "2024-01-01 10:00",
      column("t", "timestamp", "datetime"),
    );
    expect(value).toBeInstanceOf(Timestamp);
    expect((value as Timestamp).t).toBe(Date.UTC(2024, 0, 1, 10) / 1000);
    expect(() =>
      driver.coerceTableEditValue(
        "2024-02-31 10:00",
        column("t", "timestamp", "datetime"),
      ),
    ).toThrow(/Invalid MongoDB timestamp/);
  });
});

describe("MongoDB client-side table filtering", () => {
  it("scans up to the module hard cap", async () => {
    const documents = Array.from({ length: 300 }, (_, index) => ({
      _id: `id-${index}`,
      meta: { n: index },
    }));
    const s = setup(documents);
    const page = await s.driver.readTablePage(
      pageRequest({
        filters: [{ column: "meta", operator: "like", value: '"n":299' }],
      }),
    );
    expect(page.rows.map((row) => row._id)).toEqual(["id-299"]);
    expect(s.limits.at(-1)).toBe(10001);
  });

  it("reports the hard cap when the collection is larger", async () => {
    const documents = Array.from({ length: 10001 }, (_, index) => ({
      _id: `id-${index}`,
      meta: { n: index },
    }));
    const s = setup(documents);
    await expect(
      s.driver.readTablePage(
        pageRequest({
          filters: [{ column: "meta", operator: "like", value: "x" }],
        }),
      ),
    ).rejects.toThrow(/limited to collections of 10000 documents/);
  });

  it("matches like filters only against string values, like $regex", async () => {
    const s = setup([
      { _id: "a", code: "x12", meta: { k: "v" } },
      { _id: "b", code: 123, meta: { k: "v" } },
      { _id: "c", code: ["a12", 5], meta: { k: "v" } },
      { _id: "d", code: new BSONSymbol("s12"), meta: { k: "v" } },
      { _id: "e", code: "nope", meta: { k: "v" } },
    ]);
    const filters: FilterExpression[] = [
      { column: "code", operator: "like", value: "12" },
      { column: "meta", operator: "like", value: "v" },
    ];
    const page = await s.driver.readTablePage(pageRequest({ filters }));
    expect(page.rows.map((row) => row._id)).toEqual(["a", "c", "d"]);
  });

  it("keeps eq filters on JSON columns literal", async () => {
    const s = setup([{ _id: "a", meta: { k: "v" } }]);
    await s.driver.readTablePage(
      pageRequest({
        filters: [{ column: "meta", operator: "eq", value: '{"$ne":null}' }],
      }),
    );
    expect(s.find.mock.calls.at(-1)?.[0]).toEqual({
      meta: { $eq: { $ne: null } },
    });
  });
});

describe("MongoDB table page schema sampling", () => {
  it("passes the page signal and deadline to schema sampling reads", async () => {
    const s = setup([{ _id: "a", name: "x" }]);
    const signal = new AbortController().signal;
    await s.driver.readTablePage(
      pageRequest({ signal, deadline: Date.now() + 60_000 }),
    );
    expect(s.findOptions[0]).toMatchObject({
      signal,
      timeoutMS: expect.any(Number),
    });
    expect(s.listCollectionsOptions[0]).toMatchObject({
      signal,
      timeoutMS: expect.any(Number),
    });
  });

  it("does not start sampling after the page read is aborted", async () => {
    const s = setup([{ _id: "a", name: "x" }]);
    const controller = new AbortController();
    controller.abort(new Error("cancelled"));
    await expect(
      s.driver.readTablePage(pageRequest({ signal: controller.signal })),
    ).rejects.toThrow("cancelled");
    expect(s.find).not.toHaveBeenCalled();
  });
});

describe("MongoDB DBRef and DBPointer values", () => {
  const oid = new ObjectId("507f1f77bcf86cd799439011");

  it("keeps DBRef fields editable", async () => {
    const s = setup([{ _id: "a", ref: new DBRef("other", oid) }]);
    const columns = await s.driver.describeColumns("db", "", "items");
    const ref = columns.find((entry) => entry.name === "ref");
    expect(ref?.nativeType).toBe("dbRef");
    expect(ref?.isReadOnly).not.toBe(true);
    expect(s.driver.checkPersistedEdit(ref as never, "x")).toBeNull();
    const page = await s.driver.readTablePage(pageRequest());
    const hint = page.mongoValueTypes?.[0]?.ref;
    expect(hint?.nativeType).toBe("dbRef");
    expect(() =>
      s.driver.validateTableUpdates([
        {
          changes: { ref: `{"$ref":"other","$id":"${oid.toHexString()}"}` },
          originalTypes: { ref: hint as never },
        },
      ]),
    ).not.toThrow();
    expect(
      s.driver.coerceTableEditValue(
        `{"$ref":"other","$id":"${oid.toHexString()}"}`,
        ref as ColumnTypeMeta,
      ),
    ).toBeInstanceOf(DBRef);
  });

  it("marks only server-confirmed DBPointer fields and cells read-only", async () => {
    const s = setup(
      [
        { _id: "a", ref: new DBRef("db.other", oid) },
        { _id: "b", ref: new DBRef("other", oid) },
      ],
      new Set(["a:ref"]),
    );
    const columns = await s.driver.describeColumns("db", "", "items");
    expect(columns.find((entry) => entry.name === "ref")).toMatchObject({
      nativeType: "dbPointer",
      isReadOnly: true,
    });
    const page = await s.driver.readTablePage(pageRequest());
    const hints = page.mongoValueTypes ?? [];
    expect(hints[0]?.ref?.nativeType).toBe("dbPointer");
    expect(hints[1]?.ref?.nativeType).toBe("dbRef");
    expect(() =>
      s.driver.validateTableUpdates([
        { changes: { ref: "{}" }, originalTypes: { ref: hints[0].ref } },
      ]),
    ).toThrow(/field "ref": it contains deprecated DBPointer values/);
    expect(s.aggregate.mock.calls[0]?.[0]).toEqual([
      { $match: { _id: { $in: ["a", "b"] } } },
      { $project: { t0: { $type: "$ref" } } },
    ]);
  });

  it("passes the page signal to DBPointer detection", async () => {
    const s = setup([{ _id: "a", ref: new DBRef("other", oid) }]);
    const signal = new AbortController().signal;
    await s.driver.readTablePage(
      pageRequest({ signal, deadline: Date.now() + 60_000 }),
    );
    expect(s.aggregateOptions[0]).toMatchObject({
      signal,
      timeoutMS: expect.any(Number),
    });
  });

  it("treats unverifiable DBRef-looking values as DBPointer", async () => {
    const s = setup([{ _id: "a", ref: new DBRef("other", oid) }]);
    s.aggregate.mockImplementation(() => {
      throw new Error("not supported");
    });
    const columns = await s.driver.describeColumns("db", "", "items");
    expect(columns.find((entry) => entry.name === "ref")).toMatchObject({
      nativeType: "dbPointer",
      isReadOnly: true,
    });
  });

  it("propagates DBPointer detection timeouts", async () => {
    const s = setup([{ _id: "a", ref: new DBRef("other", oid) }]);
    s.aggregate.mockImplementation(() => {
      throw new MongoOperationTimeoutError("timed out");
    });
    await expect(s.driver.readTablePage(pageRequest())).rejects.toBeInstanceOf(
      MongoOperationTimeoutError,
    );
  });
});

describe("MongoDB view detection during page reads", () => {
  it("propagates listCollections timeouts instead of assuming a collection", async () => {
    const s = setup([{ _id: "a", name: "x" }]);
    s.failListCollections(new MongoOperationTimeoutError("timed out"));
    await expect(s.driver.readTablePage(pageRequest())).rejects.toBeInstanceOf(
      MongoOperationTimeoutError,
    );
  });

  it("propagates listCollections aborts", async () => {
    const s = setup([{ _id: "a", name: "x" }]);
    const controller = new AbortController();
    s.failListCollections(() => {
      controller.abort(new Error("aborted"));
      throw new Error("listCollections interrupted");
    });
    await expect(
      s.driver.readTablePage(pageRequest({ signal: controller.signal })),
    ).rejects.toThrow("listCollections interrupted");
  });

  it("still falls back to a collection for other listCollections errors", async () => {
    const s = setup([{ _id: "a", name: "x" }]);
    s.markAsView();
    expect(
      (await s.driver.describeColumns("db", "", "items")).find(
        (entry) => entry.name === "_id",
      )?.isPrimaryKey,
    ).toBe(false);
    s.failListCollections(new Error("listCollections not supported"));
    const columns = await s.driver.describeColumns("db", "", "items");
    expect(columns.find((entry) => entry.name === "_id")?.isPrimaryKey).toBe(
      true,
    );
  });
});

describe("MongoDB date filters", () => {
  it("normalizes display dates into Date criteria", async () => {
    const s = setup([{ _id: "a", at: new Date("2024-01-01T12:00:00Z") }]);
    await s.driver.readTablePage(
      pageRequest({
        filters: [{ column: "at", operator: "gt", value: "2024-01-01 10:00" }],
      }),
    );
    const criteria = s.find.mock.calls.at(-1)?.[0] as {
      at: { $gt: Date };
    };
    expect(criteria.at.$gt).toBeInstanceOf(Date);
    expect(criteria.at.$gt.toISOString()).toBe("2024-01-01T10:00:00.000Z");
  });

  it.each([
    "2024-01",
    "2024/01/01",
    "2024-02-31",
  ])("rejects the unparseable date filter %j", async (value) => {
    const s = setup([{ _id: "a", at: new Date("2024-01-01T12:00:00Z") }]);
    await expect(
      s.driver.readTablePage(
        pageRequest({ filters: [{ column: "at", operator: "gt", value }] }),
      ),
    ).rejects.toThrow(/Invalid MongoDB date filter value/);
  });
});

describe("MongoDB dates outside four-digit years", () => {
  it.each([
    ["0999-03-04 05:06:07", "0999-03-04T05:06:07.000Z"],
    ["0000-01-01 00:00:00", "0000-01-01T00:00:00.000Z"],
    ["+010000-01-01 00:00:00", "+010000-01-01T00:00:00.000Z"],
    ["-000044-03-15 12:00:00", "-000044-03-15T12:00:00.000Z"],
  ])("round-trips %j", async (display, iso) => {
    const s = setup([{ _id: "a", at: new Date(iso) }]);
    const page = await s.driver.readTablePage(pageRequest());
    expect(page.rows[0].at).toBe(display);
    const date = column("at", "date", "datetime");
    const original = s.driver.coerceOriginalValue(display, date);
    expect((original as Date).toISOString()).toBe(iso);
  });
});

describe("MongoDB insert validation", () => {
  it.each([
    "a.b",
    "$x",
    "",
  ])("rejects the unsupported field name %j", async (name) => {
    const s = setup([{ _id: "a", plain: "x" }]);
    const columns = await s.driver.describeColumns("db", "", "items");
    const service = new TableMutationService(s.manager as never, {
      getColumns: async () => columns,
    });
    const insert = service.prepareInsertRow("g4", "db", "", "items", {
      plain: "y",
      [name]: "z",
    });
    await expect(insert).rejects.toBeInstanceOf(PersistedEditValidationError);
    await expect(insert).rejects.toThrow(/literal field .*read-only/);
  });

  it("rejects unparseable dates and keeps quoted strings", async () => {
    const s = setup([{ _id: "a", at: new Date("2024-01-01T00:00:00Z") }]);
    const columns = await s.driver.describeColumns("db", "", "items");
    const service = new TableMutationService(s.manager as never, {
      getColumns: async () => columns,
    });
    await expect(
      service.prepareInsertRow("g4", "db", "", "items", { at: "2024/01/01" }),
    ).rejects.toThrow(/Invalid MongoDB date value "2024\/01\/01"/);
    const plan = await service.prepareInsertRow("g4", "db", "", "items", {
      at: '"2024/01/01"',
    });
    expect(plan.values).toEqual({ at: "2024/01/01" });
  });
});

describe("MongoDB SSH remote target", () => {
  it.each([
    { connectionUri: "mongodb://h1:27017,h2:27017/db" },
    { connectionUri: "mongodb://u:p@h1,h2/db?replicaSet=rs" },
    { connectionUri: "mongodb+srv://cluster.example.com/db" },
    {
      mongoConnectionMode: "fields" as const,
      host: "h1,h2",
      mongoScheme: "mongodb" as const,
    },
    {
      mongoConnectionMode: "fields" as const,
      host: "cluster.example.com",
      mongoScheme: "mongodb+srv" as const,
    },
  ])("rejects unsupported tunnel targets %j", (config) => {
    expect(() =>
      resolveMongoRemoteTarget({ type: "mongodb", ...config }),
    ).toThrow(
      "SSH tunnel requires a single MongoDB host; multi-host and mongodb+srv URIs are not supported.",
    );
  });

  it("keeps single-host targets", () => {
    expect(
      resolveMongoRemoteTarget({
        type: "mongodb",
        connectionUri: "mongodb://u:p,x@db.internal:27018/app",
      }),
    ).toEqual({ host: "db.internal", port: 27018 });
  });
});

describe("MongoDB fields outside the mutation schema sample", () => {
  const documents = [
    ...Array.from({ length: 50 }, (_, index) => ({
      _id: `id-${index}`,
      plain: "old",
    })),
    { _id: "late-doc", plain: "old", late: "old" },
  ];

  it("shows page-only sampled fields as read-only", async () => {
    const s = setup(documents);
    const page = await s.driver.readTablePage(pageRequest());
    expect(page.columns.find((entry) => entry.name === "late")).toMatchObject({
      isReadOnly: true,
    });
    expect(
      page.columns.find((entry) => entry.name === "plain")?.isReadOnly,
    ).not.toBe(true);
  });

  it("rejects updates of unknown fields instead of reporting success", async () => {
    const s = setup(documents);
    const columns = await s.driver.describeColumns("db", "", "items");
    const result = await applyChangesTransactional(
      s.manager as never,
      "g4",
      "db",
      "",
      "items",
      [
        {
          primaryKeys: {
            _id: { $rapidbMongoId: { type: "string", value: "a" } },
          },
          changes: { late: "new" },
        },
      ],
      columns,
    );
    expect(result).toMatchObject({
      success: false,
      rowOutcomes: [
        {
          status: "prevalidation_failed",
          message: expect.stringContaining('unknown column "late"'),
        },
      ],
    });
    expect(s.updateOne).not.toHaveBeenCalled();
  });

  it("rejects inserts of unknown fields instead of dropping them", async () => {
    const s = setup(documents);
    const columns = await s.driver.describeColumns("db", "", "items");
    const service = new TableMutationService(s.manager as never, {
      getColumns: async () => columns,
    });
    const insert = service.prepareInsertRow("g4", "db", "", "items", {
      plain: "x",
      late: "y",
    });
    await expect(insert).rejects.toBeInstanceOf(PersistedEditValidationError);
    await expect(insert).rejects.toThrow('unknown column "late"');
    expect(s.insertOne).not.toHaveBeenCalled();
  });
});
