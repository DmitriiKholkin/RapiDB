import {
  Binary,
  BSONRegExp,
  BSONSymbol,
  Code,
  DBRef,
  Decimal128,
  Int32,
  Long,
  MaxKey,
  MinKey,
  ObjectId,
  Timestamp,
} from "mongodb";
import { describe, expect, it, vi } from "vitest";
import { MongoDBDriver } from "../../src/extension/dbDrivers/mongodb";
import type { FilterExpression } from "../../src/shared/tableTypes";
import { NULL_SENTINEL } from "../../src/shared/tableTypes";

type Row = Record<string, unknown> & { _id: string };

type FilterSpec = {
  value?: string | [string, string];
  expectedIds: string[];
};

const rows: Row[] = [
  {
    _id: "507f1f77bcf86cd799439011",
    txt: "alpha",
    intNum: 2,
    floatNum: 2.5,
    boolVal: true,
    dateVal: "2026-01-02",
    timeVal: "10:20:30",
    dateTimeVal: "2026-01-02T10:20:30Z",
    uuidVal: "123e4567-e89b-12d3-a456-426614174000",
    jsonVal: '{"k":"one"}',
    arrayVal: "[1,2,3]",
  },
  {
    _id: "507f1f77bcf86cd799439012",
    txt: "beta",
    intNum: 10,
    floatNum: 10.75,
    boolVal: false,
    dateVal: "2026-02-03",
    timeVal: "16:45:00",
    dateTimeVal: "2026-02-03T16:45:00Z",
    uuidVal: "123e4567-e89b-12d3-a456-426614174001",
    jsonVal: '{"k":"two"}',
    arrayVal: "[4,5,6]",
  },
  {
    _id: "507f1f77bcf86cd799439013",
    txt: null,
    intNum: null,
    floatNum: null,
    boolVal: null,
    dateVal: null,
    timeVal: null,
    dateTimeVal: null,
    uuidVal: null,
    jsonVal: null,
    arrayVal: null,
  },
];

function createDriverWithRows(sampleRows: Row[]): MongoDBDriver {
  const driver = new MongoDBDriver({
    id: "mongodb-filter-coverage",
    type: "mongodb",
    name: "mongo-filter-test",
    host: "localhost",
    port: 27017,
    database: "test",
  });

  const readRowsMock = vi.fn().mockResolvedValue(sampleRows);
  (driver as unknown as { readRows: typeof readRowsMock }).readRows =
    readRowsMock;
  return driver;
}

async function applyFilter(
  driver: MongoDBDriver,
  filter: FilterExpression,
): Promise<string[]> {
  const page = await driver.readTablePage({
    database: "test",
    schema: "test",
    table: "users",
    page: 1,
    pageSize: 100,
    filters: [filter],
    sort: null,
    skipCount: false,
  });
  return page.rows.map((row) => String(row._id));
}

describe("MongoDBDriver readTablePage filter coverage", () => {
  it("supports every advertised operator for each inferred MongoDB column type", async () => {
    const driver = createDriverWithRows(rows);
    const firstPage = await driver.readTablePage({
      database: "test",
      schema: "test",
      table: "users",
      page: 1,
      pageSize: 100,
      filters: [],
      sort: null,
      skipCount: false,
    });

    const columnsByName = new Map(
      firstPage.columns.map((column) => [column.name, column]),
    );

    const specsByColumn: Record<string, Record<string, FilterSpec>> = {
      txt: {
        like: { value: "alp", expectedIds: [rows[0]._id] },
        is_null: { expectedIds: [rows[2]._id] },
        is_not_null: { expectedIds: [rows[0]._id, rows[1]._id] },
      },
      intNum: {
        eq: { value: "10", expectedIds: [rows[1]._id] },
        neq: { value: "10", expectedIds: [rows[0]._id] },
        gt: { value: "2", expectedIds: [rows[1]._id] },
        gte: { value: "10", expectedIds: [rows[1]._id] },
        lt: { value: "10", expectedIds: [rows[0]._id] },
        lte: { value: "2", expectedIds: [rows[0]._id] },
        between: {
          value: ["2", "10"],
          expectedIds: [rows[0]._id, rows[1]._id],
        },
        in: { value: "2,10", expectedIds: [rows[0]._id, rows[1]._id] },
        is_null: { expectedIds: [rows[2]._id] },
        is_not_null: { expectedIds: [rows[0]._id, rows[1]._id] },
      },
      floatNum: {
        eq: { value: "10.75", expectedIds: [rows[1]._id] },
        neq: { value: "10.75", expectedIds: [rows[0]._id] },
        gt: { value: "2.5", expectedIds: [rows[1]._id] },
        gte: { value: "10.75", expectedIds: [rows[1]._id] },
        lt: { value: "10.75", expectedIds: [rows[0]._id] },
        lte: { value: "2.5", expectedIds: [rows[0]._id] },
        between: {
          value: ["2.5", "10.75"],
          expectedIds: [rows[0]._id, rows[1]._id],
        },
        in: { value: "2.5,10.75", expectedIds: [rows[0]._id, rows[1]._id] },
        is_null: { expectedIds: [rows[2]._id] },
        is_not_null: { expectedIds: [rows[0]._id, rows[1]._id] },
      },
      boolVal: {
        eq: { value: "true", expectedIds: [rows[0]._id] },
        neq: { value: "true", expectedIds: [rows[1]._id] },
        is_null: { expectedIds: [rows[2]._id] },
        is_not_null: { expectedIds: [rows[0]._id, rows[1]._id] },
      },
      dateVal: {
        eq: { value: "2026-01-02", expectedIds: [rows[0]._id] },
        neq: { value: "2026-01-02", expectedIds: [rows[1]._id] },
        gt: { value: "2026-01-15", expectedIds: [rows[1]._id] },
        gte: { value: "2026-02-03", expectedIds: [rows[1]._id] },
        lt: { value: "2026-02-03", expectedIds: [rows[0]._id] },
        lte: { value: "2026-01-02", expectedIds: [rows[0]._id] },
        between: {
          value: ["2026-01-01", "2026-01-31"],
          expectedIds: [rows[0]._id],
        },
        in: {
          value: "2026-01-02,2026-02-03",
          expectedIds: [rows[0]._id, rows[1]._id],
        },
        is_null: { expectedIds: [rows[2]._id] },
        is_not_null: { expectedIds: [rows[0]._id, rows[1]._id] },
      },
      timeVal: {
        eq: { value: "10:20:30", expectedIds: [rows[0]._id] },
        neq: { value: "10:20:30", expectedIds: [rows[1]._id] },
        gt: { value: "12:00:00", expectedIds: [rows[1]._id] },
        gte: { value: "16:45:00", expectedIds: [rows[1]._id] },
        lt: { value: "16:45:00", expectedIds: [rows[0]._id] },
        lte: { value: "10:20:30", expectedIds: [rows[0]._id] },
        between: {
          value: ["10:00:00", "12:00:00"],
          expectedIds: [rows[0]._id],
        },
        in: {
          value: "10:20:30,16:45:00",
          expectedIds: [rows[0]._id, rows[1]._id],
        },
        is_null: { expectedIds: [rows[2]._id] },
        is_not_null: { expectedIds: [rows[0]._id, rows[1]._id] },
      },
      dateTimeVal: {
        eq: { value: "2026-01-02T10:20:30Z", expectedIds: [rows[0]._id] },
        neq: { value: "2026-01-02T10:20:30Z", expectedIds: [rows[1]._id] },
        gt: { value: "2026-01-15T00:00:00Z", expectedIds: [rows[1]._id] },
        gte: { value: "2026-02-03T16:45:00Z", expectedIds: [rows[1]._id] },
        lt: { value: "2026-02-03T16:45:00Z", expectedIds: [rows[0]._id] },
        lte: { value: "2026-01-02T10:20:30Z", expectedIds: [rows[0]._id] },
        between: {
          value: ["2026-01-02T00:00:00Z", "2026-01-02T23:59:59Z"],
          expectedIds: [rows[0]._id],
        },
        in: {
          value: "2026-01-02T10:20:30Z,2026-02-03T16:45:00Z",
          expectedIds: [rows[0]._id, rows[1]._id],
        },
        is_null: { expectedIds: [rows[2]._id] },
        is_not_null: { expectedIds: [rows[0]._id, rows[1]._id] },
      },
      uuidVal: {
        like: { value: "4000", expectedIds: [rows[0]._id] },
        in: {
          value:
            "123e4567-e89b-12d3-a456-426614174000,123e4567-e89b-12d3-a456-426614174001",
          expectedIds: [rows[0]._id, rows[1]._id],
        },
        is_null: { expectedIds: [rows[2]._id] },
        is_not_null: { expectedIds: [rows[0]._id, rows[1]._id] },
      },
      jsonVal: {
        like: { value: '"two"', expectedIds: [rows[1]._id] },
        is_null: { expectedIds: [rows[2]._id] },
        is_not_null: { expectedIds: [rows[0]._id, rows[1]._id] },
      },
      arrayVal: {
        like: { value: "[4", expectedIds: [rows[1]._id] },
        is_null: { expectedIds: [rows[2]._id] },
        is_not_null: { expectedIds: [rows[0]._id, rows[1]._id] },
      },
    };

    for (const [columnName, specs] of Object.entries(specsByColumn)) {
      const column = columnsByName.get(columnName);
      expect(column, `column ${columnName} should exist`).toBeDefined();

      const operators = column?.filterOperators ?? [];
      for (const operator of operators) {
        const spec = specs[operator];
        expect(
          spec,
          `missing test spec for ${columnName}.${operator}`,
        ).toBeDefined();

        let filter: FilterExpression;
        if (operator === "between") {
          filter = {
            column: columnName,
            operator,
            value: spec?.value as [string, string],
          };
        } else if (operator === "is_null" || operator === "is_not_null") {
          filter = { column: columnName, operator };
        } else {
          filter = {
            column: columnName,
            operator,
            value: String(spec?.value ?? ""),
          };
        }

        const actualIds = await applyFilter(driver, filter);
        expect(actualIds, `${columnName}.${operator}`).toEqual(
          spec?.expectedIds ?? [],
        );
      }
    }
  });
});

describe("MongoDBDriver schema type inference", () => {
  it("preserves detailed BSON native types for schema metadata", async () => {
    const driver = new MongoDBDriver({
      id: "mongodb-schema-types",
      type: "mongodb",
      name: "mongo-schema-types",
      host: "localhost",
      port: 27017,
      database: "test",
    });

    const readSchemaDocumentsMock = vi.fn().mockResolvedValue([
      {
        _id: new ObjectId("64a1b2c3d4e5f67890abcdef"),
        t_double: Math.PI,
        t_string: "Hello, World!",
        t_object: { nested: { value: 42 } },
        t_array: [1, "two", true],
        t_binary: new Binary(Buffer.from([1, 2, 3]), 0),
        t_binary_uuid: new Binary(
          Buffer.from("00112233445566778899aabbccddeeff", "hex"),
          4,
        ),
        t_dbpointer: new DBRef(
          "users",
          new ObjectId("64a1b2c3d4e5f67890abcdeb"),
          "test",
        ),
        t_objectid: new ObjectId("64a1b2c3d4e5f67890abcdea"),
        t_bool: true,
        t_date: new Date("2024-07-04T12:00:00.000Z"),
        t_null: null,
        t_regex: new BSONRegExp("quick\\s+fox", "i"),
        t_js: new Code("function() { return this.score > 100; }"),
        t_js_scope: new Code("function() { return x; }", { x: 1 }),
        t_symbol: new BSONSymbol("alpha"),
        t_int32: new Int32(2147483647),
        t_int64: Long.fromString("9223372036854775807"),
        t_decimal128: Decimal128.fromString("123456789.987654321"),
        t_timestamp: new Timestamp({ t: 1720094400, i: 1 }),
        t_minkey: new MinKey(),
        t_maxkey: new MaxKey(),
        t_undefined: undefined,
        t_empty_obj: {},
        t_empty_arr: [],
      },
    ]);

    (
      driver as unknown as {
        readSchemaDocuments: typeof readSchemaDocumentsMock;
      }
    ).readSchemaDocuments = readSchemaDocumentsMock;

    const columns = await driver.describeColumns("test", "test", "bson_types");
    const described = await driver.describeTable("test", "test", "bson_types");

    const typeByName = new Map(
      columns.map((column) => [column.name, column.type]),
    );
    const categoryByName = new Map(
      columns.map((column) => [column.name, column.category]),
    );
    const describedTypeByName = new Map(
      described.map((column) => [column.name, column.type]),
    );

    expect(typeByName.get("_id")).toBe("objectId");
    expect(typeByName.get("t_double")).toBe("double");
    expect(typeByName.get("t_string")).toBe("string");
    expect(typeByName.get("t_object")).toBe("object");
    expect(typeByName.get("t_array")).toBe("array");
    expect(typeByName.get("t_binary")).toBe("binData");
    expect(typeByName.get("t_binary_uuid")).toBe("binData");
    expect(typeByName.get("t_dbpointer")).toBe("dbPointer");
    expect(typeByName.get("t_objectid")).toBe("objectId");
    expect(typeByName.get("t_bool")).toBe("bool");
    expect(typeByName.get("t_date")).toBe("date");
    expect(typeByName.get("t_null")).toBe("null");
    expect(typeByName.get("t_regex")).toBe("regex");
    expect(typeByName.get("t_js")).toBe("javascript");
    expect(typeByName.get("t_js_scope")).toBe("javascriptWithScope");
    expect(typeByName.get("t_symbol")).toBe("symbol");
    expect(typeByName.get("t_int32")).toBe("int");
    expect(typeByName.get("t_int64")).toBe("long");
    expect(typeByName.get("t_decimal128")).toBe("decimal");
    expect(typeByName.get("t_timestamp")).toBe("timestamp");
    expect(typeByName.get("t_minkey")).toBe("minKey");
    expect(typeByName.get("t_maxkey")).toBe("maxKey");
    expect(typeByName.get("t_undefined")).toBe("undefined");
    expect(typeByName.get("t_empty_obj")).toBe("object");
    expect(typeByName.get("t_empty_arr")).toBe("array");

    expect(categoryByName.get("t_object")).toBe("json");
    expect(categoryByName.get("t_array")).toBe("array");
    expect(categoryByName.get("t_binary")).toBe("binary");
    expect(categoryByName.get("t_binary_uuid")).toBe("binary");
    expect(categoryByName.get("t_dbpointer")).toBe("other");
    expect(categoryByName.get("t_js_scope")).toBe("other");
    expect(categoryByName.get("t_symbol")).toBe("text");
    expect(categoryByName.get("t_decimal128")).toBe("decimal");
    expect(categoryByName.get("t_timestamp")).toBe("datetime");

    expect(
      columns.find((column) => column.name === "t_binary_uuid")?.bsonSubtype,
    ).toBe(4);

    expect(describedTypeByName).toEqual(typeByName);
  });

  it("keeps table page column types from BSON schema sampling", async () => {
    const driver = new MongoDBDriver({
      id: "mongodb-table-page-types",
      type: "mongodb",
      name: "mongo-table-page-types",
      host: "localhost",
      port: 27017,
      database: "test",
    });

    const readSchemaDocumentsMock = vi.fn().mockResolvedValue([
      {
        _id: new ObjectId("64a1b2c3d4e5f67890abcdef"),
        t_binary: new Binary(Buffer.from([1, 2, 3, 4, 5, 6, 7, 255]), 0),
        t_binary_uuid: new Binary(
          Buffer.from("112233445566778899aabbccddeeffff", "hex"),
          4,
        ),
        t_date: new Date("2024-07-04T12:00:00.000Z"),
        t_decimal128: Decimal128.fromString("123456789.987654321"),
        t_int64: Long.fromString("9223372036854775807"),
        t_timestamp: new Timestamp({ t: 1720094400, i: 1 }),
      },
    ]);
    const readRowsMock = vi.fn().mockResolvedValue([
      {
        _id: "64a1b2c3d4e5f67890abcdef",
        t_binary: "0x01020304050607ff",
        t_binary_uuid: "0x112233445566778899aabbccddeeffff",
        t_date: "2024-07-04 12:00:00",
        t_decimal128: "123456789.987654321",
        t_int64: "9223372036854775807",
        t_timestamp: "2024-07-04 12:00:00",
      },
    ]);

    (
      driver as unknown as {
        readSchemaDocuments: typeof readSchemaDocumentsMock;
        readRows: typeof readRowsMock;
      }
    ).readSchemaDocuments = readSchemaDocumentsMock;
    (
      driver as unknown as {
        readSchemaDocuments: typeof readSchemaDocumentsMock;
        readRows: typeof readRowsMock;
      }
    ).readRows = readRowsMock;

    const page = await driver.readTablePage({
      database: "test",
      schema: "test",
      table: "bson_types",
      page: 1,
      pageSize: 50,
      filters: [],
      sort: null,
      skipCount: false,
    });

    const columnsByName = new Map(
      page.columns.map((column) => [column.name, column]),
    );

    expect(columnsByName.get("t_binary")?.type).toBe("binData");
    expect(columnsByName.get("t_binary")?.category).toBe("binary");
    expect(columnsByName.get("t_binary")?.filterable).toBe(true);
    expect(columnsByName.get("t_binary")?.filterOperators).toEqual([
      "eq",
      "neq",
      "is_null",
      "is_not_null",
    ]);
    expect(columnsByName.get("t_binary_uuid")?.type).toBe("binData");
    expect(columnsByName.get("t_binary_uuid")?.category).toBe("binary");
    expect(columnsByName.get("t_binary_uuid")?.filterable).toBe(true);
    expect(columnsByName.get("t_binary_uuid")?.bsonSubtype).toBe(4);
    expect(columnsByName.get("t_date")?.type).toBe("date");
    expect(columnsByName.get("t_date")?.category).toBe("datetime");
    expect(columnsByName.get("t_decimal128")?.type).toBe("decimal");
    expect(columnsByName.get("t_int64")?.type).toBe("long");
    expect(columnsByName.get("t_timestamp")?.type).toBe("timestamp");
    expect(columnsByName.get("t_timestamp")?.category).toBe("datetime");

    expect(page.rows[0]).toEqual({
      _id: "64a1b2c3d4e5f67890abcdef",
      t_binary: "0x01020304050607ff",
      t_binary_uuid: "0x112233445566778899aabbccddeeffff",
      t_date: "2024-07-04 12:00:00",
      t_decimal128: "123456789.987654321",
      t_int64: "9223372036854775807",
      t_timestamp: "2024-07-04 12:00:00",
    });
  });

  it("filters MongoDB binary columns by their displayed hex value", async () => {
    const driver = new MongoDBDriver({
      id: "mongodb-binary-filter-values",
      type: "mongodb",
      name: "mongo-binary-filter-values",
      host: "localhost",
      port: 27017,
      database: "test",
    });

    const readSchemaDocumentsMock = vi.fn().mockResolvedValue([
      {
        _id: new ObjectId("64a1b2c3d4e5f67890abcdef"),
        t_binary: new Binary(Buffer.from([1, 2, 3]), 0),
      },
    ]);
    const readRowsMock = vi.fn().mockResolvedValue([
      { _id: "64a1b2c3d4e5f67890abcdef", t_binary: "0x010203" },
      { _id: "64a1b2c3d4e5f67890abcdee", t_binary: "0x040506" },
    ]);

    (
      driver as unknown as {
        readSchemaDocuments: typeof readSchemaDocumentsMock;
        readRows: typeof readRowsMock;
      }
    ).readSchemaDocuments = readSchemaDocumentsMock;
    (
      driver as unknown as {
        readSchemaDocuments: typeof readSchemaDocumentsMock;
        readRows: typeof readRowsMock;
      }
    ).readRows = readRowsMock;

    const eqPage = await driver.readTablePage({
      database: "test",
      schema: "test",
      table: "bson_types",
      page: 1,
      pageSize: 50,
      filters: [{ column: "t_binary", operator: "eq", value: "0x010203" }],
      sort: null,
      skipCount: false,
    });
    const neqPage = await driver.readTablePage({
      database: "test",
      schema: "test",
      table: "bson_types",
      page: 1,
      pageSize: 50,
      filters: [{ column: "t_binary", operator: "neq", value: "0x010203" }],
      sort: null,
      skipCount: false,
    });

    expect(eqPage.rows.map((row) => row._id)).toEqual([
      "64a1b2c3d4e5f67890abcdef",
    ]);
    expect(neqPage.rows.map((row) => row._id)).toEqual([
      "64a1b2c3d4e5f67890abcdee",
    ]);
  });

  it("prefers non-null samples for nullable MongoDB fields", async () => {
    const driver = new MongoDBDriver({
      id: "mongodb-nullable-sample-types",
      type: "mongodb",
      name: "mongo-nullable-sample-types",
      host: "localhost",
      port: 27017,
      database: "test",
    });

    const readSchemaDocumentsMock = vi.fn().mockResolvedValue([
      {
        _id: new ObjectId("64a1b2c3d4e5f67890abcdef"),
        notes: null,
      },
      {
        _id: new ObjectId("64a1b2c3d4e5f67890abcdee"),
        notes: "Priority order - handle with care",
      },
    ]);

    (
      driver as unknown as {
        readSchemaDocuments: typeof readSchemaDocumentsMock;
      }
    ).readSchemaDocuments = readSchemaDocumentsMock;

    const columns = await driver.describeColumns("test", "test", "orders");
    const notesColumn = columns.find((column) => column.name === "notes");

    expect(notesColumn?.type).toBe("string");
    expect(notesColumn?.nativeType).toBe("string");
    expect(notesColumn?.category).toBe("text");
    expect(notesColumn?.nullable).toBe(true);
  });

  it("does not mark _id as primary key for MongoDB views", async () => {
    const driver = new MongoDBDriver({
      id: "mongodb-view-schema",
      type: "mongodb",
      name: "mongo-view-schema",
      host: "localhost",
      port: 27017,
      database: "test",
    });

    const readSchemaDocumentsMock = vi.fn().mockResolvedValue([
      {
        _id: new ObjectId("64a1b2c3d4e5f67890abcdef"),
        status: "active",
      },
    ]);

    (
      driver as unknown as {
        readSchemaDocuments: typeof readSchemaDocumentsMock;
      }
    ).readSchemaDocuments = readSchemaDocumentsMock;

    (driver as unknown as { isView: () => Promise<boolean> }).isView = vi
      .fn()
      .mockResolvedValue(true);

    const columns = await driver.describeColumns(
      "test",
      "test",
      "active_users",
    );
    const idColumn = columns.find((column) => column.name === "_id");

    expect(idColumn?.isPrimaryKey).toBe(false);
    expect(idColumn?.primaryKeyOrdinal).toBeUndefined();
  });

  it("preserves MongoDB collection field order with _id forced first", async () => {
    const driver = new MongoDBDriver({
      id: "mongodb-column-order",
      type: "mongodb",
      name: "mongo-column-order",
      host: "localhost",
      port: 27017,
      database: "test",
    });

    const readSchemaDocumentsMock = vi.fn().mockResolvedValue([
      {
        beta: "B",
        _id: new ObjectId("64a1b2c3d4e5f67890abcdef"),
        alpha: "A",
      },
      {
        alpha: "A2",
        gamma: "G",
      },
    ]);

    (
      driver as unknown as {
        readSchemaDocuments: typeof readSchemaDocumentsMock;
      }
    ).readSchemaDocuments = readSchemaDocumentsMock;

    (driver as unknown as { isView: () => Promise<boolean> }).isView = vi
      .fn()
      .mockResolvedValue(false);

    const columns = await driver.describeColumns("test", "test", "events");
    expect(columns.map((column) => column.name)).toEqual([
      "_id",
      "beta",
      "alpha",
      "gamma",
    ]);
  });

  it("normalizes datetime filters against Mongo display values", async () => {
    const driver = createDriverWithRows([
      {
        _id: "507f1f77bcf86cd799439011",
        t_date: "2024-07-04 12:00:00",
        t_timestamp: "2024-07-04 12:00:00",
      },
    ]);

    const readSchemaDocumentsMock = vi.fn().mockResolvedValue([
      {
        _id: new ObjectId("507f1f77bcf86cd799439011"),
        t_date: new Date("2024-07-04T12:00:00.000Z"),
        t_timestamp: new Timestamp({ t: 1720094400, i: 1 }),
      },
    ]);
    (
      driver as unknown as {
        readSchemaDocuments: typeof readSchemaDocumentsMock;
      }
    ).readSchemaDocuments = readSchemaDocumentsMock;

    const dateMatches = await applyFilter(driver, {
      column: "t_date",
      operator: "eq",
      value: "2024-07-04T12:00:00.000Z",
    });
    const timestampMatches = await applyFilter(driver, {
      column: "t_timestamp",
      operator: "eq",
      value: "Timestamp(1720094400, 1)",
    });

    expect(dateMatches).toEqual(["507f1f77bcf86cd799439011"]);
    expect(timestampMatches).toEqual(["507f1f77bcf86cd799439011"]);
  });

  it("coerces Mongo NULL sentinel values to real nulls for preview and persistence", () => {
    const driver = new MongoDBDriver({
      id: "mongodb-null-sentinel",
      type: "mongodb",
      name: "mongo-null-sentinel",
      host: "localhost",
      port: 27017,
      database: "test",
    });

    const column = {
      name: "t_null",
      type: "null",
      nativeType: "null",
      category: "other",
      nullable: true,
      defaultValue: undefined,
      isPrimaryKey: false,
      isForeignKey: false,
      filterable: true,
      filterOperators: ["is_null", "is_not_null"],
      valueSemantics: "plain",
    } as const;

    const coerced = driver.coerceInputValue(NULL_SENTINEL, column);
    const preview = driver.buildMutationPreviewStatement(
      "insert",
      "test",
      "test",
      "bson_types",
      { values: { t_null: coerced } },
    );

    expect(coerced).toBeNull();
    expect(preview).toContain('"t_null": null');
  });

  it("routes Mongo string-search filters for complex BSON displays to client-side filtering", () => {
    const driver = new MongoDBDriver({
      id: "mongodb-client-filter-routing",
      type: "mongodb",
      name: "mongo-client-filter-routing",
      host: "localhost",
      port: 27017,
      database: "test",
    });

    const shouldUseClientSideFiltering = (
      driver as unknown as {
        shouldUseClientSideFiltering: (
          filters: readonly FilterExpression[],
          columns: ReadonlyArray<{
            name: string;
            nativeType: string;
            category: string;
          }>,
        ) => boolean;
      }
    ).shouldUseClientSideFiltering.bind(driver);

    expect(
      shouldUseClientSideFiltering(
        [{ column: "id", operator: "like", value: "64a1b2c3d4e5f67890abcdef" }],
        [{ name: "id", nativeType: "objectId", category: "text" }],
      ),
    ).toBe(true);
    expect(
      shouldUseClientSideFiltering(
        [{ column: "payload", operator: "like", value: "nested" }],
        [{ name: "payload", nativeType: "object", category: "json" }],
      ),
    ).toBe(true);
    expect(
      shouldUseClientSideFiltering(
        [{ column: "items", operator: "like", value: "3" }],
        [{ name: "items", nativeType: "array", category: "array" }],
      ),
    ).toBe(true);
    expect(
      shouldUseClientSideFiltering(
        [{ column: "fn", operator: "like", value: "100" }],
        [{ name: "fn", nativeType: "javascript", category: "other" }],
      ),
    ).toBe(true);
    expect(
      shouldUseClientSideFiltering(
        [{ column: "name", operator: "like", value: "alp" }],
        [{ name: "name", nativeType: "string", category: "text" }],
      ),
    ).toBe(false);
  });
});
