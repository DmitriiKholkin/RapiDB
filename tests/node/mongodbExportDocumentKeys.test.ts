import {
  Binary,
  BSON,
  BSONRegExp,
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
import { describe, expect, it } from "vitest";
import type { ConnectionManager } from "../../src/extension/connectionManager";
import { MongoDBDriver } from "../../src/extension/dbDrivers/mongodb";
import { TableReadService } from "../../src/extension/table/tableReadService";

async function exportedDocument(
  document: Record<string, unknown>,
  rename = false,
  selectedColumns?: readonly string[],
) {
  const bytes = Buffer.from(BSON.serialize(document));
  if (rename) {
    const key = Buffer.from("_userkind\0");
    let offset = bytes.indexOf(key);
    while (offset !== -1) {
      Buffer.from("_bsontype").copy(bytes, offset);
      offset = bytes.indexOf(key, offset + key.length);
    }
  }
  const driver = new MongoDBDriver({
    id: "export-keys",
    name: "Export keys",
    type: "mongodb",
  });
  const find = (_query: unknown, options: BSON.DeserializeOptions) => {
    const cursor = {
      sort: () => cursor,
      skip: () => cursor,
      limit: () => cursor,
      toArray: async () => [BSON.deserialize(bytes, options)],
    };
    return cursor;
  };
  Object.assign(driver, {
    requireDb: () => ({ collection: () => ({ find }) }),
    isView: async () => false,
  });
  const manager = {
    getConnection: () => ({ type: "mongodb" }),
    getDriver: () => driver,
  } as unknown as ConnectionManager;
  const service = new TableReadService(manager);
  const page = await service.getPage(
    "export-keys",
    "qa",
    "qa",
    "items",
    1,
    10,
    [],
    null,
    true,
  );
  const chunks = [];
  for await (const chunk of service.exportAll(
    "export-keys",
    "qa",
    "qa",
    "items",
    10,
    null,
    [],
    undefined,
    selectedColumns,
  ))
    chunks.push(chunk);
  expect(chunks).toHaveLength(1);
  return { page, exported: JSON.parse(JSON.stringify(chunks[0].rows[0])) };
}

describe("MongoDB native export distinguishes document keys from BSON instances", () => {
  it("exports selected fields without inspecting hidden ambiguous fields", async () => {
    const { exported } = await exportedDocument(
      { _id: "ok", payload: { $oid: "507f1f77bcf86cd799439011" } },
      false,
      ["_id"],
    );
    expect(exported).toEqual({ _id: "ok" });
    await expect(
      exportedDocument(
        { _id: "ok", payload: { $oid: "507f1f77bcf86cd799439011" } },
        false,
        ["payload"],
      ),
    ).rejects.toThrow(/Extended JSON/);
  });

  it.each([
    { $oid: "507f1f77bcf86cd799439011" },
    { $date: "2024-01-01T00:00:00Z" },
    { $numberLong: "42" },
  ])("rejects ambiguous literal EJSON document %j", async (payload) => {
    await expect(exportedDocument({ _id: "literal", payload })).rejects.toThrow(
      /cannot preserve the literal document key/,
    );
  });

  it("rejects ambiguous documents inside arrays and Code scopes", async () => {
    await expect(
      exportedDocument({ list: [{ $oid: "literal" }] }),
    ).rejects.toThrow(/Extended JSON/);
    await expect(
      exportedDocument({
        code: new Code("return x", { x: { $oid: "literal" } }),
      }),
    ).rejects.toThrow(/Extended JSON/);
  });

  it.each([
    "customer",
    "Int32",
    17,
    null,
    false,
  ])("preserves a root discriminator-like field %j and exact native values", async (kind) => {
    const { page, exported } = await exportedDocument(
      {
        _id: "root",
        _userkind: kind,
        exact: Long.fromString("9007199254740993"),
        pattern: new BSONRegExp("a.b", "su"),
      },
      true,
    );
    expect(page.rows[0]).toHaveProperty("_bsontype");
    expect(exported).toEqual({
      _id: "root",
      _bsontype: kind === 17 ? { $numberInt: "17" } : kind,
      exact: { $numberLong: "9007199254740993" },
      pattern: { $regularExpression: { pattern: "a.b", options: "su" } },
    });
  });

  it("preserves nested document/array keys and Code scopes with native precision", async () => {
    const { exported } = await exportedDocument(
      {
        _id: "nested",
        payload: {
          _userkind: "customer",
          value: Decimal128.fromString("1.234567890123456789"),
        },
        list: [{ _userkind: "Int32", value: new Int32(42) }],
        code: new Code("return n", {
          _userkind: "scope",
          n: Long.fromString("9007199254740993"),
        }),
      },
      true,
    );
    expect(exported.payload).toEqual({
      _bsontype: "customer",
      value: { $numberDecimal: "1.234567890123456789" },
    });
    expect(exported.list).toEqual([
      { _bsontype: "Int32", value: { $numberInt: "42" } },
    ]);
    expect(exported.code).toEqual({
      $code: "return n",
      $scope: { _bsontype: "scope", n: { $numberLong: "9007199254740993" } },
    });
  });

  it("matches canonical EJSON for native types including nested Code/DBRef", async () => {
    const source = {
      _id: "native",
      oid: new ObjectId("507f1f77bcf86cd799439011"),
      bin: new Binary(Buffer.from([0, 255]), 0),
      n: new Int32(42),
      long: Long.fromString("9007199254740993"),
      double: new Double(-0),
      decimal: Decimal128.fromString("1.2300"),
      regex: new BSONRegExp("a.b", "s"),
      date: new Date("2024-02-29T12:34:56.123Z"),
      timestamp: new Timestamp({ t: 42, i: 2 }),
      min: new MinKey(),
      max: new MaxKey(),
      code: new Code("return n", { n: new Int32(7) }),
      ref: new DBRef("items", new ObjectId("507f1f77bcf86cd799439012"), "qa", {
        extra: Long.fromString("9007199254740993"),
      }),
    };
    const { exported } = await exportedDocument(source);
    expect(exported).toEqual(BSON.EJSON.serialize(source, { relaxed: false }));
  });
});
