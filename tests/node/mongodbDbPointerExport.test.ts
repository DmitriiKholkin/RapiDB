import {
  BSON,
  BSONRegExp,
  BSONType,
  DBRef,
  type Document,
  Long,
  ObjectId,
} from "mongodb";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { MongoDBDriver } from "../../src/extension/dbDrivers/mongodb";
import { colKey, type QueryResult } from "../../src/extension/dbDrivers/types";
import { startMongoWireFixture } from "../support/mongodbWireFixture";

function int32(value: number) {
  const bytes = Buffer.alloc(4);
  bytes.writeInt32LE(value);
  return bytes;
}

function element(type: number, name: string, value: Uint8Array) {
  return Buffer.concat([Buffer.from([type]), Buffer.from(`${name}\0`), value]);
}

function wireDocument(base: Document, ...extra: Buffer[]) {
  const body = BSON.serialize(base).subarray(4, -1);
  const size = 5 + body.length + extra.reduce((sum, e) => sum + e.length, 0);
  return Buffer.concat([int32(size), body, ...extra, Buffer.from([0])]);
}

const oid = new ObjectId("507f1f77bcf86cd799439011");
const ejsonId = { $oid: oid.toHexString() };
const namespace = "catalog.édition";
const code = "return value";
const codeText = Buffer.from(`${code}\0`);
const asDbRef = new DBRef("édition", oid, "catalog");
const dbRefJson = { $ref: "édition", $id: ejsonId, $db: "catalog" };
const dbPointerJson = (ref: string) => ({
  $dbPointer: { $ref: ref, $id: ejsonId },
});

function dbPointer(name: string, ref = namespace) {
  const text = Buffer.from(`${ref}\0`);
  return element(
    BSONType.dbPointer,
    name,
    Buffer.concat([int32(text.length), text, oid.id]),
  );
}

const scope = wireDocument(
  { ref: asDbRef, score: Long.fromString("9007199254740993") },
  dbPointer("pointer"),
);
const scopedCode = Buffer.concat([
  int32(8 + codeText.length + scope.length),
  int32(codeText.length),
  codeText,
  scope,
]);
const fields = [
  dbPointer("pointer"),
  element(
    BSONType.object,
    "nested",
    wireDocument({ ref: asDbRef }, dbPointer("pointer")),
  ),
  element(
    BSONType.array,
    "array",
    wireDocument({ "0": asDbRef }, dbPointer("1", ".edge")),
  ),
  element(BSONType.javascriptWithScope, "scoped", scopedCode),
];
const input = {
  _id: "fixture",
  ref: asDbRef,
  pattern: new BSONRegExp("(?>cat)", "ix"),
  "literal.field": "force client-side sort",
};
const wireRow = wireDocument(input, ...fields);
const expected = {
  _id: input._id,
  ref: dbRefJson,
  pattern: { $regularExpression: { pattern: "(?>cat)", options: "ix" } },
  "literal.field": input["literal.field"],
  pointer: dbPointerJson(namespace),
  nested: { ref: dbRefJson, pointer: dbPointerJson(namespace) },
  array: [dbRefJson, dbPointerJson(".edge")],
  scoped: {
    $code: code,
    $scope: {
      ref: dbRefJson,
      score: { $numberLong: "9007199254740993" },
      pointer: dbPointerJson(namespace),
    },
  },
};

function queryJsonRow(result: QueryResult, index = 0) {
  const row = result.exportRows?.[index];
  const serialize = result.serializeJsonExportRow;
  expect.assert(row);
  expect.assert(serialize);
  return JSON.parse(
    serialize(
      result.columns.flatMap((name, slot) =>
        Object.hasOwn(row, colKey(slot)) ? [[name, row[colKey(slot)]]] : [],
      ),
    ),
  );
}

describe("MongoDB deprecated DBPointer JSON export from wire replies", () => {
  let fixture: Awaited<ReturnType<typeof startMongoWireFixture>>;
  let driver: MongoDBDriver;
  const commands: Document[] = [];

  beforeAll(async () => {
    fixture = await startMongoWireFixture((command) => {
      commands.push(command);
      if (command.findAndModify)
        return wireDocument({ ok: 1, ...input }, ...fields);
      const collection = command.find ?? command.aggregate;
      const project = command.pipeline?.at(-1)?.$project;
      if (command.listCollections || project) {
        return {
          ok: 1,
          cursor: {
            id: Long.ZERO,
            ns: "qa.items",
            firstBatch: project
              ? [
                  {
                    _id: input._id,
                    ...Object.fromEntries(
                      Object.entries(project).map(([name, value]) => [
                        name,
                        (value as Document).$type === "$pointer"
                          ? "dbPointer"
                          : "object",
                      ]),
                    ),
                  },
                ]
              : [{ name: "items", type: "collection" }],
          },
        };
      }
      if (collection === "items" || command.getMore) {
        const hasMore =
          !command.getMore && command.comment === "multiple batches";
        return wireDocument(
          { ok: 1 },
          element(
            BSONType.object,
            "cursor",
            wireDocument(
              { id: hasMore ? Long.ONE : Long.ZERO, ns: "qa.items" },
              element(
                BSONType.array,
                command.getMore ? "nextBatch" : "firstBatch",
                wireDocument({}, element(BSONType.object, "0", wireRow)),
              ),
            ),
          ),
        );
      }
      return { ok: 0, errmsg: "Unexpected DBPointer fixture command" };
    });
    driver = new MongoDBDriver({
      id: "dbpointer-fixture",
      name: "DBPointer fixture",
      type: "mongodb",
      mongoConnectionMode: "uri",
      connectionUri: `mongodb://127.0.0.1:${fixture.port}/qa?directConnection=true`,
    });
    await driver.connect();
  });

  afterAll(async () => {
    try {
      await driver?.disconnect();
    } finally {
      await fixture?.close();
    }
  });

  it("starts from distinct wire types that ordinary BSON decoding conflates", () => {
    const decoded = BSON.deserialize(wireRow, { bsonRegExp: true });
    expect(decoded.pointer).toBeInstanceOf(DBRef);
    expect(decoded.pointer).toEqual(decoded.ref);
    expect(decoded.nested.pointer).toEqual(decoded.nested.ref);
    expect(decoded.scoped.scope.pointer).toEqual(decoded.scoped.scope.ref);
  });

  it("rejects ambiguous JSON if the source has already lost wire provenance", () => {
    const decoded = BSON.deserialize(wireRow, { bsonRegExp: true });
    const row = Reflect.get(driver, "toRow").call(driver, decoded);
    const chunk = { columns: [], rows: [row] };
    expect(() => driver.formatTableExportChunk(chunk, ["pointer"])).toThrow(
      /cannot distinguish.*deprecated BSON DBPointer/,
    );
    expect(
      driver.formatTableExportChunk(chunk, ["pointer"], "csv").rows,
    ).toEqual([{ pointer: dbRefJson }]);
  });

  it.each([
    "find({})",
    "findOne({})",
    "aggregate([])",
  ])("exports BSON pointers and ordinary DBRefs from %s", async (helper) => {
    const result = await driver.query(`db.items.${helper}`);
    expect(queryJsonRow(result)).toEqual(expected);
    // The display/CSV path remains distinct from the BSON JSON serializer.
    const { rows, columns } = result;
    expect(rows[0][colKey(columns.indexOf("pointer"))]).toBe(
      rows[0][colKey(columns.indexOf("ref"))],
    );
  });

  it("retains wire types from subsequent cursor batches", async () => {
    const result = await driver.query(
      'db.items.find({}, {}, { comment: "multiple batches" }).limit(2)',
    );
    expect(queryJsonRow(result, 0)).toEqual(expected);
    expect(queryJsonRow(result, 1)).toEqual(expected);
  });

  it("exports a runCommand cursor reply without submitting another command", async () => {
    const before = commands.length;
    const result = await driver.query('db.runCommand({ find: "items" })');
    expect(queryJsonRow(result).cursor.firstBatch).toEqual([expected]);
    expect(commands.slice(before).map((command) => command.find)).toEqual([
      "items",
    ]);
  });

  it("keeps reference types when combining query and command result rows", async () => {
    const result = await driver.query(
      'db.items.findOne({}); db.runCommand({ find: "items" })',
    );
    expect(queryJsonRow(result, 0)).toEqual(expected);
    expect(queryJsonRow(result, 1).cursor.firstBatch).toEqual([expected]);
  });

  it("exports inline values from a mutation command reply without replaying it", async () => {
    const before = commands.length;
    const result = await driver.query(
      'db.runCommand({ findAndModify: "items", query: {}, update: { $set: { seen: true } } })',
    );
    expect(queryJsonRow(result)).toEqual({
      ok: { $numberInt: "1" },
      ...expected,
    });
    expect(
      commands.slice(before).map((command) => command.findAndModify),
    ).toEqual(["items"]);
  });

  it.each([
    null,
    { column: "literal.field", direction: "asc" } as const,
  ])("exports table rows as canonical JSON while retaining legacy CSV with sort %j", async (sort) => {
    const page = await driver.readTablePage({
      database: "qa",
      schema: "qa",
      table: "items",
      page: 1,
      pageSize: 10,
      filters: [],
      sort,
      skipCount: true,
    });
    expect(page.mongoValueTypes?.[0].pointer.nativeType).toBe("dbPointer");
    expect(page.mongoValueTypes?.[0].ref.nativeType).toBe("dbRef");
    expect(driver.formatTableExportChunk(page).rows).toEqual([expected]);
    expect(driver.formatTableExportChunk(page, ["ref"]).rows).toEqual([
      { ref: dbRefJson },
    ]);
    expect(
      driver.formatTableExportChunk(page, ["pointer", "ref"], "csv").rows,
    ).toEqual([{ pointer: dbRefJson, ref: dbRefJson }]);
  });
});
