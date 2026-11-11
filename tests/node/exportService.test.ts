import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { marshall, NumberValueImpl } from "@aws-sdk/util-dynamodb";
import { errors } from "@elastic/transport";
import {
  Binary,
  BSON,
  BSONRegExp,
  Decimal128,
  Int32,
  Long,
  MaxKey,
  MinKey,
  ObjectId,
  Timestamp,
  UUID,
} from "mongodb";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { MongoDBDriver } from "../../src/extension/dbDrivers/mongodb";
import { MySQLDriver } from "../../src/extension/dbDrivers/mysql";
import type {
  ColumnTypeMeta,
  FilterExpression,
  QueryResult,
} from "../../src/extension/dbDrivers/types";
import type { ChunkedExportData } from "../../src/extension/utils/exportService";
import { formatQueryResult } from "../../src/extension/utils/queryResultFormatting";
import { createElasticsearchHttpFixture } from "../support/elasticsearchHttpFixture";
import {
  createElasticsearchPitMock,
  required,
} from "../support/elasticsearchPitMock";

type VscodeMockShape = {
  showSaveDialog: ReturnType<typeof vi.fn>;
  withProgress: ReturnType<typeof vi.fn>;
  showInformationMessage: ReturnType<typeof vi.fn>;
  showErrorMessage: ReturnType<typeof vi.fn>;
};

function createMongoExportChunk() {
  return {
    columns: [
      { name: "_id", category: "text", nativeType: "objectId" },
      { name: "label", category: "text", nativeType: "string" },
      { name: "t_double", category: "float", nativeType: "double" },
      { name: "t_string", category: "text", nativeType: "string" },
      { name: "t_object", category: "json", nativeType: "object" },
      { name: "t_array", category: "array", nativeType: "array" },
      { name: "t_binary", category: "binary", nativeType: "binData" },
      { name: "t_binary_uuid", category: "binary", nativeType: "binData" },
      { name: "t_objectid", category: "text", nativeType: "objectId" },
      { name: "t_bool_true", category: "boolean", nativeType: "bool" },
      { name: "t_bool_false", category: "boolean", nativeType: "bool" },
      { name: "t_date", category: "datetime", nativeType: "date" },
      { name: "t_null", category: "other", nativeType: "null" },
      { name: "t_regex", category: "other", nativeType: "regex" },
      { name: "t_js", category: "other", nativeType: "javascript" },
      { name: "t_int32", category: "integer", nativeType: "int" },
      { name: "t_int64", category: "integer", nativeType: "long" },
      { name: "t_decimal128", category: "decimal", nativeType: "decimal" },
      { name: "t_timestamp", category: "datetime", nativeType: "timestamp" },
      { name: "t_minkey", category: "other", nativeType: "minKey" },
      { name: "t_maxkey", category: "other", nativeType: "maxKey" },
      { name: "t_undefined_str", category: "text", nativeType: "string" },
      { name: "t_nested_array", category: "array", nativeType: "array" },
      { name: "t_empty_obj", category: "json", nativeType: "object" },
      { name: "t_empty_arr", category: "array", nativeType: "array" },
      { name: "t_long_string", category: "text", nativeType: "string" },
      { name: "t_unicode_keys", category: "json", nativeType: "object" },
    ] as const,
    rows: [
      {
        _id: "6a06f7843c8b7b044f3d8c69",
        label: "All BSON types showcase 1",
        t_double: 3.14159265358971,
        t_string: "Hello, World! Привет мир! 你好世界 🔥😀1",
        t_object: '{"nested":{"deep":{"value":42}},"arr":[1,2,3,1]}',
        t_array: '[1,"two",3,true,null,{"k":"v"},[7,8,9,1]]',
        t_binary: "AQIDBAUGB/0=",
        t_binary_uuid: "ESIzRFVmd4iZqrvM3e1//w==",
        t_objectid: "64a1b2c3d4e5f67890abcdef",
        t_bool_true: null,
        t_bool_false: true,
        t_date: "2024-07-04 12:00:11",
        t_null: null,
        t_regex: "/quick\\s+fox/gi",
        t_js: "function() { return this.score > 111; }",
        t_int32: 2147483641,
        t_int64: "9223372036854775801",
        t_decimal128: "123456789.987654311",
        t_timestamp: "2024-07-04 12:00:11",
        t_minkey: "MinKey()",
        t_maxkey: "MaxKey()",
        t_undefined_str: "N/A (undefined not supported in modern BSON)",
        t_nested_array: "[[1,2],[3,4],[5,null,7,1]]",
        t_empty_obj: "{}",
        t_empty_arr: "[]",
        t_long_string: `${"x".repeat(1000)}1`,
        t_unicode_keys:
          '{"1":"1","ключ1":"значение1","键":"值","مفتاح":"قيمة"}',
      },
    ],
  };
}

describe("exportService", () => {
  let tempDir: string;
  let outputPath: string;
  let vscodeMock: VscodeMockShape;

  beforeEach(() => {
    vi.resetModules();
    tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "rapidb-export-"));
    outputPath = path.join(tempDir, "export.out");

    vscodeMock = {
      showSaveDialog: vi.fn(async () => ({ fsPath: outputPath })),
      withProgress: vi.fn(async (_options, task) =>
        task(
          {},
          {
            onCancellationRequested: vi.fn(() => ({ dispose: vi.fn() })),
          },
        ),
      ),
      showInformationMessage: vi.fn(),
      showErrorMessage: vi.fn(),
    };

    vi.doMock("vscode", () => ({
      Uri: {
        file: (fsPath: string) => ({ fsPath }),
      },
      ProgressLocation: {
        Notification: 15,
      },
      window: vscodeMock,
    }));
  });

  afterEach(() => {
    fs.rmSync(tempDir, { recursive: true, force: true });
  });

  it.each(
    (["timeout", "shards"] as const).flatMap((failure) =>
      (["csv", "json"] as const).flatMap((format) =>
        (["sorted", "filtered"] as const).map((mode) => ({
          failure,
          format,
          mode,
        })),
      ),
    ),
  )("preserves the destination for partial Elasticsearch HTTP results ($failure $format $mode)", async ({
    failure,
    format,
    mode,
  }) => {
    const fixture = await createElasticsearchHttpFixture({
      window: 10000,
      partialSearch: failure,
    });
    try {
      const exporters = await import("../../src/extension/utils/exportService");
      fs.writeFileSync(outputPath, "original file");
      const exporter =
        format === "csv"
          ? exporters.exportTableDataAsCsv
          : exporters.exportTableDataAsJson;
      await exporter({
        fileName: "records",
        loadChunks: (signal) =>
          fixture.service.exportAll(
            "es-http",
            "default",
            "indices",
            "records",
            500,
            mode === "sorted" ? { column: "_id", direction: "asc" } : null,
            mode === "filtered"
              ? [{ column: "_id", operator: "like", value: "doc" }]
              : [],
            signal,
          ),
      });
      expect(fs.readFileSync(outputPath, "utf8")).toBe("original file");
      expect(fs.readdirSync(tempDir)).toEqual(["export.out"]);
      expect(vscodeMock.showInformationMessage).not.toHaveBeenCalled();
      expect(vscodeMock.showErrorMessage).toHaveBeenCalledWith(
        expect.stringMatching(/incomplete results/i),
      );
      expect(fixture.activeReaders).toBe(0);
    } finally {
      await fixture.close();
    }
  });

  it.each([
    "csv",
    "json",
  ] as const)("exports exact filtered/sorted rows across a small Elasticsearch window (%s)", async (format) => {
    const fixture = await createElasticsearchHttpFixture({ window: 3 });
    try {
      const exporters = await import("../../src/extension/utils/exportService");
      const exporter =
        format === "csv"
          ? exporters.exportTableDataAsCsv
          : exporters.exportTableDataAsJson;
      await exporter({
        fileName: "records",
        loadChunks: (signal) =>
          fixture.service.exportAll(
            "es-http",
            "default",
            "indices",
            "records",
            2,
            { column: "_id", direction: "desc" },
            [{ column: "_id", operator: "like", value: "doc-1" }],
            signal,
          ),
      });
      const output = fs.readFileSync(outputPath, "utf8");
      if (format === "json") {
        expect(
          JSON.parse(output).map((row: { _id: string }) => row._id),
        ).toEqual([
          "doc-16",
          "doc-15",
          "doc-14",
          "doc-13",
          "doc-12",
          "doc-11",
          "doc-10",
          "doc-1",
        ]);
      } else {
        expect(
          output
            .trim()
            .split(/\r?\n/)
            .slice(1)
            .map((line) => line.split(",")[0]),
        ).toEqual([
          "doc-16",
          "doc-15",
          "doc-14",
          "doc-13",
          "doc-12",
          "doc-11",
          "doc-10",
          "doc-1",
        ]);
      }
      expect(vscodeMock.showErrorMessage).not.toHaveBeenCalled();
      expect(vscodeMock.showInformationMessage).toHaveBeenCalledOnce();
      expect(fixture.pits.size).toBe(0);
      expect(fixture.searches.length).toBeGreaterThan(5);
    } finally {
      await fixture.close();
    }
  });

  it.each([
    "csv",
    "json",
  ] as const)("preserves the destination for an over-cap filtered Elasticsearch snapshot with a small window (%s)", async (format) => {
    const { service, client, snapshots } = createElasticsearchPitMock(10001, 3);
    const exporters = await import("../../src/extension/utils/exportService");
    const exporter =
      format === "csv"
        ? exporters.exportTableDataAsCsv
        : exporters.exportTableDataAsJson;
    fs.writeFileSync(outputPath, "original file");
    await exporter({
      fileName: "records",
      loadChunks: (signal) =>
        service.exportAll(
          "es-pit",
          "default",
          "indices",
          "records",
          2,
          null,
          [{ column: "_id", operator: "like", value: "doc-10000" }],
          signal,
        ),
    });
    expect(fs.readFileSync(outputPath, "utf8")).toBe("original file");
    expect(fs.readdirSync(tempDir)).toEqual(["export.out"]);
    expect(vscodeMock.showInformationMessage).not.toHaveBeenCalled();
    expect(vscodeMock.showErrorMessage).toHaveBeenCalledWith(
      expect.stringContaining("10000-row safety limit"),
    );
    expect(client.closePointInTime).toHaveBeenCalledOnce();
    expect(snapshots.size).toBe(0);
  });

  it.each([
    "csv",
    "json",
  ] as const)("preserves the destination when the export connection changes (%s)", async (format) => {
    const { TableReadService } = await import(
      "../../src/extension/table/tableReadService"
    );
    const exporters = await import("../../src/extension/utils/exportService");
    const columns = [
      { name: "id", category: "text" as const, nativeType: "text" },
    ];
    const readTablePage = vi.fn(async () => ({
      columns,
      rows: [{ id: "server-a" }],
      totalCount: 2,
    }));
    let driver = { readTablePage };
    const service = new TableReadService({
      getConnection: () => ({ id: "c1" }),
      getDriver: () => driver,
    } as never);
    fs.writeFileSync(outputPath, "original file");
    const exporter =
      format === "csv"
        ? exporters.exportTableDataAsCsv
        : exporters.exportTableDataAsJson;
    await exporter({
      fileName: "records",
      loadChunks: async function* (signal) {
        for await (const chunk of service.exportAll(
          "c1",
          "db",
          "public",
          "records",
          1,
          null,
          [],
          signal,
        )) {
          yield chunk;
          driver = { readTablePage: vi.fn() };
        }
      },
    });
    expect(fs.readFileSync(outputPath, "utf8")).toBe("original file");
    expect(fs.readdirSync(tempDir)).toEqual(["export.out"]);
    expect(vscodeMock.showInformationMessage).not.toHaveBeenCalled();
    expect(vscodeMock.showErrorMessage).toHaveBeenCalledWith(
      expect.stringMatching(/connection changed/i),
    );
    expect(driver.readTablePage).not.toHaveBeenCalled();
  });

  it.each(
    (["map", "list"] as const).flatMap((sampleType) =>
      (["cursor", "materialized", "client-filter"] as const).flatMap(
        (readPath) =>
          (["csv", "json"] as const).map((format) => ({
            sampleType,
            readPath,
            format,
          })),
      ),
    ),
  )("exports unwrapped string values with text metadata through $readPath $format after sampling $sampleType", async ({
    sampleType,
    readPath,
    format,
  }) => {
    const { DynamoDBDriver } = await import(
      "../../src/extension/dbDrivers/dynamodb"
    );
    const { TableReadService } = await import(
      "../../src/extension/table/tableReadService"
    );
    const exporters = await import("../../src/extension/utils/exportService");
    const driver = new DynamoDBDriver({
      id: "ddb-type-change",
      name: "Dynamo",
      type: "dynamodb",
      awsRegion: "us-east-1",
    });
    const send = vi.fn(async (command: { input: Record<string, unknown> }) => {
      switch (command.constructor.name) {
        case "DescribeTableCommand":
          return {
            Table: {
              KeySchema: [
                { AttributeName: "pk", KeyType: "HASH" },
                { AttributeName: "sk", KeyType: "RANGE" },
              ],
              AttributeDefinitions: [
                { AttributeName: "pk", AttributeType: "S" },
                { AttributeName: "sk", AttributeType: "S" },
              ],
            },
          };
        case "ScanCommand":
          return {
            Items: [
              marshall({
                pk: "A",
                sk: "0000",
                value: sampleType === "map" ? { sampled: true } : ["sampled"],
              }),
            ],
          };
        case "QueryCommand": {
          const cursor = command.input.ExclusiveStartKey as
            | Record<string, { S?: string }>
            | undefined;
          const remaining = ["0001", "0002"].filter(
            (sk) => sk > (cursor?.sk?.S ?? ""),
          );
          const selected = remaining.slice(
            0,
            Number(command.input.Limit ?? remaining.length),
          );
          return {
            Items: selected.map((sk) =>
              marshall({ pk: "B", sk, value: "hello" }),
            ),
            ...(selected.length < remaining.length
              ? {
                  LastEvaluatedKey: marshall({
                    pk: "B",
                    sk: selected.at(-1),
                  }),
                }
              : {}),
          };
        }
        default:
          throw new Error(`Unexpected ${command.constructor.name}`);
      }
    });
    Object.assign(driver, { connected: true, client: { send } });
    const described = await driver.describeColumns(
      "us-east-1",
      "",
      "type_change",
    );
    expect(described.find((column) => column.name === "value")).toMatchObject({
      nativeType: sampleType,
      category: sampleType === "map" ? "json" : "array",
    });
    const service = new TableReadService({
      getConnection: () => ({ type: "dynamodb" }),
      getDriver: () => driver,
    } as never);
    const filters: FilterExpression[] = [
      { column: "pk", operator: "eq", value: "B" },
    ];
    if (readPath === "client-filter")
      filters.push({ column: "value", operator: "is_not_null" });
    const sort = {
      column: readPath === "materialized" ? "value" : "sk",
      direction: "asc" as const,
    };
    const chunks: ChunkedExportData[] = [];
    const exporter =
      format === "csv"
        ? exporters.exportTableDataAsCsv
        : exporters.exportTableDataAsJson;
    await exporter({
      fileName: "type_change",
      loadChunks: async function* (signal) {
        for await (const chunk of service.exportAll(
          "ddb-type-change",
          "us-east-1",
          "",
          "type_change",
          1,
          sort,
          filters,
          signal,
        )) {
          chunks.push(chunk);
          yield chunk;
        }
      },
    });
    expect(vscodeMock.showErrorMessage).not.toHaveBeenCalled();
    expect(vscodeMock.showInformationMessage).toHaveBeenCalledOnce();
    const output = fs.readFileSync(outputPath, "utf8");
    if (format === "csv")
      expect(output).toBe(
        'pk,sk,value\n"{""S"":""B""}","{""S"":""0001""}","{""S"":""hello""}"\n"{""S"":""B""}","{""S"":""0002""}","{""S"":""hello""}"\n',
      );
    else
      expect(JSON.parse(output)).toEqual([
        { pk: { S: "B" }, sk: { S: "0001" }, value: { S: "hello" } },
        { pk: { S: "B" }, sk: { S: "0002" }, value: { S: "hello" } },
      ]);
    expect(chunks).toHaveLength(2);
    for (const chunk of chunks) {
      expect(
        chunk.columns.find((column) => column.name === "value"),
      ).toMatchObject({ nativeType: "string", category: "text" });
      expect(chunk.rows[0]?.value).toEqual({ S: "hello" });
    }
    expect(
      send.mock.calls.some(
        ([command]) => command.constructor.name === "QueryCommand",
      ),
    ).toBe(true);
  });

  it.each([
    "csv",
    "json",
  ] as const)("exports DynamoDB Table Viewer values losslessly through TableReadService (%s)", async (format) => {
    const { DynamoDBDriver } = await import(
      "../../src/extension/dbDrivers/dynamodb"
    );
    const { TableReadService } = await import(
      "../../src/extension/table/tableReadService"
    );
    const exporters = await import("../../src/extension/utils/exportService");
    const driver = new DynamoDBDriver({
      id: "ddb-native-export",
      name: "Dynamo export",
      type: "dynamodb",
      awsRegion: "us-east-1",
    });
    const items = [
      {
        pk: "item-1",
        exact: NumberValueImpl.from("9007199254740993.1250"),
        binary: Buffer.from([1, 2, 3]),
        stringSet: new Set(["alpha", "beta"]),
        numberSet: new Set([
          NumberValueImpl.from("2.00"),
          NumberValueImpl.from("9007199254740993"),
        ]),
        binarySet: new Set([Buffer.from([1, 2, 3]), Buffer.from([4, 5, 6])]),
      },
      { pk: "item-2", nullValue: null },
    ];
    const send = vi.fn(async (command: { constructor: { name: string } }) => {
      if (command.constructor.name === "DescribeTableCommand") {
        return {
          Table: {
            KeySchema: [{ AttributeName: "pk", KeyType: "HASH" }],
            AttributeDefinitions: [{ AttributeName: "pk", AttributeType: "S" }],
          },
        };
      }
      if (command.constructor.name === "ScanCommand") {
        return { Items: items.map((item) => marshall(item)) };
      }
      throw new Error(`Unexpected ${command.constructor.name}`);
    });
    Object.assign(driver, { connected: true, client: { send } });
    const service = new TableReadService({
      getConnection: () => ({ type: "dynamodb" }),
      getDriver: () => driver,
    } as never);
    const exporter =
      format === "csv"
        ? exporters.exportTableDataAsCsv
        : exporters.exportTableDataAsJson;

    await exporter({
      fileName: "dynamodb_native_table_export",
      loadChunks: (signal) =>
        service.exportAll(
          "ddb-native-export",
          "us-east-1",
          "us-east-1",
          "users",
          500,
          null,
          [],
          signal,
        ),
    });

    const output = fs.readFileSync(outputPath, "utf8");
    if (format === "json") {
      expect(JSON.parse(output)).toEqual([
        {
          pk: { S: "item-1" },
          exact: { N: "9007199254740993.1250" },
          binary: { B: "AQID" },
          stringSet: { SS: ["alpha", "beta"] },
          numberSet: { NS: ["2.00", "9007199254740993"] },
          binarySet: { BS: ["AQID", "BAUG"] },
        },
        { pk: { S: "item-2" }, nullValue: { NULL: true } },
      ]);
      return;
    }

    expect(output).toContain('""N"":""9007199254740993.1250""');
    expect(output).toContain('""B"":""AQID""');
    expect(output).toContain('""SS"":[""alpha"",""beta""]');
    expect(output).toContain('""NS"":[""2.00"",""9007199254740993""]');
    expect(output).toContain('""BS"":[""AQID"",""BAUG""]');
    expect(output).toContain("MISSING");
    expect(output).toContain('""NULL"":true');
  });

  it("exports MongoDB table JSON with numeric and structured types preserved", async () => {
    const { exportTableDataAsJson } = await import(
      "../../src/extension/utils/exportService"
    );
    const chunk = createMongoExportChunk();

    await exportTableDataAsJson({
      fileName: "bson_types",
      loadChunks: async function* () {
        yield chunk;
      },
    });

    const output = fs.readFileSync(outputPath, "utf8");

    expect(output).toContain(
      '"t_object":{"nested":{"deep":{"value":42}},"arr":[1,2,3,1]}',
    );
    expect(output).toContain(
      '"t_array":[1,"two",3,true,null,{"k":"v"},[7,8,9,1]]',
    );
    expect(output).toContain('"t_nested_array":[[1,2],[3,4],[5,null,7,1]]');
    expect(output).toContain('"t_empty_obj":{}');
    expect(output).toContain('"t_empty_arr":[]');
    expect(output).toContain(
      '"t_unicode_keys":{"1":"1","ключ1":"значение1","键":"值","مفتاح":"قيمة"}',
    );
    expect(output).toContain('"t_int64":9223372036854775801');
    expect(output).not.toContain('"t_int64":"9223372036854775801"');
    expect(output).toContain('"t_decimal128":"123456789.987654311"');
  });

  it("exports MongoDB table JSON as lossless BSON Extended JSON", async () => {
    const { exportTableDataAsJson } = await import(
      "../../src/extension/utils/exportService"
    );
    const driver = new MongoDBDriver({
      id: "mongodb-ejson-export",
      type: "mongodb",
      name: "mongo-ejson-export",
      host: "localhost",
      port: 27017,
      database: "test",
    });
    const document = {
      _id: new ObjectId("64a1b2c3d4e5f67890abcdef"),
      decimal: Decimal128.fromString("12345678901234567890.123456789"),
      int32: new Int32(2_147_483_647),
      int64: Long.fromString("9223372036854775807"),
      binary: new Binary(Buffer.from([0x00, 0xff, 0x64]), 0),
      uuid: new UUID("00112233445566778899aabbccddeeff"),
      date: new Date("2024-07-04T12:00:11.123Z"),
      regex: new BSONRegExp("quick\\s+fox", "i"),
      timestamp: new Timestamp({ t: 1_720_094_400, i: 1 }),
      min: new MinKey(),
      max: new MaxKey(),
      nested: {
        decimal: Decimal128.fromString("0.1234567890123456789012345678"),
        values: [new ObjectId("64a1b2c3d4e5f67890abcdea")],
      },
    };
    const displayRow = (
      driver as unknown as {
        toRow(value: Record<string, unknown>): Record<string, unknown>;
      }
    ).toRow(document);
    const columns: ColumnTypeMeta[] = Object.keys(document).map((name) => ({
      name,
      type: "bson",
      category: "other",
      nativeType: "bson",
      nullable: true,
      isPrimaryKey: name === "_id",
      isForeignKey: false,
      filterable: true,
      filterOperators: [],
      valueSemantics: "plain",
    }));
    const chunk = driver.formatTableExportChunk({
      columns,
      rows: [displayRow],
    });

    expect(displayRow.decimal).toBe("12345678901234567890.123456789");
    expect(displayRow.binary).toBe("0x00ff64");

    await exportTableDataAsJson({
      fileName: "bson_types",
      loadChunks: async function* () {
        yield chunk;
      },
    });

    const output = fs.readFileSync(outputPath, "utf8");
    expect(JSON.parse(output)).toEqual([
      BSON.EJSON.serialize(document, { relaxed: false }),
    ]);
    expect(output).toContain(
      '"$numberDecimal":"12345678901234567890.123456789"',
    );
    expect(output).toContain('"$binary":{"base64":"AP9k","subType":"00"}');
    expect(output).toContain('"$oid":"64a1b2c3d4e5f67890abcdef"');
    expect(output).toContain(
      '"$regularExpression":{"pattern":"quick\\\\s+fox","options":"i"}',
    );
    expect(output).toContain('"$timestamp":{"t":1720094400,"i":1}');
    expect(output).toContain('"$minKey":1');
    expect(output).toContain('"$maxKey":1');
  });

  it("preserves canonical EJSON user marker keys and heterogeneous scalar values without SQL wrappers", async () => {
    const { exportTableDataAsJson } = await import(
      "../../src/extension/utils/exportService"
    );
    const driver = new MongoDBDriver({
      id: "mongo-marker-export",
      name: "Mongo QA",
      type: "mongodb",
    });
    const document = {
      _id: "one",
      payload: {
        $rapiDB: "business-data",
        nested: { $rapiDB: { type: "json-value", value: null } },
      },
      items: [{ $rapiDB: "array-user-data" }, null, "123"],
      jsonLooking: "null",
      numericLooking: "9007199254740993",
      booleanLooking: "true",
      realNull: null,
      realNumber: new Int32(2),
    };
    const row = (
      driver as unknown as {
        toRow(value: Record<string, unknown>): Record<string, unknown>;
      }
    ).toRow(document);
    const columns: ColumnTypeMeta[] = Object.keys(document).map((name) => ({
      name,
      type: "object",
      nativeType: "object",
      category:
        name === "numericLooking"
          ? "decimal"
          : name === "booleanLooking"
            ? "boolean"
            : "json",
      nullable: true,
      isPrimaryKey: name === "_id",
      isForeignKey: false,
      filterable: true,
      filterOperators: [],
      valueSemantics: "plain",
    }));
    const chunk = driver.formatTableExportChunk({ columns, rows: [row] });
    expect(
      chunk.columns.every((column) => column.exportCategory === "other"),
    ).toBe(true);
    expect(columns.every((column) => column.exportCategory === undefined)).toBe(
      true,
    );
    await exportTableDataAsJson({
      fileName: "mongo_marker_qa",
      loadChunks: async function* () {
        yield chunk;
      },
    });
    const output = fs.readFileSync(outputPath, "utf8");
    expect(JSON.parse(output)).toEqual([
      BSON.EJSON.serialize(document, { relaxed: false }),
    ]);
    expect(BSON.EJSON.parse(output, { relaxed: false })).toEqual([document]);
    expect(JSON.parse(output)[0].payload.$rapiDB).toBe("business-data");
    expect(JSON.parse(output)[0].jsonLooking).toBe("null");
    expect(JSON.parse(output)[0].numericLooking).toBe("9007199254740993");
    expect(JSON.parse(output)[0].booleanLooking).toBe("true");
  });

  it("preserves JSON-looking Mongo strings in CSV across chunks with sampled object metadata", async () => {
    const { exportTableDataAsCsv } = await import(
      "../../src/extension/utils/exportService"
    );
    const driver = new MongoDBDriver({
      id: "mongo-csv",
      name: "Mongo",
      type: "mongodb",
    });
    const column = {
      name: "payload",
      type: "object",
      nativeType: "object",
      category: "json",
    } as ColumnTypeMeta;
    const documents = [{ payload: { n: 1 } }, { payload: ' { "n" : 1 } ' }];
    const chunks = documents.map((document) =>
      driver.formatTableExportChunk({
        columns: [column],
        rows: [Reflect.get(driver, "toRow").call(driver, document)],
      }),
    );
    await exportTableDataAsCsv({
      fileName: "mixed_mongo",
      loadChunks: async function* () {
        yield* chunks;
      },
    });
    expect(fs.readFileSync(outputPath, "utf8")).toBe(
      'payload\n"{""n"":{""$numberInt"":""1""}}"\n" { ""n"" : 1 } "\n',
    );
  });

  it("exports MongoDB table CSV without leading apostrophes", async () => {
    const { exportTableDataAsCsv } = await import(
      "../../src/extension/utils/exportService"
    );
    const chunk = createMongoExportChunk();

    await exportTableDataAsCsv({
      fileName: "bson_types",
      loadChunks: async function* () {
        yield chunk;
      },
    });

    const output = fs.readFileSync(outputPath, "utf8");

    expect(output).toContain(",9223372036854775801,");
    expect(output).toContain(",123456789.987654311,");
    expect(output).not.toContain("'9223372036854775801");
    expect(output).not.toContain("'123456789.987654311");
    expect(output).toContain(
      '"{""nested"":{""deep"":{""value"":42}},""arr"":[1,2,3,1]}"',
    );
    expect(output).toContain(
      '"[1,""two"",3,true,null,{""k"":""v""},[7,8,9,1]]"',
    );
  });

  it("hardens table CSV headers and text cells while preserving numeric negatives", async () => {
    const { exportTableDataAsCsv } = await import(
      "../../src/extension/utils/exportService"
    );
    const formulaColumn = {
      name: "\t=header",
      category: "text",
      nativeType: "string",
    } as const;
    const labelColumn = {
      name: "label",
      category: "text",
      nativeType: "string",
    } as const;
    const amountColumn = {
      name: "amount",
      category: "integer",
      nativeType: "int",
    } as const;

    await exportTableDataAsCsv({
      fileName: "formula_cells",
      loadChunks: async function* () {
        yield {
          columns: [formulaColumn, labelColumn, amountColumn],
          rows: [
            {
              [formulaColumn.name]: "\r\n@SUM(A1:A2)",
              label: '\u200b=HYPERLINK("https://example.com")',
              amount: -42,
            },
          ],
        };
      },
    });

    expect(fs.readFileSync(outputPath, "utf8")).toBe(
      '"\'\t=header",label,amount\n' +
        '"\'\r\n@SUM(A1:A2)","\'\u200b=HYPERLINK(""https://example.com"")",-42\n',
    );
  });

  it("exports literal markers and backslashes unchanged for normal CSV consumers", async () => {
    const { exportTableDataAsCsv } = await import(
      "../../src/extension/utils/exportService"
    );
    await exportTableDataAsCsv({
      fileName: "literal_markers",
      loadChunks: async function* () {
        yield {
          columns: [
            { name: "payload", category: "text", nativeType: "string" },
          ],
          rows: [{ payload: "MISSING" }, { payload: "\\MISSING" }],
        };
      },
    });
    expect(fs.readFileSync(outputPath, "utf8")).toBe(
      "payload\nMISSING\n\\MISSING\n",
    );
  });

  it.each([
    false,
    true,
  ])("rejects ambiguous CSV missing markers across chunks (literal first: %s)", async (literalFirst) => {
    const { exportTableDataAsCsv } = await import(
      "../../src/extension/utils/exportService"
    );
    fs.writeFileSync(outputPath, "previous file");
    await exportTableDataAsCsv({
      fileName: "ambiguous",
      loadChunks: async function* () {
        const column = {
          name: "payload",
          category: "text",
          nativeType: "string",
        } as const;
        for (const row of literalFirst
          ? [{ payload: "MISSING" }, {}]
          : [{}, { payload: "MISSING" }])
          yield { columns: [column], rows: [row] };
      },
    });
    expect(vscodeMock.showErrorMessage).toHaveBeenCalledWith(
      expect.stringMatching(/cannot distinguish/),
    );
    expect(fs.readFileSync(outputPath, "utf8")).toBe("previous file");
  });

  it("maps every CSV chunk by frozen header names, including dropped descriptors and missing values", async () => {
    const { exportTableDataAsCsv } = await import(
      "../../src/extension/utils/exportService"
    );
    const x = { name: "x", category: "text", nativeType: "string" } as const;
    const y = { ...x, name: "y" };
    await exportTableDataAsCsv({
      fileName: "sparse",
      loadChunks: async function* () {
        yield { columns: [x, y], rows: [{ x: "X1" }] };
        yield { columns: [y, x], rows: [{ y: "Y2", x: "X2" }] };
        yield { columns: [y], rows: [{ y: "Y3" }, { x: "X4" }, {}] };
      },
    });
    expect(vscodeMock.showErrorMessage).not.toHaveBeenCalled();
    expect(fs.readFileSync(outputPath, "utf8")).toBe(
      "x,y\nX1,MISSING\nX2,Y2\nMISSING,Y3\nX4,MISSING\nMISSING,MISSING\n",
    );
  });

  it.each([
    "new descriptor",
    "undeclared row key",
    "undeclared first-row key",
    "category drift",
    "native type drift",
    "duplicate descriptor",
  ])("rejects CSV %s atomically rather than omitting or misformatting data", async (mode) => {
    const { exportTableDataAsCsv } = await import(
      "../../src/extension/utils/exportService"
    );
    fs.writeFileSync(outputPath, "old export\n");
    const column = {
      name: "amount",
      category: "text",
      nativeType: "string",
    } as const;
    await exportTableDataAsCsv({
      fileName: "drift",
      loadChunks: async function* () {
        if (mode === "undeclared first-row key") {
          yield {
            columns: [column],
            rows: [{ amount: "first", later: "Y" }],
          };
          return;
        }
        yield { columns: [column], rows: [{ amount: "first" }] };
        if (mode === "new descriptor") {
          yield {
            columns: [column, { ...column, name: "later" }],
            rows: [{ later: "Y" }],
          };
        } else if (mode === "undeclared row key") {
          yield {
            columns: [column],
            rows: [{ amount: "second", later: "Y" }],
          };
        } else if (mode === "duplicate descriptor") {
          yield { columns: [column, column], rows: [{ amount: "second" }] };
        } else {
          yield {
            columns: [
              {
                ...column,
                category: mode === "category drift" ? "decimal" : "text",
                nativeType: "number",
              },
            ],
            rows: [{ amount: "9007199254740993.1250" }],
          };
        }
      },
    });
    expect(fs.readFileSync(outputPath, "utf8")).toBe("old export\n");
    expect(fs.readdirSync(tempDir)).toEqual(["export.out"]);
    expect(vscodeMock.showInformationMessage).not.toHaveBeenCalled();
    expect(vscodeMock.showErrorMessage).toHaveBeenCalledWith(
      expect.stringMatching(/CSV export failed:.*(column|schema)/i),
    );
  });

  it("allows JSON schema growth and uses each chunk's exact numeric metadata", async () => {
    const { exportTableDataAsJson } = await import(
      "../../src/extension/utils/exportService"
    );
    await exportTableDataAsJson({
      fileName: "evolving",
      loadChunks: async function* () {
        yield {
          columns: [{ name: "x", category: "text", nativeType: "string" }],
          rows: [{ x: "first" }],
        };
        yield {
          columns: [{ name: "y", category: "decimal", nativeType: "number" }],
          rows: [{ y: "9007199254740993.1250" }],
        };
      },
    });
    expect(vscodeMock.showErrorMessage).not.toHaveBeenCalled();
    expect(fs.readFileSync(outputPath, "utf8")).toContain(
      '"y":"9007199254740993.1250"',
    );
  });

  it("exports high precision SQL Server DECIMAL as a JSON string", async () => {
    const { exportTableDataAsJson } = await import(
      "../../src/extension/utils/exportService"
    );
    const exactValue = "12345678901234567890.12345678";
    await exportTableDataAsJson({
      fileName: "mssql_decimal_precision",
      loadChunks: async function* () {
        yield {
          columns: [
            {
              name: "decimal_value",
              category: "decimal",
              nativeType: "decimal(38,8)",
            },
          ],
          rows: [{ decimal_value: exactValue }],
        };
      },
    });

    const output = fs.readFileSync(outputPath, "utf8");
    expect(output).toContain(`"decimal_value":"${exactValue}"`);
    expect(JSON.parse(output)).toEqual([{ decimal_value: exactValue }]);
  });

  it("keeps decimal precision boundaries and exponent values safe for JSON.parse", async () => {
    const { exportTableDataAsJson } = await import(
      "../../src/extension/utils/exportService"
    );
    await exportTableDataAsJson({
      fileName: "decimal_json_precision_boundaries",
      loadChunks: async function* () {
        yield {
          columns: [
            {
              name: "fifteen_digits",
              category: "decimal",
              nativeType: "decimal",
            },
            {
              name: "sixteen_digits",
              category: "decimal",
              nativeType: "decimal",
            },
            {
              name: "exponent_value",
              category: "decimal",
              nativeType: "decimal",
            },
            {
              name: "trailing_zeroes",
              category: "decimal",
              nativeType: "decimal",
            },
          ],
          rows: [
            {
              fifteen_digits: "123456789012345.0",
              sixteen_digits: "1234567890123456",
              exponent_value: "1.234567890123456e15",
              trailing_zeroes: "1234567890123456.0000",
            },
          ],
        };
      },
    });

    const output = fs.readFileSync(outputPath, "utf8");
    expect(output).toContain('"fifteen_digits":123456789012345.0');
    expect(JSON.parse(output)).toEqual([
      {
        fifteen_digits: 123456789012345,
        sixteen_digits: "1234567890123456",
        exponent_value: "1.234567890123456e15",
        trailing_zeroes: "1234567890123456.0000",
      },
    ]);
  });

  it("exports high precision Oracle NUMBER digits without JavaScript rounding", async () => {
    const { exportTableDataAsJson } = await import(
      "../../src/extension/utils/exportService"
    );
    const exactValue = "123456789.1234567890";
    await exportTableDataAsJson({
      fileName: "oracle_number_precision",
      loadChunks: async function* () {
        yield {
          columns: [
            {
              name: "NUM_COL",
              category: "decimal",
              nativeType: "NUMBER(38,10)",
            },
          ],
          rows: [{ NUM_COL: exactValue }],
        };
      },
    });

    const output = fs.readFileSync(outputPath, "utf8");
    expect(output).toContain(`"NUM_COL":"${exactValue}"`);
    expect(JSON.parse(output)).toEqual([{ NUM_COL: exactValue }]);
  });

  it("preserves MySQL JSON scalar null separately from SQL NULL in table JSON export", async () => {
    const { exportTableDataAsJson } = await import(
      "../../src/extension/utils/exportService"
    );

    await exportTableDataAsJson({
      fileName: "mysql_json_null_values",
      loadChunks: async function* () {
        yield {
          columns: [
            { name: "id", category: "integer", nativeType: "int" },
            { name: "col_json", category: "json", nativeType: "json" },
          ],
          rows: [
            { id: 2, col_json: "null" },
            { id: 3, col_json: null },
            { id: 4, col_json: '"null"' },
            {
              id: 5,
              col_json: '{"$rapiDB":{"type":"json-null"}}',
            },
            { id: 6, col_json: '{"nested":[null]}' },
          ],
        };
      },
    });

    const rows = JSON.parse(fs.readFileSync(outputPath, "utf8")) as Array<{
      id: number;
      col_json: unknown;
    }>;

    expect(rows[0]).toEqual({
      id: 2,
      col_json: { $rapiDB: { type: "json-null" } },
    });
    expect(rows[1]).toEqual({ id: 3, col_json: null });
    expect(rows[2]).toEqual({ id: 4, col_json: "null" });
    expect(rows[3]).toEqual({
      id: 5,
      col_json: {
        $rapiDB: {
          type: "json-value",
          value: { $rapiDB: { type: "json-null" } },
        },
      },
    });
    expect(rows[4]).toEqual({ id: 6, col_json: { nested: [null] } });
  });

  it("rejects undeclared JSON row keys atomically instead of silently omitting data", async () => {
    const { exportTableDataAsJson } = await import(
      "../../src/extension/utils/exportService"
    );
    fs.writeFileSync(outputPath, "old export\n");
    await exportTableDataAsJson({
      fileName: "incomplete_schema",
      loadChunks: async function* () {
        yield {
          columns: [{ name: "x", category: "text", nativeType: "string" }],
          rows: [{ x: "X", y: "must not disappear" }],
        };
      },
    });
    expect(fs.readFileSync(outputPath, "utf8")).toBe("old export\n");
    expect(fs.readdirSync(tempDir)).toEqual(["export.out"]);
    expect(vscodeMock.showErrorMessage).toHaveBeenCalledWith(
      expect.stringContaining('Unexpected export row column "y"'),
    );
    expect(vscodeMock.showInformationMessage).not.toHaveBeenCalled();
  });

  it("exports query-result JSON with category-aware values", async () => {
    const { exportQueryResultsAsJson } = await import(
      "../../src/extension/utils/exportService"
    );

    await exportQueryResultsAsJson({
      columns: ["t_object", "t_array", "t_int64", "t_decimal128"],
      columnMeta: [
        { category: "json" },
        { category: "array" },
        { category: "integer" },
        { category: "decimal" },
      ],
      rows: [
        {
          __col_0: '{"nested":{"value":42}}',
          __col_1: '[1,"two",3]',
          __col_2: "9223372036854775801",
          __col_3: "123456789.987654311",
        },
      ],
    });

    const output = fs.readFileSync(outputPath, "utf8");

    expect(output).toContain('"t_object":{"nested":{"value":42}}');
    expect(output).toContain('"t_array":[1,"two",3]');
    expect(output).toContain('"t_int64":9223372036854775801');
    expect(output).toContain('"t_decimal128":"123456789.987654311"');
  });

  it.each([
    "csv",
    "json",
  ] as const)("exports formatted query dates without asserting a timezone (%s)", async (format) => {
    const formatted = formatQueryResult(
      {
        columns: [
          "date_value",
          "local_timestamp",
          "payload",
          "binary",
          "missing",
        ],
        columnMeta: [
          { category: "datetime" },
          { category: "datetime" },
          { category: "json" },
          { category: "binary" },
          { category: "text" },
        ],
        rows: [
          {
            __col_0: new Date("2024-07-04T12:00:11.123Z"),
            __col_1: "2024-07-04 12:00:11.123",
            __col_2: { nested: [1, true] },
            __col_3: Buffer.from([0xde, 0xad]),
            __col_4: null,
          },
          { __col_0: new Date("2025-01-01T00:00:00Z") },
        ],
        missingCells: [
          { rowIndex: 0, columnIndex: 4 },
          { rowIndex: 1, columnIndex: 4 },
        ],
        rowCount: 2,
        executionTimeMs: 1,
      },
      1,
    );
    expect(formatted.exportRows).toBeUndefined();
    expect(formatted.missingCells).toEqual([{ rowIndex: 0, columnIndex: 4 }]);
    const exporters = await import("../../src/extension/utils/exportService");
    if (format === "csv") {
      await exporters.exportQueryResultsAsCsv(formatted);
      expect(fs.readFileSync(outputPath, "utf8")).toBe(
        "date_value,local_timestamp,payload,binary,missing\n" +
          '2024-07-04 12:00:11.123,2024-07-04 12:00:11.123,"{""nested"":[1,true]}",0xdead,MISSING\n',
      );
    } else {
      await exporters.exportQueryResultsAsJson(formatted);
      expect(JSON.parse(fs.readFileSync(outputPath, "utf8"))).toEqual([
        {
          date_value: "2024-07-04 12:00:11.123",
          local_timestamp: "2024-07-04 12:00:11.123",
          payload: { nested: [1, true] },
          binary: "0xdead",
        },
      ]);
    }
  });

  it("preserves MySQL native JSON metadata through Query Results JSON export", async () => {
    const driver = new MySQLDriver({
      id: "mysql-json-export",
      name: "MySQL JSON export",
      type: "mysql",
    });
    const parsed = (
      driver as unknown as {
        _parseQueryResult(
          rows: unknown[][],
          fields: unknown[],
          executionTimeMs: number,
        ): QueryResult;
      }
    )._parseQueryResult(
      [
        [
          '{"name":"object","nested":[1,null]}',
          "[1,null]",
          '"text"',
          "null",
          null,
        ],
      ],
      [
        { name: "object_value", type: 245 },
        { name: "array_value", columnType: 245 },
        { name: "string_value", typeName: "JSON" },
        { name: "json_null", extendedFormat: "json" },
        { name: "sql_null", type: 253 },
      ],
      0,
    );

    expect(parsed.columnMeta).toEqual([
      { category: "json" },
      { category: "json" },
      { category: "json" },
      { category: "json" },
      { category: null },
    ]);

    const formatted = formatQueryResult(parsed, 100);
    const { exportQueryResultsAsJson } = await import(
      "../../src/extension/utils/exportService"
    );
    await exportQueryResultsAsJson({
      columns: formatted.columns,
      columnMeta: formatted.columnMeta,
      rows: formatted.rows,
    });

    expect(JSON.parse(fs.readFileSync(outputPath, "utf8"))).toEqual([
      {
        object_value: { name: "object", nested: [1, null] },
        array_value: [1, null],
        string_value: "text",
        json_null: { $rapiDB: { type: "json-null" } },
        sql_null: null,
      },
    ]);
  });

  it("exports the first MySQL JSON result set from an unbounded script as an object", async () => {
    const driver = new MySQLDriver({
      id: "mysql-json-script-export",
      name: "MySQL JSON script export",
      type: "mysql",
    });
    const connection = {
      connection: {
        config: {},
        query: vi.fn((command: { emit(event: string): void }) => {
          queueMicrotask(() => command.emit("end"));
          return command;
        }),
      },
      query: vi.fn(async (input: { sql: string } | string) => {
        const sql = typeof input === "string" ? input : input.sql;
        if (sql === "DO 0") return [{ serverStatus: 2 }, []];
        if (sql.startsWith("SELECT JSON_OBJECT")) {
          return [
            [['{"nested":{"value":42}}']],
            [{ name: "payload", columnType: 245 }],
          ];
        }
        if (sql === "SELECT 2 AS later") {
          return [[[2]], [{ name: "later", columnType: 3 }]];
        }
        return [[], []];
      }),
      release: vi.fn(),
      destroy: vi.fn(),
    };
    (driver as unknown as { pool: unknown }).pool = {
      getConnection: vi.fn(async () => connection),
    };

    const result = await driver.query(
      "SELECT JSON_OBJECT('value', 42) AS payload; SELECT 2 AS later",
    );
    expect(result.columnMeta).toEqual([{ category: "json" }]);
    expect(result.resultSets?.[0].columnMeta).toEqual([{ category: "json" }]);

    const formatted = formatQueryResult(result, 100);
    const { exportQueryResultsAsJson } = await import(
      "../../src/extension/utils/exportService"
    );
    await exportQueryResultsAsJson({
      columns: formatted.columns,
      columnMeta: formatted.columnMeta,
      rows: formatted.rows,
    });

    expect(JSON.parse(fs.readFileSync(outputPath, "utf8"))).toEqual([
      { payload: { nested: { value: 42 } } },
    ]);
  });

  it.each([
    "csv",
    "json",
  ] as const)("exports DynamoDB Query Results with native AttributeValue envelopes (%s)", async (format) => {
    const exporters = await import("../../src/extension/utils/exportService");
    const result = {
      columns: [
        "pk",
        "binary",
        "stringSet",
        "numberSet",
        "binarySet",
        "missing",
        "nullValue",
      ],
      rows: [
        {
          __col_0: "user-0006",
          __col_1: "0x010203",
          __col_2: "<<'tag1', 'tag2'>>",
          __col_3: "<<1, 9007199254740993>>",
          __col_4: "<<0x010203, 0x040506>>",
          __col_5: null,
          __col_6: null,
        },
      ],
      exportRows: [
        {
          __col_0: { S: "user-0006" },
          __col_1: { B: "AQID" },
          __col_2: { SS: ["tag1", "tag2"] },
          __col_3: { NS: ["1", "9007199254740993"] },
          __col_4: { BS: ["AQID", "BAUG"] },
          __col_6: { NULL: true },
        },
      ],
      missingCells: [{ rowIndex: 0, columnIndex: 5 }],
    };

    if (format === "csv") {
      await exporters.exportQueryResultsAsCsv(result);
      expect(fs.readFileSync(outputPath, "utf8")).toBe(
        "pk,binary,stringSet,numberSet,binarySet,missing,nullValue\n" +
          '"{""S"":""user-0006""}","{""B"":""AQID""}","{""SS"":[""tag1"",""tag2""]}","{""NS"":[""1"",""9007199254740993""]}","{""BS"":[""AQID"",""BAUG""]}",MISSING,"{""NULL"":true}"\n',
      );
      return;
    }

    await exporters.exportQueryResultsAsJson(result);
    expect(JSON.parse(fs.readFileSync(outputPath, "utf8"))).toEqual([
      {
        pk: { S: "user-0006" },
        binary: { B: "AQID" },
        stringSet: { SS: ["tag1", "tag2"] },
        numberSet: { NS: ["1", "9007199254740993"] },
        binarySet: { BS: ["AQID", "BAUG"] },
        nullValue: { NULL: true },
      },
    ]);
  });

  it.each([
    "csv",
    "json",
  ] as const)("exports missing DynamoDB table attributes differently from NULL (%s)", async (format) => {
    const exporters = await import("../../src/extension/utils/exportService");
    const options = {
      fileName: "dynamodb_missing_and_null",
      loadChunks: async function* () {
        yield {
          columns: [
            { name: "pk", category: "text" as const, nativeType: "string" },
            {
              name: "missing",
              category: "other" as const,
              nativeType: "null",
            },
            {
              name: "nullValue",
              category: "other" as const,
              nativeType: "null",
            },
          ],
          rows: [
            { pk: { S: "item-1" }, nullValue: { NULL: true } },
            { pk: { S: "item-2" }, nullValue: { NULL: true } },
          ],
          missingCells: [{ rowIndex: 0, columnIndex: 1 }],
        };
      },
    };

    if (format === "csv") {
      await exporters.exportTableDataAsCsv(options);
      expect(fs.readFileSync(outputPath, "utf8")).toBe(
        'pk,missing,nullValue\n"{""S"":""item-1""}",MISSING,"{""NULL"":true}"\n' +
          '"{""S"":""item-2""}",MISSING,"{""NULL"":true}"\n',
      );
      return;
    }

    await exporters.exportTableDataAsJson(options);
    expect(JSON.parse(fs.readFileSync(outputPath, "utf8"))).toEqual([
      { pk: { S: "item-1" }, nullValue: { NULL: true } },
      { pk: { S: "item-2" }, nullValue: { NULL: true } },
    ]);
  });

  it("exports Redis structured replies as JSON and keeps JSON-looking scalar strings", async () => {
    const { exportQueryResultsAsJson } = await import(
      "../../src/extension/utils/exportService"
    );

    await exportQueryResultsAsJson({
      columns: ["results", "objectReply", "scalarString"],
      columnMeta: [
        { category: "json" },
        { category: "json" },
        { category: null },
      ],
      rows: [
        {
          __col_0: '["string","123","true","null"]',
          __col_1: '{"first":"value-1","second":"value-2"}',
          __col_2: '["not","an","array"]',
        },
      ],
    });

    const output = fs.readFileSync(outputPath, "utf8");
    expect(JSON.parse(output)).toEqual([
      {
        results: ["string", "123", "true", "null"],
        objectReply: { first: "value-1", second: "value-2" },
        scalarString: '["not","an","array"]',
      },
    ]);
  });

  it("hardens query-result CSV headers and text cells while preserving numeric negatives", async () => {
    const { exportQueryResultsAsCsv } = await import(
      "../../src/extension/utils/exportService"
    );

    await exportQueryResultsAsCsv({
      columns: ["=header", "amount", "label"],
      columnMeta: [
        { category: "text" },
        { category: "integer" },
        { category: "text" },
      ],
      rows: [
        {
          __col_0: "\u00a0@SUM(A1:A2)",
          __col_1: "-42",
          __col_2: ' \t=HYPERLINK("https://example.com")',
        },
      ],
    });

    expect(fs.readFileSync(outputPath, "utf8")).toBe(
      "'=header,amount,label\n" +
        `'\u00a0@SUM(A1:A2),-42,"' \t=HYPERLINK(""https://example.com"")"\n`,
    );
  });

  it("keeps a literal (NULL) in a different column than SQL NULL", async () => {
    const { exportQueryResultsAsCsv } = await import(
      "../../src/extension/utils/exportService"
    );

    await exportQueryResultsAsCsv({
      columns: ["null_value", "empty_value", "literal_marker"],
      columnMeta: [
        { category: "text" },
        { category: "text" },
        { category: "text" },
      ],
      rows: [
        {
          __col_0: null,
          __col_1: "",
          __col_2: "(NULL)",
        },
      ],
    });

    expect(fs.readFileSync(outputPath, "utf8")).toBe(
      "null_value,empty_value,literal_marker\n" + "(NULL),,(NULL)\n",
    );
  });

  it.each([
    "null",
    "missing",
  ])("rejects a query CSV %s marker collision", async (kind) => {
    const { exportQueryResultsAsCsv } = await import(
      "../../src/extension/utils/exportService"
    );
    fs.writeFileSync(outputPath, "previous file");
    await exportQueryResultsAsCsv({
      columns: ["value"],
      rows:
        kind === "null"
          ? [{ __col_0: null }, { __col_0: "(NULL)" }]
          : [{ __col_0: null }, { __col_0: "MISSING" }],
      ...(kind === "missing"
        ? { missingCells: [{ rowIndex: 0, columnIndex: 0 }] }
        : {}),
    });
    expect(vscodeMock.showErrorMessage).toHaveBeenCalledWith(
      expect.stringMatching(/cannot distinguish/),
    );
    expect(fs.readFileSync(outputPath, "utf8")).toBe("previous file");
  });

  it("writes table CSV NULL with the query CSV marker and keeps empty strings empty", async () => {
    const { exportTableDataAsCsv } = await import(
      "../../src/extension/utils/exportService"
    );
    const text = { category: "text", nativeType: "text" } as const;

    await exportTableDataAsCsv({
      fileName: "nulls",
      loadChunks: async function* () {
        yield {
          columns: [
            { ...text, name: "null_value" },
            { ...text, name: "empty_value" },
            { ...text, name: "literal_marker" },
          ],
          rows: [
            { null_value: null, empty_value: "", literal_marker: "(NULL)" },
          ],
        };
      },
    });

    expect(vscodeMock.showErrorMessage).not.toHaveBeenCalled();
    expect(fs.readFileSync(outputPath, "utf8")).toBe(
      "null_value,empty_value,literal_marker\n(NULL),,(NULL)\n",
    );
  });

  it("rejects a table CSV NULL marker collision across chunks", async () => {
    const { exportTableDataAsCsv } = await import(
      "../../src/extension/utils/exportService"
    );
    fs.writeFileSync(outputPath, "previous file");
    const column = {
      name: "value",
      category: "text",
      nativeType: "text",
    } as const;

    await exportTableDataAsCsv({
      fileName: "null_collision",
      loadChunks: async function* () {
        yield { columns: [column], rows: [{ value: null }] };
        yield { columns: [column], rows: [{ value: "(NULL)" }] };
      },
    });

    expect(vscodeMock.showErrorMessage).toHaveBeenCalledWith(
      expect.stringMatching(/cannot distinguish/),
    );
    expect(fs.readFileSync(outputPath, "utf8")).toBe("previous file");
  });

  it.each([
    "null",
    "missing",
  ])("rejects a literal before a table CSV %s marker in a later chunk", async (kind) => {
    const { exportTableDataAsCsv } = await import(
      "../../src/extension/utils/exportService"
    );
    const column = {
      name: "value",
      category: "text",
      nativeType: "text",
    } as const;
    fs.writeFileSync(outputPath, "previous file");
    let closed = false;
    await exportTableDataAsCsv({
      fileName: "collision",
      loadChunks: async function* () {
        try {
          yield {
            columns: [column],
            rows: [{ value: kind === "null" ? "(NULL)" : "MISSING" }],
          };
          yield {
            columns: [column],
            rows: [kind === "null" ? { value: null } : {}],
          };
        } finally {
          closed = true;
        }
      },
    });
    expect(closed).toBe(true);
    expect(vscodeMock.showErrorMessage).toHaveBeenCalledWith(
      expect.stringMatching(/cannot distinguish.*Export JSON/),
    );
    expect(fs.readFileSync(outputPath, "utf8")).toBe("previous file");
  });

  it("preserves missing, NULL, empty and quoted text in streamed CSV and JSON", async () => {
    const { exportTableDataAsCsv, exportTableDataAsJson } = await import(
      "../../src/extension/utils/exportService"
    );
    const columns = ["value", "quoted"].map((name) => ({
      name,
      category: "text" as const,
      nativeType: "text",
    }));
    const options = {
      fileName: "values",
      loadChunks: async function* () {
        yield {
          columns,
          rows: [
            { value: null, quoted: 'a,"b"\nnext' },
            { value: "", quoted: "text" },
          ],
        };
        yield {
          columns,
          rows: [{ quoted: "text" }, { value: "ignored", quoted: "text" }],
          missingCells: [{ rowIndex: 1, columnIndex: 0 }],
        };
      },
    };
    await exportTableDataAsCsv(options);
    expect(fs.readFileSync(outputPath, "utf8")).toBe(
      'value,quoted\n(NULL),"a,""b""\nnext"\n,text\nMISSING,text\nMISSING,text\n',
    );
    await exportTableDataAsJson(options);
    expect(JSON.parse(fs.readFileSync(outputPath, "utf8"))).toEqual([
      { value: null, quoted: 'a,"b"\nnext' },
      { value: "", quoted: "text" },
      { quoted: "text" },
      { quoted: "text" },
    ]);
    expect(vscodeMock.showErrorMessage).not.toHaveBeenCalled();
  });

  it.skipIf(process.platform === "win32")(
    "keeps the permissions of an exported file it replaces",
    async () => {
      fs.writeFileSync(outputPath, "previous export\n");
      fs.chmodSync(outputPath, 0o644);
      const { exportTableDataAsCsv } = await import(
        "../../src/extension/utils/exportService"
      );

      await exportTableDataAsCsv({
        fileName: "shared_export",
        loadChunks: async function* () {
          yield {
            columns: [{ name: "id", category: "integer", nativeType: "int" }],
            rows: [{ id: 1 }],
          };
        },
      });

      expect(vscodeMock.showErrorMessage).not.toHaveBeenCalled();
      expect(fs.readFileSync(outputPath, "utf8")).toBe("id\n1\n");
      expect(fs.statSync(outputPath).mode & 0o777).toBe(0o644);
    },
  );

  it.each([
    ["EPERM", true],
    ["ENOTSUP", true],
    ["EINVAL", true],
    ["EIO", false],
  ] as const)("treats a %s permission copy failure as best-effort: %s", async (code, installed) => {
    fs.writeFileSync(outputPath, "previous export\n");
    vi.doMock("node:fs", async () => {
      const actual = await vi.importActual<typeof import("node:fs")>("node:fs");
      return {
        ...actual,
        chmodSync: vi.fn(() => {
          throw Object.assign(new Error(`chmod ${code}`), { code });
        }),
      };
    });

    try {
      const { exportTableDataAsCsv } = await import(
        "../../src/extension/utils/exportService"
      );
      await exportTableDataAsCsv({
        fileName: "chmod_export",
        loadChunks: async function* () {
          yield {
            columns: [{ name: "id", category: "integer", nativeType: "int" }],
            rows: [{ id: 1 }],
          };
        },
      });
    } finally {
      vi.doUnmock("node:fs");
    }

    expect(fs.readFileSync(outputPath, "utf8")).toBe(
      installed ? "id\n1\n" : "previous export\n",
    );
    expect(fs.readdirSync(tempDir)).toEqual(["export.out"]);
    if (installed) {
      expect(vscodeMock.showErrorMessage).not.toHaveBeenCalled();
    } else {
      expect(vscodeMock.showErrorMessage).toHaveBeenCalledWith(
        expect.stringContaining(`chmod ${code}`),
      );
    }
  });

  it("removes temporary output when export fails", async () => {
    const { exportTableDataAsCsv } = await import(
      "../../src/extension/utils/exportService"
    );

    await exportTableDataAsCsv({
      fileName: "failed_export",
      loadChunks: async function* () {
        yield {
          columns: [{ name: "id", category: "integer", nativeType: "int" }],
          rows: [{ id: 1 }],
        };
        throw new Error("read failed");
      },
    });

    expect(fs.existsSync(outputPath)).toBe(false);
    expect(
      fs.readdirSync(tempDir).some((name) => name.includes(".rapidb-")),
    ).toBe(false);
    expect(vscodeMock.showErrorMessage).toHaveBeenCalledWith(
      expect.stringContaining("read failed"),
    );
  });

  it.each([
    { kind: "empty", contents: [] as const },
    { kind: "non-empty", contents: ["keep.txt"] as const },
  ])("rejects an existing $kind directory destination without changing it or creating a temp file", async ({
    contents,
  }) => {
    fs.mkdirSync(outputPath);
    for (const name of contents) {
      fs.writeFileSync(path.join(outputPath, name), "keep this file");
    }

    const { exportTableDataAsCsv } = await import(
      "../../src/extension/utils/exportService"
    );
    await exportTableDataAsCsv({
      fileName: "directory_export",
      loadChunks: async function* () {
        yield {
          columns: [{ name: "id", category: "integer", nativeType: "int" }],
          rows: [{ id: 1 }],
        };
      },
    });

    expect(fs.lstatSync(outputPath).isDirectory()).toBe(true);
    expect(fs.readdirSync(outputPath)).toEqual(contents);
    for (const name of contents) {
      expect(fs.readFileSync(path.join(outputPath, name), "utf8")).toBe(
        "keep this file",
      );
    }
    expect(fs.readdirSync(tempDir)).toEqual(["export.out"]);
    expect(vscodeMock.withProgress).not.toHaveBeenCalled();
    expect(vscodeMock.showErrorMessage).toHaveBeenCalledWith(
      expect.stringContaining("Cannot export to a directory"),
    );
  });

  it.skipIf(process.platform === "win32")(
    "creates exported files with owner-only permissions regardless of umask",
    async () => {
      fs.chmodSync(tempDir, 0o777);
      const originalUmask = process.umask(0);
      try {
        const { exportTableDataAsCsv } = await import(
          "../../src/extension/utils/exportService"
        );
        await exportTableDataAsCsv({
          fileName: "private_export",
          loadChunks: async function* () {
            yield {
              columns: [{ name: "id", category: "integer", nativeType: "int" }],
              rows: [{ id: 1 }],
            };
          },
        });
      } finally {
        process.umask(originalUmask);
      }

      expect(fs.statSync(outputPath).mode & 0o777).toBe(0o600);
    },
  );

  it("requests exclusive owner-only staging-file creation on every platform", async () => {
    let createWriteStreamSpy: ReturnType<typeof vi.fn> | undefined;
    vi.doMock("node:fs", async () => {
      const actual = await vi.importActual<typeof import("node:fs")>("node:fs");
      createWriteStreamSpy = vi.fn(actual.createWriteStream);
      return { ...actual, createWriteStream: createWriteStreamSpy };
    });

    try {
      const { exportTableDataAsCsv } = await import(
        "../../src/extension/utils/exportService"
      );
      await exportTableDataAsCsv({
        fileName: "staging_options",
        loadChunks: async function* () {
          yield {
            columns: [{ name: "id", category: "integer", nativeType: "int" }],
            rows: [{ id: 1 }],
          };
        },
      });
    } finally {
      vi.doUnmock("node:fs");
    }

    expect(createWriteStreamSpy).toHaveBeenCalledWith(
      expect.stringContaining(".rapidb-"),
      expect.objectContaining({ encoding: "utf8", flags: "wx", mode: 0o600 }),
    );
  });

  it.skipIf(process.platform === "win32")(
    "does not follow or remove a colliding temporary-path symlink",
    async () => {
      const targetPath = path.join(tempDir, "symlink-target.txt");
      const temporaryPath = `${outputPath}.rapidb-known.tmp`;
      fs.writeFileSync(targetPath, "do not overwrite");
      fs.symlinkSync(targetPath, temporaryPath);
      vi.doMock("node:crypto", async () => {
        const actual =
          await vi.importActual<typeof import("node:crypto")>("node:crypto");
        return { ...actual, randomUUID: () => "known" };
      });

      try {
        const { exportTableDataAsCsv } = await import(
          "../../src/extension/utils/exportService"
        );
        await exportTableDataAsCsv({
          fileName: "symlink_export",
          loadChunks: async function* () {
            yield {
              columns: [{ name: "id", category: "integer", nativeType: "int" }],
              rows: [{ id: 1 }],
            };
          },
        });
      } finally {
        vi.doUnmock("node:crypto");
      }

      expect(fs.readFileSync(targetPath, "utf8")).toBe("do not overwrite");
      expect(fs.lstatSync(temporaryPath).isSymbolicLink()).toBe(true);
      expect(vscodeMock.showErrorMessage).toHaveBeenCalledWith(
        expect.stringContaining("CSV export failed"),
      );
    },
  );

  it.each([
    "csv",
    "json",
  ] as const)("reports a staging I/O error between %s chunks without hanging or replacing the destination", async (format) => {
    fs.writeFileSync(outputPath, "previous export\n");
    let stream: fs.WriteStream | undefined;
    let cancel: (() => void) | undefined;
    vscodeMock.withProgress.mockImplementation(async (_options, task) =>
      task(
        {},
        {
          onCancellationRequested: (listener: () => void) => {
            cancel = listener;
            return { dispose: vi.fn() };
          },
        },
      ),
    );
    vi.doMock("node:fs", async () => {
      const actual = await vi.importActual<typeof import("node:fs")>("node:fs");
      return {
        ...actual,
        createWriteStream: (
          ...args: Parameters<typeof fs.createWriteStream>
        ) => {
          stream = actual.createWriteStream(...args);
          return stream;
        },
      };
    });
    let exporting: Promise<void> | undefined;
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      const exporters = await import("../../src/extension/utils/exportService");
      const exporter =
        format === "csv"
          ? exporters.exportTableDataAsCsv
          : exporters.exportTableDataAsJson;
      exporting = exporter({
        fileName: "disk_failure",
        loadChunks: async function* () {
          const chunk: ChunkedExportData = {
            columns: [{ name: "id", category: "integer", nativeType: "int" }],
            rows: [{ id: 1 }],
          };
          yield chunk;
          const staging = required(stream);
          await new Promise<void>((resolve) => {
            staging.once("close", resolve);
            staging.destroy(new Error("ENOSPC: simulated disk full"));
          });
          yield chunk;
        },
      });
      const settled = await Promise.race([
        exporting.then(() => true),
        new Promise<false>((resolve) => {
          timer = setTimeout(() => resolve(false), 500);
        }),
      ]);
      expect(settled).toBe(true);
      expect(vscodeMock.showErrorMessage).toHaveBeenCalledWith(
        expect.stringContaining("ENOSPC"),
      );
      expect(vscodeMock.showInformationMessage).not.toHaveBeenCalled();
      expect(fs.readFileSync(outputPath, "utf8")).toBe("previous export\n");
      expect(fs.readdirSync(tempDir)).toEqual(["export.out"]);
    } finally {
      if (timer) clearTimeout(timer);
      cancel?.();
      await exporting;
      vi.doUnmock("node:fs");
    }
  });

  it("cancels while the staging stream closes without replacing the old export", async () => {
    const previousOutput = "previous export\n";
    fs.writeFileSync(outputPath, previousOutput);
    let cancel: (() => void) | undefined;
    let markClosePending!: () => void;
    let finishClose!: () => void;
    const closePending = new Promise<void>((resolve) => {
      markClosePending = resolve;
    });
    const closeGate = new Promise<void>((resolve) => {
      finishClose = resolve;
    });
    vscodeMock.withProgress.mockImplementation(async (_options, task) =>
      task(
        {},
        {
          onCancellationRequested: (listener: () => void) => {
            cancel = listener;
            return { dispose: vi.fn() };
          },
        },
      ),
    );
    vi.doMock("node:fs", async () => {
      const actual = await vi.importActual<typeof import("node:fs")>("node:fs");
      return {
        ...actual,
        createWriteStream: vi.fn((filePath: unknown, options: unknown) => {
          const stream = actual.createWriteStream(
            filePath as string,
            options as never,
          );
          const end = stream.end.bind(stream);
          stream.end = ((callback?: () => void) => {
            end(() => {
              markClosePending();
              void closeGate.then(() => callback?.());
            });
            return stream;
          }) as typeof stream.end;
          return stream;
        }),
      };
    });

    let exporting: Promise<void> | undefined;
    try {
      const { exportTableDataAsCsv } = await import(
        "../../src/extension/utils/exportService"
      );
      exporting = exportTableDataAsCsv({
        fileName: "cancel_during_close",
        loadChunks: async function* () {
          yield {
            columns: [{ name: "id", category: "integer", nativeType: "int" }],
            rows: [{ id: 1 }],
          };
        },
      });
      await closePending;
      required(cancel)();
      finishClose();
      await exporting;
    } finally {
      finishClose();
      vi.doUnmock("node:fs");
    }

    expect(fs.readFileSync(outputPath, "utf8")).toBe(previousOutput);
    expect(fs.readdirSync(tempDir)).toEqual(["export.out"]);
    expect(vscodeMock.showInformationMessage).not.toHaveBeenCalled();
    expect(vscodeMock.showErrorMessage).not.toHaveBeenCalled();
  });

  it("reports staging cleanup failures when cancellation occurs before replacement", async () => {
    const previousOutput = "previous export\n";
    fs.writeFileSync(outputPath, previousOutput);
    let cancel: (() => void) | undefined;
    vscodeMock.withProgress.mockImplementation(async (_options, task) =>
      task(
        {},
        {
          onCancellationRequested: (listener: () => void) => {
            cancel = listener;
            return { dispose: vi.fn() };
          },
        },
      ),
    );
    vi.doMock("node:fs", async () => {
      const actual = await vi.importActual<typeof import("node:fs")>("node:fs");
      return {
        ...actual,
        rmSync: vi.fn((targetPath: unknown, options: unknown) => {
          if (String(targetPath).endsWith(".tmp")) {
            throw new Error("staging cleanup denied");
          }
          return actual.rmSync(targetPath as never, options as never);
        }),
      };
    });

    try {
      const { exportTableDataAsCsv } = await import(
        "../../src/extension/utils/exportService"
      );
      await exportTableDataAsCsv({
        fileName: "cancel_with_staging_cleanup_failure",
        loadChunks: async function* () {
          yield {
            columns: [{ name: "id", category: "integer", nativeType: "int" }],
            rows: [{ id: 1 }],
          };
          required(cancel)();
        },
      });
    } finally {
      vi.doUnmock("node:fs");
    }

    const stagingFile = fs
      .readdirSync(tempDir)
      .find((name) => name.endsWith(".tmp"));
    expect(fs.readFileSync(outputPath, "utf8")).toBe(previousOutput);
    expect(stagingFile).toBeDefined();
    expect(fs.existsSync(path.join(tempDir, stagingFile as string))).toBe(true);
    expect(vscodeMock.showInformationMessage).not.toHaveBeenCalled();
    expect(vscodeMock.showErrorMessage).toHaveBeenCalledWith(
      expect.stringContaining("staging cleanup denied"),
    );
    expect(String(vscodeMock.showErrorMessage.mock.calls[0]?.[0])).toContain(
      path.join(tempDir, stagingFile as string),
    );
  });

  it("waits for asynchronous stream destruction before deleting a staging file", async () => {
    fs.writeFileSync(outputPath, "previous export\n");
    let beginDestroy!: () => void;
    let releaseDestroy!: () => void;
    const destroying = new Promise<void>((resolve) => {
      beginDestroy = resolve;
    });
    const gate = new Promise<void>((resolve) => {
      releaseDestroy = resolve;
    });
    const removed = vi.fn();
    vi.doMock("node:fs", async () => {
      const actual = await vi.importActual<typeof import("node:fs")>("node:fs");
      return {
        ...actual,
        createWriteStream: vi.fn((filePath: unknown, options: unknown) => {
          const stream = actual.createWriteStream(
            filePath as string,
            options as never,
          );
          const destroy = stream._destroy.bind(stream);
          stream._destroy = (error, callback) => {
            beginDestroy();
            void gate.then(() => destroy(error, callback));
          };
          return stream;
        }),
        rmSync: vi.fn((target: unknown, options: unknown) => {
          removed();
          return actual.rmSync(target as never, options as never);
        }),
      };
    });
    try {
      const { exportTableDataAsCsv } = await import(
        "../../src/extension/utils/exportService"
      );
      const exporting = exportTableDataAsCsv({
        fileName: "delayed_close",
        loadChunks: () => ({
          [Symbol.asyncIterator]: () => ({
            next: () => Promise.reject(new Error("read failed")),
          }),
        }),
      });
      await destroying;
      expect(removed).not.toHaveBeenCalled();
      expect(
        fs.readdirSync(tempDir).some((name) => name.endsWith(".tmp")),
      ).toBe(true);
      releaseDestroy();
      await exporting;
      expect(removed).toHaveBeenCalledOnce();
      expect(fs.readdirSync(tempDir)).toEqual(["export.out"]);
      expect(fs.readFileSync(outputPath, "utf8")).toBe("previous export\n");
    } finally {
      releaseDestroy();
      vi.doUnmock("node:fs");
    }
  });

  it("reports a non-cancellation failure that races with cancellation", async () => {
    const previousOutput = "previous export\n";
    fs.writeFileSync(outputPath, previousOutput);
    let cancel: (() => void) | undefined;
    vscodeMock.withProgress.mockImplementation(async (_options, task) =>
      task(
        {},
        {
          onCancellationRequested: (listener: () => void) => {
            cancel = listener;
            return { dispose: vi.fn() };
          },
        },
      ),
    );

    const { exportTableDataAsCsv } = await import(
      "../../src/extension/utils/exportService"
    );
    await exportTableDataAsCsv({
      fileName: "failure_racing_with_cancel",
      loadChunks: async function* () {
        yield {
          columns: [{ name: "id", category: "integer", nativeType: "int" }],
          rows: [],
        };
        required(cancel)();
        throw new Error("read failed while cancelling");
      },
    });

    expect(fs.readFileSync(outputPath, "utf8")).toBe(previousOutput);
    expect(fs.readdirSync(tempDir)).toEqual(["export.out"]);
    expect(vscodeMock.showInformationMessage).not.toHaveBeenCalled();
    expect(vscodeMock.showErrorMessage).toHaveBeenCalledWith(
      expect.stringContaining("read failed while cancelling"),
    );
  });

  it("sanitizes the default export file name into one portable path segment", async () => {
    const { exportTableDataAsCsv, sanitizeExportFileName } = await import(
      "../../src/extension/utils/exportService"
    );
    expect(sanitizeExportFileName("mydb_../../../.ssh/x")).toBe(
      "mydb_.._.._.._.ssh_x",
    );
    expect(sanitizeExportFileName("..")).toBe("export");
    expect(sanitizeExportFileName('a\\b:c*d?e"f<g>h|i\u0001j')).toBe(
      "a_b_c_d_e_f_g_h_i_j",
    );
    expect(sanitizeExportFileName(" .hidden. ")).toBe("hidden");
    expect(sanitizeExportFileName("nul.txt")).toBe("_nul.txt");
    expect(sanitizeExportFileName("CON.foo")).toBe("_CON.foo");
    expect(sanitizeExportFileName("Com1")).toBe("_Com1");
    expect(sanitizeExportFileName("lpt9.csv")).toBe("_lpt9.csv");
    expect(sanitizeExportFileName("com0")).toBe("com0");
    expect(sanitizeExportFileName("lpt0.csv")).toBe("lpt0.csv");
    expect(sanitizeExportFileName("public_users")).toBe("public_users");
    expect(sanitizeExportFileName("a".repeat(300), ".json")).toBe(
      "a".repeat(250),
    );
    const longUnicode = sanitizeExportFileName("é😀".repeat(100), ".csv");
    expect(Buffer.byteLength(`${longUnicode}.csv`, "utf8")).toBeLessThanOrEqual(
      255,
    );
    expect(longUnicode).toBe(`${"é😀".repeat(41)}é`);
    expect(sanitizeExportFileName(`${"a".repeat(250)}. tail`, ".csv")).toBe(
      "a".repeat(250),
    );
    expect(sanitizeExportFileName(`CON${" ".repeat(247)}x`, ".json")).toBe(
      "_CON",
    );
    expect(sanitizeExportFileName(`NUL${" ".repeat(260)}x`, ".json")).toBe(
      "_NUL",
    );
    expect(sanitizeExportFileName(`CON${"\u3000".repeat(83)}x`, ".json")).toBe(
      "_CON",
    );
    expect(sanitizeExportFileName(`CON.${"a".repeat(246)}`, ".json")).toBe(
      `_CON.${"a".repeat(245)}`,
    );

    vscodeMock.showSaveDialog.mockResolvedValueOnce(undefined);
    const loadChunks = vi.fn();
    await exportTableDataAsCsv({ fileName: "../../etc/passwd", loadChunks });

    const [{ defaultUri }] = vscodeMock.showSaveDialog.mock.calls[0];
    expect(path.dirname(defaultUri.fsPath)).toBe(
      path.join(os.homedir(), "Downloads"),
    );
    expect(path.basename(defaultUri.fsPath)).toBe("_.._etc_passwd.csv");
    expect(loadChunks).not.toHaveBeenCalled();
  });

  it("refuses to replace an existing file on Windows", async () => {
    fs.writeFileSync(outputPath, "previous export\n");
    const loadChunks = vi.fn(async function* () {
      yield {
        columns: [
          { name: "id", category: "integer" as const, nativeType: "int" },
        ],
        rows: [{ id: 1 }],
      };
    });
    const platform = Object.getOwnPropertyDescriptor(process, "platform")!;
    Object.defineProperty(process, "platform", { ...platform, value: "win32" });
    try {
      const { exportTableDataAsCsv } = await import(
        "../../src/extension/utils/exportService"
      );
      await exportTableDataAsCsv({
        fileName: "windows_existing",
        loadChunks,
      });
    } finally {
      Object.defineProperty(process, "platform", platform);
    }
    expect(fs.readFileSync(outputPath, "utf8")).toBe("previous export\n");
    expect(loadChunks).not.toHaveBeenCalled();
    expect(vscodeMock.withProgress).not.toHaveBeenCalled();
    expect(vscodeMock.showSaveDialog).toHaveBeenCalledWith(
      expect.objectContaining({
        title: expect.stringContaining("existing files cannot be replaced"),
      }),
    );
    expect(fs.readdirSync(tempDir)).toEqual(["export.out"]);
    expect(vscodeMock.showErrorMessage).toHaveBeenCalledWith(
      expect.stringContaining("Choose a new file name"),
    );
  });

  it("publishes a new file on Windows without replacing another file", async () => {
    const platform = Object.getOwnPropertyDescriptor(process, "platform")!;
    Object.defineProperty(process, "platform", { ...platform, value: "win32" });
    try {
      const { exportTableDataAsCsv } = await import(
        "../../src/extension/utils/exportService"
      );
      await exportTableDataAsCsv({
        fileName: "windows_new",
        loadChunks: async function* () {
          yield {
            columns: [{ name: "id", category: "integer", nativeType: "int" }],
            rows: [{ id: 1 }],
          };
        },
      });
    } finally {
      Object.defineProperty(process, "platform", platform);
    }
    expect(fs.readFileSync(outputPath, "utf8")).toBe("id\n1\n");
    expect(fs.readdirSync(tempDir)).toEqual(["export.out"]);
    expect(vscodeMock.showErrorMessage).not.toHaveBeenCalled();
  });

  it("fails without publishing if the filesystem does not support hard links", async () => {
    const loadChunks = vi.fn(async function* () {
      yield {
        columns: [
          { name: "id", category: "integer" as const, nativeType: "int" },
        ],
        rows: [{ id: 1 }],
      };
    });
    vi.doMock("node:fs", async () => {
      const actual = await vi.importActual<typeof import("node:fs")>("node:fs");
      return {
        ...actual,
        linkSync: vi.fn(() => {
          throw new Error("hard links unsupported");
        }),
      };
    });
    try {
      const { exportTableDataAsCsv } = await import(
        "../../src/extension/utils/exportService"
      );
      await exportTableDataAsCsv({
        fileName: "unsupported_link",
        loadChunks,
      });
    } finally {
      vi.doUnmock("node:fs");
    }
    expect(fs.existsSync(outputPath)).toBe(false);
    expect(loadChunks).not.toHaveBeenCalled();
    expect(fs.readdirSync(tempDir)).toEqual([]);
    expect(vscodeMock.showErrorMessage).toHaveBeenCalledWith(
      expect.stringContaining("hard links unsupported"),
    );
  });

  it("does not overwrite a file created just before publishing a new export", async () => {
    vi.doMock("node:fs", async () => {
      const actual = await vi.importActual<typeof import("node:fs")>("node:fs");
      return {
        ...actual,
        linkSync: vi.fn((source: unknown, destination: unknown) => {
          if (String(destination) === outputPath) {
            actual.writeFileSync(outputPath, "competing file", { flag: "wx" });
          }
          return actual.linkSync(source as never, destination as never);
        }),
      };
    });
    try {
      const { exportTableDataAsCsv } = await import(
        "../../src/extension/utils/exportService"
      );
      await exportTableDataAsCsv({
        fileName: "raced",
        loadChunks: async function* () {
          yield {
            columns: [{ name: "id", category: "integer", nativeType: "int" }],
            rows: [{ id: 1 }],
          };
        },
      });
    } finally {
      vi.doUnmock("node:fs");
    }
    expect(fs.readFileSync(outputPath, "utf8")).toBe("competing file");
    expect(fs.readdirSync(tempDir)).toEqual(["export.out"]);
    expect(vscodeMock.showErrorMessage).toHaveBeenCalled();
  });

  it("does not replace a file created during chunk loading after a new name was chosen", async () => {
    const { exportTableDataAsCsv } = await import(
      "../../src/extension/utils/exportService"
    );
    await exportTableDataAsCsv({
      fileName: "raced_during_read",
      loadChunks: async function* () {
        fs.writeFileSync(outputPath, "other writer", { flag: "wx" });
        yield {
          columns: [{ name: "id", category: "integer", nativeType: "int" }],
          rows: [{ id: 1 }],
        };
      },
    });
    expect(fs.readFileSync(outputPath, "utf8")).toBe("other writer");
    expect(fs.readdirSync(tempDir)).toEqual(["export.out"]);
    expect(vscodeMock.showErrorMessage).toHaveBeenCalled();
  });

  it.skipIf(process.platform === "win32")(
    "preserves an existing output when replacing it fails",
    async () => {
      const previousOutput = "previous export\n";
      fs.writeFileSync(outputPath, previousOutput);
      vi.doMock("node:fs", async () => {
        const actual =
          await vi.importActual<typeof import("node:fs")>("node:fs");
        return {
          ...actual,
          renameSync: vi.fn((source: unknown, destination: unknown) => {
            if (
              String(source).includes(".tmp") &&
              String(destination) === outputPath
            ) {
              throw new Error("replace failed");
            }
            return actual.renameSync(source as never, destination as never);
          }),
        };
      });

      try {
        const { exportTableDataAsCsv } = await import(
          "../../src/extension/utils/exportService"
        );
        await exportTableDataAsCsv({
          fileName: "failed_replacement",
          loadChunks: async function* () {
            yield {
              columns: [{ name: "id", category: "integer", nativeType: "int" }],
              rows: [{ id: 1 }],
            };
          },
        });
      } finally {
        vi.doUnmock("node:fs");
      }

      expect(fs.readFileSync(outputPath, "utf8")).toBe(previousOutput);
      expect(
        fs.readdirSync(tempDir).some((name) => name.includes(".rapidb-")),
      ).toBe(false);
      expect(vscodeMock.showErrorMessage).toHaveBeenCalledWith(
        expect.stringContaining("replace failed"),
      );
    },
  );

  it.each([
    "csv",
    "json",
  ])("keeps the old %s file on a failed Elasticsearch cursor export", async (format) => {
    fs.writeFileSync(outputPath, "old export\n");
    const exporters = await import("../../src/extension/utils/exportService");
    const { service, client, snapshots } = createElasticsearchPitMock(9, 3);
    const realSearch = required(client.search.getMockImplementation());
    client.search
      .mockImplementationOnce(realSearch)
      .mockRejectedValueOnce(new Error("cursor fetch failed"));
    const exporter =
      format === "csv"
        ? exporters.exportTableDataAsCsv
        : exporters.exportTableDataAsJson;
    await exporter({
      fileName: "records",
      loadChunks: (signal) =>
        service.exportAll(
          "es-pit",
          "default",
          "indices",
          "records",
          500,
          null,
          [],
          signal,
        ),
    });
    expect(fs.readFileSync(outputPath, "utf8")).toBe("old export\n");
    expect(fs.readdirSync(tempDir)).toEqual(["export.out"]);
    expect(snapshots.size).toBe(0);
    expect(vscodeMock.showInformationMessage).not.toHaveBeenCalled();
    expect(vscodeMock.showErrorMessage).toHaveBeenCalledWith(
      expect.stringContaining("cursor fetch failed"),
    );
  });

  it("keeps the old file if Elasticsearch PIT close fails after all rows were written", async () => {
    fs.writeFileSync(outputPath, "old export\n");
    const { exportTableDataAsJson } = await import(
      "../../src/extension/utils/exportService"
    );
    const { service, client } = createElasticsearchPitMock(9, 3);
    client.closePointInTime.mockRejectedValueOnce(
      new Error("PIT close failed"),
    );
    await exportTableDataAsJson({
      fileName: "records",
      loadChunks: (signal) =>
        service.exportAll(
          "es-pit",
          "default",
          "indices",
          "records",
          500,
          null,
          [],
          signal,
        ),
    });
    expect(fs.readFileSync(outputPath, "utf8")).toBe("old export\n");
    expect(fs.readdirSync(tempDir)).toEqual(["export.out"]);
    expect(vscodeMock.showInformationMessage).not.toHaveBeenCalled();
    expect(vscodeMock.showErrorMessage).toHaveBeenCalledWith(
      expect.stringContaining("PIT close failed"),
    );
  });

  it("cancels Elasticsearch mid-fetch, closes PIT and retains the old file", async () => {
    fs.writeFileSync(outputPath, "old export\n");
    let cancel!: () => void;
    vscodeMock.withProgress.mockImplementation(async (_options, task) =>
      task(
        {},
        {
          onCancellationRequested: (listener: () => void) => {
            cancel = listener;
            return { dispose: vi.fn() };
          },
        },
      ),
    );
    const { exportTableDataAsCsv } = await import(
      "../../src/extension/utils/exportService"
    );
    const { service, client, snapshots } = createElasticsearchPitMock(9, 3);
    const realSearch = required(client.search.getMockImplementation());
    client.search
      .mockImplementationOnce(realSearch)
      .mockImplementationOnce(async (_request, options) => {
        cancel();
        options?.signal?.throwIfAborted();
        throw new Error("Expected cancellation");
      });
    await exportTableDataAsCsv({
      fileName: "records",
      loadChunks: (signal) =>
        service.exportAll(
          "es-pit",
          "default",
          "indices",
          "records",
          500,
          null,
          [],
          signal,
        ),
    });
    expect(fs.readFileSync(outputPath, "utf8")).toBe("old export\n");
    expect(fs.readdirSync(tempDir)).toEqual(["export.out"]);
    expect(snapshots.size).toBe(0);
    expect(vscodeMock.showInformationMessage).not.toHaveBeenCalled();
    expect(vscodeMock.showErrorMessage).not.toHaveBeenCalled();
  });

  it("silently cancels a real SDK HTTP RequestAbortedError and preserves the old export", async () => {
    fs.writeFileSync(outputPath, "old export\n");
    let cancel: (() => void) | undefined;
    vscodeMock.withProgress.mockImplementation(async (_options, task) =>
      task(
        {},
        {
          onCancellationRequested: (listener: () => void) => {
            cancel = listener;
            return { dispose: vi.fn() };
          },
        },
      ),
    );
    const { exportTableDataAsCsv } = await import(
      "../../src/extension/utils/exportService"
    );
    const fixture = await createElasticsearchHttpFixture({ stallSearch: true });
    let sdkErrorName: string | undefined;
    try {
      const exporting = exportTableDataAsCsv({
        fileName: "records",
        loadChunks: async function* (signal) {
          yield {
            columns: [{ name: "_id", category: "text", nativeType: "text" }],
            rows: [{ _id: "one" }],
          };
          try {
            await fixture.client.search(
              { query: { match_all: {} } },
              { signal, requestTimeout: 1000 },
            );
          } catch (error) {
            sdkErrorName = error instanceof Error ? error.name : undefined;
            throw error;
          }
        },
      });
      await fixture.searchStarted.promise;
      required(cancel)();
      await exporting;
      expect(sdkErrorName).toBe("RequestAbortedError");
      expect(fs.readFileSync(outputPath, "utf8")).toBe("old export\n");
      expect(fs.readdirSync(tempDir)).toEqual(["export.out"]);
      expect(vscodeMock.showInformationMessage).not.toHaveBeenCalled();
      expect(vscodeMock.showErrorMessage).not.toHaveBeenCalled();
    } finally {
      await fixture.close();
    }
  });

  it("reports an Elastic response-size RequestAbortedError racing with cancellation", async () => {
    const previousOutput = "old export\n";
    fs.writeFileSync(outputPath, previousOutput);
    let cancel: (() => void) | undefined;
    vscodeMock.withProgress.mockImplementation(async (_options, task) =>
      task(
        {},
        {
          onCancellationRequested: (listener: () => void) => {
            cancel = listener;
            return { dispose: vi.fn() };
          },
        },
      ),
    );

    const { exportTableDataAsCsv } = await import(
      "../../src/extension/utils/exportService"
    );
    const responseSizeError = new errors.RequestAbortedError(
      "The content length (100) is bigger than the maximum allowed string (10)",
    );
    await exportTableDataAsCsv({
      fileName: "response_size_failure_racing_with_cancel",
      loadChunks: async function* () {
        yield {
          columns: [{ name: "id", category: "integer", nativeType: "int" }],
          rows: [],
        };
        required(cancel)();
        throw responseSizeError;
      },
    });

    expect(fs.readFileSync(outputPath, "utf8")).toBe(previousOutput);
    expect(fs.readdirSync(tempDir)).toEqual(["export.out"]);
    expect(vscodeMock.showInformationMessage).not.toHaveBeenCalled();
    expect(vscodeMock.showErrorMessage).toHaveBeenCalledWith(
      expect.stringContaining(responseSizeError.message),
    );
  });

  it.each(
    (["sorted", "filtered"] as const).flatMap((mode) =>
      (["page-only", "full"] as const).flatMap((scope) =>
        (["abort", "deadline"] as const).map((reason) => ({
          mode,
          scope,
          reason,
        })),
      ),
    ),
  )("retains the old file for a real HTTP $mode $scope export on $reason", async ({
    mode,
    scope,
    reason,
  }) => {
    fs.writeFileSync(outputPath, "old export\n");
    let cancel: (() => void) | undefined;
    vscodeMock.withProgress.mockImplementation(async (_options, task) =>
      task(
        {},
        {
          onCancellationRequested: (listener: () => void) => {
            cancel = listener;
            return { dispose: vi.fn() };
          },
        },
      ),
    );
    const exporters = await import("../../src/extension/utils/exportService");
    const fixture = await createElasticsearchHttpFixture({
      stallSearch: true,
      window: 10000,
    });
    const sort =
      mode === "sorted" ? { column: "_id", direction: "asc" as const } : null;
    const filters =
      mode === "filtered"
        ? [{ column: "_id", operator: "like" as const, value: "doc" }]
        : [];
    const exporter =
      mode === "sorted"
        ? exporters.exportTableDataAsCsv
        : exporters.exportTableDataAsJson;
    try {
      const exporting = exporter({
        fileName: "records",
        loadChunks: (signal) =>
          scope === "full"
            ? fixture.service.exportAll(
                "es-http",
                "default",
                "indices",
                "records",
                500,
                sort,
                filters,
                signal,
              )
            : (async function* () {
                const page = await fixture.service.getPage(
                  "es-http",
                  "default",
                  "indices",
                  "records",
                  1,
                  25,
                  filters,
                  sort,
                  true,
                  signal,
                );
                signal.throwIfAborted();
                yield { columns: page.columns, rows: page.rows };
              })(),
      });
      await fixture.searchStarted.promise;
      if (reason === "abort") required(cancel)();
      await exporting;
      await vi.waitFor(() => {
        expect(fixture.pendingSearches.size).toBe(0);
        expect(fixture.activeReaders).toBe(0);
      });
      expect(fs.readFileSync(outputPath, "utf8")).toBe("old export\n");
      expect(fs.readdirSync(tempDir)).toEqual(["export.out"]);
      expect(vscodeMock.showInformationMessage).not.toHaveBeenCalled();
      if (reason === "abort")
        expect(vscodeMock.showErrorMessage).not.toHaveBeenCalled();
      else
        expect(vscodeMock.showErrorMessage).toHaveBeenCalledWith(
          expect.stringContaining("timed out"),
        );
      expect(fixture.searches).toHaveLength(1);
      expect(fixture.requests).toHaveLength(4);
      expect(fixture.pits.size).toBe(0);
    } finally {
      await fixture.close();
    }
  });
});
