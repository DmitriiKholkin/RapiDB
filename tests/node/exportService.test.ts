import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { marshall } from "@aws-sdk/util-dynamodb";
import { errors } from "@elastic/transport";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { FilterExpression } from "../../src/extension/dbDrivers/types";
import type { ChunkedExportData } from "../../src/extension/utils/exportService";
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
              ? { LastEvaluatedKey: marshall({ pk: "B", sk: selected.at(-1) }) }
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
      filters.push({ column: "value", operator: "ilike", value: "hello" });
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
      expect(output).toBe("pk,sk,value\nB,0001,hello\nB,0002,hello\n");
    else
      expect(JSON.parse(output)).toEqual([
        { pk: "B", sk: "0001", value: "hello" },
        { pk: "B", sk: "0002", value: "hello" },
      ]);
    expect(chunks).toHaveLength(2);
    for (const chunk of chunks) {
      expect(
        chunk.columns.find((column) => column.name === "value"),
      ).toMatchObject({ nativeType: "string", category: "text" });
      expect(chunk.rows[0]?.value).toBe("hello");
    }
    expect(
      send.mock.calls.some(
        ([command]) => command.constructor.name === "QueryCommand",
      ),
    ).toBe(true);
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
    expect(output).toContain('"t_decimal128":123456789.987654311');
    expect(output).not.toContain('"t_decimal128":"123456789.987654311"');
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
      "x,y\nX1,\nX2,Y2\n,Y3\nX4,\n,\n",
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
          yield { columns: [column], rows: [{ amount: "first", later: "Y" }] };
          return;
        }
        yield { columns: [column], rows: [{ amount: "first" }] };
        if (mode === "new descriptor") {
          yield {
            columns: [column, { ...column, name: "later" }],
            rows: [{ later: "Y" }],
          };
        } else if (mode === "undeclared row key") {
          yield { columns: [column], rows: [{ amount: "second", later: "Y" }] };
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
      '"y":9007199254740993.1250',
    );
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
    expect(output).toContain('"t_decimal128":123456789.987654311');
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

  it("restores a directory swapped in before the backup rename and cleans the temp file", async () => {
    fs.writeFileSync(outputPath, "previous export\n");
    let swappedDirectory = false;
    vi.doMock("node:fs", async () => {
      const actual = await vi.importActual<typeof import("node:fs")>("node:fs");
      return {
        ...actual,
        renameSync: vi.fn((source: unknown, destination: unknown) => {
          const sourcePath = String(source);
          const destinationPath = String(destination);
          if (sourcePath.includes(".tmp") && destinationPath === outputPath) {
            throw new Error("replace failed");
          }
          if (
            sourcePath === outputPath &&
            destinationPath.includes(".rapidb-") &&
            destinationPath.endsWith(".bak")
          ) {
            actual.unlinkSync(outputPath);
            actual.mkdirSync(outputPath);
            actual.writeFileSync(
              path.join(outputPath, "keep.txt"),
              "keep this directory",
            );
            swappedDirectory = true;
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
        fileName: "raced_directory_export",
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

    expect(swappedDirectory).toBe(true);
    expect(fs.lstatSync(outputPath).isDirectory()).toBe(true);
    expect(fs.readFileSync(path.join(outputPath, "keep.txt"), "utf8")).toBe(
      "keep this directory",
    );
    expect(fs.readdirSync(tempDir)).toEqual(["export.out"]);
    expect(vscodeMock.showErrorMessage).toHaveBeenCalledWith(
      expect.stringContaining("Cannot export to a directory"),
    );
  });

  it("uses the regular-file backup fallback when direct replacement fails", async () => {
    fs.writeFileSync(outputPath, "previous export\n");
    let directReplacementAttempts = 0;
    vi.doMock("node:fs", async () => {
      const actual = await vi.importActual<typeof import("node:fs")>("node:fs");
      return {
        ...actual,
        renameSync: vi.fn((source: unknown, destination: unknown) => {
          if (
            String(source).includes(".tmp") &&
            String(destination) === outputPath &&
            directReplacementAttempts++ === 0
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
        fileName: "fallback_export",
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

    expect(directReplacementAttempts).toBe(2);
    expect(fs.readFileSync(outputPath, "utf8")).not.toBe("previous export\n");
    expect(fs.readdirSync(tempDir)).toEqual(["export.out"]);
    expect(vscodeMock.showErrorMessage).not.toHaveBeenCalled();
  });

  it("restores the original file when inspecting the moved backup fails", async () => {
    const previousOutput = "previous export\n";
    fs.writeFileSync(outputPath, previousOutput);
    let backupPath: string | undefined;
    let directReplacementAttempts = 0;
    vi.doMock("node:fs", async () => {
      const actual = await vi.importActual<typeof import("node:fs")>("node:fs");
      return {
        ...actual,
        renameSync: vi.fn((source: unknown, destination: unknown) => {
          if (
            String(source).includes(".tmp") &&
            String(destination) === outputPath &&
            directReplacementAttempts++ === 0
          ) {
            throw new Error("replace failed");
          }
          return actual.renameSync(source as never, destination as never);
        }),
        lstatSync: vi.fn((targetPath: unknown) => {
          if (String(targetPath).endsWith(".bak")) {
            backupPath = String(targetPath);
            throw new Error("backup inspection failed");
          }
          return actual.lstatSync(targetPath as never);
        }),
      };
    });

    try {
      const { exportTableDataAsCsv } = await import(
        "../../src/extension/utils/exportService"
      );
      await exportTableDataAsCsv({
        fileName: "inspection_failure_export",
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
    expect(backupPath).toBeDefined();
    expect(fs.existsSync(backupPath as string)).toBe(false);
    expect(fs.readdirSync(tempDir)).toEqual(["export.out"]);
    expect(vscodeMock.showErrorMessage).toHaveBeenCalledWith(
      expect.stringContaining("backup inspection failed"),
    );
  });

  it("reports the backup path and both causes when restoring the original fails", async () => {
    const previousOutput = "previous export\n";
    fs.writeFileSync(outputPath, previousOutput);
    let backupPath: string | undefined;
    let directReplacementAttempts = 0;
    vi.doMock("node:fs", async () => {
      const actual = await vi.importActual<typeof import("node:fs")>("node:fs");
      return {
        ...actual,
        renameSync: vi.fn((source: unknown, destination: unknown) => {
          const sourcePath = String(source);
          const destinationPath = String(destination);
          if (sourcePath.includes(".tmp") && destinationPath === outputPath) {
            if (directReplacementAttempts++ === 0) {
              throw new Error("direct replacement failed");
            }
            throw new Error("backup install failed");
          }
          if (sourcePath.endsWith(".bak") && destinationPath === outputPath) {
            throw new Error("backup restore failed");
          }
          if (sourcePath === outputPath && destinationPath.endsWith(".bak")) {
            backupPath = destinationPath;
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
        fileName: "restore_failure_export",
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

    expect(backupPath).toBeDefined();
    expect(fs.readFileSync(backupPath as string, "utf8")).toBe(previousOutput);
    expect(fs.existsSync(outputPath)).toBe(false);
    const errorMessage = String(vscodeMock.showErrorMessage.mock.calls[0]?.[0]);
    expect(errorMessage).toContain(backupPath);
    expect(errorMessage).toContain("backup install failed");
    expect(errorMessage).toContain("backup restore failed");
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

  it("reports replacement and restore failures when cancellation races with replacement", async () => {
    const previousOutput = "previous export\n";
    fs.writeFileSync(outputPath, previousOutput);
    let backupPath: string | undefined;
    let cancel: (() => void) | undefined;
    let directReplacementAttempts = 0;
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
        renameSync: vi.fn((source: unknown, destination: unknown) => {
          const sourcePath = String(source);
          const destinationPath = String(destination);
          if (sourcePath.includes(".tmp") && destinationPath === outputPath) {
            if (directReplacementAttempts++ === 0) {
              throw new Error("direct replacement failed");
            }
            required(cancel)();
            throw new Error("backup install failed");
          }
          if (sourcePath.endsWith(".bak") && destinationPath === outputPath) {
            throw new Error("backup restore failed");
          }
          if (sourcePath === outputPath && destinationPath.endsWith(".bak")) {
            backupPath = destinationPath;
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
        fileName: "cancel_during_replacement",
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

    expect(backupPath).toBeDefined();
    expect(fs.readFileSync(backupPath as string, "utf8")).toBe(previousOutput);
    expect(fs.existsSync(outputPath)).toBe(false);
    const errorMessage = String(vscodeMock.showErrorMessage.mock.calls[0]?.[0]);
    expect(errorMessage).toContain(backupPath);
    expect(errorMessage).toContain("backup install failed");
    expect(errorMessage).toContain("backup restore failed");
    expect(vscodeMock.showInformationMessage).not.toHaveBeenCalled();
  });

  it("preserves replacement diagnostics when temp cleanup also fails after restore fails", async () => {
    fs.writeFileSync(outputPath, "previous export\n");
    let backupPath: string | undefined;
    let directReplacementAttempts = 0;
    vi.doMock("node:fs", async () => {
      const actual = await vi.importActual<typeof import("node:fs")>("node:fs");
      return {
        ...actual,
        renameSync: vi.fn((source: unknown, destination: unknown) => {
          const sourcePath = String(source);
          const destinationPath = String(destination);
          if (sourcePath.includes(".tmp") && destinationPath === outputPath) {
            if (directReplacementAttempts++ === 0) {
              throw new Error("direct replacement failed");
            }
            throw new Error("backup install failed");
          }
          if (sourcePath.endsWith(".bak") && destinationPath === outputPath) {
            throw new Error("backup restore failed");
          }
          if (sourcePath === outputPath && destinationPath.endsWith(".bak")) {
            backupPath = destinationPath;
          }
          return actual.renameSync(source as never, destination as never);
        }),
        rmSync: vi.fn((targetPath: unknown, options: unknown) => {
          if (String(targetPath).endsWith(".tmp")) {
            throw new Error("temporary cleanup failed");
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
        fileName: "restore_and_temp_cleanup_failure_export",
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

    expect(backupPath).toBeDefined();
    expect(fs.readFileSync(backupPath as string, "utf8")).toBe(
      "previous export\n",
    );
    expect(fs.existsSync(outputPath)).toBe(false);
    const errorMessage = String(vscodeMock.showErrorMessage.mock.calls[0]?.[0]);
    expect(errorMessage).toContain(backupPath);
    expect(errorMessage).toContain("backup install failed");
    expect(errorMessage).toContain("backup restore failed");
    expect(errorMessage).toContain("temporary cleanup failed");
  });

  it("does not restore the backup over a destination created during replacement", async () => {
    const previousOutput = "previous export\n";
    const competingOutput = "competing export\n";
    fs.writeFileSync(outputPath, previousOutput);
    let backupPath: string | undefined;
    let directReplacementAttempts = 0;
    vi.doMock("node:fs", async () => {
      const actual = await vi.importActual<typeof import("node:fs")>("node:fs");
      return {
        ...actual,
        renameSync: vi.fn((source: unknown, destination: unknown) => {
          const sourcePath = String(source);
          const destinationPath = String(destination);
          if (sourcePath.includes(".tmp") && destinationPath === outputPath) {
            if (directReplacementAttempts++ === 0) {
              throw new Error("direct replacement failed");
            }
            actual.writeFileSync(outputPath, competingOutput);
            throw new Error("backup install failed");
          }
          if (sourcePath === outputPath && destinationPath.endsWith(".bak")) {
            backupPath = destinationPath;
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
        fileName: "competing_destination_export",
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

    expect(fs.readFileSync(outputPath, "utf8")).toBe(competingOutput);
    expect(backupPath).toBeDefined();
    expect(fs.readFileSync(backupPath as string, "utf8")).toBe(previousOutput);
    const errorMessage = String(vscodeMock.showErrorMessage.mock.calls[0]?.[0]);
    expect(errorMessage).toContain(backupPath);
    expect(errorMessage).toContain("A destination already exists");
    expect(errorMessage).toContain("backup install failed");
  });

  it("reports the leftover backup path when cleanup fails after installation", async () => {
    const previousOutput = "previous export\n";
    fs.writeFileSync(outputPath, previousOutput);
    let backupPath: string | undefined;
    let directReplacementAttempts = 0;
    vi.doMock("node:fs", async () => {
      const actual = await vi.importActual<typeof import("node:fs")>("node:fs");
      return {
        ...actual,
        renameSync: vi.fn((source: unknown, destination: unknown) => {
          const sourcePath = String(source);
          const destinationPath = String(destination);
          if (
            sourcePath.includes(".tmp") &&
            destinationPath === outputPath &&
            directReplacementAttempts++ === 0
          ) {
            throw new Error("replace failed");
          }
          if (sourcePath === outputPath && destinationPath.endsWith(".bak")) {
            backupPath = destinationPath;
          }
          return actual.renameSync(source as never, destination as never);
        }),
        unlinkSync: vi.fn((targetPath: unknown) => {
          if (String(targetPath) === backupPath) {
            throw new Error("backup cleanup failed");
          }
          return actual.unlinkSync(targetPath as never);
        }),
      };
    });

    try {
      const { exportTableDataAsCsv } = await import(
        "../../src/extension/utils/exportService"
      );
      await exportTableDataAsCsv({
        fileName: "backup_cleanup_export",
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

    expect(backupPath).toBeDefined();
    expect(fs.readFileSync(outputPath, "utf8")).not.toBe(previousOutput);
    expect(fs.readFileSync(backupPath as string, "utf8")).toBe(previousOutput);
    const errorMessage = String(vscodeMock.showErrorMessage.mock.calls[0]?.[0]);
    expect(errorMessage).toContain(backupPath);
    expect(errorMessage).toContain("backup cleanup failed");
    expect(errorMessage).toContain("Export was installed");
  });

  it("preserves an existing output when replacing it fails", async () => {
    const previousOutput = "previous export\n";
    fs.writeFileSync(outputPath, previousOutput);
    vi.doMock("node:fs", async () => {
      const actual = await vi.importActual<typeof import("node:fs")>("node:fs");
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
  });

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
            // Let the actual SDK error reach exportService's signal-based
            // normalization, rather than replacing it with a mock AbortError.
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
      expect(fixture.requests).toHaveLength(1);
      expect(fixture.pits.size).toBe(0);
    } finally {
      await fixture.close();
    }
  });
});
