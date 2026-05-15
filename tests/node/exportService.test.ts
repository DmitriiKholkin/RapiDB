import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

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
});
