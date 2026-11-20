import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import mssql from "mssql";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { MSSQLDriver } from "../../src/extension/dbDrivers/mssql";
import type { QueryColumnMeta } from "../../src/extension/dbDrivers/types";
import {
  formatExportCellValue,
  formatTableCsvExportValue,
  serializeJsonExportRecord,
} from "../../src/extension/utils/exportValueFormat";
import { formatQueryResult } from "../../src/extension/utils/queryResultFormatting";
import {
  MockMssqlRequest,
  mockMssqlSessionPool,
} from "../helpers/mssqlQuerySessionHarness";

function jsonRecord(value: unknown, category: QueryColumnMeta["category"]) {
  return serializeJsonExportRecord([
    { key: "v", sourceKey: "v", category, value },
  ]);
}

describe("export value fidelity", () => {
  let tempDir: string;
  let outputPath: string;

  beforeEach(() => {
    vi.resetModules();
    tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "rapidb-export-fidelity-"));
    outputPath = path.join(tempDir, "export.out");
    vi.doMock("vscode", () => ({
      Uri: { file: (fsPath: string) => ({ fsPath }) },
      ProgressLocation: { Notification: 15 },
      workspace: { getConfiguration: () => ({ get: () => false }) },
      window: {
        showSaveDialog: vi.fn(async () => ({ fsPath: outputPath })),
        showWarningMessage: vi.fn(async () => "Replace File"),
        withProgress: vi.fn(async (_options, task) =>
          task(
            {},
            { onCancellationRequested: vi.fn(() => ({ dispose: vi.fn() })) },
          ),
        ),
        showInformationMessage: vi.fn(),
        showErrorMessage: vi.fn(),
      },
    }));
  });

  afterEach(() => {
    fs.rmSync(tempDir, { recursive: true, force: true });
  });

  async function exportQuery(
    format: "csv" | "json",
    columns: string[],
    columnMeta: QueryColumnMeta[] | undefined,
    rows: Record<string, unknown>[],
  ): Promise<string> {
    const exporters = await import("../../src/extension/utils/exportService");
    const formatted = formatQueryResult(
      { columns, columnMeta, rows, rowCount: rows.length, executionTimeMs: 1 },
      rows.length,
    );
    if (format === "csv") await exporters.exportQueryResultsAsCsv(formatted);
    else await exporters.exportQueryResultsAsJson(formatted);
    return fs.readFileSync(outputPath, "utf8");
  }

  it("does not prefix real numbers in CSV when the column category is unknown", async () => {
    const output = await exportQuery(
      "csv",
      ["n", "neg_float", "text_num", "cmd", "formula", "inf"],
      undefined,
      [
        {
          __col_0: -5,
          __col_1: -1.5e-7,
          __col_2: "-5",
          __col_3: "-cmd",
          __col_4: "=1+1",
          __col_5: -Infinity,
        },
      ],
    );
    expect(output).toBe(
      "n,neg_float,text_num,cmd,formula,inf\n" +
        "-5,-1.5e-7,'-5,'-cmd,'=1+1,'-Infinity\n",
    );
  });

  it("preserves numeric-looking array strings in query CSV and JSON exports", async () => {
    const columns = ["untyped", "array", "custom"];
    const columnMeta: QueryColumnMeta[] = [
      { category: null },
      { category: "array" },
      { category: "array", nativeType: "app.custom[]" },
    ];
    const values = ["123", 123, ["123", 123]];
    const rows = [{ __col_0: values, __col_1: values, __col_2: values }];
    const text = '["123",123,["123",123]]';
    const csvCell = `"${text.replaceAll('"', '""')}"`;

    expect(await exportQuery("csv", columns, columnMeta, rows)).toBe(
      `untyped,array,custom\n${csvCell},${csvCell},${csvCell}\n`,
    );
    expect(
      JSON.parse(await exportQuery("json", columns, columnMeta, rows)),
    ).toEqual([{ untyped: text, array: values, custom: values }]);
  });

  it("does not prefix real numbers in table CSV with a null category", async () => {
    const { exportTableDataAsCsv } = await import(
      "../../src/extension/utils/exportService"
    );
    await exportTableDataAsCsv({
      fileName: "mixed",
      loadChunks: async function* () {
        yield {
          columns: [
            { name: "mixed", category: null as never, nativeType: "" },
            { name: "money", category: "decimal", nativeType: "money" },
            { name: "f", category: "float", nativeType: "float8" },
          ],
          rows: [
            { mixed: -5, money: "-$5.00", f: "-Infinity" },
            { mixed: -12345678901234567890n, money: "-5.00", f: "-0" },
            { mixed: "-5+A1", money: "-5", f: -0 },
          ],
        };
      },
    });
    expect(fs.readFileSync(outputPath, "utf8")).toBe(
      "mixed,money,f\n-5,'-$5.00,'-Infinity\n" +
        "-12345678901234567890,-5.00,-0\n'-5+A1,-5,-0\n",
    );
  });

  it.each([
    '{"x":1,"x":2}',
    '{"b":1,"2":2,"a":{"z":0,"y":1}}',
    '{"big":123456789012345678901234567890,"d":1.10,"e":1E+2,"neg":-0}',
    '[{"x":1,"x":2},"s",null]',
    '"text"',
  ])("preserves PostgreSQL json text %s in JSON and CSV export", async (text) => {
    const meta: QueryColumnMeta[] = [{ category: "json", nativeType: "json" }];
    const json = await exportQuery("json", ["payload"], meta, [
      { __col_0: text },
    ]);
    expect(json).toBe(`[\n{"payload":${text}}\n]\n`);
    expect(() => JSON.parse(json)).not.toThrow();

    const csv = await exportQuery("csv", ["payload"], meta, [
      { __col_0: text },
    ]);
    expect(csv).toBe(`payload\n"${text.replace(/"/g, '""')}"\n`);
  });

  it("preserves SQL Server temporal sql_variant values in a selected query result set", async () => {
    // Vitest's forked worker isolates this from the host and other test files.
    vi.stubEnv("TZ", "Europe/Berlin");
    expect(new Date(Date.UTC(2024, 0, 2)).getTimezoneOffset()).toBe(-60);

    // The property bytes include one scale byte for the newer clock types.
    const fixtures = [
      ["d", 0x28, "", "46460b", "2024-01-02"],
      ["t", 0x29, "07", "87f0831802", "00:15:00.1234567"],
      ["t2", 0x29, "02", "6e0000", "00:00:01.10"],
      ["dt", 0x3d, "", "ebb00000e30c0000", "2024-01-02 00:00:10.997"],
      ["sdt", 0x3a, "", "44b19600", "2024-03-31 02:30:00"],
      ["dt2", 0x2a, "07", "44da7df4149f460b", "2024-03-31 02:30:00.1234500"],
      [
        "dto",
        0x2b,
        "07",
        "b01cb8c8c0fe460b5901",
        "2024-07-05 04:45:00.1230000+05:45",
      ],
      ["n", 0x38, "", "2a000000", 42],
    ] as const;
    const tediousParser = require("tedious/lib/value-parser") as {
      readValue: (
        buffer: Buffer,
        offset: number,
        metadata: { type: { name: string } },
        options: { useUTC: boolean },
      ) => { value: unknown; offset: number };
    };
    const values = fixtures.map(([, type, properties, payload]) => {
      const data = Buffer.from(properties + payload, "hex");
      const value = Buffer.alloc(6 + data.length);
      value.writeUInt32LE(data.length + 2);
      value[4] = type;
      value[5] = properties.length / 2;
      data.copy(value, 6);
      // Use a nonzero offset and a following value to catch read overruns.
      const parsed = tediousParser.readValue(
        Buffer.concat([Buffer.from([0xaa]), value, Buffer.from([0xbb])]),
        1,
        { type: { name: "Variant" } },
        { useUTC: false },
      );
      expect(parsed.offset).toBe(value.length + 1);
      return parsed.value;
    });
    const ordinaryDate = tediousParser.readValue(
      Buffer.from("0346460b", "hex"),
      0,
      { type: { name: "Date" } },
      { useUTC: false },
    ).value;

    const text = "2024-03-31T02:30:00.1234500";
    const request = new MockMssqlRequest();
    request.execute.mockImplementation(async () => {
      request.emit("recordset", [{ name: "first", type: mssql.Date }]);
      request.emit("row", [ordinaryDate]);
      request.emit("recordset", [
        ...fixtures.map(([name]) => ({ name, type: mssql.Variant })),
        { name: "text", type: mssql.NVarChar },
      ]);
      request.emit("row", [...values, text]);
      return { recordsets: [], recordset: [], rowsAffected: [], output: {} };
    });
    const driver = new MSSQLDriver({
      id: "mssql-temporal-export",
      name: "MSSQL",
      type: "mssql",
    });
    (driver as unknown as { pool: unknown }).pool = mockMssqlSessionPool([
      request,
    ]);
    const result = formatQueryResult(
      await driver.query("SELECT ordinary_date; SELECT temporal_values"),
      2,
    );
    expect(result.rows).toEqual([{ __col_0: "2024-01-02" }]);
    const selected = result.resultSets?.[1];
    if (!selected) throw new Error("Expected a second SQL Server result set");

    const { exportQueryResultsAsCsv, exportQueryResultsAsJson } = await import(
      "../../src/extension/utils/exportService"
    );
    await exportQueryResultsAsCsv(selected);
    expect(fs.readFileSync(outputPath, "utf8")).toBe(
      `d,t,t2,dt,sdt,dt2,dto,n,text\n${fixtures.map((entry) => entry[4]).join(",")},${text}\n`,
    );
    await exportQueryResultsAsJson(selected);
    expect(JSON.parse(fs.readFileSync(outputPath, "utf8"))).toEqual([
      Object.fromEntries([
        ...fixtures.map(([name, , , , expected]) => [name, expected]),
        ["text", text],
      ]),
    ]);
  });

  it("embeds validated json text with surrounding whitespace removed", () => {
    expect(jsonRecord(' \n{"x":1, "x":2}\t', "json")).toBe(
      '{"v":{"x":1, "x":2}}',
    );
    expect(formatTableCsvExportValue(' {"x":1,"x":2} ', "json")).toBe(
      ' {"x":1,"x":2} ',
    );
  });

  it("keeps reserved-tag wrapping and invalid-text handling for json values", () => {
    expect(jsonRecord("null", "json")).toBe(
      '{"v":{"$rapiDB":{"type":"json-null"}}}',
    );
    expect(jsonRecord('{"$rapiDB":1,"$rapiDB":2}', "json")).toBe(
      '{"v":{"$rapiDB":{"type":"json-value","value":{"$rapiDB":1,"$rapiDB":2}}}}',
    );
    expect(jsonRecord("{not json", "json")).toBe('{"v":"{not json"}');
    expect(jsonRecord('{"a":1} ', "json")).toBe('{"v":{"a":1}}');
    expect(() =>
      JSON.parse(jsonRecord('{"a":1} /* x */', "json")),
    ).not.toThrow();
  });

  it("preserves the sign of negative zero in CSV and JSON", async () => {
    expect(formatExportCellValue(-0)).toBe("-0");
    expect(formatTableCsvExportValue(-0, "float")).toBe("-0");
    expect(jsonRecord(-0, "float")).toBe('{"v":-0}');
    expect(jsonRecord({ a: -0, b: [-0, 0] }, "json")).toBe(
      '{"v":{"a":-0,"b":[-0,0]}}',
    );
    expect(jsonRecord(0, "float")).toBe('{"v":0}');

    const meta: QueryColumnMeta[] = [{ category: "float" }];
    expect(await exportQuery("csv", ["f"], meta, [{ __col_0: -0 }])).toBe(
      "f\n-0\n",
    );
    const json = await exportQuery("json", ["f"], meta, [{ __col_0: -0 }]);
    expect(json).toBe('[\n{"f":-0}\n]\n');
    expect(Object.is(JSON.parse(json)[0].f, -0)).toBe(true);
  });
});
