import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { QueryColumnMeta } from "../../src/extension/dbDrivers/types";
import {
  formatExportCellValue,
  formatTableCsvExportValue,
  serializeJsonExportRecord,
} from "../../src/extension/utils/exportValueFormat";
import { formatQueryResult } from "../../src/extension/utils/queryResultFormatting";

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
      window: {
        showSaveDialog: vi.fn(async () => ({ fsPath: outputPath })),
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
