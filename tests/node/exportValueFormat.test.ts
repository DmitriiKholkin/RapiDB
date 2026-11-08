import { describe, expect, it } from "vitest";
import {
  formatExportCellValue,
  formatTableCsvExportValue,
  normalizeJsonExportValue,
  serializeJsonExportRecord,
} from "../../src/extension/utils/exportValueFormat";

describe("export Date formatting", () => {
  it.each([
    "2024-07-04T12:00:11.123Z",
    "2024-07-04T12:00:11.000Z",
    "0001-01-01T00:00:00.001Z",
    "+010000-01-01T00:00:00.999Z",
    "-000001-12-31T23:59:59.999Z",
  ])("preserves the exact UTC instant %s", (iso) => {
    const date = new Date(iso);
    expect(formatExportCellValue(date)).toBe(iso);
    expect(formatTableCsvExportValue(date, "datetime")).toBe(iso);
    expect(normalizeJsonExportValue(date, "datetime")).toBe(iso);
    expect(new Date(formatExportCellValue(date)).getTime()).toBe(
      date.getTime(),
    );
  });

  it("preserves nested dates in CSV and JSON including arrays", () => {
    const date = new Date("2024-07-04T12:00:11.123Z");
    const value = { date, nested: [date, { date }] };
    const expected = {
      date: date.toISOString(),
      nested: [date.toISOString(), { date: date.toISOString() }],
    };
    expect(JSON.parse(formatTableCsvExportValue(value, "json"))).toEqual(
      expected,
    );
    expect(
      JSON.parse(
        serializeJsonExportRecord([
          { key: "value", sourceKey: "value", category: "json", value },
        ]),
      ),
    ).toEqual({ value: expected });
  });

  it("retains existing invalid date and non-date handling", () => {
    const invalid = new Date(NaN);
    expect(formatExportCellValue(invalid)).toBe("");
    expect(normalizeJsonExportValue(invalid, "datetime")).toBeNull();
    expect(
      JSON.parse(
        serializeJsonExportRecord([
          {
            key: "value",
            sourceKey: "value",
            category: "json",
            value: [invalid, null, true, "text", 42],
          },
        ]),
      ),
    ).toEqual({ value: [null, null, true, "text", 42] });
    expect(formatExportCellValue(null)).toBe("");
    expect(formatExportCellValue(123n)).toBe("123");
  });
});
