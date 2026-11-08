import { act, renderHook } from "@testing-library/react";
import type React from "react";
import { describe, expect, it } from "vitest";
import type { ColumnTypeMeta } from "../../src/shared/tableTypes";
import { NULL_SENTINEL } from "../../src/shared/tableTypes";
import { useCellSelection } from "../../src/webview/components/table/useCellSelection";
import {
  formatNormalizedPasteValue,
  parseTsv,
  validatePasteData,
  validatePasteValue,
} from "../../src/webview/utils/pasteUtils";

function makeColumn(
  name: string,
  category: ColumnTypeMeta["category"],
  nativeType: string = category,
  nullable = true,
): ColumnTypeMeta {
  return {
    name,
    type: nativeType,
    nativeType,
    nullable,
    isPrimaryKey: false,
    isForeignKey: false,
    category,
    filterable: true,
    filterOperators: ["eq"],
    valueSemantics: "plain",
  };
}

const SQL_INTEGER = makeColumn("qty", "integer", "integer");
const FLOAT = makeColumn("ratio", "float", "float8");
const DECIMAL = makeColumn("amount", "decimal", "numeric(10,2)");
const DYNAMO_NUMBER = makeColumn("price", "integer", "number");
const MONGO_INT = makeColumn("price", "integer", "int");
const MONGO_LONG = makeColumn("price", "integer", "long");
const TEXT = makeColumn("label", "text", "text");
const MONGO_DATE = makeColumn("seen_at", "datetime", "date");
const MONGO_TIMESTAMP = makeColumn("ts", "datetime", "timestamp");
const SQL_DATE = makeColumn("born", "date", "date");

function mouseEvent(shiftKey = false): React.MouseEvent {
  return {
    target: document.createElement("div"),
    button: 0,
    shiftKey,
    clientX: 0,
    clientY: 0,
    preventDefault: () => {},
  } as unknown as React.MouseEvent;
}

describe("decimal comma detection ignores whitespace before the comma", () => {
  it.each([
    SQL_INTEGER,
    FLOAT,
    DECIMAL,
  ])("rejects a leading zero group split by whitespace for $category", (column) => {
    for (const value of ["0 ,125", "0 ,125", "0, 125"]) {
      expect(validatePasteValue(value, column).valid).toBe(false);
    }
  });

  it.each([
    FLOAT,
    DECIMAL,
  ])("rejects an ambiguous comma split by whitespace for $category", (column) => {
    for (const value of ["1 ,234", "1 ,234"]) {
      const result = validatePasteValue(value, column);
      expect(result.valid).toBe(false);
      expect(result.error).toContain("Ambiguous");
    }
  });
});

const MONGO_OPTIONS = { mongoTypes: true };

describe("schemaless numeric fields", () => {
  it.each([
    DYNAMO_NUMBER,
    MONGO_INT,
    MONGO_LONG,
  ])("rejects a single comma group for $nativeType even when sampled as integer", (column) => {
    for (const value of ["2,500", "-2,500", "$2,500"]) {
      const result = validatePasteValue(value, column, false, MONGO_OPTIONS);
      expect(result.valid).toBe(false);
      expect(result.error).toContain("Ambiguous");
    }
  });

  it.each([
    DYNAMO_NUMBER,
    MONGO_INT,
    MONGO_LONG,
  ])("keeps unambiguous values working for $nativeType", (column) => {
    expect(validatePasteValue("2500", column, false, MONGO_OPTIONS)).toEqual({
      valid: true,
      coercedValue: "2500",
    });
    expect(
      validatePasteValue("2,500,000", column, false, MONGO_OPTIONS),
    ).toEqual({
      valid: true,
      coercedValue: "2500000",
    });
  });

  it("keeps grouped integers working for typed SQL integer columns", () => {
    expect(validatePasteValue("2,500", SQL_INTEGER)).toEqual({
      valid: true,
      coercedValue: "2500",
    });
  });
});

describe("decimal comma error messages", () => {
  it.each([
    "1,000",
    "$1,000",
  ])("suggests unambiguous spellings for %s in a decimal column", (value) => {
    const result = validatePasteValue(value, DECIMAL);
    expect(result.valid).toBe(false);
    expect(result.error).toContain("Ambiguous");
    expect(result.error).toContain("1000");
    expect(result.error).toContain("1,000.00");
  });

  it.each([
    SQL_INTEGER,
    FLOAT,
    DECIMAL,
  ])("reports 1,23 as a decimal comma, not as ambiguous, for $category", (column) => {
    const result = validatePasteValue("1,23", column);
    expect(result.valid).toBe(false);
    expect(result.error).toContain("Invalid number value");
    expect(result.error).toContain("decimal separator");
    expect(result.error).not.toContain("Ambiguous");
  });

  it.each([
    FLOAT,
    DECIMAL,
  ])("rejects -0,5 as a decimal comma for $category", (column) => {
    const result = validatePasteValue("-0,5", column);
    expect(result.valid).toBe(false);
    expect(result.error).toContain("decimal separator");
  });

  it("rejects 0,5 for integer columns", () => {
    const result = validatePasteValue("0,5", SQL_INTEGER);
    expect(result.valid).toBe(false);
    expect(result.error).toContain("decimal separator");
  });
});

describe("blank cells pasted into numeric columns", () => {
  it.each([
    SQL_INTEGER,
    FLOAT,
    DECIMAL,
    DYNAMO_NUMBER,
  ])("treats an unquoted empty value as NULL for nullable $category", (column) => {
    const result = validatePasteValue("", column, false);
    expect(result).toEqual({ valid: true, coercedValue: null });
    expect(formatNormalizedPasteValue("", result.coercedValue)).toBe(
      NULL_SENTINEL,
    );
  });

  it("pastes a single blank Excel cell as NULL", () => {
    const pasteData = parseTsv("\r\n");
    expect(pasteData.rows).toEqual([[""]]);
    const result = validatePasteData(
      pasteData,
      0,
      0,
      [SQL_INTEGER],
      1,
      () => true,
    );
    expect(result.errors).toEqual([]);
    const [cell] = result.rows[0];
    expect(formatNormalizedPasteValue(cell.value, cell.normalized)).toBe(
      NULL_SENTINEL,
    );
  });

  it("still rejects a quoted empty value", () => {
    const pasteData = parseTsv('""');
    const result = validatePasteData(
      pasteData,
      0,
      0,
      [SQL_INTEGER],
      1,
      () => true,
    );
    expect(result.errors).toHaveLength(1);
    expect(result.errors[0].message).toContain("Empty value");
  });

  it("still rejects an unquoted empty value in a NOT NULL column", () => {
    const column = makeColumn("qty", "integer", "integer", false);
    const result = validatePasteValue("", column);
    expect(result.valid).toBe(false);
    expect(result.error).toContain("Empty value");
    expect(result.error).not.toContain("use NULL");
  });

  it("keeps unquoted empty text as an empty string", () => {
    const result = validatePasteValue("", TEXT);
    expect(result).toEqual({ valid: true, coercedValue: "" });
    expect(formatNormalizedPasteValue("", result.coercedValue)).toBe("");
  });
});

describe("RapiDB copy never emits an unquoted empty field", () => {
  function copy(
    values: unknown[][],
    isColumnCollapsed?: (col: number) => boolean,
  ): string | null {
    const { result } = renderHook(() =>
      useCellSelection({
        rowCount: values.length,
        colCount: values[0].length,
        getCellValue: (row, col) => values[row][col],
        scrollRef: { current: null },
        getCellFromPoint: () => null,
        isColumnCollapsed,
      }),
    );
    act(() => result.current.handleCellMouseDown(0, 0, mouseEvent()));
    act(() =>
      result.current.handleCellMouseDown(
        values.length - 1,
        values[0].length - 1,
        mouseEvent(true),
      ),
    );
    return result.current.copySelection();
  }

  it("quotes empty strings and marks missing values as NULL", () => {
    const text = copy([
      ["", null, undefined, "x"],
      ["y", "", "", ""],
    ]);
    const parsed = parseTsv(text ?? "");
    parsed.rows.forEach((row, rowIndex) => {
      row.forEach((value, colIndex) => {
        if (value === "") {
          expect(parsed.quoted?.[rowIndex]?.[colIndex]).toBe(true);
        }
      });
    });
  });

  it("does not emit blank lines when every selected column is collapsed", () => {
    const text = copy(
      [
        [1, 2],
        [3, 4],
      ],
      () => true,
    );
    expect(text).toBe("");
  });
});

describe("quoted string values in MongoDB date fields", () => {
  it.each([
    MONGO_DATE,
    MONGO_TIMESTAMP,
  ])("accepts quoted text without digits for $nativeType", (column) => {
    for (const value of ["'N/A'", '"pending"', '  "pending"  ', "''"]) {
      expect(validatePasteValue(value, column, false, MONGO_OPTIONS)).toEqual({
        valid: true,
        coercedValue: value,
      });
    }
  });

  it("accepts a TSV cell that carries a quoted Mongo string", () => {
    const pasteData = parseTsv('"""pending"""');
    expect(pasteData.rows).toEqual([['"pending"']]);
    expect(
      validatePasteValue(
        pasteData.rows[0][0],
        MONGO_DATE,
        pasteData.quoted?.[0]?.[0],
        MONGO_OPTIONS,
      ).valid,
    ).toBe(true);
  });

  it("rejects unmatched quotes and unquoted text", () => {
    for (const value of [`"N/A'`, '"', "pending"]) {
      expect(
        validatePasteValue(value, MONGO_DATE, false, MONGO_OPTIONS).valid,
      ).toBe(false);
    }
  });

  it("keeps rejecting quoted text in SQL date columns", () => {
    expect(validatePasteValue('"pending"', SQL_DATE).valid).toBe(false);
  });
});
