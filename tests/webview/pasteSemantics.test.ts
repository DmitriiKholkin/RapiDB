import { act, renderHook } from "@testing-library/react";
import type React from "react";
import { describe, expect, it } from "vitest";
import type { ColumnTypeMeta } from "../../src/shared/tableTypes";
import { NULL_SENTINEL } from "../../src/shared/tableTypes";
import { useCellSelection } from "../../src/webview/components/table/useCellSelection";
import {
  formatNormalizedPasteValue,
  parseTsv,
  validatePasteValue,
} from "../../src/webview/utils/pasteUtils";

function makeColumn(
  name: string,
  category: ColumnTypeMeta["category"],
  nullable = true,
): ColumnTypeMeta {
  return {
    name,
    type: category,
    nativeType: category,
    nullable,
    isPrimaryKey: false,
    isForeignKey: false,
    category,
    filterable: true,
    filterOperators: ["eq"],
    valueSemantics: category === "boolean" ? "boolean" : "plain",
  };
}

const INTEGER = makeColumn("qty", "integer");
const FLOAT = makeColumn("ratio", "float");
const DECIMAL = makeColumn("amount", "decimal");
const BOOLEAN = makeColumn("active", "boolean");
const DATE = makeColumn("born", "date");
const DATETIME = makeColumn("seen_at", "datetime");

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

describe("numeric paste with comma separators", () => {
  it.each([
    FLOAT,
    DECIMAL,
  ])("rejects a comma-decimal value with a leading zero group for $category", (column) => {
    expect(validatePasteValue("0,125", column).valid).toBe(false);
  });

  it("rejects a leading zero group for integer columns", () => {
    expect(validatePasteValue("0,125", INTEGER).valid).toBe(false);
  });

  it.each([
    FLOAT,
    DECIMAL,
  ])("rejects an ambiguous single comma before three digits for $category", (column) => {
    for (const value of ["1,234", "-1,234", "$1,234", "(1,234)"]) {
      const result = validatePasteValue(value, column);
      expect(result.valid).toBe(false);
      expect(result.error).toContain("Ambiguous");
    }
  });

  it("keeps unambiguous grouped values working", () => {
    expect(validatePasteValue("1,234", INTEGER)).toEqual({
      valid: true,
      coercedValue: "1234",
    });
    expect(validatePasteValue("1,234,567", INTEGER)).toEqual({
      valid: true,
      coercedValue: "1234567",
    });
    for (const column of [FLOAT, DECIMAL]) {
      expect(validatePasteValue("1,234,567", column)).toEqual({
        valid: true,
        coercedValue: "1234567",
      });
      expect(validatePasteValue("1,234.5", column)).toEqual({
        valid: true,
        coercedValue: "1234.5",
      });
    }
  });
});

describe("empty numeric paste", () => {
  it.each([
    INTEGER,
    FLOAT,
    DECIMAL,
  ])("rejects quoted empty and whitespace-only values for $category", (column) => {
    expect(validatePasteValue("", column, true).valid).toBe(false);
    expect(validatePasteValue("  ", column).valid).toBe(false);
  });
});

describe("copying MISSING / DEFAULT cells", () => {
  it("copies undefined cells as the bare NULL marker, distinct from empty text", () => {
    const values: unknown[] = [undefined, null, ""];
    const { result } = renderHook(() =>
      useCellSelection({
        rowCount: 1,
        colCount: values.length,
        getCellValue: (_row, col) => values[col],
        scrollRef: { current: null },
        getCellFromPoint: () => null,
      }),
    );

    act(() => result.current.handleCellMouseDown(0, 0, mouseEvent()));
    act(() => result.current.handleCellMouseDown(0, 2, mouseEvent(true)));

    const text = result.current.copySelection();
    expect(text).toBe('NULL\tNULL\t""');

    const parsed = parseTsv(text ?? "");
    const [missing] = parsed.rows[0];
    const pasted = validatePasteValue(
      missing,
      INTEGER,
      parsed.quoted?.[0]?.[0],
    );
    expect(pasted).toEqual({ valid: true, coercedValue: null });
    expect(formatNormalizedPasteValue(missing, pasted.coercedValue)).toBe(
      NULL_SENTINEL,
    );
  });
});

describe("boolean paste normalization", () => {
  it.each([
    ["yes", "true"],
    ["YES", "true"],
    ["no", "false"],
    ["No", "false"],
    ["TRUE", "true"],
    ["false", "false"],
    ["1", "1"],
    ["0", "0"],
  ])("normalizes %s to a driver-parseable %s", (value, expected) => {
    const result = validatePasteValue(value, BOOLEAN);
    expect(result).toEqual({ valid: true, coercedValue: expected });
    expect(formatNormalizedPasteValue(value, result.coercedValue)).toBe(
      expected,
    );
  });

  it("still rejects unknown boolean text", () => {
    expect(validatePasteValue("maybe", BOOLEAN).valid).toBe(false);
  });
});

describe("date paste validation", () => {
  it.each([
    DATE,
    DATETIME,
  ])("accepts database date literals that JS Date cannot parse for $category", (column) => {
    for (const value of [
      "infinity",
      "-infinity",
      "0044-03-15 BC",
      "2024-01-15 10:00:00 BC",
      "0000-00-00",
    ]) {
      expect(validatePasteValue(value, column)).toEqual({
        valid: true,
        coercedValue: value,
      });
    }
  });

  it("passes calendar-invalid dates through unchanged for the driver to reject", () => {
    expect(validatePasteValue("2024-02-30", DATE)).toEqual({
      valid: true,
      coercedValue: "2024-02-30",
    });
  });

  it("rejects obviously non-date input", () => {
    for (const value of ["not-a-date", "", "hello world"]) {
      expect(validatePasteValue(value, DATE).valid).toBe(false);
    }
  });
});
