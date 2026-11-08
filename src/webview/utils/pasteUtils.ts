import { normalizeNumericToken } from "../../shared/numericNormalization";
import type { ColumnTypeMeta } from "../../shared/tableTypes";
import { NULL_SENTINEL } from "../../shared/tableTypes";

const TSV_NULL_MARKER = "NULL";
const DATE_KEYWORD_RE =
  /^(?:[+-]?infinity|epoch|now|today|tomorrow|yesterday)$/i;
const QUOTED_VALUE_RE = /^(["']).*\1$/s;
const MONGO_NUMBER_NATIVE_TYPES = new Set(["int", "long"]);
const QUOTED_STRING_DATE_NATIVE_TYPES = new Set(["date", "timestamp"]);

export interface PasteValidationOptions {
  mongoTypes?: boolean;
}

type DecimalCommaIssue =
  | { kind: "decimalComma" }
  | { kind: "ambiguous"; integerPart: string }
  | { kind: "invalid" };

function findDecimalCommaIssue(
  value: string,
  rejectSingleGroupComma: boolean,
): DecimalCommaIssue | null {
  const compact = value.replace(/\s+/g, "");
  const integerPart = /[\d,]+/.exec(compact)?.[0];
  if (!integerPart?.includes(",")) {
    return null;
  }
  const groups = integerPart.split(",");
  const isSingleComma =
    groups.length === 2 &&
    groups[0] !== "" &&
    groups[1] !== "" &&
    !compact.includes(".");
  if (isSingleComma && (groups[0].startsWith("0") || groups[1].length !== 3)) {
    return { kind: "decimalComma" };
  }
  if (groups[0].startsWith("0")) {
    return { kind: "invalid" };
  }
  return rejectSingleGroupComma && isSingleComma
    ? { kind: "ambiguous", integerPart }
    : null;
}

function rejectsSingleGroupComma(
  column: ColumnTypeMeta,
  options: PasteValidationOptions,
): boolean {
  return (
    column.category !== "integer" ||
    column.nativeType === "number" ||
    (options.mongoTypes === true &&
      MONGO_NUMBER_NATIVE_TYPES.has(column.nativeType))
  );
}

function decimalCommaError(
  issue: DecimalCommaIssue,
  value: string,
  column: ColumnTypeMeta,
): string {
  switch (issue.kind) {
    case "decimalComma":
      return `Invalid number value "${value}" for column "${column.name}": a comma cannot be used as the decimal separator (use a dot)`;
    case "ambiguous": {
      const plain = issue.integerPart.replace(/,/g, "");
      const suggestion =
        column.category === "integer"
          ? plain
          : `${plain} or ${issue.integerPart}.00`;
      return `Ambiguous number value "${value}" for column "${column.name}": the comma could be a decimal or thousands separator (write ${suggestion})`;
    }
    case "invalid":
      return `Invalid number value "${value}" for column "${column.name}"`;
  }
}

function acceptsQuotedStringDate(
  column: ColumnTypeMeta,
  options: PasteValidationOptions,
): boolean {
  return (
    options.mongoTypes === true &&
    column.category === "datetime" &&
    QUOTED_STRING_DATE_NATIVE_TYPES.has(column.nativeType)
  );
}

export interface PasteData {
  rows: string[][];
  quoted?: boolean[][];
}

export interface PasteValidationError {
  rowIndex: number;
  columnIndex: number;
  columnName: string;
  value: string;
  message: string;
}

export interface PasteValidationResult {
  errors: PasteValidationError[];
  rows: Array<
    Array<{
      column: ColumnTypeMeta;
      value: string;
      normalized: unknown;
    }>
  >;
}

function serializeTsvCell(value: string | null): string {
  if (value === null) {
    return TSV_NULL_MARKER;
  }
  return value === "" || value === TSV_NULL_MARKER || /["\t\r\n]/.test(value)
    ? `"${value.replace(/"/g, '""')}"`
    : value;
}

export function serializeTsv(
  rows: readonly (readonly (string | null)[])[],
): string {
  return rows.map((row) => row.map(serializeTsvCell).join("\t")).join("\n");
}

function hasValidClosingQuote(text: string, openingIndex: number): boolean {
  for (let index = openingIndex + 1; index < text.length; index++) {
    if (text[index] !== '"') {
      continue;
    }

    if (text[index + 1] === '"') {
      index++;
      continue;
    }

    const next = text[index + 1];
    return (
      next === undefined || next === "\t" || next === "\r" || next === "\n"
    );
  }

  return false;
}

export function parseTsv(text: string): PasteData {
  if (text.length === 0) {
    return { rows: [] };
  }

  const rows: string[][] = [];
  const quoted: boolean[][] = [];
  let row: string[] = [];
  let rowQuoted: boolean[] = [];
  let field = "";
  let fieldQuoted = false;
  let inQuotes = false;
  let atFieldStart = true;
  let endedWithRowDelimiter = false;

  const finishField = () => {
    row.push(field);
    rowQuoted.push(fieldQuoted);
    field = "";
    fieldQuoted = false;
    atFieldStart = true;
  };

  const finishRow = () => {
    finishField();
    rows.push(row);
    quoted.push(rowQuoted);
    row = [];
    rowQuoted = [];
  };

  for (let index = 0; index < text.length; index++) {
    const char = text[index];

    if (inQuotes) {
      if (char === '"') {
        if (text[index + 1] === '"') {
          field += '"';
          index++;
        } else {
          inQuotes = false;
        }
      } else {
        field += char;
      }
      endedWithRowDelimiter = false;
      continue;
    }

    if (char === '"' && atFieldStart && hasValidClosingQuote(text, index)) {
      inQuotes = true;
      fieldQuoted = true;
      atFieldStart = false;
      endedWithRowDelimiter = false;
      continue;
    }

    if (char === "\t") {
      finishField();
      endedWithRowDelimiter = false;
      continue;
    }

    if (char === "\r" || char === "\n") {
      finishRow();
      if (char === "\r" && text[index + 1] === "\n") {
        index++;
      }
      endedWithRowDelimiter = true;
      continue;
    }

    field += char;
    atFieldStart = false;
    endedWithRowDelimiter = false;
  }

  if (!endedWithRowDelimiter) {
    finishRow();
  }

  return { rows, quoted };
}

export function validatePasteValue(
  value: string,
  column: ColumnTypeMeta,
  quoted = false,
  options: PasteValidationOptions = {},
): { valid: boolean; coercedValue: unknown; error?: string } {
  if (value === NULL_SENTINEL || (!quoted && value === TSV_NULL_MARKER)) {
    if (column.nullable) {
      return { valid: true, coercedValue: null };
    }
    return {
      valid: false,
      coercedValue: null,
      error: `Column "${column.name}" does not allow NULL values`,
    };
  }

  switch (column.category) {
    case "integer":
    case "float":
    case "decimal": {
      if (value === "" && !quoted && column.nullable) {
        return { valid: true, coercedValue: null };
      }
      if (value.trim() === "") {
        return {
          valid: false,
          coercedValue: value,
          error: column.nullable
            ? `Empty value is not a valid number for column "${column.name}" (use NULL)`
            : `Empty value is not a valid number for column "${column.name}"`,
        };
      }
      const commaIssue = findDecimalCommaIssue(
        value,
        rejectsSingleGroupComma(column, options),
      );
      if (commaIssue) {
        return {
          valid: false,
          coercedValue: value,
          error: decimalCommaError(commaIssue, value, column),
        };
      }
      const normalized = normalizeNumericToken(
        value,
        column.category === "float",
      );
      if (
        normalized !== null &&
        (column.category !== "integer" || /^[+-]?\d+$/.test(normalized))
      ) {
        return { valid: true, coercedValue: normalized };
      }
      return {
        valid: false,
        coercedValue: value,
        error: `Invalid number value "${value}" for column "${column.name}"`,
      };
    }

    case "boolean": {
      const lower = value.trim().toLowerCase();
      if (lower === "true" || lower === "yes") {
        return { valid: true, coercedValue: "true" };
      }
      if (lower === "false" || lower === "no") {
        return { valid: true, coercedValue: "false" };
      }
      if (lower === "1" || lower === "0") {
        return { valid: true, coercedValue: lower };
      }
      return {
        valid: false,
        coercedValue: value,
        error: `Invalid boolean value "${value}" for column "${column.name}"`,
      };
    }

    case "date":
    case "datetime": {
      const trimmed = value.trim();
      if (
        !/\d/.test(trimmed) &&
        !DATE_KEYWORD_RE.test(trimmed) &&
        !(
          acceptsQuotedStringDate(column, options) &&
          QUOTED_VALUE_RE.test(trimmed)
        )
      ) {
        return {
          valid: false,
          coercedValue: value,
          error: `Invalid date value "${value}" for column "${column.name}"`,
        };
      }
      return { valid: true, coercedValue: value };
    }

    case "uuid": {
      const uuidRegex =
        /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
      if (!uuidRegex.test(value)) {
        return {
          valid: false,
          coercedValue: value,
          error: `Invalid UUID value "${value}" for column "${column.name}"`,
        };
      }
      return { valid: true, coercedValue: value };
    }

    default:
      return { valid: true, coercedValue: value };
  }
}

export function validatePasteData(
  pasteData: PasteData,
  startRow: number,
  startCol: number,
  columns: ColumnTypeMeta[],
  totalRows: number,
  canEditColumn: (column: ColumnTypeMeta) => boolean,
  options: PasteValidationOptions = {},
): PasteValidationResult {
  const errors: PasteValidationError[] = [];
  const rows: PasteValidationResult["rows"] = [];

  for (let r = 0; r < pasteData.rows.length; r++) {
    const row = pasteData.rows[r];
    const targetRow = startRow + r;
    const normalizedRow: PasteValidationResult["rows"][number] = [];

    if (targetRow >= totalRows) {
      errors.push({
        rowIndex: targetRow,
        columnIndex: startCol,
        columnName: columns[startCol]?.name ?? "",
        value: "",
        message: `Paste would exceed table bounds (row ${targetRow + 1} does not exist)`,
      });
      rows.push(normalizedRow);
      continue;
    }

    for (let c = 0; c < row.length; c++) {
      const value = row[c];
      const targetCol = startCol + c;

      if (targetCol >= columns.length) {
        errors.push({
          rowIndex: targetRow,
          columnIndex: targetCol,
          columnName: "",
          value,
          message: `Paste would exceed table bounds (column index ${targetCol} out of range)`,
        });
        continue;
      }

      const column = columns[targetCol];
      if (!column) continue;

      if (!canEditColumn(column)) {
        errors.push({
          rowIndex: targetRow,
          columnIndex: targetCol,
          columnName: column.name,
          value,
          message: `Cannot paste into read-only column "${column.name}"`,
        });
        continue;
      }

      if (column.isPrimaryKey) {
        errors.push({
          rowIndex: targetRow,
          columnIndex: targetCol,
          columnName: column.name,
          value,
          message: `Cannot paste into primary key column "${column.name}"`,
        });
        continue;
      }

      const validation = validatePasteValue(
        value,
        column,
        pasteData.quoted?.[r]?.[c],
        options,
      );
      if (!validation.valid) {
        errors.push({
          rowIndex: targetRow,
          columnIndex: targetCol,
          columnName: column.name,
          value,
          message: validation.error ?? "Validation failed",
        });
        continue;
      }

      normalizedRow.push({
        column,
        value,
        normalized: validation.coercedValue,
      });
    }

    rows.push(normalizedRow);
  }

  return { errors, rows };
}

export function formatNormalizedPasteValue(
  originalValue: string,
  normalized: unknown,
): string {
  if (
    normalized === null &&
    (originalValue === NULL_SENTINEL ||
      originalValue === TSV_NULL_MARKER ||
      originalValue === "")
  ) {
    return NULL_SENTINEL;
  }
  if (normalized === null || normalized === undefined) {
    return originalValue;
  }
  if (typeof normalized === "string") {
    return normalized;
  }
  if (typeof normalized === "number" || typeof normalized === "boolean") {
    return String(normalized);
  }
  if (typeof normalized === "bigint") {
    return normalized.toString();
  }
  return originalValue;
}
