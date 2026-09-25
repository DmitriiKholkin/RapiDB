import { normalizeNumericToken } from "../../shared/numericNormalization";
import type { ColumnTypeMeta } from "../../shared/tableTypes";
import { NULL_SENTINEL } from "../../shared/tableTypes";

export interface PasteCellTarget {
  rowIndex: number;
  columnIndex: number;
  columnName: string;
  column: ColumnTypeMeta;
}

export interface PasteData {
  rows: string[][];
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

function serializeTsvCell(value: string): string {
  return value === "" || /["\t\r\n]/.test(value)
    ? `"${value.replace(/"/g, '""')}"`
    : value;
}

export function serializeTsv(rows: readonly (readonly string[])[]): string {
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
  let row: string[] = [];
  let field = "";
  let inQuotes = false;
  let atFieldStart = true;
  let endedWithRowDelimiter = false;

  const finishField = () => {
    row.push(field);
    field = "";
    atFieldStart = true;
  };

  const finishRow = () => {
    finishField();
    rows.push(row);
    row = [];
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

  return { rows };
}

export function validatePasteValue(
  value: string,
  column: ColumnTypeMeta,
): { valid: boolean; coercedValue: unknown; error?: string } {
  if (value === NULL_SENTINEL || value === "NULL") {
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
      if (value.trim() === "") {
        return { valid: true, coercedValue: value };
      }
      const normalized = normalizeNumericToken(
        value,
        column.category === "float",
      );
      // Integer drivers use decimal integer literals (including BigInt), not
      // rounded JS numbers. Leave native ranges and decimal scale to the driver.
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
      const lower = value.toLowerCase();
      if (
        lower === "true" ||
        lower === "false" ||
        lower === "1" ||
        lower === "0" ||
        lower === "yes" ||
        lower === "no"
      ) {
        return { valid: true, coercedValue: value };
      }
      return {
        valid: false,
        coercedValue: value,
        error: `Invalid boolean value "${value}" for column "${column.name}"`,
      };
    }

    case "date":
    case "datetime": {
      const date = new Date(value);
      if (Number.isNaN(date.getTime())) {
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

      const validation = validatePasteValue(value, column);
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
  if (originalValue === NULL_SENTINEL || originalValue === "NULL") {
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
