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

export function parseTsv(text: string): PasteData {
  const lines = text.split("\n");
  const rows: string[][] = [];

  for (const line of lines) {
    if (line.trim() === "") continue;
    const cells = line.split("\t");
    rows.push(cells);
  }

  return { rows };
}

export function validatePasteValue(
  value: string,
  column: ColumnTypeMeta,
): { valid: boolean; coercedValue: unknown; error?: string } {
  if (value === "" || value === NULL_SENTINEL || value === "NULL") {
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
      const normalized = normalizeNumericToken(value);
      if (normalized !== null) {
        return { valid: true, coercedValue: normalized };
      }
      const num = Number(value);
      if (Number.isNaN(num)) {
        return {
          valid: false,
          coercedValue: value,
          error: `Invalid number value "${value}" for column "${column.name}"`,
        };
      }
      return { valid: true, coercedValue: value };
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
  if (
    originalValue === "" ||
    originalValue === NULL_SENTINEL ||
    originalValue === "NULL"
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
