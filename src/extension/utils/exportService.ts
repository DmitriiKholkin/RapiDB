import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import * as vscode from "vscode";
import type {
  ColumnTypeMeta,
  QueryColumnMeta,
  TypeCategory,
} from "../dbDrivers/types";
import { csvCell } from "./csvUtils";
import { normalizeUnknownError } from "./errorHandling";

const DOWNLOADS_DIRECTORY = "Downloads";
const CSV_EXTENSION = "csv";
const JSON_EXTENSION = "json";
const LINE_BREAK = "\n";

type ExportFormat = typeof CSV_EXTENSION | typeof JSON_EXTENSION;

export interface QueryResultExport {
  columns: readonly string[];
  columnMeta?: readonly QueryColumnMeta[];
  rows: readonly Record<string, unknown>[];
}

export interface ChunkedExportData {
  columns: ReadonlyArray<
    Pick<ColumnTypeMeta, "name" | "category" | "nativeType">
  >;
  rows: ReadonlyArray<Record<string, unknown>>;
}

interface ExportRequest {
  defaultFileName: string;
  format: ExportFormat;
  progressTitle: string;
  successLabel: string;
  errorLabel: string;
  write: (filePath: string, signal: AbortSignal) => Promise<void>;
}

type ExportColumnDescriptor = {
  key: string;
  sourceKey: string;
  category?: TypeCategory | null;
  nativeType?: string;
};

type JsonExportScalar = null | boolean | number | string | RawJsonLiteral;

type JsonExportValue =
  | JsonExportScalar
  | JsonExportValue[]
  | { [key: string]: JsonExportValue };

type RawJsonLiteral = {
  readonly __rapidbRawJsonLiteral: unique symbol;
  readonly literal: string;
};

const JSON_NUMBER_LITERAL_RE = /^-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?$/;

export async function exportQueryResultsAsCsv(
  result: QueryResultExport,
): Promise<void> {
  await runExport({
    defaultFileName: "query_results",
    format: CSV_EXTENSION,
    progressTitle: "RapiDB: Exporting query results…",
    successLabel: "query results",
    errorLabel: "CSV export failed",
    write: async (filePath, signal) => {
      await writeQueryResultsCsv(filePath, result, signal);
    },
  });
}

export async function exportQueryResultsAsJson(
  result: QueryResultExport,
): Promise<void> {
  await runExport({
    defaultFileName: "query_results",
    format: JSON_EXTENSION,
    progressTitle: "RapiDB: Exporting query results…",
    successLabel: "query results",
    errorLabel: "JSON export failed",
    write: async (filePath, signal) => {
      await writeQueryResultsJson(filePath, result, signal);
    },
  });
}

export async function exportTableDataAsCsv(options: {
  fileName: string;
  loadChunks: (signal: AbortSignal) => AsyncIterable<ChunkedExportData>;
}): Promise<void> {
  const { fileName, loadChunks } = options;
  await runExport({
    defaultFileName: fileName,
    format: CSV_EXTENSION,
    progressTitle: `RapiDB: Exporting ${fileName}…`,
    successLabel: fileName,
    errorLabel: "CSV export failed",
    write: async (filePath, signal) => {
      await writeChunkedCsv(filePath, loadChunks(signal), signal);
    },
  });
}

export async function exportTableDataAsJson(options: {
  fileName: string;
  loadChunks: (signal: AbortSignal) => AsyncIterable<ChunkedExportData>;
}): Promise<void> {
  const { fileName, loadChunks } = options;
  await runExport({
    defaultFileName: fileName,
    format: JSON_EXTENSION,
    progressTitle: `RapiDB: Exporting ${fileName} as JSON…`,
    successLabel: fileName,
    errorLabel: "JSON export failed",
    write: async (filePath, signal) => {
      await writeChunkedJson(filePath, loadChunks(signal), signal);
    },
  });
}

async function runExport(request: ExportRequest): Promise<void> {
  const saveUri = await vscode.window.showSaveDialog({
    defaultUri: buildDefaultExportUri(request.defaultFileName, request.format),
    filters: buildExportFilters(request.format),
  });
  if (!saveUri) {
    return;
  }

  try {
    await vscode.window.withProgress(
      {
        location: vscode.ProgressLocation.Notification,
        title: request.progressTitle,
        cancellable: true,
      },
      async (_progress, token) => {
        const abortController = new AbortController();
        const cancelSubscription = token.onCancellationRequested(() => {
          abortController.abort();
        });

        try {
          await request.write(saveUri.fsPath, abortController.signal);
        } finally {
          cancelSubscription.dispose();
        }
      },
    );

    vscode.window.showInformationMessage(
      `[RapiDB] Exported ${request.successLabel} → ${path.basename(saveUri.fsPath)}`,
    );
  } catch (error: unknown) {
    const normalized = normalizeUnknownError(error);
    if (normalized.name === "AbortError") {
      return;
    }

    vscode.window.showErrorMessage(
      `[RapiDB] ${request.errorLabel}: ${normalized.message}`,
    );
  }
}

function buildDefaultExportUri(
  defaultFileName: string,
  format: ExportFormat,
): vscode.Uri {
  return vscode.Uri.file(
    path.join(
      os.homedir(),
      DOWNLOADS_DIRECTORY,
      `${defaultFileName}.${format}`,
    ),
  );
}

function buildExportFilters(format: ExportFormat): Record<string, string[]> {
  return format === CSV_EXTENSION
    ? { "CSV files": [CSV_EXTENSION], "All files": ["*"] }
    : { "JSON files": [JSON_EXTENSION], "All files": ["*"] };
}

async function writeQueryResultsCsv(
  filePath: string,
  result: QueryResultExport,
  signal: AbortSignal,
): Promise<void> {
  const exportColumns = buildQueryExportColumns(result);

  await withWriteStream(filePath, async (writeStream) => {
    throwIfAborted(signal);
    writeStream.write(result.columns.map(csvCell).join(",") + LINE_BREAK);

    for (const row of result.rows) {
      throwIfAborted(signal);
      writeStream.write(
        exportColumns
          .map((column) =>
            csvCell(
              formatTableCsvExportValue(
                row[column.sourceKey],
                column.category ?? null,
              ),
            ),
          )
          .join(",") + LINE_BREAK,
      );
    }
  });
}

async function writeQueryResultsJson(
  filePath: string,
  result: QueryResultExport,
  signal: AbortSignal,
): Promise<void> {
  const exportColumns = buildQueryExportColumns(result);

  await withWriteStream(filePath, async (writeStream) => {
    throwIfAborted(signal);
    writeStream.write("[\n");

    for (let index = 0; index < result.rows.length; index++) {
      throwIfAborted(signal);
      const row = result.rows[index];
      writeStream.write(
        `${index === 0 ? "" : ",\n"}${serializeJsonExportRecord(
          exportColumns.map((column) => ({
            ...column,
            value: row[column.sourceKey],
          })),
        )}`,
      );
    }

    writeStream.write("\n]\n");
  });
}

async function writeChunkedCsv(
  filePath: string,
  chunks: AsyncIterable<ChunkedExportData>,
  signal: AbortSignal,
): Promise<void> {
  await withWriteStream(filePath, async (writeStream) => {
    let headerWritten = false;

    for await (const chunk of chunks) {
      throwIfAborted(signal);
      if (!headerWritten) {
        writeStream.write(
          chunk.columns.map((column) => csvCell(column.name)).join(",") +
            LINE_BREAK,
        );
        headerWritten = true;
      }

      for (const row of chunk.rows) {
        throwIfAborted(signal);
        writeStream.write(
          chunk.columns
            .map((column) =>
              csvCell(
                formatTableCsvExportValue(
                  row[column.name],
                  column.category ?? null,
                ),
              ),
            )
            .join(",") + LINE_BREAK,
        );
      }
    }
  });
}

async function writeChunkedJson(
  filePath: string,
  chunks: AsyncIterable<ChunkedExportData>,
  signal: AbortSignal,
): Promise<void> {
  await withWriteStream(filePath, async (writeStream) => {
    throwIfAborted(signal);
    writeStream.write("[\n");
    let firstRow = true;

    for await (const chunk of chunks) {
      for (const row of chunk.rows) {
        throwIfAborted(signal);
        writeStream.write(
          `${firstRow ? "" : ",\n"}${serializeJsonExportRecord(
            chunk.columns.map((column) => ({
              key: column.name,
              sourceKey: column.name,
              category: column.category ?? null,
              nativeType: column.nativeType,
              value: row[column.name],
            })),
          )}`,
        );
        firstRow = false;
      }
    }

    writeStream.write("\n]\n");
  });
}

function queryColumnKey(index: number): string {
  return `__col_${index}`;
}

function formatExportCellValue(value: unknown): string {
  if (value == null) {
    return "";
  }

  if (typeof value === "bigint") {
    return value.toString();
  }

  if (value instanceof Date) {
    if (Number.isNaN(value.getTime())) {
      return "";
    }

    const pad = (numericValue: number) => String(numericValue).padStart(2, "0");

    return (
      `${value.getUTCFullYear()}-${pad(value.getUTCMonth() + 1)}-${pad(value.getUTCDate())} ` +
      `${pad(value.getUTCHours())}:${pad(value.getUTCMinutes())}:${pad(value.getUTCSeconds())}`
    );
  }

  if (Array.isArray(value) || (value !== null && typeof value === "object")) {
    try {
      return JSON.stringify(value);
    } catch {
      return String(value);
    }
  }

  return String(value);
}

function buildQueryExportColumns(
  result: QueryResultExport,
): ExportColumnDescriptor[] {
  const seenColumnNames = new Map<string, number>();

  return result.columns.map((columnName, index) => {
    const seenCount = seenColumnNames.get(columnName) ?? 0;
    seenColumnNames.set(columnName, seenCount + 1);

    return {
      key: seenCount === 0 ? columnName : `${columnName}_${seenCount + 1}`,
      sourceKey: queryColumnKey(index),
      category: result.columnMeta?.[index]?.category ?? null,
    } satisfies ExportColumnDescriptor;
  });
}

function formatTableCsvExportValue(
  value: unknown,
  category: TypeCategory | null,
): string {
  if (typeof value === "string") {
    const parsed = tryParseStructuredExportValue(value, category);
    if (parsed !== undefined) {
      return formatExportCellValue(parsed);
    }
  }

  return formatExportCellValue(value);
}

function serializeJsonExportRecord(
  entries: ReadonlyArray<
    ExportColumnDescriptor & {
      value: unknown;
    }
  >,
): string {
  return `{${entries
    .map(
      (entry) =>
        `${JSON.stringify(entry.key)}:${stringifyJsonExportValue(
          normalizeJsonExportValue(entry.value, entry.category ?? null),
        )}`,
    )
    .join(",")}}`;
}

function normalizeJsonExportValue(
  value: unknown,
  category: TypeCategory | null,
): JsonExportValue {
  if (value == null) {
    return null;
  }

  if (value instanceof Date) {
    return Number.isNaN(value.getTime()) ? null : formatExportCellValue(value);
  }

  if (typeof value === "boolean") {
    return value;
  }

  if (typeof value === "number") {
    return Number.isFinite(value) ? value : null;
  }

  if (typeof value === "bigint") {
    return rawJsonLiteral(value.toString());
  }

  if (typeof value === "string") {
    const parsed = tryParseStructuredExportValue(value, category);
    if (parsed !== undefined) {
      return normalizeNestedJsonExportValue(parsed);
    }

    if (category === "boolean") {
      const lowered = value.trim().toLowerCase();
      if (lowered === "true") return true;
      if (lowered === "false") return false;
    }

    if (isNumericExportCategory(category)) {
      const numericLiteral = toJsonNumericLiteral(value);
      if (numericLiteral) {
        return rawJsonLiteral(numericLiteral);
      }
    }

    return value;
  }

  return normalizeNestedJsonExportValue(value);
}

function normalizeNestedJsonExportValue(value: unknown): JsonExportValue {
  if (value == null) {
    return null;
  }

  if (value instanceof Date) {
    return Number.isNaN(value.getTime()) ? null : formatExportCellValue(value);
  }

  if (typeof value === "boolean") {
    return value;
  }

  if (typeof value === "number") {
    return Number.isFinite(value) ? value : null;
  }

  if (typeof value === "bigint") {
    return rawJsonLiteral(value.toString());
  }

  if (typeof value === "string") {
    return value;
  }

  if (Array.isArray(value)) {
    return value.map((entry) => normalizeNestedJsonExportValue(entry));
  }

  if (typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>).map(([key, entry]) => [
        key,
        normalizeNestedJsonExportValue(entry),
      ]),
    );
  }

  return String(value);
}

function stringifyJsonExportValue(value: JsonExportValue): string {
  if (isRawJsonLiteral(value)) {
    return value.literal;
  }

  if (value === null) {
    return "null";
  }

  if (typeof value === "boolean") {
    return value ? "true" : "false";
  }

  if (typeof value === "number") {
    return Number.isFinite(value) ? String(value) : "null";
  }

  if (typeof value === "string") {
    return JSON.stringify(value);
  }

  if (Array.isArray(value)) {
    return `[${value.map((entry) => stringifyJsonExportValue(entry)).join(",")}]`;
  }

  return `{${Object.entries(value)
    .map(
      ([key, entry]) =>
        `${JSON.stringify(key)}:${stringifyJsonExportValue(entry)}`,
    )
    .join(",")}}`;
}

function tryParseStructuredExportValue(
  value: string,
  category: TypeCategory | null,
): unknown | undefined {
  if (category !== "json" && category !== "array") {
    return undefined;
  }

  const trimmed = value.trim();
  if (!trimmed) {
    return undefined;
  }

  try {
    return JSON.parse(trimmed) as unknown;
  } catch {
    return undefined;
  }
}

function isNumericExportCategory(category: TypeCategory | null): boolean {
  return (
    category === "integer" || category === "float" || category === "decimal"
  );
}

function toJsonNumericLiteral(value: string): string | null {
  const trimmed = value.trim();
  if (!trimmed) {
    return null;
  }

  const normalized = trimmed.startsWith("+") ? trimmed.slice(1) : trimmed;
  return JSON_NUMBER_LITERAL_RE.test(normalized) ? normalized : null;
}

function rawJsonLiteral(literal: string): RawJsonLiteral {
  return {
    __rapidbRawJsonLiteral: Symbol(
      "rapidb-raw-json",
    ) as RawJsonLiteral["__rapidbRawJsonLiteral"],
    literal,
  };
}

function isRawJsonLiteral(value: JsonExportValue): value is RawJsonLiteral {
  return typeof value === "object" && value !== null && "literal" in value;
}

function throwIfAborted(signal: AbortSignal): void {
  if (!signal.aborted) {
    return;
  }

  const error = new Error("The operation was aborted.");
  error.name = "AbortError";
  throw error;
}

async function withWriteStream(
  filePath: string,
  writer: (writeStream: fs.WriteStream) => Promise<void>,
): Promise<void> {
  const writeStream = fs.createWriteStream(filePath, { encoding: "utf8" });

  try {
    await writer(writeStream);
    await closeWriteStream(writeStream);
  } catch (error) {
    writeStream.destroy();
    throw error;
  }
}

function closeWriteStream(writeStream: fs.WriteStream): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    writeStream.end((error?: Error | null) => {
      if (error) {
        reject(error);
        return;
      }

      resolve();
    });
  });
}
