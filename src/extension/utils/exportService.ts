import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import * as vscode from "vscode";
import { csvCell } from "./csvUtils";
import { normalizeUnknownError } from "./errorHandling";

const DOWNLOADS_DIRECTORY = "Downloads";
const CSV_EXTENSION = "csv";
const JSON_EXTENSION = "json";
const LINE_BREAK = "\n";

type ExportFormat = typeof CSV_EXTENSION | typeof JSON_EXTENSION;

export interface QueryResultExport {
  columns: readonly string[];
  rows: readonly Record<string, unknown>[];
}

export interface ChunkedExportData {
  columns: ReadonlyArray<{ name: string }>;
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

interface QueryJsonRow {
  [key: string]: unknown;
}

export async function exportQueryResultsAsCsv(
  result: QueryResultExport,
): Promise<void> {
  await runExport({
    defaultFileName: "query_results",
    format: CSV_EXTENSION,
    progressTitle: "RapiDB: Exporting query results…",
    successLabel: "query results",
    errorLabel: "CSV export failed",
    write: async (filePath) => {
      await writeQueryResultsCsv(filePath, result);
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
    write: async (filePath) => {
      await writeQueryResultsJson(filePath, result);
    },
  });
}

export async function exportTableDataAsCsv(options: {
  tableName: string;
  loadChunks: (signal: AbortSignal) => AsyncIterable<ChunkedExportData>;
}): Promise<void> {
  const { tableName, loadChunks } = options;
  await runExport({
    defaultFileName: tableName,
    format: CSV_EXTENSION,
    progressTitle: `RapiDB: Exporting ${tableName}…`,
    successLabel: tableName,
    errorLabel: "CSV export failed",
    write: async (filePath, signal) => {
      await writeChunkedCsv(filePath, loadChunks(signal));
    },
  });
}

export async function exportTableDataAsJson(options: {
  tableName: string;
  loadChunks: (signal: AbortSignal) => AsyncIterable<ChunkedExportData>;
}): Promise<void> {
  const { tableName, loadChunks } = options;
  await runExport({
    defaultFileName: tableName,
    format: JSON_EXTENSION,
    progressTitle: `RapiDB: Exporting ${tableName} as JSON…`,
    successLabel: tableName,
    errorLabel: "JSON export failed",
    write: async (filePath, signal) => {
      await writeChunkedJson(filePath, loadChunks(signal));
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
): Promise<void> {
  await withWriteStream(filePath, async (writeStream) => {
    writeStream.write(result.columns.map(csvCell).join(",") + LINE_BREAK);

    for (const row of result.rows) {
      writeStream.write(
        result.columns
          .map((_, index) => csvCell(row[queryColumnKey(index)]))
          .join(",") + LINE_BREAK,
      );
    }
  });
}

async function writeQueryResultsJson(
  filePath: string,
  result: QueryResultExport,
): Promise<void> {
  await withWriteStream(filePath, async (writeStream) => {
    writeStream.write("[\n");

    for (let index = 0; index < result.rows.length; index++) {
      const row = result.rows[index];
      writeStream.write(
        `${index === 0 ? "" : ",\n"}${JSON.stringify(toQueryJsonRow(result.columns, row))}`,
      );
    }

    writeStream.write("\n]\n");
  });
}

async function writeChunkedCsv(
  filePath: string,
  chunks: AsyncIterable<ChunkedExportData>,
): Promise<void> {
  await withWriteStream(filePath, async (writeStream) => {
    let headerWritten = false;

    for await (const chunk of chunks) {
      if (!headerWritten) {
        writeStream.write(
          chunk.columns.map((column) => csvCell(column.name)).join(",") +
            LINE_BREAK,
        );
        headerWritten = true;
      }

      for (const row of chunk.rows) {
        writeStream.write(
          chunk.columns
            .map((column) => csvCell(formatExportCellValue(row[column.name])))
            .join(",") + LINE_BREAK,
        );
      }
    }
  });
}

async function writeChunkedJson(
  filePath: string,
  chunks: AsyncIterable<ChunkedExportData>,
): Promise<void> {
  await withWriteStream(filePath, async (writeStream) => {
    writeStream.write("[\n");
    let firstRow = true;

    for await (const chunk of chunks) {
      for (const row of chunk.rows) {
        const serializableRow = Object.fromEntries(
          Object.entries(row).map(([key, value]) => [
            key,
            toJsonExportValue(value),
          ]),
        );
        writeStream.write(
          `${firstRow ? "" : ",\n"}${JSON.stringify(serializableRow)}`,
        );
        firstRow = false;
      }
    }

    writeStream.write("\n]\n");
  });
}

function toQueryJsonRow(
  columns: readonly string[],
  row: Record<string, unknown>,
): QueryJsonRow {
  const seenColumnNames = new Map<string, number>();
  const jsonRow: QueryJsonRow = {};

  for (let index = 0; index < columns.length; index++) {
    const columnName = columns[index];
    const seenCount = seenColumnNames.get(columnName) ?? 0;
    seenColumnNames.set(columnName, seenCount + 1);

    const exportKey =
      seenCount === 0 ? columnName : `${columnName}_${seenCount + 1}`;
    jsonRow[exportKey] = row[queryColumnKey(index)];
  }

  return jsonRow;
}

function queryColumnKey(index: number): string {
  return `__col_${index}`;
}

function toJsonExportValue(value: unknown): unknown {
  if (value instanceof Date) {
    return Number.isNaN(value.getTime()) ? null : formatExportCellValue(value);
  }

  return value ?? null;
}

function formatExportCellValue(value: unknown): string {
  if (value == null) {
    return "";
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

  return String(value);
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
