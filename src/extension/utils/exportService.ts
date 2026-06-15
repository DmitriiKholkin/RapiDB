import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import * as vscode from "vscode";
import type { ColumnTypeMeta, QueryColumnMeta } from "../dbDrivers/types";
import { csvCell } from "./csvUtils";
import { normalizeUnknownError } from "./errorHandling";
import {
  buildQueryExportColumns,
  type ExportColumnDescriptor,
  formatExportCellValue,
  formatTableCsvExportValue,
  type JsonExportValue,
  queryColumnKey,
  serializeJsonExportRecord,
} from "./exportValueFormat";

const DOWNLOADS_DIRECTORY = "Downloads";
const CSV_EXTENSION = "csv";
const JSON_EXTENSION = "json";
const LINE_BREAK = "\n";
const LAST_EXPORT_DIRECTORY_STATE_KEY = "rapidb.lastExportDirectory";

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
  context?: vscode.ExtensionContext;
  write: (filePath: string, signal: AbortSignal) => Promise<void>;
}

interface ExportDialogOptions {
  context?: vscode.ExtensionContext;
}

export async function exportQueryResultsAsCsv(
  result: QueryResultExport,
  options?: ExportDialogOptions,
): Promise<void> {
  await runExport({
    defaultFileName: "query_results",
    format: CSV_EXTENSION,
    progressTitle: "RapiDB: Exporting query results…",
    successLabel: "query results",
    errorLabel: "CSV export failed",
    context: options?.context,
    write: async (filePath, signal) => {
      await writeQueryResultsCsv(filePath, result, signal);
    },
  });
}

export async function exportQueryResultsAsJson(
  result: QueryResultExport,
  options?: ExportDialogOptions,
): Promise<void> {
  await runExport({
    defaultFileName: "query_results",
    format: JSON_EXTENSION,
    progressTitle: "RapiDB: Exporting query results…",
    successLabel: "query results",
    errorLabel: "JSON export failed",
    context: options?.context,
    write: async (filePath, signal) => {
      await writeQueryResultsJson(filePath, result, signal);
    },
  });
}

export async function exportTableDataAsCsv(options: {
  fileName: string;
  loadChunks: (signal: AbortSignal) => AsyncIterable<ChunkedExportData>;
  context?: vscode.ExtensionContext;
}): Promise<void> {
  const { fileName, loadChunks, context } = options;
  await runExport({
    defaultFileName: fileName,
    format: CSV_EXTENSION,
    progressTitle: `RapiDB: Exporting ${fileName}…`,
    successLabel: fileName,
    errorLabel: "CSV export failed",
    context,
    write: async (filePath, signal) => {
      await writeChunkedCsv(filePath, loadChunks(signal), signal);
    },
  });
}

export async function exportTableDataAsJson(options: {
  fileName: string;
  loadChunks: (signal: AbortSignal) => AsyncIterable<ChunkedExportData>;
  context?: vscode.ExtensionContext;
}): Promise<void> {
  const { fileName, loadChunks, context } = options;
  await runExport({
    defaultFileName: fileName,
    format: JSON_EXTENSION,
    progressTitle: `RapiDB: Exporting ${fileName} as JSON…`,
    successLabel: fileName,
    errorLabel: "JSON export failed",
    context,
    write: async (filePath, signal) => {
      await writeChunkedJson(filePath, loadChunks(signal), signal);
    },
  });
}

async function runExport(request: ExportRequest): Promise<void> {
  const defaultUri = buildDefaultExportUri(
    request.context,
    request.defaultFileName,
    request.format,
  );
  const saveUri = await vscode.window.showSaveDialog({
    defaultUri,
    filters: buildExportFilters(request.format),
  });
  if (!saveUri) {
    return;
  }

  if (request.context) {
    await persistLastExportDirectory(request.context, saveUri.fsPath);
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
  context: vscode.ExtensionContext | undefined,
  defaultFileName: string,
  format: ExportFormat,
): vscode.Uri {
  const savedDirectory = getLastExportDirectory(context);
  if (savedDirectory) {
    return vscode.Uri.file(
      path.join(savedDirectory, `${defaultFileName}.${format}`),
    );
  }

  return vscode.Uri.file(
    path.join(
      os.homedir(),
      DOWNLOADS_DIRECTORY,
      `${defaultFileName}.${format}`,
    ),
  );
}

function getLastExportDirectory(
  context: vscode.ExtensionContext | undefined,
): string | undefined {
  const savedDirectory = context?.globalState.get<string>(
    LAST_EXPORT_DIRECTORY_STATE_KEY,
  );
  if (!savedDirectory) {
    return undefined;
  }

  return path.isAbsolute(savedDirectory) ? savedDirectory : undefined;
}

async function persistLastExportDirectory(
  context: vscode.ExtensionContext,
  filePath: string,
): Promise<void> {
  const directoryPath = path.dirname(filePath);
  if (!path.isAbsolute(directoryPath)) {
    return;
  }

  await context.globalState.update(
    LAST_EXPORT_DIRECTORY_STATE_KEY,
    directoryPath,
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
  const exportColumns = buildQueryExportColumns(
    result.columns,
    result.columnMeta,
  );

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
  const exportColumns = buildQueryExportColumns(
    result.columns,
    result.columnMeta,
  );

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

// Re-export value-formatting types and helpers for downstream consumers
// that need the column descriptor type.
export type { ExportColumnDescriptor, JsonExportValue };
export { formatExportCellValue, queryColumnKey };
