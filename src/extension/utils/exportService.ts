import { randomUUID } from "node:crypto";
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
const NUMERIC_CSV_VALUE = /^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$/;

type ExportFormat = typeof CSV_EXTENSION | typeof JSON_EXTENSION;

function formatCsvExportCell(
  value: unknown,
  category: ColumnTypeMeta["category"] | QueryColumnMeta["category"] | null,
): string {
  const formatted = formatTableCsvExportValue(value, category);
  const trustedNumeric =
    (category === "integer" ||
      category === "float" ||
      category === "decimal") &&
    NUMERIC_CSV_VALUE.test(formatted);
  return csvCell(formatted, trustedNumeric);
}

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
  write: (
    filePath: string,
    signal: AbortSignal,
    state: ExportOperationState,
  ) => Promise<void>;
}

interface ExportOperationState {
  replacementStarted: boolean;
}

class TemporaryFileCleanupError extends Error {
  constructor(error: unknown, cleanupError: unknown, temporaryPath: string) {
    super(
      `${normalizeUnknownError(error).message} ` +
        `Temporary file cleanup failed: ${normalizeUnknownError(cleanupError).message}. ` +
        `The staging file remains at ${temporaryPath}.`,
      { cause: error },
    );
    this.name = "TemporaryFileCleanupError";
  }
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
    write: async (filePath, signal, state) => {
      await writeQueryResultsCsv(filePath, result, signal, state);
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
    write: async (filePath, signal, state) => {
      await writeQueryResultsJson(filePath, result, signal, state);
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
    write: async (filePath, signal, state) => {
      await writeChunkedCsv(filePath, loadChunks(signal), signal, state);
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
    write: async (filePath, signal, state) => {
      await writeChunkedJson(filePath, loadChunks(signal), signal, state);
    },
  });
}

async function runExport(request: ExportRequest): Promise<void> {
  const operationState: ExportOperationState = { replacementStarted: false };
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

  try {
    assertNotDirectoryDestination(saveUri.fsPath);
    if (request.context) {
      await persistLastExportDirectory(request.context, saveUri.fsPath);
    }

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
          await request.write(
            saveUri.fsPath,
            abortController.signal,
            operationState,
          );
        } catch (error) {
          if (
            abortController.signal.aborted &&
            !operationState.replacementStarted &&
            isSignalCancellationError(error)
          ) {
            throw new DOMException("Export cancelled by user", "AbortError");
          }
          throw error;
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
    if (
      normalized.name === "AbortError" &&
      !operationState.replacementStarted
    ) {
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
  state: ExportOperationState,
): Promise<void> {
  const exportColumns = buildQueryExportColumns(
    result.columns,
    result.columnMeta,
  );

  await withWriteStream(
    filePath,
    async (writeStream) => {
      throwIfAborted(signal);
      await writeStreamChunk(
        writeStream,
        result.columns.map((column) => csvCell(column)).join(",") + LINE_BREAK,
        signal,
      );

      for (const row of result.rows) {
        throwIfAborted(signal);
        await writeStreamChunk(
          writeStream,
          exportColumns
            .map((column) =>
              formatCsvExportCell(
                row[column.sourceKey],
                column.category ?? null,
              ),
            )
            .join(",") + LINE_BREAK,
          signal,
        );
      }
    },
    signal,
    state,
  );
}

async function writeQueryResultsJson(
  filePath: string,
  result: QueryResultExport,
  signal: AbortSignal,
  state: ExportOperationState,
): Promise<void> {
  const exportColumns = buildQueryExportColumns(
    result.columns,
    result.columnMeta,
  );

  await withWriteStream(
    filePath,
    async (writeStream) => {
      throwIfAborted(signal);
      await writeStreamChunk(writeStream, "[\n", signal);

      for (let index = 0; index < result.rows.length; index++) {
        throwIfAborted(signal);
        const row = result.rows[index];
        await writeStreamChunk(
          writeStream,
          `${index === 0 ? "" : ",\n"}${serializeJsonExportRecord(
            exportColumns.map((column) => ({
              ...column,
              value: row[column.sourceKey],
            })),
          )}`,
          signal,
        );
      }

      await writeStreamChunk(writeStream, "\n]\n", signal);
    },
    signal,
    state,
  );
}

async function writeChunkedCsv(
  filePath: string,
  chunks: AsyncIterable<ChunkedExportData>,
  signal: AbortSignal,
  state: ExportOperationState,
): Promise<void> {
  await withWriteStream(
    filePath,
    async (writeStream) => {
      let headerColumns: ChunkedExportData["columns"] | undefined;
      let headerByName: ReturnType<typeof indexExportColumns> | undefined;

      for await (const chunk of chunks) {
        throwIfAborted(signal);
        const currentByName = indexExportColumns(chunk.columns);
        if (!headerColumns) {
          // Copy descriptors so a producer cannot mutate the frozen schema.
          headerColumns = chunk.columns.map((column) => ({ ...column }));
          headerByName = indexExportColumns(headerColumns);
          await writeStreamChunk(
            writeStream,
            headerColumns.map((column) => csvCell(column.name)).join(",") +
              LINE_BREAK,
            signal,
          );
        }
        for (const column of currentByName.values()) {
          const header = headerByName?.get(column.name);
          if (!header) {
            throw new Error(
              `CSV schema changed: unexpected column "${column.name}". Select a stable set of columns or export JSON.`,
            );
          }
          if (
            header.category !== column.category ||
            header.nativeType !== column.nativeType
          ) {
            throw new Error(
              `CSV schema changed: type of column "${column.name}" changed. Export JSON or narrow the selection.`,
            );
          }
        }

        for (const row of chunk.rows) {
          throwIfAborted(signal);
          assertExportRowColumns(row, headerByName ?? currentByName);
          await writeStreamChunk(
            writeStream,
            headerColumns
              .map((column) =>
                formatCsvExportCell(row[column.name], column.category ?? null),
              )
              .join(",") + LINE_BREAK,
            signal,
          );
        }
      }
    },
    signal,
    state,
  );
}

function indexExportColumns(columns: ChunkedExportData["columns"]) {
  const byName = new Map<string, ChunkedExportData["columns"][number]>();
  for (const column of columns) {
    if (byName.has(column.name)) {
      throw new Error(`Duplicate export column "${column.name}".`);
    }
    byName.set(column.name, column);
  }
  return byName;
}

function assertExportRowColumns(
  row: Record<string, unknown>,
  columns: ReturnType<typeof indexExportColumns>,
): void {
  for (const name of Object.keys(row)) {
    if (!columns.has(name)) {
      throw new Error(
        `Unexpected export row column "${name}" is missing from the schema.`,
      );
    }
  }
}

async function writeChunkedJson(
  filePath: string,
  chunks: AsyncIterable<ChunkedExportData>,
  signal: AbortSignal,
  state: ExportOperationState,
): Promise<void> {
  await withWriteStream(
    filePath,
    async (writeStream) => {
      throwIfAborted(signal);
      await writeStreamChunk(writeStream, "[\n", signal);
      let firstRow = true;

      for await (const chunk of chunks) {
        throwIfAborted(signal);
        const columnsByName = indexExportColumns(chunk.columns);
        for (const row of chunk.rows) {
          throwIfAborted(signal);
          assertExportRowColumns(row, columnsByName);
          await writeStreamChunk(
            writeStream,
            `${firstRow ? "" : ",\n"}${serializeJsonExportRecord(
              chunk.columns.map((column) => ({
                key: column.name,
                sourceKey: column.name,
                category: column.category ?? null,
                nativeType: column.nativeType,
                value: row[column.name],
              })),
            )}`,
            signal,
          );
          firstRow = false;
        }
      }

      await writeStreamChunk(writeStream, "\n]\n", signal);
    },
    signal,
    state,
  );
}

function throwIfAborted(signal: AbortSignal): void {
  if (!signal.aborted) {
    return;
  }

  const error = new Error("The operation was aborted.");
  error.name = "AbortError";
  throw error;
}

function isSignalCancellationError(error: unknown): boolean {
  if (!(error instanceof Error)) return false;
  if (error.name === "AbortError") return true;

  // Elastic transport 9 uses these messages for signal-triggered aborts, but
  // also uses RequestAbortedError for response-size-limit failures. Match only
  // its explicit abort messages so a racing size-limit error is still reported.
  return (
    error.name === "RequestAbortedError" &&
    (error.message === "Request aborted" ||
      error.message === "Request has been aborted by the user")
  );
}

async function writeStreamChunk(
  stream: fs.WriteStream,
  chunk: string,
  signal: AbortSignal,
): Promise<void> {
  throwIfAborted(signal);
  if (stream.write(chunk)) return;

  await new Promise<void>((resolve, reject) => {
    const cleanup = () => {
      stream.off("drain", onDrain);
      stream.off("error", onError);
      signal.removeEventListener("abort", onAbort);
    };
    const onDrain = () => {
      cleanup();
      resolve();
    };
    const onError = (error: Error) => {
      cleanup();
      reject(error);
    };
    const onAbort = () => {
      cleanup();
      const error = new Error("The operation was aborted.");
      error.name = "AbortError";
      reject(error);
    };
    stream.once("drain", onDrain);
    stream.once("error", onError);
    signal.addEventListener("abort", onAbort, { once: true });
  });
}

async function withWriteStream(
  filePath: string,
  writer: (writeStream: fs.WriteStream) => Promise<void>,
  signal: AbortSignal,
  state: ExportOperationState,
): Promise<void> {
  const temporaryPath = `${filePath}.rapidb-${randomUUID()}.tmp`;
  const writeStream = fs.createWriteStream(temporaryPath, {
    encoding: "utf8",
    // Exclusive creation rejects collisions instead of following symlinks.
    flags: "wx",
    // POSIX enforces owner-only access. Windows inherits the parent-directory
    // ACL; replacing an existing file does not preserve that file's ACL.
    mode: 0o600,
  });
  let temporaryFileCreated = false;

  try {
    await waitForWriteStreamOpen(writeStream);
    temporaryFileCreated = true;
    await writer(writeStream);
    await closeWriteStream(writeStream);
    throwIfAborted(signal);
    state.replacementStarted = true;
    replaceExportFile(temporaryPath, filePath);
  } catch (error) {
    writeStream.destroy();
    if (temporaryFileCreated) {
      try {
        fs.rmSync(temporaryPath, { force: true });
      } catch (cleanupError) {
        throw new TemporaryFileCleanupError(error, cleanupError, temporaryPath);
      }
    }
    throw error;
  }
}

function waitForWriteStreamOpen(writeStream: fs.WriteStream): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    writeStream.once("open", () => resolve());
    writeStream.once("error", reject);
  });
}

function replaceExportFile(temporaryPath: string, filePath: string): void {
  assertNotDirectoryDestination(filePath);

  try {
    fs.renameSync(temporaryPath, filePath);
    return;
  } catch (directError) {
    let destinationStats: fs.Stats | undefined;
    try {
      destinationStats = lstatDestinationIfPresent(filePath);
    } catch {
      throw directError;
    }
    if (!destinationStats || destinationStats.isDirectory()) {
      throw directError;
    }
  }

  const backupPath = `${filePath}.rapidb-${randomUUID()}.bak`;
  fs.renameSync(filePath, backupPath);

  try {
    if (fs.lstatSync(backupPath).isDirectory()) {
      throw new Error("Cannot export to a directory.");
    }
    fs.renameSync(temporaryPath, filePath);
  } catch (operationError) {
    try {
      restoreExportBackup(backupPath, filePath);
    } catch (restoreError) {
      throw new Error(
        `Export replacement failed: ${normalizeUnknownError(operationError).message}. ` +
          `Restoring the original file failed: ${normalizeUnknownError(restoreError).message}. ` +
          `The original file remains at ${backupPath}.`,
      );
    }
    throw operationError;
  }

  try {
    fs.unlinkSync(backupPath);
  } catch (cleanupError) {
    if ((cleanupError as NodeJS.ErrnoException).code !== "ENOENT") {
      throw new Error(
        `Export was installed, but the previous file remains at ${backupPath} ` +
          `because backup cleanup failed: ${normalizeUnknownError(cleanupError).message}`,
      );
    }
  }
}

function restoreExportBackup(backupPath: string, filePath: string): void {
  if (lstatDestinationIfPresent(filePath)) {
    throw new Error(
      `A destination already exists at ${filePath}; refusing to overwrite it with the backup.`,
    );
  }

  // The check and rename cannot be made atomic with Node's portable fs API.
  // Refuse an already-present destination rather than overwriting a competitor.
  fs.renameSync(backupPath, filePath);
}

function assertNotDirectoryDestination(filePath: string): void {
  if (lstatDestinationIfPresent(filePath)?.isDirectory()) {
    throw new Error("Cannot export to a directory.");
  }
}

function lstatDestinationIfPresent(filePath: string): fs.Stats | undefined {
  try {
    return fs.lstatSync(filePath);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      return undefined;
    }
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
