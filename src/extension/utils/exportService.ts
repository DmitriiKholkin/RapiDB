import { randomUUID } from "node:crypto";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import * as vscode from "vscode";
import type { QueryMissingCell } from "../../shared/tableTypes";
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
const CSV_NULL_MARKER = "(NULL)";

type ExportFormat = typeof CSV_EXTENSION | typeof JSON_EXTENSION;

function trackCsvMarker(
  state: Map<string, { marker: boolean; literal: boolean }>,
  column: string,
  marker: string,
  isMarker: boolean,
): void {
  const key = JSON.stringify([column, marker]);
  const entry = state.get(key) ?? { marker: false, literal: false };
  if (isMarker) entry.marker = true;
  else entry.literal = true;
  if (entry.marker && entry.literal) {
    throw new Error(
      `CSV cannot distinguish ${JSON.stringify(marker)} from a missing or NULL value in column ${JSON.stringify(column)}. Export JSON instead.`,
    );
  }
  state.set(key, entry);
}

function formatCsvExportCell(
  value: unknown,
  category: ColumnTypeMeta["category"] | QueryColumnMeta["category"] | null,
  markerState: Map<string, { marker: boolean; literal: boolean }>,
  column: string,
  nativeType?: string,
): string {
  if (value == null) {
    trackCsvMarker(markerState, column, CSV_NULL_MARKER, true);
    return csvCell(CSV_NULL_MARKER);
  }
  const formatted = formatTableCsvExportValue(value, category, nativeType);
  if (formatted === "MISSING" || formatted === CSV_NULL_MARKER) {
    trackCsvMarker(markerState, column, formatted, false);
  }
  const trustedNumeric =
    (category === "integer" ||
      category === "float" ||
      category === "decimal" ||
      typeof value === "number" ||
      typeof value === "bigint") &&
    NUMERIC_CSV_VALUE.test(formatted);
  return csvCell(formatted, trustedNumeric);
}

export interface QueryResultExport {
  columns: readonly string[];
  columnMeta?: readonly QueryColumnMeta[];
  rows: readonly Record<string, unknown>[];
  exportRows?: readonly Record<string, unknown>[];
  missingCells?: readonly QueryMissingCell[];
}

export interface ChunkedExportData {
  columns: ReadonlyArray<
    Pick<ColumnTypeMeta, "name" | "category" | "exportCategory" | "nativeType">
  >;
  rows: ReadonlyArray<Record<string, unknown>>;
  missingCells?: readonly QueryMissingCell[];
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
      await writeChunkedCsv(
        filePath,
        lazyChunks(loadChunks, signal),
        signal,
        state,
      );
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
      await writeChunkedJson(
        filePath,
        lazyChunks(loadChunks, signal),
        signal,
        state,
      );
    },
  });
}

async function* lazyChunks(
  loadChunks: (signal: AbortSignal) => AsyncIterable<ChunkedExportData>,
  signal: AbortSignal,
): AsyncIterable<ChunkedExportData> {
  yield* loadChunks(signal);
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
    ...(process.platform === "win32"
      ? {
          title:
            "Export: choose a new file name (existing files cannot be replaced)",
        }
      : {}),
  });
  if (!saveUri) {
    return;
  }

  try {
    assertNotDirectoryDestination(saveUri.fsPath);
    assertWindowsDestinationIsNew(saveUri.fsPath);
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

const MAX_FILE_NAME_BYTES = 255;

function truncateFileName(name: string, maxBytes: number): string {
  if (Buffer.byteLength(name, "utf8") <= maxBytes) return name;
  let truncated = "";
  let bytes = 0;
  for (const codePoint of name) {
    bytes += Buffer.byteLength(codePoint, "utf8");
    if (bytes > maxBytes) break;
    truncated += codePoint;
  }
  return truncated.replace(/[\s.]+$/, "");
}

export function sanitizeExportFileName(name: string, extension = ""): string {
  const maxBytes = Math.max(
    1,
    MAX_FILE_NAME_BYTES - Buffer.byteLength(extension, "utf8"),
  );
  const sanitized = truncateFileName(
    name
      // biome-ignore lint/suspicious/noControlCharactersInRegex: control characters are invalid in file names
      .replace(/[/\\:*?"<>|\u0000-\u001f\u007f]/g, "_")
      .replace(/^[\s.]+/, "")
      .replace(/[\s.]+$/, ""),
    maxBytes,
  );
  if (!sanitized) return "export";
  if (/^(con|prn|aux|nul|com[1-9¹²³]|lpt[1-9¹²³])(\..*)?$/i.test(sanitized)) {
    return truncateFileName(`_${sanitized}`, maxBytes);
  }
  return sanitized;
}

function buildDefaultExportUri(
  context: vscode.ExtensionContext | undefined,
  defaultFileName: string,
  format: ExportFormat,
): vscode.Uri {
  const extension = `.${format}`;
  const fileName = `${sanitizeExportFileName(defaultFileName, extension)}${extension}`;
  const savedDirectory = getLastExportDirectory(context);
  if (savedDirectory) {
    return vscode.Uri.file(path.join(savedDirectory, fileName));
  }

  return vscode.Uri.file(
    path.join(os.homedir(), DOWNLOADS_DIRECTORY, fileName),
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

      const missingCells = new Set(
        result.missingCells?.map(
          ({ rowIndex, columnIndex }) => `${rowIndex}:${columnIndex}`,
        ),
      );
      const markerState = new Map<
        string,
        { marker: boolean; literal: boolean }
      >();
      for (const [rowIndex, row] of result.rows.entries()) {
        throwIfAborted(signal);
        const exportRow = result.exportRows?.[rowIndex];
        await writeStreamChunk(
          writeStream,
          exportColumns
            .map((column, columnIndex) => {
              const missing =
                missingCells.has(`${rowIndex}:${columnIndex}`) ||
                (exportRow !== undefined &&
                  !Object.hasOwn(exportRow, column.sourceKey));
              if (missing) {
                trackCsvMarker(markerState, column.key, "MISSING", true);
                return csvCell("MISSING");
              }
              const value =
                exportRow && Object.hasOwn(exportRow, column.sourceKey)
                  ? exportRow[column.sourceKey]
                  : row[column.sourceKey];
              return formatCsvExportCell(
                value,
                column.category ?? null,
                markerState,
                column.key,
              );
            })
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
      const missingCells = new Set(
        result.missingCells?.map(
          ({ rowIndex, columnIndex }) => `${rowIndex}:${columnIndex}`,
        ),
      );

      for (let index = 0; index < result.rows.length; index++) {
        throwIfAborted(signal);
        const row = result.rows[index];
        const exportRow = result.exportRows?.[index];
        await writeStreamChunk(
          writeStream,
          `${index === 0 ? "" : ",\n"}${serializeJsonExportRecord(
            exportColumns.map((column, columnIndex) => ({
              ...column,
              value:
                exportRow && Object.hasOwn(exportRow, column.sourceKey)
                  ? exportRow[column.sourceKey]
                  : row[column.sourceKey],
              omit:
                missingCells.has(`${index}:${columnIndex}`) ||
                (exportRow !== undefined &&
                  !Object.hasOwn(exportRow, column.sourceKey)),
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
      const markerState = new Map<
        string,
        { marker: boolean; literal: boolean }
      >();

      for await (const chunk of chunks) {
        throwIfAborted(signal);
        const currentByName = indexExportColumns(chunk.columns);
        const currentColumnIndexByName = new Map(
          chunk.columns.map((column, index) => [column.name, index]),
        );
        const missingCells = new Set(
          chunk.missingCells?.map(
            ({ rowIndex, columnIndex }) => `${rowIndex}:${columnIndex}`,
          ),
        );
        if (!headerColumns) {
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
            header.exportCategory !== column.exportCategory ||
            header.nativeType !== column.nativeType
          ) {
            throw new Error(
              `CSV schema changed: type of column "${column.name}" changed. Export JSON or narrow the selection.`,
            );
          }
        }

        for (const [rowIndex, row] of chunk.rows.entries()) {
          throwIfAborted(signal);
          assertExportRowColumns(row, headerByName ?? currentByName);
          await writeStreamChunk(
            writeStream,
            headerColumns
              .map((column) => {
                const columnIndex = currentColumnIndexByName.get(column.name);
                const missing =
                  !Object.hasOwn(row, column.name) ||
                  (columnIndex !== undefined &&
                    missingCells.has(`${rowIndex}:${columnIndex}`));
                if (missing) {
                  trackCsvMarker(markerState, column.name, "MISSING", true);
                  return csvCell("MISSING");
                }
                return formatCsvExportCell(
                  row[column.name],
                  column.exportCategory ?? column.category ?? null,
                  markerState,
                  column.name,
                  column.nativeType,
                );
              })
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
        const currentColumnIndexByName = new Map(
          chunk.columns.map((column, index) => [column.name, index]),
        );
        const missingCells = new Set(
          chunk.missingCells?.map(
            ({ rowIndex, columnIndex }) => `${rowIndex}:${columnIndex}`,
          ),
        );
        for (const [rowIndex, row] of chunk.rows.entries()) {
          throwIfAborted(signal);
          assertExportRowColumns(row, columnsByName);
          await writeStreamChunk(
            writeStream,
            `${firstRow ? "" : ",\n"}${serializeJsonExportRecord(
              chunk.columns.map((column) => ({
                key: column.name,
                sourceKey: column.name,
                category: column.exportCategory ?? column.category ?? null,
                nativeType: column.nativeType,
                value: row[column.name],
                omit:
                  !Object.hasOwn(row, column.name) ||
                  (() => {
                    const columnIndex = currentColumnIndexByName.get(
                      column.name,
                    );
                    return (
                      columnIndex !== undefined &&
                      missingCells.has(`${rowIndex}:${columnIndex}`)
                    );
                  })(),
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
  if (stream.errored) throw stream.errored;
  if (stream.destroyed) {
    throw new Error("Export staging stream closed before completion.");
  }
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
    flags: "wx",
    mode: 0o600,
  });
  let temporaryFileCreated = false;

  try {
    await waitForWriteStreamOpen(writeStream);
    temporaryFileCreated = true;
    const destinationExisted = !!lstatDestinationIfPresent(filePath);
    if (destinationExisted) assertWindowsDestinationIsNew(filePath);
    const hardLinksSupported =
      destinationExisted || probeHardLinkSupport(temporaryPath);
    await writer(writeStream);
    await closeWriteStream(writeStream);
    throwIfAborted(signal);
    state.replacementStarted = true;
    replaceExportFile(
      temporaryPath,
      filePath,
      destinationExisted,
      hardLinksSupported,
    );
  } catch (error) {
    await destroyAndWaitForClose(writeStream);
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

function replaceExportFile(
  temporaryPath: string,
  filePath: string,
  destinationExisted: boolean,
  hardLinksSupported: boolean,
): void {
  assertNotDirectoryDestination(filePath);
  const destination = lstatDestinationIfPresent(filePath);
  if (!destinationExisted || !destination) {
    let linked = false;
    if (hardLinksSupported) {
      try {
        fs.linkSync(temporaryPath, filePath);
        linked = true;
      } catch (error) {
        if (!isHardLinkUnsupportedError(error)) throw hardLinkError(error);
      }
    }
    if (!linked) copyNewExportFile(temporaryPath, filePath);
    try {
      fs.unlinkSync(temporaryPath);
    } catch (error) {
      throw new Error(
        `Export was installed, but staging cleanup failed: ${normalizeUnknownError(error).message}. Staging file: ${temporaryPath}.`,
      );
    }
    return;
  }
  assertWindowsDestinationIsNew(filePath);
  preserveDestinationMode(temporaryPath, filePath);
  fs.renameSync(temporaryPath, filePath);
}

function assertWindowsDestinationIsNew(filePath: string): void {
  if (process.platform === "win32" && lstatDestinationIfPresent(filePath)) {
    throw new Error(
      "Cannot safely replace an existing export on Windows. Choose a new file name instead.",
    );
  }
}

function hardLinkError(error: unknown): Error {
  return new Error(
    `Cannot publish a new export using a hard link: ${normalizeUnknownError(error).message}. ` +
      "The destination may already exist or its filesystem may not support hard links. Choose a new name on a supported filesystem.",
    { cause: error },
  );
}

const HARD_LINK_UNSUPPORTED_CODES = new Set([
  "EPERM",
  "EACCES",
  "ENOTSUP",
  "EOPNOTSUPP",
  "EXDEV",
  "ENOSYS",
  "EISDIR",
  "EINVAL",
]);

function isHardLinkUnsupportedError(error: unknown): boolean {
  const code = (error as NodeJS.ErrnoException | undefined)?.code;
  return typeof code === "string" && HARD_LINK_UNSUPPORTED_CODES.has(code);
}

const COPY_NOT_CREATED_CODES = new Set([
  "EEXIST",
  "EACCES",
  "EPERM",
  "ENOENT",
  "ENOTDIR",
  "EISDIR",
  "EROFS",
]);

function copyNewExportFile(temporaryPath: string, filePath: string): void {
  try {
    fs.copyFileSync(temporaryPath, filePath, fs.constants.COPYFILE_EXCL);
  } catch (error) {
    const code = (error as NodeJS.ErrnoException).code;
    if (!code || !COPY_NOT_CREATED_CODES.has(code)) {
      try {
        fs.rmSync(filePath, { force: true });
      } catch {}
    }
    throw new Error(
      `Cannot publish a new export: ${normalizeUnknownError(error).message}. The destination may already exist. Choose a new name and retry.`,
      { cause: error },
    );
  }
}

function probeHardLinkSupport(temporaryPath: string): boolean {
  const probePath = `${temporaryPath}.${randomUUID()}.probe`;
  try {
    fs.linkSync(temporaryPath, probePath);
  } catch (error) {
    if (isHardLinkUnsupportedError(error)) return false;
    throw hardLinkError(error);
  }
  try {
    fs.unlinkSync(probePath);
  } catch (error) {
    throw new Error(
      `Hard-link probe cleanup failed: ${normalizeUnknownError(error).message}. Probe file remains at ${probePath}.`,
      { cause: error },
    );
  }
  return true;
}

async function destroyAndWaitForClose(stream: fs.WriteStream): Promise<void> {
  if (stream.closed) return;
  await new Promise<void>((resolve) => {
    stream.once("close", resolve);
    stream.destroy();
  });
}

function preserveDestinationMode(
  temporaryPath: string,
  filePath: string,
): void {
  const destinationStats = lstatDestinationIfPresent(filePath);
  if (!destinationStats?.isFile()) {
    return;
  }
  try {
    fs.chmodSync(temporaryPath, destinationStats.mode & 0o777);
  } catch (error) {
    const code = (error as NodeJS.ErrnoException).code;
    if (code !== "EPERM" && code !== "ENOTSUP" && code !== "EINVAL") {
      throw error;
    }
  }
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
    let ended = false;
    let closed = false;
    const finish = () => {
      if (!ended || !closed) return;
      writeStream.off("error", onError);
      resolve();
    };
    const onError = (error: Error) => {
      writeStream.off("close", onClose);
      reject(error);
    };
    const onClose = () => {
      closed = true;
      finish();
    };
    writeStream.once("error", onError);
    writeStream.once("close", onClose);
    writeStream.end((error?: Error | null) => {
      if (error) {
        writeStream.off("close", onClose);
        writeStream.off("error", onError);
        reject(error);
      } else {
        ended = true;
        finish();
      }
    });
  });
}

export type { ExportColumnDescriptor, JsonExportValue };
export { formatExportCellValue, queryColumnKey };
