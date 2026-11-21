import { createHash } from "node:crypto";
import type { QueryColumnMeta } from "../../shared/tableTypes";
import { SQLITE_ARTIFACTS } from "../utils/sqliteArtifactPins";
import {
  ensureSQLiteRuntimeInstalled,
  probeInstalledBetterSqlite3Runtime,
  type SQLiteInstalledRuntimeProbe,
  sqliteNativeBindingPath,
} from "../utils/sqliteInstaller";
import {
  resolveSQLiteNativeTarget,
  sqliteLinuxRuntimeRequirementHint,
} from "../utils/sqliteNativeTarget";

interface BetterSqlite3OpenOptions {
  readonly?: boolean;
  fileMustExist?: boolean;
  nativeBinding?: string;
}

interface BetterSqlite3Statement {
  readonly reader: boolean;
  safeIntegers(toggle?: boolean): BetterSqlite3Statement;
  raw(toggle?: boolean): BetterSqlite3Statement;
  columns(): { name: string }[];
  all(...params: unknown[]): unknown[];
  iterate(...params: unknown[]): IterableIterator<unknown[]>;
  get(...params: unknown[]): unknown;
  run(...params: unknown[]): {
    changes: number;
  };
}

interface BetterSqlite3Database {
  readonly open: boolean;
  readonly inTransaction: boolean;
  close(): void;
  exec(sql: string): void;
  function(
    name: string,
    options: { deterministic?: boolean },
    fn: (value: unknown) => unknown,
  ): void;
  pragma(source: string, options?: { simple?: boolean }): unknown;
  prepare(sql: string): BetterSqlite3Statement;
}

type BetterSqlite3Constructor = new (
  path: string,
  options?: BetterSqlite3OpenOptions,
) => BetterSqlite3Database;

type BetterSqlite3Module =
  | BetterSqlite3Constructor
  | {
      default?: BetterSqlite3Constructor;
    };

interface SQLiteQueryRows {
  columns: string[];
  columnMeta: QueryColumnMeta[];
  rows: unknown[][];
  exactIntegerRows?: ([columnIndex: number, value: bigint][] | undefined)[];
}

export interface SQLiteDatabase {
  readonly isOpen: boolean;
  readonly inTransaction: boolean;
  close(): void;
  exec(sql: string): void;
  all(sql: string, params?: readonly unknown[]): unknown[];
  allRaw(sql: string, params?: readonly unknown[]): SQLiteQueryRows;
  queryBounded(
    sql: string,
    params: readonly unknown[],
    limit: number,
    onResultSet?: () => void,
  ): SQLiteQueryRows & {
    rowCount: number;
    changes?: number;
    truncated: boolean;
  };
  get(sql: string, params?: readonly unknown[]): unknown;
  run(
    sql: string,
    params?: readonly unknown[],
  ): {
    changes: number;
  };
}

export interface SQLiteRuntimeOpenConfig {
  filePath: string;
  readOnly?: boolean;
}

// The prebuild target RapiDB selects for this host (glibc and musl Linux
// differ), or the raw platform when it is unsupported.
function sqliteRuntimeTarget(
  probe: SQLiteInstalledRuntimeProbe | null,
): string {
  if (probe?.target) return probe.target;
  try {
    return resolveSQLiteNativeTarget();
  } catch {
    return `${process.platform}-${process.arch}`;
  }
}

function sqliteDateTimeKeyFromSeconds(
  seconds: number,
  fraction: string,
): string | null {
  if (
    !Number.isSafeInteger(seconds) ||
    seconds < -210_866_760_000 ||
    seconds > 253_402_300_799
  ) {
    return null;
  }
  const sortableSeconds = seconds + 1_000_000_000_000;
  if (sortableSeconds < 0 || sortableSeconds >= 10_000_000_000_000) {
    return null;
  }
  return `${String(sortableSeconds).padStart(13, "0")}.${fraction.replace(/0+$/, "")}`;
}

function sqliteTimeSortKey(value: unknown): string | null {
  const key = sqliteDateTimeSortKey(value);
  if (key === null) return null;
  const [whole, fraction] = key.split(".");
  const seconds = Number(whole) - 1_000_000_000_000;
  const timeOfDay = ((seconds % 86_400) + 86_400) % 86_400;
  return `${String(timeOfDay).padStart(5, "0")}.${fraction}`;
}

// Calendar day (UTC, after any offset) of RAPIDB_DATETIME_KEY as a sortable
// day number, so hour 24 is the next day exactly as for datetime filters.
function sqliteDateKey(value: unknown): string | null {
  const key = sqliteDateTimeSortKey(value);
  if (key === null) return null;
  const seconds = Number(key.split(".")[0]) - 1_000_000_000_000;
  const days = Math.floor(seconds / 86_400) + 10_000_000;
  return String(days).padStart(8, "0");
}

function sqliteDateTimeSortKey(value: unknown): string | null {
  if (typeof value === "number" || typeof value === "bigint") {
    const julianDay = Number(value);
    if (
      !Number.isFinite(julianDay) ||
      julianDay < 0 ||
      julianDay >= 5_373_484.5
    )
      return null;
    const unixMilliseconds =
      Math.round(julianDay * 86_400_000) - 2_440_587.5 * 86_400_000;
    if (!Number.isSafeInteger(unixMilliseconds)) return null;
    const seconds = Math.floor(unixMilliseconds / 1000);
    const milliseconds = unixMilliseconds - seconds * 1000;
    const fraction =
      milliseconds === 0 ? "" : String(milliseconds).padStart(3, "0");
    return sqliteDateTimeKeyFromSeconds(seconds, fraction);
  }
  if (typeof value !== "string") return null;

  const text = value.trim();
  const normalized = /^\d{2}:\d{2}/.test(text) ? `2000-01-01 ${text}` : text;
  const match =
    /^(-?\d{4})-(\d{2})-(\d{2})(?:[\t-\r T]+(\d{2}):(\d{2})(?::(\d{2})(?:\.(\d+))?)?(?:[\t-\r ]*(Z|[+-]\d{2}:\d{2}))?)?$/i.exec(
      normalized,
    );
  if (!match) return null;

  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  const hours = Number(match[4] ?? "00");
  const minutes = Number(match[5] ?? "00");
  const secondsPart = Number(match[6] ?? "00");
  if (
    month < 1 ||
    month > 12 ||
    day < 1 ||
    day > 31 ||
    hours > 24 ||
    minutes > 59 ||
    secondsPart > 59
  ) {
    return null;
  }

  let offsetSeconds = 0;
  const timezone = match[8];
  if (timezone && timezone.toUpperCase() !== "Z") {
    const sign = timezone[0] === "+" ? 1 : -1;
    const offsetDigits = timezone.slice(1).replace(":", "");
    const offsetHours = Number(offsetDigits.slice(0, 2));
    const offsetMinutes = Number(offsetDigits.slice(2));
    if (offsetHours > 14 || offsetMinutes > 59) {
      return null;
    }
    offsetSeconds = sign * (offsetHours * 60 + offsetMinutes) * 60;
  }

  let adjustedYear = year;
  adjustedYear -= month <= 2 ? 1 : 0;
  const era = Math.floor(adjustedYear / 400);
  const yearOfEra = adjustedYear - era * 400;
  const adjustedMonth = month + (month > 2 ? -3 : 9);
  const dayOfYear = Math.floor((153 * adjustedMonth + 2) / 5) + day - 1;
  const dayOfEra =
    yearOfEra * 365 +
    Math.floor(yearOfEra / 4) -
    Math.floor(yearOfEra / 100) +
    dayOfYear;
  const daysSinceUnixEpoch = era * 146_097 + dayOfEra - 719_468;
  const utcSeconds =
    daysSinceUnixEpoch * 86_400 +
    hours * 3_600 +
    minutes * 60 +
    secondsPart -
    offsetSeconds;
  return sqliteDateTimeKeyFromSeconds(utcSeconds, match[7] ?? "");
}

// Set only by an extension running in ExtensionMode.Development/Test and by
// the test setup. A global symbol survives module re-imports in tests.
const DEVELOPMENT_RUNTIME_FLAG = Symbol.for(
  "rapidb.sqlite.allowDevelopmentRuntime",
);

export function allowSQLiteDevelopmentRuntime(allowed: boolean): void {
  (globalThis as Record<symbol, unknown>)[DEVELOPMENT_RUNTIME_FLAG] = allowed;
}

export function sqliteDevelopmentRuntimeAllowed(): boolean {
  return (
    (globalThis as Record<symbol, unknown>)[DEVELOPMENT_RUNTIME_FLAG] === true
  );
}

// Production loads only the hash-verified managed runtime. Resolving the bare
// "better-sqlite3" name could pick up an unverified copy from any parent
// node_modules folder, so that fallback exists only for development and tests.
export function resolveBetterSqlite3LoadTargets(
  validatedManagedPackage: string | null,
): string[] {
  const targets = validatedManagedPackage ? [validatedManagedPackage] : [];
  if (sqliteDevelopmentRuntimeAllowed()) targets.push("better-sqlite3");
  return targets;
}

function errorMessage(error: unknown): string {
  if (error instanceof Error && error.message.trim().length > 0) {
    return error.message;
  }
  return String(error);
}

function withCause(message: string, cause: unknown): Error {
  const wrapped = new Error(message);
  Object.defineProperty(wrapped, "cause", {
    value: cause,
    enumerable: false,
    configurable: true,
    writable: true,
  });
  return wrapped;
}

function installedProbeDetails(probe: SQLiteInstalledRuntimeProbe): string {
  return [
    `target=${probe.target}`,
    `runtime=${probe.runtime}`,
    `nodeApi=${probe.nodeApi}`,
    `installedPackage=${probe.installedPackagePath ?? "<unresolved>"}`,
    `installedPackageExists=${probe.installedPackageExists}`,
    `installedBinary=${probe.installedBinaryPath ?? "<unresolved>"}`,
    `installedBinaryExists=${probe.installedBinaryExists}`,
  ].join("; ");
}

export function formatSQLiteRuntimeLoadErrorMessage(
  error: unknown,
  attemptedTargets: readonly string[],
  probe: SQLiteInstalledRuntimeProbe | null,
): string {
  const errorMsg = errorMessage(error);

  const isModuleVersionError =
    errorMsg.includes("NODE_MODULE_VERSION") ||
    errorMsg.includes("MODULE_VERSION") ||
    errorMsg.includes("different Node.js version") ||
    errorMsg.includes("Node-API version");

  const versionHint = isModuleVersionError
    ? " The SQLite binary requires a newer Node.js/Node-API than this VS Code runtime provides. Update VS Code."
    : "";
  const isLibraryVersionError =
    /\bGLIBC(?:XX)?_\d|\bCXXABI_\d|version [`'"]?[^\s`'"]+[`'"]? not found/i.test(
      errorMsg,
    );
  const libraryHint = isLibraryVersionError
    ? ` ${sqliteLinuxRuntimeRequirementHint(SQLITE_ARTIFACTS.prebuilds["linux-x64"] ?? {})}`
    : "";

  const managedHint =
    " RapiDB stores the SQLite runtime in the extension cache and downloads the integrity-pinned better-sqlite3 package from registry.npmjs.org on demand.";
  const probeHint = probe
    ? ` Installed runtime probe: ${installedProbeDetails(probe)}.`
    : "";
  const attemptHint =
    attemptedTargets.length > 0
      ? ` Attempted load targets: ${attemptedTargets.join(", ")}.`
      : "";
  const runtimeHint = `Node.js ${process.version}, platform: ${process.platform}-${process.arch}`;

  return `[RapiDB] SQLite runtime is unavailable because better-sqlite3 could not be loaded for ${sqliteRuntimeTarget(probe)}.${versionHint}${libraryHint}${managedHint}${attemptHint}${probeHint} Runtime: ${runtimeHint}. Error: ${errorMsg}`.trim();
}

export class SQLiteRuntimeLoadError extends Error {
  constructor(
    cause: unknown,
    attemptedTargets: readonly string[],
    probe: SQLiteInstalledRuntimeProbe | null,
  ) {
    super(formatSQLiteRuntimeLoadErrorMessage(cause, attemptedTargets, probe), {
      cause,
    });
    this.name = "SQLiteRuntimeLoadError";
  }
}

function sqliteOpenError(
  config: SQLiteRuntimeOpenConfig,
  error: unknown,
): Error {
  const accessMode = config.readOnly ? "read-only" : "read-write";
  return withCause(
    `[RapiDB] SQLite failed to open "${config.filePath}" in ${accessMode} mode. ${errorMessage(error)}`,
    error,
  );
}

interface LoadedSQLiteRuntime {
  Database: BetterSqlite3Constructor;
  nativeBinding?: string;
}

function loadBetterSqlite3(
  attemptedTargets: readonly string[],
  managedPackage: string | null,
  installError?: unknown,
  probe: SQLiteInstalledRuntimeProbe | null = null,
): LoadedSQLiteRuntime {
  let lastError: unknown;
  let managedError: unknown;
  for (const [index, target] of attemptedTargets.entries()) {
    const nativeBinding =
      index === 0 && managedPackage
        ? sqliteNativeBindingPath(managedPackage)
        : undefined;
    try {
      const loaded = require(target) as BetterSqlite3Module | null;
      const Database = typeof loaded === "function" ? loaded : loaded?.default;
      if (typeof Database !== "function") {
        throw new Error(
          `better-sqlite3 load target ${target} did not export a database constructor.`,
        );
      }

      // require() only loads the JS wrapper. Force the addon to initialize
      // before touching a user path, and pin managed loads to the exact
      // hash-verified binary selected by the installer rather than any
      // ambient prebuild search path.
      let smoke: BetterSqlite3Database;
      try {
        smoke = new Database(
          ":memory:",
          nativeBinding ? { nativeBinding } : {},
        );
      } catch (error) {
        if (nativeBinding) managedError = error;
        throw error;
      }
      try {
        const row = smoke.prepare("SELECT 1 AS ok").get() as { ok?: number };
        if (row?.ok !== 1)
          throw new Error("Unexpected SQLite smoke test result");
      } finally {
        smoke.close();
      }
      return { Database, nativeBinding };
    } catch (error) {
      lastError = error;
    }
  }

  throw new SQLiteRuntimeLoadError(
    installError ??
      managedError ??
      lastError ??
      new Error(
        "No verified SQLite runtime is available to load; RapiDB loads better-sqlite3 only from its managed, hash-verified runtime.",
      ),
    attemptedTargets,
    probe,
  );
}

async function selectSQLiteRuntime(): Promise<LoadedSQLiteRuntime> {
  let managedPackage: string | null = null;
  let installError: unknown;
  let probe: SQLiteInstalledRuntimeProbe | null = null;
  try {
    managedPackage = await ensureSQLiteRuntimeInstalled();
  } catch (error) {
    installError = error;
  }
  try {
    probe = probeInstalledBetterSqlite3Runtime();
  } catch {
    // Probe data is diagnostic only.
  }
  return loadBetterSqlite3(
    resolveBetterSqlite3LoadTargets(managedPackage),
    managedPackage,
    installError,
    probe,
  );
}

function executeStatement<T>(
  statement: BetterSqlite3Statement,
  kind: "all" | "get" | "run",
  params: readonly unknown[],
): T {
  const exactStatement = statement.safeIntegers(true);
  let result: unknown;
  if (params.length === 0) {
    result = exactStatement[kind]();
  } else {
    result = exactStatement[kind](params);
  }
  return normalizeSqliteIntegers(result) as T;
}

function normalizeSqliteIntegers(value: unknown): unknown {
  if (typeof value === "bigint") {
    return value <= BigInt(Number.MAX_SAFE_INTEGER) &&
      value >= BigInt(Number.MIN_SAFE_INTEGER)
      ? Number(value)
      : value.toString();
  }
  if (Array.isArray(value)) {
    return value.map(normalizeSqliteIntegers);
  }
  if (
    value &&
    typeof value === "object" &&
    !Buffer.isBuffer(value) &&
    !(value instanceof Uint8Array)
  ) {
    return Object.fromEntries(
      Object.entries(value).map(([key, entry]) => [
        key,
        normalizeSqliteIntegers(entry),
      ]),
    );
  }
  return value;
}

function inferSqliteQueryColumnMeta(
  rows: readonly unknown[][],
  columnCount: number,
): QueryColumnMeta[] {
  return Array.from({ length: columnCount }, (_, columnIndex) => {
    let sawInteger = false;
    let sawFloat = false;
    let sawOtherValue = false;
    for (const row of rows) {
      const value = row[columnIndex];
      if (value === null || value === undefined) continue;
      if (typeof value === "bigint") {
        sawInteger = true;
      } else if (typeof value === "number") {
        sawFloat = true;
      } else {
        sawOtherValue = true;
      }
    }
    return {
      category:
        !sawOtherValue && (sawInteger || sawFloat)
          ? sawInteger && sawFloat
            ? "decimal"
            : sawInteger
              ? "integer"
              : "float"
          : null,
    };
  });
}

function formatSqliteQueryRows(
  columns: string[],
  rawRows: unknown[][],
): SQLiteQueryRows {
  const exactIntegerRows: NonNullable<SQLiteQueryRows["exactIntegerRows"]> = [];
  const rows = rawRows.map((rawRow) => {
    let exactIntegers: (typeof exactIntegerRows)[number];
    const row = rawRow.map((value, index) => {
      if (typeof value !== "bigint") return value;
      const display = normalizeSqliteIntegers(value);
      if (typeof display === "string") {
        // safeIntegers(true) distinguishes SQLite INTEGER from identical TEXT.
        exactIntegers ??= [];
        exactIntegers.push([index, value]);
      }
      return display;
    });
    exactIntegerRows.push(exactIntegers);
    return row;
  });
  return {
    columns,
    columnMeta: inferSqliteQueryColumnMeta(rawRows, columns.length),
    rows,
    ...(exactIntegerRows.some(Boolean) ? { exactIntegerRows } : {}),
  };
}

class BetterSqlite3DatabaseAdapter implements SQLiteDatabase {
  constructor(private readonly database: BetterSqlite3Database) {}

  get isOpen(): boolean {
    return this.database.open;
  }

  get inTransaction(): boolean {
    return this.database.inTransaction;
  }

  close(): void {
    if (this.database.open) {
      this.database.close();
    }
  }

  exec(sql: string): void {
    this.database.exec(sql);
  }

  all(sql: string, params: readonly unknown[] = []): unknown[] {
    return executeStatement<unknown[]>(
      this.database.prepare(sql),
      "all",
      params,
    );
  }

  allRaw(sql: string, params: readonly unknown[] = []): SQLiteQueryRows {
    const statement = this.database.prepare(sql);
    if (!statement.reader)
      throw new TypeError(
        "This statement does not return data. Use run() instead.",
      );
    const columns = statement.columns().map((column) => column.name);
    const exactStatement = statement.raw().safeIntegers(true);
    const rawRows = params.length
      ? (exactStatement.all(params) as unknown[][])
      : (exactStatement.all() as unknown[][]);
    return formatSqliteQueryRows(columns, rawRows);
  }

  queryBounded(
    sql: string,
    params: readonly unknown[],
    limit: number,
    onResultSet?: () => void,
  ): ReturnType<SQLiteDatabase["queryBounded"]> {
    const statement = this.database.prepare(sql);
    if (!statement.reader) {
      const info = executeStatement<{ changes: number }>(
        statement,
        "run",
        params,
      );
      return {
        columns: [],
        columnMeta: [],
        rows: [],
        rowCount: info.changes,
        changes: info.changes,
        truncated: false,
      };
    }
    onResultSet?.();
    const exact = statement.raw().safeIntegers(true);
    const iterator = params.length ? exact.iterate(params) : exact.iterate();
    const rawRows: unknown[][] = [];
    let rowCount = 0;
    for (const row of iterator) {
      rowCount++;
      if (rawRows.length < limit) {
        rawRows.push(row);
      }
    }
    const columns = statement.columns().map((column) => column.name);
    return {
      ...formatSqliteQueryRows(columns, rawRows),
      rowCount,
      truncated: rowCount > rawRows.length,
    };
  }

  get(sql: string, params: readonly unknown[] = []): unknown {
    return executeStatement<unknown>(this.database.prepare(sql), "get", params);
  }

  run(
    sql: string,
    params: readonly unknown[] = [],
  ): {
    changes: number;
  } {
    return executeStatement<{ changes: number }>(
      this.database.prepare(sql),
      "run",
      params,
    );
  }
}

export async function openSQLiteDatabase(
  config: SQLiteRuntimeOpenConfig,
  loadTargets?: readonly string[],
  managedPackage: string | null = null,
  beforeOpen?: () => void,
): Promise<SQLiteDatabase> {
  const { Database, nativeBinding } = loadTargets
    ? loadBetterSqlite3(loadTargets, managedPackage)
    : await selectSQLiteRuntime();
  const readOnly = config.readOnly === true;
  let database: BetterSqlite3Database;
  beforeOpen?.();
  try {
    database = new Database(config.filePath, {
      readonly: readOnly,
      fileMustExist: readOnly,
      ...(nativeBinding ? { nativeBinding } : {}),
    });
  } catch (error) {
    throw sqliteOpenError(config, error);
  }

  try {
    database.pragma("schema_version", { simple: true });
    database.pragma("foreign_keys = ON");
    database.function(
      "RAPIDB_DATETIME_KEY",
      { deterministic: true },
      sqliteDateTimeSortKey,
    );
    database.function(
      "RAPIDB_DATE_KEY",
      { deterministic: true },
      sqliteDateKey,
    );
    database.function(
      "RAPIDB_TIME_KEY",
      { deterministic: true },
      sqliteTimeSortKey,
    );
    database.function("RAPIDB_REAL_KEY", { deterministic: true }, (value) =>
      typeof value === "number" ? String(value) : null,
    );
    database.function(
      "RAPIDB_DELETE_HASH",
      { deterministic: true },
      (value) => {
        const hash = createHash("sha256");
        if (typeof value === "string") return hash.update(value).digest("hex");
        if (typeof value !== "number") return null;
        // SQLite REAL values bypass text formatting (including signed zero).
        const bytes = Buffer.alloc(8);
        bytes.writeDoubleBE(value);
        return hash.update("r:").update(bytes).digest("hex");
      },
    );

    return new BetterSqlite3DatabaseAdapter(database);
  } catch (error) {
    if (database.open) {
      try {
        database.close();
      } catch {}
    }
    throw error;
  }
}
