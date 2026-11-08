import type { QueryColumnMeta } from "../../shared/tableTypes";
import {
  ensureSQLiteRuntimeInstalled,
  probeInstalledBetterSqlite3Runtime,
  resolveInstalledBetterSqlite3PackagePath,
  type SQLiteInstalledRuntimeProbe,
} from "../utils/sqliteInstaller";

interface BetterSqlite3OpenOptions {
  readonly?: boolean;
  fileMustExist?: boolean;
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

export interface SQLiteDatabase {
  readonly isOpen: boolean;
  readonly inTransaction: boolean;
  close(): void;
  exec(sql: string): void;
  all(sql: string, params?: readonly unknown[]): unknown[];
  allRaw(
    sql: string,
    params?: readonly unknown[],
  ): {
    columns: string[];
    columnMeta: QueryColumnMeta[];
    rows: unknown[][];
  };
  queryBounded(
    sql: string,
    params: readonly unknown[],
    limit: number,
    onResultSet?: () => void,
  ): {
    columns: string[];
    columnMeta: QueryColumnMeta[];
    rows: unknown[][];
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

export type SQLiteRuntimeTargetId =
  | "win32-x64"
  | "win32-arm64"
  | "linux-x64"
  | "linux-arm64"
  | "darwin-x64"
  | "darwin-arm64";

let cachedBetterSqlite3: BetterSqlite3Constructor | null = null;

export function resolveSQLiteRuntimeTarget(
  platform = process.platform,
  arch = process.arch,
): SQLiteRuntimeTargetId | null {
  if (platform === "win32") {
    if (arch === "x64") return "win32-x64";
    if (arch === "arm64") return "win32-arm64";
    return null;
  }
  if (platform === "darwin") {
    if (arch === "x64") return "darwin-x64";
    if (arch === "arm64") return "darwin-arm64";
    return null;
  }
  if (platform === "linux") {
    if (arch === "x64") return "linux-x64";
    if (arch === "arm64") return "linux-arm64";
    return null;
  }
  return null;
}

function sqliteRuntimeTarget(): string {
  return resolveSQLiteRuntimeTarget() ?? `${process.platform}-${process.arch}`;
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

export function resolveBetterSqlite3LoadTargets(
  baseDir = __dirname,
  preferManagedRuntime = true,
): string[] {
  const targets: string[] = [];
  if (preferManagedRuntime) {
    const installedPackage = resolveInstalledBetterSqlite3PackagePath(baseDir);
    if (installedPackage) {
      targets.push(installedPackage);
    }
  }
  targets.push("better-sqlite3");
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
    `packageRoot=${probe.packageRoot ?? "<not-found>"}`,
    `bundledPackage=${probe.bundledPackagePath ?? "<unresolved>"}`,
    `bundledPackageExists=${probe.bundledPackageExists}`,
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
    errorMsg.includes("different Node.js version");

  const versionHint = isModuleVersionError
    ? " The SQLite binary was compiled for a different Node.js version. " +
      "RapiDB re-downloads a host-compatible binary automatically when the cached runtime metadata does not match this host, but the cached copy still failed to load. " +
      "Delete the cached SQLite runtime directory (installedPackage below) and retry the SQLite connection to force a fresh download."
    : "";

  const managedHint =
    " RapiDB stores the SQLite runtime in the extension cache and downloads a platform-matched prebuild on demand.";
  const probeHint = probe
    ? ` Installed runtime probe: ${installedProbeDetails(probe)}.`
    : "";
  const attemptHint =
    attemptedTargets.length > 0
      ? ` Attempted load targets: ${attemptedTargets.join(", ")}.`
      : "";
  const runtimeHint = `Node.js ${process.version}, platform: ${process.platform}-${process.arch}`;

  return `[RapiDB] SQLite runtime is unavailable because better-sqlite3 could not be loaded for ${sqliteRuntimeTarget()}.${versionHint}${managedHint}${attemptHint}${probeHint} Runtime: ${runtimeHint}. Error: ${errorMsg}`.trim();
}

function sqliteRuntimeLoadError(
  error: unknown,
  attemptedTargets: readonly string[],
  probe: SQLiteInstalledRuntimeProbe | null,
): Error {
  return withCause(
    formatSQLiteRuntimeLoadErrorMessage(error, attemptedTargets, probe),
    error,
  );
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

async function loadBetterSqlite3(
  loadTargets?: readonly string[],
): Promise<BetterSqlite3Constructor> {
  if (cachedBetterSqlite3) {
    return cachedBetterSqlite3;
  }

  let installError: unknown = null;
  try {
    if (!loadTargets) await ensureSQLiteRuntimeInstalled(__dirname);
  } catch (error) {
    installError = error;
  }

  const attemptedTargets =
    loadTargets ?? resolveBetterSqlite3LoadTargets(__dirname, true);
  const probe = loadTargets
    ? null
    : probeInstalledBetterSqlite3Runtime(__dirname);

  let lastError: unknown = null;
  for (const target of attemptedTargets) {
    let loaded: BetterSqlite3Module;
    try {
      loaded = require(target) as BetterSqlite3Module;
    } catch (error) {
      lastError = error;
      continue;
    }
    const candidate =
      typeof loaded === "function" ? loaded : (loaded.default ?? null);

    if (typeof candidate !== "function") {
      lastError = new Error(
        `better-sqlite3 load target ${target} did not export a database constructor.`,
      );
      continue;
    }

    cachedBetterSqlite3 = candidate;
    return candidate;
  }

  throw sqliteRuntimeLoadError(
    lastError ??
      installError ??
      new Error("better-sqlite3 did not export a database constructor."),
    attemptedTargets,
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

  allRaw(
    sql: string,
    params: readonly unknown[] = [],
  ): {
    columns: string[];
    columnMeta: QueryColumnMeta[];
    rows: unknown[][];
  } {
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
    return {
      columns,
      columnMeta: inferSqliteQueryColumnMeta(rawRows, columns.length),
      rows: normalizeSqliteIntegers(rawRows) as unknown[][],
    };
  }

  queryBounded(
    sql: string,
    params: readonly unknown[],
    limit: number,
    onResultSet?: () => void,
  ) {
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
    const rows: unknown[][] = [];
    const rawRows: unknown[][] = [];
    let rowCount = 0;
    for (const row of iterator) {
      rowCount++;
      if (rows.length < limit) {
        rawRows.push(row);
        rows.push(normalizeSqliteIntegers(row) as unknown[]);
      }
    }
    const columns = statement.columns().map((column) => column.name);
    return {
      columns,
      columnMeta: inferSqliteQueryColumnMeta(rawRows, columns.length),
      rows,
      rowCount,
      truncated: rowCount > rows.length,
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
): Promise<SQLiteDatabase> {
  const Database = await loadBetterSqlite3(loadTargets);
  const readOnly = config.readOnly === true;
  let database: BetterSqlite3Database;
  try {
    database = new Database(config.filePath, {
      readonly: readOnly,
      fileMustExist: readOnly,
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
      "RAPIDB_TIME_KEY",
      { deterministic: true },
      sqliteTimeSortKey,
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
