import { existsSync } from "node:fs";
import { join } from "node:path";
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
  all(...params: unknown[]): unknown[];
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
  sqliteWalMode?: "auto" | "off";
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

  // Detect MODULE_VERSION mismatch (common error pattern)
  const isModuleVersionError =
    errorMsg.includes("NODE_MODULE_VERSION") ||
    errorMsg.includes("MODULE_VERSION") ||
    errorMsg.includes("different Node.js version");

  const versionHint = isModuleVersionError
    ? " The SQLite binary was compiled for a different Node.js version. " +
      "RapiDB will fetch a host-compatible binary automatically, but the cached copy is stale or incompatible. " +
      "Reload VS Code and retry the SQLite connection to force a fresh download."
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

async function loadBetterSqlite3(): Promise<BetterSqlite3Constructor> {
  if (cachedBetterSqlite3) {
    return cachedBetterSqlite3;
  }

  let installError: unknown = null;
  try {
    await ensureSQLiteRuntimeInstalled(__dirname);
  } catch (error) {
    installError = error;
  }

  const attemptedTargets = resolveBetterSqlite3LoadTargets(__dirname, true);
  const probe = probeInstalledBetterSqlite3Runtime(__dirname);

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
  if (params.length === 0) {
    return statement[kind]() as T;
  }

  return statement[kind](params) as T;
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
): Promise<SQLiteDatabase> {
  const Database = await loadBetterSqlite3();
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
    database.pragma("foreign_keys = ON");

    if (!readOnly) {
      if (config.sqliteWalMode === "off") {
        database.pragma("journal_mode = DELETE");
      } else {
        try {
          database.pragma("journal_mode = WAL", { simple: true });
        } catch {}
      }
    }

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
