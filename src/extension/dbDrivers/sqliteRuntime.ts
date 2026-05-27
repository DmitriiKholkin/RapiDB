import { existsSync } from "node:fs";
import { dirname, join } from "node:path";

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
  | "alpine-x64"
  | "alpine-arm64"
  | "darwin-x64"
  | "darwin-arm64";

let cachedBetterSqlite3: BetterSqlite3Constructor | null = null;

function detectLinuxLibc(): "glibc" | "musl" | "unknown" {
  if (process.platform !== "linux") {
    return "unknown";
  }
  const report = process.report?.getReport?.() as
    | { header?: { glibcVersionRuntime?: string } }
    | undefined;
  const glibcVersion = report?.header?.glibcVersionRuntime;
  return typeof glibcVersion === "string" && glibcVersion.length > 0
    ? "glibc"
    : "musl";
}

export function resolveSQLiteRuntimeTarget(
  platform = process.platform,
  arch = process.arch,
  linuxLibc = detectLinuxLibc(),
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
    if (arch === "x64") {
      return linuxLibc === "musl" ? "alpine-x64" : "linux-x64";
    }
    if (arch === "arm64") {
      return linuxLibc === "musl" ? "alpine-arm64" : "linux-arm64";
    }
    return null;
  }
  return null;
}

function sqliteRuntimeTarget(): string {
  return resolveSQLiteRuntimeTarget() ?? `${process.platform}-${process.arch}`;
}

function findPackageRoot(startDir: string): string | null {
  let currentDir = startDir;
  while (true) {
    if (existsSync(join(currentDir, "package.json"))) {
      return currentDir;
    }
    const parentDir = dirname(currentDir);
    if (parentDir === currentDir) {
      return null;
    }
    currentDir = parentDir;
  }
}

function resolveStagedBetterSqlite3PackagePath(baseDir: string): string | null {
  const packageRoot = findPackageRoot(baseDir);
  if (!packageRoot) {
    return null;
  }
  const runtimeTarget = resolveSQLiteRuntimeTarget();
  if (!runtimeTarget) {
    return null;
  }
  const candidate = join(
    packageRoot,
    ".rapidb-vscode",
    "better-sqlite3",
    runtimeTarget,
    "node_modules",
    "better-sqlite3",
  );
  return existsSync(join(candidate, "package.json")) ? candidate : null;
}

export function resolveBetterSqlite3LoadTargets(
  baseDir = __dirname,
  preferStagedRuntime = Boolean(process.versions.electron),
): string[] {
  const targets: string[] = [];
  if (preferStagedRuntime) {
    const stagedPackage = resolveStagedBetterSqlite3PackagePath(baseDir);
    if (stagedPackage) {
      targets.push(stagedPackage);
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

function sqliteRuntimeLoadError(error: unknown): Error {
  const stagedHint = process.versions.electron
    ? ' Run "npm run native:sqlite:vscode" from the repository root to prepare a VS Code-compatible SQLite binary, or ship the staged .rapidb-vscode runtime in the VSIX for this target.'
    : "";
  return withCause(
    `[RapiDB] SQLite runtime is unavailable because better-sqlite3 could not be loaded for ${sqliteRuntimeTarget()}.${stagedHint} ${errorMessage(error)}`.trim(),
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

function loadBetterSqlite3(): BetterSqlite3Constructor {
  if (cachedBetterSqlite3) {
    return cachedBetterSqlite3;
  }

  let lastError: unknown = null;
  for (const target of resolveBetterSqlite3LoadTargets()) {
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
      new Error("better-sqlite3 did not export a database constructor."),
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

export function openSQLiteDatabase(
  config: SQLiteRuntimeOpenConfig,
): SQLiteDatabase {
  const Database = loadBetterSqlite3();
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
