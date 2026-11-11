import { execFile } from "node:child_process";
import { randomUUID } from "node:crypto";
import {
  chmodSync,
  cpSync,
  existsSync,
  lstatSync,
  mkdirSync,
  mkdtempSync,
  readdirSync,
  readFileSync,
  renameSync,
  rmdirSync,
  rmSync,
  statSync,
  utimesSync,
  writeFileSync,
} from "node:fs";
import { hostname, tmpdir } from "node:os";
import { basename, dirname, join, relative, resolve } from "node:path";
import { promisify } from "node:util";
import { logger } from "./logger";
import {
  extractSQLiteArchive,
  SQLITE_ARCHIVE_LIMITS,
  SQLITE_SOURCE,
  verifySQLiteIntegrity,
} from "./sqliteArchive";
import {
  type SQLiteArtifactPin,
  type SQLiteArtifactTarget,
  type SQLiteHeaderPins,
  sqliteArtifactPolicyId,
  sqliteHeaderPins,
  sqlitePrebuiltFileName,
  sqlitePrebuiltPin,
} from "./sqliteArtifactPins";
import {
  assertSQLiteBuildPath,
  createSQLiteBuildToolLock,
  pinSQLiteSourceNodeActions,
  resolveSQLiteBuildTools,
} from "./sqliteBuildTools";

const execFileAsync = promisify(execFile);

const SQLITE_DOWNLOAD_TIMEOUT_MS = 60_000;
const SQLITE_BUILD_STEP_TIMEOUT_MS = 300_000;
const SQLITE_INSTALL_MAX_DURATION_MS =
  5 * SQLITE_DOWNLOAD_TIMEOUT_MS + 2 * SQLITE_BUILD_STEP_TIMEOUT_MS + 120_000;
const SQLITE_INSTALL_LOCK_OWNER_FILE = "owner.json";
const SQLITE_INSTALL_LOCK_TRANSIENT_ERRORS = new Set([
  "EPERM",
  "EBUSY",
  "EACCES",
]);
const DEFAULT_SQLITE_INSTALL_LOCK_TIMINGS = {
  waitMs: SQLITE_INSTALL_MAX_DURATION_MS + 60_000,
  initGraceMs: 10_000,
  heartbeatMs: 15_000,
  staleHeartbeatMs: 120_000,
  clockSkewMs: 300_000,
  fastPollMs: 150,
  slowPollMs: 1_000,
  fastPollWindowMs: 5_000,
  releaseRetryMs: 100,
  releaseAttempts: 5,
};
let installLockTimings = { ...DEFAULT_SQLITE_INSTALL_LOCK_TIMINGS };
const SQLITE_RUNTIME_LAST_USED_FILE = ".rapidb-last-used";
const SQLITE_OBSOLETE_RUNTIME_MAX_IDLE_MS = 30 * 24 * 60 * 60 * 1000;
// Runtimes used this recently cannot be cleanup candidates, so their use can
// be recorded without the runtime lock.
const SQLITE_RUNTIME_RECENT_USE_MS = 7 * 24 * 60 * 60 * 1000;
const SQLITE_RUNTIME_USE_CLOCK_SKEW_MS = 5 * 60 * 1000;
const SQLITE_OBSOLETE_RUNTIME_MAX_REMOVALS = 3;
const SQLITE_RUNTIME_VERSION_PATTERN = /^\d+\.\d+\.\d+(?:-[A-Za-z0-9.-]+)?$/;
const SQLITE_RUNTIME_NAME =
  "(?:electron|node)-abi-[0-9]+(?:-[a-z0-9]+){2,3}(?:-archive-v1|-pins-v[0-9]+-[0-9a-f]{16})?";
const SQLITE_RUNTIME_DIRECTORY_PATTERN = new RegExp(`^${SQLITE_RUNTIME_NAME}$`);
const SQLITE_RUNTIME_TRASH_PATTERN = new RegExp(
  `^${SQLITE_RUNTIME_NAME}\\.obsolete-[0-9]+-[0-9a-f-]{36}$`,
);
const SQLITE_USE_TRACKING_POLICY_PATTERN = /-pins-v([0-9]+)-[0-9a-f]{16}$/;
const SQLITE_USE_TRACKING_MIN_POLICY_VERSION = 5;

interface SQLiteInstallLockOwner {
  pid: number;
  hostname: string;
  createdAt: number;
  token: string;
}

type BetterSqlite3PackageJson = {
  name?: string;
  version?: string;
  repository?: unknown;
};

export interface SQLiteInstalledRuntimeProbe {
  runtime: "electron" | "node";
  target: string;
  packageRoot: string | null;
  bundledPackagePath: string | null;
  bundledPackageExists: boolean;
  installedPackagePath: string | null;
  installedPackageExists: boolean;
  installedBinaryPath: string | null;
  installedBinaryExists: boolean;
}

interface SQLiteInstallerConfiguration {
  storageRoot: string;
  log?: (message: string) => void;
  allowInstall?: () => boolean | Promise<boolean>;
  workspaceRoots?: () => readonly string[];
}

interface InstalledRuntimeManifest {
  betterSqlite3Version: string;
  runtime: "electron" | "node";
  abi: string;
  platform: NodeJS.Platform;
  arch: NodeJS.Architecture;
  libc: string;
}

let installerConfiguration: SQLiteInstallerConfiguration | null = null;
let inFlightInstall: Promise<string | null> | null = null;
let obsoleteRuntimeCleanupDone = false;

function installerLog(message: string): void {
  const line = `[RapiDB SQLite] ${message}`;
  installerConfiguration?.log?.(line);
  if (!installerConfiguration?.log) {
    logger.info(line);
  }
}

function errorMessage(error: unknown): string {
  if (error instanceof Error && error.message.trim().length > 0) {
    return error.message;
  }
  return String(error);
}

function currentRuntime(): "electron" | "node" {
  return process.versions.electron ? "electron" : "node";
}

function detectLinuxLibc(): string {
  if (process.platform !== "linux") {
    return "";
  }

  try {
    const detectLibc = require("detect-libc") as {
      GLIBC?: string;
      familySync?: () => string | null;
      isNonGlibcLinuxSync?: () => boolean;
    };
    if (!detectLibc.isNonGlibcLinuxSync?.()) {
      return "";
    }
    const family = detectLibc.familySync?.();
    if (!family || family === detectLibc.GLIBC) {
      return "";
    }
    return family.toLowerCase();
  } catch {
    return "";
  }
}

function currentTargetLabel(): string {
  return [process.platform, detectLinuxLibc() || null, process.arch]
    .filter(
      (part): part is string => typeof part === "string" && part.length > 0,
    )
    .join("-");
}

function currentArtifactTarget(
  betterSqlite3Version: string,
): SQLiteArtifactTarget {
  return {
    betterSqlite3Version,
    runtime: currentRuntime(),
    abi: process.versions.modules,
    platform: process.platform,
    arch: process.arch,
    libc: detectLinuxLibc(),
    electronVersion: process.versions.electron,
  };
}

function currentRuntimeStorageKey(betterSqlite3Version: string): string {
  return [
    currentRuntime(),
    `abi-${process.versions.modules}`,
    process.platform,
    detectLinuxLibc() || null,
    process.arch,
    sqliteArtifactPolicyId(currentArtifactTarget(betterSqlite3Version)),
  ]
    .filter(
      (part): part is string => typeof part === "string" && part.length > 0,
    )
    .join("-");
}

function findPackageRoot(startDir: string): string | null {
  let currentDir = resolve(startDir);
  while (true) {
    if (existsSync(join(currentDir, "package.json"))) {
      return currentDir;
    }
    const parentDir = resolve(currentDir, "..");
    if (parentDir === currentDir) {
      return null;
    }
    currentDir = parentDir;
  }
}

function bundledPackagePathFor(baseDir: string): string | null {
  const packageRoot = findPackageRoot(baseDir);
  if (!packageRoot) {
    return null;
  }

  for (const candidate of [
    join(packageRoot, ".rapidb-runtime", "node_modules", "better-sqlite3"),
    join(packageRoot, "node_modules", "better-sqlite3"),
  ]) {
    if (existsSync(join(candidate, "package.json"))) {
      return candidate;
    }
  }

  return join(packageRoot, ".rapidb-runtime", "node_modules", "better-sqlite3");
}

function bundledHelperPackagePathFor(
  baseDir: string,
  packageName: "bindings" | "file-uri-to-path",
): string | null {
  const packageRoot = findPackageRoot(baseDir);
  if (!packageRoot) {
    return null;
  }

  for (const candidate of [
    join(packageRoot, ".rapidb-runtime", "node_modules", packageName),
    join(packageRoot, "node_modules", packageName),
  ]) {
    if (existsSync(join(candidate, "package.json"))) {
      return candidate;
    }
  }

  return join(packageRoot, ".rapidb-runtime", "node_modules", packageName);
}

function readBundledBetterSqlite3Package(
  baseDir: string,
): BetterSqlite3PackageJson {
  const bundledPackagePath = bundledPackagePathFor(baseDir);
  if (!bundledPackagePath) {
    throw new Error(
      "Could not resolve the extension package root for SQLite runtime installation.",
    );
  }

  const packageJsonPath = join(bundledPackagePath, "package.json");
  if (!existsSync(packageJsonPath)) {
    throw new Error(
      "The packaged better-sqlite3 scaffold is missing. Ensure node_modules/better-sqlite3 is included in the VSIX.",
    );
  }

  return JSON.parse(
    readFileSync(packageJsonPath, "utf8"),
  ) as BetterSqlite3PackageJson;
}

function expectedManifestFor(baseDir: string): InstalledRuntimeManifest {
  const pkg = readBundledBetterSqlite3Package(baseDir);
  if (!pkg.version) {
    throw new Error("better-sqlite3/package.json does not declare a version.");
  }
  return {
    betterSqlite3Version: pkg.version,
    runtime: currentRuntime(),
    abi: process.versions.modules,
    platform: process.platform,
    arch: process.arch,
    libc: detectLinuxLibc(),
  };
}

function runtimeRootFor(baseDir: string): string | null {
  const packageRoot = findPackageRoot(baseDir);
  if (!packageRoot || !installerConfiguration) {
    return null;
  }

  const manifest = expectedManifestFor(baseDir);
  return join(
    installerConfiguration.storageRoot,
    "sqlite-runtime",
    "better-sqlite3",
    manifest.betterSqlite3Version,
    currentRuntimeStorageKey(manifest.betterSqlite3Version),
  );
}

function installedPackagePathFor(baseDir: string): string | null {
  const runtimeRoot = runtimeRootFor(baseDir);
  if (!runtimeRoot) {
    return null;
  }
  return join(runtimeRoot, "node_modules", "better-sqlite3");
}

function installedBinaryPathFor(baseDir: string): string | null {
  const installedPackagePath = installedPackagePathFor(baseDir);
  if (!installedPackagePath) {
    return null;
  }
  return join(installedPackagePath, "build", "Release", "better_sqlite3.node");
}

function installedManifestPathFor(baseDir: string): string | null {
  const runtimeRoot = runtimeRootFor(baseDir);
  if (!runtimeRoot) {
    return null;
  }
  return join(runtimeRoot, "runtime.json");
}

function manifestMatches(baseDir: string): boolean {
  const manifestPath = installedManifestPathFor(baseDir);
  const binaryPath = installedBinaryPathFor(baseDir);
  const packagePath = installedPackagePathFor(baseDir);
  if (!manifestPath || !binaryPath || !packagePath) {
    return false;
  }
  if (
    !existsSync(manifestPath) ||
    !existsSync(binaryPath) ||
    !existsSync(join(packagePath, "package.json"))
  ) {
    return false;
  }

  let actual: Partial<InstalledRuntimeManifest> | null;
  try {
    actual = JSON.parse(readFileSync(manifestPath, "utf8"));
  } catch {
    return false;
  }
  if (!actual || typeof actual !== "object") {
    return false;
  }
  const expected = expectedManifestFor(baseDir);
  return (
    actual.betterSqlite3Version === expected.betterSqlite3Version &&
    actual.runtime === expected.runtime &&
    actual.abi === expected.abi &&
    actual.platform === expected.platform &&
    actual.arch === expected.arch &&
    actual.libc === expected.libc
  );
}

function copyDirectory(sourceRoot: string, targetRoot: string): void {
  cpSync(sourceRoot, targetRoot, {
    recursive: true,
    force: true,
    filter(sourcePath) {
      const rel = relative(sourceRoot, sourcePath);
      return rel !== "" || existsSync(sourceRoot);
    },
  });
}

function copyBundledRuntimeScaffold(
  baseDir: string,
  runtimeRoot: string,
): void {
  const bundledBetterSqlite3Path = bundledPackagePathFor(baseDir);
  const bundledBindingsPath = bundledHelperPackagePathFor(baseDir, "bindings");
  const bundledFileUriToPathPath = bundledHelperPackagePathFor(
    baseDir,
    "file-uri-to-path",
  );
  if (
    !bundledBetterSqlite3Path ||
    !bundledBindingsPath ||
    !bundledFileUriToPathPath ||
    !existsSync(join(bundledBetterSqlite3Path, "package.json")) ||
    !existsSync(join(bundledBindingsPath, "package.json")) ||
    !existsSync(join(bundledFileUriToPathPath, "package.json"))
  ) {
    throw new Error(
      "The packaged better-sqlite3 scaffold is incomplete. Include better-sqlite3, bindings, and file-uri-to-path in the VSIX.",
    );
  }

  const nodeModulesRoot = join(runtimeRoot, "node_modules");
  mkdirSync(nodeModulesRoot, { recursive: true });
  copyDirectory(
    bundledBetterSqlite3Path,
    join(nodeModulesRoot, "better-sqlite3"),
  );
  copyDirectory(bundledBindingsPath, join(nodeModulesRoot, "bindings"));
  copyDirectory(
    bundledFileUriToPathPath,
    join(nodeModulesRoot, "file-uri-to-path"),
  );
  rmSync(join(nodeModulesRoot, "better-sqlite3", "build"), {
    recursive: true,
    force: true,
  });
}

async function waitFor(milliseconds: number): Promise<void> {
  await new Promise((resolvePromise) => {
    setTimeout(resolvePromise, milliseconds);
  });
}

function isProcessAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    return (error as NodeJS.ErrnoException).code !== "ESRCH";
  }
}

function isTransientLockError(error: unknown): boolean {
  return SQLITE_INSTALL_LOCK_TRANSIENT_ERRORS.has(
    (error as NodeJS.ErrnoException).code ?? "",
  );
}

function readInstallLockOwner(
  lockRoot: string,
): { owner: SQLiteInstallLockOwner; heartbeatAt: number } | null {
  try {
    const ownerPath = join(lockRoot, SQLITE_INSTALL_LOCK_OWNER_FILE);
    const owner = JSON.parse(
      readFileSync(ownerPath, "utf8"),
    ) as Partial<SQLiteInstallLockOwner>;
    if (
      typeof owner.pid === "number" &&
      Number.isSafeInteger(owner.pid) &&
      owner.pid > 0 &&
      typeof owner.hostname === "string" &&
      typeof owner.createdAt === "number" &&
      Number.isFinite(owner.createdAt) &&
      typeof owner.token === "string"
    ) {
      return {
        owner: owner as SQLiteInstallLockOwner,
        heartbeatAt: statSync(ownerPath).mtimeMs,
      };
    }
  } catch {}
  return null;
}

function inspectInstallLock(lockRoot: string): {
  id: string;
  stale: boolean;
  heartbeatAt: number | null;
  description: string;
} | null {
  let stats: import("node:fs").BigIntStats;
  try {
    stats = statSync(lockRoot, { bigint: true });
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      return null;
    }
    throw error;
  }
  const record = readInstallLockOwner(lockRoot);
  const now = Date.now();
  if (!record) {
    const age = now - Number(stats.mtimeMs);
    return {
      id: `dir:${stats.dev}:${stats.ino}`,
      stale: age > installLockTimings.initGraceMs,
      heartbeatAt: null,
      description: `no owner record, created ${Math.round(age / 1000)}s ago`,
    };
  }
  const { owner, heartbeatAt } = record;
  const sameHost = owner.hostname === hostname();
  return {
    id: `owner:${owner.token}`,
    stale:
      owner.createdAt > now + installLockTimings.clockSkewMs ||
      (sameHost && !isProcessAlive(owner.pid)),
    heartbeatAt,
    description: `pid ${owner.pid} on ${owner.hostname}, started ${Math.round((now - owner.createdAt) / 1000)}s ago, last heartbeat ${Math.round((now - heartbeatAt) / 1000)}s ago`,
  };
}

function removeInstallLock(
  lockRoot: string,
  expectedId: string,
): { status: "removed" | "busy" | "kept"; error?: unknown } {
  const tombstone = `${lockRoot}.stale-${process.pid}-${randomUUID()}`;
  try {
    renameSync(lockRoot, tombstone);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      return { status: "kept" };
    }
    if (isTransientLockError(error)) {
      return { status: "busy", error };
    }
    throw error;
  }
  if (inspectInstallLock(tombstone)?.id !== expectedId) {
    try {
      renameSync(tombstone, lockRoot);
    } catch (error) {
      installerLog(
        `Could not restore SQLite runtime installation lock ${tombstone} to ${lockRoot}: ${errorMessage(error)}. Leaving it in place.`,
      );
    }
    return { status: "kept" };
  }
  try {
    rmSync(tombstone, { recursive: true, force: true });
  } catch (error) {
    installerLog(
      `Could not delete SQLite runtime installation lock ${tombstone}: ${errorMessage(error)}.`,
    );
  }
  return { status: "removed" };
}

function tryCreateInstallLock(lockRoot: string, token: string): boolean {
  try {
    mkdirSync(lockRoot, { recursive: false });
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "EEXIST") {
      return false;
    }
    throw error;
  }
  const owner: SQLiteInstallLockOwner = {
    pid: process.pid,
    hostname: hostname(),
    createdAt: Date.now(),
    token,
  };
  try {
    writeFileSync(
      join(lockRoot, SQLITE_INSTALL_LOCK_OWNER_FILE),
      JSON.stringify(owner),
      { flag: "wx" },
    );
    return true;
  } catch (error) {
    const code = (error as NodeJS.ErrnoException).code;
    if (code === "EEXIST" || code === "ENOENT") {
      return false;
    }
    try {
      rmdirSync(lockRoot);
    } catch {}
    throw error;
  }
}

function refreshInstallLockHeartbeat(lockRoot: string, token: string): unknown {
  try {
    if (readInstallLockOwner(lockRoot)?.owner.token === token) {
      const now = new Date();
      utimesSync(join(lockRoot, SQLITE_INSTALL_LOCK_OWNER_FILE), now, now);
    }
    return null;
  } catch (error) {
    return error;
  }
}

async function releaseInstallLock(
  lockRoot: string,
  token: string,
): Promise<void> {
  for (let attempt = 1; ; attempt += 1) {
    try {
      if (readInstallLockOwner(lockRoot)?.owner.token !== token) {
        return;
      }
      const removal = removeInstallLock(lockRoot, `owner:${token}`);
      if (removal.status !== "busy") {
        return;
      }
      if (attempt >= installLockTimings.releaseAttempts) {
        throw removal.error;
      }
    } catch (error) {
      installerLog(
        `Could not release SQLite runtime installation lock at ${lockRoot}: ${errorMessage(error)}. Other windows will reclaim it once this process exits.`,
      );
      return;
    }
    await waitFor(installLockTimings.releaseRetryMs * attempt);
  }
}

function removeAbandonedInstallDirectories(runtimeRoot: string): void {
  const parent = dirname(runtimeRoot);
  const temporaryPrefix = `${basename(runtimeRoot)}.tmp-`;
  const tombstonePrefix = `${basename(runtimeRoot)}.lock.stale-`;
  let entries: string[];
  try {
    entries = readdirSync(parent);
  } catch {
    return;
  }
  for (const entry of entries) {
    const path = join(parent, entry);
    if (entry.startsWith(tombstonePrefix)) {
      const tombstone = inspectInstallLock(path);
      if (
        tombstone &&
        !tombstone.stale &&
        (tombstone.heartbeatAt === null ||
          Date.now() - tombstone.heartbeatAt <=
            installLockTimings.staleHeartbeatMs)
      ) {
        continue;
      }
    } else if (entry.startsWith(temporaryPrefix)) {
      const pid = Number(entry.slice(temporaryPrefix.length).split("-")[0]);
      if (
        pid !== process.pid &&
        Number.isSafeInteger(pid) &&
        pid > 0 &&
        isProcessAlive(pid)
      ) {
        continue;
      }
    } else {
      continue;
    }
    try {
      rmSync(path, { recursive: true, force: true });
    } catch {}
  }
}

function isRealDirectory(path: string): boolean {
  try {
    return lstatSync(path).isDirectory();
  } catch {
    return false;
  }
}

function refreshRuntimeUse(runtimeRoot: string): boolean {
  try {
    const now = new Date();
    utimesSync(join(runtimeRoot, SQLITE_RUNTIME_LAST_USED_FILE), now, now);
    return true;
  } catch {
    return false;
  }
}

function markRuntimeUsed(runtimeRoot: string): void {
  if (refreshRuntimeUse(runtimeRoot)) {
    return;
  }
  const marker = join(runtimeRoot, SQLITE_RUNTIME_LAST_USED_FILE);
  try {
    writeFileSync(marker, "", { flag: "a" });
  } catch (error) {
    installerLog(
      `Could not record SQLite runtime use at ${marker}: ${errorMessage(error)}.`,
    );
  }
}

function tracksRuntimeUse(name: string): boolean {
  const policy = SQLITE_USE_TRACKING_POLICY_PATTERN.exec(name);
  return (
    policy !== null &&
    Number(policy[1]) >= SQLITE_USE_TRACKING_MIN_POLICY_VERSION
  );
}

function runtimeLastUsedAt(runtimeRoot: string): number | null | undefined {
  try {
    if (!lstatSync(join(runtimeRoot, "runtime.json")).isFile()) {
      return undefined;
    }
  } catch {
    return undefined;
  }
  try {
    const marker = lstatSync(join(runtimeRoot, SQLITE_RUNTIME_LAST_USED_FILE));
    return marker.isFile() ? marker.mtimeMs : undefined;
  } catch (error) {
    return (error as NodeJS.ErrnoException).code === "ENOENT"
      ? null
      : undefined;
  }
}

function isIdleSince(lastUsedAt: number): boolean {
  return Date.now() - lastUsedAt > SQLITE_OBSOLETE_RUNTIME_MAX_IDLE_MS;
}

function wasUsedRecently(runtimeRoot: string): boolean {
  try {
    const marker = lstatSync(join(runtimeRoot, SQLITE_RUNTIME_LAST_USED_FILE));
    const age = Date.now() - marker.mtimeMs;
    return (
      marker.isFile() &&
      age >= -SQLITE_RUNTIME_USE_CLOCK_SKEW_MS &&
      age < SQLITE_RUNTIME_RECENT_USE_MS
    );
  } catch {
    return false;
  }
}

function isOlderThanIdleLimit(path: string): boolean {
  try {
    return isIdleSince(lstatSync(path).mtimeMs);
  } catch {
    return false;
  }
}

async function isRuntimeUnlocked(runtimeRoot: string): Promise<boolean> {
  const lockRoot = `${runtimeRoot}.lock`;
  const token = randomUUID();
  if (!tryCreateInstallLock(lockRoot, token)) {
    return false;
  }
  await releaseInstallLock(lockRoot, token);
  return true;
}

async function removeObsoleteRuntime(runtimeRoot: string): Promise<boolean> {
  const observed = runtimeLastUsedAt(runtimeRoot);
  if (observed === undefined || (observed !== null && !isIdleSince(observed))) {
    return false;
  }
  const lockRoot = `${runtimeRoot}.lock`;
  const token = randomUUID();
  if (!tryCreateInstallLock(lockRoot, token)) {
    return false;
  }
  const trash = `${runtimeRoot}.obsolete-${process.pid}-${randomUUID()}`;
  try {
    const lastUsedAt = runtimeLastUsedAt(runtimeRoot);
    if (lastUsedAt === null) {
      markRuntimeUsed(runtimeRoot);
      return false;
    }
    if (lastUsedAt === undefined || !isIdleSince(lastUsedAt)) {
      return false;
    }
    renameSync(runtimeRoot, trash);
    // Readers record stale use under this lock; this still catches lock-free
    // use if clocks diverged by more than the recent-use margin.
    if (runtimeLastUsedAt(trash) !== lastUsedAt) {
      if (!existsSync(runtimeRoot)) {
        renameSync(trash, runtimeRoot);
      }
      installerLog(
        `Kept SQLite runtime ${runtimeRoot}; it was used while being removed.`,
      );
      return false;
    }
  } finally {
    await releaseInstallLock(lockRoot, token);
  }
  installerLog(
    `Removing SQLite runtime ${runtimeRoot}; it has not been used for over 30 days.`,
  );
  rmSync(trash, { recursive: true, force: true });
  return true;
}

export async function removeObsoleteSQLiteRuntimes(
  baseDir: string,
): Promise<string[]> {
  const removed: string[] = [];
  try {
    const currentRoot = runtimeRootFor(baseDir);
    if (!currentRoot) {
      return removed;
    }
    const cacheRoot = dirname(dirname(currentRoot));
    if (!isRealDirectory(dirname(cacheRoot)) || !isRealDirectory(cacheRoot)) {
      return removed;
    }
    for (const version of readdirSync(cacheRoot)) {
      const versionRoot = join(cacheRoot, version);
      if (
        !SQLITE_RUNTIME_VERSION_PATTERN.test(version) ||
        !isRealDirectory(versionRoot)
      ) {
        continue;
      }
      let entries: string[];
      try {
        entries = readdirSync(versionRoot);
      } catch {
        continue;
      }
      const busy = new Set<string>();
      const abandoned = new Set<string>();
      for (const entry of entries) {
        const name = entry.split(".")[0];
        if (
          (entry.startsWith(`${name}.tmp-`) ||
            entry.startsWith(`${name}.lock.stale-`)) &&
          SQLITE_RUNTIME_DIRECTORY_PATTERN.test(name)
        ) {
          const path = join(versionRoot, entry);
          if (
            join(versionRoot, name) !== currentRoot &&
            isRealDirectory(path) &&
            isOlderThanIdleLimit(path)
          ) {
            abandoned.add(entry);
          } else {
            busy.add(name);
          }
        }
      }
      for (const entry of entries) {
        if (removed.length >= SQLITE_OBSOLETE_RUNTIME_MAX_REMOVALS) {
          return removed;
        }
        const path = join(versionRoot, entry);
        if (path === currentRoot || !isRealDirectory(path)) {
          continue;
        }
        try {
          if (
            abandoned.has(entry) ||
            (SQLITE_RUNTIME_TRASH_PATTERN.test(entry) &&
              (await isRuntimeUnlocked(join(versionRoot, entry.split(".")[0]))))
          ) {
            rmSync(path, { recursive: true, force: true });
            removed.push(path);
          } else if (
            SQLITE_RUNTIME_DIRECTORY_PATTERN.test(entry) &&
            tracksRuntimeUse(entry) &&
            !busy.has(entry) &&
            (await removeObsoleteRuntime(path))
          ) {
            removed.push(path);
          }
        } catch (error) {
          installerLog(
            `Could not remove obsolete SQLite runtime ${path}: ${errorMessage(error)}.`,
          );
        }
      }
    }
  } catch (error) {
    installerLog(
      `Could not clean up obsolete SQLite runtimes: ${errorMessage(error)}.`,
    );
  }
  return removed;
}

async function removeObsoleteSQLiteRuntimesOnce(
  baseDir: string,
): Promise<void> {
  if (!obsoleteRuntimeCleanupDone) {
    obsoleteRuntimeCleanupDone = true;
    await removeObsoleteSQLiteRuntimes(baseDir);
  }
}

function refreshInstalledRuntimeUse(baseDir: string): boolean {
  const runtimeRoot = runtimeRootFor(baseDir);
  return (
    runtimeRoot !== null &&
    manifestMatches(baseDir) &&
    wasUsedRecently(runtimeRoot) &&
    refreshRuntimeUse(runtimeRoot) &&
    existsSync(join(runtimeRoot, "runtime.json"))
  );
}

async function recordInstalledRuntimeUse(
  baseDir: string,
): Promise<string | null> {
  const runtimeRoot = runtimeRootFor(baseDir);
  const installedPackagePath = installedPackagePathFor(baseDir);
  if (
    !runtimeRoot ||
    !installedPackagePath ||
    !isRealDirectory(dirname(runtimeRoot))
  ) {
    return null;
  }
  return withInstallLock<string | null>(
    `${runtimeRoot}.lock`,
    async () => {
      if (!manifestMatches(baseDir)) {
        return null;
      }
      markRuntimeUsed(runtimeRoot);
      return installedPackagePath;
    },
    () => (refreshInstalledRuntimeUse(baseDir) ? installedPackagePath : null),
  );
}

async function withInstallLock<T>(
  lockRoot: string,
  action: () => Promise<T>,
  completed: () => T | null,
): Promise<T> {
  mkdirSync(dirname(lockRoot), { recursive: true });
  const token = randomUUID();
  const startedAt = performance.now();
  const deadline = startedAt + installLockTimings.waitMs;
  const reported = new Set<string>();
  const report = (key: string, message: string): void => {
    if (!reported.has(key)) {
      reported.add(key);
      installerLog(message);
    }
  };
  const observedHeartbeat = { key: "", since: 0 };
  while (true) {
    const result = completed();
    if (result !== null) {
      return result;
    }
    if (tryCreateInstallLock(lockRoot, token)) {
      break;
    }
    const current = inspectInstallLock(lockRoot);
    let stale = current?.stale ?? false;
    if (current && !stale && current.heartbeatAt !== null) {
      const key = `${current.id}:${current.heartbeatAt}`;
      if (observedHeartbeat.key !== key) {
        observedHeartbeat.key = key;
        observedHeartbeat.since = performance.now();
      } else if (
        performance.now() - observedHeartbeat.since >
        installLockTimings.staleHeartbeatMs
      ) {
        stale = true;
      }
    }
    if (current && stale) {
      report(
        `stale:${current.id}`,
        `Removing stale SQLite runtime installation lock at ${lockRoot} (${current.description}).`,
      );
      const removal = removeInstallLock(lockRoot, current.id);
      if (removal.status === "removed") {
        continue;
      }
      if (removal.status === "busy") {
        report(
          `busy:${current.id}`,
          `SQLite runtime installation lock at ${lockRoot} is busy (${errorMessage(removal.error)}); retrying.`,
        );
      }
    }
    if (current) {
      report(
        "waiting",
        `Waiting for another SQLite runtime installation to finish (${current.description}); lock at ${lockRoot}.`,
      );
    }
    if (performance.now() >= deadline) {
      throw new Error(
        `Timed out waiting for SQLite runtime installation lock at ${lockRoot} (${current?.description ?? "lock released"}). If no other VS Code window is installing the SQLite runtime, close other windows and delete this folder, then retry.`,
      );
    }
    await waitFor(
      performance.now() - startedAt < installLockTimings.fastPollWindowMs
        ? installLockTimings.fastPollMs
        : installLockTimings.slowPollMs,
    );
  }

  const heartbeat = setInterval(() => {
    const error = refreshInstallLockHeartbeat(lockRoot, token);
    if (error) {
      report(
        "heartbeat",
        `Could not refresh SQLite runtime installation lock heartbeat at ${lockRoot}: ${errorMessage(error)}.`,
      );
    }
  }, installLockTimings.heartbeatMs);
  heartbeat.unref?.();
  try {
    return await action();
  } finally {
    clearInterval(heartbeat);
    await releaseInstallLock(lockRoot, token);
  }
}

const GITHUB_DOWNLOAD_HOSTS = [
  "github.com",
  "release-assets.githubusercontent.com",
  "objects.githubusercontent.com",
];

interface DownloadLimits {
  maxBytes?: number;
  allowedHosts?: readonly string[];
}

export function downloadToFile(
  url: string,
  destPath: string,
  timeoutMs = SQLITE_DOWNLOAD_TIMEOUT_MS,
  options: DownloadLimits = {},
): Promise<void> {
  const { createWriteStream } = require("node:fs") as typeof import("node:fs");
  const https = require("node:https") as typeof import("node:https");
  const http = require("node:http") as typeof import("node:http");
  const maxBytes = options.maxBytes ?? SQLITE_ARCHIVE_LIMITS.compressedBytes;
  const allowLoopbackHttp = url.startsWith("http://") && !options.allowedHosts;

  return new Promise<void>((resolvePromise, rejectPromise) => {
    let settled = false;
    let abortActiveRequest: (() => void) | undefined;
    let downloadFile: import("node:fs").WriteStream | undefined;
    const resolveOnce = (): void => {
      if (!settled) {
        settled = true;
        clearTimeout(deadline);
        resolvePromise();
      }
    };
    const rejectOnce = (error: Error): void => {
      if (!settled) {
        settled = true;
        clearTimeout(deadline);
        abortActiveRequest?.();
        const cleanup = (): void => {
          try {
            rmSync(destPath, { force: true });
          } catch {}
          rejectPromise(error);
        };
        if (downloadFile && !downloadFile.closed)
          downloadFile.once("close", cleanup);
        else cleanup();
      }
    };
    const deadline = setTimeout(() => {
      const error = new Error(`Timed out downloading ${url}`);
      abortActiveRequest?.();
      rejectOnce(error);
    }, timeoutMs);
    const follow = (target: string, depth: number): void => {
      if (settled) {
        return;
      }
      if (depth > 5) {
        rejectOnce(new Error(`Too many redirects downloading ${url}`));
        return;
      }
      let resolvedTarget: string;
      try {
        resolvedTarget = new URL(target).toString();
      } catch (error) {
        rejectOnce(
          error instanceof Error ? error : new Error(`Invalid URL: ${target}`),
        );
        return;
      }
      if (
        options.allowedHosts &&
        !options.allowedHosts.includes(new URL(resolvedTarget).hostname)
      ) {
        rejectOnce(
          new Error(`Refusing untrusted SQLite download host: ${target}`),
        );
        return;
      }
      if (
        !resolvedTarget.startsWith("https://") &&
        !resolvedTarget.startsWith("http://")
      ) {
        rejectOnce(new Error(`Unsupported download protocol: ${target}`));
        return;
      }
      if (resolvedTarget.startsWith("http://")) {
        let hostname = "";
        try {
          hostname = new URL(resolvedTarget).hostname;
        } catch {}
        if (
          !allowLoopbackHttp ||
          (hostname !== "127.0.0.1" &&
            hostname !== "localhost" &&
            hostname !== "[::1]")
        ) {
          rejectOnce(new Error(`Refusing non-loopback http URL: ${target}`));
          return;
        }
      }
      const client = resolvedTarget.startsWith("http://") ? http : https;
      let activeResponse: any | undefined;
      let activeFile: any | undefined;
      let request: any;
      abortActiveRequest = () => {
        activeResponse?.destroy();
        activeFile?.destroy();
        request?.destroy();
      };
      try {
        request = client.get(resolvedTarget, (response) => {
          activeResponse = response;
          if (settled) {
            response.destroy();
            return;
          }
          const status = response.statusCode ?? 0;
          if (
            (status === 301 ||
              status === 302 ||
              status === 303 ||
              status === 307 ||
              status === 308) &&
            response.headers.location
          ) {
            response.destroy();
            let next: string;
            try {
              next = new URL(
                response.headers.location,
                resolvedTarget,
              ).toString();
            } catch (error) {
              rejectOnce(
                error instanceof Error
                  ? error
                  : new Error(`Invalid redirect: ${response.headers.location}`),
              );
              return;
            }
            follow(next, depth + 1);
            return;
          }
          if (status !== 200) {
            response.resume();
            rejectOnce(
              new Error(`HTTP ${status} downloading ${resolvedTarget}`),
            );
            return;
          }
          const declaredLength = Number(response.headers["content-length"]);
          if (declaredLength > maxBytes) {
            rejectOnce(
              new Error(
                `SQLite download exceeds size limit (${maxBytes} bytes).`,
              ),
            );
            return;
          }
          const file = createWriteStream(destPath);
          downloadFile = file;
          activeFile = file;
          let receivedBytes = 0;
          response.on("data", (chunk: Buffer) => {
            receivedBytes += chunk.length;
            if (receivedBytes > maxBytes) {
              rejectOnce(
                new Error(
                  `SQLite download exceeds size limit (${maxBytes} bytes).`,
                ),
              );
            }
          });
          response.on("error", (err: Error) => {
            try {
              file.destroy();
            } catch {}
            rejectOnce(err);
          });
          file.on("error", (err: Error) => {
            try {
              response.destroy();
            } catch {}
            rejectOnce(err);
          });
          file.on("finish", () => {
            file.close((closeErr?: Error | null) => {
              if (closeErr) {
                rejectOnce(closeErr);
              } else {
                resolveOnce();
              }
            });
          });
          response.pipe(file);
        });
      } catch (error) {
        rejectOnce(
          error instanceof Error
            ? error
            : new Error(`Request failed: ${target}`),
        );
        return;
      }
      request.on("error", (err: Error) => {
        try {
          activeResponse?.destroy();
        } catch {}
        try {
          activeFile?.destroy();
        } catch {}
        rejectOnce(err);
      });
      request.setTimeout(timeoutMs, () => {
        const timeoutError = new Error(
          `Timed out downloading ${resolvedTarget}`,
        );
        try {
          activeResponse?.destroy();
        } catch {}
        try {
          activeFile?.destroy();
        } catch {}
        request.destroy(timeoutError);
      });
    };
    follow(url, 0);
  });
}

export async function downloadPinnedSQLiteArtifact(
  pin: SQLiteArtifactPin,
  destination: string,
  allowedHosts: readonly string[],
): Promise<void> {
  if (
    !pin.integrity ||
    !Number.isSafeInteger(pin.size) ||
    pin.size <= 0 ||
    pin.size > SQLITE_ARCHIVE_LIMITS.compressedBytes
  )
    throw new Error("Missing/invalid static SQLite artifact pin.");
  await downloadToFile(pin.url, destination, SQLITE_DOWNLOAD_TIMEOUT_MS, {
    maxBytes: pin.size,
    allowedHosts,
  });
  try {
    const bytes = readFileSync(destination);
    if (bytes.length !== pin.size)
      throw new Error("SQLite artifact size does not match its reviewed pin.");
    verifySQLiteIntegrity(bytes, pin.integrity);
  } catch (error) {
    rmSync(destination, { force: true });
    throw error;
  }
}

export async function preparePinnedSQLiteHeaders(
  runtimeRoot: string,
  pins: SQLiteHeaderPins,
  platform: NodeJS.Platform,
  arch: NodeJS.Architecture,
): Promise<string> {
  const headerDir = join(runtimeRoot, "electron-headers");
  const archivePath = join(runtimeRoot, "electron-headers.tgz");
  const library =
    platform === "win32" ? pins.windowsLibraries[arch] : undefined;
  if (platform === "win32" && !library)
    throw new Error(
      `Unsupported pinned SQLite Windows import library: ${arch}.`,
    );
  try {
    await downloadPinnedSQLiteArtifact(pins.archive, archivePath, [
      "artifacts.electronjs.org",
    ]);
    await extractSQLiteArchive(
      archivePath,
      headerDir,
      "headers",
      pins.archive.integrity,
      SQLITE_ARCHIVE_LIMITS,
      pins.archiveRoot,
    );
    const versionHeader = readFileSync(
      join(headerDir, "include/node/node_version.h"),
      "utf8",
    );
    if (
      !new RegExp(`#define NODE_MODULE_VERSION\\s+${pins.abi}(?:\\s|$)`).test(
        versionHeader,
      )
    )
      throw new Error(
        "Pinned Electron headers do not match the expected SQLite ABI.",
      );
    if (library) {
      const releaseDir = join(headerDir, "Release");
      mkdirSync(releaseDir);
      await downloadPinnedSQLiteArtifact(
        library,
        join(releaseDir, "node.lib"),
        ["artifacts.electronjs.org"],
      );
    }
    return headerDir;
  } catch (error) {
    rmSync(headerDir, { recursive: true, force: true });
    throw error;
  } finally {
    rmSync(archivePath, { force: true });
  }
}

export function sqliteNodeGypArguments(
  toolDir: string,
  headerDir: string,
  headerVersion: string,
  platform: NodeJS.Platform,
  arch: NodeJS.Architecture,
  python?: string,
): string[] {
  assertSQLiteBuildPath(toolDir, "tools", platform);
  assertSQLiteBuildPath(headerDir, "headers", platform);
  return [
    join(toolDir, "node_modules/node-gyp/bin/node-gyp.js"),
    "rebuild",
    `--target=${headerVersion}`,
    `--arch=${arch}`,
    `--target_platform=${platform}`,
    `--nodedir=${headerDir}`,
    `--devdir=${join(toolDir, "node-gyp-devdir")}`,
    ...(python ? [`--python=${python}`] : []),
    "--release",
  ];
}

function applyElectron42V8Patch(sourceDir: string): void {
  const macrosPath = join(sourceDir, "src", "util", "macros.cpp");
  let macros = readFileSync(macrosPath, "utf8");
  macros = macros.replace(
    "#define OnlyAddon static_cast<Addon*>(info.Data().As<v8::External>()->Value())",
    [
      "#if defined(NODE_MODULE_VERSION) && NODE_MODULE_VERSION >= 146",
      "#define EXTERNAL_NEW(isolate, value) v8::External::New((isolate), (value), 0)",
      "#define EXTERNAL_VALUE(value) (value)->Value(0)",
      "#else",
      "#define EXTERNAL_NEW(isolate, value) v8::External::New((isolate), (value))",
      "#define EXTERNAL_VALUE(value) (value)->Value()",
      "#endif",
      "#define OnlyAddon static_cast<Addon*>(EXTERNAL_VALUE(info.Data().As<v8::External>()))",
    ].join("\n"),
  );
  writeFileSync(macrosPath, macros, "utf8");

  const mainPath = join(sourceDir, "src", "better_sqlite3.cpp");
  let mainCpp = readFileSync(mainPath, "utf8");
  mainCpp = mainCpp.replace(
    "v8::Local<v8::External> data = v8::External::New(isolate, addon);",
    "v8::Local<v8::External> data = EXTERNAL_NEW(isolate, addon);",
  );
  if (!mainCpp.includes("__builtin_frame_address")) {
    mainCpp =
      "#ifdef _MSC_VER\n" +
      "#define __builtin_frame_address(x) ((void*)0)\n" +
      "#endif\n" +
      mainCpp;
  }
  writeFileSync(mainPath, mainCpp, "utf8");

  const helpersPath = join(sourceDir, "src", "util", "helpers.cpp");
  let helpers = readFileSync(helpersPath, "utf8");
  helpers = helpers.replace(
    "\t\tfunc,\n\t\t0,\n\t\tdata",
    "\t\tfunc,\n\t\tnullptr,\n\t\tdata",
  );
  writeFileSync(helpersPath, helpers, "utf8");
}

async function downloadPatchedPrebuilt(
  runtimeRoot: string,
  baseDir: string,
): Promise<void> {
  const pkg = readBundledBetterSqlite3Package(baseDir);
  if (!pkg.version) {
    throw new Error("Cannot determine better-sqlite3 version.");
  }

  const fileName = sqlitePrebuiltFileName(
    "patched",
    currentArtifactTarget(pkg.version),
  );
  const pin = sqlitePrebuiltPin("patched", fileName);
  const downloadUrl = pin.url;
  const tarballPath = join(runtimeRoot, fileName);

  try {
    installerLog(`Trying patched prebuilt from ${downloadUrl}…`);
    await downloadPinnedSQLiteArtifact(pin, tarballPath, GITHUB_DOWNLOAD_HOSTS);

    const scaffoldDir = join(runtimeRoot, "node_modules", "better-sqlite3");
    await extractSQLiteArchive(
      tarballPath,
      scaffoldDir,
      "prebuilt",
      pin.integrity,
    );

    const binaryPath = join(
      scaffoldDir,
      "build",
      "Release",
      "better_sqlite3.node",
    );
    if (!existsSync(binaryPath)) {
      throw new Error(
        "Patched prebuilt tarball did not contain build/Release/better_sqlite3.node.",
      );
    }

    installerLog("Patched prebuilt installed successfully.");
  } finally {
    rmSync(tarballPath, { force: true });
  }
}

function resolveTrustedSQLiteBuildTools(baseDir: string, buildRoot: string) {
  const buildTools = resolveSQLiteBuildTools(
    findPackageRoot(baseDir) ?? baseDir,
    buildRoot,
    process.env,
    installerConfiguration?.workspaceRoots?.() ?? [],
  );
  const python = buildTools.python;
  if (!python) {
    const candidates =
      process.platform === "win32" ? "py.exe/python.exe" : "python3/python";
    throw new Error(
      `A trusted Python 3 installation is required for the verified SQLite source build. No ${candidates} entry on the sanitized PATH resolved outside the extension package, runtime and all workspace roots. Add the actual Python installation directory to the extension host PATH; workspace-controlled interpreters are never executed.`,
    );
  }
  return { ...buildTools, python };
}

async function rebuildFromSourceWithPatch(
  runtimeRoot: string,
  baseDir: string,
): Promise<void> {
  assertSQLiteBuildPath(runtimeRoot, "runtime");
  const pkg = readBundledBetterSqlite3Package(baseDir);
  if (!pkg.version) {
    throw new Error(
      "Cannot determine better-sqlite3 version for source build.",
    );
  }

  const headerPins = sqliteHeaderPins(
    process.versions.electron ?? "",
    process.versions.modules,
    process.platform,
    process.arch,
  );
  const buildRoot = createSQLiteBuildRoot();
  const sourceDir = join(buildRoot, "better-sqlite3-build-source");
  const tarballPath = join(buildRoot, "better-sqlite3-source.tgz");
  const toolDir = join(buildRoot, "sqlite-build-tools");

  try {
    if (pkg.version !== SQLITE_SOURCE.version) {
      throw new Error(
        `No locked SQLite source integrity for better-sqlite3 ${pkg.version}. Update the source pin from package-lock.json.`,
      );
    }
    resolveTrustedSQLiteBuildTools(baseDir, buildRoot);
    const tarballUrl = SQLITE_SOURCE.url;
    installerLog(`Downloading better-sqlite3 ${pkg.version} source tarball…`);
    await downloadToFile(tarballUrl, tarballPath, SQLITE_DOWNLOAD_TIMEOUT_MS, {
      allowedHosts: ["registry.npmjs.org"],
    });

    mkdirSync(sourceDir, { recursive: true });
    await extractSQLiteArchive(
      tarballPath,
      sourceDir,
      "source",
      SQLITE_SOURCE.integrity,
    );
    const buildTools = resolveTrustedSQLiteBuildTools(baseDir, buildRoot);
    const python = buildTools.python;
    pinSQLiteSourceNodeActions(sourceDir, buildTools.node);
    const toolLock = createSQLiteBuildToolLock();
    for (const directory of [
      buildTools.env.HOME,
      buildTools.env.TMPDIR,
      buildTools.env.APPDATA,
      buildTools.env.LOCALAPPDATA,
    ]) {
      if (directory) mkdirSync(directory, { recursive: true });
    }
    mkdirSync(toolDir);
    writeFileSync(
      join(toolDir, "package.json"),
      JSON.stringify(toolLock.manifest),
    );
    writeFileSync(
      join(toolDir, "package-lock.json"),
      JSON.stringify(toolLock.lock),
    );
    const userConfig = join(toolDir, "empty-user.npmrc");
    const globalConfig = join(toolDir, "empty-global.npmrc");
    writeFileSync(userConfig, "");
    writeFileSync(globalConfig, "");
    installerLog(
      "Installing lockfile-verified SQLite build tools (no install scripts)…",
    );
    await execFileAsync(
      buildTools.node,
      [
        buildTools.npmCli,
        "ci",
        "--ignore-scripts",
        "--no-audit",
        "--no-fund",
        "--registry=https://registry.npmjs.org",
        `--cache=${join(toolDir, "cache")}`,
        `--userconfig=${userConfig}`,
        `--globalconfig=${globalConfig}`,
      ],
      {
        cwd: toolDir,
        timeout: SQLITE_BUILD_STEP_TIMEOUT_MS,
        env: buildTools.env,
      },
    );

    installerLog("Applying Electron 42 V8 API compatibility patch…");
    applyElectron42V8Patch(sourceDir);
    const headerDir = await preparePinnedSQLiteHeaders(
      buildRoot,
      headerPins,
      process.platform,
      process.arch,
    );

    const electronVersion = process.versions.electron;
    installerLog(
      `Building better-sqlite3 from source for Electron ${electronVersion} (ABI ${process.versions.modules}, ${process.platform}-${process.arch})…`,
    );
    await execFileAsync(
      buildTools.node,
      sqliteNodeGypArguments(
        toolDir,
        headerDir,
        headerPins.version,
        process.platform,
        process.arch,
        python,
      ),
      {
        cwd: sourceDir,
        timeout: SQLITE_BUILD_STEP_TIMEOUT_MS,
        env: { ...buildTools.env, NODE_GYP_FORCE_PYTHON: python },
      },
    );

    const builtBinary = join(
      sourceDir,
      "build",
      "Release",
      "better_sqlite3.node",
    );
    if (!existsSync(builtBinary)) {
      throw new Error("Source build did not produce better_sqlite3.node.");
    }

    const targetDir = join(
      runtimeRoot,
      "node_modules",
      "better-sqlite3",
      "build",
      "Release",
    );
    mkdirSync(targetDir, { recursive: true });
    cpSync(builtBinary, join(targetDir, "better_sqlite3.node"));

    installerLog(
      "Successfully built better-sqlite3 from source with Electron 42 V8 patch.",
    );
  } finally {
    rmSync(buildRoot, { recursive: true, force: true });
  }
}

function createSQLiteBuildRoot(): string {
  const candidates = [tmpdir()];
  if (process.platform === "win32") {
    const systemRoot = process.env.SystemRoot ?? process.env.WINDIR;
    if (systemRoot) candidates.push(join(systemRoot, "Temp"));
  } else {
    candidates.push("/tmp");
  }

  let lastError: unknown;
  for (const directory of [...new Set(candidates)]) {
    const prefix = join(directory, "rapidb-sqlite-build-");
    let created: string | undefined;
    try {
      if (/\s/.test(prefix)) {
        throw new Error("SQLite temporary build root contains whitespace.");
      }
      assertSQLiteBuildPath(prefix, "temporary build root");
      created = mkdtempSync(prefix);
      if (process.platform !== "win32") chmodSync(created, 0o700);
      assertSQLiteBuildPath(created, "temporary build root");
      return created;
    } catch (error) {
      if (created) rmSync(created, { recursive: true, force: true });
      lastError = error;
    }
  }

  throw new Error(
    `Could not create a private SQLite build directory without spaces. ${errorMessage(lastError ?? "No temporary directory is available.")}`,
  );
}

async function downloadPrebuiltBinary(
  runtimeRoot: string,
  baseDir: string,
): Promise<void> {
  const betterSqlite3PackageRoot = join(
    runtimeRoot,
    "node_modules",
    "better-sqlite3",
  );
  const pkg = JSON.parse(
    readFileSync(join(betterSqlite3PackageRoot, "package.json"), "utf8"),
  ) as BetterSqlite3PackageJson;
  if (
    !pkg.version ||
    !/^\d+\.\d+\.\d+(?:-[A-Za-z0-9.-]+)?$/.test(pkg.version)
  ) {
    throw new Error("Invalid better-sqlite3 version for runtime installation.");
  }
  const fileName = sqlitePrebuiltFileName(
    "official",
    currentArtifactTarget(pkg.version),
  );
  const tarballPath = join(runtimeRoot, "official-prebuilt.tar.gz");

  try {
    const pin = sqlitePrebuiltPin("official", fileName);
    const downloadUrl = pin.url;
    installerLog(`Downloading official SQLite prebuilt from ${downloadUrl}…`);
    await downloadPinnedSQLiteArtifact(pin, tarballPath, GITHUB_DOWNLOAD_HOSTS);
    await extractSQLiteArchive(
      tarballPath,
      betterSqlite3PackageRoot,
      "prebuilt",
      pin.integrity,
    );

    const binaryPath = join(
      betterSqlite3PackageRoot,
      "build",
      "Release",
      "better_sqlite3.node",
    );
    if (!existsSync(binaryPath)) {
      throw new Error(
        `Downloaded SQLite runtime did not produce better_sqlite3.node for ${currentTargetLabel()} (${currentRuntime()}, ABI ${process.versions.modules}).`,
      );
    }
  } catch (prebuiltError) {
    if (
      currentRuntime() === "electron" &&
      Number(process.versions.modules) >= 146
    ) {
      let patchedPrebuiltInstalled = false;
      try {
        await downloadPatchedPrebuilt(runtimeRoot, baseDir);
        patchedPrebuiltInstalled = true;
      } catch (patchedError) {
        installerLog(
          `Patched prebuilt not available: ${errorMessage(patchedError)}`,
        );
      }

      if (!patchedPrebuiltInstalled) {
        installerLog("Attempting source build with Electron 42 V8 patch…");
        try {
          await rebuildFromSourceWithPatch(runtimeRoot, baseDir);
        } catch (sourceError) {
          installerLog(`Source build failed: ${errorMessage(sourceError)}`);
          throw new Error(
            `SQLite runtime installation failed. Official prebuilt: ${errorMessage(prebuiltError)}. Verified source build: ${errorMessage(sourceError)}. Patched prebuilts require a published upstream release and reviewed static pins.`,
          );
        }
      }
    } else {
      throw prebuiltError;
    }
  } finally {
    rmSync(tarballPath, { force: true });
  }

  writeFileSync(
    join(runtimeRoot, "runtime.json"),
    `${JSON.stringify(expectedManifestFor(baseDir), null, 2)}\n`,
    "utf8",
  );
}

async function installManagedRuntime(baseDir: string): Promise<string | null> {
  const runtimeRoot = runtimeRootFor(baseDir);
  const installedPackagePath = installedPackagePathFor(baseDir);
  const configuration = installerConfiguration;
  if (!runtimeRoot || !installedPackagePath || !configuration) {
    return null;
  }

  mkdirSync(configuration.storageRoot, { recursive: true });
  if (manifestMatches(baseDir)) {
    return installedPackagePath;
  }

  const lockRoot = `${runtimeRoot}.lock`;
  return withInstallLock(
    lockRoot,
    async () => {
      if (manifestMatches(baseDir)) {
        return installedPackagePath;
      }

      removeAbandonedInstallDirectories(runtimeRoot);
      const tempRoot = `${runtimeRoot}.tmp-${process.pid}-${Date.now()}`;
      rmSync(tempRoot, { recursive: true, force: true });
      rmSync(runtimeRoot, { recursive: true, force: true });
      mkdirSync(tempRoot, { recursive: true });

      try {
        copyBundledRuntimeScaffold(baseDir, tempRoot);
        await downloadPrebuiltBinary(tempRoot, baseDir);
        writeFileSync(join(tempRoot, SQLITE_RUNTIME_LAST_USED_FILE), "");
        renameSync(tempRoot, runtimeRoot);
      } catch (error) {
        rmSync(tempRoot, { recursive: true, force: true });
        throw error;
      }

      installerLog(
        `SQLite runtime ready at ${runtimeRoot} for ${currentTargetLabel()} (${currentRuntime()}, ABI ${process.versions.modules}).`,
      );
      return installedPackagePath;
    },
    () => (manifestMatches(baseDir) ? installedPackagePath : null),
  );
}

export function configureSQLiteInstaller(
  configuration: SQLiteInstallerConfiguration,
): void {
  installerConfiguration = {
    storageRoot: resolve(configuration.storageRoot),
    log: configuration.log,
    allowInstall: configuration.allowInstall,
    workspaceRoots: configuration.workspaceRoots,
  };
}

export function resetSQLiteInstallerForTests(): void {
  installerConfiguration = null;
  inFlightInstall = null;
  obsoleteRuntimeCleanupDone = false;
  installLockTimings = { ...DEFAULT_SQLITE_INSTALL_LOCK_TIMINGS };
}

export function setSQLiteInstallLockTimingsForTests(
  timings: Partial<typeof DEFAULT_SQLITE_INSTALL_LOCK_TIMINGS>,
): void {
  installLockTimings = { ...installLockTimings, ...timings };
}

export function probeInstalledBetterSqlite3Runtime(
  baseDir: string,
): SQLiteInstalledRuntimeProbe {
  const packageRoot = findPackageRoot(baseDir);
  const bundledPackagePath = bundledPackagePathFor(baseDir);
  const installedPackagePath = installedPackagePathFor(baseDir);
  const installedBinaryPath = installedBinaryPathFor(baseDir);
  return {
    runtime: currentRuntime(),
    target: currentTargetLabel(),
    packageRoot,
    bundledPackagePath,
    bundledPackageExists: bundledPackagePath
      ? existsSync(join(bundledPackagePath, "package.json"))
      : false,
    installedPackagePath,
    installedPackageExists: installedPackagePath
      ? existsSync(join(installedPackagePath, "package.json"))
      : false,
    installedBinaryPath,
    installedBinaryExists: installedBinaryPath
      ? existsSync(installedBinaryPath)
      : false,
  };
}

export function resolveInstalledBetterSqlite3PackagePath(
  baseDir: string,
): string | null {
  const probe = probeInstalledBetterSqlite3Runtime(baseDir);
  return probe.installedPackageExists && probe.installedBinaryExists
    ? probe.installedPackagePath
    : null;
}

export async function ensureSQLiteRuntimeInstalled(
  baseDir: string,
): Promise<string | null> {
  const installedPackagePath =
    resolveInstalledBetterSqlite3PackagePath(baseDir);
  if (installedPackagePath && refreshInstalledRuntimeUse(baseDir)) {
    await removeObsoleteSQLiteRuntimesOnce(baseDir);
    return installedPackagePath;
  }
  if (!installerConfiguration) {
    return null;
  }
  if (!inFlightInstall) {
    const allowInstall = installerConfiguration.allowInstall;
    inFlightInstall = (async () => {
      let installed = await recordInstalledRuntimeUse(baseDir);
      if (!installed) {
        if (allowInstall && !(await allowInstall())) {
          throw new Error(
            "SQLite runtime installation was not authorized. The database was not opened.",
          );
        }
        installed = await installManagedRuntime(baseDir);
      }
      if (installed) {
        await removeObsoleteSQLiteRuntimesOnce(baseDir);
      }
      return installed;
    })().finally(() => {
      inFlightInstall = null;
    });
  }
  return inFlightInstall;
}
