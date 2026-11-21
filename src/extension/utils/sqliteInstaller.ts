import { randomUUID } from "node:crypto";
import {
  existsSync,
  lstatSync,
  mkdirSync,
  readdirSync,
  readFileSync,
  renameSync,
  rmdirSync,
  rmSync,
  statSync,
  unlinkSync,
  utimesSync,
  writeFileSync,
} from "node:fs";
import { hostname } from "node:os";
import { basename, dirname, join, resolve } from "node:path";
import { logger } from "./logger";
import {
  extractSQLitePackage,
  SQLITE_ARCHIVE_LIMITS,
  sqliteSha256Matches,
  verifySQLiteIntegrity,
} from "./sqliteArchive";
import {
  reviewedSQLiteVersion,
  SQLITE_ARTIFACTS,
  SQLITE_INSTALLED_BINARY_PATH,
  SQLITE_REGISTRY_HOST,
  type SQLitePackagePlan,
  type SQLiteTarballPin,
  sqliteArtifactPolicyId,
  sqlitePackagePlan,
} from "./sqliteArtifactPins";
import {
  currentSQLiteHostProbe,
  resolveSQLiteNativeHost,
  type SQLiteHostProbe,
  type SQLiteNativeHost,
  type SQLiteNativeTarget,
  sqliteHostLibraryRequirementError,
} from "./sqliteNativeTarget";
import { oldestVSCodeWithNodeApi } from "./vscodeNodeApi";

const SQLITE_DOWNLOAD_IDLE_TIMEOUT_MS = 60_000;
const SQLITE_TARBALL_DOWNLOAD_TIMEOUT_MS = 10 * 60_000;
const SQLITE_INSTALL_MAX_DURATION_MS =
  SQLITE_TARBALL_DOWNLOAD_TIMEOUT_MS + 120_000;
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
const SQLITE_RUNTIME_MANIFEST_FILE = "runtime.json";
const SQLITE_RUNTIME_MANIFEST_SCHEMA = 3;
const SQLITE_OBSOLETE_RUNTIME_MAX_IDLE_MS = 30 * 24 * 60 * 60 * 1000;
// Runtimes used this recently cannot be cleanup candidates, so their use can
// be recorded without the runtime lock.
const SQLITE_RUNTIME_RECENT_USE_MS = 7 * 24 * 60 * 60 * 1000;
const SQLITE_RUNTIME_USE_CLOCK_SKEW_MS = 5 * 60 * 1000;
const SQLITE_OBSOLETE_RUNTIME_MAX_REMOVALS = 3;
// Bounds the mtime scan of a runtime written by a RapiDB version that did not
// record use; larger trees are kept.
const SQLITE_UNTRACKED_RUNTIME_MAX_ENTRIES = 10_000;
// Errors that prove a runtime is incomplete or was tampered with. Any other
// I/O error (EACCES, EPERM, EBUSY, EMFILE, EIO, ... — often an antivirus scan
// or a sharing violation) is temporary: the runtime is neither used nor
// replaced, and the connection fails with a retryable error.
const SQLITE_RUNTIME_CORRUPTION_ERRORS = new Set([
  "ENOENT",
  "ENOTDIR",
  "ELOOP",
  "EISDIR",
]);
const SQLITE_RUNTIME_VERSION_PATTERN = /^\d+\.\d+\.\d+(?:-[A-Za-z0-9.-]+)?$/;
// better-sqlite3 <= 12 runtimes were keyed by the host's Electron/Node ABI.
// RapiDB never loads them again; they are removed once idle.
const SQLITE_LEGACY_RUNTIME_NAME =
  "(?:electron|node)-abi-[0-9]+(?:-[a-z0-9]+){2,3}(?:-archive-v1|-pins-v[0-9]+-[0-9a-f]{16})?";
const SQLITE_NAPI_RUNTIME_NAME =
  "(?:darwin|linux|linuxmusl|win32)-(?:x64|arm64)-pins-v[0-9]+-[0-9a-f]{16}";
const SQLITE_RUNTIME_NAME = `(?:${SQLITE_LEGACY_RUNTIME_NAME}|${SQLITE_NAPI_RUNTIME_NAME})`;
const SQLITE_RUNTIME_DIRECTORY_PATTERN = new RegExp(`^${SQLITE_RUNTIME_NAME}$`);
const SQLITE_RUNTIME_TRASH_PATTERN = new RegExp(
  `^${SQLITE_RUNTIME_NAME}\\.obsolete-[0-9]+-[0-9a-f-]{36}$`,
);

interface SQLiteInstallLockOwner {
  pid: number;
  hostname: string;
  createdAt: number;
  token: string;
}

export interface SQLiteInstalledRuntimeProbe {
  runtime: "electron" | "node";
  target: string;
  nodeApi: string;
  installedPackagePath: string | null;
  installedPackageExists: boolean;
  installedBinaryPath: string | null;
  installedBinaryExists: boolean;
}

export interface SQLiteInstallProgress {
  // Aborted only by an explicit user cancellation of the installation.
  signal: AbortSignal;
  report(receivedBytes: number, totalBytes: number): void;
}

interface SQLiteInstallerConfiguration {
  storageRoot: string;
  log?: (message: string) => void;
  allowInstall?: () => boolean | Promise<boolean>;
  // False in an untrusted (Restricted Mode) workspace.
  isTrusted?: () => boolean;
  withInstallProgress?: <T>(
    task: (progress: SQLiteInstallProgress) => Promise<T>,
  ) => Promise<T>;
}

export class SQLiteRuntimeUnreadableError extends Error {
  constructor(path: string, cause: unknown) {
    const code = (cause as NodeJS.ErrnoException)?.code;
    super(
      `The SQLite runtime at ${path} could not be read right now (${code ?? errorMessage(cause)}); it was neither used nor replaced. This is usually temporary, for example while an antivirus scan or another program holds the file. Try again in a moment.`,
      { cause },
    );
    this.name = "SQLiteRuntimeUnreadableError";
  }
}

export class SQLiteRuntimeInUseError extends Error {
  constructor(path: string, cause: unknown) {
    super(
      `The SQLite runtime at ${path} failed verification and must be replaced, but it is in use (${(cause as NodeJS.ErrnoException)?.code ?? errorMessage(cause)}). Close SQLite connections in other VS Code windows (or close those windows), then try again. Nothing was deleted or downloaded.`,
      { cause },
    );
    this.name = "SQLiteRuntimeInUseError";
  }
}

export class SQLiteRuntimeInstallCancelledError extends Error {
  constructor() {
    super(
      "The SQLite runtime download was cancelled. Nothing was installed; open the SQLite database again to retry.",
    );
    this.name = "SQLiteRuntimeInstallCancelledError";
  }
}

interface InstalledRuntimeManifest {
  schemaVersion: number;
  betterSqlite3Version: string;
  target: SQLiteNativeTarget;
  policy: string;
  nodeApiVersion: number;
}

interface RuntimeLayout {
  plan: SQLitePackagePlan;
  policy: string;
  runtimeRoot: string;
  packagePath: string;
  manifest: InstalledRuntimeManifest;
}

let installerConfiguration: SQLiteInstallerConfiguration | null = null;
let inFlightInstall: Promise<string | null> | null = null;
let obsoleteRuntimeCleanupDone = false;
let hostProbeOverride: SQLiteHostProbe | null = null;
let nodeApiVersionOverride: string | undefined;
let cachedHost: { host: SQLiteNativeHost } | { error: Error } | null = null;

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

function hostNodeApiVersion(): string {
  return nodeApiVersionOverride ?? process.versions.napi ?? "";
}

function currentHost(): SQLiteNativeHost {
  if (!cachedHost) {
    try {
      cachedHost = {
        host: resolveSQLiteNativeHost(
          hostProbeOverride ?? currentSQLiteHostProbe(),
        ),
      };
    } catch (error) {
      cachedHost = {
        error: error instanceof Error ? error : new Error(String(error)),
      };
    }
  }
  if ("error" in cachedHost) throw cachedHost.error;
  return cachedHost.host;
}

function currentTarget(): SQLiteNativeTarget {
  return currentHost().target;
}

function assertHostNodeApi(plan: SQLitePackagePlan): void {
  const available = Number(hostNodeApiVersion());
  if (!Number.isSafeInteger(available) || available < plan.nodeApiVersion) {
    const vscode = oldestVSCodeWithNodeApi(plan.nodeApiVersion);
    throw new Error(
      `SQLite requires Node-API ${plan.nodeApiVersion} (better-sqlite3 ${plan.version}), but this ${currentRuntime() === "electron" ? "VS Code" : "Node.js"} runtime (Node.js ${process.version}) provides Node-API ${hostNodeApiVersion() || "unknown"}. ${vscode ? `Update VS Code to ${vscode} or later.` : "Update VS Code."} No SQLite binary was downloaded.`,
    );
  }
}

// glibc prebuilds need a minimum glibc/libstdc++; refuse older hosts before
// asking for consent or downloading anything.
function assertHostLibraries(plan: SQLitePackagePlan): void {
  const error = sqliteHostLibraryRequirementError(
    currentHost(),
    plan.libraryRequirements,
  );
  if (error) throw error;
}

// Throws for unsupported hosts; returns null only when the installer has not
// been configured (development and tests without global storage).
function currentLayout(): RuntimeLayout | null {
  if (!installerConfiguration) return null;
  const target = currentTarget();
  const plan = sqlitePackagePlan(target);
  const version = reviewedSQLiteVersion();
  if (plan.version !== version) {
    throw new Error(
      "The SQLite artifact manifest changed after it was reviewed; refusing to install.",
    );
  }
  assertHostNodeApi(plan);
  assertHostLibraries(plan);
  const policy = sqliteArtifactPolicyId(target);
  const runtimeRoot = join(
    installerConfiguration.storageRoot,
    "sqlite-runtime",
    "better-sqlite3",
    version,
    `${target}-${policy}`,
  );
  return {
    plan,
    policy,
    runtimeRoot,
    packagePath: join(runtimeRoot, "node_modules", "better-sqlite3"),
    manifest: {
      schemaVersion: SQLITE_RUNTIME_MANIFEST_SCHEMA,
      betterSqlite3Version: version,
      target,
      policy,
      nodeApiVersion: plan.nodeApiVersion,
    },
  };
}

function tryCurrentLayout(): RuntimeLayout | null {
  try {
    return currentLayout();
  } catch {
    return null;
  }
}

export function sqliteNativeBindingPath(packagePath: string): string {
  return join(packagePath, ...SQLITE_INSTALLED_BINARY_PATH.split("/"));
}

function isRealDirectory(path: string): boolean {
  try {
    return lstatSync(path).isDirectory();
  } catch {
    return false;
  }
}

class RuntimeInvalid extends Error {}

// A corrupted or incomplete runtime is reported as invalid; any other I/O
// error is temporary and propagates as SQLiteRuntimeUnreadableError.
function runtimeIoFailure(error: unknown, path: string): Error {
  const code = (error as NodeJS.ErrnoException)?.code;
  return code && SQLITE_RUNTIME_CORRUPTION_ERRORS.has(code)
    ? new RuntimeInvalid(`${path}: ${code}`)
    : new SQLiteRuntimeUnreadableError(path, error);
}

function lstatRuntimePath(path: string): import("node:fs").Stats {
  try {
    return lstatSync(path);
  } catch (error) {
    throw runtimeIoFailure(error, path);
  }
}

// Every directory below the configured storage root must be a real directory;
// the root itself is configured by VS Code and may be reached through links.
function assertRealDirectoryChain(from: string, to: string): void {
  let directory = to;
  while (directory !== from) {
    if (!lstatRuntimePath(directory).isDirectory())
      throw new RuntimeInvalid(`${directory} is not a real directory`);
    const parent = dirname(directory);
    if (parent === directory)
      throw new RuntimeInvalid(`${to} is outside ${from}`);
    directory = parent;
  }
  let root: import("node:fs").Stats;
  try {
    root = statSync(from);
  } catch (error) {
    throw runtimeIoFailure(error, from);
  }
  if (!root.isDirectory())
    throw new RuntimeInvalid(`${from} is not a directory`);
}

function readRuntimeFile(path: string): Buffer {
  try {
    return readFileSync(path);
  } catch (error) {
    throw runtimeIoFailure(error, path);
  }
}

type RuntimeEntryKind = "file" | "directory";

// The complete set of entries a runtime may contain. Installation locks,
// staging directories and tombstones are siblings of the runtime directory,
// never inside it.
function expectedRuntimeTree(
  layout: RuntimeLayout,
): Map<string, { kind: RuntimeEntryKind; required: boolean }> {
  const expected = new Map<
    string,
    { kind: RuntimeEntryKind; required: boolean }
  >([
    [SQLITE_RUNTIME_MANIFEST_FILE, { kind: "file", required: true }],
    [SQLITE_RUNTIME_LAST_USED_FILE, { kind: "file", required: false }],
    ["node_modules", { kind: "directory", required: true }],
    ["node_modules/better-sqlite3", { kind: "directory", required: true }],
  ]);
  for (const file of layout.plan.files) {
    const parts = `node_modules/better-sqlite3/${file.installPath}`.split("/");
    for (let depth = 3; depth < parts.length; depth += 1) {
      expected.set(parts.slice(0, depth).join("/"), {
        kind: "directory",
        required: true,
      });
    }
    expected.set(parts.join("/"), { kind: "file", required: true });
  }
  return expected;
}

// Names are compared exactly as readdir returns them, so a case variant
// (lib/Util next to lib/util.js on a case-insensitive file system) or an
// extensionless file that require() would resolve is never accepted.
function assertExactRuntimeTree(
  layout: RuntimeLayout,
  runtimeRoot: string,
): void {
  const expected = expectedRuntimeTree(layout);
  const seen = new Set<string>();
  const visit = (relative: string): void => {
    const directory = relative
      ? join(runtimeRoot, ...relative.split("/"))
      : runtimeRoot;
    let names: string[];
    try {
      names = readdirSync(directory);
    } catch (error) {
      throw runtimeIoFailure(error, directory);
    }
    for (const name of names) {
      const child = relative ? `${relative}/${name}` : name;
      const entry = expected.get(child);
      if (!entry) throw new RuntimeInvalid(`unexpected runtime entry ${child}`);
      const stats = lstatRuntimePath(join(directory, name));
      const kind = stats.isFile()
        ? "file"
        : stats.isDirectory()
          ? "directory"
          : null;
      if (kind !== entry.kind)
        throw new RuntimeInvalid(
          `runtime entry ${child} is not a ${entry.kind}`,
        );
      seen.add(child);
      if (kind === "directory") visit(child);
    }
  };
  visit("");
  for (const [path, entry] of expected) {
    if (entry.required && !seen.has(path))
      throw new RuntimeInvalid(`missing runtime entry ${path}`);
  }
}

function assertRuntimeManifest(
  layout: RuntimeLayout,
  runtimeRoot: string,
): void {
  let actual: Partial<InstalledRuntimeManifest> | null;
  try {
    actual = JSON.parse(
      readRuntimeFile(join(runtimeRoot, SQLITE_RUNTIME_MANIFEST_FILE)).toString(
        "utf8",
      ),
    );
  } catch (error) {
    if (error instanceof SQLiteRuntimeUnreadableError) throw error;
    throw new RuntimeInvalid("unreadable runtime manifest");
  }
  const expected = layout.manifest;
  if (
    !actual ||
    typeof actual !== "object" ||
    actual.schemaVersion !== expected.schemaVersion ||
    actual.betterSqlite3Version !== expected.betterSqlite3Version ||
    actual.target !== expected.target ||
    actual.policy !== expected.policy ||
    actual.nodeApiVersion !== expected.nodeApiVersion
  ) {
    throw new RuntimeInvalid("runtime manifest does not match");
  }
}

// A runtime is used only if its manifest matches, it contains exactly the
// expected entries and every installed file, including the native binary,
// still matches its reviewed SHA-256 pin. A corrupted or modified cache is
// never loaded; it is reinstalled after consent instead. Returns false for an
// invalid runtime and throws SQLiteRuntimeUnreadableError for a temporary I/O
// error.
function verifyRuntimeAt(
  layout: RuntimeLayout,
  runtimeRoot: string,
  storageRoot: string,
): boolean {
  try {
    assertRealDirectoryChain(storageRoot, runtimeRoot);
    assertExactRuntimeTree(layout, runtimeRoot);
    assertRuntimeManifest(layout, runtimeRoot);
    const packagePath = join(runtimeRoot, "node_modules", "better-sqlite3");
    for (const file of layout.plan.files) {
      const path = join(packagePath, ...file.installPath.split("/"));
      const data = readRuntimeFile(path);
      if (data.length !== file.size || !sqliteSha256Matches(data, file.sha256))
        throw new RuntimeInvalid(`${file.installPath} does not match its pin`);
    }
    return true;
  } catch (error) {
    if (error instanceof RuntimeInvalid) return false;
    throw error;
  }
}

function installedRuntimeValid(layout: RuntimeLayout): boolean {
  const configuration = installerConfiguration;
  return (
    configuration !== null &&
    verifyRuntimeAt(layout, layout.runtimeRoot, configuration.storageRoot)
  );
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
  const trashPrefix = `${basename(runtimeRoot)}.obsolete-`;
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
    } else if (
      entry.startsWith(trashPrefix) &&
      SQLITE_RUNTIME_TRASH_PATTERN.test(entry)
    ) {
      // Left behind when deleting a replaced runtime failed (for example a
      // binary still loaded on Windows); this window holds the runtime lock.
      removeRuntimeTrash(path);
      continue;
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

// Removes a runtime that was already renamed to an installer-owned trash name.
// A symlink is unlinked, never followed; rmSync does not follow links inside.
function removeRuntimeTrash(path: string): boolean {
  try {
    if (lstatSync(path).isSymbolicLink()) unlinkSync(path);
    else rmSync(path, { recursive: true, force: true });
    return true;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return true;
    installerLog(
      `Could not delete replaced SQLite runtime ${path}: ${errorMessage(error)}. It will be removed later.`,
    );
    return false;
  }
}

// Moves a runtime that failed verification out of the way with one atomic
// rename under the runtime lock, then deletes it best effort. If it cannot be
// moved (on Windows, typically because another window still has its binary
// loaded) nothing is deleted.
function moveInvalidRuntimeAside(runtimeRoot: string): void {
  const trash = `${runtimeRoot}.obsolete-${process.pid}-${randomUUID()}`;
  try {
    renameSync(runtimeRoot, trash);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return;
    throw new SQLiteRuntimeInUseError(runtimeRoot, error);
  }
  installerLog(
    `Replacing SQLite runtime ${runtimeRoot}; it failed verification.`,
  );
  removeRuntimeTrash(trash);
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

function runtimeLastUsedAt(runtimeRoot: string): number | null | undefined {
  try {
    if (!lstatSync(join(runtimeRoot, SQLITE_RUNTIME_MANIFEST_FILE)).isFile()) {
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

// Newest mtime anywhere in a runtime tree (symlinks are not followed), or
// null if the tree cannot be read completely or is unexpectedly large.
function newestTreeModification(root: string): number | null {
  let newest = Number.NEGATIVE_INFINITY;
  let entries = 0;
  const visit = (path: string): boolean => {
    if (++entries > SQLITE_UNTRACKED_RUNTIME_MAX_ENTRIES) return false;
    let stats: import("node:fs").Stats;
    try {
      stats = lstatSync(path);
    } catch {
      return false;
    }
    newest = Math.max(newest, stats.mtimeMs);
    if (!stats.isDirectory()) return true;
    let names: string[];
    try {
      names = readdirSync(path);
    } catch {
      return false;
    }
    return names.every((name) => visit(join(path, name)));
  };
  return visit(root) ? newest : null;
}

// RapiDB versions that did not record use leave no marker. Such a runtime may
// still be loaded by an old RapiDB in another window, so it is removed only
// once nothing in it has changed for the idle period; no marker is created.
function isIdle(runtimeRoot: string, lastUsedAt: number | null): boolean {
  if (lastUsedAt !== null) return isIdleSince(lastUsedAt);
  const modified = newestTreeModification(runtimeRoot);
  return modified !== null && isIdleSince(modified);
}

async function removeObsoleteRuntime(runtimeRoot: string): Promise<boolean> {
  const observed = runtimeLastUsedAt(runtimeRoot);
  if (observed === undefined || !isIdle(runtimeRoot, observed)) {
    return false;
  }
  const lockRoot = `${runtimeRoot}.lock`;
  const token = randomUUID();
  if (!tryCreateInstallLock(lockRoot, token)) {
    return false;
  }
  const trash = `${runtimeRoot}.obsolete-${process.pid}-${randomUUID()}`;
  let untracked = false;
  try {
    const lastUsedAt = runtimeLastUsedAt(runtimeRoot);
    if (lastUsedAt === undefined || !isIdle(runtimeRoot, lastUsedAt)) {
      return false;
    }
    untracked = lastUsedAt === null;
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
    untracked
      ? `Removing SQLite runtime ${runtimeRoot}; it records no use and has not been modified for over 30 days.`
      : `Removing SQLite runtime ${runtimeRoot}; it has not been used for over 30 days.`,
  );
  removeRuntimeTrash(trash);
  return true;
}

// Removes idle runtimes of other versions, targets or pin policies, including
// ABI-keyed better-sqlite3 <= 12 runtimes. Only real directories with an
// installer-created name inside the cache are touched, symlinks are never
// followed, and a runtime is removed only under its own installation lock
// once it has not been used for 30 days. Runtimes without a use marker
// (written by RapiDB versions that did not record use) are removed only once
// nothing in them has been modified for 30 days. Removal is an atomic rename
// to an installer-owned trash name followed by a best-effort delete; leftover
// trash is retried on later runs.
export async function removeObsoleteSQLiteRuntimes(): Promise<string[]> {
  const removed: string[] = [];
  try {
    const layout = currentLayout();
    if (!layout) {
      return removed;
    }
    const currentRoot = layout.runtimeRoot;
    const currentVersion = basename(dirname(currentRoot));
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
        const trash = SQLITE_RUNTIME_TRASH_PATTERN.test(entry);
        if (path === currentRoot || (!trash && !isRealDirectory(path))) {
          continue;
        }
        try {
          if (abandoned.has(entry)) {
            rmSync(path, { recursive: true, force: true });
            removed.push(path);
          } else if (
            trash &&
            (await isRuntimeUnlocked(join(versionRoot, entry.split(".")[0])))
          ) {
            if (removeRuntimeTrash(path)) removed.push(path);
          } else if (
            SQLITE_RUNTIME_DIRECTORY_PATTERN.test(entry) &&
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
      if (version !== currentVersion) {
        removeEmptyVersionDirectory(versionRoot);
      }
    }
  } catch (error) {
    installerLog(
      `Could not clean up obsolete SQLite runtimes: ${errorMessage(error)}.`,
    );
  }
  return removed;
}

function removeEmptyVersionDirectory(versionRoot: string): void {
  try {
    if (
      isRealDirectory(versionRoot) &&
      isOlderThanIdleLimit(versionRoot) &&
      readdirSync(versionRoot).length === 0
    ) {
      // rmdir never removes a non-empty directory, so a concurrent install
      // into this version cannot lose files.
      rmdirSync(versionRoot);
    }
  } catch {}
}

async function removeObsoleteSQLiteRuntimesOnce(): Promise<void> {
  if (!obsoleteRuntimeCleanupDone) {
    obsoleteRuntimeCleanupDone = true;
    await removeObsoleteSQLiteRuntimes();
  }
}

function refreshInstalledRuntimeUse(layout: RuntimeLayout): boolean {
  const { runtimeRoot } = layout;
  return (
    installedRuntimeValid(layout) &&
    wasUsedRecently(runtimeRoot) &&
    refreshRuntimeUse(runtimeRoot) &&
    existsSync(join(runtimeRoot, SQLITE_RUNTIME_MANIFEST_FILE))
  );
}

async function recordInstalledRuntimeUse(
  layout: RuntimeLayout,
): Promise<string | null> {
  const { runtimeRoot, packagePath } = layout;
  if (!isRealDirectory(dirname(runtimeRoot))) {
    return null;
  }
  return withInstallLock<string | null>(
    `${runtimeRoot}.lock`,
    async () => {
      if (!installedRuntimeValid(layout)) {
        return null;
      }
      markRuntimeUsed(runtimeRoot);
      return packagePath;
    },
    () => (refreshInstalledRuntimeUse(layout) ? packagePath : null),
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

interface DownloadLimits {
  maxBytes?: number;
  allowedHosts?: readonly string[];
  idleTimeoutMs?: number;
  onProgress?: (receivedBytes: number) => void;
  signal?: AbortSignal;
}

// Every hop, including redirects, must be plain HTTPS to an allowed host on
// the default port, without credentials.
function isAllowedDownloadUrl(
  target: string,
  allowedHosts: readonly string[],
): boolean {
  try {
    const url = new URL(target);
    return (
      url.protocol === "https:" &&
      allowedHosts.includes(url.hostname) &&
      (url.port === "" || url.port === "443") &&
      url.username === "" &&
      url.password === ""
    );
  } catch {
    return false;
  }
}

export function downloadToFile(
  url: string,
  destPath: string,
  timeoutMs = SQLITE_DOWNLOAD_IDLE_TIMEOUT_MS,
  options: DownloadLimits = {},
): Promise<void> {
  const { createWriteStream } = require("node:fs") as typeof import("node:fs");
  const https = require("node:https") as typeof import("node:https");
  const http = require("node:http") as typeof import("node:http");
  const maxBytes = options.maxBytes ?? SQLITE_ARCHIVE_LIMITS.compressedBytes;
  const idleTimeoutMs = options.idleTimeoutMs ?? timeoutMs;
  const allowLoopbackHttp = url.startsWith("http://") && !options.allowedHosts;

  return new Promise<void>((resolvePromise, rejectPromise) => {
    let settled = false;
    let abortActiveRequest: (() => void) | undefined;
    let downloadFile: import("node:fs").WriteStream | undefined;
    const onAbort = (): void => {
      rejectOnce(new SQLiteRuntimeInstallCancelledError());
    };
    const resolveOnce = (): void => {
      if (!settled) {
        settled = true;
        clearTimeout(deadline);
        options.signal?.removeEventListener("abort", onAbort);
        resolvePromise();
      }
    };
    const rejectOnce = (error: Error): void => {
      if (!settled) {
        settled = true;
        clearTimeout(deadline);
        options.signal?.removeEventListener("abort", onAbort);
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
        !isAllowedDownloadUrl(resolvedTarget, options.allowedHosts)
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
          const file = createWriteStream(destPath, {
            flags: "wx",
            mode: 0o600,
          });
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
              return;
            }
            try {
              options.onProgress?.(receivedBytes);
            } catch {}
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
      request.setTimeout(idleTimeoutMs, () => {
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
    if (options.signal?.aborted) {
      onAbort();
      return;
    }
    options.signal?.addEventListener("abort", onAbort, { once: true });
    follow(url, 0);
  });
}

// Decimal megabytes (10^6 bytes), as in the documentation.
export function formatSQLiteDownloadSize(bytes: number): string {
  return `${(bytes / 1_000_000).toFixed(1)} MB`;
}

// Download the integrity-pinned npm tarball. Only HTTPS to the npm registry is
// allowed, including for every redirect hop; the size must match the pin
// exactly and the SHA-512 is checked before the archive is opened.
export async function downloadPinnedSQLiteTarball(
  pin: SQLiteTarballPin,
  destination: string,
  progress?: SQLiteInstallProgress,
): Promise<void> {
  if (
    !pin.integrity ||
    !Number.isSafeInteger(pin.size) ||
    pin.size <= 0 ||
    pin.size > SQLITE_ARCHIVE_LIMITS.compressedBytes
  )
    throw new Error("Missing/invalid static SQLite artifact pin.");
  let reported = 0;
  await downloadToFile(
    pin.url,
    destination,
    SQLITE_TARBALL_DOWNLOAD_TIMEOUT_MS,
    {
      maxBytes: pin.size,
      allowedHosts: [SQLITE_REGISTRY_HOST],
      idleTimeoutMs: SQLITE_DOWNLOAD_IDLE_TIMEOUT_MS,
      signal: progress?.signal,
      onProgress: (received) => {
        try {
          progress?.report(received, pin.size);
        } catch {}
        const quarter = Math.floor((received / pin.size) * 4);
        if (quarter > reported && quarter < 4) {
          reported = quarter;
          installerLog(
            `Downloaded ${formatSQLiteDownloadSize(received)} of ${formatSQLiteDownloadSize(pin.size)}…`,
          );
        }
      },
    },
  );
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

async function stageRuntime(
  layout: RuntimeLayout,
  stagingRoot: string,
  progress?: SQLiteInstallProgress,
): Promise<void> {
  const { plan } = layout;
  const tarballPath = join(stagingRoot, "better-sqlite3.tgz");
  try {
    installerLog(
      `Downloading better-sqlite3 ${plan.version} (${formatSQLiteDownloadSize(plan.tarball.size)}) from ${plan.tarball.url}…`,
    );
    await downloadPinnedSQLiteTarball(plan.tarball, tarballPath, progress);
    if (progress?.signal.aborted)
      throw new SQLiteRuntimeInstallCancelledError();
    mkdirSync(join(stagingRoot, "node_modules"));
    await extractSQLitePackage({
      archivePath: tarballPath,
      destination: join(stagingRoot, "node_modules", "better-sqlite3"),
      integrity: plan.tarball.integrity,
      files: plan.files,
    });
  } finally {
    rmSync(tarballPath, { force: true });
  }
  writeFileSync(
    join(stagingRoot, SQLITE_RUNTIME_MANIFEST_FILE),
    `${JSON.stringify(layout.manifest, null, 2)}\n`,
    "utf8",
  );
  writeFileSync(join(stagingRoot, SQLITE_RUNTIME_LAST_USED_FILE), "");
  const configuration = installerConfiguration;
  if (
    !configuration ||
    !verifyRuntimeAt(layout, stagingRoot, configuration.storageRoot)
  ) {
    throw new Error(
      "The staged SQLite runtime failed verification; it was not installed.",
    );
  }
}

async function installManagedRuntime(
  layout: RuntimeLayout,
): Promise<string | null> {
  const configuration = installerConfiguration;
  if (!configuration) {
    return null;
  }
  const { runtimeRoot, packagePath } = layout;

  mkdirSync(configuration.storageRoot, { recursive: true });
  if (installedRuntimeValid(layout)) {
    return packagePath;
  }

  const lockRoot = `${runtimeRoot}.lock`;
  return withInstallLock(
    lockRoot,
    async () => {
      if (installedRuntimeValid(layout)) {
        return packagePath;
      }

      removeAbandonedInstallDirectories(runtimeRoot);
      // An existing runtime here failed verification; never load or reuse it.
      moveInvalidRuntimeAside(runtimeRoot);
      const tempRoot = `${runtimeRoot}.tmp-${process.pid}-${Date.now()}`;
      rmSync(tempRoot, { recursive: true, force: true });
      mkdirSync(tempRoot, { recursive: true });

      const withProgress =
        configuration.withInstallProgress ??
        (<T>(task: (progress: SQLiteInstallProgress) => Promise<T>) =>
          task({ signal: new AbortController().signal, report: () => {} }));
      try {
        await withProgress((progress) =>
          stageRuntime(layout, tempRoot, progress),
        );
        renameSync(tempRoot, runtimeRoot);
      } catch (error) {
        rmSync(tempRoot, { recursive: true, force: true });
        throw error;
      }

      installerLog(
        `SQLite runtime better-sqlite3 ${layout.plan.version} (${layout.plan.target}, Node-API ${layout.plan.nodeApiVersion}) is ready at ${runtimeRoot}.`,
      );
      return packagePath;
    },
    () => (installedRuntimeValid(layout) ? packagePath : null),
  );
}

export function sqliteRuntimeDownloadSummary(): string {
  const { betterSqlite3Version, tarball } = SQLITE_ARTIFACTS;
  return `better-sqlite3 ${betterSqlite3Version}, ${formatSQLiteDownloadSize(tarball.size)} from ${SQLITE_REGISTRY_HOST}`;
}

export function configureSQLiteInstaller(
  configuration: SQLiteInstallerConfiguration,
): void {
  installerConfiguration = {
    storageRoot: resolve(configuration.storageRoot),
    log: configuration.log,
    allowInstall: configuration.allowInstall,
    isTrusted: configuration.isTrusted,
    withInstallProgress: configuration.withInstallProgress,
  };
}

export function resetSQLiteInstallerForTests(): void {
  installerConfiguration = null;
  inFlightInstall = null;
  obsoleteRuntimeCleanupDone = false;
  installLockTimings = { ...DEFAULT_SQLITE_INSTALL_LOCK_TIMINGS };
  hostProbeOverride = null;
  nodeApiVersionOverride = undefined;
  cachedHost = null;
}

export function setSQLiteInstallLockTimingsForTests(
  timings: Partial<typeof DEFAULT_SQLITE_INSTALL_LOCK_TIMINGS>,
): void {
  installLockTimings = { ...installLockTimings, ...timings };
}

export function setSQLiteHostForTests(
  host: { probe?: SQLiteHostProbe | null; nodeApiVersion?: string } = {},
): void {
  hostProbeOverride = host.probe ?? null;
  nodeApiVersionOverride = host.nodeApiVersion;
  cachedHost = null;
}

export function probeInstalledBetterSqlite3Runtime(): SQLiteInstalledRuntimeProbe {
  let target: string;
  try {
    target = currentTarget();
  } catch (error) {
    target = `unsupported (${errorMessage(error)})`;
  }
  const layout = tryCurrentLayout();
  const installedPackagePath = layout?.packagePath ?? null;
  const installedBinaryPath = installedPackagePath
    ? sqliteNativeBindingPath(installedPackagePath)
    : null;
  return {
    runtime: currentRuntime(),
    target,
    nodeApi: hostNodeApiVersion() || "unknown",
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

export async function ensureSQLiteRuntimeInstalled(): Promise<string | null> {
  const layout = currentLayout();
  if (!layout) {
    return null;
  }
  if (refreshInstalledRuntimeUse(layout)) {
    await removeObsoleteSQLiteRuntimesOnce();
    return layout.packagePath;
  }
  if (!inFlightInstall) {
    const allowInstall = installerConfiguration?.allowInstall;
    const isTrusted = installerConfiguration?.isTrusted;
    inFlightInstall = (async () => {
      let installed = await recordInstalledRuntimeUse(layout);
      if (!installed) {
        if (isTrusted && !isTrusted()) {
          throw new Error(
            'SQLite runtime installation is disabled because this workspace is not trusted (Restricted Mode). RapiDB downloads the native SQLite runtime only in trusted workspaces. To allow it, trust this workspace (command "Workspaces: Manage Workspace Trust", or the Restricted Mode indicator in the status bar) and open the database again; once installed, the runtime is also used in untrusted workspaces. The database was not opened.',
          );
        }
        if (allowInstall && !(await allowInstall())) {
          throw new Error(
            "SQLite runtime installation was not authorized. The database was not opened.",
          );
        }
        installed = await installManagedRuntime(layout);
      }
      if (installed) {
        await removeObsoleteSQLiteRuntimesOnce();
      }
      return installed;
    })().finally(() => {
      inFlightInstall = null;
    });
  }
  return inFlightInstall;
}
