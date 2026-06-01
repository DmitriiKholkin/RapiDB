import {
  cpSync,
  existsSync,
  mkdirSync,
  readFileSync,
  renameSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { dirname, join, relative, resolve } from "node:path";

type BetterSqlite3PackageJson = {
  name?: string;
  version?: string;
  repository?: unknown;
};

type PrebuildInstallDownload = (
  downloadUrl: string,
  options: {
    pkg: BetterSqlite3PackageJson;
    runtime: "electron" | "node";
    abi: string;
    platform: NodeJS.Platform;
    arch: NodeJS.Architecture;
    libc?: string;
    path: string;
    force: boolean;
    nolocal: boolean;
    "tag-prefix": string;
    log: {
      http(...args: unknown[]): void;
      silly(...args: unknown[]): void;
      debug(...args: unknown[]): void;
      info(...args: unknown[]): void;
      warn(...args: unknown[]): void;
      error(...args: unknown[]): void;
      critical(...args: unknown[]): void;
      alert(...args: unknown[]): void;
      emergency(...args: unknown[]): void;
      notice(...args: unknown[]): void;
      verbose(...args: unknown[]): void;
      fatal(...args: unknown[]): void;
    };
  },
  callback: (error?: Error | null, resolved?: string) => void,
) => void;

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

function installerLog(message: string): void {
  const line = `[RapiDB SQLite] ${message}`;
  installerConfiguration?.log?.(line);
  if (!installerConfiguration?.log) {
    console.log(line);
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

function currentRuntimeStorageKey(): string {
  return [
    currentRuntime(),
    `abi-${process.versions.modules}`,
    process.platform,
    detectLinuxLibc() || null,
    process.arch,
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
    currentRuntimeStorageKey(),
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

  const actual = JSON.parse(
    readFileSync(manifestPath, "utf8"),
  ) as Partial<InstalledRuntimeManifest>;
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

function createLogger() {
  return {
    http(...args: unknown[]) {
      installerLog(args.map(String).join(" "));
    },
    silly() {},
    debug() {},
    info(...args: unknown[]) {
      installerLog(args.map(String).join(" "));
    },
    warn(...args: unknown[]) {
      installerLog(args.map(String).join(" "));
    },
    error(...args: unknown[]) {
      installerLog(args.map(String).join(" "));
    },
    critical(...args: unknown[]) {
      installerLog(args.map(String).join(" "));
    },
    alert(...args: unknown[]) {
      installerLog(args.map(String).join(" "));
    },
    emergency(...args: unknown[]) {
      installerLog(args.map(String).join(" "));
    },
    notice(...args: unknown[]) {
      installerLog(args.map(String).join(" "));
    },
    verbose() {},
    fatal(...args: unknown[]) {
      installerLog(args.map(String).join(" "));
    },
  };
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

async function withInstallLock<T>(
  lockRoot: string,
  action: () => Promise<T>,
): Promise<T> {
  mkdirSync(dirname(lockRoot), { recursive: true });
  const deadline = Date.now() + 30_000;
  while (true) {
    try {
      mkdirSync(lockRoot, { recursive: false });
      break;
    } catch (error) {
      const nodeError = error as NodeJS.ErrnoException;
      if (nodeError.code !== "EEXIST") {
        throw error;
      }
      if (Date.now() >= deadline) {
        throw new Error(
          `Timed out waiting for SQLite runtime installation lock at ${lockRoot}.`,
        );
      }
      await waitFor(150);
    }
  }

  try {
    return await action();
  } finally {
    rmSync(lockRoot, { recursive: true, force: true });
  }
}

async function downloadPrebuiltBinary(
  runtimeRoot: string,
  baseDir: string,
): Promise<void> {
  const prebuildInstall = require("prebuild-install") as {
    download: PrebuildInstallDownload;
  };
  const prebuildInstallUtil = require("prebuild-install/util") as {
    getDownloadUrl(options: {
      pkg: BetterSqlite3PackageJson;
      runtime: "electron" | "node";
      abi: string;
      platform: NodeJS.Platform;
      arch: NodeJS.Architecture;
      libc?: string;
      "tag-prefix": string;
    }): string;
  };

  const betterSqlite3PackageRoot = join(
    runtimeRoot,
    "node_modules",
    "better-sqlite3",
  );
  const pkg = JSON.parse(
    readFileSync(join(betterSqlite3PackageRoot, "package.json"), "utf8"),
  ) as BetterSqlite3PackageJson;
  const options = {
    pkg,
    runtime: currentRuntime(),
    abi: process.versions.modules,
    platform: process.platform,
    arch: process.arch,
    libc: detectLinuxLibc(),
    path: betterSqlite3PackageRoot,
    force: true,
    nolocal: true,
    "tag-prefix": "v",
    log: createLogger(),
  };
  const downloadUrl = prebuildInstallUtil.getDownloadUrl(options);
  await new Promise<void>((resolvePromise, rejectPromise) => {
    prebuildInstall.download(downloadUrl, options, (error) => {
      if (error) {
        rejectPromise(
          new Error(
            `Could not download the SQLite runtime for ${currentTargetLabel()} (${currentRuntime()}, ABI ${process.versions.modules}) from ${downloadUrl}. ${errorMessage(error)}`,
          ),
        );
        return;
      }
      resolvePromise();
    });
  });

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
  return withInstallLock(lockRoot, async () => {
    if (manifestMatches(baseDir)) {
      return installedPackagePath;
    }

    const tempRoot = `${runtimeRoot}.tmp-${process.pid}-${Date.now()}`;
    rmSync(tempRoot, { recursive: true, force: true });
    rmSync(runtimeRoot, { recursive: true, force: true });
    mkdirSync(tempRoot, { recursive: true });

    try {
      copyBundledRuntimeScaffold(baseDir, tempRoot);
      await downloadPrebuiltBinary(tempRoot, baseDir);
      renameSync(tempRoot, runtimeRoot);
    } catch (error) {
      rmSync(tempRoot, { recursive: true, force: true });
      throw error;
    }

    installerLog(
      `SQLite runtime ready at ${runtimeRoot} for ${currentTargetLabel()} (${currentRuntime()}, ABI ${process.versions.modules}).`,
    );
    return installedPackagePath;
  });
}

export function configureSQLiteInstaller(configuration: {
  storageRoot: string;
  log?: (message: string) => void;
}): void {
  installerConfiguration = {
    storageRoot: resolve(configuration.storageRoot),
    log: configuration.log,
  };
}

export function resetSQLiteInstallerForTests(): void {
  installerConfiguration = null;
  inFlightInstall = null;
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
  if (resolveInstalledBetterSqlite3PackagePath(baseDir)) {
    return resolveInstalledBetterSqlite3PackagePath(baseDir);
  }
  if (!installerConfiguration) {
    return null;
  }
  if (!inFlightInstall) {
    inFlightInstall = installManagedRuntime(baseDir).finally(() => {
      inFlightInstall = null;
    });
  }
  return inFlightInstall;
}

export async function warmupSQLiteRuntime(baseDir: string): Promise<void> {
  try {
    await ensureSQLiteRuntimeInstalled(baseDir);
  } catch (error) {
    installerLog(`[best-effort] ${errorMessage(error)}`);
  }
}
