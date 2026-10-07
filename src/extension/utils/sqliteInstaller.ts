import { execFile } from "node:child_process";
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
import { promisify } from "node:util";
import { logger } from "./logger";
import {
  extractSQLiteArchive,
  SQLITE_ARCHIVE_LIMITS,
  SQLITE_SOURCE,
  verifySQLiteIntegrity,
} from "./sqliteArchive";
import {
  SQLITE_ARTIFACT_POLICY_ID,
  type SQLiteArtifactPin,
  type SQLiteHeaderPins,
  sqliteHeaderPins,
  sqlitePrebuiltPin,
} from "./sqliteArtifactPins";
import {
  assertSQLiteBuildPath,
  createSQLiteBuildToolLock,
  pinSQLiteSourceNodeActions,
  resolveSQLiteBuildTools,
} from "./sqliteBuildTools";

const execFileAsync = promisify(execFile);

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

function currentRuntimeStorageKey(): string {
  return [
    currentRuntime(),
    `abi-${process.versions.modules}`,
    process.platform,
    detectLinuxLibc() || null,
    process.arch,
    // Do not reuse binaries extracted by older, unbounded installers.
    SQLITE_ARTIFACT_POLICY_ID,
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

// --- Electron 42 V8 API compatibility fallback chain --------------------------
// Better-sqlite3 prebuilt binaries are not yet published for Electron 42
// (NODE_MODULE_VERSION >= 146). The fallback chain is:
//   1. Official prebuilt from the trusted WiseLibs HTTPS release origin
//   2. Patched prebuilt with a publisher-reviewed checksum committed here
//   3. Source build with V8 API patch via node-gyp (requires build toolchain)
//
// To publish patched prebuilts, build on each target platform and upload:
//   gh release create rapidb-patched-sqlite /tmp/rapidb-patched-prebuilts/*.tar.gz
// Then independently review and commit each asset's checksum below. Uploading
// assets (or a checksum next to them) alone does not enable this fallback.
// ------------------------------------------------------------------------------

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
  timeoutMs = 60_000,
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
        // Wait for a pending open/close before removing the partial file (also
        // required on Windows). Otherwise a late open can recreate it.
        if (downloadFile && !downloadFile.closed)
          downloadFile.once("close", cleanup);
        else cleanup();
      }
    };
    // Socket timeout is only an inactivity timer. A server can drip bytes
    // indefinitely, so also bound the entire download across redirects.
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
      // http is only for loopback tests; production URLs are https.
      // Reject non-loopback http (including https->http downgrade redirects).
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
      // biome-ignore lint/suspicious/noExplicitAny: node http/https interop for tests
      let activeResponse: any | undefined;
      // biome-ignore lint/suspicious/noExplicitAny: node fs WriteStream interop
      let activeFile: any | undefined;
      // biome-ignore lint/suspicious/noExplicitAny: node ClientRequest interop
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
            // Free the socket before following the redirect.
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
  await downloadToFile(pin.url, destination, 60_000, {
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

/** node-gyp is never allowed to download or reuse global headers. Both gypi
 * files and Windows node.lib are verified before it sees this private nodedir.
 */
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
    "--release",
  ];
}

/**
 * Applies the V8 External API compatibility patch from
 * https://github.com/WiseLibs/better-sqlite3/pull/1475
 *
 * The patch adds version-guarded macros for tagged v8::External
 * creation/access (required by V8 14+ / NODE_MODULE_VERSION >= 146)
 * and fixes a SetNativeDataProperty overload ambiguity.
 */
function applyElectron42V8Patch(sourceDir: string): void {
  // 1. src/util/macros.cpp — add EXTERNAL_NEW / EXTERNAL_VALUE macros
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

  // 2. src/better_sqlite3.cpp — use EXTERNAL_NEW macro + MSVC compat for Electron 42 headers
  const mainPath = join(sourceDir, "src", "better_sqlite3.cpp");
  let mainCpp = readFileSync(mainPath, "utf8");
  mainCpp = mainCpp.replace(
    "v8::Local<v8::External> data = v8::External::New(isolate, addon);",
    "v8::Local<v8::External> data = EXTERNAL_NEW(isolate, addon);",
  );
  // Electron 42 V8 headers use __builtin_frame_address (GCC/Clang only)
  if (!mainCpp.includes("__builtin_frame_address")) {
    mainCpp =
      "#ifdef _MSC_VER\n" +
      "#define __builtin_frame_address(x) ((void*)0)\n" +
      "#endif\n" +
      mainCpp;
  }
  writeFileSync(mainPath, mainCpp, "utf8");

  // 3. src/util/helpers.cpp — pass nullptr instead of 0 for missing setter
  //    (better-sqlite3 source uses tabs for indentation)
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

  const abi = process.versions.modules;
  const platform = process.platform;
  const arch = process.arch;
  const tagPrefix = "v";
  const fileName = `better-sqlite3-${tagPrefix}${pkg.version}-electron-${tagPrefix}${abi}-${platform}-${arch}.tar.gz`;
  const pin = sqlitePrebuiltPin("patched", fileName);
  const downloadUrl = pin.url;
  const tarballPath = join(runtimeRoot, fileName);

  try {
    installerLog(`Trying patched prebuilt from ${downloadUrl}…`);
    await downloadPinnedSQLiteArtifact(pin, tarballPath, GITHUB_DOWNLOAD_HOSTS);

    // Extract into the scaffold (tarball contains build/Release/better_sqlite3.node)
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

async function rebuildFromSourceWithPatch(
  runtimeRoot: string,
  baseDir: string,
): Promise<void> {
  // cwd, headers, devdir and private tool/home paths all derive from this root.
  // Reject expansion syntax before downloading, patching or executing tools.
  assertSQLiteBuildPath(runtimeRoot, "runtime");
  const pkg = readBundledBetterSqlite3Package(baseDir);
  if (!pkg.version) {
    throw new Error(
      "Cannot determine better-sqlite3 version for source build.",
    );
  }

  const sourceDir = join(runtimeRoot, "better-sqlite3-build-source");
  const tarballPath = join(runtimeRoot, "better-sqlite3-source.tgz");
  const toolDir = join(runtimeRoot, "sqlite-build-tools");
  const headerPins = sqliteHeaderPins(
    process.versions.electron ?? "",
    process.versions.modules,
    process.platform,
    process.arch,
  );

  try {
    // Download the npm package tarball (includes src/, deps/, binding.gyp)
    if (pkg.version !== SQLITE_SOURCE.version) {
      throw new Error(
        `No locked SQLite source integrity for better-sqlite3 ${pkg.version}. Update the source pin from package-lock.json.`,
      );
    }
    const tarballUrl = SQLITE_SOURCE.url;
    installerLog(`Downloading better-sqlite3 ${pkg.version} source tarball…`);
    await downloadToFile(tarballUrl, tarballPath, 60_000, {
      allowedHosts: ["registry.npmjs.org"],
    });

    // Extract
    mkdirSync(sourceDir, { recursive: true });
    await extractSQLiteArchive(
      tarballPath,
      sourceDir,
      "source",
      SQLITE_SOURCE.integrity,
    );
    // Read current roots at discovery time; standalone Node callers have no
    // workspace. Keep the installer/worker independent of the VS Code module.
    const buildTools = resolveSQLiteBuildTools(
      findPackageRoot(baseDir) ?? baseDir,
      runtimeRoot,
      process.env,
      installerConfiguration?.workspaceRoots?.() ?? [],
    );
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
      { cwd: toolDir, timeout: 300_000, env: buildTools.env },
    );

    // Apply the Electron 42 V8 API patch
    installerLog("Applying Electron 42 V8 API compatibility patch…");
    applyElectron42V8Patch(sourceDir);
    const headerDir = await preparePinnedSQLiteHeaders(
      runtimeRoot,
      headerPins,
      process.platform,
      process.arch,
    );

    // Build native module for the current Electron ABI
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
      ),
      { cwd: sourceDir, timeout: 300_000, env: buildTools.env },
    );

    // Copy the built binary into the scaffold
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
    rmSync(tarballPath, { force: true });
    rmSync(sourceDir, { recursive: true, force: true });
    rmSync(toolDir, { recursive: true, force: true });
    rmSync(join(runtimeRoot, "electron-headers"), {
      recursive: true,
      force: true,
    });
    rmSync(join(runtimeRoot, "build-home"), { recursive: true, force: true });
  }
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
  const fileName = `better-sqlite3-v${pkg.version}-${currentRuntime()}-v${process.versions.modules}-${process.platform}${detectLinuxLibc()}-${process.arch}.tar.gz`;
  // Do not use prebuild-install's environment-controlled mirrors, npm cache,
  // arbitrary archive extraction, or require() of a downloaded addon here.
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
    // Electron 42+ (NODE_MODULE_VERSION >= 146) has no published prebuilts
    // yet. Try patched prebuilt first (no build tools needed), then source build.
    if (
      currentRuntime() === "electron" &&
      Number(process.versions.modules) >= 146
    ) {
      // Step 1: try patched prebuilt from RapiDB GitHub releases
      let patchedPrebuiltInstalled = false;
      try {
        await downloadPatchedPrebuilt(runtimeRoot, baseDir);
        patchedPrebuiltInstalled = true;
      } catch (patchedError) {
        installerLog(
          `Patched prebuilt not available: ${errorMessage(patchedError)}`,
        );
      }

      // Step 2: try building from source with the V8 API compat patch
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
    if (
      installerConfiguration.allowInstall &&
      !(await installerConfiguration.allowInstall())
    ) {
      throw new Error(
        "SQLite runtime installation was not authorized. The database was not opened.",
      );
    }
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
