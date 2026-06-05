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

// --- Electron 42 V8 API compatibility fallback chain --------------------------\n// Better-sqlite3 prebuilt binaries are not yet published for Electron 42\n// (NODE_MODULE_VERSION >= 146). The fallback chain is:\n//   1. Official prebuilt from GitHub releases (prebuild-install)\n//   2. Patched prebuilt from RapiDB GitHub releases (no build tools needed)\n//   3. Source build with V8 API patch via node-gyp (requires build toolchain)\n//\n// To publish patched prebuilts, build on each target platform and upload:\n//   gh release create rapidb-patched-sqlite /tmp/better-sqlite3-v12.10.0-electron-v146-darwin-arm64.tar.gz\n// ------------------------------------------------------------------------------\n\nconst PATCHED_PREBUILDS_RELEASE_URL =\n  "https://github.com/DmitriiKholkin/RapiDB/releases/download/rapidb-patched-sqlite";

function downloadToFile(url: string, destPath: string): Promise<void> {
  const { createWriteStream } = require("node:fs") as typeof import("node:fs");
  const https = require("node:https") as typeof import("node:https");

  return new Promise<void>((resolvePromise, rejectPromise) => {
    const follow = (target: string, depth: number): void => {
      if (depth > 5) {
        rejectPromise(new Error(`Too many redirects downloading ${url}`));
        return;
      }
      https
        .get(target, (response) => {
          if (
            (response.statusCode === 301 || response.statusCode === 302) &&
            response.headers.location
          ) {
            follow(response.headers.location, depth + 1);
            return;
          }
          if (response.statusCode !== 200) {
            rejectPromise(
              new Error(`HTTP ${response.statusCode} downloading ${target}`),
            );
            return;
          }
          const file = createWriteStream(destPath);
          response.pipe(file);
          file.on("finish", () => {
            file.close(() => resolvePromise());
          });
          file.on("error", (err: Error) => {
            rmSync(destPath, { force: true });
            rejectPromise(err);
          });
        })
        .on("error", rejectPromise);
    };
    follow(url, 0);
  });
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
  const { execSync } =
    require("node:child_process") as typeof import("node:child_process");

  const pkg = readBundledBetterSqlite3Package(baseDir);
  if (!pkg.version) {
    throw new Error("Cannot determine better-sqlite3 version.");
  }

  const abi = process.versions.modules;
  const platform = process.platform;
  const arch = process.arch;
  const tagPrefix = "v";
  const fileName = `better-sqlite3-${tagPrefix}${pkg.version}-electron-${tagPrefix}${abi}-${platform}-${arch}.tar.gz`;
  const downloadUrl = `${PATCHED_PREBUILDS_RELEASE_URL}/${fileName}`;
  const tarballPath = join(runtimeRoot, fileName);

  try {
    installerLog(`Trying patched prebuilt from ${downloadUrl}…`);
    await downloadToFile(downloadUrl, tarballPath);

    // Extract into the scaffold (tarball contains build/Release/better_sqlite3.node)
    const scaffoldDir = join(runtimeRoot, "node_modules", "better-sqlite3");
    execSync(`tar xzf "${tarballPath}" -C "${scaffoldDir}"`, {
      stdio: "pipe",
    });

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
  const { execSync } =
    require("node:child_process") as typeof import("node:child_process");

  const pkg = readBundledBetterSqlite3Package(baseDir);
  if (!pkg.version) {
    throw new Error(
      "Cannot determine better-sqlite3 version for source build.",
    );
  }

  // Verify npx is available (node-gyp itself is fetched via npx on demand)
  try {
    execSync("npx --version", { stdio: "ignore" });
  } catch {
    throw new Error(
      "npx is required to build better-sqlite3 from source for Electron 42+. " +
        "Ensure npm is installed and available on PATH.\n" +
        "Also ensure Python 3 and a C++ compiler are available " +
        "(Xcode Command Line Tools on macOS, build-essential on Linux, Visual Studio Build Tools on Windows).",
    );
  }

  const sourceDir = join(runtimeRoot, "better-sqlite3-build-source");
  const tarballPath = join(runtimeRoot, "better-sqlite3-source.tgz");

  try {
    // Download the npm package tarball (includes src/, deps/, binding.gyp)
    const tarballUrl = `https://registry.npmjs.org/better-sqlite3/-/better-sqlite3-${pkg.version}.tgz`;
    installerLog(`Downloading better-sqlite3 ${pkg.version} source tarball…`);
    await downloadToFile(tarballUrl, tarballPath);

    // Extract
    mkdirSync(sourceDir, { recursive: true });
    execSync(
      `tar xzf "${tarballPath}" -C "${sourceDir}" --strip-components=1`,
      { stdio: "pipe" },
    );

    // Apply the Electron 42 V8 API patch
    installerLog("Applying Electron 42 V8 API compatibility patch…");
    applyElectron42V8Patch(sourceDir);

    // Build native module for the current Electron ABI
    const electronVersion = process.versions.electron;
    installerLog(
      `Building better-sqlite3 from source for Electron ${electronVersion} (ABI ${process.versions.modules}, ${process.platform}-${process.arch})…`,
    );
    execSync(
      [
        "npx node-gyp rebuild",
        `--target=${electronVersion}`,
        `--arch=${process.arch}`,
        `--target_platform=${process.platform}`,
        "--dist-url=https://electronjs.org/headers",
        "--runtime=electron",
      ].join(" "),
      { cwd: sourceDir, stdio: "pipe", timeout: 300_000 },
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

  try {
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
  } catch (prebuiltError) {
    // Electron 42+ (NODE_MODULE_VERSION >= 146) has no published prebuilts
    // yet. Try patched prebuilt first (no build tools needed), then source build.
    if (Number(process.versions.modules) >= 146) {
      // Step 1: try patched prebuilt from RapiDB GitHub releases
      try {
        await downloadPatchedPrebuilt(runtimeRoot, baseDir);
        return;
      } catch (patchedError) {
        installerLog(
          `Patched prebuilt not available: ${errorMessage(patchedError)}`,
        );
      }

      // Step 2: try building from source with the V8 API compat patch
      installerLog("Attempting source build with Electron 42 V8 patch…");
      try {
        await rebuildFromSourceWithPatch(runtimeRoot, baseDir);
      } catch (sourceError) {
        installerLog(`Source build failed: ${errorMessage(sourceError)}`);
        throw prebuiltError;
      }
    } else {
      throw prebuiltError;
    }
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
