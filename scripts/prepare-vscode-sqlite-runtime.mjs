import { spawnSync } from "node:child_process";
import {
  existsSync,
  mkdirSync,
  readdirSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { validateBinaryModuleVersion } from "./utils/extractModuleVersion.mjs";

const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const isStrict = process.argv.includes("--strict");

const SUPPORTED_TARGETS = {
  "win32-x64": { platform: "win32", arch: "x64" },
  "win32-arm64": { platform: "win32", arch: "arm64" },
  "linux-x64": { platform: "linux", arch: "x64", libc: "glibc" },
  "linux-arm64": { platform: "linux", arch: "arm64", libc: "glibc" },
  "darwin-x64": { platform: "darwin", arch: "x64" },
  "darwin-arm64": { platform: "darwin", arch: "arm64" },
};

const RUNTIME_KEEP_PACKAGES = new Set([
  "better-sqlite3",
  "bindings",
  "file-uri-to-path",
]);

const DOCKER_TARGETS = {
  "linux-x64": {
    platform: "linux/amd64",
    image: "node:20-bookworm",
    installCommand: "apt-get update && apt-get install -y python3 make g++",
  },
  "linux-arm64": {
    platform: "linux/arm64/v8",
    image: "node:20-bookworm",
    installCommand: "apt-get update && apt-get install -y python3 make g++",
  },
};

const NODE_SERVER_RUNTIME_TARGETS = new Set([
  "win32-x64",
  "win32-arm64",
  "linux-x64",
  "linux-arm64",
  "darwin-x64",
  "darwin-arm64",
]);

const VSCODE_SERVER_NODE_VERSION =
  process.env.RAPIDB_VSCODE_SERVER_NODE_VERSION?.trim() || "20.9.0";
const VSCODE_SERVER_MODULE_VERSION = Number.parseInt(
  process.env.RAPIDB_VSCODE_SERVER_MODULE_VERSION?.trim() || "115",
  10,
);

function stagedRuntimeRootFor(targetId) {
  return join(repoRoot, ".rapidb-vscode", "better-sqlite3", targetId);
}

/**
 * Staged Node.js runtime root — used by VS Code Server (plain Node.js, not Electron).
 * Stored separately from the Electron staged runtime because the ABIs differ.
 */
function stagedNodeRuntimeRootFor(targetId) {
  return join(repoRoot, ".rapidb-vscode", "better-sqlite3-node", targetId);
}

function log(message) {
  console.log(`[RapiDB SQLite] ${message}`);
}

function fail(message) {
  if (isStrict) {
    throw new Error(message);
  }
  log(message);
}

function readJson(filePath) {
  return JSON.parse(readFileSync(filePath, "utf8"));
}

function detectHostRuntimeTarget() {
  if (process.platform === "win32") {
    if (process.arch === "x64") return "win32-x64";
    if (process.arch === "arm64") return "win32-arm64";
    return null;
  }
  if (process.platform === "darwin") {
    if (process.arch === "x64") return "darwin-x64";
    if (process.arch === "arm64") return "darwin-arm64";
    return null;
  }
  if (process.platform === "linux") {
    if (process.arch === "x64") return "linux-x64";
    if (process.arch === "arm64") return "linux-arm64";
  }
  return null;
}

function pruneUnsupportedStagedRuntimes() {
  for (const subdir of ["better-sqlite3", "better-sqlite3-node"]) {
    const stagedRoot = join(repoRoot, ".rapidb-vscode", subdir);
    if (!existsSync(stagedRoot)) {
      continue;
    }
    for (const entryName of readdirSync(stagedRoot)) {
      if (!(entryName in SUPPORTED_TARGETS)) {
        rmSync(join(stagedRoot, entryName), { recursive: true, force: true });
      }
    }
  }
}

function parseTargetIds() {
  const explicitTargets = [];
  const args = process.argv.slice(2);
  for (let index = 0; index < args.length; index += 1) {
    if (args[index] !== "--target") {
      continue;
    }
    const targetId = args[index + 1];
    if (!targetId || !(targetId in SUPPORTED_TARGETS)) {
      throw new Error(
        `Unsupported SQLite runtime target: ${targetId ?? "<missing>"}.`,
      );
    }
    explicitTargets.push(targetId);
    index += 1;
  }

  if (args.includes("--all-targets")) {
    return Object.keys(SUPPORTED_TARGETS);
  }

  if (explicitTargets.length > 0) {
    return explicitTargets;
  }

  const hostTarget = detectHostRuntimeTarget();
  if (!hostTarget) {
    throw new Error(
      "Could not resolve the host SQLite runtime target. Pass --target explicitly.",
    );
  }
  return [hostTarget];
}

function parseSemverLines(output) {
  return output
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => /^\d+\.\d+\.\d+$/.test(line));
}

function readElectronVersionFromMacApp(appPath) {
  const infoPlistPath = join(
    appPath,
    "Contents",
    "Frameworks",
    "Electron Framework.framework",
    "Versions",
    "A",
    "Resources",
    "Info.plist",
  );
  if (!existsSync(infoPlistPath)) {
    return null;
  }
  const result = spawnSync(
    "plutil",
    ["-extract", "CFBundleVersion", "raw", infoPlistPath],
    { encoding: "utf8" },
  );
  if (result.status !== 0) {
    return null;
  }
  const version = result.stdout.trim();
  return /^\d+\.\d+\.\d+$/.test(version) ? version : null;
}

function readElectronVersionFromExecutable(executablePath) {
  if (!executablePath) {
    return null;
  }
  const result = spawnSync(executablePath, ["--version"], { encoding: "utf8" });
  if (result.status !== 0) {
    return null;
  }
  const versions = parseSemverLines(result.stdout);
  return versions.length >= 4 ? versions[3] : null;
}

function detectElectronVersion() {
  const fromEnv = process.env.RAPIDB_VSCODE_ELECTRON_VERSION?.trim();
  if (fromEnv && /^\d+\.\d+\.\d+$/.test(fromEnv)) {
    return fromEnv;
  }

  const execPath = process.env.VSCODE_EXEC_PATH?.trim();
  if (process.platform === "darwin" && execPath) {
    const appMarker = ".app/Contents/";
    const markerIndex = execPath.indexOf(appMarker);
    if (markerIndex >= 0) {
      const appPath = execPath.slice(0, markerIndex + 4);
      const version = readElectronVersionFromMacApp(appPath);
      if (version) {
        return version;
      }
    }
  }

  if (process.platform === "darwin") {
    for (const appPath of [
      "/Applications/Visual Studio Code.app",
      "/Applications/Visual Studio Code - Insiders.app",
    ]) {
      const version = readElectronVersionFromMacApp(appPath);
      if (version) {
        return version;
      }
    }
  }

  if (execPath) {
    const version = readElectronVersionFromExecutable(execPath);
    if (version) {
      return version;
    }
  }

  const cli = process.platform === "win32" ? "code.cmd" : "code";
  const version = readElectronVersionFromExecutable(cli);
  if (version) {
    return version;
  }

  return null;
}

function npmCommand() {
  return process.platform === "win32" ? "npm.cmd" : "npm";
}

function dockerCommand() {
  return process.platform === "win32" ? "docker.exe" : "docker";
}

function installedBetterSqlite3Version() {
  const packageJsonPath = join(
    repoRoot,
    "node_modules",
    "better-sqlite3",
    "package.json",
  );
  if (!existsSync(packageJsonPath)) {
    throw new Error(
      'better-sqlite3 is not installed in the workspace yet. Run "npm install" first.',
    );
  }
  return readJson(packageJsonPath).version;
}

function prepareStagingRoot(stagedRuntimeRoot) {
  rmSync(stagedRuntimeRoot, {
    recursive: true,
    force: true,
    maxRetries: 5,
    retryDelay: 200,
  });
  mkdirSync(stagedRuntimeRoot, { recursive: true });
  writeFileSync(
    join(stagedRuntimeRoot, "package.json"),
    `${JSON.stringify(
      {
        name: "rapidb-sqlite-vscode-runtime",
        private: true,
      },
      null,
      2,
    )}\n`,
    "utf8",
  );
}

function installStagedRuntime(stagedRuntimeRoot, betterSqlite3Version) {
  const result = spawnSync(
    npmCommand(),
    [
      "install",
      "--omit=dev",
      "--no-package-lock",
      "--ignore-scripts",
      `better-sqlite3@${betterSqlite3Version}`,
    ],
    {
      cwd: stagedRuntimeRoot,
      stdio: "inherit",
    },
  );
  if (result.status !== 0) {
    throw new Error(
      `Failed to install better-sqlite3 into ${stagedRuntimeRoot}.`,
    );
  }
}

function resolvePrebuildInstallCli(stagedRuntimeRoot) {
  const cliPath = join(
    stagedRuntimeRoot,
    "node_modules",
    "prebuild-install",
    "bin.js",
  );
  if (!existsSync(cliPath)) {
    throw new Error(
      `prebuild-install was not found under ${stagedRuntimeRoot}.`,
    );
  }
  return cliPath;
}

function downloadElectronPrebuild(
  stagedRuntimeRoot,
  targetId,
  electronVersion,
) {
  const target = SUPPORTED_TARGETS[targetId];
  const betterSqlite3Path = join(
    stagedRuntimeRoot,
    "node_modules",
    "better-sqlite3",
  );
  const args = [
    resolvePrebuildInstallCli(stagedRuntimeRoot),
    "--runtime",
    "electron",
    "--target",
    electronVersion,
    "--platform",
    target.platform,
    "--arch",
    target.arch,
  ];
  if (target.libc) {
    args.push("--libc", target.libc);
  }
  const result = spawnSync(process.execPath, args, {
    cwd: betterSqlite3Path,
    stdio: "inherit",
  });
  return result.status === 0;
}

function downloadNodePrebuild(stagedRuntimeRoot, targetId, nodeVersion) {
  const target = SUPPORTED_TARGETS[targetId];
  const betterSqlite3Path = join(
    stagedRuntimeRoot,
    "node_modules",
    "better-sqlite3",
  );
  const args = [
    resolvePrebuildInstallCli(stagedRuntimeRoot),
    "--runtime",
    "node",
    "--target",
    nodeVersion,
    "--platform",
    target.platform,
    "--arch",
    target.arch,
  ];
  if (target.libc) {
    args.push("--libc", target.libc);
  }
  const result = spawnSync(process.execPath, args, {
    cwd: betterSqlite3Path,
    stdio: "inherit",
  });
  return result.status === 0 && hasNativeBinary(stagedRuntimeRoot);
}

async function rebuildStagedRuntime(
  stagedRuntimeRoot,
  electronVersion,
  targetId,
) {
  let rebuild;
  try {
    ({ rebuild } = await import("@electron/rebuild"));
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    throw new Error(
      `@electron/rebuild is unavailable. Run "npm install" in the repository root before preparing the VS Code SQLite runtime. ${message}`,
    );
  }

  await rebuild({
    buildPath: stagedRuntimeRoot,
    electronVersion,
    arch: SUPPORTED_TARGETS[targetId].arch,
    force: true,
    onlyModules: ["better-sqlite3"],
    headerURL: "https://www.electronjs.org/headers",
    buildFromSource: true,
  });
}

function canUseDockerFallback(targetId) {
  return targetId in DOCKER_TARGETS;
}

function dockerAvailable() {
  const result = spawnSync(dockerCommand(), ["version"], {
    stdio: "ignore",
  });
  return result.status === 0;
}

function shellEscape(value) {
  return `'${String(value).replace(/'/g, `'"'"'`)}'`;
}

function dockerTargetBuildScript(targetId, electronVersion) {
  const target = SUPPORTED_TARGETS[targetId];
  const mountedRuntimeRoot = `/workspace/.rapidb-vscode/better-sqlite3/${targetId}`;
  const localRuntimeRoot = `/tmp/rapidb-sqlite-runtime/${targetId}`;
  const mountedBetterSqlite3Root = `${mountedRuntimeRoot}/node_modules/better-sqlite3`;
  const localBetterSqlite3Root = `${localRuntimeRoot}/node_modules/better-sqlite3`;
  const localBinaryPath = `${localBetterSqlite3Root}/build/Release/better_sqlite3.node`;
  return [
    `rm -rf ${shellEscape("/tmp/rapidb-sqlite-runtime")}`,
    `mkdir -p ${shellEscape(localRuntimeRoot)}`,
    `cp -R ${shellEscape(`${mountedRuntimeRoot}/.`)} ${shellEscape(localRuntimeRoot)}`,
    `cd ${shellEscape(localRuntimeRoot)}`,
    `node -e ${shellEscape(
      `const fs = require('node:fs'); const filePath = 'node_modules/better-sqlite3/deps/common.gypi'; const source = fs.readFileSync(filePath, 'utf8'); fs.writeFileSync(filePath, source.replace(/'-O3'/g, "'-O1'").replace(/'GCC_OPTIMIZATION_LEVEL': '3'/g, "'GCC_OPTIMIZATION_LEVEL': '1'"));`,
    )}`,
    DOCKER_TARGETS[targetId].installCommand,
    "npm install --no-package-lock --ignore-scripts @electron/rebuild",
    `set +e; node -e ${shellEscape(
      `import('@electron/rebuild').then(async ({ rebuild }) => { await rebuild({ buildPath: process.cwd(), electronVersion: ${JSON.stringify(
        electronVersion,
      )}, arch: ${JSON.stringify(target.arch)}, onlyModules: ['better-sqlite3'], force: true, buildFromSource: true, headerURL: 'https://www.electronjs.org/headers' }); }).catch((error) => { console.error(error); process.exit(1); });`,
    )}; exit_code=$?; set -e`,
    `if [ -f ${shellEscape(localBinaryPath)} ]; then rm -rf ${shellEscape(`${mountedBetterSqlite3Root}/build`)} && mkdir -p ${shellEscape(mountedBetterSqlite3Root)} && cp -R ${shellEscape(`${localBetterSqlite3Root}/build`)} ${shellEscape(mountedBetterSqlite3Root)} && exit 0; fi`,
    `exit "$exit_code"`,
  ].join(" && ");
}

function dockerBuildTargetRuntime(
  stagedRuntimeRoot,
  targetId,
  electronVersion,
) {
  if (!dockerAvailable()) {
    throw new Error(
      `No Electron prebuild was available for ${targetId}, and Docker is not available for an emulated source build.`,
    );
  }

  const target = DOCKER_TARGETS[targetId];
  const workingDirectory = `/workspace/.rapidb-vscode/better-sqlite3/${targetId}`;
  const result = spawnSync(
    dockerCommand(),
    [
      "run",
      "--rm",
      "--platform",
      target.platform,
      "-v",
      `${repoRoot}:/workspace`,
      "-w",
      workingDirectory,
      target.image,
      "sh",
      "-lc",
      dockerTargetBuildScript(targetId, electronVersion),
    ],
    {
      stdio: "inherit",
    },
  );

  if (result.status !== 0 && !hasNativeBinary(stagedRuntimeRoot)) {
    throw new Error(`Docker fallback build failed for ${targetId}.`);
  }
}

function hasNativeBinary(stagedRuntimeRoot) {
  return existsSync(
    join(
      stagedRuntimeRoot,
      "node_modules",
      "better-sqlite3",
      "build",
      "Release",
      "better_sqlite3.node",
    ),
  );
}

function cleanupHelperPackage(packageRoot) {
  rmSync(join(packageRoot, "LICENSE"), { force: true });
  rmSync(join(packageRoot, "LICENSE.md"), { force: true });
  rmSync(join(packageRoot, "README.md"), { force: true });
  rmSync(join(packageRoot, "readme.md"), { force: true });
  rmSync(join(packageRoot, "CHANGELOG.md"), { force: true });
  rmSync(join(packageRoot, ".github"), { recursive: true, force: true });
  rmSync(join(packageRoot, "test"), { recursive: true, force: true });
  rmSync(join(packageRoot, "tests"), { recursive: true, force: true });
  rmSync(join(packageRoot, "example"), { recursive: true, force: true });
  rmSync(join(packageRoot, "examples"), { recursive: true, force: true });
}

function cleanupBetterSqlite3BuildArtifacts(betterSqlite3Root) {
  const buildRoot = join(betterSqlite3Root, "build");
  const releaseRoot = join(buildRoot, "Release");

  rmSync(join(buildRoot, "deps"), { recursive: true, force: true });
  rmSync(join(buildRoot, "node_gyp_bins"), { recursive: true, force: true });
  rmSync(join(buildRoot, "Makefile"), { force: true });
  rmSync(join(buildRoot, "binding.Makefile"), { force: true });
  rmSync(join(buildRoot, "better_sqlite3.target.mk"), { force: true });
  rmSync(join(buildRoot, "test_extension.target.mk"), { force: true });
  rmSync(join(buildRoot, "config.gypi"), { force: true });

  rmSync(join(releaseRoot, ".deps"), { recursive: true, force: true });
  rmSync(join(releaseRoot, ".forge-meta"), { recursive: true, force: true });
  rmSync(join(releaseRoot, "obj"), { recursive: true, force: true });
  rmSync(join(releaseRoot, "obj.target"), { recursive: true, force: true });
  rmSync(join(releaseRoot, "sqlite3.a"), { force: true });
  rmSync(join(releaseRoot, "test_extension.node"), { force: true });
}

function cleanupTargetRuntime(stagedRuntimeRoot) {
  const nodeModulesPath = join(stagedRuntimeRoot, "node_modules");
  if (!existsSync(nodeModulesPath)) {
    return;
  }

  rmSync(join(nodeModulesPath, ".bin"), { recursive: true, force: true });
  for (const entryName of readdirSync(nodeModulesPath)) {
    if (!RUNTIME_KEEP_PACKAGES.has(entryName)) {
      rmSync(join(nodeModulesPath, entryName), {
        recursive: true,
        force: true,
      });
    }
  }

  const betterSqlite3Root = join(nodeModulesPath, "better-sqlite3");
  rmSync(join(betterSqlite3Root, "deps"), { recursive: true, force: true });
  rmSync(join(betterSqlite3Root, "src"), { recursive: true, force: true });
  rmSync(join(betterSqlite3Root, "node_modules"), {
    recursive: true,
    force: true,
  });
  rmSync(join(betterSqlite3Root, "binding.gyp"), { force: true });
  rmSync(join(betterSqlite3Root, "README.md"), { force: true });
  rmSync(join(betterSqlite3Root, "LICENSE"), { force: true });
  cleanupBetterSqlite3BuildArtifacts(betterSqlite3Root);
  cleanupHelperPackage(join(nodeModulesPath, "bindings"));
  cleanupHelperPackage(join(nodeModulesPath, "file-uri-to-path"));
  rmSync(join(stagedRuntimeRoot, "package.json"), { force: true });
  rmSync(join(stagedRuntimeRoot, "package-lock.json"), { force: true });
}

function isHostTarget(targetId) {
  return targetId === detectHostRuntimeTarget();
}

function shouldPrepareNodeServerRuntimeTarget(targetId) {
  return NODE_SERVER_RUNTIME_TARGETS.has(targetId);
}

async function prepareTargetRuntime(
  targetId,
  electronVersion,
  betterSqlite3Version,
) {
  const stagedRuntimeRoot = stagedRuntimeRootFor(targetId);
  prepareStagingRoot(stagedRuntimeRoot);
  installStagedRuntime(stagedRuntimeRoot, betterSqlite3Version);

  if (!downloadElectronPrebuild(stagedRuntimeRoot, targetId, electronVersion)) {
    if (isHostTarget(targetId)) {
      await rebuildStagedRuntime(stagedRuntimeRoot, electronVersion, targetId);
    } else if (canUseDockerFallback(targetId)) {
      dockerBuildTargetRuntime(stagedRuntimeRoot, targetId, electronVersion);
    } else {
      throw new Error(
        `No Electron prebuild was available for ${targetId}. Build that target on a matching host.`,
      );
    }
  }

  if (!hasNativeBinary(stagedRuntimeRoot)) {
    throw new Error(
      `The staged SQLite runtime for ${targetId} does not contain better_sqlite3.node after preparation.`,
    );
  }

  cleanupTargetRuntime(stagedRuntimeRoot);
  log(`Staged SQLite runtime is ready at ${stagedRuntimeRoot}.`);
}

/**
 * Prepares the Node.js staged runtime for VS Code Server.
 * Stored in .rapidb-vscode/better-sqlite3-node/<targetId>/.
 *
 * This step is BEST-EFFORT: a failure here is logged but never propagates to
 * the caller, so it cannot block `vsce package` or `native:sqlite:vscode:all`.
 * Desktop VS Code (Electron) is unaffected — it uses the separate Electron staged
 * runtime. VS Code Server users on a platform without a binary will see a clear
 * error at runtime rather than a broken build.
 */
async function prepareNodeTargetRuntime(targetId, betterSqlite3Version) {
  if (!shouldPrepareNodeServerRuntimeTarget(targetId)) {
    log(
      `Skipping Node.js staged runtime for ${targetId}: only Linux targets are required for VS Code Server packaging.`,
    );
    return;
  }

  const stagedRuntimeRoot = stagedNodeRuntimeRootFor(targetId);

  try {
    prepareStagingRoot(stagedRuntimeRoot);
    installStagedRuntime(stagedRuntimeRoot, betterSqlite3Version);
  } catch (error) {
    log(
      `[best-effort] Could not set up staging directory for Node.js runtime on ${targetId}: ${error instanceof Error ? error.message : error}`,
    );
    return;
  }

  if (
    !downloadNodePrebuild(
      stagedRuntimeRoot,
      targetId,
      VSCODE_SERVER_NODE_VERSION,
    )
  ) {
    fail(
      `[best-effort] Could not download Node.js prebuild for ${targetId} (Node ${VSCODE_SERVER_NODE_VERSION}).`,
    );
    return;
  }

  if (!hasNativeBinary(stagedRuntimeRoot)) {
    fail(
      `[best-effort] Node.js staged runtime for ${targetId} has no native binary after preparation.`,
    );
    return;
  }

  // Validate MODULE_VERSION for VS Code Server Node.js runtime.
  // This prevents mismatched binaries from shipping in the VSIX.
  const binaryPath = join(
    stagedRuntimeRoot,
    "node_modules",
    "better-sqlite3",
    "build",
    "Release",
    "better_sqlite3.node",
  );
  const versionCheck = validateBinaryModuleVersion(
    binaryPath,
    VSCODE_SERVER_MODULE_VERSION,
    VSCODE_SERVER_NODE_VERSION,
  );
  if (!versionCheck.isValid) {
    fail(`Binary validation failed for ${targetId}: ${versionCheck.reason}.`);
    return;
  }
  log(
    `✓ Binary MODULE_VERSION validation passed for ${targetId}: ${versionCheck.reason}`,
  );

  cleanupTargetRuntime(stagedRuntimeRoot);
  log(
    `Staged Node.js SQLite runtime (VS Code Server) ready at ${stagedRuntimeRoot}.`,
  );
}

async function main() {
  pruneUnsupportedStagedRuntimes();

  const electronVersion = detectElectronVersion();
  if (!electronVersion) {
    fail(
      'Could not detect the local VS Code Electron version. Set RAPIDB_VSCODE_ELECTRON_VERSION and rerun "npm run native:sqlite:vscode".',
    );
    return;
  }

  const betterSqlite3Version = installedBetterSqlite3Version();
  const targetIds = parseTargetIds();
  for (const targetId of targetIds) {
    log(
      `Preparing staged better-sqlite3 ${betterSqlite3Version} for VS Code Electron ${electronVersion} (${targetId}).`,
    );
    await prepareTargetRuntime(targetId, electronVersion, betterSqlite3Version);
  }

  // Also prepare Node.js binaries required by VS Code Server support.
  for (const targetId of targetIds) {
    log(
      `Preparing staged better-sqlite3 ${betterSqlite3Version} for VS Code Server / Node.js (${targetId}).`,
    );
    await prepareNodeTargetRuntime(targetId, betterSqlite3Version);
  }
}

try {
  await main();
} catch (error) {
  const message = error instanceof Error ? error.message : String(error);
  if (isStrict) {
    console.error(`[RapiDB SQLite] ${message}`);
    process.exit(1);
  }
  log(message);
}
