import {
  existsSync,
  lstatSync,
  readdirSync,
  realpathSync,
  rmSync,
} from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

function assertDirectoryOrMissing(targetPath) {
  const entry = lstatSync(targetPath, { throwIfNoEntry: false });
  if (entry && !entry.isDirectory()) {
    throw new Error(
      `[RapiDB Package] Expected a real directory: ${targetPath}`,
    );
  }
}

function removeIfExists(targetPath) {
  if (!existsSync(targetPath)) {
    return;
  }
  rmSync(targetPath, {
    recursive: true,
    force: true,
    maxRetries: 5,
    retryDelay: 200,
  });
}

function removeMatchingFiles(directoryPath, matcher) {
  if (!existsSync(directoryPath)) {
    return;
  }

  for (const entry of readdirSync(directoryPath, { withFileTypes: true })) {
    const entryPath = path.join(directoryPath, entry.name);
    if (entry.isDirectory()) {
      removeMatchingFiles(entryPath, matcher);
      continue;
    }
    if (matcher(entry.name, entryPath)) {
      rmSync(entryPath, { force: true });
    }
  }
}

function pruneOracleRuntime(oracledbRoot) {
  if (!existsSync(oracledbRoot)) {
    return;
  }

  for (const relativePath of [
    ".DS_Store",
    "CHANGELOG.md",
    "README.md",
    "SECURITY.md",
    "examples",
    "package",
    "build",
  ]) {
    removeIfExists(path.join(oracledbRoot, relativePath));
  }

  removeMatchingFiles(
    oracledbRoot,
    (fileName) =>
      fileName === ".DS_Store" || fileName.endsWith("-buildinfo.txt"),
  );
}

const workspaceRoot = path.dirname(
  path.dirname(realpathSync(fileURLToPath(import.meta.url))),
);
if (realpathSync(process.cwd()) !== workspaceRoot) {
  throw new Error(
    `[RapiDB Package] Run the preparation script from the RapiDB workspace root: ${workspaceRoot}`,
  );
}

const stagingRoot = path.join(workspaceRoot, ".rapidb-vscode");
// Former packaged better-sqlite3 scaffold. The SQLite runtime is now
// downloaded on demand from the npm registry, so no part of it is packaged.
const runtimeRoot = path.join(workspaceRoot, ".rapidb-runtime");
const sourceNodeModules = path.join(workspaceRoot, "node_modules");
const oracledbRoot = path.join(sourceNodeModules, "oracledb");
// Check parent paths first so pruning cannot traverse a linked dependency tree.
for (const directory of [
  stagingRoot,
  runtimeRoot,
  sourceNodeModules,
  oracledbRoot,
]) {
  assertDirectoryOrMissing(directory);
}

removeIfExists(stagingRoot);
removeIfExists(runtimeRoot);
pruneOracleRuntime(oracledbRoot);

console.log(
  "[RapiDB Package] Removed staged SQLite artifacts and pruned Oracle thick-mode packaging leftovers.",
);
