import { existsSync, readdirSync, rmSync } from "node:fs";
import path from "node:path";

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
  ]) {
    removeIfExists(path.join(oracledbRoot, relativePath));
  }

  removeMatchingFiles(
    oracledbRoot,
    (fileName) =>
      fileName === ".DS_Store" || fileName.endsWith("-buildinfo.txt"),
  );
}

const workspaceRoot = process.cwd();

removeIfExists(path.join(workspaceRoot, ".rapidb-vscode"));
pruneOracleRuntime(path.join(workspaceRoot, "node_modules", "oracledb"));

console.log(
  "[RapiDB Package] Cleaned staged SQLite artifacts and pruned oracledb packaging leftovers.",
);
