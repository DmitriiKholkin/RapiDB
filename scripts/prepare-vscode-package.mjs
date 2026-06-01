import { cpSync, existsSync, mkdirSync, readdirSync, rmSync } from "node:fs";
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

function copyIfExists(sourcePath, targetPath) {
  if (!existsSync(sourcePath)) {
    return;
  }
  mkdirSync(path.dirname(targetPath), { recursive: true });
  cpSync(sourcePath, targetPath, {
    recursive: true,
    force: true,
  });
}

function copyPackageFiles(
  sourceNodeModules,
  runtimeNodeModules,
  packageName,
  files,
) {
  for (const relativePath of files) {
    copyIfExists(
      path.join(sourceNodeModules, packageName, relativePath),
      path.join(runtimeNodeModules, packageName, relativePath),
    );
  }
}

function prepareSqliteRuntimeScaffold(workspaceRoot) {
  const sourceNodeModules = path.join(workspaceRoot, "node_modules");
  const runtimeRoot = path.join(workspaceRoot, ".rapidb-runtime");
  const runtimeNodeModules = path.join(runtimeRoot, "node_modules");
  removeIfExists(runtimeRoot);
  mkdirSync(runtimeNodeModules, { recursive: true });

  copyPackageFiles(sourceNodeModules, runtimeNodeModules, "better-sqlite3", [
    "package.json",
    "LICENSE",
    "lib",
  ]);
  copyPackageFiles(sourceNodeModules, runtimeNodeModules, "bindings", [
    "package.json",
    "bindings.js",
    "LICENSE.md",
  ]);
  copyPackageFiles(sourceNodeModules, runtimeNodeModules, "file-uri-to-path", [
    "package.json",
    "index.js",
    "LICENSE",
  ]);
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

const workspaceRoot = process.cwd();

removeIfExists(path.join(workspaceRoot, ".rapidb-vscode"));
prepareSqliteRuntimeScaffold(workspaceRoot);
pruneOracleRuntime(path.join(workspaceRoot, "node_modules", "oracledb"));

console.log(
  "[RapiDB Package] Prepared the packaged SQLite scaffold, cleaned staged SQLite artifacts, and pruned Oracle thick-mode packaging leftovers.",
);
