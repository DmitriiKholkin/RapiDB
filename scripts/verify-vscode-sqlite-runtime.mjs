import { existsSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { validateBinaryModuleVersion } from "./utils/extractModuleVersion.mjs";

const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");

const SUPPORTED_TARGETS = [
  "win32-x64",
  "win32-arm64",
  "linux-x64",
  "linux-arm64",
  "darwin-x64",
  "darwin-arm64",
];

const NODE_SERVER_RUNTIME_TARGETS = new Set([
  "win32-x64",
  "win32-arm64",
  "linux-x64",
  "linux-arm64",
  "darwin-x64",
  "darwin-arm64",
]);

function parseTargetIds() {
  const explicitTargets = new Set();
  const args = process.argv.slice(2);
  for (let index = 0; index < args.length; index += 1) {
    const argument = args[index];
    if (argument !== "--target") {
      throw new Error(
        `Unexpected argument: ${argument}. Supported arguments: --target <target-id>.`,
      );
    }

    const targetId = args[index + 1];
    if (targetId?.startsWith("--")) {
      throw new Error(
        `Unsupported SQLite runtime target: ${targetId}. Pass --target <target-id>.`,
      );
    }
    if (!targetId || !SUPPORTED_TARGETS.includes(targetId)) {
      throw new Error(
        `Unsupported SQLite runtime target: ${targetId ?? "<missing>"}.`,
      );
    }
    explicitTargets.add(targetId);
    index += 1;
  }

  return explicitTargets.size > 0
    ? Array.from(explicitTargets)
    : SUPPORTED_TARGETS;
}

function stagedRuntimeRootFor(targetId) {
  return join(repoRoot, ".rapidb-vscode", "better-sqlite3", targetId);
}

function stagedNodeRuntimeRootFor(targetId) {
  return join(repoRoot, ".rapidb-vscode", "better-sqlite3-node", targetId);
}

function nodeRuntimeTargetsForVerification(targetIds) {
  return targetIds.filter((targetId) =>
    NODE_SERVER_RUNTIME_TARGETS.has(targetId),
  );
}

function checkTarget(targetId, isNodeRuntime = false) {
  const stagedRuntimeRoot = isNodeRuntime
    ? stagedNodeRuntimeRootFor(targetId)
    : stagedRuntimeRootFor(targetId);
  const packageJsonPath = join(
    stagedRuntimeRoot,
    "node_modules",
    "better-sqlite3",
    "package.json",
  );
  const nativeBinaryPath = join(
    stagedRuntimeRoot,
    "node_modules",
    "better-sqlite3",
    "build",
    "Release",
    "better_sqlite3.node",
  );
  const bindingsPackageJsonPath = join(
    stagedRuntimeRoot,
    "node_modules",
    "bindings",
    "package.json",
  );
  const fileUriToPathPackageJsonPath = join(
    stagedRuntimeRoot,
    "node_modules",
    "file-uri-to-path",
    "package.json",
  );

  const checks = [
    { name: "better-sqlite3 package", path: packageJsonPath },
    { name: "native better-sqlite3 binary", path: nativeBinaryPath },
    { name: "bindings helper package", path: bindingsPackageJsonPath },
    {
      name: "file-uri-to-path helper package",
      path: fileUriToPathPackageJsonPath,
    },
  ].map((entry) => ({
    ...entry,
    exists: existsSync(entry.path),
  }));

  return {
    targetId,
    stagedRuntimeRoot,
    checks,
    ok: checks.every((entry) => entry.exists),
  };
}

function main() {
  const targetIds = parseTargetIds();
  const nodeTargetIds = nodeRuntimeTargetsForVerification(targetIds);
  const failures = [];
  const versionCheckDetails = [];

  // Check Electron staged runtimes
  for (const targetId of targetIds) {
    const result = checkTarget(targetId, false);
    if (!result.ok) {
      failures.push({ ...result, kind: "Electron" });
    }
  }

  // Check Node.js staged runtimes (for VS Code Server)
  for (const targetId of nodeTargetIds) {
    const result = checkTarget(targetId, true);
    if (!result.ok) {
      failures.push({ ...result, kind: "Node.js (VS Code Server)" });
    } else {
      // Additional validation: check MODULE_VERSION for Node.js runtime
      const nativeBinaryPath = join(
        stagedNodeRuntimeRootFor(targetId),
        "node_modules",
        "better-sqlite3",
        "build",
        "Release",
        "better_sqlite3.node",
      );
      if (existsSync(nativeBinaryPath)) {
        const versionCheck = validateBinaryModuleVersion(
          nativeBinaryPath,
          115,
          "20.x",
        );
        if (versionCheck.isValid) {
          versionCheckDetails.push(
            `  ✓ ${targetId} (Node.js): ${versionCheck.reason}`,
          );
        } else {
          failures.push({
            targetId,
            kind: "Node.js (VS Code Server)",
            stagedRuntimeRoot: stagedNodeRuntimeRootFor(targetId),
            checks: [],
            ok: false,
            versionCheckError: versionCheck.reason,
          });
          versionCheckDetails.push(
            `  ✗ ${targetId} (Node.js): ${versionCheck.reason}`,
          );
        }
      }
    }
  }

  if (failures.length === 0) {
    const versionDetails =
      versionCheckDetails.length > 0
        ? `\nBinary MODULE_VERSION validation:\n${versionCheckDetails.join("\n")}`
        : "";
    console.log(
      `[RapiDB SQLite Verify] Verified Electron + Node.js staged runtimes for ${targetIds.length} target(s): ${targetIds.join(", ")}.${versionDetails}`,
    );
    return;
  }

  const details = failures
    .map((failure) => {
      if (failure.versionCheckError) {
        return [
          `  - target ${failure.targetId} [${failure.kind ?? "Electron"}]`,
          `    staged root: ${failure.stagedRuntimeRoot}`,
          `    error: ${failure.versionCheckError}`,
        ]
          .filter((part) => part.length > 0)
          .join("\n");
      }

      const missing = failure.checks
        .filter((entry) => !entry.exists)
        .map((entry) => `    - missing ${entry.name}: ${entry.path}`)
        .join("\n");
      const present = failure.checks
        .filter((entry) => entry.exists)
        .map((entry) => `    - present ${entry.name}: ${entry.path}`)
        .join("\n");
      return [
        `  - target ${failure.targetId} [${failure.kind ?? "Electron"}]`,
        `    staged root: ${failure.stagedRuntimeRoot}`,
        missing,
        present,
      ]
        .filter((part) => part.length > 0)
        .join("\n");
    })
    .join("\n");

  throw new Error(
    [
      "[RapiDB SQLite Verify] Staged VS Code SQLite runtimes are incomplete or invalid.",
      `Checked Electron targets: ${targetIds.join(", ")}`,
      `Checked Node.js (VS Code Server) targets: ${nodeTargetIds.join(", ") || "<none>"}`,
      details,
      'Run "npm run native:sqlite:vscode:all" and re-run this verifier before packaging.',
    ].join("\n"),
  );
}

main();
