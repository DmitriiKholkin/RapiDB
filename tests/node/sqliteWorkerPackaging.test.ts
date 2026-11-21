import { fork } from "node:child_process";
import { copyFile, mkdir, readFile, writeFile } from "node:fs/promises";
import { isBuiltin } from "node:module";
import { dirname, join, resolve } from "node:path";
import { listFiles, PackageManager } from "@vscode/vsce";
import { build } from "esbuild";
import { describe, expect, it } from "vitest";
import { sqliteWorkerConfig } from "../../esbuild.config.mjs";
import type {
  SQLiteWorkerRequest,
  SQLiteWorkerResponse,
} from "../../src/extension/dbDrivers/sqliteWorkerProtocol";
import { SQLITE_ARTIFACTS } from "../../src/extension/utils/sqliteArtifactPins";
import { sqliteNativeBindingPath } from "../../src/extension/utils/sqliteInstaller";
import { resolveSQLiteNativeTarget } from "../../src/extension/utils/sqliteNativeTarget";
import { createProjectTempDir } from "../runtime/tempDirectories";

describe("SQLite packaged execution process", () => {
  it("ships a node20 CJS worker without SQLite files and runs it on a managed runtime outside the package", async () => {
    const root = await createProjectTempDir("sqlite-worker-package");
    const dist = join(root, "dist");
    await mkdir(dist);
    const workerFile = join(dist, "sqliteWorker.js");
    const result = await build({
      ...sqliteWorkerConfig,
      outfile: workerFile,
      minify: true,
      sourcemap: false,
      metafile: true,
    });
    expect(sqliteWorkerConfig.target).toBe("node20");
    expect(sqliteWorkerConfig.format).toBe("cjs");
    if (!result.metafile) throw new Error("Missing worker build metadata");
    const imports = Object.values(result.metafile.outputs).flatMap((output) =>
      output.imports.map((entry) => entry.path),
    );
    expect(imports.filter((path) => !isBuiltin(path))).toEqual([]);
    const bundle = await readFile(workerFile, "utf8");
    expect(bundle).not.toMatch(/require\(["'](?:tsx|typescript|vscode)["']\)/);

    // Lay out the managed runtime exactly as the installer does: reviewed
    // JavaScript files plus one prebuild as build/Release/better_sqlite3.node.
    const source = dirname(require.resolve("better-sqlite3/package.json"));
    const sqlitePackage = join(
      root,
      "global-storage",
      "node_modules",
      "better-sqlite3",
    );
    for (const path of Object.keys(SQLITE_ARTIFACTS.files)) {
      await mkdir(dirname(join(sqlitePackage, path)), { recursive: true });
      await copyFile(join(source, path), join(sqlitePackage, path));
    }
    await mkdir(dirname(sqliteNativeBindingPath(sqlitePackage)), {
      recursive: true,
    });
    await copyFile(
      join(source, "prebuilds", `${resolveSQLiteNativeTarget()}.node`),
      sqliteNativeBindingPath(sqlitePackage),
    );
    const ignore = await readFile(resolve(".vscodeignore"), "utf8");
    await writeFile(join(root, ".vscodeignore"), ignore);
    await writeFile(
      join(root, "package.json"),
      JSON.stringify({
        name: "sqlite-worker-fixture",
        publisher: "rapidb",
        version: "1.0.0",
        engines: { vscode: "^1.101.0" },
        main: "./dist/sqliteWorker.js",
        activationEvents: ["onStartupFinished"],
      }),
    );
    const files = await listFiles({
      cwd: root,
      packageManager: PackageManager.None,
    });
    expect(files).toContain("dist/sqliteWorker.js");
    expect(files.filter((file) => /sqlite3|\.node$/.test(file))).toEqual([]);
    expect(
      files.filter((file) => /\.tsx?$/.test(file) && !file.endsWith(".d.ts")),
    ).toEqual([]);

    const child = fork(workerFile, [], {
      execArgv: [],
      env: {
        ...process.env,
        NODE_ENV: "production",
        ELECTRON_RUN_AS_NODE: "1",
        NODE_PATH: "",
      },
      serialization: "advanced",
      stdio: ["ignore", "ignore", "ignore", "ipc"],
    });
    const request = (input: SQLiteWorkerRequest) =>
      new Promise<SQLiteWorkerResponse>((resolve, reject) => {
        const timer = setTimeout(
          () => reject(new Error("Packaged worker did not respond")),
          5000,
        );
        const message = (response: SQLiteWorkerResponse) => {
          if (response.id !== input.id) return;
          clearTimeout(timer);
          child.off("message", message);
          resolve(response);
        };
        child.on("message", message);
        child.send(input);
      });
    try {
      const deadline = Date.now() + 10000;
      expect(
        await request({
          id: 1,
          method: "connect",
          args: [],
          deadline,
          config: {
            id: "package",
            name: "Package",
            type: "sqlite",
            filePath: ":memory:",
          },
          runtimeTargets: [sqlitePackage],
          managedRuntimePackage: sqlitePackage,
        }),
      ).not.toHaveProperty("error");
      const query = await request({
        id: 2,
        method: "query",
        args: [
          "CREATE TABLE data(value INTEGER); INSERT INTO data VALUES(42); SELECT value FROM data",
        ],
        deadline,
      });
      expect(query.error).toBeUndefined();
      expect(query.value).toMatchObject({ rows: [{ __col_0: 42 }] });
      expect(
        await request({
          id: 3,
          method: "describeColumns",
          args: ["main", "main", "data"],
          deadline,
        }),
      ).toMatchObject({
        value: [
          expect.objectContaining({ name: "value", category: "integer" }),
        ],
      });
    } finally {
      const exit = new Promise<void>((resolve) =>
        child.once("exit", () => resolve()),
      );
      child.kill("SIGKILL");
      await exit;
    }
  }, 20000);
});
