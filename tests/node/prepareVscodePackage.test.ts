import { spawnSync } from "node:child_process";
import { existsSync } from "node:fs";
import {
  copyFile,
  lstat,
  mkdir,
  readFile,
  symlink,
  writeFile,
} from "node:fs/promises";
import { dirname, join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import {
  cleanupTempDirectories,
  createProjectTempDir,
} from "../runtime/tempDirectories";

const scriptSource = new URL(
  "../../scripts/prepare-vscode-package.mjs",
  import.meta.url,
);
const stagedFile = ".rapidb-vscode/stale.txt";
const runtimeFile = ".rapidb-runtime/stale.txt";
const oracleReadme = "node_modules/oracledb/README.md";
const sentinel = "fixture contents";

async function put(root: string, file: string, contents = sentinel) {
  const target = join(root, file);
  await mkdir(dirname(target), { recursive: true });
  await writeFile(target, contents);
}

async function fixture() {
  const root = await createProjectTempDir("prepare-vscode-package");
  const workspace = join(root, "workspace");
  const script = join(workspace, "scripts", "prepare-vscode-package.mjs");
  await mkdir(dirname(script), { recursive: true });
  await copyFile(scriptSource, script);
  return { root, workspace, script };
}

function prepare(script: string, cwd: string) {
  const result = spawnSync(process.execPath, [script], {
    cwd,
    encoding: "utf8",
    timeout: 10_000,
  });
  expect(result.error).toBeUndefined();
  expect(result.signal).toBeNull();
  return result;
}

afterEach(cleanupTempDirectories);

describe("VS Code package preparation", () => {
  it("rejects a different working directory without changing either directory", async () => {
    const { root, workspace, script } = await fixture();
    const foreign = join(root, "unrelated");
    for (const directory of [workspace, foreign]) {
      for (const file of [stagedFile, runtimeFile, oracleReadme]) {
        await put(directory, file);
      }
    }

    const result = prepare(script, foreign);

    expect(result.status).toBe(1);
    expect(result.stderr).toContain("RapiDB workspace root");
    for (const directory of [workspace, foreign]) {
      for (const file of [stagedFile, runtimeFile, oracleReadme]) {
        expect(await readFile(join(directory, file), "utf8")).toBe(sentinel);
      }
    }
  });

  it("removes the former SQLite scaffold and prunes only the intended Oracle artifacts", async () => {
    const { root, workspace, script } = await fixture();
    const unwanted = [
      ".rapidb-runtime/node_modules/better-sqlite3/lib/index.js",
      ".rapidb-runtime/node_modules/better-sqlite3/build/Release/better_sqlite3.node",
      stagedFile,
      runtimeFile,
      oracleReadme,
      "node_modules/oracledb/build/Release/oracledb.node",
      "node_modules/oracledb/lib/.DS_Store",
      "node_modules/oracledb/lib/native-buildinfo.txt",
    ];
    const kept = [
      "node_modules/better-sqlite3/lib/index.js",
      "node_modules/better-sqlite3/prebuilds/darwin-arm64.node",
      "node_modules/oracledb/lib/thin/index.js",
      "node_modules/unrelated/README.md",
    ];
    for (const file of [...unwanted, ...kept]) {
      await put(workspace, file);
    }
    const foreign = join(root, "unrelated");
    const foreignFile = "outside-buildinfo.txt";
    await put(foreign, foreignFile);
    await symlink(
      foreign,
      join(workspace, "node_modules/oracledb/lib/linked"),
      "junction",
    );

    const result = prepare(script, workspace);

    expect(result.stderr).toBe("");
    expect(result.status).toBe(0);
    for (const file of unwanted) {
      expect(existsSync(join(workspace, file))).toBe(false);
    }
    for (const file of kept) {
      expect(await readFile(join(workspace, file), "utf8")).toBe(sentinel);
    }
    expect(existsSync(join(workspace, ".rapidb-runtime"))).toBe(false);
    expect(await readFile(join(foreign, foreignFile), "utf8")).toBe(sentinel);
  });

  it.each([
    ".rapidb-vscode",
    ".rapidb-runtime",
    "node_modules",
    "node_modules/oracledb",
  ])("refuses to prepare through a linked %s", async (linkedPath) => {
    const { root, workspace, script } = await fixture();
    const foreign = join(root, "unrelated");
    const foreignFile =
      linkedPath === "node_modules" ? "oracledb/README.md" : "README.md";
    const link = join(workspace, linkedPath);
    const untouched =
      linkedPath === ".rapidb-vscode" ? runtimeFile : stagedFile;
    await put(workspace, untouched);
    await put(foreign, foreignFile);
    await mkdir(dirname(link), { recursive: true });
    await symlink(foreign, link, "junction");

    const result = prepare(script, workspace);

    expect(result.status).toBe(1);
    expect(result.stderr).toContain("Expected a real directory");
    expect((await lstat(link)).isSymbolicLink()).toBe(true);
    expect(await readFile(join(workspace, untouched), "utf8")).toBe(sentinel);
    expect(await readFile(join(foreign, foreignFile), "utf8")).toBe(sentinel);
  });
});
