import { randomUUID } from "node:crypto";
import {
  existsSync,
  mkdirSync,
  readdirSync,
  readFileSync,
  renameSync,
  rmSync,
  statSync,
  symlinkSync,
  utimesSync,
  writeFileSync,
} from "node:fs";
import { hostname } from "node:os";
import { basename, dirname, join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  SQLITE_ARTIFACTS,
  type SQLiteArtifactManifest,
  type SQLiteArtifactTarget,
  sqliteArtifactPolicyId,
} from "../../src/extension/utils/sqliteArtifactPins";
import {
  configureSQLiteInstaller,
  ensureSQLiteRuntimeInstalled,
  probeInstalledBetterSqlite3Runtime,
  removeObsoleteSQLiteRuntimes,
  resetSQLiteInstallerForTests,
  setSQLiteInstallLockTimingsForTests,
} from "../../src/extension/utils/sqliteInstaller";
import { createProjectTempDir } from "../runtime/tempDirectories";

vi.mock("node:fs", async (importOriginal) => {
  const actual = await importOriginal<typeof import("node:fs")>();
  return { ...actual, renameSync: vi.fn(actual.renameSync) };
});

afterEach(() => {
  vi.mocked(renameSync).mockReset();
  resetSQLiteInstallerForTests();
});

const DAY_MS = 24 * 60 * 60 * 1000;
const POLICY = "pins-v5-0123456789abcdef";

function manifestCopy(): SQLiteArtifactManifest {
  return JSON.parse(JSON.stringify(SQLITE_ARTIFACTS));
}

function target(overrides: Partial<SQLiteArtifactTarget> = {}) {
  return {
    betterSqlite3Version: "12.10.0",
    runtime: "electron",
    abi: "146",
    platform: "darwin",
    arch: "arm64",
    libc: "",
    electronVersion: "42.0.0",
    ...overrides,
  } satisfies SQLiteArtifactTarget;
}

function pin(url: string) {
  return {
    url,
    integrity: `sha512-${url}`,
    size: 1,
    releaseId: 1,
    assetId: 1,
    apiDigest: "sha256:0",
  };
}

describe("SQLite runtime cache identity", () => {
  it("ignores pins for unrelated platforms, ABIs and Electron versions", () => {
    const manifest = manifestCopy();
    const before = sqliteArtifactPolicyId(target(), manifest);
    manifest.prebuilts[
      "official/better-sqlite3-v12.10.0-electron-v146-linux-x64.tar.gz"
    ] = pin("changed-linux");
    manifest.prebuilts[
      "patched/better-sqlite3-v12.10.0-electron-v146-win32-arm64.tar.gz"
    ] = pin("changed-windows");
    manifest.prebuilts[
      "official/better-sqlite3-v12.10.0-node-v146-darwin-arm64.tar.gz"
    ] = pin("changed-node");
    manifest.headers["43-148"].archive.integrity = "sha512-changed";
    manifest.headers["42-146"].windowsLibraries.x64 = pin("changed-lib");
    expect(sqliteArtifactPolicyId(target(), manifest)).toBe(before);
    expect(before).toMatch(/^pins-v5-[0-9a-f]{16}$/);

    const node = target({ runtime: "node", electronVersion: undefined });
    const nodeBefore = sqliteArtifactPolicyId(node, manifest);
    manifest.headers["42-146"].archive.integrity = "sha512-changed";
    manifest.prebuilts[
      "patched/better-sqlite3-v12.10.0-electron-v146-darwin-arm64.tar.gz"
    ] = pin("changed-patched");
    expect(sqliteArtifactPolicyId(node, manifest)).toBe(nodeBefore);
  });

  it.each([
    [
      "official prebuilt",
      (manifest: SQLiteArtifactManifest) => {
        manifest.prebuilts[
          "official/better-sqlite3-v12.10.0-electron-v146-darwin-arm64.tar.gz"
        ] = pin("official");
      },
    ],
    [
      "patched prebuilt",
      (manifest: SQLiteArtifactManifest) => {
        manifest.prebuilts[
          "patched/better-sqlite3-v12.10.0-electron-v146-darwin-arm64.tar.gz"
        ] = pin("patched");
      },
    ],
    [
      "header archive",
      (manifest: SQLiteArtifactManifest) => {
        manifest.headers["42-146"].archive.integrity = "sha512-changed";
      },
    ],
    [
      "schema version",
      (manifest: SQLiteArtifactManifest) => {
        manifest.schemaVersion += 1;
      },
    ],
  ])("changes when the target's %s pin changes", (_name, mutate) => {
    const manifest = manifestCopy();
    const before = sqliteArtifactPolicyId(target(), manifest);
    mutate(manifest);
    expect(sqliteArtifactPolicyId(target(), manifest)).not.toBe(before);
  });

  it("scopes Windows import libraries to the current architecture", () => {
    const windows = target({ platform: "win32", arch: "x64" });
    const manifest = manifestCopy();
    const before = sqliteArtifactPolicyId(windows, manifest);
    manifest.headers["42-146"].windowsLibraries.arm64 = pin("arm64");
    expect(sqliteArtifactPolicyId(windows, manifest)).toBe(before);
    manifest.headers["42-146"].windowsLibraries.x64 = pin("x64");
    expect(sqliteArtifactPolicyId(windows, manifest)).not.toBe(before);
  });

  it("keys the managed runtime directory by the scoped policy", async () => {
    const f = await fixture();
    expect(basename(f.currentRoot)).toBe(
      `node-abi-${process.versions.modules}-${process.platform}-${process.arch}-${sqliteArtifactPolicyId(
        target({
          runtime: process.versions.electron ? "electron" : "node",
          abi: process.versions.modules,
          platform: process.platform,
          arch: process.arch,
          electronVersion: process.versions.electron,
        }),
      )}`,
    );
  });
});

async function fixture() {
  const root = await createProjectTempDir("sqlite-runtime-cache");
  writeFileSync(join(root, "package.json"), "{}");
  for (const name of ["better-sqlite3", "bindings", "file-uri-to-path"]) {
    const packagePath = join(root, "node_modules", name);
    mkdirSync(packagePath, { recursive: true });
    writeFileSync(
      join(packagePath, "package.json"),
      JSON.stringify({ name, version: "12.10.0" }),
    );
  }
  const logs: string[] = [];
  configureSQLiteInstaller({
    storageRoot: join(root, "storage"),
    allowInstall: () => true,
    log: (line) => logs.push(line),
  });
  const baseDir = join(root, "dist");
  const probe = probeInstalledBetterSqlite3Runtime(baseDir);
  const currentRoot = dirname(dirname(probe.installedPackagePath as string));
  const cacheRoot = dirname(dirname(currentRoot));
  return { root, logs, baseDir, currentRoot, cacheRoot };
}

function runtime(
  path: string,
  idleDays: number | null,
  manifest: object = { betterSqlite3Version: "0.0.0" },
) {
  mkdirSync(path, { recursive: true });
  writeFileSync(join(path, "runtime.json"), JSON.stringify(manifest));
  if (idleDays !== null) {
    const marker = join(path, ".rapidb-last-used");
    writeFileSync(marker, "");
    const time = new Date(Date.now() - idleDays * DAY_MS);
    utimesSync(marker, time, time);
  }
  return path;
}

async function failObsoleteRenames(error: Error) {
  const actual = (await vi.importActual<typeof import("node:fs")>("node:fs"))
    .renameSync;
  vi.mocked(renameSync).mockImplementation((from, to) => {
    if (String(to).includes(".obsolete-")) throw error;
    actual(from, to);
  });
}

function markerAge(path: string) {
  return Date.now() - statSync(join(path, ".rapidb-last-used")).mtimeMs;
}

function obsoleteName(abi: number, policy = POLICY) {
  return `node-abi-${abi}-${process.platform}-${process.arch}-${policy}`;
}

function currentRuntime(root: string) {
  const path = runtime(root, 90, {
    betterSqlite3Version: "12.10.0",
    runtime: process.versions.electron ? "electron" : "node",
    abi: process.versions.modules,
    platform: process.platform,
    arch: process.arch,
    libc: "",
  });
  const packagePath = join(path, "node_modules", "better-sqlite3");
  mkdirSync(join(packagePath, "build", "Release"), { recursive: true });
  writeFileSync(join(packagePath, "package.json"), "{}");
  writeFileSync(
    join(packagePath, "build", "Release", "better_sqlite3.node"),
    "",
  );
  return packagePath;
}

function setMarkerAge(runtimeRoot: string, days: number) {
  const time = new Date(Date.now() - days * DAY_MS);
  utimesSync(join(runtimeRoot, ".rapidb-last-used"), time, time);
}

function otherVersionBaseDir(root: string) {
  const packagePath = join(root, "other", "node_modules", "better-sqlite3");
  mkdirSync(packagePath, { recursive: true });
  writeFileSync(join(root, "other", "package.json"), "{}");
  writeFileSync(
    join(packagePath, "package.json"),
    JSON.stringify({ name: "better-sqlite3", version: "12.11.0" }),
  );
  return join(root, "other", "dist");
}

function liveLock(runtimeRoot: string, token: string) {
  const owner = {
    pid: process.pid,
    hostname: hostname(),
    createdAt: Date.now(),
    token,
  };
  mkdirSync(`${runtimeRoot}.lock`, { recursive: true });
  writeFileSync(
    join(`${runtimeRoot}.lock`, "owner.json"),
    JSON.stringify(owner),
  );
  return owner;
}

describe("SQLite obsolete runtime cleanup", () => {
  it("removes only long-unused, unlocked runtimes created by the installer", async () => {
    const f = await fixture();
    const version = join(f.cacheRoot, "12.10.0");
    const current = runtime(f.currentRoot, 90);
    const old = runtime(join(version, obsoleteName(1)), 31);
    const oldOtherVersion = runtime(
      join(
        f.cacheRoot,
        "12.9.0",
        obsoleteName(130, "pins-v6-0123456789abcdef"),
      ),
      400,
    );
    const trash = join(
      version,
      `${obsoleteName(2)}.obsolete-1-${randomUUID()}`,
    );
    mkdirSync(join(trash, "node_modules"), { recursive: true });
    const lockedTrash = join(
      version,
      `${obsoleteName(9)}.obsolete-1-${randomUUID()}`,
    );
    mkdirSync(join(lockedTrash, "node_modules"), { recursive: true });
    liveLock(join(version, obsoleteName(9)), "removing-window");
    const recent = runtime(join(version, obsoleteName(3)), 5);
    const unmarked = runtime(join(version, obsoleteName(4)), null);
    const incomplete = join(version, obsoleteName(5));
    mkdirSync(incomplete);
    writeFileSync(join(incomplete, ".rapidb-last-used"), "");
    const past = new Date(Date.now() - 60 * DAY_MS);
    utimesSync(join(incomplete, ".rapidb-last-used"), past, past);
    const locked = runtime(join(version, obsoleteName(6)), 60);
    const owner = liveLock(locked, "other-window");
    const installing = runtime(join(version, obsoleteName(7)), 60);
    mkdirSync(`${installing}.tmp-${process.pid}-${Date.now()}`);
    const unrelated = runtime(join(version, "my-notes"), 60);
    const outside = runtime(join(f.root, "outside", obsoleteName(8)), 60);
    const linked = join(version, obsoleteName(8));
    symlinkSync(outside, linked, "dir");
    const linkedVersion = join(f.cacheRoot, "12.8.0");
    symlinkSync(dirname(outside), linkedVersion, "dir");

    const removed = await removeObsoleteSQLiteRuntimes(f.baseDir);

    expect(removed.sort()).toEqual([old, oldOtherVersion, trash].sort());
    for (const path of [old, oldOtherVersion, trash]) {
      expect(existsSync(path)).toBe(false);
    }
    expect(existsSync(`${old}.lock`)).toBe(false);
    for (const path of [
      current,
      recent,
      unmarked,
      incomplete,
      locked,
      installing,
      unrelated,
      outside,
      linked,
      join(linkedVersion, obsoleteName(8)),
      lockedTrash,
    ]) {
      expect(existsSync(path)).toBe(true);
    }
    expect(markerAge(unmarked)).toBeLessThan(60_000);
    expect(
      JSON.parse(readFileSync(join(`${locked}.lock`, "owner.json"), "utf8")),
    ).toEqual(owner);
    expect(f.logs.join("\n")).toContain(`Removing SQLite runtime ${old}`);
  });

  it("removes abandoned install leftovers of obsolete runtimes only after the idle limit", async () => {
    const f = await fixture();
    const version = join(f.cacheRoot, "12.10.0");
    const name = obsoleteName(1);
    const runtimeRoot = runtime(join(version, name), 45);
    const leftover = join(version, `${name}.tmp-1-1`);
    mkdirSync(leftover);
    const past = new Date(Date.now() - 45 * DAY_MS);
    utimesSync(leftover, past, past);
    const currentLeftover = `${f.currentRoot}.tmp-1-1`;
    mkdirSync(currentLeftover, { recursive: true });
    utimesSync(currentLeftover, past, past);

    expect((await removeObsoleteSQLiteRuntimes(f.baseDir)).sort()).toEqual(
      [leftover, runtimeRoot].sort(),
    );
    expect(existsSync(currentLeftover)).toBe(true);
  });

  it("bounds the number of removals per run", async () => {
    const f = await fixture();
    const version = join(f.cacheRoot, "12.10.0");
    const paths = [1, 2, 3, 4, 5].map((abi) =>
      runtime(join(version, obsoleteName(abi)), 40),
    );
    expect(await removeObsoleteSQLiteRuntimes(f.baseDir)).toHaveLength(3);
    expect(await removeObsoleteSQLiteRuntimes(f.baseDir)).toHaveLength(2);
    expect(paths.some((path) => existsSync(path))).toBe(false);
  });

  it("logs and keeps a runtime it cannot move, releasing its lock", async () => {
    const f = await fixture();
    const old = runtime(join(f.cacheRoot, "12.10.0", obsoleteName(1)), 40);
    await failObsoleteRenames(
      Object.assign(new Error("in use"), { code: "EPERM" }),
    );
    expect(await removeObsoleteSQLiteRuntimes(f.baseDir)).toEqual([]);
    expect(existsSync(join(old, "runtime.json"))).toBe(true);
    expect(existsSync(`${old}.lock`)).toBe(false);
    expect(f.logs.join("\n")).toContain(
      `Could not remove obsolete SQLite runtime ${old}: in use`,
    );
  });

  it("marks the current runtime as used and cleans up without failing a successful load", async () => {
    const f = await fixture();
    const packagePath = currentRuntime(f.currentRoot);
    const old = runtime(join(f.cacheRoot, "12.10.0", obsoleteName(1)), 40);
    await failObsoleteRenames(new Error("unexpected failure"));

    await expect(ensureSQLiteRuntimeInstalled(f.baseDir)).resolves.toBe(
      packagePath,
    );
    expect(markerAge(f.currentRoot)).toBeLessThan(60_000);
    expect(existsSync(old)).toBe(true);
    expect(f.logs.join("\n")).toContain("unexpected failure");

    vi.mocked(renameSync).mockReset();
    resetSQLiteInstallerForTests();
    configureSQLiteInstaller({ storageRoot: join(f.root, "storage") });
    await expect(ensureSQLiteRuntimeInstalled(f.baseDir)).resolves.toBe(
      packagePath,
    );
    expect(existsSync(old)).toBe(false);
  });

  it("never touches runtimes of older extension versions that do not record use", async () => {
    const f = await fixture();
    const version = join(f.cacheRoot, "12.10.0");
    const legacy = [
      join(f.cacheRoot, "12.9.0", "electron-abi-130-darwin-arm64"),
      join(version, `node-abi-1-${process.platform}-${process.arch}`),
      join(version, obsoleteName(2).replace(`-${POLICY}`, "-archive-v1")),
      join(version, obsoleteName(3, "pins-v2-0123456789abcdef")),
      join(version, obsoleteName(4, "pins-v4-0123456789abcdef")),
    ];
    const marked = legacy.map((path) => runtime(path, 400));
    const unmarked = runtime(
      join(version, obsoleteName(5, "pins-v4-0123456789abcdef")),
      null,
    );
    const ages = marked.map(markerAge);

    expect(await removeObsoleteSQLiteRuntimes(f.baseDir)).toEqual([]);
    for (const [index, path] of marked.entries()) {
      expect(existsSync(join(path, "runtime.json"))).toBe(true);
      expect(markerAge(path)).toBeGreaterThanOrEqual(ages[index]);
    }
    expect(existsSync(join(unmarked, ".rapidb-last-used"))).toBe(false);
    expect(existsSync(`${unmarked}.lock`)).toBe(false);
  });

  it("restores a runtime whose use was recorded right before it was moved", async () => {
    const f = await fixture();
    const version = join(f.cacheRoot, "12.10.0");
    const old = runtime(join(version, obsoleteName(1)), 40);
    const actual = (await vi.importActual<typeof import("node:fs")>("node:fs"))
      .renameSync;
    vi.mocked(renameSync).mockImplementation((from, to) => {
      if (String(to).includes(".obsolete-")) {
        const now = new Date();
        utimesSync(join(String(from), ".rapidb-last-used"), now, now);
      }
      actual(from, to);
    });

    expect(await removeObsoleteSQLiteRuntimes(f.baseDir)).toEqual([]);
    expect(existsSync(join(old, "runtime.json"))).toBe(true);
    expect(markerAge(old)).toBeLessThan(60_000);
    expect(readdirSync(version).sort()).toEqual([basename(old)]);
    expect(f.logs.join("\n")).toContain(`Kept SQLite runtime ${old}`);
  });

  it("removes a runtime once it is moved, failing later attempts to record use", async () => {
    const f = await fixture();
    const old = runtime(join(f.cacheRoot, "12.10.0", obsoleteName(1)), 40);
    const actual = (await vi.importActual<typeof import("node:fs")>("node:fs"))
      .renameSync;
    let lateUse: unknown = null;
    vi.mocked(renameSync).mockImplementation((from, to) => {
      actual(from, to);
      if (String(to).includes(".obsolete-")) {
        try {
          const now = new Date();
          utimesSync(join(String(from), ".rapidb-last-used"), now, now);
        } catch (error) {
          lateUse = error;
        }
      }
    });

    expect(await removeObsoleteSQLiteRuntimes(f.baseDir)).toEqual([old]);
    expect(lateUse).toMatchObject({ code: "ENOENT" });
    expect(existsSync(old)).toBe(false);
  });
});

describe("SQLite runtime use recording", () => {
  it("records use of an unmarked runtime under the lock without asking to install", async () => {
    const f = await fixture();
    const packagePath = currentRuntime(f.currentRoot);
    rmSync(join(f.currentRoot, ".rapidb-last-used"));
    const allowInstall = vi.fn(() => false);
    configureSQLiteInstaller({
      storageRoot: join(f.root, "storage"),
      allowInstall,
    });
    liveLock(f.currentRoot, "cleanup-window");

    const ensured = ensureSQLiteRuntimeInstalled(f.baseDir);
    await new Promise((resolve) => setTimeout(resolve, 300));
    expect(existsSync(join(f.currentRoot, ".rapidb-last-used"))).toBe(false);
    rmSync(`${f.currentRoot}.lock`, { recursive: true, force: true });

    await expect(ensured).resolves.toBe(packagePath);
    expect(markerAge(f.currentRoot)).toBeLessThan(60_000);
    expect(allowInstall).not.toHaveBeenCalled();
    expect(existsSync(`${f.currentRoot}.lock`)).toBe(false);
  });

  it.each([
    ["one day old", 1],
    ["six days old", 6],
    ["one minute in the future", -1 / (24 * 60)],
  ])("records use without the lock when the marker is %s", async (_name, days) => {
    const f = await fixture();
    const packagePath = currentRuntime(f.currentRoot);
    setMarkerAge(f.currentRoot, days);
    const owner = liveLock(f.currentRoot, "cleanup-window");
    setSQLiteInstallLockTimingsForTests({ waitMs: 500 });

    await expect(ensureSQLiteRuntimeInstalled(f.baseDir)).resolves.toBe(
      packagePath,
    );
    expect(markerAge(f.currentRoot)).toBeLessThan(60_000);
    expect(markerAge(f.currentRoot)).toBeGreaterThanOrEqual(-60_000);
    expect(
      JSON.parse(
        readFileSync(join(`${f.currentRoot}.lock`, "owner.json"), "utf8"),
      ),
    ).toEqual(owner);
  });

  it.each([
    ["ten days old", 10],
    ["forty days old", 40],
    ["two days in the future", -2],
  ])("records use under the lock when the marker is %s", async (_name, days) => {
    const f = await fixture();
    const packagePath = currentRuntime(f.currentRoot);
    setMarkerAge(f.currentRoot, days);
    const before = markerAge(f.currentRoot);
    const allowInstall = vi.fn(() => false);
    configureSQLiteInstaller({
      storageRoot: join(f.root, "storage"),
      allowInstall,
    });
    liveLock(f.currentRoot, "cleanup-window");

    let settled = false;
    const ensured = ensureSQLiteRuntimeInstalled(f.baseDir).finally(() => {
      settled = true;
    });
    await new Promise((resolve) => setTimeout(resolve, 300));
    expect(settled).toBe(false);
    expect(Math.abs(markerAge(f.currentRoot) - before)).toBeLessThan(1_000);
    rmSync(`${f.currentRoot}.lock`, { recursive: true, force: true });

    await expect(ensured).resolves.toBe(packagePath);
    expect(markerAge(f.currentRoot)).toBeLessThan(60_000);
    expect(markerAge(f.currentRoot)).toBeGreaterThanOrEqual(-60_000);
    expect(allowInstall).not.toHaveBeenCalled();
    expect(existsSync(`${f.currentRoot}.lock`)).toBe(false);
  });

  it.each([
    ["removed", false],
    ["restored", true],
  ])("waits for a cleanup that holds the lock and uses the %s runtime", async (_name, usedMeanwhile) => {
    const f = await fixture();
    const packagePath = currentRuntime(f.currentRoot);
    setMarkerAge(f.currentRoot, 40);
    const allowInstall = vi.fn(() => false);
    configureSQLiteInstaller({
      storageRoot: join(f.root, "storage"),
      allowInstall,
    });
    const otherBaseDir = otherVersionBaseDir(f.root);
    const actual = (await vi.importActual<typeof import("node:fs")>("node:fs"))
      .renameSync;
    let ensured = null as Promise<string | null> | null;
    let settled = false;
    let ageAtRename = 0;
    vi.mocked(renameSync).mockImplementation((from, to) => {
      if (from === f.currentRoot && String(to).includes(".obsolete-")) {
        ensured = ensureSQLiteRuntimeInstalled(f.baseDir).finally(() => {
          settled = true;
        });
        ensured.catch(() => {});
        ageAtRename = markerAge(f.currentRoot);
        actual(from, to);
        if (usedMeanwhile) {
          const now = new Date();
          utimesSync(join(String(to), ".rapidb-last-used"), now, now);
        }
        return;
      }
      actual(from, to);
    });

    const removed = await removeObsoleteSQLiteRuntimes(otherBaseDir);

    expect(ensured).not.toBeNull();
    expect(ageAtRename).toBeGreaterThan(30 * DAY_MS);
    expect(settled).toBe(false);
    if (usedMeanwhile) {
      expect(removed).toEqual([]);
      await expect(ensured).resolves.toBe(packagePath);
      expect(existsSync(join(packagePath, "package.json"))).toBe(true);
      expect(markerAge(f.currentRoot)).toBeLessThan(60_000);
      expect(allowInstall).not.toHaveBeenCalled();
    } else {
      expect(removed).toEqual([f.currentRoot]);
      await expect(ensured).rejects.toThrow(/not authorized/);
      expect(allowInstall).toHaveBeenCalledTimes(1);
      expect(existsSync(f.currentRoot)).toBe(false);
    }
  });

  it("falls back to installation instead of returning a runtime that was moved away", async () => {
    const f = await fixture();
    currentRuntime(f.currentRoot);
    renameSync(f.currentRoot, `${f.currentRoot}.obsolete-1-${randomUUID()}`);
    const allowInstall = vi.fn(() => false);
    configureSQLiteInstaller({
      storageRoot: join(f.root, "storage"),
      allowInstall,
    });

    await expect(ensureSQLiteRuntimeInstalled(f.baseDir)).rejects.toThrow(
      /not authorized/,
    );
    expect(allowInstall).toHaveBeenCalledTimes(1);
    expect(existsSync(f.currentRoot)).toBe(false);
  });
});
