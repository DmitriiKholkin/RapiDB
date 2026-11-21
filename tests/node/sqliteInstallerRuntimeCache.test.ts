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
  sqliteArtifactPolicyId,
} from "../../src/extension/utils/sqliteArtifactPins";
import {
  configureSQLiteInstaller,
  ensureSQLiteRuntimeInstalled,
  probeInstalledBetterSqlite3Runtime,
  removeObsoleteSQLiteRuntimes,
  resetSQLiteInstallerForTests,
  setSQLiteHostForTests,
  setSQLiteInstallLockTimingsForTests,
} from "../../src/extension/utils/sqliteInstaller";
import { createProjectTempDir } from "../runtime/tempDirectories";
import {
  FIXTURE_TARGET,
  hostProbe,
  mockRegistry,
  pinFixturePackage,
  restoreSQLiteArtifacts,
} from "./sqliteRuntimeFixtures";

vi.mock("node:fs", async (importOriginal) => {
  const actual = await importOriginal<typeof import("node:fs")>();
  return { ...actual, renameSync: vi.fn(actual.renameSync) };
});

afterEach(() => {
  vi.mocked(renameSync).mockReset();
  vi.restoreAllMocks();
  resetSQLiteInstallerForTests();
  restoreSQLiteArtifacts();
});

const DAY_MS = 24 * 60 * 60 * 1000;
const POLICY = "pins-v6-0123456789abcdef";
const LEGACY_VERSION = "12.11.1";
const VERSION = SQLITE_ARTIFACTS.betterSqlite3Version;

function manifestCopy(): SQLiteArtifactManifest {
  return JSON.parse(JSON.stringify(SQLITE_ARTIFACTS));
}

describe("SQLite runtime cache identity", () => {
  it("ignores pins for other targets", () => {
    const manifest = manifestCopy();
    const before = sqliteArtifactPolicyId("linux-x64", manifest);
    manifest.prebuilds["linuxmusl-x64"].sha256 = "0".repeat(64);
    manifest.prebuilds["win32-arm64"].size += 1;
    delete manifest.prebuilds["darwin-arm64"];
    expect(sqliteArtifactPolicyId("linux-x64", manifest)).toBe(before);
    expect(before).toMatch(/^pins-v6-[0-9a-f]{16}$/);
  });

  it.each([
    [
      "target prebuild",
      (manifest: SQLiteArtifactManifest) => {
        manifest.prebuilds["linux-x64"].sha256 = "0".repeat(64);
      },
    ],
    [
      "tarball",
      (manifest: SQLiteArtifactManifest) => {
        manifest.tarball.size += 1;
      },
    ],
    [
      "JavaScript file",
      (manifest: SQLiteArtifactManifest) => {
        manifest.files["lib/database.js"].sha256 = "0".repeat(64);
      },
    ],
    [
      "Node-API version",
      (manifest: SQLiteArtifactManifest) => {
        manifest.nodeApiVersion += 1;
      },
    ],
    [
      "schema version",
      (manifest: SQLiteArtifactManifest) => {
        manifest.schemaVersion += 1;
      },
    ],
  ])("changes when the %s pin changes", (_name, mutate) => {
    const manifest = manifestCopy();
    const before = sqliteArtifactPolicyId("linux-x64", manifest);
    mutate(manifest);
    expect(sqliteArtifactPolicyId("linux-x64", manifest)).not.toBe(before);
  });

  it("keys the managed runtime directory by version, target and the scoped policy", async () => {
    const f = await fixture();
    expect(basename(f.currentRoot)).toBe(
      `${FIXTURE_TARGET}-${sqliteArtifactPolicyId(FIXTURE_TARGET)}`,
    );
    expect(basename(dirname(f.currentRoot))).toBe(VERSION);
  });
});

async function fixture() {
  const root = await createProjectTempDir("sqlite-runtime-cache");
  const logs: string[] = [];
  configureSQLiteInstaller({
    storageRoot: join(root, "storage"),
    allowInstall: () => true,
    log: (line) => logs.push(line),
  });
  setSQLiteHostForTests({ probe: hostProbe(FIXTURE_TARGET) });
  const probe = probeInstalledBetterSqlite3Runtime();
  const currentRoot = dirname(dirname(probe.installedPackagePath as string));
  const cacheRoot = dirname(dirname(currentRoot));
  return { root, logs, currentRoot, cacheRoot };
}

function reconfigure(
  root: string,
  allowInstall?: () => boolean | Promise<boolean>,
) {
  configureSQLiteInstaller({
    storageRoot: join(root, "storage"),
    allowInstall,
  });
  setSQLiteHostForTests({ probe: hostProbe(FIXTURE_TARGET) });
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

function ageTree(path: string, days: number) {
  const time = new Date(Date.now() - days * DAY_MS);
  for (const entry of readdirSync(path, { recursive: true }) as string[]) {
    utimesSync(join(path, entry), time, time);
  }
  utimesSync(path, time, time);
}

function markerAge(path: string) {
  return Date.now() - statSync(join(path, ".rapidb-last-used")).mtimeMs;
}

function obsoleteName(index: number, policy = POLICY) {
  return `${["linuxmusl-x64", "linux-arm64", "darwin-x64", "win32-x64", "win32-arm64", "darwin-arm64", "linuxmusl-arm64", "linux-x64"][index % 8]}-${policy.slice(0, -2)}${String(index).padStart(2, "0")}`;
}

function legacyName(abi: number, suffix = "-pins-v5-0123456789abcdef") {
  return `electron-abi-${abi}-linux-x64${suffix}`;
}

// Install the current runtime through the real installer (with a mocked
// registry), then age its use marker.
async function installCurrent(root: string, idleDays = 90) {
  const pkg = pinFixturePackage();
  const urls = mockRegistry(() => ({ data: pkg.data }));
  const packagePath = await ensureSQLiteRuntimeInstalled();
  expect(urls).toHaveLength(1);
  vi.restoreAllMocks();
  resetSQLiteInstallerForTests();
  reconfigure(root);
  const runtimeRoot = dirname(dirname(packagePath as string));
  setMarkerAge(runtimeRoot, idleDays);
  return packagePath as string;
}

function setMarkerAge(runtimeRoot: string, days: number) {
  const time = new Date(Date.now() - days * DAY_MS);
  utimesSync(join(runtimeRoot, ".rapidb-last-used"), time, time);
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
    const version = join(f.cacheRoot, VERSION);
    const current = runtime(f.currentRoot, 90);
    const old = runtime(join(version, obsoleteName(1)), 31);
    const oldOtherVersion = runtime(
      join(f.cacheRoot, "13.0.0", obsoleteName(2)),
      400,
    );
    const trash = join(
      version,
      `${obsoleteName(3)}.obsolete-1-${randomUUID()}`,
    );
    mkdirSync(join(trash, "node_modules"), { recursive: true });
    const lockedTrash = join(
      version,
      `${obsoleteName(9)}.obsolete-1-${randomUUID()}`,
    );
    mkdirSync(join(lockedTrash, "node_modules"), { recursive: true });
    liveLock(join(version, obsoleteName(9)), "removing-window");
    const recent = runtime(join(version, obsoleteName(4)), 5);
    const unmarked = runtime(join(version, obsoleteName(5)), null);
    const incomplete = join(version, obsoleteName(6));
    mkdirSync(incomplete);
    writeFileSync(join(incomplete, ".rapidb-last-used"), "");
    const past = new Date(Date.now() - 60 * DAY_MS);
    utimesSync(join(incomplete, ".rapidb-last-used"), past, past);
    const locked = runtime(join(version, obsoleteName(7)), 60);
    const owner = liveLock(locked, "other-window");
    const installing = runtime(join(version, obsoleteName(10)), 60);
    mkdirSync(`${installing}.tmp-${process.pid}-${Date.now()}`);
    const unrelated = runtime(join(version, "my-notes"), 60);
    const outside = runtime(join(f.root, "outside", obsoleteName(8)), 60);
    const linked = join(version, obsoleteName(8));
    symlinkSync(outside, linked, "dir");
    const linkedVersion = join(f.cacheRoot, "12.8.0");
    symlinkSync(dirname(outside), linkedVersion, "dir");

    const removed = await removeObsoleteSQLiteRuntimes();

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
    // A runtime without a use marker is neither marked nor removed while its
    // files are recent.
    expect(existsSync(join(unmarked, ".rapidb-last-used"))).toBe(false);
    expect(
      JSON.parse(readFileSync(join(`${locked}.lock`, "owner.json"), "utf8")),
    ).toEqual(owner);
    expect(f.logs.join("\n")).toContain(`Removing SQLite runtime ${old}`);
  });

  it("removes abandoned install leftovers of obsolete runtimes only after the idle limit", async () => {
    const f = await fixture();
    const version = join(f.cacheRoot, VERSION);
    const name = obsoleteName(1);
    const runtimeRoot = runtime(join(version, name), 45);
    const leftover = join(version, `${name}.tmp-1-1`);
    mkdirSync(leftover);
    const past = new Date(Date.now() - 45 * DAY_MS);
    utimesSync(leftover, past, past);
    const currentLeftover = `${f.currentRoot}.tmp-1-1`;
    mkdirSync(currentLeftover, { recursive: true });
    utimesSync(currentLeftover, past, past);

    expect((await removeObsoleteSQLiteRuntimes()).sort()).toEqual(
      [leftover, runtimeRoot].sort(),
    );
    expect(existsSync(currentLeftover)).toBe(true);
  });

  it("bounds the number of removals per run", async () => {
    const f = await fixture();
    const version = join(f.cacheRoot, VERSION);
    const paths = [1, 2, 3, 4, 5].map((index) =>
      runtime(join(version, obsoleteName(index)), 40),
    );
    expect(await removeObsoleteSQLiteRuntimes()).toHaveLength(3);
    expect(await removeObsoleteSQLiteRuntimes()).toHaveLength(2);
    expect(paths.some((path) => existsSync(path))).toBe(false);
  });

  it("logs and keeps a runtime it cannot move, releasing its lock", async () => {
    const f = await fixture();
    const old = runtime(join(f.cacheRoot, VERSION, obsoleteName(1)), 40);
    await failObsoleteRenames(
      Object.assign(new Error("in use"), { code: "EPERM" }),
    );
    expect(await removeObsoleteSQLiteRuntimes()).toEqual([]);
    expect(existsSync(join(old, "runtime.json"))).toBe(true);
    expect(existsSync(`${old}.lock`)).toBe(false);
    expect(f.logs.join("\n")).toContain(
      `Could not remove obsolete SQLite runtime ${old}: in use`,
    );
  });

  it("marks the current runtime as used and cleans up without failing a successful load", async () => {
    const f = await fixture();
    const packagePath = await installCurrent(f.root);
    configureSQLiteInstaller({
      storageRoot: join(f.root, "storage"),
      log: (line) => f.logs.push(line),
    });
    const old = runtime(join(f.cacheRoot, VERSION, obsoleteName(1)), 40);
    await failObsoleteRenames(new Error("unexpected failure"));

    await expect(ensureSQLiteRuntimeInstalled()).resolves.toBe(packagePath);
    expect(markerAge(f.currentRoot)).toBeLessThan(60_000);
    expect(existsSync(old)).toBe(true);
    expect(f.logs.join("\n")).toContain("unexpected failure");

    vi.mocked(renameSync).mockReset();
    resetSQLiteInstallerForTests();
    reconfigure(f.root);
    await expect(ensureSQLiteRuntimeInstalled()).resolves.toBe(packagePath);
    expect(existsSync(old)).toBe(false);
  });

  it("restores a runtime whose use was recorded right before it was moved", async () => {
    const f = await fixture();
    const version = join(f.cacheRoot, VERSION);
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

    expect(await removeObsoleteSQLiteRuntimes()).toEqual([]);
    expect(existsSync(join(old, "runtime.json"))).toBe(true);
    expect(markerAge(old)).toBeLessThan(60_000);
    expect(readdirSync(version).sort()).toEqual([basename(old)]);
    expect(f.logs.join("\n")).toContain(`Kept SQLite runtime ${old}`);
  });

  it("removes a runtime once it is moved, failing later attempts to record use", async () => {
    const f = await fixture();
    const old = runtime(join(f.cacheRoot, VERSION, obsoleteName(1)), 40);
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

    expect(await removeObsoleteSQLiteRuntimes()).toEqual([old]);
    expect(lateUse).toMatchObject({ code: "ENOENT" });
    expect(existsSync(old)).toBe(false);
  });
});

describe("SQLite better-sqlite3 12.x (ABI-keyed) runtime migration", () => {
  it("removes idle legacy runtimes of every former cache policy", async () => {
    const f = await fixture();
    const legacyVersion = join(f.cacheRoot, LEGACY_VERSION);
    const legacy = [
      legacyName(146),
      legacyName(143, "-archive-v1"),
      legacyName(140, ""),
      legacyName(145, "-pins-v2-0123456789abcdef"),
    ].map((name) =>
      runtime(join(legacyVersion, name), 45, {
        betterSqlite3Version: LEGACY_VERSION,
        runtime: "electron",
        abi: "146",
      }),
    );
    const node = runtime(
      join(
        f.cacheRoot,
        "12.10.0",
        "node-abi-137-linux-x64-pins-v5-0123456789abcdef",
      ),
      400,
    );
    const removed: string[] = [];
    for (let run = 0; run < 3; run++)
      removed.push(...(await removeObsoleteSQLiteRuntimes()));
    expect(removed.sort()).toEqual([...legacy, node].sort());
    for (const path of [...legacy, node]) expect(existsSync(path)).toBe(false);
    expect(f.logs.join("\n")).toContain(`Removing SQLite runtime ${legacy[0]}`);
  });

  it("removes legacy runtimes without a use marker only once nothing in them changed for 30 days", async () => {
    const f = await fixture();
    const untracked = runtime(
      join(f.cacheRoot, LEGACY_VERSION, legacyName(143, "-archive-v1")),
      null,
    );
    mkdirSync(join(untracked, "node_modules", "better-sqlite3", "lib"), {
      recursive: true,
    });
    const nested = join(untracked, "node_modules", "better-sqlite3", "lib");
    writeFileSync(join(nested, "index.js"), "");

    expect(await removeObsoleteSQLiteRuntimes()).toEqual([]);
    expect(existsSync(join(untracked, ".rapidb-last-used"))).toBe(false);
    expect(existsSync(`${untracked}.lock`)).toBe(false);

    // Everything is old except one nested file: still kept.
    ageTree(untracked, 31);
    const recent = new Date(Date.now() - 2 * DAY_MS);
    utimesSync(join(nested, "index.js"), recent, recent);
    expect(await removeObsoleteSQLiteRuntimes()).toEqual([]);
    expect(existsSync(untracked)).toBe(true);

    ageTree(untracked, 31);
    expect(await removeObsoleteSQLiteRuntimes()).toEqual([untracked]);
    expect(existsSync(untracked)).toBe(false);
    expect(
      vi
        .mocked(renameSync)
        .mock.calls.some(
          ([from, to]) =>
            from === untracked && String(to).includes(".obsolete-"),
        ),
    ).toBe(true);
    expect(f.logs.join("\n")).toContain(
      `Removing SQLite runtime ${untracked}; it records no use and has not been modified for over 30 days.`,
    );
  });

  it("keeps an old runtime without a use marker while another window holds its lock", async () => {
    const f = await fixture();
    const untracked = runtime(
      join(f.cacheRoot, LEGACY_VERSION, legacyName(146)),
      null,
    );
    ageTree(untracked, 400);
    const owner = liveLock(untracked, "old-rapidb-window");
    expect(await removeObsoleteSQLiteRuntimes()).toEqual([]);
    expect(existsSync(join(untracked, "runtime.json"))).toBe(true);
    expect(
      JSON.parse(readFileSync(join(`${untracked}.lock`, "owner.json"), "utf8")),
    ).toEqual(owner);
  });

  it("keeps an old runtime without a use marker that cannot be moved", async () => {
    const f = await fixture();
    const untracked = runtime(
      join(f.cacheRoot, LEGACY_VERSION, legacyName(146)),
      null,
    );
    ageTree(untracked, 400);
    await failObsoleteRenames(
      Object.assign(new Error("in use"), { code: "EBUSY" }),
    );
    expect(await removeObsoleteSQLiteRuntimes()).toEqual([]);
    expect(existsSync(join(untracked, "runtime.json"))).toBe(true);
    expect(existsSync(`${untracked}.lock`)).toBe(false);
  });

  it("keeps recently used, locked, incomplete or linked legacy runtimes", async () => {
    const f = await fixture();
    const legacyVersion = join(f.cacheRoot, LEGACY_VERSION);
    const recent = runtime(join(legacyVersion, legacyName(146)), 3);
    const locked = runtime(join(legacyVersion, legacyName(143)), 60);
    liveLock(locked, "older-rapidb-window");
    const incomplete = join(legacyVersion, legacyName(140));
    mkdirSync(join(incomplete, "node_modules"), { recursive: true });
    const outside = runtime(join(f.root, "outside", legacyName(139)), 400);
    const linked = join(legacyVersion, legacyName(139));
    symlinkSync(outside, linked, "dir");
    const notes = runtime(join(legacyVersion, "electron-notes"), 400);

    expect(await removeObsoleteSQLiteRuntimes()).toEqual([]);
    for (const path of [recent, locked, incomplete, outside, linked, notes])
      expect(existsSync(path)).toBe(true);
    expect(markerAge(outside)).toBeGreaterThan(300 * DAY_MS);
  });

  it("removes an emptied legacy version directory only once it is idle, never the current one", async () => {
    const f = await fixture();
    const emptyOld = join(f.cacheRoot, LEGACY_VERSION);
    const emptyFresh = join(f.cacheRoot, "12.10.0");
    const nonEmpty = join(f.cacheRoot, "12.9.0");
    const current = join(f.cacheRoot, VERSION);
    for (const path of [emptyOld, emptyFresh, nonEmpty, current])
      mkdirSync(path, { recursive: true });
    writeFileSync(join(nonEmpty, "keep.txt"), "");
    const past = new Date(Date.now() - 40 * DAY_MS);
    for (const path of [emptyOld, nonEmpty, current])
      utimesSync(path, past, past);

    expect(await removeObsoleteSQLiteRuntimes()).toEqual([]);
    expect(existsSync(emptyOld)).toBe(false);
    for (const path of [emptyFresh, nonEmpty, current])
      expect(existsSync(path)).toBe(true);
  });

  it("does not touch anything outside the managed cache", async () => {
    const f = await fixture();
    const sibling = runtime(
      join(
        f.root,
        "storage",
        "other-extension-data",
        LEGACY_VERSION,
        legacyName(146),
      ),
      400,
    );
    const linkedCache = join(f.root, "linked-cache");
    runtime(join(linkedCache, LEGACY_VERSION, legacyName(143)), 400);
    mkdirSync(dirname(f.cacheRoot), { recursive: true });
    symlinkSync(linkedCache, f.cacheRoot, "dir");
    expect(await removeObsoleteSQLiteRuntimes()).toEqual([]);
    expect(existsSync(sibling)).toBe(true);
    expect(
      existsSync(
        join(linkedCache, LEGACY_VERSION, legacyName(143), "runtime.json"),
      ),
    ).toBe(true);
  });
});

describe("SQLite runtime use recording", () => {
  it("records use of an unmarked runtime under the lock without asking to install", async () => {
    const f = await fixture();
    const packagePath = await installCurrent(f.root);
    rmSync(join(f.currentRoot, ".rapidb-last-used"));
    const allowInstall = vi.fn(() => false);
    reconfigure(f.root, allowInstall);
    liveLock(f.currentRoot, "cleanup-window");

    const ensured = ensureSQLiteRuntimeInstalled();
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
    const packagePath = await installCurrent(f.root);
    setMarkerAge(f.currentRoot, days);
    const owner = liveLock(f.currentRoot, "cleanup-window");
    setSQLiteInstallLockTimingsForTests({ waitMs: 500 });

    await expect(ensureSQLiteRuntimeInstalled()).resolves.toBe(packagePath);
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
    const packagePath = await installCurrent(f.root);
    setMarkerAge(f.currentRoot, days);
    const before = markerAge(f.currentRoot);
    const allowInstall = vi.fn(() => false);
    reconfigure(f.root, allowInstall);
    liveLock(f.currentRoot, "cleanup-window");

    let settled = false;
    const ensured = ensureSQLiteRuntimeInstalled().finally(() => {
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
  ])("waits for another window's cleanup that holds the lock and uses the %s runtime", async (_name, usedMeanwhile) => {
    const f = await fixture();
    const packagePath = await installCurrent(f.root);
    setMarkerAge(f.currentRoot, 40);
    const allowInstall = vi.fn(() => false);
    reconfigure(f.root, allowInstall);
    // Another window on a host that selects a different target sees this
    // runtime as obsolete.
    const installerPath = "../../src/extension/utils/sqliteInstaller";
    const other = (await import(
      `${installerPath}?other-window`
    )) as typeof import("../../src/extension/utils/sqliteInstaller");
    other.configureSQLiteInstaller({ storageRoot: join(f.root, "storage") });
    other.setSQLiteHostForTests({ probe: hostProbe("linuxmusl-x64") });
    const actual = (await vi.importActual<typeof import("node:fs")>("node:fs"))
      .renameSync;
    let ensured = null as Promise<string | null> | null;
    let settled = false;
    let ageAtRename = 0;
    vi.mocked(renameSync).mockImplementation((from, to) => {
      if (from === f.currentRoot && String(to).includes(".obsolete-")) {
        ensured = ensureSQLiteRuntimeInstalled().finally(() => {
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

    const removed = await other.removeObsoleteSQLiteRuntimes();
    other.resetSQLiteInstallerForTests();

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
    await installCurrent(f.root);
    renameSync(f.currentRoot, `${f.currentRoot}.obsolete-1-${randomUUID()}`);
    const allowInstall = vi.fn(() => false);
    reconfigure(f.root, allowInstall);

    await expect(ensureSQLiteRuntimeInstalled()).rejects.toThrow(
      /not authorized/,
    );
    expect(allowInstall).toHaveBeenCalledTimes(1);
    expect(existsSync(f.currentRoot)).toBe(false);
  });
});
