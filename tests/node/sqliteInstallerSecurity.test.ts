import { spawn } from "node:child_process";
import {
  cpSync,
  existsSync,
  lstatSync,
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
import { gzipSync } from "node:zlib";
import { afterEach, describe, expect, it, vi } from "vitest";
import { formatSQLiteRuntimeLoadErrorMessage } from "../../src/extension/dbDrivers/sqliteRuntime";
import {
  extractSQLitePackage,
  SQLITE_ARCHIVE_LIMITS,
} from "../../src/extension/utils/sqliteArchive";
import {
  SQLITE_ARTIFACTS,
  type SQLiteInstallFile,
  sqlitePackagePlan,
} from "../../src/extension/utils/sqliteArtifactPins";
import {
  configureSQLiteInstaller,
  ensureSQLiteRuntimeInstalled,
  formatSQLiteDownloadSize,
  probeInstalledBetterSqlite3Runtime,
  resetSQLiteInstallerForTests,
  type SQLiteInstallProgress,
  SQLiteRuntimeInstallCancelledError,
  SQLiteRuntimeInUseError,
  SQLiteRuntimeUnreadableError,
  setSQLiteHostForTests,
  setSQLiteInstallLockTimingsForTests,
  sqliteRuntimeDownloadSummary,
} from "../../src/extension/utils/sqliteInstaller";
import {
  detectSQLiteLinuxLibc,
  resolveSQLiteNativeTarget,
  SQLITE_NATIVE_TARGETS,
  type SQLiteHostProbe,
  SQLiteUnsupportedTargetError,
} from "../../src/extension/utils/sqliteNativeTarget";
import { createProjectTempDir } from "../runtime/tempDirectories";
import {
  FIXTURE_TARGET,
  gzipTar,
  hostProbe,
  https,
  mockRegistry,
  pinFixturePackage,
  pinTarball,
  restoreSQLiteArtifacts,
  sha256Hex,
  sha512Integrity,
  TARBALL_URL,
  type TarEntry,
  tar,
  unexpectedDownload,
} from "./sqliteRuntimeFixtures";

vi.mock("node:fs", async (importOriginal) => {
  const actual = await importOriginal<typeof import("node:fs")>();
  return {
    ...actual,
    renameSync: vi.fn(actual.renameSync),
    readFileSync: vi.fn(actual.readFileSync),
  };
});

afterEach(() => {
  vi.mocked(renameSync).mockReset();
  vi.mocked(readFileSync).mockReset();
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
  resetSQLiteInstallerForTests();
  restoreSQLiteArtifacts();
});

describe("SQLite native target selection", () => {
  it.each([
    ["darwin", "x64", "darwin-x64"],
    ["darwin", "arm64", "darwin-arm64"],
    ["win32", "x64", "win32-x64"],
    ["win32", "arm64", "win32-arm64"],
  ])("maps %s-%s to %s without probing libc", (platform, arch, expected) => {
    const readProcessMaps = vi.fn(() => "");
    expect(
      resolveSQLiteNativeTarget({
        platform: platform as NodeJS.Platform,
        arch,
        readProcessMaps,
      }),
    ).toBe(expected);
    expect(readProcessMaps).not.toHaveBeenCalled();
  });

  it.each([
    "linux-x64",
    "linux-arm64",
    "linuxmusl-x64",
    "linuxmusl-arm64",
  ])("selects %s when the loaded C library and the process report agree", (target) => {
    expect(resolveSQLiteNativeTarget(hostProbe(target))).toBe(target);
  });

  it.each([
    ["loader map unavailable, glibc reported", null, "2.31", "glibc"],
    [
      "glibc loader, report unavailable",
      "/lib/aarch64-linux-gnu/ld-linux-aarch64.so.1",
      undefined,
      "glibc",
    ],
    [
      "musl loader, report unavailable",
      "/lib/ld-musl-aarch64.so.1",
      undefined,
      "musl",
    ],
    ["no signal at all", null, undefined, null],
    [
      "unrecognized loader, no glibc reported",
      "/system/bin/linker64",
      null,
      null,
    ],
    ["no loader map, no glibc reported", null, null, null],
    [
      "both loaders mapped",
      "/lib/ld-musl-x86_64.so.1\n/lib/x86_64-linux-gnu/libc.so.6",
      undefined,
      null,
    ],
    [
      "musl loader but glibc reported",
      "/lib/ld-musl-x86_64.so.1",
      "2.39",
      null,
    ],
    [
      "glibc loader but no glibc reported",
      "/lib64/ld-linux-x86-64.so.2",
      null,
      null,
    ],
  ])("detects libc for %s", (_name, maps, report, expected) => {
    expect(
      detectSQLiteLinuxLibc({
        platform: "linux",
        arch: "x64",
        readProcessMaps: () => {
          if (maps === null) throw new Error("ENOENT");
          return maps;
        },
        glibcVersionRuntime: () => report,
      }),
    ).toBe(expected);
  });

  it("fails closed instead of guessing an ambiguous Linux C library", () => {
    expect(() =>
      resolveSQLiteNativeTarget({
        platform: "linux",
        arch: "x64",
        readProcessMaps: () => {
          throw new Error("EACCES");
        },
        glibcVersionRuntime: () => null,
      }),
    ).toThrow(
      /could not reliably determine whether this process uses glibc or musl/,
    );
  });

  it.each([
    ["linux", "ia32"],
    ["linux", "arm"],
    ["win32", "ia32"],
    ["darwin", "ppc64"],
    ["freebsd", "x64"],
    ["openbsd", "arm64"],
    ["sunos", "x64"],
    ["aix", "ppc64"],
  ])("rejects unsupported %s-%s with a clear error", (platform, arch) => {
    let error: unknown;
    try {
      resolveSQLiteNativeTarget({
        platform: platform as NodeJS.Platform,
        arch,
        readProcessMaps: () => "/lib/x86_64-linux-gnu/libc.so.6",
        glibcVersionRuntime: () => "2.39",
      });
    } catch (caught) {
      error = caught;
    }
    expect(error).toBeInstanceOf(SQLiteUnsupportedTargetError);
    expect((error as Error).message).toContain(`${platform}-${arch}`);
    expect((error as Error).message).toContain("No binary was downloaded");
  });

  it.skipIf(!["darwin", "linux", "win32"].includes(process.platform))(
    "resolves the current test host to a pinned target",
    () => {
      expect(SQLITE_NATIVE_TARGETS).toContain(resolveSQLiteNativeTarget());
    },
  );
});

async function extractionFixture(entries: TarEntry[], pinnedFiles?: string[]) {
  const root = await createProjectTempDir("sqlite-archive");
  const archivePath = join(root, "package.tgz");
  const data = gzipTar(entries);
  writeFileSync(archivePath, data);
  const contents = new Map<string, Buffer>();
  for (const entry of entries) {
    if ((entry.type ?? "0") === "0" && entry.name.startsWith("package/"))
      contents.set(entry.name.slice(8), Buffer.from(entry.data ?? ""));
  }
  const files: SQLiteInstallFile[] = (
    pinnedFiles ?? ["package.json", "lib/index.js", "prebuilds/linux-x64.node"]
  ).map((path) => {
    const data = contents.get(path) ?? Buffer.from("missing");
    return {
      archivePath: path,
      installPath: path.startsWith("prebuilds/")
        ? "build/Release/better_sqlite3.node"
        : path,
      sha256: sha256Hex(data),
      size: data.length,
    };
  });
  return {
    root,
    archivePath,
    data,
    files,
    destination: join(root, "extract"),
    extract: (
      overrides: Partial<Parameters<typeof extractSQLitePackage>[0]> = {},
    ) =>
      extractSQLitePackage({
        archivePath,
        destination: join(root, "extract"),
        integrity: sha512Integrity(data),
        files,
        ...overrides,
      }),
  };
}

const PACKAGE_ENTRIES: TarEntry[] = [
  { name: "package/package.json", data: '{"name":"better-sqlite3"}' },
  { name: "package/lib/index.js", data: "module.exports = 1;" },
  { name: "package/prebuilds/linux-x64.node", data: "linux addon" },
];

describe("SQLite package archive extraction", () => {
  it("checks the pinned digest before decompressing and writes nothing on mismatch", async () => {
    const f = await extractionFixture(PACKAGE_ENTRIES);
    await expect(
      f.extract({ integrity: sha512Integrity(Buffer.from("other")) }),
    ).rejects.toThrow("integrity mismatch");
    await expect(f.extract({ integrity: "" })).rejects.toThrow(
      "Trusted static integrity is required",
    );
    expect(existsSync(f.destination)).toBe(false);
  });

  it("extracts only reviewed files and installs the selected prebuild as the managed binary", async () => {
    const f = await extractionFixture([
      ...PACKAGE_ENTRIES,
      { name: "package/prebuilds/linuxmusl-x64.node", data: "musl addon" },
      { name: "package/src/better_sqlite3.cpp", data: "source" },
      { name: "package/deps/download.sh", data: "#!/bin/sh\nexit 1" },
      { name: "package/binding.gyp", data: "{}" },
    ]);
    await f.extract();
    const listing = (dir: string, prefix = ""): string[] =>
      readdirSync(dir, { withFileTypes: true }).flatMap((entry) =>
        entry.isDirectory()
          ? listing(join(dir, entry.name), `${prefix}${entry.name}/`)
          : [`${prefix}${entry.name}`],
      );
    expect(listing(f.destination).sort()).toEqual(
      [
        "build/Release/better_sqlite3.node",
        "lib/index.js",
        "package.json",
      ].sort(),
    );
    expect(
      readFileSync(
        join(f.destination, "build/Release/better_sqlite3.node"),
        "utf8",
      ),
    ).toBe("linux addon");
    if (process.platform !== "win32")
      expect(statSync(join(f.destination, "lib/index.js")).mode & 0o077).toBe(
        0,
      );
  });

  it("rejects a reviewed file whose SHA-256 differs even if the archive digest was re-pinned", async () => {
    const f = await extractionFixture(PACKAGE_ENTRIES);
    f.files[2] = { ...f.files[2], sha256: sha256Hex("expected addon") };
    await expect(f.extract()).rejects.toThrow(
      "prebuilds/linux-x64.node does not match its reviewed SHA-256 pin",
    );
    f.files[2] = { ...f.files[2], sha256: sha256Hex("linux addon"), size: 999 };
    await expect(f.extract()).rejects.toThrow("reviewed SHA-256 pin");
    expect(existsSync(f.destination)).toBe(false);
  });

  it("rejects an archive that lacks a reviewed file", async () => {
    const f = await extractionFixture(PACKAGE_ENTRIES.slice(0, 2));
    await expect(f.extract()).rejects.toThrow(
      "missing reviewed file prebuilds/linux-x64.node",
    );
    expect(existsSync(f.destination)).toBe(false);
  });

  it.each([
    "../escape",
    "package/../../escape",
    "package/lib/../../../escape",
    "/package/lib/index.js",
    "package\\lib\\evil.js",
    "C:package/evil.js",
    "package/con.js",
    "package/lib/CONIN$",
    "package/lib/conout$.js",
    "package/lib/COM¹",
    "package/lib/lpt³.txt",
    "package/lib/Com0.js",
    "package/lib/LPT0",
    "package/lib/nul .js",
    "package/lib/evil. ",
    "package/lib/evil\u0001.js",
  ])("rejects unsafe path %j even when it would be skipped", async (name) => {
    const f = await extractionFixture([
      ...PACKAGE_ENTRIES,
      { name, data: "x" },
    ]);
    await expect(f.extract()).rejects.toThrow("Unsafe SQLite archive path");
    expect(existsSync(f.destination)).toBe(false);
  });

  it.each([
    ["symlink", "2", "../../outside"],
    ["hard link", "1", "package/lib/index.js"],
    ["character device", "3", undefined],
    ["block device", "4", undefined],
    ["FIFO", "6", undefined],
    ["contiguous file", "7", undefined],
  ])("rejects a %s entry", async (_name, type, linkname) => {
    const f = await extractionFixture([
      ...PACKAGE_ENTRIES,
      { name: "package/lib/linked.js", type, linkname },
    ]);
    await expect(f.extract()).rejects.toThrow(
      /Unsupported SQLite archive entry type|links are not allowed/,
    );
    expect(existsSync(f.destination)).toBe(false);
  });

  it("rejects a reviewed path delivered as a symlink", async () => {
    const f = await extractionFixture([
      PACKAGE_ENTRIES[0],
      { name: "package/lib/index.js", type: "2", linkname: "/etc/passwd" },
      PACKAGE_ENTRIES[2],
    ]);
    await expect(f.extract()).rejects.toThrow(
      /Unsupported SQLite archive entry type/,
    );
    expect(existsSync(f.destination)).toBe(false);
  });

  it.each([
    [
      "entries outside the package root",
      [{ name: "other/file.js", data: "x" }],
    ],
    ["a root-level file", [{ name: "evil.js", data: "x" }]],
    [
      "a duplicate reviewed path",
      [{ name: "package/lib/index.js", data: "module.exports = 2;" }],
    ],
    ["a case-folded duplicate", [{ name: "package/LIB/index.js", data: "x" }]],
    [
      "a file shadowing a reviewed directory",
      [{ name: "package/LIB", data: "x" }],
    ],
  ])("rejects %s", async (_name, extra) => {
    const f = await extractionFixture([...PACKAGE_ENTRIES, ...extra]);
    await expect(f.extract()).rejects.toThrow(
      /must contain only package\/|Duplicate or conflicting|Conflicting/,
    );
    expect(existsSync(f.destination)).toBe(false);
  });

  it("rejects truncated payloads before writing", async () => {
    const f = await extractionFixture(PACKAGE_ENTRIES);
    const truncated = gzipSync(
      tar([...PACKAGE_ENTRIES, { name: "package/x", data: "abc", size: 4096 }]),
    );
    writeFileSync(f.archivePath, truncated);
    await expect(
      f.extract({ integrity: sha512Integrity(truncated) }),
    ).rejects.toThrow();
    expect(existsSync(f.destination)).toBe(false);
  });

  it.each([
    ["compressed size", { compressedBytes: 64 }, "compressed size limit"],
    ["expanded size", { expandedBytes: 2048 }, /./],
    ["entry count", { entries: 2 }, "entry limit"],
    ["file size", { fileBytes: 8 }, "file size limit"],
  ])("enforces the %s limit", async (_name, limit, message) => {
    const f = await extractionFixture(PACKAGE_ENTRIES);
    await expect(
      f.extract({ limits: { ...SQLITE_ARCHIVE_LIMITS, ...limit } }),
    ).rejects.toThrow(message);
    expect(existsSync(f.destination)).toBe(false);
  });

  it("does not follow a preexisting symlink inside the destination", async () => {
    const f = await extractionFixture(PACKAGE_ENTRIES);
    const outside = join(f.root, "outside");
    mkdirSync(outside);
    mkdirSync(f.destination);
    symlinkSync(outside, join(f.destination, "lib"), "junction");
    await expect(f.extract()).rejects.toThrow(
      "Unsafe SQLite extraction directory",
    );
    expect(readdirSync(outside)).toEqual([]);
  });

  it("refuses to overwrite a preexisting file or symlink at a destination path", async () => {
    const f = await extractionFixture(PACKAGE_ENTRIES);
    const outside = join(f.root, "outside.json");
    writeFileSync(outside, "outside");
    mkdirSync(f.destination);
    symlinkSync(outside, join(f.destination, "package.json"));
    await expect(f.extract()).rejects.toThrow(/EEXIST|ELOOP/);
    expect(readFileSync(outside, "utf8")).toBe("outside");
  });
});

async function runtimeFixture({
  allowInstall = () => true,
  target = FIXTURE_TARGET,
}: {
  allowInstall?: () => boolean | Promise<boolean>;
  target?: string;
} = {}) {
  const root = await createProjectTempDir("sqlite-install-security");
  const logs: string[] = [];
  configureSQLiteInstaller({
    storageRoot: join(root, "storage"),
    allowInstall,
    log: (line) => logs.push(line),
  });
  setSQLiteHostForTests({ probe: hostProbe(target) });
  return { root, logs };
}

const INSTALLED_FIXTURE_TREE = [
  ".rapidb-last-used",
  "node_modules/",
  "node_modules/better-sqlite3/",
  "node_modules/better-sqlite3/LICENSE",
  "node_modules/better-sqlite3/build/",
  "node_modules/better-sqlite3/build/Release/",
  "node_modules/better-sqlite3/build/Release/better_sqlite3.node",
  "node_modules/better-sqlite3/lib/",
  "node_modules/better-sqlite3/lib/index.js",
  "node_modules/better-sqlite3/package.json",
  "runtime.json",
];

function runtimeTree(root: string, prefix = ""): string[] {
  return readdirSync(join(root, prefix))
    .sort()
    .flatMap((name) => {
      const path = `${prefix}${name}`;
      return lstatSync(join(root, path)).isDirectory()
        ? [`${path}/`, ...runtimeTree(root, `${path}/`)]
        : [path];
    });
}

function installedPaths() {
  const probe = probeInstalledBetterSqlite3Runtime();
  const packagePath = probe.installedPackagePath as string;
  return {
    packagePath,
    binaryPath: probe.installedBinaryPath as string,
    runtimeRoot: dirname(dirname(packagePath)),
  };
}

describe("SQLite runtime installer", () => {
  it.each([
    false,
    true,
  ])("downloads the pinned npm tarball once, only after authorization (%s)", async (authorized) => {
    const allowInstall = vi.fn(() => authorized);
    const f = await runtimeFixture({ allowInstall });
    const pkg = pinFixturePackage();
    const urls = mockRegistry(() => ({ data: pkg.data }));
    const paths = installedPaths();
    expect(paths.runtimeRoot).toMatch(
      new RegExp(
        `sqlite-runtime[\\\\/]better-sqlite3[\\\\/]${SQLITE_ARTIFACTS.betterSqlite3Version.replaceAll(".", "\\.")}[\\\\/]${FIXTURE_TARGET}-pins-v6-[0-9a-f]{16}$`,
      ),
    );
    if (authorized) {
      const installed = await ensureSQLiteRuntimeInstalled();
      expect(installed).toBe(paths.packagePath);
      expect(readFileSync(paths.binaryPath, "utf8")).toBe("fixture addon");
      expect(urls).toEqual([TARBALL_URL]);
      expect(
        JSON.parse(
          readFileSync(join(paths.runtimeRoot, "runtime.json"), "utf8"),
        ),
      ).toMatchObject({
        schemaVersion: 3,
        betterSqlite3Version: SQLITE_ARTIFACTS.betterSqlite3Version,
        target: FIXTURE_TARGET,
        nodeApiVersion: SQLITE_ARTIFACTS.nodeApiVersion,
      });
      expect(readdirSync(dirname(paths.runtimeRoot))).toEqual([
        `${FIXTURE_TARGET}-${paths.runtimeRoot.split(`${FIXTURE_TARGET}-`).pop()}`,
      ]);
      await expect(ensureSQLiteRuntimeInstalled()).resolves.toBe(installed);
      expect(urls).toHaveLength(1);
      expect(f.logs.join("\n")).toContain(
        `Downloading better-sqlite3 ${SQLITE_ARTIFACTS.betterSqlite3Version}`,
      );
    } else {
      await expect(ensureSQLiteRuntimeInstalled()).rejects.toThrow(
        "SQLite runtime installation was not authorized",
      );
      expect(urls).toEqual([]);
      expect(existsSync(paths.runtimeRoot)).toBe(false);
    }
    expect(allowInstall).toHaveBeenCalledOnce();
  });

  it("reuses an installed runtime in a new window without network or a prompt", async () => {
    const f = await runtimeFixture();
    const pkg = pinFixturePackage();
    mockRegistry(() => ({ data: pkg.data }));
    const installed = await ensureSQLiteRuntimeInstalled();
    vi.restoreAllMocks();

    resetSQLiteInstallerForTests();
    const allowInstall = vi.fn(() => true);
    configureSQLiteInstaller({
      storageRoot: join(f.root, "storage"),
      allowInstall,
    });
    setSQLiteHostForTests({ probe: hostProbe(FIXTURE_TARGET) });
    const urls = mockRegistry(unexpectedDownload);
    await expect(ensureSQLiteRuntimeInstalled()).resolves.toBe(installed);
    expect(urls).toEqual([]);
    expect(allowInstall).not.toHaveBeenCalled();
  });

  it("accepts a runtime copied from another computer's storage for offline use", async () => {
    const f = await runtimeFixture();
    const pkg = pinFixturePackage();
    mockRegistry(() => ({ data: pkg.data }));
    await ensureSQLiteRuntimeInstalled();
    vi.restoreAllMocks();
    const offlineStorage = join(f.root, "offline-storage");
    cpSync(
      join(f.root, "storage", "sqlite-runtime"),
      join(offlineStorage, "sqlite-runtime"),
      { recursive: true },
    );

    resetSQLiteInstallerForTests();
    const allowInstall = vi.fn(() => true);
    configureSQLiteInstaller({ storageRoot: offlineStorage, allowInstall });
    setSQLiteHostForTests({ probe: hostProbe(FIXTURE_TARGET) });
    const urls = mockRegistry(unexpectedDownload);
    const installed = await ensureSQLiteRuntimeInstalled();
    expect(installed?.startsWith(offlineStorage)).toBe(true);
    expect(urls).toEqual([]);
    expect(allowInstall).not.toHaveBeenCalled();
  });

  it("ignores npm registry, mirror and proxy environment variables", async () => {
    await runtimeFixture();
    const pkg = pinFixturePackage();
    for (const name of [
      "npm_config_registry",
      "NPM_CONFIG_REGISTRY",
      "npm_config_better_sqlite3_binary_host",
      "npm_config_proxy",
      "npm_config_https_proxy",
      "npm_config_noproxy",
      "HTTPS_PROXY",
      "https_proxy",
      "HTTP_PROXY",
    ])
      vi.stubEnv(name, "https://evil.invalid:8080/");
    vi.stubEnv("npm_config_strict_ssl", "false");
    vi.stubEnv("NODE_TLS_REJECT_UNAUTHORIZED", "1");
    const urls = mockRegistry(() => ({ data: pkg.data }));
    await expect(ensureSQLiteRuntimeInstalled()).resolves.toBeTruthy();
    expect(urls).toEqual([TARBALL_URL]);
    // The request carries no agent/proxy/TLS options of its own: only the URL
    // and the response callback (VS Code's own proxy support still applies).
    const calls = vi.mocked(https.get).mock.calls as unknown[][];
    expect(calls).toHaveLength(1);
    expect(calls[0]).toHaveLength(2);
    expect(calls[0][0]).toBe(TARBALL_URL);
    expect(typeof calls[0][1]).toBe("function");
  });

  it("explains that installation is disabled in an untrusted workspace and how to allow it", async () => {
    const allowInstall = vi.fn(() => true);
    const f = await runtimeFixture({ allowInstall });
    configureSQLiteInstaller({
      storageRoot: join(f.root, "storage"),
      allowInstall,
      isTrusted: () => false,
    });
    const urls = mockRegistry(unexpectedDownload);
    const error = await ensureSQLiteRuntimeInstalled().catch((e) => e);
    expect(error).toBeInstanceOf(Error);
    expect(error.message).toMatch(/not trusted \(Restricted Mode\)/);
    expect(error.message).toContain("Manage Workspace Trust");
    expect(error.message).not.toContain("was not authorized");
    expect(allowInstall).not.toHaveBeenCalled();
    expect(urls).toEqual([]);
  });

  it("uses an already installed runtime in an untrusted workspace without asking", async () => {
    const f = await runtimeFixture();
    const pkg = pinFixturePackage();
    mockRegistry(() => ({ data: pkg.data }));
    const installed = await ensureSQLiteRuntimeInstalled();
    vi.restoreAllMocks();
    resetSQLiteInstallerForTests();
    const allowInstall = vi.fn(() => true);
    configureSQLiteInstaller({
      storageRoot: join(f.root, "storage"),
      allowInstall,
      isTrusted: () => false,
    });
    setSQLiteHostForTests({ probe: hostProbe(FIXTURE_TARGET) });
    const urls = mockRegistry(unexpectedDownload);
    await expect(ensureSQLiteRuntimeInstalled()).resolves.toBe(installed);
    expect(allowInstall).not.toHaveBeenCalled();
    expect(urls).toEqual([]);
  });

  it("labels download sizes in decimal megabytes", () => {
    expect(formatSQLiteDownloadSize(11_623_888)).toBe("11.6 MB");
    expect(sqliteRuntimeDownloadSummary()).toBe(
      `better-sqlite3 ${SQLITE_ARTIFACTS.betterSqlite3Version}, ${(SQLITE_ARTIFACTS.tarball.size / 1_000_000).toFixed(1)} MB from registry.npmjs.org`,
    );
  });

  it.each([
    ["linux-ia32", hostProbe("linux-ia32")],
    ["freebsd-x64", { platform: "freebsd" as NodeJS.Platform, arch: "x64" }],
    [
      "linux-x64 with an undeterminable C library",
      {
        platform: "linux" as NodeJS.Platform,
        arch: "x64",
        readProcessMaps: () => null,
        glibcVersionRuntime: () => undefined,
      },
    ],
  ])("refuses %s before asking or downloading", async (_name, probe) => {
    const allowInstall = vi.fn(() => true);
    await runtimeFixture({ allowInstall });
    setSQLiteHostForTests({ probe });
    const urls = mockRegistry(unexpectedDownload);
    await expect(ensureSQLiteRuntimeInstalled()).rejects.toThrow(
      /SQLite is not available on .*No binary was downloaded/,
    );
    expect(allowInstall).not.toHaveBeenCalled();
    expect(urls).toEqual([]);
    expect(probeInstalledBetterSqlite3Runtime()).toMatchObject({
      target: expect.stringContaining("unsupported"),
      installedPackagePath: null,
    });
  });

  it.each([
    "9",
    "",
    "not-a-number",
  ])("refuses a host with Node-API %j before asking or downloading", async (napi) => {
    const allowInstall = vi.fn(() => true);
    await runtimeFixture({ allowInstall });
    setSQLiteHostForTests({
      probe: hostProbe(FIXTURE_TARGET),
      nodeApiVersion: napi,
    });
    const urls = mockRegistry(unexpectedDownload);
    await expect(ensureSQLiteRuntimeInstalled()).rejects.toThrow(
      `SQLite requires Node-API ${SQLITE_ARTIFACTS.nodeApiVersion}`,
    );
    expect(allowInstall).not.toHaveBeenCalled();
    expect(urls).toEqual([]);
  });

  it("fails closed for a target without a reviewed prebuild pin", async () => {
    await runtimeFixture();
    delete SQLITE_ARTIFACTS.prebuilds[FIXTURE_TARGET];
    const urls = mockRegistry(unexpectedDownload);
    await expect(ensureSQLiteRuntimeInstalled()).rejects.toThrow(
      `no reviewed prebuild pin for ${FIXTURE_TARGET}`,
    );
    expect(urls).toEqual([]);
  });

  it.each([
    [
      "a different version",
      () => {
        SQLITE_ARTIFACTS.betterSqlite3Version = "13.9.9";
      },
    ],
    [
      "a non-registry tarball URL",
      () => {
        SQLITE_ARTIFACTS.tarball.url =
          "https://evil.invalid/better-sqlite3.tgz";
      },
    ],
    [
      "a weak tarball digest",
      () => {
        SQLITE_ARTIFACTS.tarball.integrity = `sha1-${Buffer.alloc(20).toString("base64")}`;
      },
    ],
    [
      "a remapped prebuild path",
      () => {
        SQLITE_ARTIFACTS.prebuilds[FIXTURE_TARGET].path =
          "prebuilds/linuxmusl-x64.node";
      },
    ],
    [
      "an unexpected pinned file",
      () => {
        SQLITE_ARTIFACTS.files["deps/download.sh"] = {
          sha256: "0".repeat(64),
          size: 1,
        };
      },
    ],
    [
      "a glibc prebuild without its minimum glibc",
      () => {
        delete SQLITE_ARTIFACTS.prebuilds[FIXTURE_TARGET].minGlibc;
      },
    ],
    [
      "a glibc prebuild without its minimum libstdc++",
      () => {
        delete SQLITE_ARTIFACTS.prebuilds[FIXTURE_TARGET].minGlibcxx;
      },
    ],
    [
      "a malformed minimum glibc",
      () => {
        SQLITE_ARTIFACTS.prebuilds[FIXTURE_TARGET].minGlibc = "2.34 or so";
      },
    ],
  ])("refuses a manifest with %s without downloading", async (_name, mutate) => {
    await runtimeFixture();
    mutate();
    const urls = mockRegistry(unexpectedDownload);
    await expect(ensureSQLiteRuntimeInstalled()).rejects.toThrow(
      /manifest|refusing to install/,
    );
    expect(urls).toEqual([]);
  });

  it("rejects same-size tarball substitution before extraction or publication", async () => {
    await runtimeFixture();
    const pkg = pinFixturePackage();
    const altered = Buffer.from(pkg.data);
    altered[altered.length - 1] ^= 1;
    const urls = mockRegistry(() => ({ data: altered }));
    await expect(ensureSQLiteRuntimeInstalled()).rejects.toThrow(
      "integrity mismatch",
    );
    expect(urls).toHaveLength(1);
    expect(existsSync(installedPaths().runtimeRoot)).toBe(false);
    expect(readdirSync(dirname(installedPaths().runtimeRoot))).toEqual([]);
  });

  it("rejects a binary substitution inside a re-pinned tarball", async () => {
    await runtimeFixture();
    const pkg = pinFixturePackage();
    const evil = gzipTar(
      Object.entries(pkg.files).map(([path, data]) => ({
        name: `package/${path}`,
        data: path.endsWith(".node") ? Buffer.from("evil addon!!!") : data,
      })),
    );
    pinTarball(evil);
    mockRegistry(() => ({ data: evil }));
    await expect(ensureSQLiteRuntimeInstalled()).rejects.toThrow(
      "does not match its reviewed SHA-256 pin",
    );
    expect(existsSync(installedPaths().runtimeRoot)).toBe(false);
  });

  it("rejects traversal and symlink entries inside the tarball without publishing", async () => {
    await runtimeFixture();
    for (const extra of [
      { name: "package/../../escape", data: "bad" },
      { name: "package/lib/link.js", type: "2", linkname: "/etc/passwd" },
    ]) {
      const pkg = pinFixturePackage({ extraEntries: [extra] });
      mockRegistry(() => ({ data: pkg.data }));
      await expect(ensureSQLiteRuntimeInstalled()).rejects.toThrow(
        /Unsafe|Unsupported/,
      );
      vi.restoreAllMocks();
    }
    expect(existsSync(installedPaths().runtimeRoot)).toBe(false);
  });

  it.each([
    [
      "an oversized Content-Length",
      (data: Buffer) => ({
        data,
        headers: { "content-length": String(data.length + 1) },
      }),
      "size limit",
    ],
    [
      "a body larger than the pin",
      (data: Buffer) => ({ data: Buffer.concat([data, Buffer.from("x")]) }),
      "size limit",
    ],
    [
      "a body smaller than the pin",
      (data: Buffer) => ({ data: data.subarray(0, data.length - 1) }),
      "does not match its reviewed pin",
    ],
    ["an HTTP error", () => ({ status: 404 }), "HTTP 404"],
  ])("rejects %s", async (_name, reply, message) => {
    await runtimeFixture();
    const pkg = pinFixturePackage();
    mockRegistry(() => reply(pkg.data));
    await expect(ensureSQLiteRuntimeInstalled()).rejects.toThrow(message);
    expect(existsSync(installedPaths().runtimeRoot)).toBe(false);
  });

  it.each([
    "https://evil.invalid/better-sqlite3.tgz",
    "http://registry.npmjs.org/better-sqlite3/-/better-sqlite3.tgz",
    "https://registry.npmjs.org.evil.invalid/better-sqlite3.tgz",
    "https://user@evil.invalid/",
    "https://registry.npmjs.org:8443/better-sqlite3/-/better-sqlite3.tgz",
    "https://registry.npmjs.org:80/better-sqlite3/-/better-sqlite3.tgz",
    "https://user:secret@registry.npmjs.org/better-sqlite3/-/better-sqlite3.tgz",
    "https://token@registry.npmjs.org/better-sqlite3/-/better-sqlite3.tgz",
  ])("refuses a redirect to %s", async (location) => {
    await runtimeFixture();
    const pkg = pinFixturePackage();
    const urls = mockRegistry((url) =>
      url === TARBALL_URL ? { status: 302, location } : { data: pkg.data },
    );
    await expect(ensureSQLiteRuntimeInstalled()).rejects.toThrow(
      "Refusing untrusted SQLite download host",
    );
    expect(urls).toEqual([TARBALL_URL]);
  });

  it.each([
    "https://registry.npmjs.org/better-sqlite3/-/redirected.tgz",
    "https://registry.npmjs.org:443/better-sqlite3/-/redirected.tgz",
  ])("follows a redirect that stays on the npm registry over HTTPS (%s)", async (mirror) => {
    await runtimeFixture();
    const pkg = pinFixturePackage();
    const urls = mockRegistry((url) =>
      url === TARBALL_URL
        ? { status: 307, location: mirror }
        : { data: pkg.data },
    );
    await expect(ensureSQLiteRuntimeInstalled()).resolves.toBeTruthy();
    expect(urls).toEqual([TARBALL_URL, new URL(mirror).toString()]);
  });

  it.each([
    [
      "a modified binary",
      (paths: ReturnType<typeof installedPaths>) =>
        writeFileSync(paths.binaryPath, "fixture addoN"),
    ],
    [
      "a modified JavaScript file",
      (paths: ReturnType<typeof installedPaths>) =>
        writeFileSync(join(paths.packagePath, "lib/index.js"), "evil()"),
    ],
    [
      "a missing file",
      (paths: ReturnType<typeof installedPaths>) =>
        rmSync(join(paths.packagePath, "package.json")),
    ],
    [
      "a binary replaced by a symlink to identical bytes",
      (paths: ReturnType<typeof installedPaths>) => {
        const copy = join(dirname(paths.runtimeRoot), "copy.node");
        cpSync(paths.binaryPath, copy);
        rmSync(paths.binaryPath);
        symlinkSync(copy, paths.binaryPath);
      },
    ],
    [
      "a corrupted runtime.json",
      (paths: ReturnType<typeof installedPaths>) =>
        writeFileSync(join(paths.runtimeRoot, "runtime.json"), "{not json"),
    ],
    [
      "a better-sqlite3 12.x runtime.json",
      (paths: ReturnType<typeof installedPaths>) =>
        writeFileSync(
          join(paths.runtimeRoot, "runtime.json"),
          JSON.stringify({
            betterSqlite3Version: "12.11.1",
            runtime: "electron",
            abi: "146",
            platform: "linux",
            arch: "x64",
            libc: "",
          }),
        ),
    ],
    [
      "a runtime for another target",
      (paths: ReturnType<typeof installedPaths>) => {
        const manifestPath = join(paths.runtimeRoot, "runtime.json");
        const manifest = JSON.parse(readFileSync(manifestPath, "utf8"));
        writeFileSync(
          manifestPath,
          JSON.stringify({ ...manifest, target: "linuxmusl-x64" }),
        );
      },
    ],
    [
      "an extra extensionless lib/util that require('./util') would load",
      (paths: ReturnType<typeof installedPaths>) =>
        writeFileSync(join(paths.packagePath, "lib", "util"), "evil()"),
    ],
    [
      "a case variant lib/Util",
      (paths: ReturnType<typeof installedPaths>) =>
        writeFileSync(join(paths.packagePath, "lib", "Util"), "evil()"),
    ],
    [
      "a pinned file renamed to a case variant",
      (paths: ReturnType<typeof installedPaths>) =>
        renameSync(
          join(paths.packagePath, "lib", "index.js"),
          join(paths.packagePath, "lib", "Index.js"),
        ),
    ],
    [
      "an extra directory",
      (paths: ReturnType<typeof installedPaths>) =>
        mkdirSync(join(paths.packagePath, "lib", "util")),
    ],
    [
      "an extra package in node_modules",
      (paths: ReturnType<typeof installedPaths>) =>
        mkdirSync(join(paths.runtimeRoot, "node_modules", "bindings")),
    ],
    [
      "an extra file in the runtime root",
      (paths: ReturnType<typeof installedPaths>) =>
        writeFileSync(join(paths.runtimeRoot, "notes.txt"), ""),
    ],
    [
      "a symlink inside the package",
      (paths: ReturnType<typeof installedPaths>) =>
        symlinkSync(
          join(paths.packagePath, "lib", "index.js"),
          join(paths.packagePath, "lib", "alias.js"),
        ),
    ],
    [
      "the use marker replaced by a directory",
      (paths: ReturnType<typeof installedPaths>) => {
        rmSync(join(paths.runtimeRoot, ".rapidb-last-used"));
        mkdirSync(join(paths.runtimeRoot, ".rapidb-last-used"));
      },
    ],
    [
      "a runtime directory replaced by a symlink",
      (paths: ReturnType<typeof installedPaths>) => {
        const moved = `${paths.runtimeRoot}-moved`;
        renameSync(paths.runtimeRoot, moved);
        symlinkSync(moved, paths.runtimeRoot, "junction");
      },
    ],
  ])("never loads a cache with %s and reinstalls it only after authorization", async (_name, tamper) => {
    const allowInstall = vi.fn(() => true);
    const f = await runtimeFixture({ allowInstall });
    const pkg = pinFixturePackage();
    mockRegistry(() => ({ data: pkg.data }));
    const installed = await ensureSQLiteRuntimeInstalled();
    vi.restoreAllMocks();
    const paths = installedPaths();
    tamper(paths);

    resetSQLiteInstallerForTests();
    const deny = vi.fn(() => false);
    configureSQLiteInstaller({
      storageRoot: join(f.root, "storage"),
      allowInstall: deny,
    });
    setSQLiteHostForTests({ probe: hostProbe(FIXTURE_TARGET) });
    const denied = mockRegistry(unexpectedDownload);
    await expect(ensureSQLiteRuntimeInstalled()).rejects.toThrow(
      "not authorized",
    );
    expect(deny).toHaveBeenCalledOnce();
    expect(denied).toEqual([]);
    vi.restoreAllMocks();

    resetSQLiteInstallerForTests();
    configureSQLiteInstaller({
      storageRoot: join(f.root, "storage"),
      allowInstall,
    });
    setSQLiteHostForTests({ probe: hostProbe(FIXTURE_TARGET) });
    const urls = mockRegistry(() => ({ data: pkg.data }));
    await expect(ensureSQLiteRuntimeInstalled()).resolves.toBe(installed);
    expect(urls).toEqual([TARBALL_URL]);
    expect(readFileSync(paths.binaryPath, "utf8")).toBe("fixture addon");
    expect(lstatSync(paths.runtimeRoot).isDirectory()).toBe(true);
    expect(runtimeTree(paths.runtimeRoot)).toEqual(INSTALLED_FIXTURE_TREE);
    expect(
      readdirSync(dirname(paths.runtimeRoot)).filter((entry) =>
        entry.includes(".obsolete-"),
      ),
    ).toEqual([]);
  });

  it("keys the cache by version, target and pins, not by the host ABI", async () => {
    await runtimeFixture();
    const before = installedPaths().runtimeRoot;
    const modules = Object.getOwnPropertyDescriptor(
      process.versions,
      "modules",
    );
    try {
      Object.defineProperty(process.versions, "modules", {
        value: "999",
        configurable: true,
      });
      expect(installedPaths().runtimeRoot).toBe(before);
    } finally {
      if (modules) Object.defineProperty(process.versions, "modules", modules);
    }
    setSQLiteHostForTests({ probe: hostProbe("linuxmusl-x64") });
    expect(installedPaths().runtimeRoot).not.toBe(before);
    expect(installedPaths().runtimeRoot).toContain("linuxmusl-x64-pins-v6-");
    expect(() => sqlitePackagePlan("linuxmusl-x64")).not.toThrow();
  });
});

async function exitedPid(): Promise<number> {
  const child = spawn(process.execPath, ["-e", ""], { stdio: "ignore" });
  await new Promise((resolvePromise) => child.once("exit", resolvePromise));
  return child.pid as number;
}

function sleep(milliseconds: number) {
  return new Promise((resolvePromise) =>
    setTimeout(resolvePromise, milliseconds),
  );
}

async function lockFixture() {
  const f = await runtimeFixture();
  const { runtimeRoot } = installedPaths();
  const lockRoot = `${runtimeRoot}.lock`;
  const pkg = pinFixturePackage({ binary: "fresh" });
  return { ...f, runtimeRoot, lockRoot, data: pkg.data };
}

function writeLock(
  lockRoot: string,
  owner: { pid: number; hostname: string; createdAt: number; token: string },
) {
  mkdirSync(lockRoot, { recursive: true });
  writeFileSync(join(lockRoot, "owner.json"), JSON.stringify(owner));
}

function liveOwner(token: string, createdAt = Date.now()) {
  return { pid: process.pid, hostname: hostname(), createdAt, token };
}

function lockToken(lockRoot: string) {
  return JSON.parse(readFileSync(join(lockRoot, "owner.json"), "utf8")).token;
}

async function actualRenameSync() {
  return (await vi.importActual<typeof import("node:fs")>("node:fs"))
    .renameSync;
}

describe("SQLite runtime installation lock recovery", () => {
  it("reclaims a lock left by a process that no longer exists", async () => {
    const f = await lockFixture();
    writeLock(f.lockRoot, {
      pid: await exitedPid(),
      hostname: hostname(),
      createdAt: Date.now(),
      token: "crashed",
    });
    const urls = mockRegistry(() => ({ data: f.data }));
    const started = Date.now();
    const installed = await ensureSQLiteRuntimeInstalled();
    expect(Date.now() - started).toBeLessThan(5_000);
    expect(urls).toHaveLength(1);
    expect(
      readFileSync(
        join(installed as string, "build/Release/better_sqlite3.node"),
        "utf8",
      ),
    ).toBe("fresh");
    expect(existsSync(f.lockRoot)).toBe(false);
  });

  it("reclaims an ownerless lock directory after the initialization grace period", async () => {
    const f = await lockFixture();
    mkdirSync(f.lockRoot, { recursive: true });
    const past = new Date(Date.now() - 60_000);
    utimesSync(f.lockRoot, past, past);
    const urls = mockRegistry(() => ({ data: f.data }));
    await expect(ensureSQLiteRuntimeInstalled()).resolves.toBeTruthy();
    expect(urls).toHaveLength(1);
    expect(existsSync(f.lockRoot)).toBe(false);
  });

  it("waits on an ownerless lock directory that is still within the initialization grace period", async () => {
    const f = await lockFixture();
    mkdirSync(f.lockRoot, { recursive: true });
    const urls = mockRegistry(() => ({ data: f.data }));
    const installation = ensureSQLiteRuntimeInstalled();
    await sleep(400);
    expect(urls).toHaveLength(0);
    expect(existsSync(f.lockRoot)).toBe(true);
    rmSync(f.lockRoot, { recursive: true, force: true });
    await expect(installation).resolves.toBeTruthy();
    expect(urls).toHaveLength(1);
  });

  it("does not steal a live owner's lock right after a sleep makes its heartbeat look old, but does once it stays unchanged", async () => {
    const f = await lockFixture();
    setSQLiteInstallLockTimingsForTests({ staleHeartbeatMs: 1_500 });
    writeLock(f.lockRoot, liveOwner("asleep"));
    const past = new Date(Date.now() - 10 * 60_000);
    utimesSync(join(f.lockRoot, "owner.json"), past, past);
    const urls = mockRegistry(() => ({ data: f.data }));
    const installation = ensureSQLiteRuntimeInstalled();
    await sleep(300);
    expect(urls).toHaveLength(0);
    expect(lockToken(f.lockRoot)).toBe("asleep");
    await expect(installation).resolves.toBeTruthy();
    expect(urls).toHaveLength(1);
    expect(existsSync(f.lockRoot)).toBe(false);
    expect(
      f.logs.filter((line) => line.includes("Removing stale")),
    ).toHaveLength(1);
  });

  it("does not steal a long-running live owner's lock while its heartbeat continues", async () => {
    const f = await lockFixture();
    setSQLiteInstallLockTimingsForTests({ staleHeartbeatMs: 300 });
    writeLock(f.lockRoot, liveOwner("long", Date.now() - 2 * 60 * 60_000));
    const beat = setInterval(() => {
      const now = new Date();
      utimesSync(join(f.lockRoot, "owner.json"), now, now);
    }, 50);
    const urls = mockRegistry(() => ({ data: f.data }));
    try {
      const installation = ensureSQLiteRuntimeInstalled();
      await sleep(900);
      expect(urls).toHaveLength(0);
      expect(lockToken(f.lockRoot)).toBe("long");
      clearInterval(beat);
      await expect(installation).resolves.toBeTruthy();
      expect(urls).toHaveLength(1);
    } finally {
      clearInterval(beat);
    }
  });

  it("refreshes its own heartbeat while installing", async () => {
    const f = await lockFixture();
    setSQLiteInstallLockTimingsForTests({ heartbeatMs: 50 });
    mockRegistry(() => ({ data: f.data }), 500);
    const installation = ensureSQLiteRuntimeInstalled();
    await sleep(100);
    const ownerPath = join(f.lockRoot, "owner.json");
    const first = statSync(ownerPath).mtimeMs;
    await sleep(250);
    expect(statSync(ownerPath).mtimeMs).toBeGreaterThan(first);
    await expect(installation).resolves.toBeTruthy();
    expect(existsSync(f.lockRoot)).toBe(false);
  });

  it("reclaims a lock whose creation time is implausibly far in the future", async () => {
    const f = await lockFixture();
    writeLock(f.lockRoot, liveOwner("future", Date.now() + 24 * 60 * 60_000));
    const urls = mockRegistry(() => ({ data: f.data }));
    await expect(ensureSQLiteRuntimeInstalled()).resolves.toBeTruthy();
    expect(urls).toHaveLength(1);
  });

  it("waits for a live owner instead of failing or stealing its lock", async () => {
    const f = await lockFixture();
    writeLock(f.lockRoot, liveOwner("live"));
    const urls = mockRegistry(() => ({ data: f.data }));
    const installation = ensureSQLiteRuntimeInstalled();
    await sleep(500);
    expect(urls).toHaveLength(0);
    expect(lockToken(f.lockRoot)).toBe("live");
    expect(f.logs.some((line) => line.includes("Waiting for another"))).toBe(
      true,
    );
    rmSync(f.lockRoot, { recursive: true, force: true });
    await expect(installation).resolves.toBeTruthy();
    expect(urls).toHaveLength(1);
  });

  it("returns as soon as another installer finishes, even while its lock remains", async () => {
    const f = await lockFixture();
    const urls = mockRegistry(() => ({ data: f.data }));
    await ensureSQLiteRuntimeInstalled();
    resetSQLiteInstallerForTests();
    configureSQLiteInstaller({
      storageRoot: join(f.root, "second-storage"),
      allowInstall: () => true,
    });
    setSQLiteHostForTests({ probe: hostProbe(FIXTURE_TARGET) });
    const target = installedPaths();
    mkdirSync(dirname(target.runtimeRoot), { recursive: true });
    writeLock(`${target.runtimeRoot}.lock`, liveOwner("other-window"));
    const installation = ensureSQLiteRuntimeInstalled();
    await sleep(300);
    cpSync(f.runtimeRoot, target.runtimeRoot, { recursive: true });
    await expect(installation).resolves.toBe(target.packagePath);
    expect(urls).toHaveLength(1);
    expect(lockToken(`${target.runtimeRoot}.lock`)).toBe("other-window");
  });

  it("reports a successful install even when releasing the lock keeps failing", async () => {
    const f = await lockFixture();
    setSQLiteInstallLockTimingsForTests({
      releaseAttempts: 2,
      releaseRetryMs: 10,
    });
    const rename = await actualRenameSync();
    vi.mocked(renameSync).mockImplementation((from, to) => {
      if (from === f.lockRoot) {
        throw Object.assign(new Error("EPERM: operation not permitted"), {
          code: "EPERM",
        });
      }
      rename(from, to);
    });
    mockRegistry(() => ({ data: f.data }));
    await expect(ensureSQLiteRuntimeInstalled()).resolves.toBeTruthy();
    expect(
      f.logs.some((line) =>
        line.includes("Could not release SQLite runtime installation lock"),
      ),
    ).toBe(true);
    vi.mocked(renameSync).mockReset();
  });

  it("retries a takeover that hits a transient filesystem error", async () => {
    const f = await lockFixture();
    writeLock(f.lockRoot, {
      pid: await exitedPid(),
      hostname: hostname(),
      createdAt: Date.now(),
      token: "crashed",
    });
    const rename = await actualRenameSync();
    let failures = 0;
    vi.mocked(renameSync).mockImplementation((from, to) => {
      if (from === f.lockRoot && failures < 2) {
        failures += 1;
        throw Object.assign(new Error("EBUSY: resource busy"), {
          code: "EBUSY",
        });
      }
      rename(from, to);
    });
    const urls = mockRegistry(() => ({ data: f.data }));
    await expect(ensureSQLiteRuntimeInstalled()).resolves.toBeTruthy();
    expect(failures).toBe(2);
    expect(urls).toHaveLength(1);
    expect(f.logs.filter((line) => line.includes("is busy"))).toHaveLength(1);
    expect(
      f.logs.filter((line) => line.includes("Removing stale")),
    ).toHaveLength(1);
  });

  it("restores a lock that changed owner during takeover and keeps waiting without spinning", async () => {
    const f = await lockFixture();
    writeLock(f.lockRoot, {
      pid: await exitedPid(),
      hostname: hostname(),
      createdAt: Date.now(),
      token: "crashed",
    });
    const rename = await actualRenameSync();
    let takeovers = 0;
    vi.mocked(renameSync).mockImplementation((from, to) => {
      if (from === f.lockRoot && String(to).includes(".stale-")) {
        takeovers += 1;
        if (takeovers === 1) {
          writeFileSync(
            join(f.lockRoot, "owner.json"),
            JSON.stringify(liveOwner("replacement")),
          );
        }
      }
      rename(from, to);
    });
    const urls = mockRegistry(() => ({ data: f.data }));
    const installation = ensureSQLiteRuntimeInstalled();
    await sleep(400);
    expect(takeovers).toBe(1);
    expect(urls).toHaveLength(0);
    expect(lockToken(f.lockRoot)).toBe("replacement");
    rmSync(f.lockRoot, { recursive: true, force: true });
    await expect(installation).resolves.toBeTruthy();
    expect(takeovers).toBe(3);
  });

  it("leaves a mismatched tombstone in place when it cannot be restored", async () => {
    const f = await lockFixture();
    writeLock(f.lockRoot, {
      pid: await exitedPid(),
      hostname: hostname(),
      createdAt: Date.now(),
      token: "crashed",
    });
    const rename = await actualRenameSync();
    let tombstone = "";
    vi.mocked(renameSync).mockImplementation((from, to) => {
      if (from === f.lockRoot && String(to).includes(".stale-") && !tombstone) {
        tombstone = String(to);
        writeFileSync(
          join(f.lockRoot, "owner.json"),
          JSON.stringify(liveOwner("replacement")),
        );
      } else if (from === tombstone && to === f.lockRoot) {
        throw Object.assign(new Error("EEXIST: file already exists"), {
          code: "EEXIST",
        });
      }
      rename(from, to);
    });
    mockRegistry(() => ({ data: f.data }));
    await expect(ensureSQLiteRuntimeInstalled()).resolves.toBeTruthy();
    expect(lockToken(tombstone)).toBe("replacement");
    expect(f.logs.some((line) => line.includes("Leaving it in place"))).toBe(
      true,
    );
  });

  it("does not release a lock that another installer re-created after a takeover", async () => {
    const f = await lockFixture();
    const foreign = liveOwner("foreign");
    mockRegistry(() => {
      renameSync(f.lockRoot, `${f.lockRoot}-taken`);
      writeLock(f.lockRoot, foreign);
      return { data: f.data };
    });
    await expect(ensureSQLiteRuntimeInstalled()).resolves.toBeTruthy();
    expect(
      JSON.parse(readFileSync(join(f.lockRoot, "owner.json"), "utf8")),
    ).toEqual(foreign);
  });

  it("lets only one of two concurrent installers (windows) download while the other waits", async () => {
    const f = await lockFixture();
    const installerPath = "../../src/extension/utils/sqliteInstaller";
    const instances = [
      await import(installerPath),
      await import(`${installerPath}?second-window`),
    ] as typeof import("../../src/extension/utils/sqliteInstaller")[];
    expect(instances[0]).not.toBe(instances[1]);
    for (const instance of instances) {
      instance.configureSQLiteInstaller({
        storageRoot: join(f.root, "storage"),
        allowInstall: () => true,
      });
      instance.setSQLiteHostForTests({ probe: hostProbe(FIXTURE_TARGET) });
    }
    const urls = mockRegistry(() => ({ data: f.data }), 200);
    const [first, second] = await Promise.all(
      instances.map((instance) => instance.ensureSQLiteRuntimeInstalled()),
    );
    instances[1].resetSQLiteInstallerForTests();
    expect(urls).toHaveLength(1);
    expect(first).toBeTruthy();
    expect(second).toBe(first);
    expect(existsSync(`${dirname(dirname(first as string))}.lock`)).toBe(false);
    expect(
      readdirSync(dirname(dirname(dirname(first as string)))).filter((entry) =>
        entry.includes(".tmp-"),
      ),
    ).toEqual([]);
  });

  it("asks for installation consent once for concurrent first-time connects", async () => {
    const f = await lockFixture();
    let approve: (value: boolean) => void = () => {};
    const allowInstall = vi.fn(
      () =>
        new Promise<boolean>((resolvePromise) => {
          approve = resolvePromise;
        }),
    );
    configureSQLiteInstaller({
      storageRoot: join(f.root, "storage"),
      allowInstall,
    });
    const urls = mockRegistry(() => ({ data: f.data }));
    const first = ensureSQLiteRuntimeInstalled();
    const second = ensureSQLiteRuntimeInstalled();
    await sleep(50);
    approve(true);
    const [firstPath, secondPath] = await Promise.all([first, second]);
    expect(allowInstall).toHaveBeenCalledTimes(1);
    expect(urls).toHaveLength(1);
    expect(secondPath).toBe(firstPath);
  });

  it("publishes atomically: a failed download leaves neither a runtime nor a staging directory", async () => {
    const f = await lockFixture();
    mockRegistry(() => ({ status: 503 }));
    await expect(ensureSQLiteRuntimeInstalled()).rejects.toThrow("HTTP 503");
    expect(readdirSync(dirname(f.runtimeRoot))).toEqual([]);
  });

  it("removes abandoned temporary installs and lock tombstones but keeps ones owned by live processes", async () => {
    const f = await lockFixture();
    const deadPid = await exitedPid();
    const abandonedTemp = `${f.runtimeRoot}.tmp-${deadPid}-1`;
    const abandonedTombstone = `${f.lockRoot}.stale-${deadPid}-x`;
    const activeTemp = `${f.runtimeRoot}.tmp-${process.ppid}-1`;
    const activeTombstone = `${f.lockRoot}.stale-${process.pid}-y`;
    for (const directory of [abandonedTemp, abandonedTombstone, activeTemp]) {
      mkdirSync(join(directory, "node_modules"), { recursive: true });
    }
    const past = new Date(Date.now() - 60_000);
    utimesSync(abandonedTombstone, past, past);
    writeLock(activeTombstone, liveOwner("misplaced"));
    mockRegistry(() => ({ data: f.data }));
    await expect(ensureSQLiteRuntimeInstalled()).resolves.toBeTruthy();
    expect(existsSync(abandonedTemp)).toBe(false);
    expect(existsSync(abandonedTombstone)).toBe(false);
    expect(existsSync(activeTemp)).toBe(true);
    expect(existsSync(activeTombstone)).toBe(true);
    expect(existsSync(f.lockRoot)).toBe(false);
  });
});

function glibcHost(
  target: string,
  glibc: string | null | undefined,
  libstdcxx?: string,
): SQLiteHostProbe {
  const base = hostProbe(target);
  return {
    ...base,
    readProcessMaps: () =>
      `7f00-7f01 r-xp 0 0:0 0 /usr/lib/x86_64-linux-gnu/libc.so.6\n${
        libstdcxx
          ? `7f02-7f03 r-xp 0 0:0 0 /usr/lib/x86_64-linux-gnu/libstdc++.so.${libstdcxx}\n`
          : ""
      }`,
    glibcVersionRuntime: () => glibc,
  };
}

describe("SQLite Linux C/C++ runtime requirements", () => {
  it.each([
    ["linux-x64", "2.28"],
    ["linux-x64", "2.31"],
    ["linux-x64", "2.33"],
    ["linux-arm64", "2.31"],
  ] as const)("refuses %s with glibc %s before asking or downloading", async (target, glibc) => {
    const allowInstall = vi.fn(() => true);
    const f = await runtimeFixture({ allowInstall, target });
    setSQLiteHostForTests({ probe: glibcHost(target, glibc) });
    const urls = mockRegistry(unexpectedDownload);
    const error = await ensureSQLiteRuntimeInstalled().catch((e) => e);
    expect(error).toBeInstanceOf(SQLiteUnsupportedTargetError);
    expect(error.message).toContain(
      `SQLite runtime requires glibc 2.34 or newer (found ${glibc})`,
    );
    expect(error.message).toContain("Ubuntu 20.04");
    expect(error.message).toContain("GLIBCXX_3.4.29");
    expect(error.message).toContain("No binary was downloaded");
    expect(allowInstall).not.toHaveBeenCalled();
    expect(urls).toEqual([]);
    expect(existsSync(join(f.root, "storage", "sqlite-runtime"))).toBe(false);
  });

  it.each(["2.34", "2.35.1", "2.39"])("installs on glibc %s", async (glibc) => {
    const allowInstall = vi.fn(() => true);
    await runtimeFixture({ allowInstall });
    setSQLiteHostForTests({
      probe: glibcHost(FIXTURE_TARGET, glibc, "6.0.30"),
    });
    const pkg = pinFixturePackage();
    const urls = mockRegistry(() => ({ data: pkg.data }));
    await expect(ensureSQLiteRuntimeInstalled()).resolves.toBe(
      installedPaths().packagePath,
    );
    expect(urls).toEqual([TARBALL_URL]);
  });

  it.each([
    ["no process report", undefined],
    ["an empty version", ""],
    ["a word", "unknown"],
    ["a single number", "2"],
    ["a prefixed version", "glibc 2.39"],
  ])("fails closed on a glibc host with %s", async (_name, glibc) => {
    const allowInstall = vi.fn(() => true);
    await runtimeFixture({ allowInstall });
    setSQLiteHostForTests({ probe: glibcHost(FIXTURE_TARGET, glibc) });
    const urls = mockRegistry(unexpectedDownload);
    await expect(ensureSQLiteRuntimeInstalled()).rejects.toThrow(
      /requires glibc 2\.34 or newer, but RapiDB could not determine the glibc version.*No binary was downloaded/,
    );
    expect(allowInstall).not.toHaveBeenCalled();
    expect(urls).toEqual([]);
  });

  it("refuses a process that already loaded an older libstdc++", async () => {
    const allowInstall = vi.fn(() => true);
    await runtimeFixture({ allowInstall });
    setSQLiteHostForTests({
      probe: glibcHost(FIXTURE_TARGET, "2.35", "6.0.28"),
    });
    const urls = mockRegistry(unexpectedDownload);
    await expect(ensureSQLiteRuntimeInstalled()).rejects.toThrow(
      "SQLite runtime requires libstdc++ with GLIBCXX_3.4.29 or newer (found GLIBCXX_3.4.28)",
    );
    expect(allowInstall).not.toHaveBeenCalled();
    expect(urls).toEqual([]);
  });

  it("does not apply glibc requirements to musl, macOS or Windows", async () => {
    for (const target of ["linuxmusl-x64", "darwin-arm64", "win32-x64"]) {
      expect(sqlitePackagePlan(target as never).libraryRequirements).toEqual(
        {},
      );
    }
    expect(sqlitePackagePlan("linux-arm64").libraryRequirements).toEqual({
      minGlibc: "2.34",
      minGlibcxx: "3.4.29",
    });
  });

  it.each([
    "Error: /lib/x86_64-linux-gnu/libc.so.6: version `GLIBC_2.34' not found (required by /x/better_sqlite3.node)",
    "/lib64/libstdc++.so.6: version `GLIBCXX_3.4.29' not found",
    "/usr/lib/libstdc++.so.6: version `CXXABI_1.3.13' not found",
  ])("explains the glibc/libstdc++ requirement when loading fails with %j", (message) => {
    const formatted = formatSQLiteRuntimeLoadErrorMessage(
      new Error(message),
      [],
      null,
    );
    expect(formatted).toContain("requires glibc 2.34 or newer");
    expect(formatted).toContain("GLIBCXX_3.4.29");
    expect(formatted).toContain("Debian 10 and 11");
    expect(formatted).toContain(message);
  });

  it("adds no glibc hint to unrelated load failures", () => {
    expect(
      formatSQLiteRuntimeLoadErrorMessage(
        new Error("Cannot find module 'x'"),
        [],
        null,
      ),
    ).not.toContain("glibc");
  });
});

describe("SQLite runtime replacement", () => {
  async function installed() {
    const allowInstall = vi.fn(() => true);
    const f = await runtimeFixture({ allowInstall });
    const pkg = pinFixturePackage();
    mockRegistry(() => ({ data: pkg.data }));
    const packagePath = await ensureSQLiteRuntimeInstalled();
    vi.restoreAllMocks();
    resetSQLiteInstallerForTests();
    const prompt = vi.fn(() => true);
    configureSQLiteInstaller({
      storageRoot: join(f.root, "storage"),
      allowInstall: prompt,
      log: (line) => f.logs.push(line),
    });
    setSQLiteHostForTests({ probe: hostProbe(FIXTURE_TARGET) });
    return { ...f, pkg, packagePath, prompt, paths: installedPaths() };
  }

  it.each([
    "EBUSY",
    "EPERM",
    "EACCES",
    "EMFILE",
  ])("treats %s while reading the runtime as temporary: no prompt, no replacement", async (code) => {
    const f = await installed();
    const actual = (await vi.importActual<typeof import("node:fs")>("node:fs"))
      .readFileSync;
    vi.mocked(readFileSync).mockImplementation(((
      path: Parameters<typeof actual>[0],
      ...rest: unknown[]
    ) => {
      if (String(path) === f.paths.binaryPath)
        throw Object.assign(new Error(`${code}: busy`), { code });
      return (actual as (...args: unknown[]) => unknown)(path, ...rest);
    }) as typeof readFileSync);
    const urls = mockRegistry(unexpectedDownload);

    const error = await ensureSQLiteRuntimeInstalled().catch((e) => e);
    expect(error).toBeInstanceOf(SQLiteRuntimeUnreadableError);
    expect(error.message).toContain(`could not be read right now (${code})`);
    expect(f.prompt).not.toHaveBeenCalled();
    expect(urls).toEqual([]);
    expect(runtimeTree(f.paths.runtimeRoot)).toEqual(INSTALLED_FIXTURE_TREE);
    expect(readdirSync(dirname(f.paths.runtimeRoot))).toEqual([
      basename(f.paths.runtimeRoot),
    ]);

    vi.mocked(readFileSync).mockReset();
    await expect(ensureSQLiteRuntimeInstalled()).resolves.toBe(f.packagePath);
    expect(urls).toEqual([]);
  });

  it("keeps a damaged runtime intact and explains why when it cannot be moved aside", async () => {
    const f = await installed();
    writeFileSync(f.paths.binaryPath, "tampered");
    const actual = await actualRenameSync();
    vi.mocked(renameSync).mockImplementation((from, to) => {
      if (String(to).includes(".obsolete-"))
        throw Object.assign(new Error("in use"), { code: "EPERM" });
      actual(from, to);
    });
    const urls = mockRegistry(unexpectedDownload);

    const error = await ensureSQLiteRuntimeInstalled().catch((e) => e);
    expect(error).toBeInstanceOf(SQLiteRuntimeInUseError);
    expect(error.message).toContain(
      "Close SQLite connections in other VS Code windows",
    );
    expect(f.prompt).toHaveBeenCalledOnce();
    expect(urls).toEqual([]);
    expect(runtimeTree(f.paths.runtimeRoot)).toEqual(INSTALLED_FIXTURE_TREE);
    expect(readFileSync(f.paths.binaryPath, "utf8")).toBe("tampered");
    expect(readdirSync(dirname(f.paths.runtimeRoot))).toEqual([
      basename(f.paths.runtimeRoot),
    ]);
  });

  it("moves a damaged runtime aside atomically before replacing it", async () => {
    const f = await installed();
    writeFileSync(f.paths.binaryPath, "tampered");
    const urls = mockRegistry(() => ({ data: f.pkg.data }));
    await expect(ensureSQLiteRuntimeInstalled()).resolves.toBe(f.packagePath);
    expect(urls).toEqual([TARBALL_URL]);
    expect(
      vi
        .mocked(renameSync)
        .mock.calls.some(
          ([from, to]) =>
            from === f.paths.runtimeRoot &&
            new RegExp(
              `^${basename(f.paths.runtimeRoot)}\\.obsolete-${process.pid}-[0-9a-f-]{36}$`,
            ).test(basename(String(to))),
        ),
    ).toBe(true);
    expect(readFileSync(f.paths.binaryPath, "utf8")).toBe("fixture addon");
    expect(f.logs.join("\n")).toContain("it failed verification");
  });

  it("removes leftover replaced runtimes on the next installation without following links", async () => {
    const f = await runtimeFixture();
    const { runtimeRoot } = installedPaths();
    const leftover = `${runtimeRoot}.obsolete-123-${"a".repeat(8)}-${"b".repeat(4)}-${"c".repeat(4)}-${"d".repeat(4)}-${"e".repeat(12)}`;
    mkdirSync(join(leftover, "node_modules"), { recursive: true });
    writeFileSync(join(leftover, "runtime.json"), "{}");
    const outside = join(f.root, "outside");
    mkdirSync(outside);
    writeFileSync(join(outside, "keep.txt"), "keep");
    const linked = `${runtimeRoot}.obsolete-124-${"1".repeat(8)}-${"2".repeat(4)}-${"3".repeat(4)}-${"4".repeat(4)}-${"5".repeat(12)}`;
    symlinkSync(outside, linked, "dir");
    const unrelated = `${runtimeRoot}.obsolete-notes`;
    mkdirSync(unrelated);
    const pkg = pinFixturePackage();
    mockRegistry(() => ({ data: pkg.data }));

    await expect(ensureSQLiteRuntimeInstalled()).resolves.toBeTruthy();
    expect(existsSync(leftover)).toBe(false);
    expect(() => lstatSync(linked)).toThrow();
    expect(readFileSync(join(outside, "keep.txt"), "utf8")).toBe("keep");
    expect(existsSync(unrelated)).toBe(true);
  });
});

describe("SQLite runtime download progress and cancellation", () => {
  function progressController() {
    const reports: [number, number][] = [];
    let controller = new AbortController();
    const calls = { count: 0 };
    const withInstallProgress = <T>(
      task: (progress: SQLiteInstallProgress) => Promise<T>,
    ): Promise<T> => {
      calls.count += 1;
      controller = new AbortController();
      return task({
        signal: controller.signal,
        report: (received, total) => reports.push([received, total]),
      });
    };
    return {
      reports,
      calls,
      withInstallProgress,
      cancel: () => controller.abort(),
    };
  }

  it("reports progress by bytes for an installation", async () => {
    const f = await runtimeFixture();
    const progress = progressController();
    configureSQLiteInstaller({
      storageRoot: join(f.root, "storage"),
      withInstallProgress: progress.withInstallProgress,
    });
    const pkg = pinFixturePackage();
    mockRegistry(() => ({ data: pkg.data }));
    await expect(ensureSQLiteRuntimeInstalled()).resolves.toBeTruthy();
    expect(progress.calls.count).toBe(1);
    expect(progress.reports.at(-1)).toEqual([pkg.data.length, pkg.data.length]);
  });

  it("aborts the download on explicit cancellation, cleans up and allows a retry", async () => {
    const f = await runtimeFixture();
    const progress = progressController();
    configureSQLiteInstaller({
      storageRoot: join(f.root, "storage"),
      allowInstall: () => true,
      withInstallProgress: progress.withInstallProgress,
    });
    const pkg = pinFixturePackage();
    mockRegistry(() => ({ data: pkg.data }), 200);
    const { runtimeRoot } = installedPaths();

    const first = ensureSQLiteRuntimeInstalled();
    const second = ensureSQLiteRuntimeInstalled();
    await sleep(50);
    expect(existsSync(`${runtimeRoot}.lock`)).toBe(true);
    progress.cancel();
    await expect(first).rejects.toBeInstanceOf(
      SQLiteRuntimeInstallCancelledError,
    );
    await expect(second).rejects.toThrow("download was cancelled");
    await sleep(250);
    expect(readdirSync(dirname(runtimeRoot))).toEqual([]);

    vi.restoreAllMocks();
    const urls = mockRegistry(() => ({ data: pkg.data }));
    await expect(ensureSQLiteRuntimeInstalled()).resolves.toBe(
      installedPaths().packagePath,
    );
    expect(urls).toEqual([TARBALL_URL]);
    expect(progress.calls.count).toBe(2);
  });
});
