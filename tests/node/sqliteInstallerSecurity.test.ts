import { execFile } from "node:child_process";
import { createHash } from "node:crypto";
import { EventEmitter } from "node:events";
import {
  chmodSync,
  existsSync,
  mkdirSync,
  readFileSync,
  realpathSync,
  symlinkSync,
  writeFileSync,
} from "node:fs";
import { createRequire } from "node:module";
import { delimiter, join } from "node:path";
import { Readable } from "node:stream";
import { promisify } from "node:util";
import { gzipSync } from "node:zlib";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  extractSQLiteArchive,
  SQLITE_ARCHIVE_LIMITS,
  SQLITE_SOURCE,
  verifySQLiteIntegrity,
} from "../../src/extension/utils/sqliteArchive";
import {
  SQLITE_ARTIFACT_POLICY_ID,
  SQLITE_ARTIFACTS,
  type SQLiteHeaderPins,
  sqliteHeaderPins,
} from "../../src/extension/utils/sqliteArtifactPins";
import {
  createSQLiteBuildToolLock,
  pinSQLiteSourceNodeActions,
  resolveSQLiteBuildTools,
  sqliteBuildEnvironment,
} from "../../src/extension/utils/sqliteBuildTools";
import {
  configureSQLiteInstaller,
  downloadToFile,
  ensureSQLiteRuntimeInstalled,
  preparePinnedSQLiteHeaders,
  probeInstalledBetterSqlite3Runtime,
  resetSQLiteInstallerForTests,
  sqliteNodeGypArguments,
} from "../../src/extension/utils/sqliteInstaller";
import { createProjectTempDir } from "../runtime/tempDirectories";

const require = createRequire(import.meta.url);
const https = require("node:https") as typeof import("node:https");
const versions = Object.getOwnPropertyDescriptors(process.versions);
const originalPrebuilts = { ...SQLITE_ARTIFACTS.prebuilts };
const originalHeaders = { ...SQLITE_ARTIFACTS.headers };

afterEach(() => {
  vi.restoreAllMocks();
  Object.defineProperties(process.versions, versions);
  if (!versions.electron)
    delete (process.versions as { electron?: string }).electron;
  resetSQLiteInstallerForTests();
  SQLITE_ARTIFACTS.prebuilts = { ...originalPrebuilts };
  SQLITE_ARTIFACTS.headers = { ...originalHeaders };
});

type Entry = {
  name: string;
  data?: string | Buffer;
  type?: string;
  linkname?: string;
  size?: number;
};

// Build raw USTAR headers, so malformed names/types are not normalized by the
// fixture builder before they reach the production parser.
function tar(entries: Entry[]): Buffer {
  const chunks: Buffer[] = [];
  for (const entry of entries) {
    const header = Buffer.alloc(512);
    const data = Buffer.from(entry.data ?? "");
    header.write(entry.name, 0, 100);
    header.write("0000600\0", 100);
    header.write("0000000\0", 108);
    header.write("0000000\0", 116);
    header.write(
      `${(entry.size ?? data.length).toString(8).padStart(11, "0")}\0`,
      124,
    );
    header.write("00000000000\0", 136);
    header.fill(32, 148, 156);
    header.write(entry.type ?? "0", 156);
    if (entry.linkname) header.write(entry.linkname, 157, 100);
    header.write("ustar\0", 257);
    header.write("00", 263);
    const checksum = header.reduce((sum, byte) => sum + byte, 0);
    header.write(`${checksum.toString(8).padStart(6, "0")}\0 `, 148);
    chunks.push(header, data, Buffer.alloc((512 - (data.length % 512)) % 512));
  }
  return Buffer.concat([...chunks, Buffer.alloc(1024)]);
}

function archive(entries: Entry[]): Buffer {
  return gzipSync(tar(entries));
}
function integrity(data: Buffer): string {
  return `sha512-${createHash("sha512").update(data).digest("base64")}`;
}

async function fixture(entries: Entry[]) {
  const root = await createProjectTempDir("sqlite-security");
  const archivePath = join(root, "archive.tgz");
  const data = archive(entries);
  writeFileSync(archivePath, data);
  return { root, archivePath, data, destination: join(root, "extract") };
}

describe("SQLite archive integrity and filesystem boundaries", () => {
  it("pins npm source to the repository lockfile, including version and URL", () => {
    const lock = JSON.parse(
      readFileSync(new URL("../../package-lock.json", import.meta.url), "utf8"),
    );
    expect(SQLITE_SOURCE).toEqual({
      version: lock.packages["node_modules/better-sqlite3"].version,
      url: lock.packages["node_modules/better-sqlite3"].resolved,
      integrity: lock.packages["node_modules/better-sqlite3"].integrity,
    });
  });

  it("checks a trusted digest before even decompressing, without writing files", async () => {
    const f = await fixture([{ name: "package/package.json", data: "{}" }]);
    await expect(
      extractSQLiteArchive(
        f.archivePath,
        f.destination,
        "source",
        SQLITE_SOURCE.integrity,
      ),
    ).rejects.toThrow("integrity mismatch");
    expect(existsSync(f.destination)).toBe(false);
    expect(() => verifySQLiteIntegrity(f.data, "sha512-not-a-digest")).toThrow(
      "Invalid pinned",
    );
    await extractSQLiteArchive(
      f.archivePath,
      f.destination,
      "source",
      integrity(f.data),
    );
    expect(readFileSync(join(f.destination, "package.json"), "utf8")).toBe(
      "{}",
    );
  });

  it("requires a trusted source pin and rejects a mismatching prebuilt pin before writes", async () => {
    const f = await fixture([
      { name: "build/Release/better_sqlite3.node", data: "addon" },
    ]);
    await expect(
      extractSQLiteArchive(f.archivePath, f.destination, "source", ""),
    ).rejects.toThrow("Trusted static integrity is required");
    await expect(
      extractSQLiteArchive(f.archivePath, f.destination, "prebuilt", ""),
    ).rejects.toThrow("Trusted static integrity is required");
    await expect(
      extractSQLiteArchive(
        f.archivePath,
        f.destination,
        "prebuilt",
        integrity(Buffer.from("different archive")),
      ),
    ).rejects.toThrow("integrity mismatch");
    expect(existsSync(f.destination)).toBe(false);
  });

  it.each([
    "../escape",
    "package/../../escape",
    "/absolute",
    "package/C:/escape",
    "package/..\\escape",
    "package/NUL.js",
    "package/folder./escape",
  ])("rejects traversal/absolute/platform-ambiguous path %s before writes", async (name) => {
    const f = await fixture([
      { name: "package/first", data: "safe" },
      { name, data: "bad" },
    ]);
    await expect(
      extractSQLiteArchive(
        f.archivePath,
        f.destination,
        "source",
        integrity(f.data),
      ),
    ).rejects.toThrow(/Unsafe|only package/);
    expect(existsSync(f.destination)).toBe(false);
  });

  it.each([
    "1",
    "2",
    "3",
    "4",
    "6",
  ])("rejects links, devices, and FIFOs (tar type %s)", async (type) => {
    const f = await fixture([
      { name: "package/link", type, linkname: "../../escape" },
    ]);
    await expect(
      extractSQLiteArchive(
        f.archivePath,
        f.destination,
        "source",
        integrity(f.data),
      ),
    ).rejects.toThrow("Unsupported");
    expect(existsSync(f.destination)).toBe(false);
  });

  it("validates effective PAX and GNU long paths, not only the next raw header", async () => {
    const recordBody = "path=package/../../escape\n";
    let record = `0 ${recordBody}`;
    while (record.length !== Number(record.split(" ")[0]))
      record = `${record.length} ${recordBody}`;
    for (const metadata of [
      { name: "PaxHeader", type: "x", data: record },
      { name: "././@LongLink", type: "L", data: "package/../../escape\0" },
    ]) {
      const f = await fixture([
        metadata,
        { name: "package/safe", data: "bad" },
      ]);
      await expect(
        extractSQLiteArchive(
          f.archivePath,
          f.destination,
          "source",
          integrity(f.data),
        ),
      ).rejects.toThrow("Unsafe");
      expect(existsSync(f.destination)).toBe(false);
    }
  });

  it.each(
    [
      [
        { name: "package/a", data: "1" },
        { name: "package/a", data: "2" },
      ],
      [
        { name: "package/a", data: "1" },
        { name: "package/A", data: "2" },
      ],
      [
        { name: "package/a", data: "1" },
        { name: "package/A/child", data: "2" },
      ],
    ].map((entries) => ({ entries })),
  )("rejects duplicates and file/directory conflicts before writes", async ({
    entries,
  }) => {
    const f = await fixture(entries);
    await expect(
      extractSQLiteArchive(
        f.archivePath,
        f.destination,
        "source",
        integrity(f.data),
      ),
    ).rejects.toThrow(/Duplicate|Conflicting/);
    expect(existsSync(f.destination)).toBe(false);
  });

  it("restricts prebuilts to the expected addon, never scaffold JavaScript", async () => {
    const f = await fixture([
      { name: "build/Release/better_sqlite3.node", data: "addon" },
      { name: "lib/index.js", data: "evil" },
    ]);
    await expect(
      extractSQLiteArchive(
        f.archivePath,
        f.destination,
        "prebuilt",
        integrity(f.data),
      ),
    ).rejects.toThrow("Unexpected");
    expect(existsSync(f.destination)).toBe(false);
  });

  it("accepts normal prebuilt directories and only writes the addon", async () => {
    const f = await fixture([
      { name: "build/", type: "5" },
      { name: "build/Release/", type: "5" },
      { name: "./build/Release/better_sqlite3.node", data: "addon" },
    ]);
    await extractSQLiteArchive(
      f.archivePath,
      f.destination,
      "prebuilt",
      integrity(f.data),
    );
    expect(
      readFileSync(
        join(f.destination, "build/Release/better_sqlite3.node"),
        "utf8",
      ),
    ).toBe("addon");
  });

  it("does not follow preexisting extraction symlinks", async () => {
    const f = await fixture([{ name: "package/build/escape", data: "bad" }]);
    mkdirSync(f.destination);
    const outside = join(f.root, "outside");
    mkdirSync(outside);
    symlinkSync(outside, join(f.destination, "build"), "dir");
    await expect(
      extractSQLiteArchive(
        f.archivePath,
        f.destination,
        "source",
        integrity(f.data),
      ),
    ).rejects.toThrow("Unsafe SQLite extraction directory");
    expect(existsSync(join(outside, "escape"))).toBe(false);
  });

  it.each([
    "compressedBytes",
    "expandedBytes",
    "fileBytes",
    "entries",
  ] as const)("bounds %s before writes", async (limit) => {
    const f = await fixture([
      { name: "package/a", data: "a".repeat(8192) },
      { name: "package/b", data: "b" },
    ]);
    await expect(
      extractSQLiteArchive(
        f.archivePath,
        f.destination,
        "source",
        integrity(f.data),
        { ...SQLITE_ARCHIVE_LIMITS, [limit]: 1 },
      ),
    ).rejects.toThrow();
    expect(existsSync(f.destination)).toBe(false);
  });

  it("rejects truncated tar payloads before writes", async () => {
    const f = await fixture([]);
    const truncated = gzipSync(
      tar([{ name: "package/file", data: "abc", size: 4096 }]),
    );
    writeFileSync(f.archivePath, truncated);
    await expect(
      extractSQLiteArchive(
        f.archivePath,
        f.destination,
        "source",
        integrity(truncated),
      ),
    ).rejects.toThrow();
    expect(existsSync(f.destination)).toBe(false);
  });
});

function mockDownloads(
  respond: (url: string) => {
    data?: Buffer;
    status?: number;
    location?: string;
  },
) {
  const urls: string[] = [];
  vi.spyOn(https, "get").mockImplementation(((
    url: string,
    callback: (response: Readable) => void,
  ) => {
    urls.push(url);
    const request = Object.assign(new EventEmitter(), {
      setTimeout: () => request,
      destroy: () => request,
    });
    queueMicrotask(() => {
      const reply = respond(String(url));
      const response = Object.assign(
        Readable.from(reply.data ? [reply.data] : []),
        {
          statusCode: reply.status ?? 200,
          headers: reply.location ? { location: reply.location } : {},
        },
      );
      callback(response);
    });
    return request;
  }) as unknown as typeof https.get);
  return urls;
}

async function runtimeFixture() {
  const root = await createProjectTempDir("sqlite-install-security");
  writeFileSync(join(root, "package.json"), "{}");
  for (const name of ["better-sqlite3", "bindings", "file-uri-to-path"]) {
    const packagePath = join(root, "node_modules", name);
    mkdirSync(packagePath, { recursive: true });
    writeFileSync(
      join(packagePath, "package.json"),
      JSON.stringify({
        name,
        version: "12.10.0",
        repository: "https://evil.invalid/fake",
      }),
    );
  }
  const logs: string[] = [];
  configureSQLiteInstaller({
    storageRoot: join(root, "storage"),
    allowInstall: () => true,
    log: (line) => logs.push(line),
  });
  return { root, logs };
}

function fixturePrebuiltPin(kind: "official" | "patched", data: Buffer) {
  const runtime = process.versions.electron ? "electron" : "node";
  const filename = `better-sqlite3-v12.10.0-${runtime}-v${process.versions.modules}-${process.platform}-${process.arch}.tar.gz`;
  const repo =
    kind === "official" ? "WiseLibs/better-sqlite3" : "DmitriiKholkin/RapiDB";
  const tag = kind === "official" ? "v12.10.0" : "rapidb-patched-sqlite";
  const url = `https://github.com/${repo}/releases/download/${tag}/${filename}`;
  SQLITE_ARTIFACTS.prebuilts[`${kind}/${filename}`] = {
    url,
    integrity: integrity(data),
    size: data.length,
    releaseId: 1,
    assetId: 1,
    apiDigest: `sha256:${createHash("sha256").update(data).digest("hex")}`,
  };
  return url;
}

describe("SQLite installer security across callers and fallback paths", () => {
  it("downloads only the official source, ignores mirror variables, and never loads the addon during installation", async () => {
    const f = await runtimeFixture();
    const data = archive([
      { name: "build/Release/better_sqlite3.node", data: "not executable" },
    ]);
    fixturePrebuiltPin("official", data);
    const urls = mockDownloads(() => ({ data }));
    vi.stubEnv("npm_config_better_sqlite3_binary_host", "https://evil.invalid");
    try {
      const installed = await ensureSQLiteRuntimeInstalled(
        join(f.root, "dist"),
      );
      expect(installed).toBeTruthy();
      expect(urls).toHaveLength(1);
      expect(urls[0]).toMatch(
        /^https:\/\/github.com\/WiseLibs\/better-sqlite3\/releases\/download\/v12\.10\.0\//,
      );
      expect(
        readFileSync(
          join(installed as string, "build/Release/better_sqlite3.node"),
          "utf8",
        ),
      ).toBe("not executable");
    } finally {
      vi.unstubAllEnvs();
    }
  });

  it("rejects official prebuilt traversal without installing or publishing runtime.json", async () => {
    const f = await runtimeFixture();
    const data = archive([{ name: "../escape", data: "bad" }]);
    fixturePrebuiltPin("official", data);
    mockDownloads(() => ({ data }));
    await expect(
      ensureSQLiteRuntimeInstalled(join(f.root, "dist")),
    ).rejects.toThrow("Unsafe");
    expect(
      existsSync(
        join(
          f.root,
          "storage/sqlite-runtime/better-sqlite3/12.10.0",
          `node-abi-${process.versions.modules}-${process.platform}-${process.arch}-${SQLITE_ARTIFACT_POLICY_ID}`,
          "runtime.json",
        ),
      ),
    ).toBe(false);
  });

  it("does not fetch unpinned patched binaries and rejects altered npm source with an actionable error", async () => {
    Object.defineProperty(process.versions, "modules", {
      value: "146",
      configurable: true,
    });
    Object.defineProperty(process.versions, "electron", {
      value: "42.0.0",
      configurable: true,
    });
    const f = await runtimeFixture();
    const urls = mockDownloads((url) =>
      url.startsWith("https://github.com/")
        ? { status: 404 }
        : { data: archive([{ name: "package/binding.gyp", data: "evil" }]) },
    );
    await expect(
      ensureSQLiteRuntimeInstalled(join(f.root, "dist")),
    ).rejects.toThrow(/integrity mismatch.*reviewed static pins/);
    expect(urls).toHaveLength(1);
    expect(urls[0]).toBe(SQLITE_SOURCE.url);
    expect(urls.some((url) => url.includes("rapidb-patched-sqlite"))).toBe(
      false,
    );
    expect(f.logs.join("\n")).toContain("no reviewed static integrity pin");
  });

  it.each([
    "",
    "-archive-v1",
    `-${SQLITE_ARTIFACT_POLICY_ID.replace("pins-v4-", "pins-v2-")}`,
    `-${SQLITE_ARTIFACT_POLICY_ID.replace("pins-v4-", "pins-v3-")}`,
  ])("does not reuse a cache suffix '%s' from an older integrity/execution policy", async (legacySuffix) => {
    const f = await runtimeFixture();
    const probe = probeInstalledBetterSqlite3Runtime(join(f.root, "dist"));
    const legacyPackage = (probe.installedPackagePath as string).replace(
      `-${SQLITE_ARTIFACT_POLICY_ID}`,
      legacySuffix,
    );
    mkdirSync(join(legacyPackage, "build/Release"), { recursive: true });
    writeFileSync(join(legacyPackage, "package.json"), "{}");
    writeFileSync(
      join(legacyPackage, "build/Release/better_sqlite3.node"),
      "legacy",
    );
    const data = archive([
      { name: "build/Release/better_sqlite3.node", data: "new" },
    ]);
    fixturePrebuiltPin("official", data);
    const urls = mockDownloads(() => ({ data }));
    const installed = await ensureSQLiteRuntimeInstalled(join(f.root, "dist"));
    expect(urls).toHaveLength(1);
    expect(installed).toBe(probe.installedPackagePath);
    expect(
      readFileSync(
        join(installed as string, "build/Release/better_sqlite3.node"),
        "utf8",
      ),
    ).toBe("new");
    expect(
      readFileSync(
        join(legacyPackage, "build/Release/better_sqlite3.node"),
        "utf8",
      ),
    ).toBe("legacy");
  });

  it("rejects same-size official binary substitution before extraction/publication", async () => {
    const f = await runtimeFixture();
    const good = archive([
      { name: "build/Release/better_sqlite3.node", data: "good" },
    ]);
    fixturePrebuiltPin("official", good);
    const altered = Buffer.from(good);
    altered[altered.length - 1] ^= 1;
    const urls = mockDownloads(() => ({ data: altered }));
    await expect(
      ensureSQLiteRuntimeInstalled(join(f.root, "dist")),
    ).rejects.toThrow("integrity mismatch");
    expect(urls).toHaveLength(1);
    expect(
      probeInstalledBetterSqlite3Runtime(join(f.root, "dist"))
        .installedBinaryExists,
    ).toBe(false);
  });

  it("never downloads unpinned executable inputs for an unsupported runtime", async () => {
    Object.defineProperty(process.versions, "modules", {
      value: "999",
      configurable: true,
    });
    Object.defineProperty(process.versions, "electron", {
      value: "99.0.0",
      configurable: true,
    });
    const f = await runtimeFixture();
    const urls = mockDownloads(() => {
      throw new Error("Unexpected unpinned download");
    });
    await expect(
      ensureSQLiteRuntimeInstalled(join(f.root, "dist")),
    ).rejects.toThrow("Unsupported SQLite source-build target");
    expect(urls).toEqual([]);
  });

  it("keeps the no-toolchain patched path when a reviewed target pin is present", async () => {
    Object.defineProperty(process.versions, "modules", {
      value: "146",
      configurable: true,
    });
    Object.defineProperty(process.versions, "electron", {
      value: "42.0.0",
      configurable: true,
    });
    const f = await runtimeFixture();
    const data = archive([
      { name: "build/Release/better_sqlite3.node", data: "patched" },
    ]);
    const expected = fixturePrebuiltPin("patched", data);
    const urls = mockDownloads(() => ({ data }));
    vi.stubEnv("PATH", "");
    try {
      const installed = await ensureSQLiteRuntimeInstalled(
        join(f.root, "dist"),
      );
      expect(urls).toEqual([expected]);
      expect(
        readFileSync(
          join(installed as string, "build/Release/better_sqlite3.node"),
          "utf8",
        ),
      ).toBe("patched");
    } finally {
      vi.unstubAllEnvs();
    }
  });

  it.skipIf(process.platform === "win32").each([true, false])(
    "passes only verified private headers and a controlled environment through the complete source fallback (workspace getter: %s)",
    async (withWorkspaceGetter) => {
      Object.defineProperty(process.versions, "modules", {
        value: "146",
        configurable: true,
      });
      Object.defineProperty(process.versions, "electron", {
        value: "42.1.0",
        configurable: true,
      });
      const f = await runtimeFixture();
      const installation = await createProjectTempDir(
        "sqlite-source-tool-fixture",
      );
      const bin = join(installation, "bin");
      const npmBin = join(installation, "lib/node_modules/npm/bin");
      mkdirSync(bin, { recursive: true });
      mkdirSync(npmBin, { recursive: true });
      writeFileSync(join(bin, "node"), "test interpreter, never executed");
      chmodSync(join(bin, "node"), 0o700);
      writeFileSync(join(npmBin, "npm-cli.js"), "test npm, never executed");
      symlinkSync(join(npmBin, "npm-cli.js"), join(bin, "npm"));
      const workspaceRoots = [
        join(installation, "workspace-first"),
        join(installation, "workspace-second"),
      ];
      const workspaceBins = workspaceRoots.map((workspace) => {
        const workspaceBin = join(workspace, "bin");
        const workspaceNpm = join(workspace, "lib/node_modules/npm/bin");
        mkdirSync(workspaceBin, { recursive: true });
        mkdirSync(workspaceNpm, { recursive: true });
        writeFileSync(join(workspaceBin, "node"), "never executed");
        chmodSync(join(workspaceBin, "node"), 0o700);
        writeFileSync(join(workspaceNpm, "npm-cli.js"), "never executed");
        symlinkSync(
          join(workspaceNpm, "npm-cli.js"),
          join(workspaceBin, "npm"),
        );
        return workspaceBin;
      });
      const source = archive([
        { name: "package/src/util/macros.cpp", data: "fixture" },
        { name: "package/src/util/helpers.cpp", data: "fixture" },
        { name: "package/src/better_sqlite3.cpp", data: "fixture" },
        {
          name: "package/deps/sqlite3.gyp",
          data: readFileSync(
            new URL(
              "../../node_modules/better-sqlite3/deps/sqlite3.gyp",
              import.meta.url,
            ),
          ),
        },
      ]);
      const headers = headerFixture();
      let currentWorkspaceRoots: readonly string[] = [];
      const workspaceRootsGetter = vi.fn(() => currentWorkspaceRoots);
      const urls = mockDownloads((url) => {
        // Folders change after configuration and before tool discovery.
        if (url === SQLITE_SOURCE.url) currentWorkspaceRoots = workspaceRoots;
        return { data: url === SQLITE_SOURCE.url ? source : headers.data };
      });
      const calls: Array<{
        file: string;
        args: string[];
        cwd: string;
        env: NodeJS.ProcessEnv;
        actions?: string;
      }> = [];
      vi.resetModules();
      vi.doMock("node:child_process", async () => {
        const actual =
          await vi.importActual<typeof import("node:child_process")>(
            "node:child_process",
          );
        return {
          ...actual,
          execFile: (
            file: string,
            args: string[],
            options: { cwd: string; env: NodeJS.ProcessEnv },
            callback: (
              error: Error | null,
              stdout: string,
              stderr: string,
            ) => void,
          ) => {
            calls.push({ file, args, cwd: options.cwd, env: options.env });
            queueMicrotask(() => {
              if (args[0].endsWith("node-gyp.js")) {
                calls[calls.length - 1].actions = readFileSync(
                  join(options.cwd, "deps/sqlite3.gyp"),
                  "utf8",
                );
                mkdirSync(join(options.cwd, "build/Release"), {
                  recursive: true,
                });
                writeFileSync(
                  join(options.cwd, "build/Release/better_sqlite3.node"),
                  "verified source build fixture",
                );
              }
              callback(null, "", "");
            });
            return new EventEmitter();
          },
        };
      });
      const freshArchive = await import(
        "../../src/extension/utils/sqliteArchive"
      );
      const sourceIntegrity = freshArchive.SQLITE_SOURCE.integrity;
      Object.defineProperty(freshArchive.SQLITE_SOURCE, "integrity", {
        value: integrity(source),
        configurable: true,
      });
      SQLITE_ARTIFACTS.headers["42-146"] = headers.pins;
      vi.stubEnv(
        "PATH",
        (withWorkspaceGetter ? [...workspaceBins, bin] : [bin]).join(delimiter),
      );
      vi.stubEnv("npm_config_nodedir", "/evil/headers");
      vi.stubEnv("npm_package_config_node_gyp_directory", "/evil/source");
      vi.stubEnv("PYTHONPATH", "/evil/python");
      const fresh = await import("../../src/extension/utils/sqliteInstaller");
      try {
        fresh.configureSQLiteInstaller({
          storageRoot: join(f.root, "fresh-storage"),
          allowInstall: () => true,
          log: () => {},
          ...(withWorkspaceGetter
            ? { workspaceRoots: workspaceRootsGetter }
            : {}),
        });
        expect(workspaceRootsGetter).not.toHaveBeenCalled();
        const installed = await fresh.ensureSQLiteRuntimeInstalled(
          join(f.root, "dist"),
        );
        expect(installed).toBeTruthy();
        expect(workspaceRootsGetter).toHaveBeenCalledTimes(
          withWorkspaceGetter ? 1 : 0,
        );
        expect(urls).toEqual([SQLITE_SOURCE.url, headers.pins.archive.url]);
        expect(calls).toHaveLength(2);
        expect(calls[0].args).toContain("--ignore-scripts");
        const build = calls[1];
        expect(build.file).toBe(realpathSync(join(bin, "node")));
        expect(build.env.PATH).toBe(bin);
        expect(build.actions).toContain(
          `'action': [${JSON.stringify(build.file)}, 'copy.js'`,
        );
        expect(build.actions).not.toContain("'action': ['node'");
        expect(build.args).toContain("--target=42.0.0");
        const nodedir = build.args
          .find((arg) => arg.startsWith("--nodedir="))
          ?.slice("--nodedir=".length);
        expect(nodedir).toContain("electron-headers");
        expect(build.args.some((arg) => arg.includes("dist-url"))).toBe(false);
        expect(build.cwd).toContain("better-sqlite3-build-source");
        expect(build.env.npm_config_nodedir).toBeUndefined();
        expect(build.env.npm_package_config_node_gyp_directory).toBeUndefined();
        expect(build.env.PYTHONPATH).toBeUndefined();
        expect(build.env.HOME).toContain("build-home");
        expect(existsSync(nodedir as string)).toBe(false);
      } finally {
        fresh.resetSQLiteInstallerForTests();
        Object.defineProperty(freshArchive.SQLITE_SOURCE, "integrity", {
          value: sourceIntegrity,
          configurable: true,
        });
        vi.unstubAllEnvs();
        vi.doUnmock("node:child_process");
        vi.resetModules();
      }
    },
  );

  it.each([
    "https://evil.invalid/asset",
    "http://127.0.0.1/asset",
  ])("rejects an untrusted/downgrade release redirect: %s", async (location) => {
    const f = await fixture([]);
    const urls = mockDownloads(() => ({ status: 302, location }));
    await expect(
      downloadToFile(
        "https://github.com/asset",
        join(f.root, "download"),
        1000,
        { allowedHosts: ["github.com"] },
      ),
    ).rejects.toThrow(/Refusing/);
    expect(urls).toHaveLength(1);
  });
});

describe("SQLite source build tool discovery", () => {
  it.skipIf(process.platform === "win32")(
    "binds real copy.js actions to the selected Node even when a rejected external candidate leads PATH",
    async () => {
      const root = await createProjectTempDir("sqlite-nested-node");
      const workspace = join(root, "workspace");
      const rejected = join(root, "rejected-external-bin");
      const installation = join(root, "trusted installation with spaces");
      const bin = join(installation, "bin");
      const npmBin = join(installation, "lib/node_modules/npm/bin");
      const source = join(root, "source with spaces");
      for (const directory of [
        workspace,
        rejected,
        bin,
        npmBin,
        join(source, "deps/sqlite3"),
      ])
        mkdirSync(directory, { recursive: true });
      const marker = join(workspace, "malicious-node-ran");
      const malicious = join(workspace, "node");
      writeFileSync(
        malicious,
        `#!/bin/sh\n/usr/bin/touch '${marker.replaceAll("'", "'\\''")}'\nexit 73\n`,
      );
      chmodSync(malicious, 0o700);
      symlinkSync(malicious, join(rejected, "node"));
      // Use the real interpreter, not an executable fixture launcher.
      const selectedNode = join(installation, "selected Node's executable");
      symlinkSync(process.execPath, selectedNode);
      symlinkSync(selectedNode, join(bin, "node"));
      writeFileSync(
        join(npmBin, "npm-cli.js"),
        "trusted npm discovery fixture",
      );
      symlinkSync(join(npmBin, "npm-cli.js"), join(bin, "npm"));
      const tools = resolveSQLiteBuildTools(
        workspace,
        join(root, "runtime"),
        { PATH: [rejected, bin].join(delimiter) },
        [workspace],
      );
      expect(tools.node).toBe(realpathSync(selectedNode));
      expect(tools.env.PATH?.split(delimiter)[0]).toBe(bin);
      expect(tools.env.PATH?.split(delimiter)).not.toContain(rejected);
      writeFileSync(
        join(source, "deps/sqlite3.gyp"),
        readFileSync(
          new URL(
            "../../node_modules/better-sqlite3/deps/sqlite3.gyp",
            import.meta.url,
          ),
        ),
      );
      writeFileSync(
        join(source, "deps/copy.js"),
        readFileSync(
          new URL(
            "../../node_modules/better-sqlite3/deps/copy.js",
            import.meta.url,
          ),
        ),
      );
      for (const filename of ["sqlite3.c", "sqlite3.h", "sqlite3ext.h"])
        writeFileSync(
          join(source, "deps/sqlite3", filename),
          `actual ${filename}`,
        );
      pinSQLiteSourceNodeActions(source, tools.node);
      const gyp = readFileSync(join(source, "deps/sqlite3.gyp"), "utf8");
      const interpreters = [
        ...gyp.matchAll(
          /'action'\s*:\s*\[\s*("(?:[^"\\]|\\.)*")(?=\s*,\s*'copy\.js')/g,
        ),
      ].map((match) => JSON.parse(match[1]) as string);
      expect(interpreters).toEqual([tools.node, tools.node]);
      // Deliberately reintroduce the rejected directory ahead of the selected
      // one. Unlike a PATH-only fix, the patched actions must still be safe.
      const env = { ...tools.env, PATH: [rejected, bin].join(delimiter) };
      // Never run the malicious fixture, even to demonstrate bare-node failure.
      expect(existsSync(marker)).toBe(false);
      for (const [index, interpreter] of interpreters.entries()) {
        const destination = join(root, `actual output ${index}`);
        await promisify(execFile)(
          interpreter,
          [
            "copy.js",
            destination,
            index === 0 ? "" : join(source, "deps/sqlite3"),
          ],
          { cwd: join(source, "deps"), env },
        );
        expect(readFileSync(join(destination, "sqlite3.c"), "utf8")).toBe(
          "actual sqlite3.c",
        );
        expect(readFileSync(join(destination, "sqlite3.h"), "utf8")).toBe(
          "actual sqlite3.h",
        );
      }
      expect(existsSync(marker)).toBe(false);
    },
  );

  it.each([
    "C:\\Program Files\\Node's installation\\node.exe",
    "C:\\Program Files (x86)\\Node's installation\\node.exe",
    "C:\\Node\\node.exe",
  ])("escapes Windows Node paths as GYP-compatible literals: %s", async (node) => {
    const root = await createProjectTempDir("sqlite-gyp-windows-path");
    mkdirSync(join(root, "deps"));
    writeFileSync(
      join(root, "deps/sqlite3.gyp"),
      "{'actions': [{'action': ['node', 'copy.js', 'out', '']}, {'action': ['node', 'copy.js', 'out', 'custom']}]}\n",
    );
    pinSQLiteSourceNodeActions(root, node, "win32");
    const gyp = readFileSync(join(root, "deps/sqlite3.gyp"), "utf8");
    const encoded = [...gyp.matchAll(/'action': \[("(?:[^"\\]|\\.)*")/g)].map(
      (match) => match[1],
    );
    expect(encoded).toEqual([JSON.stringify(node), JSON.stringify(node)]);
    expect(encoded.map((literal) => JSON.parse(literal))).toEqual([node, node]);
  });

  it("fails closed if the pinned source action shape changes", async () => {
    const root = await createProjectTempDir("sqlite-gyp-action-change");
    mkdirSync(join(root, "deps"));
    const gyp = "{'action': ['node', 'different.js']}\n";
    writeFileSync(join(root, "deps/sqlite3.gyp"), gyp);
    expect(() => pinSQLiteSourceNodeActions(root, process.execPath)).toThrow(
      "actions changed",
    );
    expect(readFileSync(join(root, "deps/sqlite3.gyp"), "utf8")).toBe(gyp);
    expect(() => pinSQLiteSourceNodeActions(root, "node")).toThrow(
      "absolute selected Node",
    );
  });
  it("does not permit environment config to override node-gyp argv or inject interpreters/loaders", async () => {
    const root = await createProjectTempDir("sqlite-controlled-env");
    const hostile = {
      ...process.env,
      npm_config_dist_url: "https://evil.invalid",
      npm_config_nodedir: "/evil/headers",
      NPM_CONFIG_DIRECTORY: "/evil/source",
      npm_config_python: "/evil/python",
      npm_package_config_node_gyp_dist_url: "https://evil.invalid",
      npm_package_config_node_gyp_nodedir: "/evil/headers",
      npm_package_config_node_gyp_directory: "/evil/source",
      NPM_PACKAGE_CONFIG_NODE_GYP_PYTHON: "/evil/python",
      PYTHONPATH: "/evil/sitecustomize",
      PYTHONHOME: "/evil/python-home",
      PYTHONSTARTUP: "/evil/startup.py",
      PYTHON: "/evil/python",
      NODE_GYP_FORCE_PYTHON: "/evil/python",
      NODE_OPTIONS: "--require=/evil/hook.js",
      NODE_PATH: "/evil/modules",
      LD_PRELOAD: "/evil/loader.so",
      DYLD_INSERT_LIBRARIES: "/evil/loader.dylib",
      BASH_ENV: "/evil/shell-hook",
      COMSPEC: "/evil/shell",
      PATHEXT: ".EVIL",
      GYP_DEFINES: "node_root_dir=/evil/headers",
      CC: "/evil/compiler",
      CXXFLAGS: "-include /evil/payload.h",
      HOME: "/evil/home",
      TMPDIR: "/evil/tmp",
    };
    const env = sqliteBuildEnvironment(hostile, process.env.PATH ?? "", root);
    for (const key of Object.keys(hostile)) {
      if (
        /^(npm_|python(?:path|home|startup)?$|node_options|node_path|node_gyp|ld_|dyld_|bash_env|gyp_|cc$|cxxflags$)/i.test(
          key,
        )
      )
        expect(env[key]).toBeUndefined();
    }
    expect(env.HOME).toBe(join(root, "build-home"));
    expect(env.TMPDIR).toBe(join(root, "build-home/tmp"));
    expect(env.PYTHONNOUSERSITE).toBe("1");
    expect(env.PYTHONSAFEPATH).toBe("1");
    expect(env.COMSPEC).not.toBe("/evil/shell");
    expect(env.PATHEXT).not.toBe(".EVIL");
    const headerDir = join(root, "verified-headers");
    const args = sqliteNodeGypArguments(
      join(root, "tools"),
      headerDir,
      "42.0.0",
      process.platform,
      process.arch,
    );
    expect(args.some((arg) => /dist-url|tarball/.test(arg))).toBe(false);
    const script = `const gyp = require(${JSON.stringify(require.resolve("node-gyp"))})(); gyp.parseArgv([process.execPath, "node-gyp", ...${JSON.stringify(args.slice(1))}]); console.log(JSON.stringify(gyp.opts));`;
    const { stdout } = await promisify(execFile)(
      process.execPath,
      ["-e", script],
      { env },
    );
    const opts = JSON.parse(stdout);
    expect(opts.nodedir).toBe(headerDir);
    expect(opts.devdir).toBe(join(root, "tools/node-gyp-devdir"));
    expect(opts.target).toBe("42.0.0");
    expect(opts.directory).toBeUndefined();
    expect(opts.python).toBeUndefined();
    expect(opts["dist-url"]).toBeUndefined();
  });
  it("retains lockfile integrity for every node-gyp dependency and excludes unrelated packages", () => {
    const tool = createSQLiteBuildToolLock();
    const repository = JSON.parse(
      readFileSync(new URL("../../package-lock.json", import.meta.url), "utf8"),
    );
    expect(tool.manifest.dependencies["node-gyp"]).toBe(
      repository.packages["node_modules/node-gyp"].version,
    );
    expect(Object.keys(tool.lock.packages).length).toBeGreaterThan(20);
    expect(tool.lock.packages["node_modules/better-sqlite3"]).toBeUndefined();
    for (const [path, entry] of Object.entries(tool.lock.packages)) {
      if (!path) continue;
      expect((entry as { integrity: string }).integrity).toBe(
        repository.packages[path].integrity,
      );
      expect((entry as { dev?: boolean }).dev).toBeUndefined();
    }
  });
  it("rejects relative, workspace, and non-npm PATH shims without executing them", async () => {
    const root = await createProjectTempDir("sqlite-build-path");
    mkdirSync(join(root, "bin"));
    writeFileSync(join(root, "bin/npx"), "malicious");
    expect(() =>
      resolveSQLiteBuildTools(
        root,
        join(root, "storage"),
        { PATH: `.:bin:${join(root, "bin")}` },
        [root],
      ),
    ).toThrow("trusted Node.js/npm");
  });

  it.skipIf(process.platform === "win32")(
    "uses absolute Node/npm paths, sanitized PATH, and no NODE_OPTIONS injection",
    async () => {
      const root = await createProjectTempDir("sqlite-build-tools");
      const bin = join(root, "installation/bin");
      const npmBin = join(root, "installation/lib/node_modules/npm/bin");
      mkdirSync(bin, { recursive: true });
      mkdirSync(npmBin, { recursive: true });
      writeFileSync(join(bin, "node"), "local node");
      chmodSync(join(bin, "node"), 0o700);
      writeFileSync(join(npmBin, "npm-cli.js"), "local npm");
      symlinkSync(join(npmBin, "npm-cli.js"), join(bin, "npm"));
      const tools = resolveSQLiteBuildTools(
        join(root, "workspace"),
        join(root, "storage"),
        {
          PATH: `.:relative:${bin}`,
          NODE_OPTIONS: "--require=evil",
          NODE_PATH: "evil",
          node_options: "--require=evil-lowercase",
          node_path: "evil-lowercase",
        },
        [join(root, "workspace")],
      );
      expect(tools.node).toBe(realpathSync(join(bin, "node")));
      expect(tools.npmCli).toBe(realpathSync(join(npmBin, "npm-cli.js")));
      expect(tools.env.PATH).toBe(bin);
      expect(tools.env.NODE_OPTIONS).toBeUndefined();
      expect(tools.env.NODE_PATH).toBeUndefined();
      expect(tools.env.node_options).toBeUndefined();
      expect(tools.env.node_path).toBeUndefined();
    },
  );
});

function headerFixture(entries: Entry[] = []) {
  const data = archive([
    { name: "node_headers/include/node/common.gypi", data: "{}" },
    { name: "node_headers/include/node/config.gypi", data: "{}" },
    {
      name: "node_headers/include/node/node_version.h",
      data: "#define NODE_MODULE_VERSION 146\n",
    },
    ...entries,
  ]);
  const library = Buffer.from("verified Windows import library");
  const baseline = originalHeaders["42-146"];
  const pins: SQLiteHeaderPins = {
    ...baseline,
    archive: {
      ...baseline.archive,
      integrity: integrity(data),
      size: data.length,
    },
    windowsLibraries: {
      x64: {
        ...baseline.windowsLibraries.x64,
        integrity: integrity(library),
        size: library.length,
      },
    },
  };
  return { data, library, pins };
}

describe("Pinned executable-input manifest and private Electron headers", () => {
  it("records bootstrap release/asset IDs and API digests for every official/patched executable pin", () => {
    expect(SQLITE_ARTIFACTS.betterSqlite3Version).toBe(SQLITE_SOURCE.version);
    const electronAbis = [
      121, 123, 125, 128, 130, 132, 133, 135, 136, 139, 140, 143, 145,
    ];
    const nodeAbis = [127, 137, 141, 147];
    const electronTargets = [
      "darwin-arm64",
      "darwin-x64",
      "linux-arm64",
      "linux-x64",
      "win32-arm64",
      "win32-ia32",
      "win32-x64",
    ];
    const nodeTargets = [
      ...electronTargets.filter((target) => target !== "win32-ia32"),
      "linux-arm",
      "linuxmusl-arm",
      "linuxmusl-arm64",
      "linuxmusl-x64",
    ];
    const expected = [
      ...electronAbis.flatMap((abi) =>
        electronTargets.map(
          (target) =>
            `official/better-sqlite3-v12.10.0-electron-v${abi}-${target}.tar.gz`,
        ),
      ),
      ...nodeAbis.flatMap((abi) =>
        nodeTargets.map(
          (target) =>
            `official/better-sqlite3-v12.10.0-node-v${abi}-${target}.tar.gz`,
        ),
      ),
    ];
    expect(Object.keys(originalPrebuilts).sort()).toEqual(expected.sort());
    for (const [key, pin] of Object.entries(originalPrebuilts)) {
      expect(key).toMatch(
        /^(?:official|patched)\/better-sqlite3-v12\.10\.0-(?:node|electron)-v\d+-(?:darwin|linux|linuxmusl|win32)-(?:arm64|x64|ia32|arm)\.tar\.gz$/,
      );
      expect(pin.assetId).toBeGreaterThan(0);
      expect(pin.releaseId).toBeGreaterThan(0);
      expect(pin.apiDigest).toMatch(/^sha256:[a-f0-9]{64}$/);
      expect(pin.integrity).toBe(
        `sha256-${Buffer.from(pin.apiDigest.slice(7), "hex").toString("base64")}`,
      );
      expect(pin.size).toBeGreaterThan(0);
      expect(pin.size).toBeLessThanOrEqual(
        SQLITE_ARCHIVE_LIMITS.compressedBytes,
      );
    }
    for (const pins of Object.values(originalHeaders)) {
      expect(pins.releaseId).toBeGreaterThan(0);
      expect(pins.checksumIntegrity).toMatch(/^sha256-/);
      expect(pins.archive.integrity).toMatch(/^sha256-/);
      expect(pins.windowsLibraries.x64.integrity).toMatch(/^sha256-/);
      expect(pins.windowsLibraries.arm64.integrity).toMatch(/^sha256-/);
    }
  });

  it("chooses an explicit ABI-compatible baseline, rejects unpinned major/ABI/import-library targets", () => {
    expect(sqliteHeaderPins("42.9.9", "146", "darwin", "arm64").version).toBe(
      "42.0.0",
    );
    expect(sqliteHeaderPins("43.0.0", "148", "linux", "x64").version).toBe(
      "43.0.0",
    );
    expect(sqliteHeaderPins("44.5.1", "149", "win32", "arm64").version).toBe(
      "44.0.0",
    );
    expect(() => sqliteHeaderPins("42.0.0", "999", "darwin", "arm64")).toThrow(
      "Unsupported",
    );
    expect(() => sqliteHeaderPins("45.0.0", "150", "darwin", "arm64")).toThrow(
      "Unsupported",
    );
    expect(() => sqliteHeaderPins("42.0.0", "146", "win32", "ia32")).toThrow(
      "Unsupported",
    );
  });

  it("verifies common.gypi/config.gypi and Windows node.lib in a private directory without SHASUMS/global cache", async () => {
    const root = await createProjectTempDir("sqlite-private-headers");
    const f = headerFixture();
    const urls = mockDownloads((url) => ({
      data: url.endsWith("node.lib") ? f.library : f.data,
    }));
    const nodedir = await preparePinnedSQLiteHeaders(
      root,
      f.pins,
      "win32",
      "x64",
    );
    expect(nodedir).toBe(join(root, "electron-headers"));
    expect(
      readFileSync(join(nodedir, "include/node/common.gypi"), "utf8"),
    ).toBe("{}");
    expect(
      readFileSync(join(nodedir, "include/node/config.gypi"), "utf8"),
    ).toBe("{}");
    expect(readFileSync(join(nodedir, "Release/node.lib"))).toEqual(f.library);
    expect(urls).toEqual([f.pins.archive.url, f.pins.windowsLibraries.x64.url]);
    expect(urls.some((url) => /SHASUMS/.test(url))).toBe(false);
    expect(existsSync(join(root, "electron-headers.tgz"))).toBe(false);
  });

  it.each([
    "headers",
    "library",
  ])("rejects substituted %s bytes and removes all staged headers", async (target) => {
    const root = await createProjectTempDir("sqlite-header-substitution");
    const f = headerFixture();
    const urls = mockDownloads((url) => {
      const isLibrary = url.endsWith("node.lib");
      const bytes = Buffer.from(isLibrary ? f.library : f.data);
      if ((target === "library") === isLibrary) bytes[bytes.length - 1] ^= 1;
      return { data: bytes };
    });
    await expect(
      preparePinnedSQLiteHeaders(root, f.pins, "win32", "x64"),
    ).rejects.toThrow("integrity mismatch");
    expect(existsSync(join(root, "electron-headers"))).toBe(false);
    expect(urls.length).toBe(target === "headers" ? 1 : 2);
  });

  it.each([
    { name: "node_headers/../../escape", data: "payload" },
    { name: "node_headers/include/node/link", type: "2", linkname: "/escape" },
  ])("rejects hostile header archive paths/links before extraction", async (entry) => {
    const root = await createProjectTempDir("sqlite-header-traversal");
    const f = headerFixture([entry]);
    mockDownloads(() => ({ data: f.data }));
    await expect(
      preparePinnedSQLiteHeaders(root, f.pins, "linux", "x64"),
    ).rejects.toThrow(/Unsafe|Unsupported/);
    expect(existsSync(join(root, "electron-headers"))).toBe(false);
  });

  it("requires pinned header ABI and a Windows library before permitting a build", async () => {
    const root = await createProjectTempDir("sqlite-header-abi");
    const f = headerFixture();
    const urls = mockDownloads(() => ({ data: f.data }));
    await expect(
      preparePinnedSQLiteHeaders(root, f.pins, "win32", "arm64"),
    ).rejects.toThrow("Unsupported pinned");
    expect(urls).toEqual([]);
    await expect(
      preparePinnedSQLiteHeaders(
        root,
        { ...f.pins, abi: "999" },
        "linux",
        "x64",
      ),
    ).rejects.toThrow("expected SQLite ABI");
    expect(existsSync(join(root, "electron-headers"))).toBe(false);
  });
});
