import { createHash } from "node:crypto";
import { EventEmitter, once } from "node:events";
import { access, cp, mkdir, readFile, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { dirname, join } from "node:path";
import { Readable } from "node:stream";
import { gzipSync } from "node:zlib";
import { afterEach, expect, it, vi } from "vitest";
import { openSQLiteDatabase } from "../../src/extension/dbDrivers/sqliteRuntime";
import { SQLiteWorkerClient } from "../../src/extension/dbDrivers/sqliteWorkerClient";
import {
  SQLITE_ARTIFACTS,
  sqlitePrebuiltFileName,
} from "../../src/extension/utils/sqliteArtifactPins";
import {
  configureSQLiteInstaller,
  probeInstalledBetterSqlite3Runtime,
  repairSQLiteManagedRuntime,
  resetSQLiteInstallerForTests,
  snapshotSQLiteManagedRuntime,
  sqliteNativeBindingPath,
} from "../../src/extension/utils/sqliteInstaller";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";
import { createProjectTempDir } from "../runtime/tempDirectories";

const require = createRequire(import.meta.url);
const https = require("node:https") as typeof import("node:https");
const clients = new Set<SQLiteWorkerClient>();
const pins = { ...SQLITE_ARTIFACTS.prebuilts };
const badBinary = Buffer.from("not a native SQLite addon");
const localAddon = sqliteNativeBindingPath(
  dirname(require.resolve("better-sqlite3/package.json")),
);

afterEach(async () => {
  await Promise.all([...clients].map((client) => client.close()));
  clients.clear();
  resetSQLiteInstallerForTests();
  SQLITE_ARTIFACTS.prebuilts = { ...pins };
});

async function fixture(
  binary: Buffer,
  options: {
    fallback?: boolean;
    artifact?: Buffer;
    allowInstall?: () => boolean | Promise<boolean>;
  } = {},
) {
  const directory = await createProjectTempDir("sqlite-native-repair");
  const storageRoot = join(directory, "storage");
  const allowInstall = vi.fn(options.allowInstall ?? (() => false));
  configureSQLiteInstaller({ storageRoot, allowInstall, log: () => {} });
  const detectLibc = require("detect-libc") as typeof import("detect-libc");
  const manifest = {
    betterSqlite3Version: require("better-sqlite3/package.json")
      .version as string,
    runtime: "node" as const,
    abi: process.versions.modules,
    platform: process.platform,
    arch: process.arch,
    libc:
      process.platform === "linux" && detectLibc.isNonGlibcLinuxSync()
        ? (detectLibc.familySync() ?? "").toLowerCase()
        : "",
  };
  const filename = sqlitePrebuiltFileName("official", manifest);
  const url = `https://github.com/WiseLibs/better-sqlite3/releases/download/v${manifest.betterSqlite3Version}/${filename}`;
  let artifact: Buffer | undefined;
  if (options.artifact) {
    const header = Buffer.alloc(512);
    header.write("build/Release/better_sqlite3.node");
    header.write("0000644\0", 100);
    header.write(
      `${options.artifact.length.toString(8).padStart(11, "0")}\0`,
      124,
    );
    header.fill(32, 148, 156);
    header.write("0", 156);
    header.write("ustar\0", 257);
    const sum = header.reduce((total, byte) => total + byte, 0);
    header.write(`${sum.toString(8).padStart(6, "0")}\0 `, 148);
    artifact = gzipSync(
      Buffer.concat([
        header,
        options.artifact,
        Buffer.alloc(((512 - (options.artifact.length % 512)) % 512) + 1024),
      ]),
    );
    SQLITE_ARTIFACTS.prebuilts[`official/${filename}`] = {
      url,
      size: artifact.length,
      integrity: `sha512-${createHash("sha512").update(artifact).digest("base64")}`,
      releaseId: 1,
      assetId: 1,
      apiDigest: `sha256:${createHash("sha256").update(artifact).digest("hex")}`,
    };
  }
  const download = vi.spyOn(https, "get").mockImplementation(((
    requestUrl: string,
    callback: (response: Readable) => void,
  ) => {
    if (requestUrl !== url || !artifact) throw new Error("Unexpected download");
    const request = Object.assign(new EventEmitter(), {
      setTimeout: () => request,
      destroy: () => request,
    });
    const response = Object.assign(Readable.from([artifact]), {
      statusCode: 200,
      headers: { "content-length": `${artifact.length}` },
    });
    queueMicrotask(() => callback(response));
    return request;
  }) as unknown as typeof https.get);

  const { installedPackagePath, installedBinaryPath } =
    probeInstalledBetterSqlite3Runtime(__dirname);
  if (!installedPackagePath || !installedBinaryPath)
    throw new Error("Missing managed fixture paths");
  const runtimeRoot = dirname(dirname(installedPackagePath));
  for (const name of ["better-sqlite3", "bindings", "file-uri-to-path"]) {
    const source = dirname(require.resolve(`${name}/package.json`));
    await cp(source, join(runtimeRoot, "node_modules", name), {
      recursive: true,
      filter: (path) =>
        !["build", "src", "deps", "node_modules"].includes(
          path.slice(source.length + 1).split(/[\\/]/)[0],
        ),
    });
  }
  await mkdir(dirname(installedBinaryPath), { recursive: true });
  await writeFile(installedBinaryPath, binary);
  await writeFile(join(runtimeRoot, "runtime.json"), JSON.stringify(manifest));
  const sentinel = join(runtimeRoot, "neighbor.txt");
  await writeFile(sentinel, "do not replace the runtime tree");

  if (options.fallback === false) {
    // Run the real bundled worker but model a VSIX without a development
    // better-sqlite3 install, even though this checkout has one available.
    const worker = process.env.RAPIDB_SQLITE_WORKER_PATH;
    const wrapper = join(directory, "no-development-runtime.cjs");
    if (!worker) throw new Error("The SQLite worker bundle was not built");
    await writeFile(
      wrapper,
      `
      const Module = require("node:module");
      const load = Module._load;
      Module._load = function (id) {
        if (id === "better-sqlite3" || id === ${JSON.stringify(require.resolve("better-sqlite3"))})
          throw new Error("Development fallback is unavailable");
        return Reflect.apply(load, this, arguments);
      };
      require(${JSON.stringify(worker)});
    `,
    );
    vi.stubEnv("RAPIDB_SQLITE_WORKER_PATH", wrapper);
  }
  return {
    directory,
    allowInstall,
    download,
    installedPackagePath,
    installedBinaryPath,
    runtimeRoot,
    sentinel,
  };
}

function client() {
  const instance = new SQLiteWorkerClient();
  clients.add(instance);
  return instance;
}

function config(filePath = ":memory:", readOnly = false) {
  return {
    id: filePath,
    name: "SQLite repair test",
    type: "sqlite",
    filePath,
    readOnly,
  } satisfies ConnectionConfig;
}

it("smoke-tests lazy addons in the direct adapter and worker, using a working fallback without a download", async () => {
  const f = await fixture(badBinary);
  // A successful JS require is not evidence that the addon can be opened.
  expect(typeof require(f.installedPackagePath)).toBe("function");
  const direct = await openSQLiteDatabase(config());
  try {
    expect(direct.get("SELECT 42 AS value")).toEqual({ value: 42 });
  } finally {
    direct.close();
  }
  const worker = client();
  await worker.open(config(), 2_000);
  await expect(
    worker.call("query", ["SELECT 42"], 2_000),
  ).resolves.toMatchObject({
    rows: [{ __col_0: 42 }],
  });
  expect(f.allowInstall).not.toHaveBeenCalled();
  expect(f.download).not.toHaveBeenCalled();
  expect(await readFile(f.installedBinaryPath)).toEqual(badBinary);
});

it("stages and smoke-tests one pinned repair for parallel workers, and reuses it for an old failure", async () => {
  const native = await readFile(localAddon);
  const f = await fixture(badBinary, {
    fallback: false,
    artifact: native,
    allowInstall: () => true,
  });
  const failed = snapshotSQLiteManagedRuntime(__dirname);
  if (!failed) throw new Error("Missing managed fixture snapshot");
  const workers = [client(), client()];
  await Promise.all(workers.map((worker) => worker.open(config(), 10_000)));
  await Promise.all(
    workers.map((worker) =>
      expect(worker.call("query", ["SELECT 42"], 2_000)).resolves.toMatchObject(
        {
          rows: [{ __col_0: 42 }],
        },
      ),
    ),
  );
  await expect(repairSQLiteManagedRuntime(__dirname, failed)).resolves.toBe(
    f.installedPackagePath,
  );
  expect(f.allowInstall).toHaveBeenCalledOnce();
  expect(f.download).toHaveBeenCalledOnce();
  expect(await readFile(f.installedBinaryPath)).toEqual(native);
  expect(await readFile(f.sentinel, "utf8")).toBe(
    "do not replace the runtime tree",
  );
}, 15_000);

it("does not replace a cache with an unusable replacement, even if its archive matches the pin", async () => {
  const f = await fixture(badBinary, {
    fallback: false,
    artifact: Buffer.from("another unusable addon"),
    allowInstall: () => true,
  });
  const file = join(f.directory, "must-not-open.sqlite");
  await expect(client().open(config(file), 2_000)).rejects.toThrow(
    /repair failed.*smoke test/i,
  );
  expect(f.allowInstall).toHaveBeenCalledOnce();
  expect(f.download).toHaveBeenCalledOnce();
  expect(await readFile(f.installedBinaryPath)).toEqual(badBinary);
  await expect(access(file)).rejects.toMatchObject({ code: "ENOENT" });
});

it("does not authorize repair for ordinary path, open or corrupt-database errors", async () => {
  const f = await fixture(await readFile(localAddon), { fallback: false });
  const worker = client();
  const corrupt = join(f.directory, "corrupt.sqlite");
  await writeFile(corrupt, "not a SQLite database");
  await expect(worker.open(config(corrupt, true), 2_000)).rejects.toThrow(
    /file is not a database/i,
  );
  expect(await readFile(corrupt, "utf8")).toBe("not a SQLite database");
  const missing = join(f.directory, "missing.sqlite");
  await expect(worker.open(config(missing, true), 2_000)).rejects.toThrow(
    /unable to open/i,
  );
  await expect(access(missing)).rejects.toMatchObject({ code: "ENOENT" });
  await expect(
    worker.open(config(join(f.directory, "no-directory", "db.sqlite")), 2_000),
  ).rejects.toThrow(/directory does not exist/i);
  expect(f.allowInstall).not.toHaveBeenCalled();
  expect(f.download).not.toHaveBeenCalled();
});

it("reports repair denial without downloading or opening the file", async () => {
  const f = await fixture(badBinary, { fallback: false });
  const file = join(f.directory, "not-created.sqlite");
  await expect(client().open(config(file), 2_000)).rejects.toThrow(
    /repair was not authorized/i,
  );
  expect(f.allowInstall).toHaveBeenCalledOnce();
  expect(f.download).not.toHaveBeenCalled();
  await expect(access(file)).rejects.toMatchObject({ code: "ENOENT" });
});

it("does not spawn a retry or download when the final waiter disconnects during repair authorization", async () => {
  const prompt = new EventEmitter();
  let respond!: (authorized: boolean) => void;
  const f = await fixture(badBinary, {
    fallback: false,
    allowInstall: () =>
      new Promise<boolean>((resolve) => {
        respond = resolve;
        prompt.emit("shown");
      }),
  });
  const worker = client();
  const file = join(f.directory, "not-created.sqlite");
  const shown = once(prompt, "shown");
  const rejected = expect(worker.open(config(file), 2_000)).rejects.toThrow(
    /cancelled/i,
  );
  await shown;
  await worker.close();
  await rejected;
  respond(true);
  await vi.waitFor(() =>
    expect(access(`${f.runtimeRoot}.lock`)).rejects.toMatchObject({
      code: "ENOENT",
    }),
  );
  expect(worker.connected).toBe(false);
  expect(f.download).not.toHaveBeenCalled();
  await expect(access(file)).rejects.toMatchObject({ code: "ENOENT" });
});
