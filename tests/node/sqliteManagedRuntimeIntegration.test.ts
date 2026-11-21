import { realpathSync } from "node:fs";
import { access, readFile, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { dirname, join } from "node:path";
import { afterEach, expect, it, vi } from "vitest";
import { openSQLiteDatabase } from "../../src/extension/dbDrivers/sqliteRuntime";
import { SQLiteWorkerClient } from "../../src/extension/dbDrivers/sqliteWorkerClient";
import { SQLITE_ARTIFACTS } from "../../src/extension/utils/sqliteArtifactPins";
import {
  configureSQLiteInstaller,
  probeInstalledBetterSqlite3Runtime,
  resetSQLiteInstallerForTests,
  sqliteNativeBindingPath,
} from "../../src/extension/utils/sqliteInstaller";
import { resolveSQLiteNativeTarget } from "../../src/extension/utils/sqliteNativeTarget";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";
import { createProjectTempDir } from "../runtime/tempDirectories";
import {
  gzipTar,
  mockRegistry,
  pinTarball,
  restoreSQLiteArtifacts,
  TARBALL_URL,
} from "./sqliteRuntimeFixtures";

const require = createRequire(import.meta.url);
const localPackage = dirname(require.resolve("better-sqlite3/package.json"));
const clients = new Set<SQLiteWorkerClient>();
const hostTarget = (() => {
  try {
    return resolveSQLiteNativeTarget();
  } catch {
    return null;
  }
})();
const supported =
  hostTarget !== null && SQLITE_ARTIFACTS.prebuilds[hostTarget] !== undefined;

afterEach(async () => {
  await Promise.all([...clients].map((client) => client.close()));
  clients.clear();
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
  resetSQLiteInstallerForTests();
  restoreSQLiteArtifacts();
});

// Re-pack the lockfile-verified local better-sqlite3 13.x package as an npm
// tarball. Only the tarball digest is re-pinned; every JavaScript file and the
// host prebuild keep their reviewed SHA-256 pins, so this exercises the real
// verification and loading path without network access.
async function localRegistryTarball(): Promise<Buffer> {
  const paths = [
    ...Object.keys(SQLITE_ARTIFACTS.files),
    ...Object.values(SQLITE_ARTIFACTS.prebuilds).map((pin) => pin.path),
    "binding.gyp",
    "README.md",
  ];
  const entries = await Promise.all(
    paths.map(async (path) => ({
      name: `package/${path}`,
      data: await readFile(join(localPackage, path)),
    })),
  );
  const data = gzipTar(entries);
  pinTarball(data);
  return data;
}

async function fixture(
  options: {
    fallback?: boolean;
    allowInstall?: () => boolean | Promise<boolean>;
  } = {},
) {
  const directory = await createProjectTempDir("sqlite-managed-runtime");
  const storageRoot = join(directory, "storage");
  const allowInstall = vi.fn(options.allowInstall ?? (() => true));
  configureSQLiteInstaller({ storageRoot, allowInstall, log: () => {} });
  const tarball = await localRegistryTarball();
  const download = mockRegistry((url) => {
    if (url !== TARBALL_URL) throw new Error(`Unexpected download ${url}`);
    return { data: tarball };
  });
  if (options.fallback === false) {
    // Run the real bundled worker but model a VSIX, which contains no
    // better-sqlite3 at all, even though this checkout has one installed.
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
  const { installedPackagePath, installedBinaryPath } =
    probeInstalledBetterSqlite3Runtime();
  if (!installedPackagePath || !installedBinaryPath)
    throw new Error("Missing managed runtime paths");
  return {
    directory,
    storageRoot,
    allowInstall,
    download,
    installedPackagePath,
    installedBinaryPath,
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
    name: "SQLite managed runtime test",
    type: "sqlite",
    filePath,
    readOnly,
  } satisfies ConnectionConfig;
}

it.skipIf(!supported)(
  "installs better-sqlite3 13.x from the pinned tarball and opens :memory: through its verified nativeBinding",
  async () => {
    const f = await fixture();
    const db = await openSQLiteDatabase(config());
    try {
      expect(db.get("SELECT 40 + 2 AS value")).toEqual({ value: 42 });
      expect(
        (db.get("SELECT sqlite_version() AS version") as { version: string })
          .version,
      ).toMatch(/^3\.\d+\.\d+$/);
    } finally {
      db.close();
    }
    expect(f.allowInstall).toHaveBeenCalledOnce();
    expect(f.download).toEqual([TARBALL_URL]);
    expect(await readFile(f.installedBinaryPath)).toEqual(
      await readFile(join(localPackage, `prebuilds/${hostTarget}.node`)),
    );
    // The managed JavaScript and the exact managed binary were loaded, not the
    // development copy in node_modules.
    const managed = realpathSync(f.installedPackagePath);
    expect(require.cache[join(managed, "lib", "index.js")]).toBeDefined();
    expect(require.cache[sqliteNativeBindingPath(managed)]).toBeDefined();
  },
  20_000,
);

it.skipIf(!supported)(
  "runs the worker on the managed runtime alone, reusing the install without another download",
  async () => {
    const f = await fixture({ fallback: false });
    const first = client();
    await first.open(config(), 10_000);
    await expect(
      first.call("query", ["SELECT 42"], 2_000),
    ).resolves.toMatchObject({ rows: [{ __col_0: 42 }] });
    await first.close();

    const second = client();
    await second.open(config(), 10_000);
    await expect(
      second.call("query", ["SELECT 7"], 2_000),
    ).resolves.toMatchObject({ rows: [{ __col_0: 7 }] });
    expect(f.allowInstall).toHaveBeenCalledOnce();
    expect(f.download).toHaveLength(1);
  },
  20_000,
);

it.skipIf(!supported)(
  "never loads a modified cached binary: it asks to reinstall and opens nothing when declined",
  async () => {
    const f = await fixture({ fallback: false });
    const worker = client();
    await worker.open(config(), 10_000);
    await worker.close();
    const original = await readFile(f.installedBinaryPath);
    const tampered = Buffer.from(original);
    tampered[tampered.length - 1] ^= 1;
    await writeFile(f.installedBinaryPath, tampered);

    resetSQLiteInstallerForTests();
    const deny = vi.fn(() => false);
    configureSQLiteInstaller({
      storageRoot: f.storageRoot,
      allowInstall: deny,
    });
    const file = join(f.directory, "must-not-open.sqlite");
    await expect(client().open(config(file), 2_000)).rejects.toThrow(
      /installation was not authorized/,
    );
    expect(deny).toHaveBeenCalledOnce();
    expect(f.download).toHaveLength(1);
    await expect(access(file)).rejects.toMatchObject({ code: "ENOENT" });
    expect(await readFile(f.installedBinaryPath)).toEqual(tampered);
  },
  20_000,
);

it.skipIf(!supported)(
  "does not ask to reinstall for ordinary path, open or corrupt-database errors",
  async () => {
    const f = await fixture({ fallback: false });
    const worker = client();
    await worker.open(config(), 10_000);
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
      worker.open(
        config(join(f.directory, "no-directory", "db.sqlite")),
        2_000,
      ),
    ).rejects.toThrow(/directory does not exist/i);
    expect(f.allowInstall).toHaveBeenCalledOnce();
    expect(f.download).toHaveLength(1);
  },
  20_000,
);
