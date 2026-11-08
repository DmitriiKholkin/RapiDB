import {
  mkdtemp,
  readdir,
  readFile,
  rm,
  stat,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it, vi } from "vitest";
import {
  createExtensionContextStub,
  FakeConnectionManagerStore,
} from "../support/fakeConnectionManagerStore";
import { MockEventEmitter } from "../support/mockVscode";

vi.mock("vscode", () => ({
  EventEmitter: MockEventEmitter,
  ProgressLocation: { Window: 10 },
  window: {},
}));

describe("SQLite test connection with the actual driver", () => {
  it("checks existing WAL and missing paths without creating the requested database or changing its journal", async () => {
    const { ConnectionManager } = await import(
      "../../src/extension/connectionManager"
    );
    const { SQLiteDriver } = await import(
      "../../src/extension/dbDrivers/sqlite"
    );
    const dir = await mkdtemp(join(tmpdir(), "rapidb-sqlite-connect-"));
    const existing = join(dir, "existing.sqlite");
    const missing = join(dir, "missing.sqlite");
    const source = new SQLiteDriver({
      id: "source",
      name: "source",
      type: "sqlite",
      filePath: existing,
    });
    const manager = new ConnectionManager(
      createExtensionContextStub() as never,
      new FakeConnectionManagerStore(),
    );
    try {
      await source.connect();
      await source.query("PRAGMA journal_mode = WAL");
      await source.query("CREATE TABLE probe (value TEXT)");
      await source.query("INSERT INTO probe VALUES ('kept')");
      const databaseBefore = await readFile(existing);
      const walBefore = await readFile(`${existing}-wal`);
      expect(
        await manager.testConnection({
          name: "existing",
          type: "sqlite",
          filePath: existing,
        }),
      ).toMatchObject({ success: true });
      expect(await readFile(existing)).toEqual(databaseBefore);
      expect(await readFile(`${existing}-wal`)).toEqual(walBefore);
      expect((await source.query("SELECT value FROM probe")).rows).toHaveLength(
        1,
      );
      expect(
        await manager.testConnection({
          name: "new",
          type: "sqlite",
          filePath: missing,
        }),
      ).toMatchObject({
        success: true,
        message: expect.stringMatching(/created when connecting/),
      });
      await expect(stat(missing)).rejects.toMatchObject({ code: "ENOENT" });
      expect(
        (await readdir(dir)).filter((name) => name.startsWith(".rapidb-test-")),
      ).toEqual([]);
    } finally {
      await manager.dispose();
      await source.disconnect();
      await rm(dir, { recursive: true, force: true });
    }
  });

  it("reports a missing file for read-only profiles without creating it", async () => {
    const { ConnectionManager } = await import(
      "../../src/extension/connectionManager"
    );
    const dir = await mkdtemp(join(tmpdir(), "rapidb-sqlite-connect-"));
    const missing = join(dir, "missing.sqlite");
    const manager = new ConnectionManager(
      createExtensionContextStub() as never,
      new FakeConnectionManagerStore(),
    );
    try {
      expect(
        await manager.testConnection({
          name: "readonly-missing",
          type: "sqlite",
          filePath: missing,
          readOnly: true,
        }),
      ).toEqual({
        success: false,
        error:
          "SQLite file does not exist; read-only connections do not create a database.",
      });
      expect(await readdir(dir)).toEqual([]);
    } finally {
      await manager.dispose();
      await rm(dir, { recursive: true, force: true });
    }
  });

  it("reports a missing parent directory for writable profiles", async () => {
    const { ConnectionManager } = await import(
      "../../src/extension/connectionManager"
    );
    const dir = await mkdtemp(join(tmpdir(), "rapidb-sqlite-connect-"));
    const parent = join(dir, "absent");
    const manager = new ConnectionManager(
      createExtensionContextStub() as never,
      new FakeConnectionManagerStore(),
    );
    try {
      expect(
        await manager.testConnection({
          name: "writable-missing-dir",
          type: "sqlite",
          filePath: join(parent, "new.sqlite"),
        }),
      ).toEqual({
        success: false,
        error: `Directory does not exist: ${parent}`,
      });
      expect(await readdir(dir)).toEqual([]);
    } finally {
      await manager.dispose();
      await rm(dir, { recursive: true, force: true });
    }
  });

  it("reports consistent messages when a path component is a file", async () => {
    const { ConnectionManager } = await import(
      "../../src/extension/connectionManager"
    );
    const dir = await mkdtemp(join(tmpdir(), "rapidb-sqlite-connect-"));
    const parent = join(dir, "not-a-directory");
    await writeFile(parent, "fixture");
    const filePath = join(parent, "db.sqlite");
    const manager = new ConnectionManager(
      createExtensionContextStub() as never,
      new FakeConnectionManagerStore(),
    );
    try {
      expect(
        await manager.testConnection({
          name: "writable-file-parent",
          type: "sqlite",
          filePath,
        }),
      ).toEqual({
        success: false,
        error: `Directory does not exist: ${parent}`,
      });
      expect(
        await manager.testConnection({
          name: "readonly-file-parent",
          type: "sqlite",
          filePath,
          readOnly: true,
        }),
      ).toEqual({
        success: false,
        error:
          "SQLite file does not exist; read-only connections do not create a database.",
      });
      expect(await readdir(dir)).toEqual(["not-a-directory"]);
      expect(await readFile(parent, "utf8")).toBe("fixture");
    } finally {
      await manager.dispose();
      await rm(dir, { recursive: true, force: true });
    }
  });
});
