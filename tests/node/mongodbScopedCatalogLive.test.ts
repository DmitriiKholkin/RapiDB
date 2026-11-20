import { randomUUID } from "node:crypto";
import { MongoClient } from "mongodb";
import { expect, it, vi } from "vitest";
import { ConnectionManager } from "../../src/extension/connectionManager";
import { colKey } from "../../src/extension/dbDrivers/types";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";
import {
  createExtensionContextStub,
  FakeConnectionManagerStore,
} from "../support/fakeConnectionManagerStore";

vi.mock("vscode", async () => ({
  EventEmitter: (await import("../support/mockVscode")).MockEventEmitter,
  ProgressLocation: { Window: 10 },
  window: {},
}));

const adminUri = process.env.RAPIDB_MONGO_SCOPED_TEST_ADMIN_URL;

it.skipIf(!adminUri)(
  "tests, connects, and discovers only permitted MongoDB namespaces as a scoped reader",
  async () => {
    const endpoint = new URL(adminUri ?? "");
    if (endpoint.protocol !== "mongodb:" || endpoint.hostname !== "127.0.0.1") {
      throw new Error("Use a disposable loopback MongoDB for this test.");
    }

    const database = `rapidb_scoped_${randomUUID().replaceAll("-", "")}`;
    const unrelatedDatabase = `${database}_other`;
    const username = "scoped_reader";
    const role = "scoped_find";
    const password = randomUUID();
    const config: ConnectionConfig = {
      id: database,
      name: "MongoDB scoped catalog test",
      type: "mongodb",
      mongoConnectionMode: "fields",
      host: endpoint.hostname,
      port: Number(endpoint.port || "27017"),
      database,
      authSource: database,
      username,
      password,
    };
    const root = new MongoClient(endpoint.toString(), {
      serverSelectionTimeoutMS: 4_000,
    });
    const db = root.db(database);
    const otherDb = root.db(unrelatedDatabase);
    const store = new FakeConnectionManagerStore();
    const manager = new ConnectionManager(
      createExtensionContextStub() as never,
      store,
    );
    let adminConnected = false;
    let roleCreated = false;
    let userCreated = false;
    try {
      await root.connect();
      adminConnected = true;
      await db.collection("items").insertOne({ marker: "visible" });
      await db.collection("hidden").insertOne({ marker: "hidden" });
      await db.createCollection("items_view", {
        viewOn: "items",
        pipeline: [],
      });
      await otherDb.collection("hidden").insertOne({ marker: "hidden" });
      await db.command({
        createRole: role,
        privileges: ["items", "items_view"].map((collection) => ({
          resource: { db: database, collection },
          actions: ["find"],
        })),
        roles: [],
      });
      roleCreated = true;
      await db.command({ createUser: username, pwd: password, roles: [role] });
      userCreated = true;

      store.setConnections([config]);
      expect.soft(await manager.testConnection(config)).toEqual({
        success: true,
      });
      await expect(manager.connectTo(config.id, "explicit")).resolves.toBe(
        undefined,
      );
      await manager.getSchemaAsync(config.id);
      expect(manager.getSchemaSnapshot(config.id).databases).toEqual([
        expect.objectContaining({ name: database }),
      ]);
      const driver = manager.getDriver(config.id);
      if (!driver) throw new Error("Scoped MongoDB driver was not registered.");
      const objects = await driver.listObjects(database, database);
      expect(objects).toHaveLength(2);
      expect(objects).toEqual(
        expect.arrayContaining([
          { schema: database, name: "items", type: "table" },
          { schema: database, name: "items_view", type: "view" },
        ]),
      );
      const result = await driver.query("db.items.find({}, {_id:0, marker:1})");
      expect(result.columns).toEqual(["marker"]);
      expect(result.rows).toEqual([{ [colKey(0)]: "visible" }]);
      await expect(driver.query("db.hidden.find({})")).rejects.toThrow(
        /not authorized/i,
      );
      expect(
        await driver.describeTable(database, database, "items_view"),
      ).toContainEqual(
        expect.objectContaining({ name: "_id", isPrimaryKey: false }),
      );
      await expect(
        driver.getCreateTableDDL(database, database, "items"),
      ).rejects.toMatchObject({ code: 13 });

      expect(
        await manager.testConnection({
          ...config,
          database: unrelatedDatabase,
        }),
      ).toMatchObject({
        success: false,
        error: expect.stringMatching(/permissions/),
      });
      expect(
        await manager.testConnection({ ...config, password: "wrong-password" }),
      ).toMatchObject({
        success: false,
        error: expect.stringMatching(/authentication failed/i),
      });
      expect(
        await manager.testConnection({
          ...config,
          username: undefined,
          password: undefined,
        }),
      ).toMatchObject({
        success: false,
        error: expect.stringMatching(/check authentication/i),
      });
      expect(
        await manager.testConnection({ ...config, database: "bad/name" }),
      ).toMatchObject({
        success: false,
        error: expect.stringMatching(/invalid database name/i),
      });
    } finally {
      try {
        await manager.dispose();
        if (userCreated) await db.command({ dropUser: username });
        if (roleCreated) await db.command({ dropRole: role });
        if (adminConnected) {
          await db.dropDatabase();
          await otherDb.dropDatabase();
        }
      } finally {
        await root.close();
      }
    }
  },
  30_000,
);
