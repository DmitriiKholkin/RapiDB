import { Admin, Collection, Db, MongoClient } from "mongodb";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ConnectionManager } from "../../src/extension/connectionManager";
import { MongoDBDriver } from "../../src/extension/dbDrivers/mongodb";
import { getConfiguredDefaultDatabaseName } from "../../src/extension/schema/schemaHelpers";
import { loadConnectionRootCatalog } from "../../src/extension/schema/schemaLoaders";
import { getConfiguredDefaultDatabaseName as getCacheDefaultDatabaseName } from "../../src/extension/schemaCacheUtils";
import type {
  ConnectionSshSettings,
  SshRuntimeRequest,
} from "../../src/extension/services/sshRuntime";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";
import { resolveMongoDatabaseName } from "../../src/shared/mongoDatabase";
import { parseConnectionFormSubmission } from "../../src/shared/webviewContracts/connection";
import {
  createExtensionContextStub,
  FakeConnectionManagerStore,
} from "../support/fakeConnectionManagerStore";

vi.mock("vscode", async () => ({
  EventEmitter: (await import("../support/mockVscode")).MockEventEmitter,
  ProgressLocation: { Window: 10 },
  window: {},
}));

function config(fields: Partial<ConnectionConfig> = {}): ConnectionConfig {
  return {
    id: "mongo-default",
    name: "Mongo default database",
    type: "mongodb",
    database: "",
    connectionUri: "mongodb://host/app",
    ...fields,
  };
}

const uriCases = [
  ["mongodb://host/app", "app"],
  ["mongodb://host:27018/app?authSource=admin", "app"],
  ["mongodb://u%40:p%3F%2F%23@host/app?authSource=auth", "app"],
  ["mongodb://a:27018,b:27019/app?replicaSet=rs0", "app"],
  ["mongodb://[::1]:27018,[2001:db8::42]:27019/app", "app"],
  ["mongodb+srv://cluster.example/app", "app"],
  ["mongodb+srv://u%40:p%25@cluster.example/a%3Fb?authSource=admin", "a?b"],
  ["mongodb://host/a%3Fb%23c%40d%25", "a?b#c@d%"],
  ["mongodb://host/a%252Fb", "a%2Fb"],
  ["mongodb://host/база数据库🚀", "база数据库🚀"],
  ["mongodb://host/ignored/../app", "app"],
  ["mongodb://host/?dbName=option", "option"],
  ["mongodb://host/?DBNAME=a%3Fb", "a?b"],
  ["mongodb://host/app?dbName=ignored&authSource=admin", "app"],
] as const;

describe("MongoDB database resolution", () => {
  it.each(
    uriCases,
  )("matches SDK parsing without networking: %s", (uri, database) => {
    expect(new MongoClient(uri).options.dbName).toBe(database);
    expect(resolveMongoDatabaseName({ connectionUri: uri })).toBe(database);
    expect(resolveMongoDatabaseName({ uri })).toBe(database);
    expect(
      getConfiguredDefaultDatabaseName(config({ connectionUri: uri })),
    ).toBe(database);
    expect(getCacheDefaultDatabaseName(config({ connectionUri: uri }))).toBe(
      database,
    );
  });

  it.each([
    undefined,
    "",
    "mongodb://host",
    "mongodb://host/",
    "mongodb://host/?authSource=auth",
    "mongodb+srv://cluster.example/",
  ])("preserves the legacy admin fallback without a URI database: %s", (connectionUri) => {
    const fields = { connectionUri };
    expect(resolveMongoDatabaseName(fields)).toBe("admin");
    expect(getConfiguredDefaultDatabaseName(config(fields))).toBe("admin");
    expect(getCacheDefaultDatabaseName(config(fields))).toBe("admin");
  });

  it("preserves explicit database precedence and connectionUri ?? uri selection", () => {
    const fields = {
      database: "explicit",
      connectionUri: "mongodb://host/app",
      uri: "mongodb://legacy/legacy",
    };
    expect(resolveMongoDatabaseName(fields)).toBe("explicit");
    expect(resolveMongoDatabaseName({ ...fields, database: "" })).toBe("app");
    expect(
      resolveMongoDatabaseName({
        ...fields,
        database: "",
        connectionUri: undefined,
      }),
    ).toBe("legacy");
    expect(
      resolveMongoDatabaseName({
        ...fields,
        database: "",
        connectionUri: "mongodb://host/",
      }),
    ).toBe("admin");
    expect(
      resolveMongoDatabaseName({ ...fields, database: "", connectionUri: "" }),
    ).toBe("admin");
  });

  it.each([
    " ",
    "\t",
    "\r\n",
  ])("ignores a whitespace-only Database field: %j", (database) => {
    const fields = { database, connectionUri: "mongodb://host/app" };
    expect(resolveMongoDatabaseName(fields)).toBe("app");
    expect(getConfiguredDefaultDatabaseName(config(fields))).toBe("app");
    expect(getCacheDefaultDatabaseName(config(fields))).toBe("app");
    expect(resolveMongoDatabaseName({ database })).toBe("admin");
  });

  it("does not silently select admin for malformed database escaping", () => {
    expect(() =>
      resolveMongoDatabaseName({ connectionUri: "mongodb://host/bad%XX" }),
    ).toThrow();
  });
});

describe("MongoDB default scopes with real SDK database and collection objects", () => {
  const drivers: MongoDBDriver[] = [];
  const managers: ConnectionManager[] = [];
  let clients: MongoClient[];
  let catalogs: string[];
  let reads: string[];
  let writes: string[];
  let commands: string[];
  let indexes: string[];
  let denyAdmin: boolean;
  let denyListDatabases: boolean;

  beforeEach(() => {
    clients = [];
    catalogs = [];
    reads = [];
    writes = [];
    commands = [];
    indexes = [];
    denyAdmin = false;
    denyListDatabases = false;
    // Stub I/O only: the real constructor, db(), collection(), databaseName,
    // and namespace ensure this test cannot mask an explicit wrong DB argument.
    vi.spyOn(MongoClient.prototype, "connect").mockImplementation(
      async function (this: MongoClient) {
        clients.push(this);
        return this;
      },
    );
    vi.spyOn(Db.prototype, "listCollections").mockImplementation(function (
      this: Db,
    ) {
      catalogs.push(this.databaseName);
      const database = this.databaseName;
      const checkPermission = () => {
        if (denyAdmin && database === "admin")
          throw new Error("not authorized on admin");
      };
      return {
        hasNext: async () => {
          checkPermission();
          return false;
        },
        toArray: async () => {
          checkPermission();
          return [{ name: "items", type: "collection", options: {} }];
        },
        close: async () => undefined,
      } as never;
    });
    vi.spyOn(Collection.prototype, "insertOne").mockImplementation(
      async function (this: Collection) {
        writes.push(this.namespace);
        return { acknowledged: true, insertedId: "probe" } as never;
      },
    );
    vi.spyOn(Collection.prototype, "find").mockImplementation(function (
      this: Collection,
    ) {
      reads.push(this.namespace);
      return {
        limit() {
          return this;
        },
        skip() {
          return this;
        },
        sort() {
          return this;
        },
        async toArray() {
          return [{ _id: "probe", value: 1 }];
        },
      } as never;
    });
    vi.spyOn(Collection.prototype, "indexes").mockImplementation(
      async function (this: Collection) {
        indexes.push(this.namespace);
        return [{ name: "_id_", key: { _id: 1 }, unique: true }];
      },
    );
    vi.spyOn(Db.prototype, "command").mockImplementation(async function (
      this: Db,
    ) {
      commands.push(this.databaseName);
      return { ok: 1 };
    });
    vi.spyOn(Admin.prototype, "listDatabases").mockImplementation(async () => {
      if (denyListDatabases) throw new Error("listDatabases denied");
      return { databases: [{ name: "other" }, { name: "app" }] } as never;
    });
  });

  afterEach(async () => {
    for (const manager of managers.splice(0)) await manager.dispose();
    for (const driver of drivers.splice(0)) await driver.disconnect();
    vi.restoreAllMocks();
  });

  async function connect(fields: Partial<ConnectionConfig> = {}) {
    const driver = new MongoDBDriver(config(fields));
    drivers.push(driver);
    await driver.connect();
    return driver;
  }

  function createManager(
    store = new FakeConnectionManagerStore(),
    dependencies = {},
  ) {
    const manager = new ConnectionManager(
      createExtensionContextStub() as never,
      store,
      dependencies,
    );
    managers.push(manager);
    return manager;
  }

  it.each([
    [{}, "app"],
    [{ database: undefined }, "app"],
    [{ database: " " }, "app"],
    [{ database: "\t" }, "app"],
    [{ database: "explicit" }, "explicit"],
    [{ connectionUri: undefined, uri: "mongodb://host/legacy" }, "legacy"],
    [{ connectionUri: "mongodb://host/" }, "admin"],
    [{ connectionUri: undefined, host: "host" }, "admin"],
    [{ connectionUri: "mongodb://a,b/app" }, "app"],
    [{ connectionUri: "mongodb+srv://cluster.example/app" }, "app"],
    [{ connectionUri: "mongodb://host/a%3Fb" }, "a?b"],
  ] satisfies Array<
    [Partial<ConnectionConfig>, string]
  >)("uses one default for connect, reads, writes, and commands: %j", async (fields, database) => {
    const driver = await connect(fields);
    expect(catalogs).toEqual([database]);
    await driver.query('db.items.insertOne({_id:"probe"})');
    await driver.query("db.items.find({})");
    await driver.query("db.runCommand({ping:1})");
    expect(writes).toEqual([`${database}.items`]);
    expect(reads).toEqual([`${database}.items`]);
    expect(commands).toEqual([database]);
  });

  it("keeps authentication independent of the selected operation database", async () => {
    await connect({
      database: "explicit",
      connectionUri: "mongodb://user:pass@host/app?authSource=auth",
    });
    expect(clients[0].options.credentials?.source).toBe("auth");
    expect(clients[0].db().databaseName).toBe("app");
    expect(catalogs).toEqual(["explicit"]);
  });

  it("honors sibling DB and explicit table scopes ahead of config.database and URI", async () => {
    const driver = await connect({ database: "explicit" });
    await driver.query(
      'db.getSiblingDB("sibling").items.insertOne({_id:"probe"})',
    );
    await driver.insertRow({
      database: "tableScope",
      schema: "tableScope",
      table: "items",
      values: { _id: "table" },
    });
    await driver.insertRow({
      database: "",
      schema: "",
      table: "items",
      values: { _id: "default" },
    });
    expect(writes).toEqual([
      "sibling.items",
      "tableScope.items",
      "explicit.items",
    ]);
  });

  it("uses the same default for metadata labels, samples, indexes, and generated DDL", async () => {
    const driver = await connect();
    expect(await driver.listSchemas("")).toEqual([{ name: "app" }]);
    expect(await driver.listObjects("")).toEqual([
      { schema: "app", name: "items", type: "table" },
    ]);
    await driver.describeColumns("", "", "items");
    await driver.getIndexes("", "", "items");
    expect(await driver.getIndexDDL("", "", "items", "_id_")).toContain(
      'db.getSiblingDB("app")',
    );
    expect(await driver.getCreateTableDDL("", "", "items")).toContain(
      'db.getSiblingDB("app")',
    );
    await driver.describeColumns("scope", "scope", "items");
    expect(reads).toEqual(["app.items", "scope.items"]);
    expect(indexes).toEqual(["app.items", "app.items"]);
    expect(
      catalogs.every((database) => database === "app" || database === "scope"),
    ).toBe(true);
  });

  it.each([
    false,
    true,
  ])("keeps startup catalog aligned with operations when listDatabases denied=%s", async (denied) => {
    denyListDatabases = denied;
    const driver = await connect();
    const catalog = await loadConnectionRootCatalog(driver, config());
    expect(catalog.defaultDatabaseName).toBe("app");
    expect(catalog.catalogSnapshot.databases[0].name).toBe("app");
    if (denied)
      expect(await driver.listDatabases()).toEqual([
        { name: "app", schemas: [] },
      ]);
  });

  it("includes an explicit configured database even when it is absent from server enumeration", async () => {
    const fields = { database: "explicit" };
    const driver = await connect(fields);
    expect(
      await loadConnectionRootCatalog(driver, config(fields)),
    ).toMatchObject({
      defaultDatabaseName: "explicit",
      catalogSnapshot: {
        databases: [{ name: "explicit" }, { name: "other" }, { name: "app" }],
      },
    });
  });

  it("does not require admin catalog access to test an app-only connection", async () => {
    denyAdmin = true;
    const manager = createManager();
    expect(await manager.testConnection(config())).toEqual({ success: true });
    expect(catalogs).toEqual(["app"]);
  });

  it.each([
    false,
    true,
  ])("preserves form/save/SecretStorage path and manager startup scopes (SSH=%s)", async (ssh) => {
    const store = new FakeConnectionManagerStore();
    const dispose = vi.fn(async () => undefined);
    const createSshRuntime = vi.fn(
      async (_settings: ConnectionSshSettings, request: SshRuntimeRequest) => {
        if (request.kind !== "tcpForward")
          throw new Error("Expected TCP forward");
        return {
          transport: {
            kind: "tcpForward" as const,
            localHost: "127.0.0.1" as const,
            localPort: 29017,
            remoteHost: request.remoteHost,
            remotePort: request.remotePort,
          },
          verifiedFingerprintSha256:
            "SHA256:AbCdEfGhIjKlMnOpQrStUvWxYz0123456789+/",
          dispose,
        };
      },
    );
    const manager = createManager(store, { createSshRuntime });
    const submission = parseConnectionFormSubmission(
      config({
        connectionUri: "mongodb://user:pass@host/app?authSource=auth",
        useSecretStorage: true,
        ...(ssh
          ? {
              ssh: {
                host: "bastion",
                username: "tunnel",
                authMethod: "password",
                password: "ssh-secret",
                hostVerificationMode: "manual",
                hostFingerprintSha256:
                  "SHA256:AbCdEfGhIjKlMnOpQrStUvWxYz0123456789+/",
              },
            }
          : {}),
      }),
    );
    expect(submission).not.toBeNull();
    const saved = await manager.saveConnection(submission as ConnectionConfig);
    expect(saved.database).toBe("");
    expect(saved.connectionUri).not.toContain("user:pass");
    expect(resolveMongoDatabaseName(saved)).toBe("app");
    expect(getConfiguredDefaultDatabaseName(saved)).toBe("app");
    await manager.connectTo(saved.id);
    await manager.getSchemaAsync(saved.id);
    expect(
      manager.getSchemaSnapshotState(saved.id, {
        kind: "database",
        database: "app",
      }).status,
    ).toBe("loaded");
    expect(manager.getSchemaSnapshot(saved.id).databases[0].name).toBe("app");
    await manager
      .getDriver(saved.id)
      ?.query('db.items.insertOne({_id:"probe"})');
    expect(writes).toEqual(["app.items"]);
    expect(clients[0].options.credentials).toMatchObject({
      username: "user",
      password: "pass",
      source: "auth",
    });
    expect(clients[0].options.dbName).toBe("app");
    expect(catalogs.every((database) => database === "app")).toBe(true);
    if (ssh) {
      expect(createSshRuntime).toHaveBeenCalledWith(
        expect.anything(),
        { kind: "tcpForward", remoteHost: "host", remotePort: 27017 },
        expect.anything(),
      );
      expect(clients[0].options.hosts[0].toString()).toBe("127.0.0.1:29017");
      expect(clients[0].options.directConnection).toBe(true);
    } else {
      expect(createSshRuntime).not.toHaveBeenCalled();
    }
    await manager.disconnectFrom(saved.id);
    if (ssh) expect(dispose).toHaveBeenCalledTimes(1);
  });
});
