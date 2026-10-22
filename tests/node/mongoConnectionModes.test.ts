import { Db, MongoClient } from "mongodb";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  restoreMongoUriCredentials,
  serializeConnectionSecretsForStoredConfig,
} from "../../src/extension/connectionSecrets";
import { MongoDBDriver } from "../../src/extension/dbDrivers/mongodb";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";
import { validateConnectionConfig } from "../../src/shared/connectionValidation";
import {
  buildMongoFieldsUri,
  getMongoFormHost,
  getMongoFormUri,
  normalizeMongoConnectionConfig,
} from "../../src/shared/mongoConnection";
import { resolveMongoDatabaseName } from "../../src/shared/mongoDatabase";
import { parseConnectionFormSubmission } from "../../src/shared/webviewContracts/connection";

const fields: ConnectionConfig = {
  id: "mongo-modes",
  name: "Mongo modes",
  type: "mongodb",
  mongoConnectionMode: "fields",
  host: "localhost",
  port: 27017,
  database: "app",
  mongoAuthMethod: "password",
  username: "user@name",
  password: "p:/?#% word",
  authSource: "admin",
  authMechanism: "SCRAM-SHA-256",
  directConnection: true,
};

afterEach(() => vi.restoreAllMocks());

describe("MongoDB connection modes", () => {
  it("round-trips fields through the webview contract and matches the equivalent URI", () => {
    const parsed = parseConnectionFormSubmission(fields);
    expect(parsed).toMatchObject(fields);
    const client = new MongoClient(buildMongoFieldsUri(parsed ?? {}));
    expect(client.options.credentials).toMatchObject({
      username: fields.username,
      password: fields.password,
      source: "admin",
      mechanism: "SCRAM-SHA-256",
    });
    expect(client.options.dbName).toBe("app");
    expect(client.options.directConnection).toBe(true);
  });

  it("uses a database user independently of the working database and permits automatic authSource", () => {
    const uri = buildMongoFieldsUri({ ...fields, authSource: undefined });
    expect(new MongoClient(uri).options.credentials?.source).toBe("app");
    expect(validateConnectionConfig(fields).valid).toBe(true);
  });

  it.each([
    {
      mongoScheme: "mongodb+srv" as const,
      mongoAuthMethod: "password" as const,
    },
    { mongoScheme: "mongodb" as const, mongoAuthMethod: "none" as const },
  ])("honors inferred fields settings in the driver ($mongoScheme, $mongoAuthMethod)", async (changes) => {
    const config = {
      ...fields,
      ...changes,
      mongoConnectionMode: undefined,
      host: "cluster.example.com",
      directConnection: undefined,
    };
    expect(validateConnectionConfig(config).valid).toBe(true);
    let client: MongoClient | undefined;
    vi.spyOn(MongoClient.prototype, "connect").mockImplementation(
      async function (this: MongoClient) {
        client = this;
        return this;
      },
    );
    vi.spyOn(Db.prototype, "listCollections").mockReturnValue({
      hasNext: async () => false,
      close: async () => undefined,
    } as never);
    const driver = new MongoDBDriver(config);
    try {
      await driver.connect();
      if (changes.mongoScheme === "mongodb+srv") {
        expect(client?.options.srvHost).toBe("cluster.example.com");
        expect(client?.options.credentials?.username).toBe(fields.username);
      } else {
        expect(client?.options.credentials).toBeUndefined();
        expect(
          serializeConnectionSecretsForStoredConfig(
            normalizeMongoConnectionConfig(config),
            JSON.stringify({ password: "previous-password" }),
          ),
        ).toBeUndefined();
      }
    } finally {
      await driver.disconnect();
    }
  });

  it("drops stale settings from both inactive editors", () => {
    const uriConfig = normalizeMongoConnectionConfig({
      ...fields,
      mongoConnectionMode: "uri" as const,
      connectionUri: "mongodb://host/uri-db",
    });
    expect(uriConfig.database).toBe("app");
    expect(uriConfig.password).toBeUndefined();
    expect(uriConfig.authSource).toBeUndefined();
    expect(
      resolveMongoDatabaseName({
        ...fields,
        mongoConnectionMode: "uri",
        connectionUri: "mongodb://host/uri-db",
      }),
    ).toBe("app");
    expect(
      normalizeMongoConnectionConfig({
        ...fields,
        connectionUri: "mongodb://ignored",
      }).connectionUri,
    ).toBeUndefined();
  });

  it("preserves legacy overrides and unknown/repeated URI options when opening the form", () => {
    const uri = getMongoFormUri({
      connectionUri:
        "mongodb://u:p@host/original?AUTHSOURCE=old&readPreferenceTags=a:b&readPreferenceTags=c:d&retryWrites=false",
      database: "app",
      authSource: "admin",
      directConnection: false,
    });
    const client = new MongoClient(uri ?? "");
    expect(client.options.dbName).toBe("original");
    expect(
      resolveMongoDatabaseName({ database: "app", connectionUri: uri }),
    ).toBe("app");
    expect(client.options.credentials?.source).toBe("admin");
    expect(client.options.directConnection).toBe(false);
    expect(client.options.retryWrites).toBe(false);
    expect(client.options.readPreference.tags).toEqual([
      { a: "b" },
      { c: "d" },
    ]);
  });

  it.each([
    "mongodb://u:p@host/users",
    "mongodb://u:p@host",
    "mongodb://u:p@host/?dbName=users",
    "mongodb+srv://u:p@cluster.example/users",
    "mongodb://host/users?authMechanism=MONGODB-X509",
  ])("preserves implicit authentication and a separate working database on edit: %s", (connectionUri) => {
    const previous = new MongoClient(connectionUri);
    const legacy = { type: "mongodb" as const, connectionUri, database: "app" };
    const formUri = getMongoFormUri(legacy);
    const saved = normalizeMongoConnectionConfig({
      ...legacy,
      connectionUri: formUri,
      mongoConnectionMode: "uri" as const,
    });
    const edited = new MongoClient(saved.connectionUri ?? "");
    expect(edited.options.credentials?.source).toBe(
      previous.options.credentials?.source,
    );
    expect(edited.options).toMatchObject({ userSpecifiedAuthSource: false });
    expect(formUri).toBe(connectionUri);
    expect(resolveMongoDatabaseName(saved)).toBe("app");
  });

  it.each([
    undefined,
    "uri",
  ] as const)("retains URI certificate files with explicit TLS verification (mode=%s)", async (mongoConnectionMode) => {
    let client: MongoClient | undefined;
    vi.spyOn(MongoClient.prototype, "connect").mockImplementation(
      async function (this: MongoClient) {
        client = this;
        return this;
      },
    );
    vi.spyOn(Db.prototype, "listCollections").mockReturnValue({
      hasNext: async () => false,
      close: async () => undefined,
    } as never);
    const driver = new MongoDBDriver({
      id: "mongo-uri-tls-files",
      name: "Mongo URI TLS files",
      type: "mongodb",
      mongoConnectionMode,
      connectionUri:
        "mongodb://host/app?tlsCAFile=%2Fcerts%2Fca.pem&tlsCertificateKeyFile=%2Fcerts%2Fclient.pem&tlsCRLFile=%2Fcerts%2Fcrl.pem&tlsInsecure=true",
      tls: { mode: "requireVerifyFull" },
    });
    try {
      await driver.connect();
      expect(client?.options).toMatchObject({
        tls: true,
        tlsCAFile: "/certs/ca.pem",
        tlsCertificateKeyFile: "/certs/client.pem",
        tlsCRLFile: "/certs/crl.pem",
        rejectUnauthorized: true,
        checkServerIdentity: expect.any(Function),
      });
    } finally {
      await driver.disconnect();
    }
  });

  it("handles IPv6 and applies the default port to every seed host", () => {
    const client = new MongoClient(
      buildMongoFieldsUri({
        ...fields,
        host: "[::1],mongo:27018",
        directConnection: false,
      }),
    );
    expect(client.options.hosts.map((host) => host.toString())).toEqual([
      "[::1]:27017",
      "mongo:27018",
    ]);
  });

  it.each([
    ["mongo-a,mongo-b", ["mongo-a:27017", "mongo-b:27018"]],
    ["mongo-a:27019,mongo-b", ["mongo-a:27019", "mongo-b:27018"]],
    ["[::1]:27019,[::2]", ["[::1]:27019", "[::2]:27018"]],
  ] as const)("preserves legacy seed ports through opening and saving fields: %s", (host, expected) => {
    const legacy: ConnectionConfig = {
      id: "legacy-seeds",
      name: "Legacy seeds",
      type: "mongodb",
      host,
      port: 27018,
      database: "app",
    };
    const untouched = normalizeMongoConnectionConfig(legacy);
    expect(untouched.mongoConnectionMode).toBeUndefined();
    const edited = normalizeMongoConnectionConfig({
      ...legacy,
      host: getMongoFormHost(legacy),
      mongoConnectionMode: "fields" as const,
    });
    const client = new MongoClient(buildMongoFieldsUri(edited));
    expect(client.options.hosts.map((entry) => entry.toString())).toEqual(
      expected,
    );
    expect(
      getMongoFormHost({ ...legacy, host: getMongoFormHost(legacy) }),
    ).toBe(edited.host);
  });

  it.each([
    { mongoConnectionMode: "uri", connectionUri: "https://host" },
    { mongoConnectionMode: "uri", connectionUri: "" },
    { mongoScheme: "mongodb+srv", host: "host:27017" },
    { mongoScheme: "mongodb+srv", directConnection: true },
    { host: "a,b", directConnection: true },
    { username: "" },
  ] as const)("rejects incompatible settings: %j", (changes) => {
    expect(validateConnectionConfig({ ...fields, ...changes }).valid).toBe(
      false,
    );
  });

  it("preserves stored URI credentials when changing the database/options and removes them explicitly", () => {
    const previous =
      "mongodb://u:p%40ss@localhost/app?authSource=admin&tlsCertificateKeyFilePassword=secret";
    const edited = "mongodb://localhost/other?authSource=admin";
    const restored = restoreMongoUriCredentials(edited, previous, true);
    expect(new MongoClient(restored ?? "").options.credentials?.password).toBe(
      "p@ss",
    );
    expect(restored).toContain("tlsCertificateKeyFilePassword=secret");
    expect(restoreMongoUriCredentials(edited, previous, false)).toBe(edited);
    expect(
      restoreMongoUriCredentials(
        "mongodb://new:newpass@localhost/app",
        previous,
        true,
      ),
    ).toContain("new:newpass@");
    expect(
      serializeConnectionSecretsForStoredConfig(
        {
          ...fields,
          mongoConnectionMode: "uri",
          connectionUri: "mongodb://localhost/app?authSource=admin",
          mongoUriHasCredentials: false,
        },
        JSON.stringify({ connectionUri: previous, password: "old" }),
      ),
    ).toBeUndefined();
  });

  it.each([
    { password: "" },
    { mongoAuthMethod: "none" as const },
  ])("removes a stored password when explicitly cleared: %j", (changes) => {
    expect(
      serializeConnectionSecretsForStoredConfig(
        { ...fields, ...changes },
        JSON.stringify({ password: "old" }),
      ),
    ).toBeUndefined();
  });

  it.each([
    ["mongodb://host/app?tls=true", undefined, true],
    ["mongodb+srv://cluster.example/app", undefined, true],
    ["mongodb://host/app?tls=true", { mode: "disabled" }, false],
  ] as const)("preserves URI/SRV TLS unless explicitly overridden: %s", async (connectionUri, tls, expected) => {
    let client: MongoClient | undefined;
    vi.spyOn(MongoClient.prototype, "connect").mockImplementation(
      async function (this: MongoClient) {
        client = this;
        return this;
      },
    );
    vi.spyOn(Db.prototype, "listCollections").mockReturnValue({
      hasNext: async () => false,
      close: async () => undefined,
    } as never);
    const driver = new MongoDBDriver({
      ...fields,
      mongoConnectionMode: "uri",
      connectionUri,
      tls,
    });
    try {
      await driver.connect();
      expect(client?.options.tls).toBe(expected);
    } finally {
      await driver.disconnect();
    }
  });
});
