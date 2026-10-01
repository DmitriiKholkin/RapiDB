import { MongoClient } from "mongodb";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MongoDBDriver } from "../../src/extension/dbDrivers/mongodb";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";

function createDriver(config: Partial<ConnectionConfig> = {}) {
  return new MongoDBDriver({
    id: "mongodb-uri",
    name: "MongoDB URI",
    type: "mongodb",
    ...config,
  });
}

function buildUri(config: Partial<ConnectionConfig> = {}) {
  return (createDriver(config) as unknown as { buildUri(): string }).buildUri();
}

// These are URI parser tests, not server-side database-name validation.
// Constructing the installed MongoClient does not open sockets or resolve DNS.
describe("MongoDBDriver — host-based URI parsing", () => {
  it.each([
    ["::1", "::1"],
    ["2001:db8::42", "2001:db8::42"],
    ["2001:0db8:0000:0000:0000:ff00:0042:8329", "2001:db8::ff00:42:8329"],
    ["::ffff:192.0.2.1", "::ffff:c000:201"],
  ])("brackets a bare IPv6 literal: %s", (host, parsedHost) => {
    const uri = buildUri({ host, port: 27018, database: "appdb" });
    expect(uri).toBe(`mongodb://[${host}]:27018/appdb`);
    const client = new MongoClient(uri);
    expect(client.options.hosts).toHaveLength(1);
    expect(client.options.hosts[0]).toMatchObject({
      host: parsedHost,
      port: 27018,
      isIPv6: true,
    });
    expect(client.options.dbName).toBe("appdb");
  });

  it.each([
    ["[::1]", "::1"],
    ["[2001:db8::42]", "2001:db8::42"],
    ["[::ffff:192.0.2.1]", "::ffff:c000:201"],
  ])("does not double-bracket IPv6: %s", (host, parsedHost) => {
    const uri = buildUri({ host });
    expect(uri).toBe(`mongodb://${host}:27017/admin`);
    expect(new MongoClient(uri).options.hosts[0]).toMatchObject({
      host: parsedHost,
      port: 27017,
      isIPv6: true,
    });
  });

  it.each([
    { host: "mongo.internal", expected: "mongo.internal" },
    { host: "127.0.0.1", expected: "127.0.0.1" },
    { host: "  ::1  ", expected: "[::1]" },
    { host: "  [::1]  ", expected: "[::1]" },
    { host: "  localhost  ", expected: "localhost" },
    { host: "   ", expected: "localhost" },
    { host: undefined, expected: "localhost" },
  ])("preserves host trimming/defaults: $host", ({ host, expected }) => {
    const uri = buildUri({ host });
    expect(uri).toBe(`mongodb://${expected}:27017/admin`);
    expect(new MongoClient(uri).options.hosts[0].toString()).toBe(
      `${expected}:27017`,
    );
  });

  it.each([undefined, ""])("defaults database %s to admin", (database) => {
    const client = new MongoClient(buildUri({ database, username: "user" }));
    expect(client.options.dbName).toBe("admin");
    expect(client.options.credentials?.source).toBe("admin");
  });

  it.each([
    ["a/b", "a%2Fb"],
    ["a?b", "a%3Fb"],
    ["a#b", "a%23b"],
    ["a@b", "a%40b"],
    ["a%b", "a%25b"],
    ["a%2Fb", "a%252Fb"],
    [
      "база数据库🚀",
      "%D0%B1%D0%B0%D0%B7%D0%B0%E6%95%B0%E6%8D%AE%E5%BA%93%F0%9F%9A%80",
    ],
    ["a/?#@% база", "a%2F%3F%23%40%25%20%D0%B1%D0%B0%D0%B7%D0%B0"],
    [
      "a?authSource=other&replicaSet=other",
      "a%3FauthSource%3Dother%26replicaSet%3Dother",
    ],
  ])("encodes database as one URI component: %s", (database, encoded) => {
    const uri = buildUri({ host: "localhost", database, username: "user" });
    expect(uri).toBe(`mongodb://user:@localhost:27017/${encoded}`);
    const client = new MongoClient(uri);
    expect(client.options.dbName).toBe(database);
    expect(client.options.credentials?.source).toBe(database);
    expect(client.options.replicaSet).toBeUndefined();
  });

  it("round-trips credentials and query values independently of database escaping", () => {
    const username = "user:/?#@% 用户";
    const password = "pass:/?#@%+ 密码🚀";
    const authSource = "auth/?#@%+ база";
    const replicaSet = "rs/?#@%+ 集群";
    const uri = buildUri({
      host: "::1",
      database: "app/?#@% 数据",
      username,
      password,
      authSource,
      replicaSet,
      directConnection: false,
    });
    expect(uri).toContain(
      "user%3A%2F%3F%23%40%25%20%E7%94%A8%E6%88%B7:pass%3A%2F%3F%23%40%25%2B%20%E5%AF%86%E7%A0%81%F0%9F%9A%80@",
    );
    expect(uri).toContain("?authSource=auth%2F%3F%23%40%25%2B+");
    const client = new MongoClient(uri);
    expect(client.options.dbName).toBe("app/?#@% 数据");
    expect(client.options.credentials).toMatchObject({
      username,
      password,
      source: authSource,
    });
    expect(client.options.replicaSet).toBe(replicaSet);
    expect(client.options.directConnection).toBe(false);
  });

  it.each([
    ["mongo-a,mongo-b", ["mongo-a:27017", "mongo-b:27019"]],
    ["mongo-a:27018,mongo-b", ["mongo-a:27018", "mongo-b:27019"]],
    ["[::1]:27018,[2001:db8::42]", ["[::1]:27018", "[2001:db8::42]:27019"]],
  ])("preserves existing seed list/port semantics: %s", (host, hosts) => {
    const uri = buildUri({ host, port: 27019, replicaSet: "rs0" });
    expect(uri).toBe(`mongodb://${host}:27019/admin?replicaSet=rs0`);
    const client = new MongoClient(uri);
    expect(client.options.hosts.map((entry) => entry.toString())).toEqual(
      hosts,
    );
    expect(client.options.replicaSet).toBe("rs0");
  });

  it.each([
    "mongo.internal:27018",
    "[::1]:27018",
    "not:an:ipv6",
  ])("does not reinterpret a host with a port or invalid colon syntax: %s", (host) => {
    const uri = buildUri({ host, port: 27019 });
    expect(uri).toBe(`mongodb://${host}:27019/admin`);
    expect(() => new MongoClient(uri)).toThrow();
  });

  it.each([
    "mongodb://localhost",
    "localhost/path",
    "localhost?replicaSet=rs",
    "localhost#fragment",
    "user@localhost",
  ])("does not normalize legacy URI syntax inside host: %s", (host) => {
    expect(buildUri({ host, port: 27019 })).toBe(
      `mongodb://${host}:27019/admin`,
    );
  });
});

describe("MongoDBDriver — real MongoClient constructor through connect()", () => {
  afterEach(() => vi.restoreAllMocks());

  it.each([
    { field: "connectionUri", scheme: "mongodb", authority: "[::1]:27018" },
    {
      field: "connectionUri",
      scheme: "mongodb+srv",
      authority: "cluster.example",
    },
    { field: "uri", scheme: "mongodb", authority: "legacy.example:27018" },
  ] as const)("parses explicit $field ($scheme) without invoking buildUri", async ({
    field,
    scheme,
    authority,
  }) => {
    let client: MongoClient | undefined;
    vi.spyOn(MongoClient.prototype, "connect").mockImplementation(
      async function (this: MongoClient) {
        client = this;
        return this;
      },
    );
    const driver = createDriver({
      [field]: `${scheme}://u%40:p%25@${authority}/a%3Fb?authSource=uri%2Fauth&retryWrites=true`,
      host: "ignored:host",
      database: "ignored/?#@%",
      username: "ignored-user",
      password: "ignored-password",
    });
    const builder = vi.spyOn(
      driver as unknown as { buildUri(): string },
      "buildUri",
    );
    try {
      await driver.connect();
      expect(builder).not.toHaveBeenCalled();
      expect(client).toBeInstanceOf(MongoClient);
      expect(client?.options.dbName).toBe("a?b");
      expect(client?.options.credentials).toMatchObject({
        username: "u@",
        password: "p%",
        source: "uri/auth",
      });
      expect(client?.options.retryWrites).toBe(true);
      if (scheme === "mongodb+srv") {
        expect(client?.options.srvHost).toBe("cluster.example");
        expect(client?.options.hosts).toEqual([]);
      } else {
        expect(client?.options.hosts[0].toString()).toBe(authority);
      }
    } finally {
      await driver.disconnect();
    }
  });

  it("parses the generated IPv6 URI and preserves constructor options without networking", async () => {
    const clients: MongoClient[] = [];
    const connect = vi
      .spyOn(MongoClient.prototype, "connect")
      .mockImplementation(async function (this: MongoClient) {
        clients.push(this);
        return this;
      });
    const driver = createDriver({
      host: "::1",
      port: 27018,
      database: "app/?#@% база",
      username: "user/?#@% 用户",
      password: "pass/?#@% 密码",
      authSource: "auth/?#@% база",
      replicaSet: "rs/?#@% 集群",
      directConnection: true,
    });
    try {
      await driver.connect();
      expect(connect).toHaveBeenCalledTimes(1);
      expect(driver.isConnected()).toBe(true);
      expect(clients[0].options.hosts[0]).toMatchObject({
        host: "::1",
        port: 27018,
        isIPv6: true,
      });
      expect(clients[0].options.dbName).toBe("app/?#@% база");
      expect(clients[0].options.credentials).toMatchObject({
        username: "user/?#@% 用户",
        password: "pass/?#@% 密码",
        source: "auth/?#@% база",
      });
      expect(clients[0].options.replicaSet).toBe("rs/?#@% 集群");
      expect(clients[0].options.directConnection).toBe(true);
    } finally {
      await driver.disconnect();
    }
  });
});
