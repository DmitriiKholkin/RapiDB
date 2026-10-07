import { EventEmitter } from "node:events";
import {
  mkdir,
  mkdtemp,
  readFile,
  rm,
  symlink,
  writeFile,
} from "node:fs/promises";
import * as https from "node:https";
import * as net from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { Duplex } from "node:stream";
import * as tls from "node:tls";
import type { Client as ElasticsearchClient } from "@elastic/elasticsearch";
import { HttpConnection } from "@elastic/transport";
import { ConnectionPool } from "mssql";
import * as mysql from "mysql2/promise";
import { Client as PostgresClient } from "pg";
import { Connection as TediousConnection } from "tedious";
import { beforeAll, describe, expect, it, vi } from "vitest";
import { ElasticsearchDriver } from "../../src/extension/dbDrivers/elasticsearch";
import { MSSQLDriver } from "../../src/extension/dbDrivers/mssql";
import { MySQLDriver } from "../../src/extension/dbDrivers/mysql";
import { PostgresDriver } from "../../src/extension/dbDrivers/postgres";
import { RedisDriver } from "../../src/extension/dbDrivers/redis";
import type { DriverConnectionConfig } from "../../src/extension/driverRuntimeConfig";
import {
  MAX_TLS_FILE_SIZE_BYTES,
  resolveConnectionTlsSettings,
} from "../../src/extension/services/connectionTls";
import {
  buildSshFingerprintSha256,
  createSshRuntime,
} from "../../src/extension/services/sshRuntime";
import {
  CONNECTION_TLS_MODES,
  type ConnectionConfig,
  type ConnectionTlsMode,
  getConnectionTlsSupport,
} from "../../src/shared/connectionConfig";
import { createMongoTlsCertificates } from "../support/mongoTlsFixture";
import { startRedisTlsFixture } from "../support/redisTlsFixture";
import { startSqlTlsFixture } from "../support/sqlTlsFixture";

vi.mock("vscode", async () => import("../support/mockVscode"));

let certificates: Awaited<ReturnType<typeof createMongoTlsCertificates>>;
let unrelatedCertificates: Awaited<
  ReturnType<typeof createMongoTlsCertificates>
>;
beforeAll(async () => {
  certificates = await createMongoTlsCertificates();
  unrelatedCertificates = await createMongoTlsCertificates();
});

const timeoutSettings = () => ({
  connectionTimeoutSeconds: 1,
  dbOperationTimeoutSeconds: 1,
  connectionTimeoutMs: 1000,
  dbOperationTimeoutMs: 1000,
});

describe("shared TLS modes", () => {
  it("advertises only the supported driver modes", () => {
    for (const type of [
      "mysql",
      "pg",
      "mongodb",
      "redis",
      "elasticsearch",
    ] as const) {
      expect(getConnectionTlsSupport(type)?.modes).toEqual(
        CONNECTION_TLS_MODES,
      );
    }
    expect(getConnectionTlsSupport("mssql")?.modes).toEqual([
      "disabled",
      "requireVerifyFull",
      "requireTrustServerCertificate",
    ]);
    for (const type of ["oracle", "sqlite", "dynamodb"] as const) {
      expect(getConnectionTlsSupport(type)).toBeUndefined();
    }
  });
  it("leaves absent/disabled TLS disabled without reading files", async () => {
    const config: ConnectionConfig = { id: "tls", name: "TLS", type: "mysql" };
    await expect(resolveConnectionTlsSettings(config)).resolves.toBeUndefined();
    expect(
      await resolveConnectionTlsSettings({
        ...config,
        tls: { mode: "disabled", caFilePath: "/does/not/exist" },
      }),
    ).toBeUndefined();
  });

  it.each(
    CONNECTION_TLS_MODES.filter((mode) => mode !== "disabled"),
  )("%s preserves the intended chain/name policy", async (mode) => {
    const settings = await resolveConnectionTlsSettings({
      id: "tls",
      name: "TLS",
      type: "mysql",
      tls: { mode },
    });
    expect(settings?.rejectUnauthorized).toBe(
      mode !== "requireTrustServerCertificate",
    );
    const skip =
      mode === "requireVerifyCa" || mode === "requireTrustServerCertificate";
    expect(settings?.skipHostnameVerification).toBe(skip);
    expect(typeof settings?.checkServerIdentity).toBe("function");
    if (!skip)
      expect(settings?.checkServerIdentity).toBe(tls.checkServerIdentity);
  });

  it("reads regular CA, client certificate, and key files byte-for-byte", async () => {
    const settings = await resolveConnectionTlsSettings({
      id: "tls-files",
      name: "TLS files",
      type: "mysql",
      tls: {
        mode: "mutualTls",
        caFilePath: certificates.caFilePath,
        certFilePath: certificates.certFilePath,
        keyFilePath: certificates.keyFilePath,
      },
    });

    expect(settings?.ca).toEqual(certificates.ca);
    expect(settings?.cert).toEqual(await readFile(certificates.certFilePath));
    expect(settings?.key).toEqual(await readFile(certificates.keyFilePath));
  });

  it("rejects oversized TLS files without including their contents in errors", async () => {
    const directory = await mkdtemp(join(tmpdir(), "rapidb-tls-size-"));
    try {
      const filePath = join(directory, "large-ca.pem");
      const privateMarker = "do-not-disclose-this-private-material";
      const contents = Buffer.alloc(MAX_TLS_FILE_SIZE_BYTES + 1, 0x61);
      Buffer.from(privateMarker).copy(contents);
      await writeFile(filePath, contents);

      let message = "";
      try {
        await resolveConnectionTlsSettings({
          id: "tls-large",
          name: "TLS large file",
          type: "mysql",
          tls: { mode: "requireVerifyCa", caFilePath: filePath },
        });
      } catch (error) {
        message = error instanceof Error ? error.message : String(error);
      }

      expect(message).toMatch(/1 MiB size limit/);
      expect(message).not.toContain(privateMarker);
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  });

  it("rejects missing and non-regular TLS paths descriptively", async () => {
    const directory = await mkdtemp(join(tmpdir(), "rapidb-tls-invalid-"));
    try {
      const folder = join(directory, "certificate-folder");
      await mkdir(folder);

      await expect(
        resolveConnectionTlsSettings({
          id: "tls-missing",
          name: "TLS missing file",
          type: "mysql",
          tls: {
            mode: "requireVerifyCa",
            caFilePath: join(directory, "missing.pem"),
          },
        }),
      ).rejects.toThrow(/CA certificate file "missing\.pem" was not found/);
      await expect(
        resolveConnectionTlsSettings({
          id: "tls-directory",
          name: "TLS directory",
          type: "mysql",
          tls: { mode: "requireVerifyCa", caFilePath: folder },
        }),
      ).rejects.toThrow(/must refer to a regular file/);
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  });

  it.skipIf(process.platform === "win32")(
    "continues to support symlinks to regular TLS files",
    async () => {
      const directory = await mkdtemp(join(tmpdir(), "rapidb-tls-link-"));
      try {
        const target = join(directory, "managed-ca.pem");
        const link = join(directory, "ca.pem");
        const contents = Buffer.from("certificate through a symlink");
        await writeFile(target, contents);
        await symlink(target, link);

        const settings = await resolveConnectionTlsSettings({
          id: "tls-symlink",
          name: "TLS symlink",
          type: "mysql",
          tls: { mode: "requireVerifyCa", caFilePath: link },
        });
        expect(settings?.ca).toEqual(contents);
      } finally {
        await rm(directory, { recursive: true, force: true });
      }
    },
  );
});

describe.each([
  "mysql",
  "pg",
] as const)("%s native TLS verification", (engine) => {
  async function probe({
    mode = "requireVerifyFull",
    identity = "db.internal",
    trusted = true,
    forwarded = false,
    ip = false,
    override = true,
    clientTrusted = true,
    serverRequiresClient = false,
  }: {
    mode?: ConnectionTlsMode;
    identity?: string;
    trusted?: boolean;
    forwarded?: boolean;
    ip?: boolean;
    override?: boolean;
    clientTrusted?: boolean;
    serverRequiresClient?: boolean;
  } = {}) {
    const fixture = await startSqlTlsFixture(engine, {
      cert: ip ? certificates.ipCert : certificates.serverCert,
      key: ip ? certificates.ipKey : certificates.serverKey,
      ca: certificates.ca,
      requestCert: mode === "mutualTls" || serverRequiresClient,
      rejectUnauthorized: true,
    });
    const config: DriverConnectionConfig = {
      id: "tls",
      name: "TLS",
      type: engine,
      // A DNS physical host catches pg's ssl.servername overwrite.
      host: "localhost",
      port: fixture.port,
      username: "test",
      tls: {
        mode,
        ...(trusted ? { caFilePath: certificates.caFilePath } : {}),
        ...(override ? { serverNameOverride: ` ${identity} ` } : {}),
        ...(mode === "mutualTls"
          ? {
              certFilePath: (clientTrusted
                ? certificates
                : unrelatedCertificates
              ).certFilePath,
              keyFilePath: (clientTrusted
                ? certificates
                : unrelatedCertificates
              ).keyFilePath,
            }
          : {}),
      },
      ...(forwarded
        ? {
            runtimeOverrides: {
              tlsServername: override ? "wrong.original.internal" : identity,
              transport: {
                kind: "tcpForward",
                localHost: "127.0.0.1",
                localPort: fixture.port,
                remoteHost: identity,
                remotePort: engine === "mysql" ? 3306 : 5432,
              },
            },
          }
        : {}),
    };
    const driver =
      engine === "mysql"
        ? new MySQLDriver(config, timeoutSettings)
        : new PostgresDriver(config, timeoutSettings);
    let error: unknown;
    try {
      await driver.connect();
    } catch (caught) {
      error = caught;
    } finally {
      await driver.disconnect();
      await fixture.close();
    }
    return { error, fixture };
  }

  it.each([
    false,
    true,
  ])("verifies the expected name (SSH=%s)", async (forwarded) => {
    const { error, fixture } = await probe({ forwarded, override: !forwarded });
    expect(error).toBeUndefined();
    expect(fixture.authentications).toBe(1);
    expect(fixture.servernames).toContain("db.internal");
  });

  it("explicit identity takes precedence over an SSH original host", async () => {
    const { error, fixture } = await probe({ forwarded: true });
    expect(error).toBeUndefined();
    expect(fixture.servernames).toContain("db.internal");
  });

  it.each([
    false,
    true,
  ])("rejects a trusted wrong name (SSH=%s)", async (forwarded) => {
    const { error, fixture } = await probe({
      identity: "wrong.internal",
      forwarded,
      override: !forwarded,
    });
    expect(error).toBeInstanceOf(Error);
    expect(String(error)).toMatch(/not.*cert|does not match/i);
    expect(fixture.authentications).toBe(0);
  });

  it("does not turn off default full hostname verification", async () => {
    const { error } = await probe({ override: false });
    expect(String(error)).toMatch(/not.*cert|does not match/i);
  });

  it.each([
    false,
    true,
  ])("CA-only accepts a trusted wrong name (SSH=%s)", async (forwarded) => {
    const { error } = await probe({
      mode: "requireVerifyCa",
      identity: "wrong.internal",
      forwarded,
    });
    expect(error).toBeUndefined();
  });

  it.each([
    "requireVerifyCa",
    "requireVerifyFull",
    "mutualTls",
  ] as const)("%s rejects an untrusted chain", async (mode) => {
    const { error, fixture } = await probe({ mode, trusted: false });
    expect(String(error)).toMatch(/certificate|issuer|self.signed/i);
    expect(fixture.authentications).toBe(0);
  });

  it.each([
    false,
    true,
  ])("explicit trust accepts an untrusted chain and wrong name (SSH=%s)", async (forwarded) => {
    const { error } = await probe({
      mode: "requireTrustServerCertificate",
      identity: "wrong.internal",
      trusted: false,
      forwarded,
    });
    expect(error).toBeUndefined();
  });

  it("mutual TLS retains full server verification", async () => {
    const trusted = await probe({ mode: "mutualTls" });
    expect(trusted.error).toBeUndefined();
    expect(trusted.fixture.authentications).toBe(1);
    expect(
      (await probe({ mode: "mutualTls", identity: "wrong.internal" })).error,
    ).toBeInstanceOf(Error);
  });

  it("does not acknowledge authentication for an unrelated-CA client certificate", async () => {
    const { error, fixture } = await probe({
      mode: "mutualTls",
      clientTrusted: false,
    });
    expect(error).toBeInstanceOf(Error);
    expect(fixture.authentications).toBe(0);
  });

  it("does not acknowledge authentication without a required client certificate", async () => {
    const { error, fixture } = await probe({ serverRequiresClient: true });
    expect(error).toBeInstanceOf(Error);
    expect(fixture.authentications).toBe(0);
  });

  it.each([
    ["192.0.2.12", "192.0.2.13", false],
    ["192.0.2.12", "192.0.2.13", true],
    ["2001:db8::12", "2001:db8::13", false],
    ["2001:db8::12", "2001:db8::13", true],
    ["[2001:db8::12]", "[2001:db8::13]", false],
    ["[2001:db8::12]", "[2001:db8::13]", true],
  ] as const)("verifies IP SAN %s, rejecting %s without IP SNI (SSH=%s)", async (identity, wrongIdentity, forwarded) => {
    const positive = await probe({
      ip: true,
      identity,
      forwarded,
      override: !forwarded,
    });
    expect(positive.error).toBeUndefined();
    expect(positive.fixture.servernames).toEqual([false]);
    expect(
      (
        await probe({
          ip: true,
          identity: wrongIdentity,
          forwarded,
          override: !forwarded,
        })
      ).error,
    ).toBeInstanceOf(Error);
  });
});

describe("mysql2 actual supported TLS properties", () => {
  it("ignores ssl.servername/checkServerIdentity without verifyIdentity", async () => {
    const fixture = await startSqlTlsFixture("mysql", {
      cert: certificates.serverCert,
      key: certificates.serverKey,
    });
    const ignoredChecker = vi.fn(() => new Error("Must not be ignored"));
    const ssl = {
      ca: certificates.ca,
      rejectUnauthorized: true,
      servername: "db.internal",
      checkServerIdentity: ignoredChecker,
    };
    try {
      const connection = await mysql.createConnection({
        host: "localhost",
        port: fixture.port,
        user: "test",
        ssl,
        connectTimeout: 1000,
      });
      await connection.end();
      expect(ignoredChecker).not.toHaveBeenCalled();
      expect(fixture.servernames).toContain("localhost");
      expect(fixture.authentications).toBe(1);
    } finally {
      await fixture.close();
    }
  });
});

describe("pg native servername handling", () => {
  it("overwrites a custom ssl.servername when the connection host is DNS", async () => {
    const fixture = await startSqlTlsFixture("pg", {
      cert: certificates.serverCert,
      key: certificates.serverKey,
    });
    const client = new PostgresClient({
      host: "localhost",
      port: fixture.port,
      user: "test",
      connectionTimeoutMillis: 1000,
      ssl: {
        ca: certificates.ca,
        servername: "db.internal",
        checkServerIdentity: tls.checkServerIdentity,
      },
    });
    try {
      await expect(client.connect()).rejects.toThrow(/localhost.*not.*cert/i);
      expect(fixture.authentications).toBe(0);
    } finally {
      await client.end();
      await fixture.close();
    }
  });
});

describe("Redis native TLS modes and SSH identity", () => {
  it.each([
    ["requireVerifyFull", "db.internal", true, true],
    ["requireVerifyFull", "wrong.internal", true, false],
    ["requireVerifyCa", "wrong.internal", true, true],
    ["requireVerifyCa", "db.internal", false, false],
    ["requireTrustServerCertificate", "wrong.internal", false, true],
    ["mutualTls", "db.internal", true, true],
  ] as const)("%s / %s / trusted=%s", async (mode, identity, trusted, success) => {
    vi.spyOn(console, "error").mockImplementation(() => undefined);
    const fixture = await startRedisTlsFixture({
      cert: certificates.serverCert,
      key: certificates.serverKey,
      ca: certificates.ca,
      requestCert: mode === "mutualTls",
      rejectUnauthorized: true,
    });
    const config: DriverConnectionConfig = {
      id: "redis-tls",
      name: "Redis TLS",
      type: "redis",
      connectionUri: "redis://db.internal:6379/0",
      tls: {
        mode,
        ...(trusted ? { caFilePath: certificates.caFilePath } : {}),
        ...(mode === "mutualTls"
          ? {
              certFilePath: certificates.certFilePath,
              keyFilePath: certificates.keyFilePath,
            }
          : {}),
      },
      runtimeOverrides: {
        tlsServername: identity,
        transport: {
          kind: "tcpForward",
          localHost: "127.0.0.1",
          localPort: fixture.port,
          remoteHost: identity,
          remotePort: 6379,
        },
      },
    };
    const driver = new RedisDriver(config, timeoutSettings);
    try {
      if (success) {
        await driver.connect();
        expect(fixture.commands).toBeGreaterThan(0);
        expect(fixture.servernames).toContain(identity);
      } else {
        await expect(driver.connect()).rejects.toBeInstanceOf(Error);
        expect(fixture.commands).toBe(0);
      }
    } finally {
      await driver.disconnect();
      await fixture.close();
    }
  });
});

describe("MSSQL supported modes reach native tedious configuration", () => {
  it.each([
    "disabled",
    "requireTrustServerCertificate",
    "requireVerifyFull",
  ] as const)("%s keeps encryption, trust and SSH server identity distinct", (mode) => {
    const config: DriverConnectionConfig = {
      id: "mssql-tls",
      name: "MSSQL TLS",
      type: "mssql",
      host: "db.internal",
      username: "test",
      password: "test-secret",
      tls: { mode, serverNameOverride: "override.internal" },
      runtimeOverrides: {
        mssqlServerName: "db.internal",
        transport: {
          kind: "tcpForward",
          localHost: "127.0.0.1",
          localPort: 11433,
          remoteHost: "db.internal",
          remotePort: 1433,
        },
      },
    };
    const driver = new MSSQLDriver(config, timeoutSettings);
    const poolConfig = (
      driver as unknown as {
        poolConfig(): ConstructorParameters<typeof ConnectionPool>[0];
      }
    ).poolConfig();
    const pool = new ConnectionPool(poolConfig);
    const nativeConfig = (
      pool as unknown as {
        _config(): ConstructorParameters<typeof TediousConnection>[0];
      }
    )._config();
    const native = new TediousConnection(nativeConfig);
    expect(native.config.server).toBe("127.0.0.1");
    expect(native.config.options.port).toBe(11433);
    expect(native.config.options.encrypt).toBe(mode !== "disabled");
    expect(native.config.options.trustServerCertificate).toBe(
      mode === "requireTrustServerCertificate",
    );
    expect(native.config.options.serverName).toBe(
      mode === "disabled" ? undefined : "override.internal",
    );
    native.close();
  });
});

describe("Elasticsearch Cloud ID / real SSH HTTPS agent TLS", () => {
  async function fixture() {
    const servernames: Array<string | false> = [];
    const drivers = new Set<ElasticsearchDriver>();
    const server = https.createServer(
      { cert: certificates.serverCert, key: certificates.serverKey },
      (_request, response) => {
        response.setHeader("x-elastic-product", "Elasticsearch");
        response.setHeader("content-type", "application/json");
        response.setHeader("content-length", "2");
        response.end("{}");
      },
    );
    server.keepAliveTimeout = 60000;
    const sockets = new Set<net.Socket>();
    server.on("connection", (socket) => {
      sockets.add(socket);
      socket.on("close", () => sockets.delete(socket));
    });
    server.on("secureConnection", (socket) => {
      servernames.push(
        (socket as tls.TLSSocket & { servername: string | false }).servername,
      );
    });
    server.on("tlsClientError", () => undefined);
    await new Promise<void>((resolve) =>
      server.listen(0, "127.0.0.1", resolve),
    );
    const address = server.address();
    if (!address || typeof address === "string") throw new Error("No TLS port");
    const destinations: string[] = [];
    const key = Buffer.from("test-host-key");
    // Only SSH transport negotiation is fake; its forwarding agents and the
    // Elasticsearch package's HttpConnection/request path are unmodified.
    const runtime = await createSshRuntime(
      {
        host: "bastion.internal",
        port: 22,
        username: "test",
        hostVerificationMode: "manual",
        fingerprintSha256: buildSshFingerprintSha256(key),
        auth: { kind: "password", password: "test-secret" },
      },
      { kind: "httpAgent" },
      {
        loadSsh2: async () => ({
          Client: class extends EventEmitter {
            connect(options: { hostVerifier: (key: Buffer) => boolean }) {
              queueMicrotask(() => {
                if (options.hostVerifier(key)) this.emit("ready");
              });
            }
            end() {
              this.emit("close");
            }
            forwardOut(
              _src: string,
              _port: number,
              host: string,
              port: number,
              callback: (error: Error | undefined, stream?: Duplex) => void,
            ) {
              destinations.push(`${host}:${port}`);
              const socket = net.connect(address.port, "127.0.0.1");
              socket.once("connect", () => callback(undefined, socket));
              socket.once("error", (error) => callback(error));
            }
          },
        }),
      },
    );
    function makeDriver(
      mode: ConnectionTlsMode,
      identity = "db.internal",
      trusted = true,
      cloud = true,
    ) {
      const config: DriverConnectionConfig = {
        id: "es-tls",
        name: "ES TLS",
        type: "elasticsearch",
        ...(cloud
          ? {
              cloudId: `test:${Buffer.from("internal$cloud").toString("base64")}`,
            }
          : { endpoint: "https://cloud.internal:443" }),
        tls: {
          mode,
          ...(trusted ? { caFilePath: certificates.caFilePath } : {}),
          serverNameOverride: identity,
        },
        runtimeOverrides: { transport: runtime.transport },
      };
      const driver = new ElasticsearchDriver(config);
      drivers.add(driver);
      return driver;
    }
    async function probe(
      mode: ConnectionTlsMode,
      identity = "db.internal",
      trusted = true,
      cloud = true,
    ) {
      const driver = makeDriver(mode, identity, trusted, cloud);
      let error: unknown;
      try {
        await driver.connect();
      } catch (caught) {
        error = caught;
      } finally {
        await driver.disconnect();
      }
      return error;
    }
    return {
      probe,
      makeDriver,
      servernames,
      destinations,
      runtime,
      async close() {
        await Promise.all([...drivers].map((driver) => driver.disconnect()));
        await runtime.dispose();
        for (const socket of sockets) socket.destroy();
        await new Promise<void>((resolve) => server.close(() => resolve()));
      },
    };
  }

  it("isolates live CA-only/full pools and closes only owned sockets", async () => {
    const f = await fixture();
    if (f.runtime.transport.kind !== "httpAgent")
      throw new Error("Expected agent");
    const runtimeAgent = f.runtime.transport.httpsAgent;
    const destroyRuntimeAgent = vi.spyOn(runtimeAgent, "destroy");
    const nativeClient = (driver: ElasticsearchDriver) =>
      Reflect.get(driver, "client") as ElasticsearchClient;
    const ownedAgent = (driver: ElasticsearchDriver) =>
      (nativeClient(driver).connectionPool.connections[0] as HttpConnection)
        .agent;
    const caOnly = f.makeDriver("requireVerifyCa", "wrong.internal");
    const wrongFull = f.makeDriver("requireVerifyFull", "wrong.internal");
    const validFull = f.makeDriver("requireVerifyFull", "db.internal");
    try {
      await caOnly.connect();
      const caAgent = ownedAgent(caOnly);
      expect(caAgent).not.toBe(runtimeAgent);
      await vi.waitFor(() =>
        expect(Object.values(caAgent?.freeSockets ?? {}).flat()).toHaveLength(
          1,
        ),
      );
      const weakSocket = Object.values(caAgent?.freeSockets ?? {}).flat()[0];
      expect(weakSocket?.destroyed).toBe(false);
      // A remains connected with a free socket and identical endpoint/CA/SNI.
      await expect(wrongFull.connect()).rejects.toThrow(
        /not.*cert|does not match/i,
      );
      expect(destroyRuntimeAgent).not.toHaveBeenCalled();
      expect(weakSocket?.destroyed).toBe(false);
      const forwardsBeforeReuse = f.destinations.length;
      await nativeClient(caOnly).ping();
      expect(f.destinations).toHaveLength(forwardsBeforeReuse);
      await validFull.connect();
      const fullAgent = ownedAgent(validFull);
      expect(fullAgent).not.toBe(caAgent);
      await vi.waitFor(() =>
        expect(Object.values(fullAgent?.freeSockets ?? {}).flat()).toHaveLength(
          1,
        ),
      );
      await nativeClient(validFull).ping();
      await vi.waitFor(() =>
        expect(Object.values(fullAgent?.freeSockets ?? {}).flat()).toHaveLength(
          1,
        ),
      );
      await nativeClient(validFull).ping();
      expect(
        f.servernames.filter((name) => name === "db.internal"),
      ).toHaveLength(1);
      await caOnly.disconnect();
      expect(destroyRuntimeAgent).not.toHaveBeenCalled();
      await vi.waitFor(() => expect(weakSocket?.destroyed).toBe(true));
      await nativeClient(validFull).ping();
      // Runtime disposal still terminates the independently pooled sockets.
      await f.runtime.dispose();
      await expect(
        nativeClient(validFull).ping({}, { maxRetries: 0 }),
      ).rejects.toThrow();
    } finally {
      await f.close();
    }
  }, 30000);

  it("native HttpConnection omits TLS settings when using an agent factory", async () => {
    const f = await fixture();
    if (f.runtime.transport.kind !== "httpAgent")
      throw new Error("Expected agent");
    const agent = f.runtime.transport.httpsAgent;
    const connection = new HttpConnection({
      url: new URL("https://cloud.internal"),
      agent: () => agent,
      tls: {
        ca: certificates.ca,
        servername: "db.internal",
        checkServerIdentity: tls.checkServerIdentity,
      },
    });
    try {
      const params = { method: "HEAD", path: "/" };
      const options = { requestId: 1, name: "tls-test", context: null };
      const request = connection.buildRequestObject(params, options);
      expect(request).not.toHaveProperty("ca");
      expect(request).not.toHaveProperty("servername");
      await expect(connection.request(params, options)).rejects.toThrow(
        /certificate/i,
      );
    } finally {
      await connection.close();
      await f.close();
    }
  });

  it.each([
    true,
    false,
  ])("preserves CA and explicit SNI (Cloud ID=%s)", async (cloud) => {
    const f = await fixture();
    try {
      expect(
        await f.probe("requireVerifyFull", "db.internal", true, cloud),
      ).toBeUndefined();
      expect(f.servernames).toContain("db.internal");
      expect(f.destinations).toContain("cloud.internal:443");
    } finally {
      await f.close();
    }
  });

  it("rejects wrong names/untrusted chains without contaminating a reused agent", async () => {
    const f = await fixture();
    const before = { ...f.runtime.transport };
    if (f.runtime.transport.kind !== "httpAgent")
      throw new Error("Expected agent");
    const agentOptions = { ...f.runtime.transport.httpsAgent.options };
    try {
      expect(
        await f.probe("requireTrustServerCertificate", "wrong.internal", false),
      ).toBeUndefined();
      expect(
        await f.probe("requireVerifyFull", "wrong.internal"),
      ).toBeInstanceOf(Error);
      expect(
        await f.probe("requireVerifyCa", "wrong.internal"),
      ).toBeUndefined();
      expect(
        await f.probe("requireVerifyCa", "db.internal", false),
      ).toBeInstanceOf(Error);
      expect(await f.probe("requireVerifyFull", "db.internal")).toBeUndefined();
      expect(f.runtime.transport).toEqual(before);
      expect(f.runtime.transport.httpsAgent.options).toEqual(agentOptions);
    } finally {
      await f.close();
    }
  }, 30000);
});
