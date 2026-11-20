import { EventEmitter } from "node:events";
import { connect, type Socket } from "node:net";
import { Client } from "pg";
import { beforeAll, describe, expect, it, vi } from "vitest";
import { ConnectionManager } from "../../src/extension/connectionManager";
import { PostgresDriver } from "../../src/extension/dbDrivers/postgres";
import { resolvePostgresPort } from "../../src/extension/services/postgresPort";
import {
  buildSshFingerprintSha256,
  createSshRuntime,
} from "../../src/extension/services/sshRuntime";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";
import {
  createExtensionContextStub,
  FakeConnectionManagerStore,
} from "../support/fakeConnectionManagerStore";
import { createMongoTlsCertificates } from "../support/mongoTlsFixture";
import { startSqlTlsFixture } from "../support/sqlTlsFixture";

vi.mock(
  "vscode",
  async () =>
    (await import("../support/mockVscode")).createMockVscodeModule().module,
);

describe("PostgreSQL effective port", () => {
  it.each([
    [undefined, 5432],
    ["", 5432],
    ["5544", 5544],
    [" \t+05544tail", 5544],
    ["65535", 65535],
  ] as const)("matches node-postgres with PGPORT %j", (envPort, expected) => {
    vi.stubEnv("PGPORT", envPort);
    expect(resolvePostgresPort()).toBe(expected);
    expect(resolvePostgresPort()).toBe(new Client().port);
    expect(resolvePostgresPort(5433)).toBe(new Client({ port: 5433 }).port);
  });

  it.each([
    "abc",
    " ",
    "0",
    "-1",
    "65536",
  ])("rejects unusable PGPORT %j unless explicitly overridden", (envPort) => {
    vi.stubEnv("PGPORT", envPort);
    expect(() => resolvePostgresPort()).toThrow(
      /PGPORT.*explicit database port/,
    );
    expect(resolvePostgresPort(5433)).toBe(5433);
  });
});

describe("PostgreSQL port routing over TCP and TLS", () => {
  let certificates: Awaited<ReturnType<typeof createMongoTlsCertificates>>;
  beforeAll(async () => {
    certificates = await createMongoTlsCertificates();
  });

  const baseConfig = {
    id: "postgres-port",
    name: "PostgreSQL port",
    type: "pg",
    host: "127.0.0.1",
    database: "test",
    username: "test",
    useSecretStorage: false,
  } satisfies ConnectionConfig;

  function startFixture() {
    return startSqlTlsFixture("pg", {
      cert: certificates.serverCert,
      key: certificates.serverKey,
    });
  }

  function tlsConfig(serverNameOverride?: string) {
    return {
      mode: serverNameOverride ? "requireVerifyFull" : "requireVerifyCa",
      caFilePath: certificates.caFilePath,
      serverNameOverride,
    } as const;
  }

  it.each([
    ["ordinary TLS", undefined, false],
    ["TLS name override", "db.internal", false],
    ["TLS name override with an explicit port", "db.internal", true],
  ] as const)("reaches the selected listener: %s", async (_, identity, explicit) => {
    const fixture = await startFixture();
    vi.stubEnv("PGPORT", explicit ? "bad-port" : String(fixture.port));
    const driver = new PostgresDriver({
      ...baseConfig,
      port: explicit ? fixture.port : undefined,
      tls: tlsConfig(identity),
    });
    try {
      await driver.connect();
      expect(fixture.authentications).toBe(1);
      expect(fixture.servernames).toEqual([identity ?? false]);
    } finally {
      await driver.disconnect();
      await fixture.close();
    }
  });

  it.each([
    undefined,
    "db.internal",
  ])("refuses a malformed inherited port before connecting (name override %s)", async (identity) => {
    vi.stubEnv("PGPORT", "bad-port");
    const driver = new PostgresDriver({
      ...baseConfig,
      tls: identity ? tlsConfig(identity) : { mode: "disabled" },
    });
    try {
      await expect(driver.connect()).rejects.toThrow(/PGPORT/);
    } finally {
      await driver.disconnect();
    }
  });

  it.each([
    false,
    true,
  ])("uses the same remote port for SSH Test and Connect, with a distinct local socket (explicit=%s)", async (explicit) => {
    const fixture = await startFixture();
    vi.stubEnv("PGPORT", explicit ? "bad-port" : String(fixture.port));
    const key = Buffer.from("synthetic SSH host key");
    const config: ConnectionConfig = {
      ...baseConfig,
      host: "db.internal",
      port: explicit ? fixture.port : undefined,
      tls: { mode: "requireVerifyFull", caFilePath: certificates.caFilePath },
      ssh: {
        host: "bastion.internal",
        username: "tunnel",
        authMethod: "password",
        password: "synthetic",
        hostVerificationMode: "manual",
        hostFingerprintSha256: buildSshFingerprintSha256(key),
      },
    };
    const destinations: { host: string; port: number }[] = [];
    const localPorts: number[] = [];
    class SyntheticSshClient extends EventEmitter {
      connect(options: { hostVerifier: (key: Buffer) => boolean }) {
        queueMicrotask(() => {
          if (options.hostVerifier(key)) this.emit("ready");
        });
      }
      end() {
        this.emit("close");
      }
      forwardOut(
        _sourceHost: string,
        _sourcePort: number,
        host: string,
        port: number,
        callback: (error: Error | undefined, stream?: Socket) => void,
      ) {
        destinations.push({ host, port });
        if (host !== config.host || port !== fixture.port) {
          callback(new Error("Unexpected forward target"));
          return;
        }
        const socket = connect(port, "127.0.0.1");
        const onError = (error: Error) => callback(error);
        socket.once("error", onError);
        socket.once("connect", () => {
          socket.removeListener("error", onError);
          callback(undefined, socket);
        });
      }
    }
    const store = new FakeConnectionManagerStore();
    store.setConnections([config]);
    const manager = new ConnectionManager(
      createExtensionContextStub() as never,
      store,
      {
        createSshRuntime: async (settings, request, options) => {
          const runtime = await createSshRuntime(settings, request, {
            ...options,
            loadSsh2: async () => ({ Client: SyntheticSshClient }),
          });
          if (runtime.transport.kind === "tcpForward") {
            localPorts.push(runtime.transport.localPort);
          }
          return runtime;
        },
      },
    );
    try {
      expect(await manager.testConnection(config)).toEqual({ success: true });
      await manager.connectTo(config.id);
      expect(manager.isConnected(config.id)).toBe(true);
      expect(destinations).toEqual([
        { host: config.host, port: fixture.port },
        { host: config.host, port: fixture.port },
      ]);
      expect(localPorts).toHaveLength(2);
      expect(localPorts).not.toContain(fixture.port);
      expect(fixture.servernames).toEqual(["db.internal", "db.internal"]);
      expect(manager.getConnection(config.id)?.port).toBe(config.port);
      if (!explicit) {
        await manager.disconnectFrom(config.id);
        vi.stubEnv("PGPORT", "bad-port");
        expect(await manager.testConnection(config)).toMatchObject({
          success: false,
          error: expect.stringContaining("PGPORT"),
        });
        await expect(manager.connectTo(config.id)).rejects.toThrow(/PGPORT/);
        expect(localPorts).toHaveLength(2);
      }
    } finally {
      await manager.dispose();
      await fixture.close();
    }
  });
});
