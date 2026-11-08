import { EventEmitter } from "node:events";
import * as net from "node:net";
import { PassThrough } from "node:stream";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { ConnectionConfig } from "../../src/extension/connectionManagerModels";
import {
  buildSshFingerprintSha256,
  type ConnectionSshSettings,
  createSshRuntime,
  type SshRuntime,
} from "../../src/extension/services/sshRuntime";
import {
  createExtensionContextStub,
  FakeConnectionManagerStore,
} from "../support/fakeConnectionManagerStore";
import { MockEventEmitter } from "../support/mockVscode";

const driverInstances: FakeDriver[] = [];
const driverConnectImpls = new Map<string, () => Promise<void>>();

class FakeDriver {
  connectCalls = 0;
  disconnectCalls = 0;
  connected = false;

  constructor(readonly config: { id: string; port?: number }) {
    driverInstances.push(this);
  }

  async connect(): Promise<void> {
    this.connectCalls += 1;
    await driverConnectImpls.get(this.config.id)?.();
    this.connected = true;
  }

  async disconnect(): Promise<void> {
    this.disconnectCalls += 1;
    this.connected = false;
  }

  isConnected(): boolean {
    return this.connected;
  }

  getCapabilities() {
    return { tabularRead: "sql" };
  }
}

function driverFactory() {
  return {
    DynamoDBDriver: FakeDriver,
    ElasticsearchDriver: FakeDriver,
    MSSQLDriver: FakeDriver,
    MongoDBDriver: FakeDriver,
    MySQLDriver: FakeDriver,
    OracleDriver: FakeDriver,
    PostgresDriver: FakeDriver,
    RedisDriver: FakeDriver,
    SQLiteDriver: FakeDriver,
  };
}

vi.mock("vscode", () => ({
  EventEmitter: MockEventEmitter,
  ProgressLocation: { Window: 10 },
  window: {},
}));
vi.mock("../../src/extension/dbDrivers/dynamodb", driverFactory);
vi.mock("../../src/extension/dbDrivers/elasticsearch", driverFactory);
vi.mock("../../src/extension/dbDrivers/mssql", driverFactory);
vi.mock("../../src/extension/dbDrivers/mongodb", driverFactory);
vi.mock("../../src/extension/dbDrivers/mysql", driverFactory);
vi.mock("../../src/extension/dbDrivers/oracle", driverFactory);
vi.mock("../../src/extension/dbDrivers/postgres", driverFactory);
vi.mock("../../src/extension/dbDrivers/redis", driverFactory);
vi.mock("../../src/extension/dbDrivers/sqlite", driverFactory);

beforeEach(() => {
  driverInstances.splice(0);
  driverConnectImpls.clear();
});

type ConnectOptions = {
  hostVerifier: (hostKey: Buffer | string) => boolean;
  readyTimeout?: number;
  keepaliveInterval?: number;
  keepaliveCountMax?: number;
};

class FakeSshClient extends EventEmitter {
  connectOptions: ConnectOptions | null = null;
  ended = false;

  connect(options: ConnectOptions): void {
    this.connectOptions = options;
    queueMicrotask(() => {
      if (!options.hostVerifier(Buffer.from("ssh-host-key"))) {
        this.emit("error", new Error("Host verification failed"));
        return;
      }
      this.emit("ready");
    });
  }

  end(): void {
    if (this.ended) return;
    this.ended = true;
    this.emit("close");
  }

  forwardOut(
    _srcIP: string,
    _srcPort: number,
    _dstIP: string,
    _dstPort: number,
    callback: (error: Error | undefined, stream?: PassThrough) => void,
  ): void {
    callback(undefined, new PassThrough());
  }
}

const createdClients: FakeSshClient[] = [];

afterEach(() => {
  for (const client of createdClients.splice(0)) client.end();
});

const sshSettings: ConnectionSshSettings = {
  host: "bastion.internal",
  port: 22,
  username: "rapidb",
  hostVerificationMode: "manual",
  fingerprintSha256: buildSshFingerprintSha256(Buffer.from("ssh-host-key")),
  readyTimeoutMs: 7_000,
  auth: { kind: "password", password: "ssh-secret" },
};

function createRealRuntime(kind: "tcpForward" | "httpAgent" = "tcpForward") {
  return createSshRuntime(
    sshSettings,
    kind === "tcpForward"
      ? { kind, remoteHost: "db.internal", remotePort: 6379 }
      : { kind },
    {
      loadSsh2: async () => ({
        Client: class extends FakeSshClient {
          constructor() {
            super();
            createdClients.push(this);
          }
        },
      }),
    },
  );
}

function canConnect(port: number): Promise<boolean> {
  return new Promise((resolve) => {
    const socket = net.connect({ host: "127.0.0.1", port });
    socket.once("connect", () => {
      socket.destroy();
      resolve(true);
    });
    socket.once("error", () => resolve(false));
  });
}

function canBind(port: number): Promise<boolean> {
  return new Promise((resolve) => {
    const server = net.createServer();
    server.once("error", () => resolve(false));
    server.listen(port, "127.0.0.1", () => {
      server.close(() => resolve(true));
    });
  });
}

describe("SSH runtime close notification", () => {
  it.each([
    "tcpForward",
    "httpAgent",
  ] as const)("notifies %s listeners once when the SSH session drops, and not after dispose", async (kind) => {
    const runtime = await createRealRuntime(kind);
    const onClose = vi.fn();
    runtime.onDidClose?.(onClose);
    createdClients[0].emit("close");
    expect(onClose).toHaveBeenCalledOnce();
    expect(onClose.mock.calls[0][0]).toBeInstanceOf(Error);

    const late = vi.fn();
    runtime.onDidClose?.(late);
    await Promise.resolve();
    expect(late).toHaveBeenCalledOnce();
    await runtime.dispose();
    if (runtime.transport.kind === "tcpForward") {
      await vi.waitFor(async () =>
        expect(
          await canConnect(
            (runtime.transport as { localPort: number }).localPort,
          ),
        ).toBe(false),
      );
    }

    const second = await createRealRuntime(kind);
    const disposedListener = vi.fn();
    second.onDidClose?.(disposedListener);
    await second.dispose();
    createdClients[1].emit("close");
    expect(disposedListener).not.toHaveBeenCalled();
  });

  it("keeps the local port reserved after an unexpected close until dispose, without forwarding", async () => {
    const runtime = await createRealRuntime("tcpForward");
    const port = (runtime.transport as { localPort: number }).localPort;
    const forwardOut = vi.spyOn(createdClients[0], "forwardOut");
    try {
      createdClients[0].emit("close");
      expect(await canBind(port)).toBe(false);

      const socket = net.connect({ host: "127.0.0.1", port });
      socket.on("error", () => undefined);
      await new Promise((resolve) => socket.once("close", resolve));
      expect(forwardOut).not.toHaveBeenCalled();
    } finally {
      await runtime.dispose();
    }
    expect(await canBind(port)).toBe(true);
  });

  it("passes the connection timeout and keepalive settings to ssh2", async () => {
    const runtime = await createRealRuntime();
    try {
      expect(createdClients[0].connectOptions).toMatchObject({
        readyTimeout: 7_000,
        keepaliveInterval: expect.any(Number),
        keepaliveCountMax: expect.any(Number),
      });
      expect(
        createdClients[0].connectOptions?.keepaliveInterval,
      ).toBeGreaterThan(0);
      expect(
        createdClients[0].connectOptions?.keepaliveCountMax,
      ).toBeGreaterThan(0);
    } finally {
      await runtime.dispose();
    }
  });
});

function createSshRedisConfig(
  id: string,
  ssh: Partial<NonNullable<ConnectionConfig["ssh"]>> = {},
): ConnectionConfig {
  return {
    id,
    name: id,
    type: "redis",
    host: "redis.internal",
    port: 6379,
    password: "redis-secret",
    ssh: {
      host: "bastion.example.com",
      port: 22,
      username: "tunnel",
      authMethod: "password",
      hostVerificationMode: "manual",
      password: "ssh-secret",
      hostFingerprintSha256: "SHA256:AbCdEfGhIjKlMnOpQrStUvWxYz0123456789+/",
      ...ssh,
    },
  };
}

function createControllableRuntime(
  fingerprint = "SHA256:AbCdEfGhIjKlMnOpQrStUvWxYz0123456789+/",
) {
  const listeners = new Set<(error: Error) => void>();
  const runtime: SshRuntime & { close(): void; listenerCount(): number } = {
    transport: {
      kind: "tcpForward",
      localHost: "127.0.0.1",
      localPort: 16379,
      remoteHost: "redis.internal",
      remotePort: 6379,
    },
    verifiedFingerprintSha256: fingerprint,
    onDidClose(listener) {
      listeners.add(listener);
      return { dispose: () => listeners.delete(listener) };
    },
    dispose: vi.fn(async () => undefined),
    close() {
      for (const listener of [...listeners]) {
        listener(new Error("[RapiDB] SSH connection closed"));
      }
    },
    listenerCount: () => listeners.size,
  };
  return runtime;
}

async function createManager(
  config: ConnectionConfig,
  createRuntime: () => Promise<SshRuntime>,
) {
  const { ConnectionManager } = await import(
    "../../src/extension/connectionManager"
  );
  const store = new FakeConnectionManagerStore();
  store.setConnections([config]);
  const createSshRuntimeMock = vi.fn(createRuntime);
  const manager = new ConnectionManager(
    createExtensionContextStub() as never,
    store,
    { createSshRuntime: createSshRuntimeMock },
  );
  return { manager, store, createSshRuntime: createSshRuntimeMock };
}

describe("ConnectionManager SSH tunnel lifecycle", () => {
  it("disconnects the driver and emits events when the SSH tunnel closes unexpectedly", async () => {
    const config = createSshRedisConfig("tunnel-drop");
    const runtime = createControllableRuntime();
    const { manager } = await createManager(config, async () => runtime);
    try {
      await manager.connectTo(config.id);
      const driver = driverInstances[0];
      expect(manager.isConnected(config.id)).toBe(true);
      const disconnected = vi.fn();
      const changed = vi.fn();
      manager.onDidDisconnect(disconnected);
      manager.onDidChangeConnections(changed);

      runtime.close();

      await vi.waitFor(() =>
        expect(disconnected).toHaveBeenCalledWith(config.id),
      );
      await vi.waitFor(() => expect(changed).toHaveBeenCalled());
      expect(driver.disconnectCalls).toBe(1);
      expect(manager.getDriver(config.id)).toBeUndefined();
      expect(manager.isConnected(config.id)).toBe(false);
      expect(runtime.dispose).toHaveBeenCalledOnce();
    } finally {
      await manager.dispose();
    }
  });

  it("disconnects when the tunnel already closed before the session was registered", async () => {
    const config = createSshRedisConfig("tunnel-drop-during-connect");
    const runtime = createControllableRuntime();
    let closed = false;
    runtime.onDidClose = (listener) => {
      if (closed) queueMicrotask(() => listener(new Error("closed")));
      return { dispose() {} };
    };
    driverConnectImpls.set(config.id, async () => {
      closed = true;
    });
    const { manager } = await createManager(config, async () => runtime);
    try {
      await manager.connectTo(config.id);
      await vi.waitFor(() =>
        expect(manager.getDriver(config.id)).toBeUndefined(),
      );
      expect(driverInstances[0].disconnectCalls).toBe(1);
      await vi.waitFor(() => expect(runtime.dispose).toHaveBeenCalledOnce());
    } finally {
      await manager.dispose();
    }
  });

  it("aborts a pending database connect when the SSH tunnel closes", async () => {
    const config = createSshRedisConfig("tunnel-drop-while-connecting");
    const runtime = createControllableRuntime();
    let connectStarted!: () => void;
    const started = new Promise<void>((resolve) => {
      connectStarted = resolve;
    });
    driverConnectImpls.set(config.id, () => {
      connectStarted();
      return new Promise<void>(() => undefined);
    });
    const { manager } = await createManager(config, async () => runtime);
    try {
      const attempt = manager.connectTo(config.id, "explicit");
      await started;

      runtime.close();

      await expect(attempt).rejects.toThrow(/SSH tunnel closed/);
      expect(driverInstances).toHaveLength(1);
      expect(driverInstances[0].disconnectCalls).toBeGreaterThan(0);
      expect(runtime.dispose).toHaveBeenCalledOnce();
      expect(manager.getDriver(config.id)).toBeUndefined();
      expect(manager.isConnecting(config.id)).toBe(false);
      expect(runtime.listenerCount()).toBe(0);
    } finally {
      await manager.dispose();
    }
  });

  it("does not create a driver when the SSH tunnel closes before the database connect starts", async () => {
    const config = createSshRedisConfig("tunnel-drop-before-driver", {
      hostVerificationMode: "trustOnFirstUse",
      hostFingerprintSha256: undefined,
    });
    const runtime = createControllableRuntime();
    const { manager, store } = await createManager(config, async () => runtime);
    const mutateConnections = store.mutateConnections.bind(store);
    vi.spyOn(store, "mutateConnections").mockImplementation(async (mutate) => {
      runtime.close();
      return await mutateConnections(mutate);
    });
    try {
      await expect(manager.connectTo(config.id, "explicit")).rejects.toThrow(
        /SSH tunnel closed/,
      );
      expect(driverInstances).toHaveLength(0);
      expect(runtime.dispose).toHaveBeenCalledOnce();
    } finally {
      await manager.dispose();
    }
  });

  it("unsubscribes the connect-time close listener once the session is registered", async () => {
    const config = createSshRedisConfig("tunnel-listener-cleanup");
    const runtime = createControllableRuntime();
    const { manager } = await createManager(config, async () => runtime);
    try {
      await manager.connectTo(config.id);
      expect(manager.isConnected(config.id)).toBe(true);
      expect(runtime.listenerCount()).toBe(1);
    } finally {
      await manager.dispose();
    }
  });

  it("ignores a late close from a runtime that was intentionally replaced", async () => {
    const config = createSshRedisConfig("tunnel-replaced");
    const runtimes = [createControllableRuntime(), createControllableRuntime()];
    let index = 0;
    const { manager } = await createManager(
      config,
      async () => runtimes[index++],
    );
    try {
      await manager.connectTo(config.id);
      const [oldRuntime, newRuntime] = runtimes;
      await manager.disconnectFrom(config.id);
      await manager.connectTo(config.id);
      expect(manager.isConnected(config.id)).toBe(true);
      oldRuntime.close();
      await new Promise((resolve) => setTimeout(resolve, 0));
      expect(manager.isConnected(config.id)).toBe(true);
      expect(driverInstances[1].disconnectCalls).toBe(0);
      expect(newRuntime.dispose).not.toHaveBeenCalled();
    } finally {
      await manager.dispose();
    }
  });

  it("does not cancel a newer connect attempt when the old tunnel closes while it is replaced", async () => {
    const config = createSshRedisConfig("tunnel-close-during-reconnect");
    const oldListeners: Array<(error: Error) => void> = [];
    const oldRuntime = createControllableRuntime();
    oldRuntime.onDidClose = (listener) => {
      oldListeners.push(listener);
      return { dispose() {} };
    };
    const newRuntime = createControllableRuntime();
    const runtimes = [oldRuntime, newRuntime];
    let index = 0;
    const { manager } = await createManager(
      config,
      async () => runtimes[index++],
    );
    try {
      await manager.connectTo(config.id);
      driverInstances[0].connected = false;
      const attempt = manager.beginConnect(config.id, "explicit");
      expect(attempt.isNew).toBe(true);
      for (const listener of oldListeners)
        listener(new Error("[RapiDB] SSH connection closed"));
      await attempt.promise;
      await new Promise((resolve) => setTimeout(resolve, 0));
      expect(manager.isConnected(config.id)).toBe(true);
      expect(driverInstances).toHaveLength(2);
      expect(driverInstances[1].disconnectCalls).toBe(0);
      expect(newRuntime.dispose).not.toHaveBeenCalled();
    } finally {
      await manager.dispose();
    }
  });

  it("passes rapidb.connectionTimeoutSeconds to the SSH handshake", async () => {
    const config = createSshRedisConfig("tunnel-timeout");
    const { manager, store, createSshRuntime } = await createManager(
      config,
      async () => createControllableRuntime(),
    );
    store.setTimeoutSettings({ connectionTimeoutSeconds: 42 });
    try {
      await manager.connectTo(config.id);
      expect(createSshRuntime).toHaveBeenCalledWith(
        expect.objectContaining({ readyTimeoutMs: 42_000 }),
        expect.anything(),
        expect.anything(),
      );
    } finally {
      await manager.dispose();
    }
  });
});

describe("SSH trust-on-first-use pinning", () => {
  it("pins the host key after the SSH handshake even when the database login fails", async () => {
    const fingerprint = "SHA256:FirstSeenBastionKey1234567890+/";
    const config = createSshRedisConfig("tofu-db-failure", {
      hostVerificationMode: "trustOnFirstUse",
      hostFingerprintSha256: undefined,
    });
    driverConnectImpls.set(config.id, async () => {
      throw new Error("WRONGPASS invalid username-password pair");
    });
    const { manager, store, createSshRuntime } = await createManager(
      config,
      async () => createControllableRuntime(fingerprint),
    );
    try {
      await expect(manager.connectTo(config.id, "explicit")).rejects.toThrow(
        /WRONGPASS/,
      );
      expect(store.getConnections()[0].ssh?.hostFingerprintSha256).toBe(
        fingerprint,
      );
      await expect(manager.connectTo(config.id, "explicit")).rejects.toThrow(
        /WRONGPASS/,
      );
      expect(createSshRuntime).toHaveBeenLastCalledWith(
        expect.objectContaining({
          hostVerificationMode: "trustOnFirstUse",
          fingerprintSha256: fingerprint,
        }),
        expect.anything(),
        expect.anything(),
      );
    } finally {
      await manager.dispose();
    }
  });
});
