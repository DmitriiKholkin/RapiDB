import { createHash } from "node:crypto";
import { EventEmitter } from "node:events";
import * as http from "node:http";
import * as net from "node:net";
import { Duplex, PassThrough } from "node:stream";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  buildSshFingerprintSha256,
  type ConnectionSshSettings,
  createSshRuntime,
} from "../../src/extension/services/sshRuntime";

type ConnectOptions = {
  hostVerifier: (hostKey: Buffer | string) => boolean;
  host: string;
  port: number;
  username: string;
  password?: string;
  privateKey?: string;
  passphrase?: string;
};

class FakeSshClient extends EventEmitter {
  connectOptions: ConnectOptions | null = null;
  forwardOutCalls: Array<{
    srcIP: string;
    srcPort: number;
    dstIP: string;
    dstPort: number;
  }> = [];
  ended = false;
  endCalls = 0;
  readonly presentedHostKey = Buffer.from("ssh-host-key");

  connect(options: ConnectOptions): void {
    this.connectOptions = options;
    queueMicrotask(() => {
      if (!options.hostVerifier(this.presentedHostKey)) {
        this.emit("error", new Error("Host verification failed"));
        return;
      }

      this.emit("ready");
    });
  }

  end(): void {
    this.endCalls += 1;
    this.ended = true;
    this.emit("close");
  }

  forwardOut(
    srcIP: string,
    srcPort: number,
    dstIP: string,
    dstPort: number,
    callback: (error: Error | undefined, stream?: Duplex) => void,
  ): void {
    this.forwardOutCalls.push({ srcIP, srcPort, dstIP, dstPort });
    const stream = new PassThrough();
    queueMicrotask(() => {
      stream.end();
    });
    callback(undefined, stream);
  }
}

const createdClients: FakeSshClient[] = [];

const sshSettings: ConnectionSshSettings = {
  host: "bastion.internal",
  port: 22,
  username: "rapidb",
  hostVerificationMode: "manual",
  fingerprintSha256: buildSshFingerprintSha256(Buffer.from("ssh-host-key")),
  auth: {
    kind: "password",
    password: "ssh-secret",
  },
};

async function createRuntime(
  ssh: ConnectionSshSettings = sshSettings,
  request:
    | {
        kind: "tcpForward";
        remoteHost: string;
        remotePort: number;
      }
    | {
        kind: "httpAgent";
      } = {
    kind: "tcpForward",
    remoteHost: "db.internal",
    remotePort: 5432,
  },
) {
  return createSshRuntime(ssh, request, {
    loadSsh2: async () => ({
      Client: class extends FakeSshClient {
        constructor() {
          super();
          createdClients.push(this);
        }
      },
    }),
  });
}

afterEach(async () => {
  await Promise.allSettled(
    createdClients.splice(0).map(async (client) => {
      if (!client.ended) {
        client.end();
      }
    }),
  );
});

describe("sshRuntime", () => {
  it("aborts a pending SSH handshake from an external signal", async () => {
    const controller = new AbortController();
    const pending = createSshRuntime(
      sshSettings,
      {
        kind: "tcpForward",
        remoteHost: "db.internal",
        remotePort: 5432,
      },
      {
        signal: controller.signal,
        loadSsh2: async () => ({
          Client: class extends FakeSshClient {
            constructor() {
              super();
              createdClients.push(this);
            }

            override connect(options: ConnectOptions): void {
              this.connectOptions = options;
            }
          },
        }),
      },
    );
    await vi.waitFor(() => expect(createdClients).toHaveLength(1));

    controller.abort();

    await expect(pending).rejects.toMatchObject({ name: "AbortError" });
    expect(createdClients[0]?.endCalls).toBe(1);
  });

  it("propagates transport failure to an active HTTP request", async () => {
    const runtime = await createRuntime(sshSettings, { kind: "httpAgent" });
    if (runtime.transport.kind !== "httpAgent")
      throw new Error("Expected agent");
    const client = createdClients[0];
    const channel = new Duplex({
      read() {},
      write(_chunk, _encoding, callback) {
        callback();
      },
    });
    vi.spyOn(client, "forwardOut").mockImplementation(
      (_a, _b, _c, _d, callback) => {
        callback(undefined, channel);
      },
    );
    const request = http.get("http://db.internal/", {
      agent: runtime.transport.httpAgent,
    });
    const error = new Promise<Error>((resolve) =>
      request.once("error", resolve),
    );
    try {
      await new Promise<void>((resolve) =>
        request.once("socket", () => resolve()),
      );
      const failure = new Error("SSH transport reset");
      expect(() => client.emit("error", failure)).not.toThrow();
      await expect(error).resolves.toBe(failure);
      expect(channel.destroyed).toBe(true);
    } finally {
      request.destroy();
      await runtime.dispose();
    }
  }, 1500);

  it("rejects an unfinished HTTPS handshake when disposed", async () => {
    const runtime = await createRuntime(sshSettings, { kind: "httpAgent" });
    if (runtime.transport.kind !== "httpAgent")
      throw new Error("Expected agent");
    const channel = new Duplex({
      read() {},
      write(_chunk, _encoding, callback) {
        callback();
      },
    });
    vi.spyOn(createdClients[0], "forwardOut").mockImplementation(
      (_a, _b, _c, _d, callback) => {
        callback(undefined, channel);
      },
    );
    const agent = runtime.transport.httpsAgent as unknown as {
      connect(req: unknown, options: { host: string }): Promise<Duplex>;
    };
    const result = agent
      .connect({}, { host: "db.internal" })
      .catch((error: unknown) => error);
    try {
      await new Promise<void>((resolve) => setImmediate(resolve));
      await runtime.dispose();
      expect(await result).toMatchObject({
        message: "[RapiDB] SSH runtime disposed",
      });
      expect(channel.destroyed).toBe(true);
    } finally {
      await runtime.dispose();
    }
  }, 1500);

  it.each([
    "tcpForward",
    "httpAgent",
  ] as const)("rejects errors immediately after ready during %s setup", async (kind) => {
    const failure = new Error("transport failed immediately after ready");
    class FailingClient extends FakeSshClient {
      constructor() {
        super();
        createdClients.push(this);
      }

      override connect(options: ConnectOptions): void {
        options.hostVerifier(this.presentedHostKey);
        this.emit("ready");
        this.emit("error", failure);
      }
    }
    await expect(
      createSshRuntime(
        sshSettings,
        kind === "tcpForward"
          ? { kind, remoteHost: "db.internal", remotePort: 5432 }
          : { kind },
        { loadSsh2: async () => ({ Client: FailingClient }) },
      ),
    ).rejects.toBe(failure);
    const client = createdClients[0];
    expect(client.endCalls).toBe(1);
    expect(() => client.emit("error", new Error("late error"))).not.toThrow();
  });

  it.each([
    "dispose",
    "error",
    "close",
  ] as const)("closes open TCP sockets and channels on %s within a bounded time", async (action) => {
    const runtime = await createRuntime();
    if (runtime.transport.kind !== "tcpForward")
      throw new Error("Expected TCP");
    const client = createdClients[0];
    const upstream = new PassThrough();
    vi.spyOn(client, "forwardOut").mockImplementation(
      (_a, _b, _c, _d, callback) => {
        callback(undefined, upstream);
      },
    );
    const socket = net.createConnection(
      runtime.transport.localPort,
      runtime.transport.localHost,
    );
    socket.on("error", () => socket.destroy());
    try {
      await new Promise<void>((resolve) => socket.once("connect", resolve));
      await vi.waitFor(() => expect(client.forwardOut).toHaveBeenCalledOnce());
      const closed = new Promise<void>((resolve) =>
        socket.once("close", () => resolve()),
      );
      if (action === "error") {
        expect(() =>
          client.emit("error", new Error("transport lost")),
        ).not.toThrow();
      } else if (action === "close") {
        client.emit("close");
      }
      const disposal = runtime.dispose();
      expect(runtime.dispose()).toBe(disposal);
      await Promise.all([disposal, closed]);
      expect(upstream.destroyed).toBe(true);
      expect(client.endCalls).toBe(1);
      expect(() =>
        client.emit("error", new Error("late transport error")),
      ).not.toThrow();
    } finally {
      socket.destroy();
      upstream.destroy();
      await runtime.dispose();
    }
  }, 1500);

  it("destroys a TCP channel returned after repeated disposal", async () => {
    const runtime = await createRuntime();
    if (runtime.transport.kind !== "tcpForward")
      throw new Error("Expected TCP");
    const client = createdClients[0];
    let finish:
      | ((error: Error | undefined, stream?: PassThrough) => void)
      | undefined;
    vi.spyOn(client, "forwardOut").mockImplementation(
      (_a, _b, _c, _d, callback) => {
        finish = callback;
      },
    );
    const socket = net.createConnection(
      runtime.transport.localPort,
      runtime.transport.localHost,
    );
    socket.on("error", () => socket.destroy());
    try {
      await vi.waitFor(() => expect(finish).toBeDefined());
      await Promise.all([runtime.dispose(), runtime.dispose()]);
      const late = new PassThrough();
      finish!(undefined, late);
      expect(late.destroyed).toBe(true);
      expect(() =>
        late.emit("error", new Error("late channel error")),
      ).not.toThrow();
      expect(client.endCalls).toBe(1);
    } finally {
      socket.destroy();
      await runtime.dispose();
    }
  }, 1500);

  it.each([
    false,
    true,
  ])("rejects pending HTTP forwarding on shutdown (error=%s) and destroys late channels", async (fail) => {
    const runtime = await createRuntime(sshSettings, { kind: "httpAgent" });
    if (runtime.transport.kind !== "httpAgent")
      throw new Error("Expected agent");
    const client = createdClients[0];
    let finish:
      | ((error: Error | undefined, stream?: PassThrough) => void)
      | undefined;
    vi.spyOn(client, "forwardOut").mockImplementation(
      (_a, _b, _c, _d, callback) => {
        finish = callback;
      },
    );
    const agent = runtime.transport.httpAgent as unknown as {
      connect(req: unknown, options: { host: string }): Promise<Duplex>;
    };
    const pending = agent.connect({}, { host: "db.internal" });
    const rejected = expect(pending).rejects.toThrow(
      fail ? "transport lost" : "disposed",
    );
    if (fail) client.emit("error", new Error("transport lost"));
    await runtime.dispose();
    await rejected;
    const late = new PassThrough();
    finish!(undefined, late);
    expect(late.destroyed).toBe(true);
    await runtime.dispose();
    expect(client.endCalls).toBe(1);
  });

  it("retains post-ready transport errors and rejects subsequent HTTP forwards", async () => {
    const runtime = await createRuntime(sshSettings, { kind: "httpAgent" });
    if (runtime.transport.kind !== "httpAgent")
      throw new Error("Expected agent");
    const client = createdClients[0];
    const failure = new Error("SSH keepalive timed out");
    expect(() => client.emit("error", failure)).not.toThrow();
    expect(client.ended).toBe(true);
    const agent = runtime.transport.httpAgent as unknown as {
      connect(req: unknown, options: { host: string }): Promise<Duplex>;
    };
    await expect(agent.connect({}, { host: "db.internal" })).rejects.toBe(
      failure,
    );
    expect(() => client.emit("error", new Error("late error"))).not.toThrow();
    await runtime.dispose();
  });

  it("propagates host verification failures without an unhandled error", async () => {
    await expect(
      createRuntime({ ...sshSettings, fingerprintSha256: "SHA256:wrong" }),
    ).rejects.toThrow("Host verification failed");
    expect(createdClients[0].ended).toBe(true);
  });

  it("verifies the exact SHA256 host fingerprint and forwards auth settings", async () => {
    const runtime = await createRuntime();
    const client = createdClients[0];

    expect(client?.connectOptions).toMatchObject({
      host: "bastion.internal",
      port: 22,
      username: "rapidb",
      password: "ssh-secret",
    });
    expect(
      client?.connectOptions?.hostVerifier(Buffer.from("ssh-host-key")),
    ).toBe(true);
    expect(
      client?.connectOptions?.hostVerifier(Buffer.from("different-host-key")),
    ).toBe(false);

    await runtime.dispose();
  });

  it("trusts the first presented fingerprint in TOFU mode and exposes it on the runtime", async () => {
    const runtime = await createRuntime({
      ...sshSettings,
      hostVerificationMode: "trustOnFirstUse",
      fingerprintSha256: undefined,
    });

    expect(runtime.verifiedFingerprintSha256).toBe(
      buildSshFingerprintSha256(Buffer.from("ssh-host-key")),
    );
    const verifier = createdClients[0]?.connectOptions?.hostVerifier;
    expect(verifier?.(Buffer.from("ssh-host-key"))).toBe(true);
    expect(verifier?.(Buffer.from("changed-host-key"))).toBe(false);

    await runtime.dispose();
  });

  it.each([
    "manual",
    "trustOnFirstUse",
  ] as const)("rejects a mismatched saved pin in %s mode", async (hostVerificationMode) => {
    await expect(
      createRuntime({
        ...sshSettings,
        hostVerificationMode,
        fingerprintSha256: "SHA256:wrong",
      }),
    ).rejects.toThrow("Host verification failed");
    expect(createdClients[0]?.ended).toBe(true);
    expect(createdClients[0]?.forwardOutCalls).toEqual([]);
  });

  it.each([
    undefined,
    "",
    "   ",
  ])("rejects manual verification without a fingerprint (%s)", async (fingerprintSha256) => {
    await expect(
      createRuntime({ ...sshSettings, fingerprintSha256 }),
    ).rejects.toThrow("Host verification failed");
    expect(createdClients[0]?.ended).toBe(true);
  });

  it("enforces a learned TOFU pin on a subsequent connection", async () => {
    const first = await createRuntime({
      ...sshSettings,
      hostVerificationMode: "trustOnFirstUse",
      fingerprintSha256: undefined,
    });
    const fingerprintSha256 = first.verifiedFingerprintSha256;
    await first.dispose();
    const second = await createRuntime({
      ...sshSettings,
      hostVerificationMode: "trustOnFirstUse",
      fingerprintSha256,
    });
    try {
      expect(second.verifiedFingerprintSha256).toBe(fingerprintSha256);
      const verifier = createdClients[1]?.connectOptions?.hostVerifier;
      expect(verifier?.(Buffer.from("ssh-host-key"))).toBe(true);
      expect(verifier?.(Buffer.from("changed-host-key"))).toBe(false);
    } finally {
      await second.dispose();
    }
  });

  it("creates HTTP-agent runtimes for private-key SSH auth and disposes the client", async () => {
    const privateKey =
      "-----BEGIN PRIVATE KEY-----\nabc\n-----END PRIVATE KEY-----";

    const runtime = await createRuntime(
      {
        ...sshSettings,
        auth: {
          kind: "privateKey",
          privateKey,
          passphrase: "key-passphrase",
        },
      },
      {
        kind: "httpAgent",
      },
    );
    const client = createdClients[0];

    expect(runtime.transport.kind).toBe("httpAgent");
    expect(client?.connectOptions).toMatchObject({
      host: "bastion.internal",
      port: 22,
      username: "rapidb",
      privateKey,
      passphrase: "key-passphrase",
    });
    expect(client?.connectOptions?.password).toBeUndefined();

    await runtime.dispose();

    expect(client?.ended).toBe(true);
  });

  it("binds TCP forwarding to 127.0.0.1 on an ephemeral port and forwards to the remote target", async () => {
    const runtime = await createRuntime();
    const client = createdClients[0];

    expect(runtime.transport.kind).toBe("tcpForward");
    if (runtime.transport.kind !== "tcpForward") {
      throw new Error("Expected tcpForward runtime");
    }

    const transport = runtime.transport;

    expect(transport.localHost).toBe("127.0.0.1");
    expect(transport.localPort).toBeGreaterThan(0);

    await new Promise<void>((resolve, reject) => {
      const socket = net.createConnection({
        host: transport.localHost,
        port: transport.localPort,
      });
      socket.once("connect", () => {
        socket.end();
      });
      socket.once("close", () => resolve());
      socket.once("error", reject);
    });

    expect(client?.forwardOutCalls).toContainEqual({
      srcIP: "127.0.0.1",
      srcPort: 0,
      dstIP: "db.internal",
      dstPort: 5432,
    });

    await runtime.dispose();
  });

  it("forwards HTTP-agent requests to the resolved remote host and port", async () => {
    const runtime = await createRuntime(sshSettings, {
      kind: "httpAgent",
    });
    const client = createdClients[0];

    expect(runtime.transport.kind).toBe("httpAgent");
    if (runtime.transport.kind !== "httpAgent") {
      throw new Error("Expected httpAgent runtime");
    }

    const transport = runtime.transport;

    const socket = await (
      transport.httpAgent as unknown as {
        connect(
          req: unknown,
          options: {
            host: string;
            port: string;
            secureEndpoint: false;
          },
        ): Promise<Duplex>;
      }
    ).connect(
      {},
      {
        host: "cluster.example.com",
        port: "9243",
        secureEndpoint: false,
      },
    );
    socket.destroy();

    expect(client?.forwardOutCalls).toContainEqual({
      srcIP: "127.0.0.1",
      srcPort: 0,
      dstIP: "cluster.example.com",
      dstPort: 9243,
    });

    await runtime.dispose();
  });

  it("builds OpenSSH-compatible SHA256 fingerprints", () => {
    expect(buildSshFingerprintSha256(Buffer.from("ssh-host-key"))).toBe(
      `SHA256:${createHash("sha256").update("ssh-host-key").digest("base64")}`,
    );
  });
});
