import { createHash } from "node:crypto";
import { EventEmitter } from "node:events";
import * as net from "node:net";
import type { Duplex } from "node:stream";
import { PassThrough } from "node:stream";
import { afterEach, describe, expect, it } from "vitest";
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
    this.ended = true;
    this.emit("close");
  }

  forwardOut(
    srcIP: string,
    srcPort: number,
    dstIP: string,
    dstPort: number,
    callback: (error: Error | undefined, stream?: PassThrough) => void,
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

    await runtime.dispose();
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
