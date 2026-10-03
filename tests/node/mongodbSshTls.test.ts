import { MongoClient } from "mongodb";
import { beforeAll, describe, expect, it, vi } from "vitest";
import { ConnectionManager } from "../../src/extension/connectionManager";
import type {
  ConnectionSshSettings,
  SshRuntimeRequest,
} from "../../src/extension/services/sshRuntime";
import type {
  ConnectionConfig,
  ConnectionTlsMode,
} from "../../src/shared/connectionConfig";
import {
  createExtensionContextStub,
  FakeConnectionManagerStore,
} from "../support/fakeConnectionManagerStore";
import {
  createMongoTlsCertificates,
  startMongoTlsFixture,
} from "../support/mongoTlsFixture";

vi.mock("vscode", async () => ({
  EventEmitter: (await import("../support/mockVscode")).MockEventEmitter,
  ProgressLocation: { Window: 10 },
  window: {},
}));

type Fixture = Awaited<ReturnType<typeof startMongoTlsFixture>>;
let certificates: Awaited<ReturnType<typeof createMongoTlsCertificates>>;
beforeAll(async () => {
  certificates = await createMongoTlsCertificates();
});

function clientTls(mode: ConnectionTlsMode = "requireVerifyFull") {
  return {
    mode,
    caFilePath: certificates.caFilePath,
    ...(mode === "mutualTls"
      ? {
          certFilePath: certificates.certFilePath,
          keyFilePath: certificates.keyFilePath,
        }
      : {}),
  };
}

async function withFixture(
  run: (fixture: Fixture) => Promise<void>,
  mode: ConnectionTlsMode = "requireVerifyFull",
  ip = false,
) {
  const fixture = await startMongoTlsFixture({
    cert: ip ? certificates.ipCert : certificates.serverCert,
    key: ip ? certificates.ipKey : certificates.serverKey,
    ca: certificates.ca,
    requestCert: mode === "mutualTls",
    rejectUnauthorized: true,
  });
  try {
    await run(fixture);
  } finally {
    await fixture.close();
  }
}

async function connectThroughManager(
  fixture: Fixture,
  fields: Partial<ConnectionConfig> = {},
) {
  const dispose = vi.fn(async () => undefined);
  // Only the SSH transport is injected. MongoDBDriver and MongoClient are real.
  const createSshRuntime = vi.fn(
    async (_settings: ConnectionSshSettings, request: SshRuntimeRequest) => {
      if (request.kind !== "tcpForward")
        throw new Error("Expected TCP forward");
      return {
        transport: {
          kind: "tcpForward" as const,
          localHost: "127.0.0.1" as const,
          localPort: fixture.port,
          remoteHost: request.remoteHost,
          remotePort: request.remotePort,
        },
        verifiedFingerprintSha256:
          "SHA256:AbCdEfGhIjKlMnOpQrStUvWxYz0123456789+/",
        dispose,
      };
    },
  );
  const manager = new ConnectionManager(
    createExtensionContextStub() as never,
    new FakeConnectionManagerStore(),
    { createSshRuntime },
  );
  try {
    return await manager.testConnection({
      name: "Mongo TLS",
      type: "mongodb",
      connectionUri:
        "mongodb://db.internal:27017/app?serverSelectionTimeoutMS=600&connectTimeoutMS=500",
      tls: clientTls(),
      ssh: {
        host: "bastion.invalid",
        username: "test",
        authMethod: "password",
        password: "test",
        hostVerificationMode: "manual",
        hostFingerprintSha256: "SHA256:AbCdEfGhIjKlMnOpQrStUvWxYz0123456789+/",
      },
      ...fields,
    });
  } finally {
    await manager.dispose();
    expect(dispose).toHaveBeenCalledTimes(1);
  }
}

describe("MongoDB SSH TLS — real official-driver handshakes", () => {
  it("rejects the trusted db.internal certificate at the local address without the original hostname", async () => {
    await withFixture(async (fixture) => {
      const client = new MongoClient(
        `mongodb://127.0.0.1:${fixture.port}/app`,
        {
          tls: true,
          ca: certificates.ca,
          serverSelectionTimeoutMS: 600,
        },
      );
      try {
        await expect(client.connect()).rejects.toThrow(/IP.*not in the cert/);
        expect(fixture.commands).toBe(0);
      } finally {
        await client.close();
      }
    });
  });

  it.each([
    ["requireVerifyFull", "connectionUri"],
    ["mutualTls", "connectionUri"],
    ["requireVerifyFull", "uri"],
    ["mutualTls", "uri"],
    ["requireVerifyFull", "host"],
    ["mutualTls", "host"],
  ] as const)("preserves the %s hostname from %s", async (mode, source) => {
    await withFixture(async (fixture) => {
      const fields: Partial<ConnectionConfig> = { tls: clientTls(mode) };
      if (source === "uri") {
        fields.connectionUri = undefined;
        fields.uri = "mongodb://db.internal/app?serverSelectionTimeoutMS=600";
        fields.host = "ignored.invalid";
      } else if (source === "host") {
        fields.connectionUri = undefined;
        fields.host = "db.internal";
      } else {
        fields.host = "ignored.invalid";
        fields.uri = "mongodb://ignored.invalid/app";
      }
      expect(await connectThroughManager(fixture, fields)).toEqual({
        success: true,
      });
      expect(fixture.commands).toBeGreaterThan(0);
      expect(fixture.handshakes).toContainEqual({
        servername: "db.internal",
        authorized: mode === "mutualTls",
      });
    }, mode);
  });

  it.each([
    "requireVerifyFull",
    "mutualTls",
  ] as const)("honors explicit serverNameOverride before the runtime hostname (%s)", async (mode) => {
    await withFixture(async (fixture) => {
      const result = await connectThroughManager(fixture, {
        connectionUri:
          "mongodb://different.internal/app?serverSelectionTimeoutMS=600",
        tls: { ...clientTls(mode), serverNameOverride: " db.internal " },
      });
      expect(result).toEqual({ success: true });
      expect(fixture.handshakes).toContainEqual({
        servername: "db.internal",
        authorized: mode === "mutualTls",
      });
    }, mode);
  });

  it("falls back to the runtime hostname for a blank override", async () => {
    await withFixture(async (fixture) => {
      const result = await connectThroughManager(fixture, {
        tls: { ...clientTls(), serverNameOverride: "  " },
      });
      expect(result).toEqual({ success: true });
    });
  });

  it.each([
    ["requireVerifyFull", "192.0.2.12", "connectionUri"],
    ["mutualTls", "192.0.2.12", "connectionUri"],
    ["requireVerifyFull", "[2001:db8::12]", "connectionUri"],
    ["mutualTls", "[2001:db8::12]", "connectionUri"],
    ["requireVerifyFull", "192.0.2.12", "host"],
    ["mutualTls", "192.0.2.12", "host"],
    ["requireVerifyFull", "2001:db8::12", "host"],
    ["mutualTls", "2001:db8::12", "host"],
  ] as const)("verifies the original IP SAN without IP SNI: %s %s via %s", async (mode, host, source) => {
    await withFixture(
      async (fixture) => {
        const fields: Partial<ConnectionConfig> = { tls: clientTls(mode) };
        if (source === "host") {
          fields.connectionUri = undefined;
          fields.host = host;
        } else {
          fields.connectionUri = `mongodb://${host}/app?serverSelectionTimeoutMS=600`;
        }
        expect(await connectThroughManager(fixture, fields)).toEqual({
          success: true,
        });
        expect(fixture.commands).toBeGreaterThan(0);
        expect(fixture.handshakes).toContainEqual({
          servername: false,
          authorized: mode === "mutualTls",
        });
      },
      mode,
      true,
    );
  });

  it.each([
    "192.0.2.12",
    "2001:db8::12",
  ])("honors an IP serverNameOverride without IP SNI: %s", async (serverNameOverride) => {
    await withFixture(
      async (fixture) => {
        const result = await connectThroughManager(fixture, {
          tls: { ...clientTls(), serverNameOverride },
        });
        expect(result).toEqual({ success: true });
        expect(fixture.handshakes).toContainEqual({
          servername: false,
          authorized: false,
        });
      },
      "requireVerifyFull",
      true,
    );
  });

  it.each([
    "requireVerifyFull",
    "mutualTls",
  ] as const)("still rejects a wrong DNS override (%s)", async (mode) => {
    await withFixture(async (fixture) => {
      const result = await connectThroughManager(fixture, {
        tls: { ...clientTls(mode), serverNameOverride: "wrong.internal" },
      });
      expect(result).toMatchObject({
        success: false,
        error: expect.stringMatching(/Hostname\/IP does not match/),
      });
      expect(fixture.commands).toBe(0);
    }, mode);
  });

  it.each([
    "192.0.2.13",
    "[2001:db8::13]",
  ])("still rejects an incorrect IP SAN: %s", async (host) => {
    await withFixture(
      async (fixture) => {
        const result = await connectThroughManager(fixture, {
          connectionUri: `mongodb://${host}/app?serverSelectionTimeoutMS=600`,
        });
        expect(result).toMatchObject({
          success: false,
          error: expect.stringMatching(/IP.*not in the cert/),
        });
        expect(fixture.commands).toBe(0);
      },
      "requireVerifyFull",
      true,
    );
  });

  it("rejects an untrusted certificate even with the correct original hostname", async () => {
    await withFixture(async (fixture) => {
      const result = await connectThroughManager(fixture, {
        tls: { mode: "requireVerifyFull" },
      });
      expect(result).toMatchObject({
        success: false,
        error: expect.stringMatching(/certificate|issuer/i),
      });
      expect(fixture.commands).toBe(0);
    });
  });

  it("requires a client certificate on the mTLS endpoint", async () => {
    await withFixture(async (fixture) => {
      const result = await connectThroughManager(fixture);
      expect(result).toMatchObject({
        success: false,
        error: expect.stringMatching(/certificate|alert|closed/i),
      });
      expect(fixture.commands).toBe(0);
    }, "mutualTls");
  });
});
