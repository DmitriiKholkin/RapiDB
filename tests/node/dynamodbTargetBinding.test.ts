import { rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { NodeHttpHandler } from "@smithy/node-http-handler";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ConnectionManager } from "../../src/extension/connectionManager";
import { connectionSessionKey } from "../../src/extension/connectionManagerStore";
import { bindConnectionSecret } from "../../src/extension/connectionSecretBinding";
import { DynamoDBTarget } from "../../src/extension/dbDrivers/dynamodbTarget";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";
import { createProjectTempDir } from "../runtime/tempDirectories";
import { awsIdentityResponse } from "../support/awsIdentityResponse";
import {
  createExtensionContextStub,
  FakeConnectionManagerStore,
} from "../support/fakeConnectionManagerStore";

vi.mock("vscode", async () => {
  const { MockEventEmitter } = await import("../support/mockVscode");
  return { EventEmitter: MockEventEmitter, window: {} };
});

const firstAccount = "012345678901";
const secondAccount = "109876543210";
const config = {
  id: "dynamo-target",
  name: "DynamoDB",
  type: "dynamodb",
  awsRegion: "us-east-1",
  useSecretStorage: true,
} satisfies ConnectionConfig;

describe("DynamoDB target binding", () => {
  let awsDir: string;
  let store: FakeConnectionManagerStore;
  let manager: ConnectionManager;
  let send: ReturnType<typeof mockHttp>;

  function mockHttp() {
    return vi
      .spyOn(NodeHttpHandler.prototype, "handle")
      .mockImplementation(async (request) => {
        if (request.hostname.startsWith("sts.")) {
          expect(request.hostname).toBe("sts.us-east-1.amazonaws.com");
          expect(request.protocol).toBe("https:");
          expect(request.body).toBe(
            "Action=GetCallerIdentity&Version=2011-06-15",
          );
          const auth = request.headers.authorization;
          expect(auth).toContain("/us-east-1/sts/aws4_request");
          return awsIdentityResponse(
            auth.includes("key-b/") ? secondAccount : firstAccount,
          );
        }
        expect(request.headers["x-amz-target"]).toMatch(/^DynamoDB_20120810\./);
        return {
          response: {
            statusCode: 200,
            headers: {},
            body: Buffer.from('{"TableNames":[]}'),
          },
        };
      });
  }

  beforeEach(async () => {
    awsDir = await createProjectTempDir("dynamodb-target", "aws-config-");
    vi.stubEnv("AWS_CONFIG_FILE", join(awsDir, "config"));
    vi.stubEnv("AWS_SHARED_CREDENTIALS_FILE", join(awsDir, "credentials"));
    vi.stubEnv("AWS_PROFILE", undefined);
    vi.stubEnv("AWS_ENDPOINT_URL", undefined);
    vi.stubEnv("AWS_ENDPOINT_URL_DYNAMODB", undefined);
    vi.stubEnv("AWS_ENDPOINT_URL_STS", "https://must-not-receive.test");
    vi.stubEnv("AWS_USE_DUALSTACK_ENDPOINT", "false");
    vi.stubEnv("AWS_USE_FIPS_ENDPOINT", "false");
    vi.stubEnv("AWS_ACCOUNT_ID_ENDPOINT_MODE", "preferred");
    vi.stubEnv("AWS_EC2_METADATA_DISABLED", "true");
    vi.stubEnv("AWS_ACCESS_KEY_ID", undefined);
    vi.stubEnv("AWS_SECRET_ACCESS_KEY", undefined);
    vi.stubEnv("AWS_SESSION_TOKEN", undefined);
    send = mockHttp();
    store = new FakeConnectionManagerStore();
    manager = new ConnectionManager(
      createExtensionContextStub() as never,
      store,
    );
  });

  afterEach(async () => {
    await manager.dispose();
    vi.restoreAllMocks();
    vi.unstubAllEnvs();
    await rm(awsDir, { recursive: true, force: true });
  });

  it("persists static intent before sanitizing and never uses the default chain when storage is lost", async () => {
    await manager.saveConnection({
      ...config,
      awsCredentialMode: "static",
      awsAccessKeyId: "key-a",
      awsSecretAccessKey: "dummy",
      awsSessionToken: "token-a",
    });
    const [persisted] = store.getConnections();
    expect(persisted).toMatchObject({
      awsCredentialMode: "static",
      awsSessionTokenRequired: true,
    });
    expect(persisted.awsAccessKeyId).toBeUndefined();
    expect(persisted.awsSessionToken).toBeUndefined();
    await store.deleteSecret(config.id);
    vi.stubEnv("AWS_ACCESS_KEY_ID", "key-b");
    vi.stubEnv("AWS_SECRET_ACCESS_KEY", "dummy");
    await expect(manager.connectTo(config.id)).rejects.toThrow(
      /static aws credentials/i,
    );
    expect(send).not.toHaveBeenCalled();
    store.setSecret(
      config.id,
      JSON.stringify({ awsAccessKeyId: "key-a", awsSecretAccessKey: "dummy" }),
    );
    await expect(manager.connectTo(config.id)).rejects.toThrow(/do not match/);
    expect(send).not.toHaveBeenCalled();
    store.setSecret(
      config.id,
      bindConnectionSecret(
        persisted,
        JSON.stringify({
          awsAccessKeyId: "key-a",
          awsSecretAccessKey: "dummy",
        }),
      ),
    );
    await expect(manager.connectTo(config.id)).rejects.toThrow(
      /session token/i,
    );
    expect(send).not.toHaveBeenCalled();
  });

  it("checks for loss of static Secret Storage before returning even an already-connected driver", async () => {
    await manager.saveConnection({
      ...config,
      awsCredentialMode: "static",
      awsAccessKeyId: "key-a",
      awsSecretAccessKey: "dummy",
    });
    await manager.connectTo(config.id);
    const key = manager.getQueryTargetSessionKey(config.id);
    if (!key) throw new Error("Expected a verified static target");
    await store.deleteSecret(config.id);
    const httpCalls = send.mock.calls.length;
    await expect(manager.getDriverForQuery(config.id, key)).rejects.toThrow(
      /different target/i,
    );
    expect(manager.isConnected(config.id)).toBe(false);
    expect(send.mock.calls).toHaveLength(httpCalls);
  });

  it.each([
    [undefined, "default"],
    ["legacy-name", "profile"],
  ] as const)("migrates an unambiguous legacy profile without keys or secrets (%s) to %s", async (awsProfile, expected) => {
    store.setConnections([{ ...config, awsProfile }]);
    if (expected === "profile") {
      await writeFile(
        join(awsDir, "credentials"),
        "[legacy-name]\naws_access_key_id = key-a\naws_secret_access_key = dummy\n",
      );
    } else {
      vi.stubEnv("AWS_ACCESS_KEY_ID", "key-a");
      vi.stubEnv("AWS_SECRET_ACCESS_KEY", "dummy");
    }
    await manager.connectTo(config.id);
    expect(store.getConnections()[0]).toMatchObject({
      awsCredentialMode: expected,
    });
    // The recorded source does not count as a session change.
    expect(manager.isConnected(config.id)).toBe(true);
    expect(manager.getQueryTargetSessionKey(config.id)).toBeTruthy();
    expect(send.mock.calls[0][0].headers.authorization).toContain(
      "Credential=key-a/",
    );
  });

  it("keeps a legacy source unresolved when Secret Storage cannot prove that no keys were saved", async () => {
    store.setConnections([{ ...config, awsProfile: "legacy-name" }]);
    vi.spyOn(store, "getSecret").mockRejectedValue(new Error("locked"));
    vi.stubEnv("AWS_ACCESS_KEY_ID", "key-b");
    vi.stubEnv("AWS_SECRET_ACCESS_KEY", "dummy");
    await expect(manager.connectTo(config.id)).rejects.toThrow();
    expect(store.getConnections()[0].awsCredentialMode).toBeUndefined();
    expect(send).not.toHaveBeenCalled();
  });

  it.each([
    undefined,
    "unchanged-name",
  ])("uses saved legacy access keys as static credentials without recording another source (%s)", async (awsProfile) => {
    store.setConnections([{ ...config, awsProfile }]);
    vi.stubEnv("AWS_ACCESS_KEY_ID", "key-b");
    vi.stubEnv("AWS_SECRET_ACCESS_KEY", "dummy");
    store.setSecret(
      config.id,
      JSON.stringify({ awsAccessKeyId: "key-a", awsSecretAccessKey: "dummy" }),
    );
    await manager.connectTo(config.id);
    expect(store.getConnections()[0].awsCredentialMode).toBeUndefined();
    expect(manager.getQueryTargetSessionKey(config.id)).toBeTruthy();
    expect(send.mock.calls[0][0].headers.authorization).toContain(
      "Credential=key-a/",
    );
  });

  it("binds custom endpoints to long-term access keys but not to rotating temporary key IDs", async () => {
    const endpoint = new URL("http://localhost:8000/");
    const make = (
      initial: Record<string, unknown>,
      next: Record<string, unknown>,
    ) =>
      new DynamoDBTarget(
        endpoint,
        "us-east-1",
        initial as never,
        async () => next as never,
      );
    const temporary = make(
      { accessKeyId: "ASIA-one", secretAccessKey: "s", sessionToken: "t1" },
      { accessKeyId: "ASIA-two", secretAccessKey: "s", sessionToken: "t2" },
    );
    const longTerm = make(
      { accessKeyId: "AKIA-one", secretAccessKey: "s" },
      { accessKeyId: "AKIA-two", secretAccessKey: "s" },
    );
    try {
      await temporary.initialize();
      const before = temporary.fingerprint;
      await expect(temporary.verify()).resolves.toMatchObject({
        accessKeyId: "ASIA-two",
      });
      expect(temporary.fingerprint).toBe(before);

      await longTerm.initialize();
      await expect(longTerm.verify()).rejects.toThrow(/different target/i);
      expect(send).not.toHaveBeenCalled();
    } finally {
      temporary.dispose();
      longTerm.dispose();
    }
  });

  it.each([
    "profile",
    "default",
  ] as const)("revalidates the %s source, allows same-account rotation, and fences changed accounts and saved bindings", async (awsCredentialMode) => {
    async function install(key: string, token: string) {
      if (awsCredentialMode === "profile") {
        await writeFile(
          join(awsDir, "credentials"),
          `[stable-name]\naws_access_key_id = ${key}\naws_secret_access_key = dummy\naws_session_token = ${token}\n`,
        );
      } else {
        vi.stubEnv("AWS_ACCESS_KEY_ID", key);
        vi.stubEnv("AWS_SECRET_ACCESS_KEY", "dummy");
        vi.stubEnv("AWS_SESSION_TOKEN", token);
      }
    }

    await install("key-a", "token-a");
    await manager.saveConnection({
      ...config,
      awsCredentialMode,
      awsProfile: awsCredentialMode === "profile" ? "stable-name" : undefined,
    });
    await manager.connectTo(config.id);
    const oldKey = manager.getQueryTargetSessionKey(config.id);
    if (!oldKey) throw new Error("Expected a verified target");
    expect(oldKey).not.toBe(connectionSessionKey(store.getConnections()[0]));
    await manager.addBookmark(
      config.id,
      '{"TableName":"Example"}',
      undefined,
      oldKey,
    );
    await manager.addToHistory(
      config.id,
      '{"TableName":"Example"}',
      undefined,
      oldKey,
    );
    const driver = manager.getDriver(config.id);
    await install("key-c", "token-rotated");
    await expect(manager.getDriverForQuery(config.id, oldKey)).resolves.toBe(
      driver,
    );
    await driver?.query('{"TableName":"Example","Key":{"id":{"S":"fixture"}}}');
    expect(send.mock.lastCall?.[0].headers).toMatchObject({
      authorization: expect.stringContaining("Credential=key-c/"),
      "x-amz-security-token": "token-rotated",
    });
    const beforeChange = send.mock.calls.length;
    await install("key-b", "token-b");
    await expect(manager.getDriverForQuery(config.id, oldKey)).rejects.toThrow(
      /different target/i,
    );
    expect(
      send.mock.calls.slice(beforeChange).map(([request]) => request.hostname),
    ).toEqual(["sts.us-east-1.amazonaws.com"]);
    expect(manager.getDriver(config.id)).toBeUndefined();
    expect(manager.getQueryTargetSessionKey(config.id)).toBeNull();

    await install("key-a", "token-a");
    await manager.connectTo(config.id, "explicit");
    expect(manager.getQueryTargetSessionKey(config.id)).toBe(oldKey);
    await manager.dispose();
    manager = new ConnectionManager(
      createExtensionContextStub() as never,
      store,
    );
    const saved = store.readBookmarks()[0].targetSessionKey;
    expect(saved).toBe(oldKey);
    expect(store.readHistory()[0].targetSessionKey).toBe(oldKey);
    expect(manager.getQueryTargetSessionKey(config.id)).toBe(oldKey);
    await install("key-b", "token-b");
    await expect(
      manager.getDriverForQuery(config.id, oldKey),
    ).resolves.toBeUndefined();
    expect(manager.getQueryTargetSessionKey(config.id)).not.toBe(oldKey);
    expect(
      send.mock.calls
        .slice(-2)
        .map(([request]) => [
          request.hostname,
          request.headers["x-amz-target"],
        ]),
    ).toEqual([
      ["sts.us-east-1.amazonaws.com", undefined],
      ["dynamodb.us-east-1.amazonaws.com", "DynamoDB_20120810.ListTables"],
    ]);
  });

  it("does not call STS for an SDK-configured custom endpoint and fences endpoint changes", async () => {
    vi.stubEnv("AWS_ENDPOINT_URL_DYNAMODB", "http://localhost:8000/dynamo");
    await manager.saveConnection({
      ...config,
      awsCredentialMode: "static",
      awsAccessKeyId: "key-a",
      awsSecretAccessKey: "dummy",
    });
    await manager.connectTo(config.id);
    const key = manager.getQueryTargetSessionKey(config.id);
    if (!key) throw new Error("Expected a verified local endpoint");
    await expect(
      manager.getDriverForQuery(config.id, key),
    ).resolves.toBeDefined();
    vi.stubEnv("AWS_ENDPOINT_URL_DYNAMODB", "http://localhost:8001/dynamo");
    await expect(manager.getDriverForQuery(config.id, key)).rejects.toThrow(
      /different target/i,
    );
    expect(
      send.mock.calls.map(([request]) => [request.hostname, request.port]),
    ).toEqual([["localhost", 8000]]);
  });

  it("verifies the AWS account through the SSH forward to regional STS", async () => {
    const credentials = { accessKeyId: "key-a", secretAccessKey: "dummy" };
    const target = new DynamoDBTarget(
      new URL("https://dynamodb.us-east-1.amazonaws.com"),
      "us-east-1",
      credentials,
      async () => credentials,
    );
    const handlers: NodeHttpHandler[] = [];
    send.mockImplementation(async function (this: NodeHttpHandler, request) {
      handlers.push(this);
      expect(request.hostname).toBe("127.0.0.1");
      expect(request.port).toBe(4567);
      expect(request.protocol).toBe("https:");
      // The signed Host header still names the STS endpoint.
      expect(request.headers.host).toBe("sts.us-east-1.amazonaws.com");
      expect(request.headers.authorization).toContain(
        "/us-east-1/sts/aws4_request",
      );
      return awsIdentityResponse(firstAccount);
    });
    try {
      expect(target.stsHostname).toBe("sts.us-east-1.amazonaws.com");
      target.routeStsThrough({ localHost: "127.0.0.1", localPort: 4567 });
      await target.initialize();
      expect(target.fingerprint).toMatch(/^[0-9a-f]{64}$/);
      expect(handlers).toHaveLength(1);
      const resolved = await (
        handlers[0] as unknown as {
          configProvider: Promise<
            { httpsAgent?: { options?: Record<string, unknown> } } | undefined
          >;
        }
      ).configProvider;
      const agent = resolved?.httpsAgent;
      // TLS SNI and certificate verification use the STS hostname.
      expect(agent?.options).toMatchObject({
        servername: "sts.us-east-1.amazonaws.com",
        rejectUnauthorized: true,
      });
    } finally {
      target.dispose();
    }
  });

  it("explains an STS endpoint that the SSH host cannot reach", async () => {
    const credentials = { accessKeyId: "key-a", secretAccessKey: "dummy" };
    const target = new DynamoDBTarget(
      new URL("https://dynamodb.us-east-1.amazonaws.com"),
      "us-east-1",
      credentials,
      async () => credentials,
    );
    send.mockRejectedValue(new Error("connect ECONNREFUSED"));
    try {
      target.routeStsThrough({ localHost: "127.0.0.1", localPort: 4567 });
      await expect(target.initialize()).rejects.toThrow(
        /sts\.us-east-1\.amazonaws\.com.*via the SSH host.*VPC endpoint/,
      );
    } finally {
      target.dispose();
    }
  });

  it("requests and requires an SSH forward to STS for an AWS endpoint", async () => {
    const { resolveDynamoDBSshEndpoint, routeDynamoDBStsThroughSsh } =
      await import("../../src/extension/dbDrivers/dynamodbConnection");
    const endpoint = await resolveDynamoDBSshEndpoint({
      ...config,
      awsCredentialMode: "static",
      awsAccessKeyId: "key-a",
      awsSecretAccessKey: "dummy",
    } as ConnectionConfig);
    try {
      expect(endpoint.request).toEqual({
        kind: "tcpForward",
        remoteHost: "dynamodb.us-east-1.amazonaws.com",
        remotePort: 443,
        extraForwards: [
          { remoteHost: "sts.us-east-1.amazonaws.com", remotePort: 443 },
        ],
      });
      const transport = {
        kind: "tcpForward" as const,
        localHost: "127.0.0.1" as const,
        localPort: 1111,
        remoteHost: "dynamodb.us-east-1.amazonaws.com",
        remotePort: 443,
      };
      expect(() => routeDynamoDBStsThroughSsh(endpoint, transport)).toThrow(
        /needs a forward to sts\.us-east-1\.amazonaws\.com:443/,
      );
      routeDynamoDBStsThroughSsh(endpoint, {
        ...transport,
        extraForwards: [
          {
            localHost: "127.0.0.1",
            localPort: 2222,
            remoteHost: "sts.us-east-1.amazonaws.com",
            remotePort: 443,
          },
        ],
      });
      send.mockImplementation(async () => awsIdentityResponse(firstAccount));
      await endpoint.target.initialize();
      expect(send.mock.calls.at(-1)?.[0]).toMatchObject({
        hostname: "127.0.0.1",
        port: 2222,
      });
    } finally {
      endpoint.target.dispose();
    }
  });

  const vpceSts = "vpce-0abc123-def456.sts.us-east-1.vpce.amazonaws.com";

  it("verifies the account through an explicit STS endpoint and binds the target to it", async () => {
    const credentials = { accessKeyId: "key-a", secretAccessKey: "dummy" };
    const dynamo = new URL("https://dynamodb.us-east-1.amazonaws.com");
    const regional = new DynamoDBTarget(
      dynamo,
      "us-east-1",
      credentials,
      async () => credentials,
    );
    const explicit = new DynamoDBTarget(
      dynamo,
      "us-east-1",
      credentials,
      async () => credentials,
      `https://${vpceSts.toUpperCase()}/`,
    );
    const handlers: NodeHttpHandler[] = [];
    send.mockImplementation(async function (this: NodeHttpHandler, request) {
      handlers.push(this);
      expect(request.protocol).toBe("https:");
      expect(request.body).toBe("Action=GetCallerIdentity&Version=2011-06-15");
      expect(request.headers.authorization).toContain(
        "/us-east-1/sts/aws4_request",
      );
      expect(request.headers.host).toBe(request.hostname);
      return awsIdentityResponse(firstAccount);
    });
    try {
      expect(explicit.stsHostname).toBe(vpceSts);
      await regional.initialize();
      await explicit.initialize();
      expect(send.mock.calls.map(([request]) => request.hostname)).toEqual([
        "sts.us-east-1.amazonaws.com",
        vpceSts,
      ]);
      const resolved = await (
        handlers[1] as unknown as {
          configProvider: Promise<
            { httpsAgent?: { options?: Record<string, unknown> } } | undefined
          >;
        }
      ).configProvider;
      expect(resolved?.httpsAgent?.options).toMatchObject({
        rejectUnauthorized: true,
      });
      // Same account, different verifying endpoint: a different target.
      expect(explicit.fingerprint).toMatch(/^[0-9a-f]{64}$/);
      expect(explicit.fingerprint).not.toBe(regional.fingerprint);
    } finally {
      regional.dispose();
      explicit.dispose();
    }
  });

  it("names the STS endpoint setting when account verification fails", async () => {
    const credentials = { accessKeyId: "key-a", secretAccessKey: "dummy" };
    send.mockRejectedValue(new Error("connect ETIMEDOUT"));
    for (const sts of [undefined, `https://${vpceSts}`]) {
      const target = new DynamoDBTarget(
        new URL("https://dynamodb.us-east-1.amazonaws.com"),
        "us-east-1",
        credentials,
        async () => credentials,
        sts,
      );
      try {
        await expect(target.initialize()).rejects.toThrow(
          sts
            ? /configured STS endpoint \(vpce-0abc123.*awsStsEndpoint/
            : /regional STS \(sts\.us-east-1\.amazonaws\.com\).*awsStsEndpoint/,
        );
      } finally {
        target.dispose();
      }
    }
  });

  it.each([
    ["HTTP", `http://${vpceSts}`],
    ["user info", `https://user:pass@${vpceSts}`],
    ["a port", `https://${vpceSts}:8443`],
    ["a path", `https://${vpceSts}/sts`],
    ["a query", `https://${vpceSts}/?Action=x`],
    ["a fragment", `https://${vpceSts}/#x`],
    ["another region", "https://sts.eu-west-1.amazonaws.com"],
    [
      "another region's VPC endpoint",
      "https://vpce-1-a.sts.eu-west-1.vpce.amazonaws.com",
    ],
    ["a non-STS AWS host", "https://dynamodb.us-east-1.amazonaws.com"],
    ["a custom host", "https://sts.example.com"],
    ["another partition", "https://sts.us-east-1.amazonaws.com.cn"],
    ["not a URL", "sts.us-east-1.amazonaws.com"],
  ])("rejects an STS endpoint with %s", (_label, sts) => {
    const credentials = { accessKeyId: "key-a", secretAccessKey: "dummy" };
    expect(
      () =>
        new DynamoDBTarget(
          new URL("https://dynamodb.us-east-1.amazonaws.com"),
          "us-east-1",
          credentials,
          async () => credentials,
          sts,
        ),
    ).toThrow(
      /STS endpoint \(awsStsEndpoint\) must be an HTTPS AWS STS endpoint/,
    );
    expect(send).not.toHaveBeenCalled();
  });

  it("accepts FIPS and dual-stack STS names and refuses an STS endpoint for custom DynamoDB endpoints", () => {
    const credentials = { accessKeyId: "key-a", secretAccessKey: "dummy" };
    const make = (endpoint: string, sts: string) =>
      new DynamoDBTarget(
        new URL(endpoint),
        "us-east-1",
        credentials,
        async () => credentials,
        sts,
      );
    for (const sts of [
      "https://sts-fips.us-east-1.amazonaws.com",
      "https://sts.us-east-1.api.aws/",
      "https://sts.us-east-1.amazonaws.com:443",
    ]) {
      make("https://dynamodb.us-east-1.amazonaws.com", sts).dispose();
    }
    expect(() => make("http://localhost:8000", `https://${vpceSts}`)).toThrow(
      /applies only to recognized AWS DynamoDB endpoints/,
    );
  });

  it("forwards account verification over SSH to the explicit STS endpoint", async () => {
    const { resolveDynamoDBSshEndpoint, routeDynamoDBStsThroughSsh } =
      await import("../../src/extension/dbDrivers/dynamodbConnection");
    const endpoint = await resolveDynamoDBSshEndpoint({
      ...config,
      awsCredentialMode: "static",
      awsAccessKeyId: "key-a",
      awsSecretAccessKey: "dummy",
      awsStsEndpoint: `https://${vpceSts}`,
    } as ConnectionConfig);
    const handlers: NodeHttpHandler[] = [];
    try {
      expect(endpoint.request.extraForwards).toEqual([
        { remoteHost: vpceSts, remotePort: 443 },
      ]);
      const transport = {
        kind: "tcpForward" as const,
        localHost: "127.0.0.1" as const,
        localPort: 1111,
        remoteHost: "dynamodb.us-east-1.amazonaws.com",
        remotePort: 443,
      };
      expect(() =>
        routeDynamoDBStsThroughSsh(endpoint, {
          ...transport,
          extraForwards: [
            {
              localHost: "127.0.0.1",
              localPort: 2222,
              remoteHost: "sts.us-east-1.amazonaws.com",
              remotePort: 443,
            },
          ],
        }),
      ).toThrow(/needs a forward to vpce-0abc123/);
      routeDynamoDBStsThroughSsh(endpoint, {
        ...transport,
        extraForwards: [
          {
            localHost: "127.0.0.1",
            localPort: 3333,
            remoteHost: vpceSts,
            remotePort: 443,
          },
        ],
      });
      send.mockImplementation(async function (this: NodeHttpHandler, request) {
        handlers.push(this);
        // Only the socket address changes; Host and signature name the
        // configured endpoint.
        expect(request.headers.host).toBe(vpceSts);
        expect(request.headers.authorization).toContain(
          "/us-east-1/sts/aws4_request",
        );
        return awsIdentityResponse(firstAccount);
      });
      await endpoint.target.initialize();
      expect(send.mock.calls.at(-1)?.[0]).toMatchObject({
        hostname: "127.0.0.1",
        port: 3333,
      });
      const resolved = await (
        handlers[0] as unknown as {
          configProvider: Promise<
            { httpsAgent?: { options?: Record<string, unknown> } } | undefined
          >;
        }
      ).configProvider;
      expect(resolved?.httpsAgent?.options).toMatchObject({
        servername: vpceSts,
        rejectUnauthorized: true,
      });
    } finally {
      endpoint.target.dispose();
    }
  });

  it("refuses to supply signing credentials after an automatic expiry refresh changes accounts", async () => {
    const now = Date.now();
    const clock = vi.spyOn(Date, "now").mockReturnValue(now);
    const initial = {
      accessKeyId: "key-a",
      secretAccessKey: "dummy",
      sessionToken: "token-a",
      expiration: new Date(now + 120_000),
    };
    const target = new DynamoDBTarget(
      new URL("https://dynamodb.us-east-1.amazonaws.com"),
      "us-east-1",
      initial,
      async () => ({
        ...initial,
        accessKeyId: "key-b",
        expiration: new Date(now + 600_000),
      }),
    );
    try {
      await target.initialize();
      await expect(target.credentials()).resolves.toMatchObject(initial);
      clock.mockReturnValue(now + 121_000);
      await expect(target.credentials()).rejects.toThrow(/different target/i);
      await expect(target.credentials()).rejects.toThrow(/different target/i);
      expect(target.fingerprint).toBeUndefined();
      expect(send.mock.calls.map(([request]) => request.hostname)).toEqual([
        "sts.us-east-1.amazonaws.com",
        "sts.us-east-1.amazonaws.com",
      ]);
    } finally {
      target.dispose();
      clock.mockRestore();
    }
  });

  it.each([
    { account: firstAccount, partition: "aws-cn" },
    { account: secondAccount, partition: "aws" },
  ])("fails before ListTables on inconsistent STS identity: $partition $account", async ({
    account,
    partition,
  }) => {
    send.mockResolvedValue(awsIdentityResponse(account, partition));
    const awsAccessKeyId = "key-a";
    const awsSecretAccessKey = "dummy";
    await manager.saveConnection({
      ...config,
      awsCredentialMode: "static",
      awsAccessKeyId,
      awsSecretAccessKey,
      endpoint: `https://${firstAccount}.ddb.us-east-1.amazonaws.com`,
    });
    await expect(manager.connectTo(config.id)).rejects.toThrow(
      /(verify|different target)/i,
    );
    expect(manager.getDriver(config.id)).toBeUndefined();
    expect(send.mock.calls.map(([request]) => request.hostname)).toEqual([
      "sts.us-east-1.amazonaws.com",
    ]);
  });
});
