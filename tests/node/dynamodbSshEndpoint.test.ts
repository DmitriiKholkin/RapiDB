import { rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { NodeHttpHandler } from "@smithy/node-http-handler";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ConnectionManager } from "../../src/extension/connectionManager";
import type { SshRuntimeRequest } from "../../src/extension/services/sshRuntime";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";
import { createProjectTempDir } from "../runtime/tempDirectories";
import { awsIdentityResponse } from "../support/awsIdentityResponse";
import {
  createExtensionContextStub,
  FakeConnectionManagerStore,
} from "../support/fakeConnectionManagerStore";
import {
  OTHER_SSH_HOST_FINGERPRINT_SHA256,
  SSH_HOST_FINGERPRINT_SHA256,
} from "../support/sshFingerprints";

vi.mock("vscode", async () => {
  const { MockEventEmitter } = await import("../support/mockVscode");
  return {
    EventEmitter: MockEventEmitter,
    ProgressLocation: { Window: 10 },
    window: {},
  };
});

const baseConfig = {
  id: "dynamodb-ssh",
  name: "DynamoDB SSH",
  type: "dynamodb",
  awsRegion: "us-east-1",
  awsAccessKeyId: "test-access",
  awsSecretAccessKey: "test-secret",
  ssh: {
    host: "bastion.test",
    username: "test",
    authMethod: "password",
    password: "test-ssh",
    hostFingerprintSha256: SSH_HOST_FINGERPRINT_SHA256,
  },
} satisfies ConnectionConfig;

function setup(
  config: ConnectionConfig,
  getHostFingerprint = () => SSH_HOST_FINGERPRINT_SHA256,
) {
  const handle = NodeHttpHandler.prototype.handle;
  const send = vi
    .spyOn(NodeHttpHandler.prototype, "handle")
    .mockImplementation(async function (this: NodeHttpHandler, request) {
      // Let Smithy initialize its agents, but stop it before socket creation.
      await expect(
        handle.call(this, request, { abortSignal: AbortSignal.abort() }),
      ).rejects.toMatchObject({ name: "AbortError" });
      const host = String(request.headers.host ?? request.hostname);
      if (host.startsWith("sts.")) {
        return awsIdentityResponse(
          "123456789012",
          host.endsWith(".cn") ? "aws-cn" : "aws",
        );
      }
      return {
        response: {
          statusCode: 200,
          headers: {},
          body: Buffer.from('{"TableNames":[]}'),
        },
      };
    });
  const dispose = vi.fn(async () => undefined);
  const createSshRuntime = vi.fn(
    async (_ssh: unknown, request: SshRuntimeRequest) => {
      if (request.kind !== "tcpForward")
        throw new Error("Expected TCP forwarding");
      // This happens after target resolution and before the real SDK client is
      // built. SDK config must not be able to retarget the pinned tunnel now.
      vi.stubEnv("AWS_ENDPOINT_URL_DYNAMODB", "https://changed.test");
      const { extraForwards, ...main } = request;
      return {
        transport: {
          ...main,
          localHost: "127.0.0.1" as const,
          localPort: 15432,
          ...(extraForwards
            ? {
                extraForwards: extraForwards.map((forward, index) => ({
                  ...forward,
                  localHost: "127.0.0.1" as const,
                  localPort: 15433 + index,
                })),
              }
            : {}),
        },
        verifiedFingerprintSha256: getHostFingerprint(),
        dispose,
      };
    },
  );
  const store = new FakeConnectionManagerStore();
  const manager = new ConnectionManager(
    createExtensionContextStub() as never,
    store,
    { createSshRuntime },
  );
  return { config, manager, store, send, createSshRuntime, dispose };
}

describe("DynamoDB SDK endpoints through the production SSH path", () => {
  let awsDir: string;

  beforeEach(async () => {
    awsDir = await createProjectTempDir("dynamodb-ssh", "aws-config-");
    vi.stubEnv("AWS_CONFIG_FILE", join(awsDir, "config"));
    vi.stubEnv("AWS_SHARED_CREDENTIALS_FILE", join(awsDir, "credentials"));
    vi.stubEnv("AWS_PROFILE", undefined);
    vi.stubEnv("AWS_ENDPOINT_URL", undefined);
    vi.stubEnv("AWS_ENDPOINT_URL_DYNAMODB", undefined);
    vi.stubEnv("AWS_EC2_METADATA_DISABLED", "true");
    vi.stubEnv("AWS_ACCOUNT_ID_ENDPOINT_MODE", "preferred");
    vi.stubEnv("AWS_USE_DUALSTACK_ENDPOINT", "false");
    vi.stubEnv("AWS_USE_FIPS_ENDPOINT", "false");
  });

  afterEach(async () => {
    vi.unstubAllEnvs();
    await rm(awsDir, { recursive: true, force: true });
  });

  it.each([
    {
      label: "China",
      region: "cn-north-1",
      url: "https://dynamodb.cn-north-1.amazonaws.com.cn",
      signingRegion: "cn-north-1",
    },
    {
      label: "commercial",
      region: "us-east-1",
      url: "https://dynamodb.us-east-1.amazonaws.com",
      signingRegion: "us-east-1",
    },
    {
      label: "legacy FIPS region",
      region: "fips-us-east-1",
      url: "https://dynamodb-fips.us-east-1.amazonaws.com",
      signingRegion: "us-east-1",
    },
    {
      label: "dual-stack setting",
      region: "cn-north-1",
      url: "https://dynamodb.cn-north-1.api.amazonwebservices.com.cn",
      signingRegion: "cn-north-1",
      env: { AWS_USE_DUALSTACK_ENDPOINT: "true" },
    },
    {
      label: "SDK local region and its signing override",
      region: "local",
      url: "http://localhost:8000",
      signingRegion: "us-east-1",
    },
    {
      label: "service endpoint setting before the global one",
      region: "cn-north-1",
      url: "http://service.test:8000/dynamo",
      signingRegion: "cn-north-1",
      env: {
        AWS_ENDPOINT_URL: "https://global.test",
        AWS_ENDPOINT_URL_DYNAMODB: "http://service.test:8000/dynamo",
      },
    },
    {
      label: "shared AWS config endpoint setting",
      region: "us-east-1",
      url: "https://shared-config.test:8443/dynamo",
      signingRegion: "us-east-1",
      sharedConfig:
        "[default]\nendpoint_url = https://shared-config.test:8443/dynamo\n",
    },
    {
      label: "explicit endpoint before legacy and AWS settings",
      region: "cn-north-1",
      url: "https://explicit.test:8443/dynamo",
      signingRegion: "cn-north-1",
      explicit: {
        endpoint: "https://explicit.test:8443/dynamo",
        awsEndpoint: "https://legacy.test",
      },
      env: { AWS_ENDPOINT_URL_DYNAMODB: "https://service.test" },
    },
    {
      label: "legacy endpoint before AWS settings",
      region: "us-east-1",
      url: "https://legacy.test",
      signingRegion: "us-east-1",
      explicit: {
        awsEndpoint: "https://legacy.test",
      },
      env: { AWS_ENDPOINT_URL_DYNAMODB: "https://service.test" },
    },
  ])("pins and signs the $label endpoint", async ({
    region,
    url,
    signingRegion,
    env,
    explicit,
    sharedConfig,
  }) => {
    for (const [key, value] of Object.entries(env ?? {})) {
      vi.stubEnv(key, value);
    }
    if (sharedConfig) {
      await writeFile(join(awsDir, "config"), sharedConfig);
    }
    const { config, manager, store, send, createSshRuntime, dispose } = setup({
      ...baseConfig,
      awsRegion: region,
      ...explicit,
    });
    try {
      await manager.saveConnection(config);
      const persisted = store.getConnections();
      await manager.connectTo(config.id);

      const endpoint = new URL(url);
      const isAws =
        url.includes("amazonaws.") || url.includes("amazonwebservices.");
      const stsHost = `sts.${signingRegion}.amazonaws.com${region.startsWith("cn-") ? ".cn" : ""}`;
      expect(createSshRuntime).toHaveBeenCalledExactlyOnceWith(
        expect.anything(),
        {
          kind: "tcpForward",
          remoteHost: endpoint.hostname,
          remotePort: Number(
            endpoint.port || (endpoint.protocol === "https:" ? 443 : 80),
          ),
          // STS account verification uses the same SSH session.
          ...(isAws
            ? { extraForwards: [{ remoteHost: stsHost, remotePort: 443 }] }
            : {}),
        },
        { signal: expect.any(AbortSignal) },
      );
      const ddbCalls = send.mock.calls.filter(
        ([request]) =>
          request.hostname === "127.0.0.1" && request.port === 15432,
      );
      expect(ddbCalls).toHaveLength(1);
      const [request] = ddbCalls[0];
      const sts = send.mock.calls.find(([request]) =>
        String(request.headers.host).startsWith("sts."),
      );
      if (isAws) {
        expect(sts?.[0]).toMatchObject({
          hostname: "127.0.0.1",
          port: 15433,
          headers: { host: stsHost },
        });
      } else {
        expect(sts).toBeUndefined();
      }
      // Nothing is sent outside the SSH forwards.
      expect(
        send.mock.calls.every(([call]) => call.hostname === "127.0.0.1"),
      ).toBe(true);
      expect(request).toMatchObject({
        protocol: endpoint.protocol,
        hostname: "127.0.0.1",
        port: 15432,
        path: endpoint.pathname === "/" ? "/" : `${endpoint.pathname}/`,
        headers: {
          host: endpoint.host,
          authorization: expect.stringContaining(
            `/${signingRegion}/dynamodb/aws4_request`,
          ),
        },
      });
      expect(request.headers.authorization).toMatch(
        /SignedHeaders=[^,]*\bhost\b/,
      );
      if (endpoint.protocol === "https:") {
        const handler = send.mock.contexts.at(-1);
        if (!(handler instanceof NodeHttpHandler))
          throw new Error("Expected the Smithy HTTP handler");
        const agent = handler.httpHandlerConfigs().httpsAgent;
        expect(agent).toMatchObject({
          options: {
            servername: endpoint.hostname,
          },
        });
        expect(agent).not.toHaveProperty("options.rejectUnauthorized", false);
      }
      expect(store.getConnections()).toEqual(persisted);
    } finally {
      await manager.dispose();
    }
    expect(dispose).toHaveBeenCalledOnce();
  });

  it("uses account metadata from the configured AWS credentials profile", async () => {
    await writeFile(
      join(awsDir, "credentials"),
      "[dynamo-test]\naws_access_key_id = profile-test-access\naws_secret_access_key = profile-test-secret\naws_account_id = 123456789012\n",
    );
    vi.stubEnv("AWS_ACCOUNT_ID_ENDPOINT_MODE", "required");
    const { config, manager, send, createSshRuntime } = setup({
      ...baseConfig,
      awsAccessKeyId: undefined,
      awsSecretAccessKey: undefined,
      awsProfile: "dynamo-test",
      awsCredentialMode: "profile",
    });
    try {
      await manager.saveConnection(config);
      await manager.connectTo(config.id);
      expect(createSshRuntime.mock.calls[0][1]).toEqual({
        kind: "tcpForward",
        remoteHost: "123456789012.ddb.us-east-1.amazonaws.com",
        remotePort: 443,
        extraForwards: [
          { remoteHost: "sts.us-east-1.amazonaws.com", remotePort: 443 },
        ],
      });
      expect(send.mock.lastCall?.[0].headers).toMatchObject({
        host: "123456789012.ddb.us-east-1.amazonaws.com",
        authorization: expect.stringContaining(
          "Credential=profile-test-access/",
        ),
      });
    } finally {
      await manager.dispose();
    }
  });

  it("does not switch to an SDK account endpoint for a table ARN", async () => {
    const { config, manager, send } = setup(baseConfig);
    try {
      await manager.saveConnection(config);
      await manager.connectTo(config.id);
      const driver = manager.getDriver(config.id);
      if (!driver) throw new Error("Expected a connected driver");
      await driver.query(
        JSON.stringify({
          TableName: "arn:aws:dynamodb:us-east-1:123456789012:table/example",
          Key: { id: { S: "test" } },
        }),
      );
      expect(send).toHaveBeenCalledTimes(3);
      expect(send.mock.lastCall?.[0]).toMatchObject({
        hostname: "127.0.0.1",
        port: 15432,
        headers: {
          host: "dynamodb.us-east-1.amazonaws.com",
          "x-amz-target": "DynamoDB_20120810.GetItem",
        },
      });
    } finally {
      await manager.dispose();
    }
  });

  it("binds a custom SSH endpoint to the verified bastion host key", async () => {
    let fingerprint = SSH_HOST_FINGERPRINT_SHA256;
    const { config, manager, send } = setup(
      { ...baseConfig, endpoint: "http://db.internal:8000" },
      () => fingerprint,
    );
    try {
      await manager.saveConnection(config);
      await manager.connectTo(config.id);
      const key = manager.getQueryTargetSessionKey(config.id);
      if (!key) throw new Error("Expected a verified SSH target key");
      await manager.disconnectFrom(config.id);
      // Simulate an SSH runtime presenting a different, newly trusted host
      // key under the same configured host name and remote server address.
      fingerprint = OTHER_SSH_HOST_FINGERPRINT_SHA256;
      await expect(
        manager.getDriverForQuery(config.id, key),
      ).resolves.toBeUndefined();
      expect(manager.getQueryTargetSessionKey(config.id)).not.toBe(key);
      expect(
        send.mock.calls.every(([request]) => request.hostname === "127.0.0.1"),
      ).toBe(true);
    } finally {
      await manager.dispose();
    }
  });

  it.each([
    "ftp://unsupported.test",
    "https://invalid.test/path#fragment",
    "https://invalid.test:0",
  ])("rejects an invalid SDK endpoint before opening SSH: %s", async (endpoint) => {
    vi.stubEnv("AWS_ENDPOINT_URL_DYNAMODB", endpoint);
    const { manager, send, createSshRuntime } = setup(baseConfig);
    try {
      await expect(manager.testConnection(baseConfig)).resolves.toMatchObject({
        success: false,
        error: expect.stringContaining("requires a valid HTTP(S) endpoint"),
      });
      expect(createSshRuntime).not.toHaveBeenCalled();
      expect(send).not.toHaveBeenCalled();
    } finally {
      await manager.dispose();
    }
  });
});
