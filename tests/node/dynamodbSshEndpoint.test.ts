import { rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { NodeHttpHandler } from "@smithy/node-http-handler";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ConnectionManager } from "../../src/extension/connectionManager";
import type { SshRuntimeRequest } from "../../src/extension/services/sshRuntime";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";
import { createProjectTempDir } from "../runtime/tempDirectories";
import {
  createExtensionContextStub,
  FakeConnectionManagerStore,
} from "../support/fakeConnectionManagerStore";
import { SSH_HOST_FINGERPRINT_SHA256 } from "../support/sshFingerprints";

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

function setup(config: ConnectionConfig) {
  const handle = NodeHttpHandler.prototype.handle;
  const send = vi
    .spyOn(NodeHttpHandler.prototype, "handle")
    .mockImplementation(async function (this: NodeHttpHandler, request) {
      // Let Smithy initialize its agents, but stop it before socket creation.
      await expect(
        handle.call(this, request, { abortSignal: AbortSignal.abort() }),
      ).rejects.toMatchObject({ name: "AbortError" });
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
      return {
        transport: {
          ...request,
          localHost: "127.0.0.1" as const,
          localPort: 15432,
        },
        verifiedFingerprintSha256: SSH_HOST_FINGERPRINT_SHA256,
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
      expect(createSshRuntime).toHaveBeenCalledExactlyOnceWith(
        expect.anything(),
        {
          kind: "tcpForward",
          remoteHost: endpoint.hostname,
          remotePort: Number(
            endpoint.port || (endpoint.protocol === "https:" ? 443 : 80),
          ),
        },
        { signal: expect.any(AbortSignal) },
      );
      expect(send).toHaveBeenCalledTimes(1);
      const [request] = send.mock.calls[0];
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
        const handler = send.mock.contexts[0];
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
    });
    try {
      await manager.saveConnection(config);
      await manager.connectTo(config.id);
      expect(createSshRuntime.mock.calls[0][1]).toEqual({
        kind: "tcpForward",
        remoteHost: "123456789012.ddb.us-east-1.amazonaws.com",
        remotePort: 443,
      });
      expect(send.mock.calls[0][0].headers).toMatchObject({
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
      expect(send).toHaveBeenCalledTimes(2);
      expect(send.mock.calls[1][0]).toMatchObject({
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
