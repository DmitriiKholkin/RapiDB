import { DynamoDBClient, ListTablesCommand } from "@aws-sdk/client-dynamodb";
import { Client as ElasticsearchClient } from "@elastic/elasticsearch";
import { MongoClient, type MongoClientOptions } from "mongodb";
import { describe, expect, it, vi } from "vitest";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";
import { validateConnectionConfig } from "../../src/shared/connectionValidation";

function expectInvalidField(config: Partial<ConnectionConfig>, field: string) {
  const result = validateConnectionConfig(config);
  expect(result.valid).toBe(false);
  expect(result.issues).toContainEqual(
    expect.objectContaining({ code: "invalid", fields: [field] }),
  );
  return result.message;
}

describe("DynamoDB region validation", () => {
  const config = { type: "dynamodb", name: "DynamoDB" } as const;
  const localEndpoint = "http://localhost:8000";
  function makeClient(region: string, endpoint?: string) {
    const handle = vi.fn(async (_request: { hostname: string }) => ({
      response: { statusCode: 200, headers: {} },
    }));
    const client = new DynamoDBClient({
      region,
      endpoint,
      maxAttempts: 1,
      credentials: { accessKeyId: "fake", secretAccessKey: "fake" },
      requestHandler: { handle, destroy: vi.fn() },
    });
    return { client, handle };
  }

  it.each([
    "bad!region",
    "local_test",
    "region.example",
    "-region",
    "region-",
    "us-east-1\n",
    "x".repeat(64),
  ])("rejects SDK-invalid regions with and without a custom endpoint: %j", async (awsRegion) => {
    for (const endpoint of [undefined, localEndpoint]) {
      const { client, handle } = makeClient(awsRegion, endpoint);
      try {
        await expect(
          client.send(new ListTablesCommand({ Limit: 1 })),
        ).rejects.toThrow(/Region not accepted/);
        expect(handle).not.toHaveBeenCalled();
        expect(
          expectInvalidField({ ...config, awsRegion, endpoint }, "awsRegion"),
        ).toContain("AWS region");
      } finally {
        client.destroy();
      }
    }
  });

  it.each([
    "local",
    "Custom9",
    "future-partition-99",
    "x".repeat(63),
  ])("preserves SDK-accepted signing regions with a local endpoint: %s", async (awsRegion) => {
    const { client, handle } = makeClient(awsRegion, localEndpoint);
    try {
      await client.send(new ListTablesCommand({ Limit: 1 }));
      expect(handle).toHaveBeenCalledWith(
        expect.objectContaining({ hostname: "localhost" }),
        expect.anything(),
      );
      expect(
        validateConnectionConfig({
          ...config,
          awsRegion,
          endpoint: localEndpoint,
          awsEndpoint: "ftp://ignored",
        }).valid,
      ).toBe(true);
    } finally {
      client.destroy();
    }
  });

  it("preserves FIPS and wildcard forms accepted by the SDK region resolver", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    try {
      for (const [awsRegion, resolved] of [
        ["fips-aws-global", "us-east-1"],
        [`fips-prod-${"x".repeat(63)}`, "x".repeat(63)],
        ["*", "*"],
      ]) {
        const { client } = makeClient(awsRegion);
        try {
          await expect(client.config.region()).resolves.toBe(resolved);
          expect(validateConnectionConfig({ ...config, awsRegion }).valid).toBe(
            true,
          );
        } finally {
          client.destroy();
        }
      }
    } finally {
      warn.mockRestore();
    }
  });
});

describe("Elasticsearch Cloud ID validation", () => {
  const config = { type: "elasticsearch", name: "Elastic" } as const;
  const withCloud = (id: string) =>
    new ElasticsearchClient({
      cloud: { id },
      auth: { username: "fake", password: "fake" },
    });
  const encode = (value: string) => Buffer.from(value).toString("base64");

  it("reports a non-string Cloud ID from hand-edited settings", () => {
    const cloudId = 123 as unknown as string;
    expect(() => withCloud(cloudId)).toThrow("Cloud ID must be a string");
    expect(expectInvalidField({ ...config, cloudId }, "cloudId")).toContain(
      "must be a string",
    );
  });

  it.each([
    "bad-id",
    "deployment:",
    "deployment:ZXM=",
    "deployment:!!!",
    `deployment:${encode("example.test$")}`,
    `deployment:${encode("bad host$es")}`,
  ])("reports SDK-rejected Cloud IDs on the active cloud field: %s", (cloudId) => {
    expect(() => withCloud(cloudId)).toThrow(/Cloud ID|Invalid URL/);
    const message = expectInvalidField(
      { ...config, cloudId, connectionUri: "https://unused.example.test" },
      "cloudId",
    );
    expect(message).toContain("Cloud ID");
    expect(message).not.toContain(cloudId);
  });

  it.each([
    [`deployment:${encode("example.test$es")}`, "https://es.example.test/"],
    [`:${encode("example.test$es$")}`, "https://es.example.test/"],
    [
      `deployment:${Buffer.from("example.test:9443$es$😃").toString("base64url")}`,
      "https://es.example.test:9443/",
    ],
    [
      `deployment:${encode("example.test$es$kibana$extra").replace(/.{4}/g, "$& \n!")}`,
      "https://es.example.test/",
    ],
  ])("preserves SDK-supported cloud variants: %j", async (cloudId, url) => {
    const client = withCloud(cloudId);
    try {
      expect(client.connectionPool.connections[0].url.href).toBe(url);
      expect(
        validateConnectionConfig({
          ...config,
          cloudId,
          connectionUri: "not-a-url",
          endpoint: "ftp://ignored",
          port: 0,
        }).valid,
      ).toBe(true);
    } finally {
      await client.close();
    }
  });
});

describe("MongoDB authSource validation", () => {
  const config = { type: "mongodb", name: "MongoDB" } as const;
  const uri = "mongodb://fake:fake@localhost/app";

  it.each([
    "authSource=",
    "AuthSource",
    "%61uthSource=admin&authSource=other",
  ])("rejects SDK-invalid authSource URI options: %s", (query) => {
    const connectionUri = `${uri}?${query}`;
    expect(() => new MongoClient(connectionUri)).toThrow(/authSource/i);
    for (const field of ["connectionUri", "uri"] as const) {
      expect(
        expectInvalidField(
          { ...config, [field]: connectionUri },
          "connectionUri",
        ),
      ).toContain("authSource");
    }
  });

  it.each([
    ["authSource=admin", undefined, "admin"],
    ["authSource=%24external", undefined, "$external"],
    ["authSource=local", undefined, "local"],
    ["authSource=tenant_%25-测试", undefined, "tenant_%-测试"],
    ["authsource=admin&AUTHSOURCE=", undefined, "admin"],
    ["authSource=unused", { authSource: "legacy-auth" }, "legacy-auth"],
  ] satisfies [
    string,
    MongoClientOptions | undefined,
    string,
  ][])("leaves supported authSource values and option precedence alone: %s", async (query, options, source) => {
    const connectionUri = `${uri}?${query}`;
    const client = new MongoClient(connectionUri, options);
    try {
      expect(client.options.credentials?.source).toBe(source);
      expect(
        validateConnectionConfig({
          ...config,
          connectionUri,
          authSource: options?.authSource,
        }).valid,
      ).toBe(true);
    } finally {
      await client.close();
    }
  });

  it("ignores a stale URI authSource in explicit fields mode", () => {
    expect(
      validateConnectionConfig({
        ...config,
        mongoConnectionMode: "fields",
        host: "localhost",
        connectionUri: `${uri}?authSource=`,
        authSource: "$external",
      }).valid,
    ).toBe(true);
  });
});
