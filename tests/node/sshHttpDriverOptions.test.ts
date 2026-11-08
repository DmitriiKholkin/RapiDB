import * as http from "node:http";
import * as https from "node:https";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";

const elasticMocks = vi.hoisted(() => ({
  constructorCalls: [] as Array<Record<string, unknown>>,
  ping: vi.fn().mockResolvedValue(true),
  close: vi.fn().mockResolvedValue(undefined),
}));

const dynamoMocks = vi.hoisted(() => ({
  constructorCalls: [] as Array<Record<string, unknown>>,
  send: vi.fn().mockResolvedValue({ TableNames: [] }),
  destroy: vi.fn(),
  handlerOptions: [] as Array<Record<string, unknown>>,
}));

vi.mock("@elastic/elasticsearch", async (importOriginal) => {
  const actual =
    await importOriginal<typeof import("@elastic/elasticsearch")>();

  class MockClient {
    constructor(options: Record<string, unknown>) {
      elasticMocks.constructorCalls.push(options);
    }

    ping = elasticMocks.ping;
    close = elasticMocks.close;
  }

  return {
    ...actual,
    Client: MockClient,
  };
});

vi.mock("@smithy/node-http-handler", () => ({
  NodeHttpHandler: class MockNodeHttpHandler {
    constructor(options: Record<string, unknown>) {
      dynamoMocks.handlerOptions.push(options);
    }
  },
}));

vi.mock("@aws-sdk/client-dynamodb", async (importOriginal) => {
  const actual =
    await importOriginal<typeof import("@aws-sdk/client-dynamodb")>();

  class MockDynamoDBClient {
    constructor(options: Record<string, unknown>) {
      dynamoMocks.constructorCalls.push(options);
    }

    send = dynamoMocks.send;
    destroy = dynamoMocks.destroy;
  }

  return {
    ...actual,
    DynamoDBClient: MockDynamoDBClient,
  };
});

import { HttpConnection } from "@elastic/transport";
import { DynamoDBDriver } from "../../src/extension/dbDrivers/dynamodb";
import { ElasticsearchDriver } from "../../src/extension/dbDrivers/elasticsearch";

describe("SSH HTTP driver options", () => {
  it.each([
    "cluster.example.com",
    "2001:db8::12",
  ])("keeps the Elasticsearch TLS identity through a TCP tunnel with HTTPS defaults: %s", async (servername) => {
    const driver = new ElasticsearchDriver({
      id: "es-defaults",
      name: "Elastic",
      type: "elasticsearch",
      endpoint: "https://127.0.0.1:15432",
      tls: { mode: "disabled" },
      runtimeOverrides: { tlsServername: servername },
    } as ConnectionConfig);
    await driver.connect();
    expect(elasticMocks.constructorCalls[0]).toMatchObject({
      node: "https://127.0.0.1:15432",
      tls: { servername },
    });
    await driver.disconnect();
  });
  beforeEach(() => {
    elasticMocks.constructorCalls.length = 0;
    elasticMocks.ping.mockClear().mockResolvedValue(true);
    elasticMocks.close.mockClear().mockResolvedValue(undefined);

    dynamoMocks.constructorCalls.length = 0;
    dynamoMocks.handlerOptions.length = 0;
    dynamoMocks.send.mockClear().mockResolvedValue({ TableNames: [] });
    dynamoMocks.destroy.mockClear();
  });

  it.each([
    "https://cluster.example.com",
    "https://[2001:db8::12]:8443/service?keep=true",
  ])("injects the SSH HTTP(S) agent into Elasticsearch without rewriting the node URL: %s", async (endpoint) => {
    const httpAgent = new http.Agent();
    const httpsAgent = new https.Agent();

    const driver = new ElasticsearchDriver({
      id: "es-ssh",
      name: "Elastic SSH",
      type: "elasticsearch",
      endpoint,
      tls: {
        mode: "requireVerifyFull",
      },
      runtimeOverrides: {
        transport: {
          kind: "httpAgent",
          httpAgent,
          httpsAgent,
        },
      },
    } as ConnectionConfig);

    await driver.connect();

    const options = elasticMocks.constructorCalls[0] as {
      Connection?: typeof HttpConnection;
      node: string;
      agent?: (input: { url: URL }) => unknown;
    };
    expect(Object.getPrototypeOf(options.Connection?.prototype)).toBe(
      HttpConnection.prototype,
    );
    expect(options.node).toBe(endpoint);
    const ownedHttpsAgent = options.agent?.({
      url: new URL(endpoint),
    });
    const ownedHttpAgent = options.agent?.({
      url: new URL(endpoint.replace("https:", "http:")),
    });
    expect(ownedHttpsAgent).not.toBe(httpsAgent);
    expect(ownedHttpAgent).not.toBe(httpAgent);
    expect(ownedHttpsAgent).toMatchObject({ protocol: "https:" });
    expect(ownedHttpAgent).toMatchObject({ protocol: "http:" });
  });

  it.each([
    "https://dynamodb.us-east-1.amazonaws.com",
    "https://[2001:db8::12]:8443/service?keep=true",
  ])("injects NodeHttpHandler with SSH agents into DynamoDB without rewriting the endpoint: %s", async (endpoint) => {
    const httpAgent = new http.Agent();
    const httpsAgent = new https.Agent();

    const driver = new DynamoDBDriver({
      id: "ddb-ssh",
      name: "Dynamo SSH",
      type: "dynamodb",
      awsRegion: "us-east-1",
      endpoint,
      runtimeOverrides: {
        transport: {
          kind: "httpAgent",
          httpAgent,
          httpsAgent,
        },
      },
    } as ConnectionConfig);

    await driver.connect();

    expect(dynamoMocks.constructorCalls[0]).toMatchObject({
      region: "us-east-1",
      endpoint,
    });
    expect(dynamoMocks.handlerOptions[0]).toMatchObject({
      httpAgent,
      httpsAgent,
    });
    expect(dynamoMocks.send).toHaveBeenCalledTimes(1);
  });

  it("keeps the remote IPv6 TLS identity in DynamoDB's tunneled HTTPS agent", async () => {
    const driver = new DynamoDBDriver({
      id: "ddb-ipv6-tunnel",
      name: "Dynamo IPv6",
      type: "dynamodb",
      endpoint: "https://127.0.0.1:15432/service?keep=true",
      runtimeOverrides: { tlsServername: "2001:db8::12" },
    } as ConnectionConfig);
    try {
      await driver.connect();
      expect(dynamoMocks.constructorCalls[0]).toMatchObject({
        endpoint: "https://127.0.0.1:15432/service?keep=true",
      });
      expect(dynamoMocks.handlerOptions[0]?.httpsAgent).toMatchObject({
        options: { servername: "2001:db8::12" },
      });
    } finally {
      await driver.disconnect();
    }
  });
});
