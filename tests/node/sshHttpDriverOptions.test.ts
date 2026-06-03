import * as http from "node:http";
import * as https from "node:https";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";

const elasticMocks = vi.hoisted(() => ({
  constructorCalls: [] as Array<Record<string, unknown>>,
  ping: vi.fn().mockResolvedValue(undefined),
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
  beforeEach(() => {
    elasticMocks.constructorCalls.length = 0;
    elasticMocks.ping.mockClear().mockResolvedValue(undefined);
    elasticMocks.close.mockClear().mockResolvedValue(undefined);

    dynamoMocks.constructorCalls.length = 0;
    dynamoMocks.handlerOptions.length = 0;
    dynamoMocks.send.mockClear().mockResolvedValue({ TableNames: [] });
    dynamoMocks.destroy.mockClear();
  });

  it("injects the SSH HTTP(S) agent into Elasticsearch without rewriting the node URL", async () => {
    const httpAgent = new http.Agent();
    const httpsAgent = new https.Agent();

    const driver = new ElasticsearchDriver({
      id: "es-ssh",
      name: "Elastic SSH",
      type: "elasticsearch",
      endpoint: "https://cluster.example.com",
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
      Connection?: unknown;
      node: string;
      agent?: (input: { url: URL }) => unknown;
    };
    expect(options.Connection).toBe(HttpConnection);
    expect(options.node).toBe("https://cluster.example.com");
    expect(
      options.agent?.({ url: new URL("https://cluster.example.com") }),
    ).toBe(httpsAgent);
    expect(
      options.agent?.({ url: new URL("http://cluster.example.com") }),
    ).toBe(httpAgent);
  });

  it("injects NodeHttpHandler with SSH agents into DynamoDB without rewriting the endpoint", async () => {
    const httpAgent = new http.Agent();
    const httpsAgent = new https.Agent();

    const driver = new DynamoDBDriver({
      id: "ddb-ssh",
      name: "Dynamo SSH",
      type: "dynamodb",
      awsRegion: "us-east-1",
      endpoint: "https://dynamodb.us-east-1.amazonaws.com",
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
      endpoint: "https://dynamodb.us-east-1.amazonaws.com",
    });
    expect(dynamoMocks.handlerOptions[0]).toMatchObject({
      httpAgent,
      httpsAgent,
    });
    expect(dynamoMocks.send).toHaveBeenCalledTimes(1);
  });
});
