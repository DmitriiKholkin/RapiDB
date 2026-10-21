import {
  createServer,
  type IncomingMessage,
  type ServerResponse,
} from "node:http";
import { describe, expect, it, vi } from "vitest";
import { DynamoDBDriver } from "../../src/extension/dbDrivers/dynamodb";
import { ElasticsearchDriver } from "../../src/extension/dbDrivers/elasticsearch";

async function serve(
  handler: (
    request: IncomingMessage,
    response: ServerResponse,
  ) => Promise<void>,
) {
  const server = createServer((request, response) => {
    void handler(request, response).catch(() => response.destroy());
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("Missing port");
  return {
    endpoint: `http://127.0.0.1:${address.port}`,
    async close() {
      server.closeAllConnections();
      await new Promise<void>((resolve, reject) =>
        server.close((error) => (error ? reject(error) : resolve())),
      );
    },
  };
}

describe("NoSQL mutation retries (real SDK + HTTP)", () => {
  it.each([
    "table insert",
    "REST insert",
    "REST script",
  ])("does not replay an Elasticsearch %s after losing its response", async (operation) => {
    let writes = 0;
    const server = await serve(async (request, response) => {
      for await (const _chunk of request) {
        // Consume the full request before simulating a committed write.
      }
      if (request.method === "HEAD") {
        response.setHeader("x-elastic-product", "Elasticsearch");
        response.end();
        return;
      }
      writes++;
      response.destroy();
    });
    const driver = new ElasticsearchDriver({
      id: "retry-es",
      name: "Retry ES",
      type: "elasticsearch",
      endpoint: server.endpoint,
    });
    try {
      await driver.connect();
      const write =
        operation === "table insert"
          ? driver.insertRow({
              database: "default",
              schema: "indices",
              table: "records",
              values: { _source: '{"count":1}' },
            })
          : driver.query(
              operation === "REST insert"
                ? 'POST /records/_doc\n{"count":1}'
                : 'POST /records/_update/selected\n{"script":{"source":"ctx._source.count += 1"}}',
            );
      await expect(write).rejects.toThrow();
      expect(writes).toBe(1);
    } finally {
      await driver.disconnect();
      await server.close();
    }
  }, 30_000);

  it.each([
    "connection loss",
    "server error",
  ])("does not replay a DynamoDB counter update after %s, even with retry environment overrides", async (failure) => {
    vi.stubEnv("AWS_MAX_ATTEMPTS", "4");
    let writes = 0;
    const server = await serve(async (request, response) => {
      for await (const _chunk of request) {
        // Consume the complete AWS request.
      }
      if (String(request.headers["x-amz-target"]).endsWith(".ListTables")) {
        response.setHeader("content-type", "application/x-amz-json-1.0");
        response.end('{"TableNames":[]}');
        return;
      }
      writes++;
      if (failure === "connection loss") response.destroy();
      else {
        response.writeHead(500, {
          "content-type": "application/x-amz-json-1.0",
        });
        response.end(
          '{"__type":"InternalServerError","message":"Response lost after commit"}',
        );
      }
    });
    const driver = new DynamoDBDriver({
      id: "retry-ddb",
      name: "Retry DynamoDB",
      type: "dynamodb",
      endpoint: server.endpoint,
      awsRegion: "us-east-1",
      awsAccessKeyId: "test",
      awsSecretAccessKey: "test",
    });
    try {
      await driver.connect();
      await expect(
        driver.query(
          JSON.stringify({
            TableName: "records",
            Key: { id: { S: "selected" } },
            UpdateExpression: "ADD #count :one",
            ExpressionAttributeNames: { "#count": "count" },
            ExpressionAttributeValues: { ":one": { N: "1" } },
          }),
        ),
      ).rejects.toThrow();
      expect(writes).toBe(1);
    } finally {
      await driver.disconnect();
      await server.close();
    }
  });
});
