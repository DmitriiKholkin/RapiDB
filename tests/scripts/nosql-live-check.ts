import assert from "node:assert/strict";
import { setTimeout as delay } from "node:timers/promises";
import {
  CreateTableCommand,
  DeleteTableCommand,
  DescribeTableCommand,
  DynamoDBClient,
  ListTablesCommand,
} from "@aws-sdk/client-dynamodb";
import {
  DynamoDBDocumentClient,
  GetCommand,
  PutCommand,
} from "@aws-sdk/lib-dynamodb";
import { Client as ElasticsearchClient } from "@elastic/elasticsearch";
import { MongoClient, ObjectId } from "mongodb";
import { createClient as createRedisClient } from "redis";
import { DynamoDBDriver } from "../../src/extension/dbDrivers/dynamodb.ts";
import { ElasticsearchDriver } from "../../src/extension/dbDrivers/elasticsearch.ts";
import { MongoDBDriver } from "../../src/extension/dbDrivers/mongodb.ts";
import { RedisDriver } from "../../src/extension/dbDrivers/redis.ts";
import type {
  ColumnTypeMeta,
  DriverTablePageResult,
  QueryResult,
} from "../../src/extension/dbDrivers/types.ts";

const LOOPBACK_HOST = "127.0.0.1";
const REDIS_URL = `redis://:redis_pass123@${LOOPBACK_HOST}:6379`;
const MONGO_URI = `mongodb://mongo_admin:mongo_pass123@${LOOPBACK_HOST}:27017/rapidb_mongo_db?authSource=admin&directConnection=true`;
const ELASTIC_NODE = `http://${LOOPBACK_HOST}:9200`;
const DYNAMODB_ENDPOINT = `http://${LOOPBACK_HOST}:8000`;
type LiveTransport = "direct" | "ssh";

interface DriverSession<TDriver> {
  driver: TDriver;
  dispose: () => Promise<void>;
}

async function loadLiveSshManagerHarness() {
  return import("../support/liveSshManagerHarness.ts");
}

function log(message: string): void {
  console.log(`[RapiDB:nosql-live] ${message}`);
}

async function waitFor(
  label: string,
  action: () => Promise<void>,
  timeoutMs = 180_000,
  retryDelayMs = 2_000,
): Promise<void> {
  const startedAt = Date.now();
  let lastError = "No attempts were made.";

  while (Date.now() - startedAt < timeoutMs) {
    try {
      await action();
      log(`${label} became ready in ${Date.now() - startedAt}ms.`);
      return;
    } catch (error) {
      lastError = error instanceof Error ? error.message : String(error);
      await delay(retryDelayMs);
    }
  }

  throw new Error(
    `${label} did not become ready within ${timeoutMs}ms. Last error: ${lastError}`,
  );
}

function rowsFromQuery(result: QueryResult): Array<Record<string, unknown>> {
  return result.rows.map((row) =>
    Object.fromEntries(
      result.columns.map((columnName, index) => [
        columnName,
        row[`__col_${index}`],
      ]),
    ),
  );
}

function findColumn(columns: ColumnTypeMeta[], name: string): ColumnTypeMeta {
  const column = columns.find((candidate) => candidate.name === name);
  assert(column, `Expected column ${name}.`);
  return column;
}

function findRow(
  page: DriverTablePageResult,
  predicate: (row: Record<string, unknown>) => boolean,
): Record<string, unknown> {
  const row = page.rows.find(predicate);
  assert(row, "Expected matching row in page result.");
  return row;
}

function isDynamoResourceNotFound(error: unknown): boolean {
  const name =
    error && typeof error === "object" && "name" in error
      ? String((error as { name?: unknown }).name)
      : "";
  const message = error instanceof Error ? error.message : String(error);
  return (
    /ResourceNotFoundException/i.test(name) ||
    /ResourceNotFoundException/i.test(message) ||
    /non-existent table/i.test(message)
  );
}

async function waitForRedis(): Promise<void> {
  await waitFor("Redis", async () => {
    const client = createRedisClient({ url: REDIS_URL });
    client.on("error", () => undefined);
    await client.connect();
    try {
      const pong = await client.ping();
      assert.equal(pong, "PONG");
    } finally {
      await client.close();
    }
  });
}

async function waitForMongo(): Promise<void> {
  await waitFor("MongoDB", async () => {
    const client = new MongoClient(MONGO_URI);
    await client.connect();
    try {
      await client.db("admin").command({ ping: 1 });
    } finally {
      await client.close();
    }
  });
}

async function waitForElasticsearch(): Promise<void> {
  await waitFor(
    "Elasticsearch",
    async () => {
      const client = new ElasticsearchClient({ node: ELASTIC_NODE });
      await client.ping();
      await client.cluster.health({
        wait_for_status: "yellow",
        timeout: "60s",
      });
      await client.close();
    },
    300_000,
    4_000,
  );
}

function createDynamoAdminClient(): DynamoDBClient {
  return new DynamoDBClient({
    region: "us-east-1",
    endpoint: DYNAMODB_ENDPOINT,
    credentials: {
      accessKeyId: "rapidb",
      secretAccessKey: "rapidb-secret",
    },
  });
}

async function waitForDynamo(): Promise<void> {
  await waitFor("DynamoDB Local", async () => {
    const client = createDynamoAdminClient();
    try {
      await client.send(new ListTablesCommand({ Limit: 1 }));
    } finally {
      client.destroy();
    }
  });
}

async function createRedisDriverSession(
  transport: LiveTransport,
): Promise<DriverSession<RedisDriver>> {
  if (transport === "direct") {
    const driver = new RedisDriver({
      id: "redis-live",
      name: "Redis Live",
      type: "redis",
      host: LOOPBACK_HOST,
      port: 6379,
      password: "redis_pass123",
    });
    await driver.connect();
    return {
      driver,
      dispose: async () => {
        await driver.disconnect();
      },
    };
  }

  const {
    connectLiveDriverViaManager,
    disposeManagedLiveDriverSession,
    withTrustOnFirstUseSsh,
  } = await loadLiveSshManagerHarness();
  const session = await connectLiveDriverViaManager(
    withTrustOnFirstUseSsh({
      id: "redis-live-ssh",
      name: "Redis Live SSH",
      type: "redis",
      host: "redis",
      port: 6379,
      password: "redis_pass123",
    }),
  );

  return {
    driver: session.driver as RedisDriver,
    dispose: async () => {
      await disposeManagedLiveDriverSession(session);
    },
  };
}

async function createMongoDriverSession(
  transport: LiveTransport,
): Promise<DriverSession<MongoDBDriver>> {
  if (transport === "direct") {
    const driver = new MongoDBDriver({
      id: "mongo-live",
      name: "Mongo Live",
      type: "mongodb",
      connectionUri: MONGO_URI,
      database: "rapidb_mongo_db",
      directConnection: true,
    });
    await driver.connect();
    return {
      driver,
      dispose: async () => {
        await driver.disconnect();
      },
    };
  }

  const {
    connectLiveDriverViaManager,
    disposeManagedLiveDriverSession,
    withTrustOnFirstUseSsh,
  } = await loadLiveSshManagerHarness();
  const session = await connectLiveDriverViaManager(
    withTrustOnFirstUseSsh({
      id: "mongo-live-ssh",
      name: "Mongo Live SSH",
      type: "mongodb",
      connectionUri:
        "mongodb://mongo_admin:mongo_pass123@mongo:27017/rapidb_mongo_db?authSource=admin&directConnection=true",
      database: "rapidb_mongo_db",
      directConnection: true,
    }),
  );

  return {
    driver: session.driver as MongoDBDriver,
    dispose: async () => {
      await disposeManagedLiveDriverSession(session);
    },
  };
}

async function createElasticsearchDriverSession(
  transport: LiveTransport,
): Promise<DriverSession<ElasticsearchDriver>> {
  if (transport === "direct") {
    const driver = new ElasticsearchDriver({
      id: "elastic-live",
      name: "Elastic Live",
      type: "elasticsearch",
      host: LOOPBACK_HOST,
      port: 9200,
    });
    await driver.connect();
    return {
      driver,
      dispose: async () => {
        await driver.disconnect();
      },
    };
  }

  const {
    connectLiveDriverViaManager,
    disposeManagedLiveDriverSession,
    withTrustOnFirstUseSsh,
  } = await loadLiveSshManagerHarness();
  const session = await connectLiveDriverViaManager(
    withTrustOnFirstUseSsh({
      id: "elastic-live-ssh",
      name: "Elastic Live SSH",
      type: "elasticsearch",
      endpoint: "http://elasticsearch:9200",
    }),
  );

  return {
    driver: session.driver as ElasticsearchDriver,
    dispose: async () => {
      await disposeManagedLiveDriverSession(session);
    },
  };
}

async function createDynamoDriverSession(
  transport: LiveTransport,
): Promise<DriverSession<DynamoDBDriver>> {
  if (transport === "direct") {
    const driver = new DynamoDBDriver({
      id: "dynamo-live",
      name: "Dynamo Live",
      type: "dynamodb",
      awsRegion: "us-east-1",
      endpoint: DYNAMODB_ENDPOINT,
      awsAccessKeyId: "rapidb",
      awsSecretAccessKey: "rapidb-secret",
    });
    await driver.connect();
    return {
      driver,
      dispose: async () => {
        await driver.disconnect();
      },
    };
  }

  const {
    connectLiveDriverViaManager,
    disposeManagedLiveDriverSession,
    withTrustOnFirstUseSsh,
  } = await loadLiveSshManagerHarness();
  const session = await connectLiveDriverViaManager(
    withTrustOnFirstUseSsh({
      id: "dynamo-live-ssh",
      name: "Dynamo Live SSH",
      type: "dynamodb",
      awsRegion: "us-east-1",
      endpoint: "http://dynamodb:8000",
      awsAccessKeyId: "rapidb",
      awsSecretAccessKey: "rapidb-secret",
    }),
  );

  return {
    driver: session.driver as DynamoDBDriver,
    dispose: async () => {
      await disposeManagedLiveDriverSession(session);
    },
  };
}

async function verifyRedisDriver(transport: LiveTransport): Promise<void> {
  log("Seeding Redis fixtures...");
  const seedClient = createRedisClient({ url: REDIS_URL });
  seedClient.on("error", () => undefined);
  await seedClient.connect();
  try {
    await seedClient.flushDb();
    await seedClient.set("users:1", "Alice");
    await seedClient.hSet("users:2", { first: "Bob", last: "Builder" });
    await seedClient.rPush("users:3", ["draft", "published"]);
    await seedClient.sAdd("users:4", ["alpha", "beta"]);
    await seedClient.zAdd("users:5", [
      { value: "silver", score: 5 },
      { value: "gold", score: 10 },
    ]);
  } finally {
    await seedClient.close();
  }

  log(
    `Verifying RedisDriver against live Redis${transport === "ssh" ? " over SSH" : ""}...`,
  );
  const session = await createRedisDriverSession(transport);
  const driver = session.driver;
  try {
    assert.equal(driver.isConnected(), true);
    const databases = await driver.listDatabases();
    assert(databases.some((database) => database.name === "db0"));

    const objects = await driver.listObjects();
    assert.deepEqual(
      objects.map((entry) => entry.name),
      ["users"],
    );

    const columns = await driver.describeColumns("db0", "db0", "users");
    assert.equal(findColumn(columns, "key").isPrimaryKey, true);
    assert.equal(findColumn(columns, "value").category, "text");

    const page = await driver.readTablePage({
      database: "db0",
      schema: "db0",
      table: "users",
      page: 1,
      pageSize: 10,
      filters: [],
      sort: { column: "key", direction: "asc" },
      skipCount: false,
    });
    assert.equal(page.totalCount, 5);
    assert.deepEqual(
      page.rows.map((row) => row.key),
      ["users:1", "users:2", "users:3", "users:4", "users:5"],
    );
    assert.equal(
      findRow(page, (row) => row.key === "users:2").value,
      '{"first":"Bob","last":"Builder"}',
    );
    assert.equal(
      findRow(page, (row) => row.key === "users:5").value,
      '[{"value":"silver","score":5},{"value":"gold","score":10}]',
    );

    const queryResult = await driver.query("GET users:1");
    assert.equal(queryResult.rowCount, 1);
    assert.equal(queryResult.rows[0]?.__col_0, "Alice");

    await driver.insertRow({
      database: "db0",
      schema: "default",
      table: "users",
      values: { key: "users:6", value: { role: "editor" } },
    });
    await driver.updateRows({
      database: "db0",
      schema: "default",
      table: "users",
      updates: [
        {
          primaryKeys: { key: "users:1" },
          changes: { value: "Alice Updated" },
        },
      ],
    });
    await driver.deleteRows({
      database: "db0",
      schema: "default",
      table: "users",
      primaryKeyValuesList: [{ key: "users:6" }],
    });

    const verifyClient = createRedisClient({ url: REDIS_URL });
    verifyClient.on("error", () => undefined);
    await verifyClient.connect();
    try {
      assert.equal(await verifyClient.get("users:1"), "Alice Updated");
      assert.equal(await verifyClient.get("users:6"), null);
    } finally {
      await verifyClient.close();
    }
  } finally {
    await session.dispose();
  }
}

async function verifyMongoDriver(transport: LiveTransport): Promise<void> {
  log("Seeding MongoDB fixtures...");
  const mongoAlphaId = new ObjectId("507f1f77bcf86cd799439011");
  const mongoBravoId = new ObjectId("507f1f77bcf86cd799439012");
  const mongoCharlieId = new ObjectId("507f1f77bcf86cd799439013");
  const mongoAlphaHex = mongoAlphaId.toHexString();
  const mongoBravoHex = mongoBravoId.toHexString();
  const mongoCharlieHex = mongoCharlieId.toHexString();
  const client = new MongoClient(MONGO_URI);
  await client.connect();
  try {
    const db = client.db("rapidb_mongo_db");
    await db.dropDatabase();
    await db.collection("users").insertMany([
      {
        _id: mongoAlphaId,
        email: "alpha@example.com",
        active: true,
        created_at: new Date("2026-04-01T09:00:00.000Z"),
        profile: { tier: "free" },
      },
      {
        _id: mongoBravoId,
        email: "bravo@example.com",
        active: false,
        created_at: new Date("2026-04-02T10:30:00.000Z"),
        profile: { tier: "pro" },
      },
    ]);
    await db
      .collection("users")
      .createIndex({ email: 1 }, { name: "users_by_email", unique: true });
    await db.createCollection("active_users", {
      viewOn: "users",
      pipeline: [{ $match: { active: true } }],
    });
  } finally {
    await client.close();
  }

  log(
    `Verifying MongoDBDriver against live MongoDB${transport === "ssh" ? " over SSH" : ""}...`,
  );
  const session = await createMongoDriverSession(transport);
  const driver = session.driver;
  try {
    assert.equal(driver.isConnected(), true);
    const databases = await driver.listDatabases();
    assert(databases.some((database) => database.name === "rapidb_mongo_db"));

    const objects = await driver.listObjects("rapidb_mongo_db");
    assert.deepEqual(
      objects.map((entry) => [entry.name, entry.type]),
      [
        ["users", "table"],
        ["active_users", "view"],
      ],
    );

    const columns = await driver.describeColumns(
      "rapidb_mongo_db",
      "rapidb_mongo_db",
      "users",
    );
    assert.equal(findColumn(columns, "_id").isPrimaryKey, true);
    assert.equal(findColumn(columns, "created_at").category, "datetime");
    assert.equal(findColumn(columns, "profile").category, "json");

    const indexes = await driver.getIndexes(
      "rapidb_mongo_db",
      "rapidb_mongo_db",
      "users",
    );
    assert(indexes.some((index) => index.name === "users_by_email"));

    const page = await driver.readTablePage({
      database: "rapidb_mongo_db",
      schema: "rapidb_mongo_db",
      table: "users",
      page: 1,
      pageSize: 10,
      filters: [],
      sort: { column: "email", direction: "asc" },
      skipCount: false,
    });
    assert.equal(page.totalCount, 2);
    assert.deepEqual(
      page.rows.map((row) => row._id),
      [mongoAlphaHex, mongoBravoHex],
    );
    assert.equal(
      findRow(page, (row) => row._id === mongoAlphaHex).profile,
      '{"tier":"free"}',
    );

    const queryResult = await driver.query(
      "db.users.find({ active: true }).limit(5)",
    );
    const queryRows = rowsFromQuery(queryResult);
    assert.equal(queryRows.length, 1);
    assert.equal(queryRows[0]?.email, "alpha@example.com");

    await driver.insertRow({
      database: "rapidb_mongo_db",
      schema: "rapidb_mongo_db",
      table: "users",
      values: {
        _id: mongoCharlieId,
        email: "charlie@example.com",
        active: true,
        profile: { tier: "starter" },
      },
    });
    await driver.updateRows({
      database: "rapidb_mongo_db",
      schema: "rapidb_mongo_db",
      table: "users",
      updates: [
        {
          primaryKeys: { _id: mongoCharlieHex },
          changes: { active: false, email: "charlie+updated@example.com" },
        },
      ],
    });
    await driver.deleteRows({
      database: "rapidb_mongo_db",
      schema: "rapidb_mongo_db",
      table: "users",
      primaryKeyValuesList: [{ _id: mongoCharlieHex }],
    });

    const verifyClient = new MongoClient(MONGO_URI);
    await verifyClient.connect();
    try {
      const doc = await verifyClient
        .db("rapidb_mongo_db")
        .collection("users")
        .findOne({ _id: mongoCharlieId });
      assert.equal(doc, null);
    } finally {
      await verifyClient.close();
    }
  } finally {
    await session.dispose();
  }
}

async function verifyElasticsearchDriver(
  transport: LiveTransport,
): Promise<void> {
  log("Seeding Elasticsearch fixtures...");
  const client = new ElasticsearchClient({ node: ELASTIC_NODE });
  try {
    await client.indices.delete({ index: "users", ignore_unavailable: true });
    await client.indices.create({
      index: "users",
      mappings: {
        properties: {
          email: { type: "keyword" },
          active: { type: "boolean" },
          created_at: { type: "date" },
          profile: { type: "object" },
        },
      },
    });
    await client.index({
      index: "users",
      id: "user-1",
      document: {
        email: "alpha@example.com",
        active: true,
        created_at: "2026-04-01T09:00:00.000Z",
        profile: { tier: "free" },
      },
      refresh: "wait_for",
    });
    await client.index({
      index: "users",
      id: "user-2",
      document: {
        email: "bravo@example.com",
        active: false,
        created_at: "2026-04-02T10:30:00.000Z",
        profile: { tier: "pro" },
      },
      refresh: "wait_for",
    });
  } finally {
    await client.close();
  }

  log(
    `Verifying ElasticsearchDriver against live Elasticsearch${transport === "ssh" ? " over SSH" : ""}...`,
  );
  const session = await createElasticsearchDriverSession(transport);
  const driver = session.driver;
  try {
    assert.equal(driver.isConnected(), true);
    const objects = await driver.listObjects();
    assert(objects.some((entry) => entry.name === "users"));

    const columns = await driver.describeColumns("default", "indices", "users");
    assert.equal(findColumn(columns, "_id").isPrimaryKey, true);
    assert.equal(findColumn(columns, "created_at").category, "datetime");
    assert.equal(findColumn(columns, "profile").category, "json");

    const indexes = await driver.getIndexes("default", "indices", "users");
    assert(indexes.some((index) => index.name === "users_id_idx"));

    const page = await driver.readTablePage({
      database: "default",
      schema: "indices",
      table: "users",
      page: 1,
      pageSize: 10,
      filters: [],
      sort: { column: "email", direction: "asc" },
      skipCount: false,
    });
    assert.equal(page.totalCount, 2);
    assert.deepEqual(
      page.rows.map((row) => row._id),
      ["user-1", "user-2"],
    );
    assert.equal(
      findRow(page, (row) => row._id === "user-1").profile,
      '{"tier":"free"}',
    );

    const queryResult = await driver.query(
      'POST /users/_search\n{\n  "query": {\n    "term": {\n      "active": true\n    }\n  },\n  "size": 10\n}',
    );
    const queryRows = rowsFromQuery(queryResult);
    assert.equal(queryRows.length, 1);
    assert.equal(queryRows[0]?.email, "alpha@example.com");

    await driver.insertRow({
      database: "default",
      schema: "indices",
      table: "users",
      values: {
        _id: "user-3",
        email: "charlie@example.com",
        active: true,
        profile: { tier: "starter" },
      },
    });
    await driver.updateRows({
      database: "default",
      schema: "indices",
      table: "users",
      updates: [
        {
          primaryKeys: { _id: "user-3" },
          changes: { email: "charlie+updated@example.com", active: false },
        },
      ],
    });
    await driver.deleteRows({
      database: "default",
      schema: "indices",
      table: "users",
      primaryKeyValuesList: [{ _id: "user-3" }],
    });

    const verifyClient = new ElasticsearchClient({ node: ELASTIC_NODE });
    try {
      const exists = await verifyClient.exists({
        index: "users",
        id: "user-3",
      });
      assert.equal(exists, false);
    } finally {
      await verifyClient.close();
    }
  } finally {
    await session.dispose();
  }
}

async function waitForDynamoTableRemoved(
  client: DynamoDBClient,
  tableName: string,
): Promise<void> {
  await waitFor(
    `DynamoDB table deletion (${tableName})`,
    async () => {
      try {
        await client.send(new DescribeTableCommand({ TableName: tableName }));
        throw new Error(`Table ${tableName} still exists.`);
      } catch (error) {
        if (isDynamoResourceNotFound(error)) {
          return;
        }
        throw error;
      }
    },
    120_000,
    1_500,
  );
}

async function waitForDynamoTableActive(
  client: DynamoDBClient,
  tableName: string,
): Promise<void> {
  await waitFor(
    `DynamoDB table activation (${tableName})`,
    async () => {
      const response = await client.send(
        new DescribeTableCommand({ TableName: tableName }),
      );
      assert.equal(response.Table?.TableStatus, "ACTIVE");
    },
    120_000,
    1_500,
  );
}

async function verifyDynamoDriver(transport: LiveTransport): Promise<void> {
  log("Seeding DynamoDB Local fixtures...");
  const adminClient = createDynamoAdminClient();
  const documentClient = DynamoDBDocumentClient.from(adminClient, {
    marshallOptions: { removeUndefinedValues: true },
  });
  const tableName = `users-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
  try {
    try {
      await adminClient.send(new DeleteTableCommand({ TableName: tableName }));
      await waitForDynamoTableRemoved(adminClient, tableName);
    } catch (error) {
      if (!isDynamoResourceNotFound(error)) {
        throw error;
      }
    }

    await adminClient.send(
      new CreateTableCommand({
        TableName: tableName,
        AttributeDefinitions: [
          { AttributeName: "tenant_id", AttributeType: "S" },
          { AttributeName: "user_id", AttributeType: "S" },
          { AttributeName: "email", AttributeType: "S" },
        ],
        KeySchema: [
          { AttributeName: "tenant_id", KeyType: "HASH" },
          { AttributeName: "user_id", KeyType: "RANGE" },
        ],
        ProvisionedThroughput: {
          ReadCapacityUnits: 1,
          WriteCapacityUnits: 1,
        },
        GlobalSecondaryIndexes: [
          {
            IndexName: "email-index",
            KeySchema: [
              { AttributeName: "email", KeyType: "HASH" },
              { AttributeName: "user_id", KeyType: "RANGE" },
            ],
            Projection: { ProjectionType: "ALL" },
            ProvisionedThroughput: {
              ReadCapacityUnits: 1,
              WriteCapacityUnits: 1,
            },
          },
        ],
      }),
    );
    await waitForDynamoTableActive(adminClient, tableName);

    await documentClient.send(
      new PutCommand({
        TableName: tableName,
        Item: {
          tenant_id: "tenant-1",
          user_id: "user-1",
          email: "alpha@example.com",
          age: 31,
          active: true,
          profile: { tier: "pro", visits: 3 },
          tags: new Set(["alpha", "beta"]),
          history: [1, "two", true],
          payload: Buffer.from([0xde, 0xad, 0xbe, 0xef]),
        },
      }),
    );
    await documentClient.send(
      new PutCommand({
        TableName: tableName,
        Item: {
          tenant_id: "tenant-2",
          user_id: "user-2",
          email: "bravo@example.com",
          age: 28,
          active: false,
          profile: { tier: "free", visits: 1 },
          tags: new Set(["gamma"]),
          history: [2, "three", false],
          payload: Buffer.from([0xca, 0xfe, 0xba, 0xbe]),
        },
      }),
    );
  } finally {
    adminClient.destroy();
  }

  log(
    `Verifying DynamoDBDriver against live DynamoDB Local${transport === "ssh" ? " over SSH" : ""}...`,
  );
  const session = await createDynamoDriverSession(transport);
  const driver = session.driver;
  try {
    assert.equal(driver.isConnected(), true);
    const databases = await driver.listDatabases();
    assert.deepEqual(databases, [{ name: "us-east-1", schemas: [] }]);

    const objects = await driver.listObjects("us-east-1");
    assert(objects.some((object) => object.name === tableName));

    const columns = await driver.describeColumns(
      "us-east-1",
      "us-east-1",
      tableName,
    );
    assert.equal(findColumn(columns, "tenant_id").primaryKeyRole, "partition");
    assert.equal(findColumn(columns, "user_id").primaryKeyRole, "sort");
    assert.equal(findColumn(columns, "email").category, "text");
    assert.equal(findColumn(columns, "profile").category, "json");

    const indexes = await driver.getIndexes(
      "us-east-1",
      "us-east-1",
      tableName,
    );
    assert(indexes.some((index) => index.name === "email-index"));

    const page = await driver.readTablePage({
      database: "us-east-1",
      schema: "us-east-1",
      table: tableName,
      page: 1,
      pageSize: 10,
      filters: [],
      sort: { column: "user_id", direction: "asc" },
      skipCount: false,
    });
    assert.equal(page.totalCount, 2);
    assert.deepEqual(
      page.rows.map((row) => row.user_id),
      ["user-1", "user-2"],
    );
    assert.equal(
      findRow(page, (row) => row.user_id === "user-1").profile,
      '{"tier":"pro","visits":3}',
    );
    assert.equal(
      findRow(page, (row) => row.user_id === "user-1").payload,
      "0xdeadbeef",
    );

    const queryResult = await driver.query(
      JSON.stringify({
        TableName: tableName,
        FilterExpression: "#tenant = :tenant",
        ExpressionAttributeNames: {
          "#tenant": "tenant_id",
        },
        ExpressionAttributeValues: {
          ":tenant": { S: "tenant-1" },
        },
      }),
      ["Scan"],
    );
    const queryRows = rowsFromQuery(queryResult);
    assert.equal(queryRows.length, 1);
    assert.equal(queryRows[0]?.tenant_id, "tenant-1");

    await driver.insertRow({
      database: "us-east-1",
      schema: "us-east-1",
      table: tableName,
      values: {
        tenant_id: "tenant-3",
        user_id: "user-3",
        email: "charlie@example.com",
        age: 22,
      },
    });
    await driver.updateRows({
      database: "us-east-1",
      schema: "us-east-1",
      table: tableName,
      updates: [
        {
          primaryKeys: { tenant_id: "tenant-3", user_id: "user-3" },
          changes: { email: "charlie+updated@example.com", age: 23 },
        },
      ],
    });
    await driver.deleteRows({
      database: "us-east-1",
      schema: "us-east-1",
      table: tableName,
      primaryKeyValuesList: [{ tenant_id: "tenant-3", user_id: "user-3" }],
    });

    const verifyClient = createDynamoAdminClient();
    const verifyDocumentClient = DynamoDBDocumentClient.from(verifyClient, {
      marshallOptions: { removeUndefinedValues: true },
    });
    try {
      const item = await verifyDocumentClient.send(
        new GetCommand({
          TableName: tableName,
          Key: { tenant_id: "tenant-3", user_id: "user-3" },
        }),
      );
      assert.equal(item.Item, undefined);
    } finally {
      verifyClient.destroy();
    }
  } finally {
    await session.dispose();
  }
}

export async function runNoSqlLiveCheck(): Promise<void> {
  log("Waiting for NoSQL services...");
  await waitForRedis();
  await waitForMongo();
  await waitForElasticsearch();
  await waitForDynamo();

  for (const transport of ["direct", "ssh"] as const) {
    await verifyRedisDriver(transport);
    await verifyMongoDriver(transport);
    await verifyElasticsearchDriver(transport);
    await verifyDynamoDriver(transport);
  }

  log("Live NoSQL driver verification completed successfully.");
}
