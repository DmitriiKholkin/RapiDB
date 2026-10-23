import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  CreateTableCommand,
  DeleteTableCommand,
  DynamoDBClient,
  PutItemCommand,
} from "@aws-sdk/client-dynamodb";
import { MongoClient } from "mongodb";
import { describe, expect, it, vi } from "vitest";
import type { ConnectionManager } from "../../src/extension/connectionManager";
import { DynamoDBDriver } from "../../src/extension/dbDrivers/dynamodb";
import { ElasticsearchDriver } from "../../src/extension/dbDrivers/elasticsearch";
import { MongoDBDriver } from "../../src/extension/dbDrivers/mongodb";
import { MySQLDriver } from "../../src/extension/dbDrivers/mysql";
import { OracleDriver } from "../../src/extension/dbDrivers/oracle";
import { SQLiteDriver } from "../../src/extension/dbDrivers/sqlite";
import { SchemaCacheManager } from "../../src/extension/schemaCacheManager";
import { TableMutationService } from "../../src/extension/table/tableMutationService";
import { TableReadService } from "../../src/extension/table/tableReadService";
import { resolveConnectionSeed } from "../runtime/testRuntimeConfig";

vi.mock("vscode", () => ({
  EventEmitter: class {
    event = () => ({ dispose() {} });
    fire() {}
    dispose() {}
  },
}));

const suffix = () => randomUUID().replaceAll("-", "");

describe("release checklist SQLite metadata", () => {
  it("discovers attached databases, indexes and triggers through the worker", async () => {
    const dir = await mkdtemp(join(tmpdir(), "rapidb-release-"));
    const config = {
      id: "sqlite-qa",
      name: "SQLite QA",
      type: "sqlite" as const,
      filePath: join(dir, "main.db"),
    };
    const driver = new SQLiteDriver(config);
    const cache = new SchemaCacheManager({
      getConnection: () => config,
      getDriver: () => driver,
      isConnected: () => driver.isConnected(),
    });
    try {
      await driver.connect();
      await driver.query(
        "CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT)",
      );
      await driver.query("CREATE INDEX items_value ON items(value)");
      await driver.query(
        "CREATE TRIGGER items_insert AFTER INSERT ON items BEGIN UPDATE items SET value = 'seen' WHERE id = new.id; END",
      );
      await driver.query("ATTACH DATABASE ? AS extra", [join(dir, "extra.db")]);
      await driver.query("CREATE TABLE extra.other (id INTEGER)");
      const snapshot = await cache.getSchemaSnapshotAsync(config.id);
      expect(snapshot.databases.map((db) => db.name)).toContain("extra");
      expect(await driver.getIndexes("main", "main", "items")).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ name: "items_value" }),
        ]),
      );
      expect(await driver.getTriggers("main", "main", "items")).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ name: "items_insert" }),
        ]),
      );
    } finally {
      cache.dispose();
      await driver.disconnect();
      await rm(dir, { recursive: true, force: true });
    }
  });
});

describe.runIf(process.env.RAPIDB_RELEASE_QA === "1")(
  "release checklist live regressions",
  () => {
    it.runIf(Boolean(process.env.RAPIDB_RELEASE_MARIA_PORT))(
      "inserts and filters signed TINYINT(1) keys on MariaDB",
      async () => {
        const config = {
          id: "maria-qa",
          name: "Maria QA",
          type: "mysql" as const,
          host: "127.0.0.1",
          port: Number(process.env.RAPIDB_RELEASE_MARIA_PORT),
          username: "mysql_user",
          password: "mysql_pass123",
          database: "rapidb_release_qa",
        };
        const driver = new MySQLDriver(config);
        const table = `items_${suffix()}`;
        const manager = {
          getConnection: () => config,
          getDriver: () => driver,
        } as unknown as ConnectionManager;
        const reader = new TableReadService(manager);
        const service = new TableMutationService(manager, reader);
        try {
          await driver.connect();
          await driver.query(
            `CREATE TABLE ${table} (id TINYINT(1) PRIMARY KEY, label VARCHAR(40)) ENGINE=InnoDB`,
          );
          await driver.query(
            `INSERT INTO ${table} VALUES (0,'zero'),(1,'one'),(-1,'negative')`,
          );
          await service.insertRow(config.id, config.database, "", table, {
            id: "-2",
            label: "minus-two",
          });
          for (const id of ["-2", "-1"]) {
            const page = await reader.getPage(
              config.id,
              config.database,
              "",
              table,
              1,
              25,
              [{ column: "id", operator: "eq", value: id }],
            );
            expect(page.rows).toHaveLength(1);
            expect(Number(page.rows[0].id)).toBe(Number(id));
          }
        } finally {
          await driver
            .query(`DROP TABLE IF EXISTS ${table}`)
            .catch(() => undefined);
          await driver.disconnect();
        }
      },
    );

    it("loads the Oracle root catalog and the application's tables", async () => {
      const config = await resolveConnectionSeed("oracle");
      const driver = new OracleDriver(config);
      const cache = new SchemaCacheManager({
        getConnection: () => config,
        getDriver: () => driver,
        isConnected: () => driver.isConnected(),
      });
      try {
        await driver.connect();
        const snapshot = await cache.getSchemaSnapshotAsync(config.id);
        expect(
          cache.getSchemaSnapshotState(config.id, { kind: "connectionRoot" })
            .status,
        ).toBe("loaded");
        expect(snapshot.databases.length).toBeGreaterThan(0);
        const tables = snapshot.databases.flatMap((db) =>
          db.schemas.flatMap((schema) =>
            schema.objects.map((object) => object.name.toUpperCase()),
          ),
        );
        expect(tables).toContain("FIXTURE_ROWS");
      } finally {
        cache.dispose();
        await driver.disconnect();
      }
    });

    it("discovers an Elasticsearch index and reads its mapping/settings", async () => {
      const config = {
        id: "es-qa",
        name: "ES QA",
        type: "elasticsearch" as const,
        endpoint: "http://127.0.0.1:9200",
      };
      const driver = new ElasticsearchDriver(config);
      const index = `release_qa_${suffix()}`;
      const cache = new SchemaCacheManager({
        getConnection: () => config,
        getDriver: () => driver,
        isConnected: () => driver.isConnected(),
      });
      try {
        await driver.connect();
        await driver.query(
          `PUT /${index}\n{"settings":{"number_of_replicas":0},"mappings":{"properties":{"amount":{"type":"long"}}}}`,
        );
        const snapshot = await cache.getSchemaSnapshotAsync(config.id);
        expect(
          snapshot.databases.flatMap((db) =>
            db.schemas.flatMap((schema) =>
              schema.objects.map((object) => object.name),
            ),
          ),
        ).toContain(index);
        expect((await driver.query(`GET /${index}/_mapping`)).rows.length).toBe(
          1,
        );
        expect(
          (await driver.query(`GET /${index}/_settings`)).rows.length,
        ).toBe(1);
      } finally {
        cache.dispose();
        await driver
          .query(`DELETE /${index}?ignore_unavailable=true`)
          .catch(() => undefined);
        await driver.disconnect();
      }
    });

    it("excludes missing MongoDB fields from Between and accepts compact In lists", async () => {
      const config = {
        id: "mongo-qa",
        name: "Mongo QA",
        type: "mongodb" as const,
        host: "127.0.0.1",
        port: 27017,
        database: "rapidb_release_qa",
        username: "mongo_admin",
        password: "mongo_pass123",
        authSource: "admin",
        directConnection: true,
      };
      const admin = new MongoClient("mongodb://127.0.0.1:27017", {
        auth: { username: config.username, password: config.password },
        authSource: "admin",
        directConnection: true,
      });
      const driver = new MongoDBDriver(config);
      const table = `items_${suffix()}`;
      try {
        await admin.connect();
        await admin
          .db(config.database)
          .collection(table)
          .insertMany([
            { amount: 12 },
            { amount: 20 },
            { amount: 30 },
            { label: "missing" },
            { amount: null },
          ]);
        await driver.connect();
        const request = {
          database: config.database,
          schema: config.database,
          table,
          page: 1,
          pageSize: 25,
          sort: null,
          skipCount: false,
        };
        const between = await driver.readTablePage({
          ...request,
          filters: [
            { column: "amount", operator: "between", value: ["12", "30"] },
          ],
        });
        expect(between.totalCount).toBe(3);
        for (const value of ["12,30", "12, 30"]) {
          const result = await driver.readTablePage({
            ...request,
            filters: [{ column: "amount", operator: "in", value }],
          });
          expect(result.rows.map((row) => row.amount).sort()).toEqual([12, 30]);
        }
        expect(
          (await driver.query("db.adminCommand({ ping: 1 })")).rows.length,
        ).toBe(1);
        await driver.query(`db.getCollection(${JSON.stringify(table)}).drop()`);
        expect(
          await admin
            .db(config.database)
            .listCollections({ name: table })
            .toArray(),
        ).toHaveLength(0);
      } finally {
        await driver.disconnect();
        await admin
          .db(config.database)
          .collection(table)
          .drop()
          .catch(() => undefined);
        await admin.close();
      }
    });

    it("invalidates DynamoDB page/count caches when the Viewer refreshes metadata", async () => {
      const config = {
        id: "dynamo-qa",
        name: "Dynamo QA",
        type: "dynamodb" as const,
        endpoint: "http://127.0.0.1:8000",
        awsRegion: "us-east-1",
        awsAccessKeyId: "rapidb",
        awsSecretAccessKey: "rapidb-secret",
      };
      const admin = new DynamoDBClient({
        endpoint: config.endpoint,
        region: config.awsRegion,
        credentials: {
          accessKeyId: config.awsAccessKeyId,
          secretAccessKey: config.awsSecretAccessKey,
        },
      });
      const driver = new DynamoDBDriver(config);
      const table = `release_qa_${suffix()}`;
      const reader = new TableReadService({
        getConnection: () => config,
        getDriver: () => driver,
      } as unknown as ConnectionManager);
      try {
        await admin.send(
          new CreateTableCommand({
            TableName: table,
            KeySchema: [{ AttributeName: "id", KeyType: "HASH" }],
            AttributeDefinitions: [{ AttributeName: "id", AttributeType: "S" }],
            BillingMode: "PAY_PER_REQUEST",
          }),
        );
        await driver.connect();
        await admin.send(
          new PutItemCommand({ TableName: table, Item: { id: { S: "one" } } }),
        );
        expect(
          (
            await reader.getPage(
              config.id,
              "default",
              "default",
              table,
              1,
              25,
              [],
            )
          ).totalCount,
        ).toBe(1);
        await admin.send(
          new PutItemCommand({ TableName: table, Item: { id: { S: "two" } } }),
        );
        reader.clearForConnection(config.id);
        await reader.getColumns(config.id, "default", "default", table);
        const page = await reader.getPage(
          config.id,
          "default",
          "default",
          table,
          1,
          25,
          [],
        );
        expect(page.totalCount).toBe(2);
        expect(page.rows).toHaveLength(2);
      } finally {
        await driver.disconnect();
        await admin
          .send(new DeleteTableCommand({ TableName: table }))
          .catch(() => undefined);
        admin.destroy();
      }
    });
  },
);
