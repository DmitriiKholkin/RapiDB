import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { afterEach, describe, expect, it, vi } from "vitest";
import { SQLiteDriver } from "../../../src/extension/dbDrivers/sqlite";
import { ErdGraphService } from "../../../src/extension/services/erdGraphService";
import { createProjectTempDir } from "../../runtime/tempDirectories";

const drivers: SQLiteDriver[] = [];
afterEach(async () => {
  await Promise.all(drivers.splice(0).map((driver) => driver.disconnect()));
});

describe("real SQLite ERD metadata", () => {
  it("keeps the diagram when a virtual table module is missing and resolves FK case", async () => {
    const filePath = join(await createProjectTempDir("erd-sqlite"), "erd.db");
    const setup = new DatabaseSync(filePath, { defensive: false });
    setup.exec(`
      CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
      CREATE TABLE orders (
        id INTEGER PRIMARY KEY,
        UserId INTEGER,
        FOREIGN KEY (UserId) REFERENCES Users(ID)
      );
      PRAGMA writable_schema = ON;
      INSERT INTO sqlite_master(type, name, tbl_name, rootpage, sql)
        VALUES ('table', 'vec_items', 'vec_items', 0,
          'CREATE VIRTUAL TABLE vec_items USING vec0(embedding)');
    `);
    setup.close();

    const driver = new SQLiteDriver({
      id: "erd-sqlite",
      name: "ERD SQLite",
      type: "sqlite",
      filePath,
    });
    drivers.push(driver);
    await driver.connect();
    const [schema] = await driver.listSchemas("main");
    const objects = await driver.listObjects("main", schema.name);
    expect(objects.map((object) => object.name).sort()).toEqual([
      "orders",
      "users",
      "vec_items",
    ]);

    const connectionManager = {
      getConnection: () => ({ id: "erd-sqlite", type: "sqlite" }),
      getDriver: () => driver,
      getSchemaSnapshotAsync: async () => ({
        databases: [
          {
            name: "main",
            schemas: [
              {
                name: schema.name,
                objects: objects.map((object) => ({
                  name: object.name,
                  type: object.type,
                  columns: [],
                })),
              },
            ],
          },
        ],
      }),
      onDidDisconnect: vi.fn(() => ({ dispose: () => undefined })),
      onDidRefreshSchemas: vi.fn(() => ({ dispose: () => undefined })),
    };
    const service = new ErdGraphService(connectionManager as never);
    try {
      const { graph, partial } = await service.getGraph({
        connectionId: "erd-sqlite",
        database: "main",
      });
      expect(partial).toBe(true);
      expect(graph.nodes.map((node) => node.table)).toEqual([
        "orders",
        "users",
      ]);
      expect(graph.omittedTables).toEqual([
        expect.objectContaining({
          table: "vec_items",
          error: expect.stringContaining("vec0"),
        }),
      ]);
      expect(graph.edges).toEqual([
        expect.objectContaining({
          fromTableId: JSON.stringify(["main", schema.name, "orders"]),
          toTableId: JSON.stringify(["main", schema.name, "users"]),
          fromColumn: "UserId",
          toColumn: "id",
        }),
      ]);
      expect(graph.externalReferences).toBeUndefined();
    } finally {
      service.dispose();
    }
  });
});
