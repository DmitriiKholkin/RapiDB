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
  it("treats only real rowid aliases as implicitly required and unique", async () => {
    const driver = new SQLiteDriver({
      id: "erd-rowid",
      name: "ERD SQLite rowid",
      type: "sqlite",
      filePath: ":memory:",
    });
    drivers.push(driver);
    await driver.connect();
    await driver.query(`
      CREATE TABLE Parent (Id INTEGER PRIMARY KEY);
      CREATE TABLE pair_parent (id INTEGER, part INTEGER, PRIMARY KEY (id, part));
      CREATE TABLE shared (id INTEGER PRIMARY KEY REFERENCES Parent);
      CREATE TABLE "Shared ""Quoted""" (
        "Child Id" "iNtEgEr",
        PRIMARY KEY ("child ID" DESC),
        FOREIGN KEY ("CHILD id") REFERENCES pArEnT(iD)
      );
      CREATE TABLE alias_with_unique (id INTEGER UNIQUE PRIMARY KEY REFERENCES Parent);
      CREATE TABLE inline_desc (id INTEGER PRIMARY KEY DESC REFERENCES Parent);
      CREATE TABLE int_key (id INT PRIMARY KEY REFERENCES Parent);
      CREATE TABLE quoted_non_alias (id " INTEGER " PRIMARY KEY REFERENCES Parent);
      CREATE TABLE strict_key (id INT PRIMARY KEY REFERENCES Parent) STRICT;
      CREATE TABLE no_rowid (id INTEGER PRIMARY KEY REFERENCES Parent) WITHOUT ROWID;
      CREATE TABLE ordinary_fk (pk INTEGER PRIMARY KEY, id INTEGER REFERENCES Parent);
      CREATE TABLE composite_subset (
        id INTEGER NOT NULL REFERENCES Parent, part INTEGER, PRIMARY KEY (id, part)
      );
      CREATE TABLE composite_shared (
        id INTEGER NOT NULL, part INTEGER, PRIMARY KEY (id, part),
        FOREIGN KEY (id, part) REFERENCES pair_parent
      );
      INSERT INTO Parent VALUES (1);
      INSERT INTO shared VALUES (NULL);
      INSERT INTO inline_desc VALUES (NULL), (NULL);
      CREATE TEMP TABLE shared (id INTEGER PRIMARY KEY DESC);
    `);

    // The real alias accepts NULL on INSERT but stores the generated rowid.
    expect(
      (await driver.query("SELECT id, rowid FROM main.shared")).rows,
    ).toEqual([{ __col_0: 1, __col_1: 1 }]);
    expect((await driver.query("SELECT id FROM inline_desc")).rows).toEqual([
      { __col_0: null },
      { __col_0: null },
    ]);
    expect(await driver.describeColumns("main", "main", "shared")).toEqual([
      expect.objectContaining({
        name: "id",
        nullable: true,
        sqliteRowidAlias: true,
      }),
    ]);
    expect(await driver.getIndexes("main", "main", "shared")).toEqual([]);
    expect(
      await driver.getConstraints("main", "main", "shared"),
    ).toContainEqual({
      name: "pk_shared",
      kind: "primary_key",
      columns: ["id"],
      source: "derived",
    });

    const service = new ErdGraphService({
      getConnection: () => ({ id: "erd-rowid", type: "sqlite" }),
      getDriver: () => driver,
      getSchemaSnapshotAsync: async () => ({ databases: [] }),
      onDidDisconnect: () => ({ dispose: () => undefined }),
      onDidRefreshSchemas: () => ({ dispose: () => undefined }),
    } as never);
    try {
      const { graph, partial } = await service.getGraph({
        connectionId: "erd-rowid",
        database: "main",
      });
      expect(partial).toBe(false);
      expect(graph.externalReferences).toBeUndefined();
      const cases = [
        ["shared", "id", "one-to-one", false],
        ['Shared "Quoted"', "Child Id", "one-to-one", false],
        ["alias_with_unique", "id", "one-to-one", false],
        ["inline_desc", "id", "one-to-one", true],
        ["int_key", "id", "one-to-one", true],
        ["quoted_non_alias", "id", "one-to-one", true],
        ["strict_key", "id", "one-to-one", false],
        ["no_rowid", "id", "one-to-one", false],
        ["ordinary_fk", "id", "many-to-one", true],
        ["composite_subset", "id", "many-to-one", false],
        ["composite_shared", "id", "one-to-one", true],
        ["composite_shared", "part", "one-to-one", true],
      ] as const;
      expect(graph.edges).toHaveLength(cases.length);
      for (const [table, column, cardinality, sourceNullable] of cases) {
        const node = graph.nodes.find((node) => node.table === table);
        const edge = graph.edges.find(
          (edge) => edge.fromTableId === node?.id && edge.fromColumn === column,
        );
        expect(edge, `${table}.${column}`).toMatchObject({
          cardinality,
          sourceNullable,
          toTableId: JSON.stringify([
            "main",
            "main",
            table === "composite_shared" ? "pair_parent" : "Parent",
          ]),
          toColumn: table === "composite_shared" ? column : "Id",
        });
        expect(
          node?.columns.find((entry) => entry.name === column),
        ).toMatchObject({
          nullable:
            table === "composite_shared" ? column === "part" : sourceNullable,
        });
      }
    } finally {
      service.dispose();
    }
  });

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
