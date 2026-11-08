import { describe, expect, it, vi } from "vitest";
import type { ForeignKeyMeta } from "../../src/extension/dbDrivers/types";
import { ErdGraphService } from "../../src/extension/services/erdGraphService";

function noopEvent() {
  return vi.fn(() => ({ dispose: () => undefined }));
}

interface TableSpec {
  database?: string;
  schema?: string;
  table: string;
  columns?: string[];
  foreignKeys?: ForeignKeyMeta[];
  fail?: () => Error;
}

function createService(
  tables: TableSpec[],
  options: { type?: string; database?: string } = {},
) {
  const defaultDatabase = options.database ?? "app";
  const databaseOf = (spec: TableSpec) => spec.database ?? defaultDatabase;
  const schemaOf = (spec: TableSpec) => spec.schema ?? "main";
  const byKey = new Map(
    tables.map((spec) => [
      JSON.stringify([databaseOf(spec), schemaOf(spec), spec.table]),
      spec,
    ]),
  );
  const lookup = (database: string, schema: string, table: string) => {
    const spec = byKey.get(JSON.stringify([database, schema, table]));
    if (!spec) throw new Error(`unknown table ${database}.${schema}.${table}`);
    if (spec.fail) throw spec.fail();
    return spec;
  };
  const driver = {
    describeColumns: vi.fn(async (db: string, schema: string, table: string) =>
      (lookup(db, schema, table).columns ?? ["id"]).map((name) => ({
        name,
        nativeType: "INTEGER",
        isPrimaryKey: name === "id",
        isForeignKey: false,
        nullable: name !== "id",
      })),
    ),
    getForeignKeys: vi.fn(
      async (db: string, schema: string, table: string) =>
        lookup(db, schema, table).foreignKeys ?? [],
    ),
    getIndexes: vi.fn(async () => []),
  };
  const databases = [...new Set(tables.map(databaseOf))];
  const connectionManager = {
    getConnection: vi.fn(() =>
      options.type ? { id: "conn-1", type: options.type } : undefined,
    ),
    getDriver: vi.fn(() => driver),
    getSchemaSnapshotAsync: vi.fn(async () => ({
      databases: databases.map((database) => {
        const inDatabase = tables.filter(
          (spec) => databaseOf(spec) === database,
        );
        return {
          name: database,
          schemas: [...new Set(inDatabase.map(schemaOf))].map((schema) => ({
            name: schema,
            objects: inDatabase
              .filter((spec) => schemaOf(spec) === schema)
              .map((spec) => ({
                name: spec.table,
                type: "table",
                columns: [],
              })),
          })),
        };
      }),
    })),
    onDidDisconnect: noopEvent(),
    onDidRefreshSchemas: noopEvent(),
  };
  return {
    driver,
    service: new ErdGraphService(connectionManager as never),
  };
}

function fk(
  column: string,
  referencedTable: string,
  referencedColumn: string,
  referencedSchema = "main",
): ForeignKeyMeta {
  return {
    constraintName: `fk_${column}`,
    column,
    referencedSchema,
    referencedTable,
    referencedColumn,
  };
}

describe("ErdGraphService partial metadata", () => {
  it("keeps readable tables and reports tables whose metadata failed", async () => {
    const { service } = createService(
      [
        { table: "users" },
        {
          table: "orders",
          columns: ["id", "user_id"],
          foreignKeys: [fk("user_id", "users", "id")],
        },
        {
          table: "vec_items",
          fail: () => new Error("no such module: vec0"),
        },
      ],
      { type: "sqlite" },
    );
    try {
      const request = { connectionId: "conn-1", database: "app" };
      const result = await service.getGraph(request);
      expect(result.partial).toBe(true);
      expect(result.graph.nodes.map((node) => node.table)).toEqual([
        "orders",
        "users",
      ]);
      expect(result.graph.edges).toHaveLength(1);
      expect(result.graph.omittedTables).toEqual([
        {
          database: "app",
          schema: "main",
          table: "vec_items",
          error: "no such module: vec0",
        },
      ]);
      expect(result.graph.externalReferences).toBeUndefined();
    } finally {
      service.dispose();
    }
  });

  it("caches a graph whose only gap is an unreadable table and retries on reload", async () => {
    const { service, driver } = createService(
      [
        { table: "users" },
        { table: "vec_items", fail: () => new Error("no such module: vec0") },
      ],
      { type: "sqlite" },
    );
    try {
      const request = { connectionId: "conn-1", database: "app" };
      const first = await service.getGraph(request);
      expect(first).toMatchObject({ fromCache: false, partial: true });
      expect(driver.describeColumns).toHaveBeenCalledTimes(2);

      expect(await service.getGraph(request)).toEqual({
        graph: first.graph,
        fromCache: true,
        partial: true,
      });
      expect(driver.describeColumns).toHaveBeenCalledTimes(2);

      const reloaded = await service.getGraph(request, true);
      expect(reloaded).toMatchObject({ fromCache: false, partial: true });
      expect(driver.describeColumns).toHaveBeenCalledTimes(4);
    } finally {
      service.dispose();
    }
  });

  it("rejects instead of returning a partial graph when disposed during a failing table load", async () => {
    const holder: { service?: ErdGraphService } = {};
    const created = createService([
      { table: "a" },
      {
        table: "b",
        fail: () => {
          holder.service?.dispose();
          return new Error("no such module: vec0");
        },
      },
    ]);
    holder.service = created.service;
    await expect(
      created.service.getGraph({ connectionId: "conn-1", database: "app" }),
    ).rejects.toThrow();
  });

  it("does not report references to failed tables as external", async () => {
    const { service } = createService(
      [
        {
          table: "orders",
          columns: ["id", "item_id"],
          foreignKeys: [fk("item_id", "Vec_Items", "id")],
        },
        { table: "vec_items", fail: () => new Error("no such module: vec0") },
      ],
      { type: "sqlite" },
    );
    try {
      const result = await service.getGraph({
        connectionId: "conn-1",
        database: "app",
      });
      expect(result.graph.omittedTables).toHaveLength(1);
      expect(result.graph.externalReferences).toBeUndefined();
    } finally {
      service.dispose();
    }
  });

  it("still fails when no table metadata can be read", async () => {
    const { service } = createService([
      { table: "a", fail: () => new Error("connection reset") },
      { table: "b", fail: () => new Error("connection reset") },
    ]);
    try {
      await expect(
        service.getGraph({ connectionId: "conn-1", database: "app" }),
      ).rejects.toThrow("connection reset");
    } finally {
      service.dispose();
    }
  });

  it("aborts the whole graph when cancellation surfaces as a table error", async () => {
    const controller = new AbortController();
    const { service } = createService([
      { table: "a" },
      {
        table: "b",
        fail: () => {
          controller.abort();
          return new Error("query cancelled");
        },
      },
    ]);
    try {
      await expect(
        service.getGraph(
          { connectionId: "conn-1", database: "app" },
          false,
          controller.signal,
        ),
      ).rejects.toThrow();
    } finally {
      service.dispose();
    }
  });

  it("propagates AbortError from a table instead of returning a partial graph", async () => {
    const { service } = createService([
      { table: "a" },
      {
        table: "b",
        fail: () => new DOMException("Operation aborted", "AbortError"),
      },
    ]);
    try {
      await expect(
        service.getGraph({ connectionId: "conn-1", database: "app" }),
      ).rejects.toMatchObject({ name: "AbortError" });
    } finally {
      service.dispose();
    }
  });
});

describe("ErdGraphService reference resolution", () => {
  it("resolves SQLite references written in a different case", async () => {
    const { service } = createService(
      [
        { table: "users", columns: ["id"] },
        {
          table: "orders",
          columns: ["id", "UserId"],
          foreignKeys: [fk("UserId", "Users", "ID")],
        },
      ],
      { type: "sqlite" },
    );
    try {
      const { graph, partial } = await service.getGraph({
        connectionId: "conn-1",
        database: "app",
      });
      expect(partial).toBe(false);
      expect(graph.edges).toEqual([
        expect.objectContaining({
          fromTableId: JSON.stringify(["app", "main", "orders"]),
          toTableId: JSON.stringify(["app", "main", "users"]),
          fromColumn: "UserId",
          toColumn: "id",
        }),
      ]);
      expect(graph.externalReferences).toBeUndefined();
    } finally {
      service.dispose();
    }
  });

  it("keeps exact case matching for case-sensitive dialects", async () => {
    const { service } = createService(
      [
        { schema: "public", table: "users", columns: ["id"] },
        { schema: "public", table: "Users", columns: ["ID"] },
        {
          schema: "public",
          table: "orders",
          columns: ["id", "a", "b"],
          foreignKeys: [
            fk("a", "Users", "ID", "public"),
            fk("b", "USERS", "id", "public"),
          ],
        },
      ],
      { type: "pg" },
    );
    try {
      const { graph } = await service.getGraph({
        connectionId: "conn-1",
        database: "app",
        schema: "public",
      });
      expect(graph.edges).toEqual([
        expect.objectContaining({
          toTableId: JSON.stringify(["app", "public", "Users"]),
          toColumn: "ID",
        }),
      ]);
      expect(graph.externalReferences).toEqual([
        expect.objectContaining({ fromColumns: ["b"], table: "USERS" }),
      ]);
    } finally {
      service.dispose();
    }
  });

  it("reports foreign keys that point outside the diagram scope", async () => {
    const { service, driver } = createService(
      [
        {
          schema: "sales",
          table: "orders",
          columns: ["id", "user_id"],
          foreignKeys: [
            fk("user_id", "users", "id", "auth"),
            fk("user_id", "users", "id", "auth"),
          ],
        },
        { schema: "auth", table: "users" },
      ],
      { type: "pg" },
    );
    try {
      const request = {
        connectionId: "conn-1",
        database: "app",
        schema: "sales",
      };
      const result = await service.getGraph(request);
      expect(result.partial).toBe(false);
      expect(result.graph.edges).toEqual([]);
      expect(result.graph.externalReferences).toEqual([
        {
          fromTableId: JSON.stringify(["app", "sales", "orders"]),
          fromColumns: ["user_id"],
          constraintName: "fk_user_id",
          database: "app",
          schema: "auth",
          table: "users",
          columns: ["id"],
        },
      ]);
      expect(driver.describeColumns).toHaveBeenCalledTimes(1);
      expect((await service.getGraph(request)).fromCache).toBe(true);
    } finally {
      service.dispose();
    }
  });

  it("does not fold case for a non-SQLite dialect even when the match is unambiguous", async () => {
    const { service } = createService(
      [
        { schema: "public", table: "users", columns: ["id"] },
        {
          schema: "public",
          table: "orders",
          columns: ["id", "user_id"],
          foreignKeys: [fk("user_id", "Users", "id", "public")],
        },
      ],
      { type: "pg" },
    );
    try {
      const { graph } = await service.getGraph({
        connectionId: "conn-1",
        database: "app",
        schema: "public",
      });
      expect(graph.edges).toEqual([]);
      expect(graph.externalReferences).toEqual([
        expect.objectContaining({
          fromColumns: ["user_id"],
          schema: "public",
          table: "Users",
          columns: ["id"],
        }),
      ]);
    } finally {
      service.dispose();
    }
  });

  it("resolves each column of a SQLite composite key when only some differ in case", async () => {
    const { service } = createService(
      [
        { table: "users", columns: ["id", "tenant_id"] },
        {
          table: "orders",
          columns: ["id", "user_id", "tenant_id"],
          foreignKeys: [
            {
              constraintName: "fk_user",
              column: "user_id",
              referencedSchema: "main",
              referencedTable: "Users",
              referencedColumn: "id",
            },
            {
              constraintName: "fk_user",
              column: "tenant_id",
              referencedSchema: "main",
              referencedTable: "Users",
              referencedColumn: "Tenant_ID",
            },
          ],
        },
      ],
      { type: "sqlite" },
    );
    try {
      const { graph } = await service.getGraph({
        connectionId: "conn-1",
        database: "app",
      });
      const users = JSON.stringify(["app", "main", "users"]);
      expect(
        graph.edges.map((edge) => [
          edge.fromColumn,
          edge.toTableId,
          edge.toColumn,
        ]),
      ).toEqual([
        ["tenant_id", users, "tenant_id"],
        ["user_id", users, "id"],
      ]);
      expect(graph.externalReferences).toBeUndefined();
    } finally {
      service.dispose();
    }
  });

  it("reports a composite foreign key outside the diagram as one relationship", async () => {
    const compositeFk = (column: string, referencedColumn: string) => ({
      constraintName: "orders_tenant_user_fkey",
      column,
      referencedSchema: "auth",
      referencedTable: "users",
      referencedColumn,
    });
    const { service } = createService(
      [
        {
          schema: "sales",
          table: "orders",
          columns: ["id", "tenant_id", "user_id"],
          foreignKeys: [
            compositeFk("tenant_id", "tenant_id"),
            compositeFk("user_id", "id"),
            compositeFk("tenant_id", "tenant_id"),
            fk("user_id", "people", "id", "auth"),
          ],
        },
      ],
      { type: "pg" },
    );
    try {
      const { graph } = await service.getGraph({
        connectionId: "conn-1",
        database: "app",
        schema: "sales",
      });
      expect(graph.externalReferences).toEqual([
        {
          fromTableId: JSON.stringify(["app", "sales", "orders"]),
          fromColumns: ["tenant_id", "user_id"],
          constraintName: "orders_tenant_user_fkey",
          database: "app",
          schema: "auth",
          table: "users",
          columns: ["tenant_id", "id"],
        },
        expect.objectContaining({
          constraintName: "fk_user_id",
          table: "people",
          fromColumns: ["user_id"],
          columns: ["id"],
        }),
      ]);
    } finally {
      service.dispose();
    }
  });

  it("treats the MySQL referenced schema as the target database", async () => {
    const { service } = createService(
      [
        {
          database: "app",
          schema: "app",
          table: "orders",
          columns: ["id", "invoice_id"],
          foreignKeys: [fk("invoice_id", "invoices", "id", "billing")],
        },
        { database: "billing", schema: "billing", table: "invoices" },
      ],
      { type: "mysql" },
    );
    try {
      const unscoped = await service.getGraph({ connectionId: "conn-1" });
      expect(unscoped.graph.edges).toEqual([
        expect.objectContaining({
          fromTableId: JSON.stringify(["app", "app", "orders"]),
          toTableId: JSON.stringify(["billing", "billing", "invoices"]),
        }),
      ]);
      expect(unscoped.graph.externalReferences).toBeUndefined();

      const scoped = await service.getGraph({
        connectionId: "conn-1",
        database: "app",
      });
      expect(scoped.graph.edges).toEqual([]);
      expect(scoped.graph.externalReferences).toEqual([
        expect.objectContaining({
          database: "billing",
          schema: "billing",
          table: "invoices",
        }),
      ]);
    } finally {
      service.dispose();
    }
  });
});
