import { describe, expect, it, vi } from "vitest";
import { ErdGraphService } from "../../src/extension/services/erdGraphService";

function noopEvent() {
  return vi.fn(() => ({ dispose: () => undefined }));
}

function createService(options: { billingWarmupFails: boolean }) {
  const snapshot = {
    databases: [
      {
        name: "app_db",
        schemas: [
          {
            name: "public",
            objects: [
              { name: "users", type: "table", columns: [] },
              { name: "broken", type: "table", columns: [] },
            ],
          },
          { name: "billing", objects: [] },
        ],
      },
    ],
  };
  const driver = {
    isConnected: () => true,
    listObjects: vi.fn(async () => {
      throw new Error("permission denied");
    }),
    describeColumns: vi.fn(
      async (_db: string, _schema: string, table: string) => {
        if (table === "broken") throw new Error("no such module: vec0");
        return [
          {
            name: "id",
            nativeType: "INTEGER",
            isPrimaryKey: true,
            isForeignKey: false,
            nullable: false,
          },
        ];
      },
    ),
    getForeignKeys: vi.fn(async () => []),
    getIndexes: vi.fn(async () => []),
  };
  const connectionManager = {
    getDriver: vi.fn(() => driver),
    getSchemaSnapshotAsync: vi.fn(async () => snapshot),
    getSchemaSnapshot: vi.fn(() => snapshot),
    getSchemaSnapshotState: vi.fn(
      (_connectionId: string, scope: { kind: string; schema?: string }) =>
        options.billingWarmupFails &&
        scope.kind === "schema" &&
        scope.schema === "billing"
          ? { status: "error", error: "boom" }
          : { status: "loaded" },
    ),
    warmSchemaScope: vi.fn(),
    onDidDisconnect: noopEvent(),
    onDidRefreshSchemas: noopEvent(),
    onDidChangeSchemaState: noopEvent(),
  };
  return {
    driver,
    service: new ErdGraphService(connectionManager as never),
  };
}

describe("ErdGraphService metadataIncomplete flag", () => {
  it("flags the graph and skips the cache when schema discovery fails alongside unreadable tables", async () => {
    const { service, driver } = createService({ billingWarmupFails: true });
    try {
      const request = { connectionId: "conn-1", database: "app_db" };
      const first = await service.getGraph(request);
      expect(first.partial).toBe(true);
      expect(first.graph.metadataIncomplete).toBe(true);
      expect(first.graph.omittedTables?.map((table) => table.table)).toEqual([
        "broken",
      ]);
      expect(first.graph.nodes.map((node) => node.table)).toEqual(["users"]);

      const second = await service.getGraph(request);
      expect(second).toMatchObject({ fromCache: false, partial: true });
      expect(second.graph.metadataIncomplete).toBe(true);
      expect(driver.listObjects).toHaveBeenCalledTimes(2);
    } finally {
      service.dispose();
    }
  });

  it("does not flag or skip the cache when the only gap is an unreadable table", async () => {
    const { service } = createService({ billingWarmupFails: false });
    try {
      const request = { connectionId: "conn-1", database: "app_db" };
      const first = await service.getGraph(request);
      expect(first.partial).toBe(true);
      expect(first.graph.metadataIncomplete).toBeUndefined();
      expect(first.graph.omittedTables).toHaveLength(1);

      const second = await service.getGraph(request);
      expect(second).toMatchObject({ fromCache: true, partial: true });
      expect(second.graph).toBe(first.graph);
    } finally {
      service.dispose();
    }
  });
});
