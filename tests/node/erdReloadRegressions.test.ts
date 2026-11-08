import { describe, expect, it, vi } from "vitest";
import { ErdGraphService } from "../../src/extension/services/erdGraphService";

function createEventSource<T>() {
  const listeners = new Set<(value: T) => void>();

  return {
    event: vi.fn((listener: (value: T) => void) => {
      listeners.add(listener);
      return {
        dispose: () => {
          listeners.delete(listener);
        },
      };
    }),
    fire(value: T) {
      for (const listener of [...listeners]) {
        listener(value);
      }
    },
  };
}

function snapshotWith(tables: string[]) {
  return {
    databases: [
      {
        name: "app_db",
        schemas: [
          {
            name: "public",
            objects: tables.map((name) => ({
              name,
              type: "table",
              columns: [],
            })),
          },
        ],
      },
    ],
  };
}

function createManager(serverTables: { current: string[] }) {
  let snapshot = snapshotWith(serverTables.current);
  let status: "idle" | "loading" | "loaded" = "loaded";
  const refreshed = createEventSource<string | undefined>();
  const schemaState = createEventSource<string>();
  const disconnected = createEventSource<string>();
  const driver = {
    isConnected: () => true,
    listSchemas: vi.fn(async () => [{ name: "public" }]),
    listObjects: vi.fn(async () => []),
    describeColumns: vi.fn(async () => []),
    getForeignKeys: vi.fn(async () => []),
    getIndexes: vi.fn(async () => []),
  };
  const manager = {
    getDriver: vi.fn(() => driver),
    getSchemaSnapshotAsync: vi.fn(async () => snapshot),
    getSchemaSnapshot: vi.fn(() => snapshot),
    getSchemaSnapshotState: vi.fn(() => ({ status })),
    warmSchemaScope: vi.fn(() => {
      if (status !== "idle") return;
      status = "loading";
      setTimeout(() => {
        snapshot = snapshotWith(serverTables.current);
        status = "loaded";
        schemaState.fire("conn-1");
      }, 5);
    }),
    refreshSchemaCache: vi.fn((request?: { connectionId?: string }) => {
      status = "idle";
      refreshed.fire(request?.connectionId);
    }),
    onDidDisconnect: disconnected.event,
    onDidRefreshSchemas: refreshed.event,
    onDidChangeSchemaState: schemaState.event,
    driver,
    disconnected,
  };
  return manager;
}

function blockFirstDescribe(manager: ReturnType<typeof createManager>) {
  let release!: () => void;
  const released = new Promise<void>((resolve) => {
    release = resolve;
  });
  let reached!: () => void;
  const started = new Promise<void>((resolve) => {
    reached = resolve;
  });
  manager.driver.describeColumns.mockImplementationOnce(async () => {
    reached();
    await released;
    return [];
  });
  return { started, release };
}

describe("ErdGraphService reload", () => {
  it("refreshes the schema catalog on reload so tables changed by other clients appear and disappear", async () => {
    const serverTables = { current: ["users"] };
    const manager = createManager(serverTables);
    const service = new ErdGraphService(manager as never);
    const request = { connectionId: "conn-1", database: "app_db" };
    try {
      const first = await service.getGraph(request);
      expect(first.graph.nodes.map((node) => node.table)).toEqual(["users"]);

      serverTables.current = ["orders"];
      expect((await service.getGraph(request)).fromCache).toBe(true);

      const reloaded = await service.getGraph(request, true);
      expect(manager.refreshSchemaCache).toHaveBeenCalledWith({
        connectionId: "conn-1",
        reason: "manual",
      });
      expect(reloaded).toMatchObject({ fromCache: false, partial: false });
      expect(reloaded.graph.nodes.map((node) => node.table)).toEqual([
        "orders",
      ]);
      expect(await service.getGraph(request)).toMatchObject({
        fromCache: true,
        graph: reloaded.graph,
      });
    } finally {
      service.dispose();
    }
  });

  it("lets an in-flight load of another scope finish when a reload refreshes the schema", async () => {
    const serverTables = { current: ["users"] };
    const manager = createManager(serverTables);
    const service = new ErdGraphService(manager as never);
    try {
      const blocked = blockFirstDescribe(manager);
      const other = service.getGraph({
        connectionId: "conn-1",
        database: "app_db",
        schema: "public",
      });
      await blocked.started;
      serverTables.current = ["orders"];
      const reloaded = await service.getGraph(
        { connectionId: "conn-1", database: "app_db" },
        true,
      );
      expect(reloaded.graph.nodes.map((node) => node.table)).toEqual([
        "orders",
      ]);
      blocked.release();
      const result = await other;
      expect(result).toMatchObject({ fromCache: false, partial: false });
      expect(result.graph.nodes.map((node) => node.table)).toEqual(["orders"]);
    } finally {
      service.dispose();
    }
  });

  it("still rejects an in-flight load when the connection disconnects", async () => {
    const manager = createManager({ current: ["users"] });
    const service = new ErdGraphService(manager as never);
    try {
      const blocked = blockFirstDescribe(manager);
      const pending = service.getGraph({
        connectionId: "conn-1",
        database: "app_db",
      });
      await blocked.started;
      manager.disconnected.fire("conn-1");
      blocked.release();
      await expect(pending).rejects.toThrow(
        "Connection changed while the ERD was loading.",
      );
    } finally {
      service.dispose();
    }
  });
});
