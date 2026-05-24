import { beforeEach, describe, expect, it, vi } from "vitest";

describe("QueryPanelController", () => {
  let showWarningMessage: ReturnType<typeof vi.fn>;
  let exportQueryResultsAsCsv: ReturnType<typeof vi.fn>;
  let exportQueryResultsAsJson: ReturnType<typeof vi.fn>;
  let formatQueryResult: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    vi.resetModules();

    showWarningMessage = vi.fn();
    exportQueryResultsAsCsv = vi.fn(async () => undefined);
    exportQueryResultsAsJson = vi.fn(async () => undefined);
    formatQueryResult = vi.fn(
      (result: {
        columns: string[];
        rows: Record<string, unknown>[];
        columnMeta?: unknown[];
        rowCount?: number;
        executionTimeMs?: number;
      }) => ({
        columns: result.columns,
        columnMeta: result.columnMeta ?? [],
        rows: result.rows,
        rowCount: result.rowCount ?? result.rows.length,
        executionTimeMs: result.executionTimeMs ?? 0,
      }),
    );

    vi.doMock("vscode", () => ({
      window: {
        showWarningMessage,
      },
      env: {
        clipboard: {
          readText: vi.fn(async () => ""),
        },
      },
    }));

    vi.doMock("../../src/extension/utils/exportService", () => ({
      exportQueryResultsAsCsv,
      exportQueryResultsAsJson,
    }));

    vi.doMock("../../src/extension/utils/queryResultFormatting", () => ({
      formatQueryResult,
    }));
  });

  it("keeps explicit, active, then initial connection precedence", async () => {
    const query = vi.fn(async () => ({
      columns: ["id"],
      rows: [{ id: 1 }],
      columnMeta: [],
      rowCount: 1,
      executionTimeMs: 5,
    }));
    const addToHistory = vi.fn(async () => undefined);
    const addBookmark = vi.fn(async () => undefined);
    const getSchemaAsync = vi.fn(async () => []);
    const connectTo = vi.fn(async () => undefined);
    const isConnected = vi.fn(
      (connectionId: string) => connectionId !== "initial",
    );
    let activeConnectionId = "active";

    const connectionManager = {
      isConnected,
      connectTo,
      addToHistory,
      addBookmark,
      getDriver: vi.fn((connectionId: string) =>
        connectionId === "override" || connectionId === "active"
          ? { query }
          : undefined,
      ),
      getQueryRowLimit: vi.fn(() => 100),
      getSchemaAsync,
    };

    const view = {
      getActiveConnectionId: vi.fn(() => activeConnectionId),
      getInitialConnectionId: vi.fn(() => "initial"),
      getLastQueryResult: vi.fn(() => null),
      postMessage: vi.fn(),
      setActiveConnectionId: vi.fn(),
      setLastQueryResult: vi.fn(),
      syncTitle: vi.fn(),
    };

    const { QueryPanelController } = await import(
      "../../src/extension/panels/queryPanelController"
    );
    const controller = new QueryPanelController(
      connectionManager as never,
      view,
    );

    await controller.handleMessage({
      type: "executeQuery",
      payload: { queryText: "select 1", connectionId: "override" },
    });

    expect(isConnected).toHaveBeenNthCalledWith(1, "override");
    expect(addToHistory).toHaveBeenNthCalledWith(1, "override", "select 1");
    expect(query).toHaveBeenNthCalledWith(1, "select 1");

    await controller.handleMessage({
      type: "addBookmark",
      payload: { queryText: "select 2" },
    });

    expect(addBookmark).toHaveBeenCalledWith("active", "select 2");

    activeConnectionId = "";

    await controller.handleMessage({
      type: "getSchema",
      payload: {},
    });

    expect(isConnected).toHaveBeenLastCalledWith("initial");
    expect(connectTo).not.toHaveBeenCalledWith("initial");
    expect(getSchemaAsync).not.toHaveBeenCalled();
    expect(view.postMessage).toHaveBeenCalledWith({
      type: "schema",
      payload: { connectionId: "initial", schema: [] },
    });
  });

  it("warns and skips both exports when cached results are empty", async () => {
    const connectionManager = {
      isConnected: vi.fn(() => true),
      connectTo: vi.fn(async () => undefined),
      addToHistory: vi.fn(async () => undefined),
      addBookmark: vi.fn(async () => undefined),
      getDriver: vi.fn(),
      getQueryRowLimit: vi.fn(() => 100),
      getSchemaAsync: vi.fn(async () => []),
    };

    const view = {
      getActiveConnectionId: vi.fn(() => "active"),
      getInitialConnectionId: vi.fn(() => "initial"),
      getLastQueryResult: vi.fn(() => ({ columns: [], rows: [] })),
      postMessage: vi.fn(),
      setActiveConnectionId: vi.fn(),
      setLastQueryResult: vi.fn(),
      syncTitle: vi.fn(),
    };

    const { QueryPanelController } = await import(
      "../../src/extension/panels/queryPanelController"
    );
    const controller = new QueryPanelController(
      connectionManager as never,
      view,
    );

    await controller.handleMessage({ type: "exportResultsCSV" });
    await controller.handleMessage({ type: "exportResultsJSON" });

    expect(showWarningMessage).toHaveBeenCalledTimes(2);
    expect(showWarningMessage).toHaveBeenCalledWith(
      "[RapiDB] No query results to export.",
    );
    expect(exportQueryResultsAsCsv).not.toHaveBeenCalled();
    expect(exportQueryResultsAsJson).not.toHaveBeenCalled();
  });

  it("applies SQL hard cap before driver.query when configured row limit exceeds safety policy", async () => {
    const query = vi.fn(async () => ({
      columns: ["id"],
      rows: [{ id: 1 }],
      columnMeta: [],
      rowCount: 1,
      executionTimeMs: 2,
    }));
    const connectionManager = {
      getConnection: vi.fn(() => ({
        id: "active",
        name: "Primary",
        type: "pg",
      })),
      isConnected: vi.fn(() => true),
      connectTo: vi.fn(async () => undefined),
      addToHistory: vi.fn(async () => undefined),
      addBookmark: vi.fn(async () => undefined),
      getDriver: vi.fn(() => ({ query })),
      getQueryRowLimit: vi.fn(() => 50_000),
      getSchemaAsync: vi.fn(async () => []),
    };

    const view = {
      getActiveConnectionId: vi.fn(() => "active"),
      getInitialConnectionId: vi.fn(() => "initial"),
      getLastQueryResult: vi.fn(() => null),
      postMessage: vi.fn(),
      setActiveConnectionId: vi.fn(),
      setLastQueryResult: vi.fn(),
      syncTitle: vi.fn(),
    };

    const { QueryPanelController } = await import(
      "../../src/extension/panels/queryPanelController"
    );
    const controller = new QueryPanelController(
      connectionManager as never,
      view,
    );

    await controller.handleMessage({
      type: "executeQuery",
      payload: { queryText: "select * from users" },
    });

    expect(query).toHaveBeenCalledWith(
      "SELECT * FROM (select * from users) AS rapidb_query_cap LIMIT 10001",
    );
    expect(formatQueryResult).toHaveBeenCalledWith(
      expect.objectContaining({ rows: [{ id: 1 }] }),
      10000,
    );
  });

  it("does not rewrite WITH queries to avoid unsafe hard-cap wrapping", async () => {
    const query = vi.fn(async () => ({
      columns: ["id"],
      rows: [{ id: 1 }],
      columnMeta: [],
      rowCount: 1,
      executionTimeMs: 2,
    }));
    const connectionManager = {
      getConnection: vi.fn(() => ({
        id: "active",
        name: "Primary",
        type: "pg",
      })),
      isConnected: vi.fn(() => true),
      connectTo: vi.fn(async () => undefined),
      addToHistory: vi.fn(async () => undefined),
      addBookmark: vi.fn(async () => undefined),
      getDriver: vi.fn(() => ({ query })),
      getQueryRowLimit: vi.fn(() => 50_000),
      getSchemaAsync: vi.fn(async () => []),
    };

    const view = {
      getActiveConnectionId: vi.fn(() => "active"),
      getInitialConnectionId: vi.fn(() => "initial"),
      getLastQueryResult: vi.fn(() => null),
      postMessage: vi.fn(),
      setActiveConnectionId: vi.fn(),
      setLastQueryResult: vi.fn(),
      syncTitle: vi.fn(),
    };

    const { QueryPanelController } = await import(
      "../../src/extension/panels/queryPanelController"
    );
    const controller = new QueryPanelController(
      connectionManager as never,
      view,
    );

    await controller.handleMessage({
      type: "executeQuery",
      payload: {
        queryText: "with src as (select * from users) select * from src",
      },
    });

    expect(query).toHaveBeenCalledWith(
      "with src as (select * from users) select * from src",
    );
    expect(formatQueryResult).toHaveBeenCalledWith(
      expect.objectContaining({ rows: [{ id: 1 }] }),
      10000,
    );
  });

  it("applies SQL hard cap when a SELECT query is prefixed with SQL comments", async () => {
    const query = vi.fn(async () => ({
      columns: ["id"],
      rows: [{ id: 1 }],
      columnMeta: [],
      rowCount: 1,
      executionTimeMs: 2,
    }));
    const connectionManager = {
      getConnection: vi.fn(() => ({
        id: "active",
        name: "Primary",
        type: "pg",
      })),
      isConnected: vi.fn(() => true),
      connectTo: vi.fn(async () => undefined),
      addToHistory: vi.fn(async () => undefined),
      addBookmark: vi.fn(async () => undefined),
      getDriver: vi.fn(() => ({ query })),
      getQueryRowLimit: vi.fn(() => 50_000),
      getSchemaAsync: vi.fn(async () => []),
    };

    const view = {
      getActiveConnectionId: vi.fn(() => "active"),
      getInitialConnectionId: vi.fn(() => "initial"),
      getLastQueryResult: vi.fn(() => null),
      postMessage: vi.fn(),
      setActiveConnectionId: vi.fn(),
      setLastQueryResult: vi.fn(),
      syncTitle: vi.fn(),
    };

    const { QueryPanelController } = await import(
      "../../src/extension/panels/queryPanelController"
    );
    const controller = new QueryPanelController(
      connectionManager as never,
      view,
    );

    await controller.handleMessage({
      type: "executeQuery",
      payload: {
        queryText: "/* leading block */ -- line comment\n select * from users",
      },
    });

    expect(query).toHaveBeenCalledWith(
      "SELECT * FROM (/* leading block */ -- line comment\n select * from users) AS rapidb_query_cap LIMIT 10001",
    );
    expect(formatQueryResult).toHaveBeenCalledWith(
      expect.objectContaining({ rows: [{ id: 1 }] }),
      10000,
    );
  });

  it("pushes merged cached schema only for the active or initial connection when schema loads", async () => {
    const getSchema = vi.fn((connectionId: string) => [
      {
        database: "app_db",
        schema: connectionId === "conn-1" ? "public" : "audit",
        object: connectionId === "conn-1" ? "users" : "events",
        columns: [],
      },
    ]);
    const connectionManager = {
      getSchema,
    };

    const view = {
      getActiveConnectionId: vi.fn(() => "conn-2"),
      getInitialConnectionId: vi.fn(() => "conn-1"),
      getLastQueryResult: vi.fn(() => null),
      postMessage: vi.fn(),
      setActiveConnectionId: vi.fn(),
      setLastQueryResult: vi.fn(),
      syncTitle: vi.fn(),
    };

    const { QueryPanelController } = await import(
      "../../src/extension/panels/queryPanelController"
    );
    const controller = new QueryPanelController(
      connectionManager as never,
      view,
    );

    await controller.handleSchemaLoaded("conn-1");

    expect(getSchema).toHaveBeenCalledWith("conn-1");
    expect(view.postMessage).toHaveBeenCalledWith({
      type: "schema",
      payload: {
        connectionId: "conn-1",
        schema: [
          {
            database: "app_db",
            schema: "public",
            object: "users",
            columns: [],
          },
        ],
      },
    });

    view.postMessage.mockClear();

    await controller.handleSchemaLoaded("conn-3");

    expect(getSchema).toHaveBeenCalledTimes(1);
    expect(view.postMessage).not.toHaveBeenCalled();
  });

  it("includes driver-owned editor presentation in pushed connection metadata", async () => {
    const connectionManager = {
      getConnections: vi.fn(() => [
        { id: "conn-1", name: "Primary", type: "pg" },
        { id: "conn-2", name: "Mongo", type: "mongodb" },
        { id: "conn-3", name: "Redis", type: "redis" },
      ]),
      getQueryEditorPresentation: vi.fn((connectionId: string) => {
        switch (connectionId) {
          case "conn-1":
            return {
              formatOnOpen: true,
              editorLanguage: "sql" as const,
              sqlDialect: "postgresql" as const,
            };
          case "conn-2":
            return {
              formatOnOpen: false,
              editorLanguage: "javascript" as const,
            };
          case "conn-3":
            return {
              formatOnOpen: false,
              editorLanguage: "plaintext" as const,
            };
          default:
            return undefined;
        }
      }),
    };

    const view = {
      getActiveConnectionId: vi.fn(() => "conn-1"),
      getInitialConnectionId: vi.fn(() => "conn-1"),
      getLastQueryResult: vi.fn(() => null),
      postMessage: vi.fn(),
      setActiveConnectionId: vi.fn(),
      setLastQueryResult: vi.fn(),
      syncTitle: vi.fn(),
    };

    const { QueryPanelController } = await import(
      "../../src/extension/panels/queryPanelController"
    );
    const controller = new QueryPanelController(
      connectionManager as never,
      view,
    );

    await controller.handleMessage({ type: "getConnections" });

    expect(connectionManager.getQueryEditorPresentation).toHaveBeenCalledTimes(
      3,
    );
    expect(view.postMessage).toHaveBeenCalledWith({
      type: "connections",
      payload: [
        {
          id: "conn-1",
          name: "Primary",
          type: "pg",
          editorPresentation: {
            formatOnOpen: true,
            editorLanguage: "sql",
            sqlDialect: "postgresql",
          },
        },
        {
          id: "conn-2",
          name: "Mongo",
          type: "mongodb",
          editorPresentation: {
            formatOnOpen: false,
            editorLanguage: "javascript",
          },
        },
        {
          id: "conn-3",
          name: "Redis",
          type: "redis",
          editorPresentation: {
            formatOnOpen: false,
            editorLanguage: "plaintext",
          },
        },
      ],
    });
  });

  it("blocks non-read queries on readonly connections before connect and history", async () => {
    const query = vi.fn(async () => ({
      columns: ["id"],
      rows: [{ id: 1 }],
      rowCount: 1,
      executionTimeMs: 5,
    }));
    const connectionManager = {
      getConnection: vi.fn(() => ({
        id: "active",
        name: "Readonly",
        type: "pg",
        readOnly: true,
      })),
      getDriverCapabilities: vi.fn(() => ({
        readOnlyQueryGuard: (queryText: string) =>
          /^\s*select\b/i.test(queryText)
            ? { allowed: true as const }
            : {
                allowed: false as const,
                reason:
                  "[RapiDB] Read-only SQL connections allow only read-only queries.",
              },
      })),
      isConnected: vi.fn(() => false),
      connectTo: vi.fn(async () => undefined),
      addToHistory: vi.fn(async () => undefined),
      addBookmark: vi.fn(async () => undefined),
      getDriver: vi.fn(() => ({ query })),
      getQueryRowLimit: vi.fn(() => 100),
      getSchemaAsync: vi.fn(async () => []),
    };

    const view = {
      getActiveConnectionId: vi.fn(() => "active"),
      getInitialConnectionId: vi.fn(() => "initial"),
      getLastQueryResult: vi.fn(() => null),
      postMessage: vi.fn(),
      setActiveConnectionId: vi.fn(),
      setLastQueryResult: vi.fn(),
      syncTitle: vi.fn(),
    };

    const { QueryPanelController } = await import(
      "../../src/extension/panels/queryPanelController"
    );
    const controller = new QueryPanelController(
      connectionManager as never,
      view,
    );

    await controller.handleMessage({
      type: "executeQuery",
      payload: { queryText: "update users set name = 'Alice'" },
    });

    expect(connectionManager.isConnected).not.toHaveBeenCalled();
    expect(connectionManager.connectTo).not.toHaveBeenCalled();
    expect(connectionManager.addToHistory).not.toHaveBeenCalled();
    expect(query).not.toHaveBeenCalled();
    expect(view.postMessage).toHaveBeenCalledWith({
      type: "queryResult",
      payload: {
        columns: [],
        columnMeta: [],
        rows: [],
        rowCount: 0,
        executionTimeMs: 0,
        error:
          "[RapiDB] Read-only SQL connections allow only read-only queries.",
      },
    });
  });

  it("posts a query error instead of silently returning when the driver is unavailable", async () => {
    const addToHistory = vi.fn(async () => undefined);
    const connectionManager = {
      isConnected: vi.fn(() => true),
      connectTo: vi.fn(async () => undefined),
      addToHistory,
      addBookmark: vi.fn(async () => undefined),
      getDriver: vi.fn(() => undefined),
      getQueryRowLimit: vi.fn(() => 100),
      getSchemaAsync: vi.fn(async () => []),
    };

    const view = {
      getActiveConnectionId: vi.fn(() => "active"),
      getInitialConnectionId: vi.fn(() => "initial"),
      getLastQueryResult: vi.fn(() => null),
      postMessage: vi.fn(),
      setActiveConnectionId: vi.fn(),
      setLastQueryResult: vi.fn(),
      syncTitle: vi.fn(),
    };

    const { QueryPanelController } = await import(
      "../../src/extension/panels/queryPanelController"
    );
    const controller = new QueryPanelController(
      connectionManager as never,
      view,
    );

    await controller.handleMessage({
      type: "executeQuery",
      payload: { queryText: "select 1", connectionId: "active" },
    });

    expect(addToHistory).not.toHaveBeenCalled();
    expect(view.postMessage).toHaveBeenCalledWith({
      type: "queryResult",
      payload: {
        columns: [],
        columnMeta: [],
        rows: [],
        rowCount: 0,
        executionTimeMs: 0,
        error:
          "[RapiDB] Cannot execute query: driver is unavailable for active.",
      },
    });
  });

  it("suppresses stale schema payloads when a newer schema request completes first", async () => {
    let resolveFirst: ((value: unknown[]) => void) | undefined;
    let resolveSecond: ((value: unknown[]) => void) | undefined;

    const getSchemaAsync = vi.fn((connectionId: string) => {
      if (connectionId === "conn-1") {
        return new Promise<unknown[]>((resolve) => {
          resolveFirst = resolve;
        });
      }
      return new Promise<unknown[]>((resolve) => {
        resolveSecond = resolve;
      });
    });

    const connectionManager = {
      isConnected: vi.fn(() => true),
      getSchemaAsync,
    };

    const view = {
      getActiveConnectionId: vi.fn(() => "conn-1"),
      getInitialConnectionId: vi.fn(() => "conn-1"),
      getLastQueryResult: vi.fn(() => null),
      postMessage: vi.fn(),
      setActiveConnectionId: vi.fn(),
      setLastQueryResult: vi.fn(),
      syncTitle: vi.fn(),
    };

    const { QueryPanelController } = await import(
      "../../src/extension/panels/queryPanelController"
    );
    const controller = new QueryPanelController(
      connectionManager as never,
      view,
    );

    const first = controller.handleMessage({
      type: "getSchema",
      payload: { connectionId: "conn-1" },
    });
    const second = controller.handleMessage({
      type: "getSchema",
      payload: { connectionId: "conn-2" },
    });

    resolveSecond?.([{ object: "newer" }]);
    await second;

    resolveFirst?.([{ object: "stale" }]);
    await first;

    expect(view.postMessage).toHaveBeenCalledTimes(1);
    expect(view.postMessage).toHaveBeenCalledWith({
      type: "schema",
      payload: { connectionId: "conn-2", schema: [{ object: "newer" }] },
    });
  });

  it("does not execute or persist stale query after a newer request supersedes it", async () => {
    let connected = false;
    let connectAttempt = 0;
    let resolveFirstConnect: (() => void) | undefined;
    const connectTo = vi.fn(async () => {
      connectAttempt += 1;
      if (connectAttempt > 1) {
        connected = true;
        return;
      }
      await new Promise<void>((resolve) => {
        resolveFirstConnect = () => {
          connected = true;
          resolve();
        };
      });
    });

    const query = vi.fn(async (queryText: string) => ({
      columns: ["q"],
      rows: [{ q: queryText }],
      columnMeta: [],
      rowCount: 1,
      executionTimeMs: 1,
    }));
    const addToHistory = vi.fn(async () => undefined);

    const connectionManager = {
      getConnection: vi.fn(() => ({
        id: "active",
        name: "Writable",
        type: "pg",
        readOnly: false,
      })),
      getDriverCapabilities: vi.fn(() => ({})),
      isConnected: vi.fn(() => connected),
      connectTo,
      addToHistory,
      addBookmark: vi.fn(async () => undefined),
      getDriver: vi.fn(() => ({ query })),
      getQueryRowLimit: vi.fn(() => 100),
      getSchemaAsync: vi.fn(async () => []),
    };

    const view = {
      getActiveConnectionId: vi.fn(() => "active"),
      getInitialConnectionId: vi.fn(() => "initial"),
      getLastQueryResult: vi.fn(() => null),
      postMessage: vi.fn(),
      setActiveConnectionId: vi.fn(),
      setLastQueryResult: vi.fn(),
      syncTitle: vi.fn(),
    };

    const { QueryPanelController } = await import(
      "../../src/extension/panels/queryPanelController"
    );
    const controller = new QueryPanelController(
      connectionManager as never,
      view,
    );

    const first = controller.handleMessage({
      type: "executeQuery",
      payload: { queryText: "select stale" },
    });

    await Promise.resolve();

    const second = controller.handleMessage({
      type: "executeQuery",
      payload: { queryText: "select fresh" },
    });

    resolveFirstConnect?.();

    await Promise.all([first, second]);

    expect(query).toHaveBeenCalledTimes(1);
    expect(query).toHaveBeenCalledWith(
      "SELECT * FROM (select fresh) AS rapidb_query_cap LIMIT 101",
    );
    expect(addToHistory).toHaveBeenCalledTimes(1);
    expect(addToHistory).toHaveBeenCalledWith("active", "select fresh");
  });

  it("actively cancels in-flight query execution when superseded", async () => {
    let resolveFirstQuery: ((value: unknown) => void) | undefined;
    const cancelCurrentOperation = vi.fn(async () => undefined);
    const query = vi
      .fn()
      .mockImplementationOnce(
        () =>
          new Promise((resolve) => {
            resolveFirstQuery = resolve;
          }),
      )
      .mockImplementationOnce(async (queryText: string) => ({
        columns: ["q"],
        rows: [{ q: queryText }],
        columnMeta: [],
        rowCount: 1,
        executionTimeMs: 1,
      }));

    const connectionManager = {
      getConnection: vi.fn(() => ({
        id: "active",
        name: "Writable",
        type: "pg",
        readOnly: false,
      })),
      getDriverCapabilities: vi.fn(() => ({})),
      isConnected: vi.fn(() => true),
      connectTo: vi.fn(async () => undefined),
      addToHistory: vi.fn(async () => undefined),
      addBookmark: vi.fn(async () => undefined),
      getDriver: vi.fn(() => ({ query, cancelCurrentOperation })),
      getQueryRowLimit: vi.fn(() => 100),
      getSchemaAsync: vi.fn(async () => []),
    };

    const view = {
      getActiveConnectionId: vi.fn(() => "active"),
      getInitialConnectionId: vi.fn(() => "initial"),
      getLastQueryResult: vi.fn(() => null),
      postMessage: vi.fn(),
      setActiveConnectionId: vi.fn(),
      setLastQueryResult: vi.fn(),
      syncTitle: vi.fn(),
    };

    const { QueryPanelController } = await import(
      "../../src/extension/panels/queryPanelController"
    );
    const controller = new QueryPanelController(
      connectionManager as never,
      view,
    );

    const first = controller.handleMessage({
      type: "executeQuery",
      payload: { queryText: "select slow" },
    });

    await Promise.resolve();

    const second = controller.handleMessage({
      type: "executeQuery",
      payload: { queryText: "select fresh" },
    });

    resolveFirstQuery?.({
      columns: ["q"],
      rows: [{ q: "select slow" }],
      columnMeta: [],
      rowCount: 1,
      executionTimeMs: 1,
    });

    await Promise.all([first, second]);

    expect(cancelCurrentOperation).toHaveBeenCalledTimes(1);
    expect(cancelCurrentOperation).toHaveBeenCalledWith(
      expect.objectContaining({
        reason: "superseded",
        operationName: "query",
        connectionId: "active",
        requestToken: 1,
        supersededByRequestToken: 2,
      }),
    );
  });

  it("rejects superseded execution when cancellation is unsupported", async () => {
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
    let resolveFirstQuery: ((value: unknown) => void) | undefined;
    const query = vi
      .fn()
      .mockImplementationOnce(
        () =>
          new Promise((resolve) => {
            resolveFirstQuery = resolve;
          }),
      )
      .mockImplementationOnce(async (queryText: string) => ({
        columns: ["q"],
        rows: [{ q: queryText }],
        columnMeta: [],
        rowCount: 1,
        executionTimeMs: 1,
      }));

    const connectionManager = {
      getConnection: vi.fn(() => ({
        id: "active",
        name: "Writable",
        type: "pg",
        readOnly: false,
      })),
      getDriverCapabilities: vi.fn(() => ({})),
      isConnected: vi.fn(() => true),
      connectTo: vi.fn(async () => undefined),
      addToHistory: vi.fn(async () => undefined),
      addBookmark: vi.fn(async () => undefined),
      getDriver: vi.fn(() => ({ query })),
      getQueryRowLimit: vi.fn(() => 100),
      getSchemaAsync: vi.fn(async () => []),
    };

    const view = {
      getActiveConnectionId: vi.fn(() => "active"),
      getInitialConnectionId: vi.fn(() => "initial"),
      getLastQueryResult: vi.fn(() => null),
      postMessage: vi.fn(),
      setActiveConnectionId: vi.fn(),
      setLastQueryResult: vi.fn(),
      syncTitle: vi.fn(),
    };

    const { QueryPanelController } = await import(
      "../../src/extension/panels/queryPanelController"
    );
    const controller = new QueryPanelController(
      connectionManager as never,
      view,
    );

    const first = controller.handleMessage({
      type: "executeQuery",
      payload: { queryText: "select slow" },
    });

    await Promise.resolve();

    const second = controller.handleMessage({
      type: "executeQuery",
      payload: { queryText: "select fresh" },
    });

    await second;

    resolveFirstQuery?.({
      columns: ["q"],
      rows: [{ q: "select slow" }],
      columnMeta: [],
      rowCount: 1,
      executionTimeMs: 1,
    });
    await first;

    expect(query).toHaveBeenCalledTimes(1);
    expect(view.postMessage).toHaveBeenCalledWith({
      type: "queryResult",
      payload: expect.objectContaining({
        error:
          "[RapiDB] Cannot execute query while a previous query is still running for this connection.",
      }),
    });

    expect(warnSpy).toHaveBeenCalledTimes(1);
    expect(warnSpy.mock.calls[0]?.[0]).toContain(
      "Query cancellation is not supported",
    );

    warnSpy.mockRestore();
  });

  it("rejects superseded execution when cancellation misses deadline", async () => {
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    let resolveFirstQuery: ((value: unknown) => void) | undefined;
    const cancelCurrentOperation = vi.fn(
      () => new Promise<void>(() => undefined),
    );
    const query = vi
      .fn()
      .mockImplementationOnce(
        () =>
          new Promise((resolve) => {
            resolveFirstQuery = resolve;
          }),
      )
      .mockImplementationOnce(async (queryText: string) => ({
        columns: ["q"],
        rows: [{ q: queryText }],
        columnMeta: [],
        rowCount: 1,
        executionTimeMs: 1,
      }));

    const connectionManager = {
      getConnection: vi.fn(() => ({
        id: "active",
        name: "Writable",
        type: "pg",
        readOnly: false,
      })),
      getDriverCapabilities: vi.fn(() => ({})),
      isConnected: vi.fn(() => true),
      connectTo: vi.fn(async () => undefined),
      addToHistory: vi.fn(async () => undefined),
      addBookmark: vi.fn(async () => undefined),
      getDriver: vi.fn(() => ({ query, cancelCurrentOperation })),
      getQueryRowLimit: vi.fn(() => 100),
      getSchemaAsync: vi.fn(async () => []),
    };

    const view = {
      getActiveConnectionId: vi.fn(() => "active"),
      getInitialConnectionId: vi.fn(() => "initial"),
      getLastQueryResult: vi.fn(() => null),
      postMessage: vi.fn(),
      setActiveConnectionId: vi.fn(),
      setLastQueryResult: vi.fn(),
      syncTitle: vi.fn(),
    };

    const { QueryPanelController } = await import(
      "../../src/extension/panels/queryPanelController"
    );
    const controller = new QueryPanelController(
      connectionManager as never,
      view,
    );

    const first = controller.handleMessage({
      type: "executeQuery",
      payload: { queryText: "select slow" },
    });

    await Promise.resolve();

    const second = controller.handleMessage({
      type: "executeQuery",
      payload: { queryText: "select fresh" },
    });

    await second;

    expect(query).toHaveBeenCalledTimes(1);
    expect(view.postMessage).toHaveBeenCalledWith({
      type: "queryResult",
      payload: expect.objectContaining({
        error:
          "[RapiDB] Cannot execute query while a previous query is still running for this connection.",
      }),
    });
    expect(errorSpy).toHaveBeenCalledWith(
      expect.stringContaining("Superseded query cancellation timed out"),
    );

    resolveFirstQuery?.({
      columns: ["q"],
      rows: [{ q: "select slow" }],
      columnMeta: [],
      rowCount: 1,
      executionTimeMs: 1,
    });
    await first;

    errorSpy.mockRestore();
  });

  it("rejects superseded execution when cancellation throws", async () => {
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    let resolveFirstQuery: ((value: unknown) => void) | undefined;
    const cancelCurrentOperation = vi.fn(async () => {
      throw new Error("cancel failed");
    });
    const query = vi
      .fn()
      .mockImplementationOnce(
        () =>
          new Promise((resolve) => {
            resolveFirstQuery = resolve;
          }),
      )
      .mockImplementationOnce(async (queryText: string) => ({
        columns: ["q"],
        rows: [{ q: queryText }],
        columnMeta: [],
        rowCount: 1,
        executionTimeMs: 1,
      }));

    const connectionManager = {
      getConnection: vi.fn(() => ({
        id: "active",
        name: "Writable",
        type: "pg",
        readOnly: false,
      })),
      getDriverCapabilities: vi.fn(() => ({})),
      isConnected: vi.fn(() => true),
      connectTo: vi.fn(async () => undefined),
      addToHistory: vi.fn(async () => undefined),
      addBookmark: vi.fn(async () => undefined),
      getDriver: vi.fn(() => ({ query, cancelCurrentOperation })),
      getQueryRowLimit: vi.fn(() => 100),
      getSchemaAsync: vi.fn(async () => []),
    };

    const view = {
      getActiveConnectionId: vi.fn(() => "active"),
      getInitialConnectionId: vi.fn(() => "initial"),
      getLastQueryResult: vi.fn(() => null),
      postMessage: vi.fn(),
      setActiveConnectionId: vi.fn(),
      setLastQueryResult: vi.fn(),
      syncTitle: vi.fn(),
    };

    const { QueryPanelController } = await import(
      "../../src/extension/panels/queryPanelController"
    );
    const controller = new QueryPanelController(
      connectionManager as never,
      view,
    );

    const first = controller.handleMessage({
      type: "executeQuery",
      payload: { queryText: "select slow" },
    });

    await Promise.resolve();

    const second = controller.handleMessage({
      type: "executeQuery",
      payload: { queryText: "select fresh" },
    });

    await second;

    expect(query).toHaveBeenCalledTimes(1);
    expect(view.postMessage).toHaveBeenCalledWith({
      type: "queryResult",
      payload: expect.objectContaining({
        error:
          "[RapiDB] Cannot execute query while a previous query is still running for this connection.",
      }),
    });
    expect(errorSpy).toHaveBeenCalledWith(
      "[RapiDB] Failed to cancel superseded query execution:",
      expect.any(Error),
    );

    resolveFirstQuery?.({
      columns: ["q"],
      rows: [{ q: "select slow" }],
      columnMeta: [],
      rowCount: 1,
      executionTimeMs: 1,
    });
    await first;

    errorSpy.mockRestore();
  });

  it("handles schema load rejection and posts safe empty schema", async () => {
    const schemaError = new Error("schema failed");
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    const connectionManager = {
      isConnected: vi.fn(() => true),
      getSchemaAsync: vi.fn(async () => {
        throw schemaError;
      }),
    };

    const view = {
      getActiveConnectionId: vi.fn(() => "conn-1"),
      getInitialConnectionId: vi.fn(() => "conn-1"),
      getLastQueryResult: vi.fn(() => null),
      postMessage: vi.fn(),
      setActiveConnectionId: vi.fn(),
      setLastQueryResult: vi.fn(),
      syncTitle: vi.fn(),
    };

    const { QueryPanelController } = await import(
      "../../src/extension/panels/queryPanelController"
    );
    const controller = new QueryPanelController(
      connectionManager as never,
      view,
    );

    await expect(
      controller.handleMessage({
        type: "getSchema",
        payload: { connectionId: "conn-1" },
      }),
    ).resolves.toBeUndefined();

    expect(view.postMessage).toHaveBeenCalledWith({
      type: "schema",
      payload: { connectionId: "conn-1", schema: [] },
    });

    errorSpy.mockRestore();
  });

  it("clears per-connection execution handle when a request turns stale after handle registration", async () => {
    const query = vi.fn(async () => ({
      columns: ["id"],
      rows: [{ id: 1 }],
      columnMeta: [],
      rowCount: 1,
      executionTimeMs: 1,
    }));

    const connectionManager = {
      getConnection: vi.fn(() => ({
        id: "active",
        name: "Primary",
        type: "pg",
        readOnly: false,
      })),
      getDriverCapabilities: vi.fn(() => ({})),
      isConnected: vi.fn(() => true),
      connectTo: vi.fn(async () => undefined),
      addToHistory: vi.fn(async () => undefined),
      addBookmark: vi.fn(async () => undefined),
      getDriver: vi.fn(() => ({ query })),
      getQueryRowLimit: vi.fn(() => 100),
      getSchemaAsync: vi.fn(async () => []),
    };

    const view = {
      getActiveConnectionId: vi.fn(() => "active"),
      getInitialConnectionId: vi.fn(() => "initial"),
      getLastQueryResult: vi.fn(() => null),
      postMessage: vi.fn(),
      setActiveConnectionId: vi.fn(),
      setLastQueryResult: vi.fn(),
      syncTitle: vi.fn(),
    };

    const { QueryPanelController } = await import(
      "../../src/extension/panels/queryPanelController"
    );
    const controllerInstance = new QueryPanelController(
      connectionManager as never,
      view,
    );
    const controller = controllerInstance as unknown as {
      handleMessage(message: unknown): Promise<void>;
      isCurrentQueryRequest(requestToken: number): boolean;
      activeQueryExecutions: Map<string, unknown>;
    };

    const currentCheck = vi
      .spyOn(controller, "isCurrentQueryRequest")
      .mockReturnValueOnce(true)
      .mockReturnValueOnce(true)
      .mockReturnValueOnce(false);

    await controller.handleMessage({
      type: "executeQuery",
      payload: { queryText: "select 1" },
    });

    expect(query).not.toHaveBeenCalled();
    expect(connectionManager.addToHistory).not.toHaveBeenCalled();
    expect(controller.activeQueryExecutions.size).toBe(0);

    currentCheck.mockRestore();
  });
});
