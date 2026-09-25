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
      // No `type` field so the SQL hard-cap path stays inactive — this
      // test focuses on connection-id precedence, not the rewrite logic.
      getConnection: vi.fn((connectionId: string) => ({
        id: connectionId,
        name: "Primary",
      })),
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
      getQueryEditorPresentation: vi.fn(() => undefined),
      getDriverEntityManifest: vi.fn(() => undefined),
      getDriverCapabilities: vi.fn(() => undefined),
      getSkipTableMutationPreview: vi.fn(() => false),
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
    expect(query).toHaveBeenNthCalledWith(1, "select 1", undefined, {
      requestToken: 1,
    });

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

  it.each([
    "exportResultsCSV",
    "exportResultsJSON",
  ])("%s preserves duplicate columns through default export, reorder, and sort", async (type) => {
    const cached = {
      columns: ["value", "value"],
      columnMeta: [
        { category: "integer" as const },
        { category: "text" as const },
      ],
      rows: [
        { __col_0: 2, __col_1: "a" },
        { __col_0: 1, __col_1: "c" },
        { __col_0: 1, __col_1: "b" },
      ],
    };
    const snapshot = structuredClone(cached);
    const { QueryPanelController } = await import(
      "../../src/extension/panels/queryPanelController"
    );
    const controller = new QueryPanelController({} as never, {
      getActiveConnectionId: () => "active",
      getInitialConnectionId: () => "initial",
      getLastQueryResult: () => cached,
      postMessage: vi.fn(),
      setActiveConnectionId: vi.fn(),
      setLastQueryResult: vi.fn(),
      syncTitle: vi.fn(),
    });
    const exporter =
      type === "exportResultsCSV"
        ? exportQueryResultsAsCsv
        : exportQueryResultsAsJson;
    for (const payload of [
      undefined,
      { columnOrder: ["__col_0", "__col_1"] },
    ]) {
      await controller.handleMessage({ type, payload });
      expect(exporter).toHaveBeenLastCalledWith(snapshot, {
        context: undefined,
      });
    }
    await controller.handleMessage({
      type,
      payload: { columnOrder: ["__col_1", "__col_0"] },
    });
    expect(exporter).toHaveBeenLastCalledWith(
      {
        ...cached,
        columnMeta: [cached.columnMeta[1], cached.columnMeta[0]],
        rows: [
          { __col_0: "a", __col_1: 2 },
          { __col_0: "c", __col_1: 1 },
          { __col_0: "b", __col_1: 1 },
        ],
      },
      { context: undefined },
    );
    for (const [id, desc, indices] of [
      ["__col_0", false, [1, 2, 0]],
      ["__col_1", false, [0, 2, 1]],
      ["__col_1", true, [1, 2, 0]],
    ] as const) {
      await controller.handleMessage({
        type,
        payload: { sort: [{ id, desc }] },
      });
      expect(exporter).toHaveBeenLastCalledWith(
        {
          ...cached,
          rows: indices.map((i) => cached.rows[i]),
        },
        { context: undefined },
      );
    }
    await controller.handleMessage({
      type,
      payload: {
        columnOrder: ["__col_1", "__col_0"],
        sort: [
          { id: "__col_0", desc: false },
          { id: "__col_1", desc: false },
        ],
      },
    });
    expect(exporter).toHaveBeenLastCalledWith(
      {
        ...cached,
        columnMeta: [cached.columnMeta[1], cached.columnMeta[0]],
        rows: [
          { __col_0: "b", __col_1: 1 },
          { __col_0: "c", __col_1: 1 },
          { __col_0: "a", __col_1: 2 },
        ],
      },
      { context: undefined },
    );
    await controller.handleMessage({
      type,
      payload: {
        columnOrder: ["__col_1"],
        sort: [{ id: "__col_0", desc: false }],
      },
    });
    expect(exporter).toHaveBeenLastCalledWith(
      {
        columns: ["value"],
        columnMeta: [cached.columnMeta[1]],
        rows: [{ __col_0: "c" }, { __col_0: "b" }, { __col_0: "a" }],
      },
      { context: undefined },
    );
    expect(cached).toEqual(snapshot);

    exporter.mockClear();
    for (const payload of [
      { columnOrder: ["__col_0", "__col_2"] },
      { sort: [{ id: "__col_2", desc: false }] },
    ]) {
      await controller.handleMessage({ type, payload });
    }
    expect(exporter).not.toHaveBeenCalled();
    expect(showWarningMessage).toHaveBeenCalledTimes(2);
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
      undefined,
      { requestToken: 1 },
    );
    expect(formatQueryResult).toHaveBeenCalledWith(
      expect.objectContaining({ rows: [{ id: 1 }] }),
      10000,
    );
  });

  it("rewrites MSSQL queries with TOP without wrapping in a derived table", async () => {
    const query = vi.fn(async () => ({
      columns: ["sequence_name"],
      rows: [{ sequence_name: "s1" }],
      columnMeta: [],
      rowCount: 1,
      executionTimeMs: 2,
    }));
    const connectionManager = {
      getConnection: vi.fn(() => ({
        id: "active",
        name: "Primary",
        type: "mssql",
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
        queryText:
          "SELECT s.name AS sequence_name FROM sys.sequences s ORDER BY s.name",
      },
    });

    expect(query).toHaveBeenCalledWith(
      "SELECT TOP (10001) s.name AS sequence_name FROM sys.sequences s ORDER BY s.name",
      undefined,
      { requestToken: 1 },
    );
  });

  async function executeSqlCapCase(
    type: string,
    queryText: string,
    bounded = false,
  ) {
    const query = vi.fn(async () => ({
      columns: ["id"],
      rows: [{ id: 1 }],
      columnMeta: [],
      rowCount: bounded ? 100 : 1,
      ...(bounded ? { truncated: true, affectedRows: 100 } : {}),
      executionTimeMs: 1,
    }));
    const connectionManager = {
      getConnection: vi.fn(() => ({ id: "active", name: "Primary", type })),
      isConnected: vi.fn(() => true),
      connectTo: vi.fn(),
      addToHistory: vi.fn(async () => undefined),
      getDriver: vi.fn(() => ({ query })),
      getDriverCapabilities: vi.fn(() =>
        bounded ? { boundedQueryResults: true } : undefined,
      ),
      getQueryRowLimit: vi.fn(() => 10),
      refreshSchemaCache: vi.fn(),
    };
    const view = {
      getActiveConnectionId: () => "active",
      getInitialConnectionId: () => "active",
      getLastQueryResult: () => null,
      postMessage: vi.fn(),
      setActiveConnectionId: vi.fn(),
      setLastQueryResult: vi.fn(),
      syncTitle: vi.fn(),
    };
    const { QueryPanelController } = await import(
      "../../src/extension/panels/queryPanelController"
    );
    await new QueryPanelController(
      connectionManager as never,
      view,
    ).handleMessage({
      type: "executeQuery",
      payload: { queryText, operationId: "cap-case", connectionId: "active" },
    });
    return { query, connectionManager, view };
  }

  it.each([
    ["pg", "UPDATE items SET id = id + 1 RETURNING *; SHOW ALL"],
    ["mssql", "SELECT * FROM a UNION ALL SELECT * FROM b; EXEC report"],
    ["sqlite", "UPDATE items SET id = id + 1 RETURNING *; PRAGMA user_version"],
  ])("passes original %s SQL and its collection budget to a capable driver", async (dialect, original) => {
    const actual = await vi.importActual<
      typeof import("../../src/extension/utils/queryResultFormatting")
    >("../../src/extension/utils/queryResultFormatting");
    formatQueryResult.mockImplementation(actual.formatQueryResult);
    const { query, connectionManager, view } = await executeSqlCapCase(
      dialect,
      original,
      true,
    );
    expect(query).toHaveBeenCalledWith(original, undefined, {
      requestToken: 1,
      hardCap: 11,
    });
    expect(connectionManager.addToHistory).toHaveBeenCalledWith(
      "active",
      original,
    );
    expect(view.postMessage).toHaveBeenCalledWith(
      expect.objectContaining({
        type: "queryResult",
        payload: expect.objectContaining({
          rowCount: 100,
          affectedRows: 100,
          truncated: true,
          truncatedAt: 10,
        }),
      }),
    );
  });

  it.each([
    "SHOW VARIABLES",
    "EXPLAIN SELECT 1",
    "CALL report()",
    "SELECT * FROM items FOR UPDATE",
    "SELECT * FROM items LOCK IN SHARE MODE",
    "SELECT 1; UPDATE items SET id = id + 1",
  ])("passes MySQL SQL unchanged using the production driver's capability: %s", async (original) => {
    const { MySQLDriver } = await import("../../src/extension/dbDrivers/mysql");
    const driver = new MySQLDriver({
      id: "mysql",
      name: "MySQL",
      type: "mysql",
    });
    const { query, view } = await executeSqlCapCase(
      "mysql",
      original,
      driver.getCapabilities().boundedQueryResults,
    );
    expect(query).toHaveBeenCalledWith(original, undefined, {
      requestToken: 1,
      hardCap: 11,
    });
    expect(view.setLastQueryResult).toHaveBeenCalled();
  });

  it("passes native Oracle PL/SQL unchanged using the production driver's capability", async () => {
    const { OracleDriver } = await import(
      "../../src/extension/dbDrivers/oracle"
    );
    const driver = new OracleDriver({
      id: "oracle",
      name: "Oracle",
      type: "oracle",
    });
    const original =
      "DECLARE c SYS_REFCURSOR; BEGIN OPEN c FOR SELECT q'[a;b]' FROM dual; DBMS_SQL.RETURN_RESULT(c); END;\n/\nUPDATE items SET id = id + 1";
    const { query, connectionManager, view } = await executeSqlCapCase(
      "oracle",
      original,
      driver.getCapabilities().boundedQueryResults,
    );
    expect(query).toHaveBeenCalledWith(original, undefined, {
      requestToken: 1,
      hardCap: 11,
    });
    expect(connectionManager.addToHistory).toHaveBeenCalledWith(
      "active",
      original,
    );
    expect(view.setLastQueryResult).toHaveBeenCalled();
  });

  it.each([
    "pg",
    "mysql",
    "sqlite",
    "mssql",
    "oracle",
  ])("executes a bounded standard CTE on %s, preserving original history", async (dialect) => {
    const prefix = "WITH src AS (SELECT id FROM items) ";
    const original = `${prefix}SELECT id FROM src ORDER BY id; -- comment`;
    const { query, connectionManager, view } = await executeSqlCapCase(
      dialect,
      original,
    );
    const expected =
      prefix +
      (dialect === "mssql"
        ? "SELECT TOP (11) id FROM src ORDER BY id"
        : dialect === "oracle"
          ? "SELECT * FROM (SELECT id FROM src ORDER BY id) rapidb_query_cap FETCH FIRST 11 ROWS ONLY"
          : dialect === "pg"
            ? "SELECT * FROM (SELECT id FROM src ORDER BY id) AS rapidb_query_cap LIMIT 11"
            : "SELECT id FROM src ORDER BY id LIMIT 11");
    expect(query).toHaveBeenCalledWith(expected, undefined, {
      requestToken: 1,
    });
    expect(connectionManager.addToHistory).toHaveBeenCalledWith(
      "active",
      original,
    );
    expect(view.setLastQueryResult).toHaveBeenCalled();
    expect(view.postMessage).toHaveBeenCalledWith(
      expect.objectContaining({
        type: "queryResult",
        payload: expect.objectContaining({ operationId: "cap-case" }),
      }),
    );
  });

  it.each([
    [
      "pg",
      "((SELECT * FROM items))",
      "SELECT * FROM (SELECT * FROM items) AS rapidb_query_cap LIMIT 11",
    ],
    [
      "pg",
      "TABLE items",
      "SELECT * FROM (TABLE items) AS rapidb_query_cap LIMIT 11",
    ],
    ["pg", "INSERT INTO items VALUES (1)", "INSERT INTO items VALUES (1)"],
    [
      "mysql",
      "SET STATEMENT max_statement_time = 1 FOR SELECT * FROM items",
      "SET STATEMENT max_statement_time = 1 FOR SELECT * FROM items LIMIT 11",
    ],
    ["mssql", "UPDATE items SET id = 1", "UPDATE items SET id = 1"],
    ["sqlite", "CREATE TABLE items (id int)", "CREATE TABLE items (id int)"],
    [
      "pg",
      "INSERT INTO items VALUES (1); SELECT * FROM items",
      "INSERT INTO items VALUES (1)\n;\nSELECT * FROM ( SELECT * FROM items) AS rapidb_query_cap LIMIT 11",
    ],
    [
      "mysql",
      "INSERT INTO items VALUES (1); SELECT * FROM items",
      "INSERT INTO items VALUES (1)\n;\n SELECT * FROM items LIMIT 11",
    ],
    [
      "oracle",
      "INSERT INTO items VALUES (1); SELECT * FROM items",
      "INSERT INTO items VALUES (1)\n;\nSELECT * FROM ( SELECT * FROM items) rapidb_query_cap FETCH FIRST 11 ROWS ONLY",
    ],
    [
      "pg",
      "DELETE FROM items RETURNING *",
      "WITH rapidb_returning_cap AS (DELETE FROM items RETURNING *) SELECT * FROM rapidb_returning_cap LIMIT 11",
    ],
  ])("executes the supported %s path: %s", async (dialect, original, expected) => {
    const { query } = await executeSqlCapCase(dialect, original);
    expect(query).toHaveBeenCalledWith(expected, undefined, {
      requestToken: 1,
    });
  });

  it.each([
    ["pg", "INSERT INTO items VALUES (1); EXPLAIN SELECT * FROM items"],
    [
      "sqlite",
      "INSERT INTO items VALUES (1); UPDATE items SET id = 2 RETURNING *",
    ],
    ["pg", "WITH c AS (SELECT 1) INSERT INTO items SELECT * FROM c"],
    ["mysql", "INSERT INTO items VALUES (1); SHOW TABLES"],
    ["mysql", "SET STATEMENT max_statement_time = 1 FOR SHOW TABLES"],
    ["sqlite", "UPDATE items SET id = 2 RETURNING *"],
    ["mssql", "INSERT INTO items VALUES (1) SELECT * FROM items"],
    ["mssql", "INSERT INTO items VALUES (1) (SELECT * FROM items)"],
    ["mssql", "SELECT * FROM items (SELECT * FROM items)"],
    [
      "mssql",
      "WITH c AS (SELECT id FROM items) SELECT * FROM c SELECT * FROM items",
    ],
    ["mssql", "UPDATE items SET id = 2 OUTPUT inserted.*"],
    ["oracle", "INSERT INTO items VALUES (1); CALL report()"],
  ])("rejects %s unbounded SQL before driver, history, or connection side effects: %s", async (dialect, original) => {
    const { query, connectionManager, view } = await executeSqlCapCase(
      dialect,
      original,
    );
    expect(query).not.toHaveBeenCalled();
    expect(connectionManager.connectTo).not.toHaveBeenCalled();
    expect(connectionManager.addToHistory).not.toHaveBeenCalled();
    expect(view.postMessage).toHaveBeenCalledWith(
      expect.objectContaining({
        type: "queryResult",
        payload: expect.objectContaining({
          error: expect.stringContaining("cannot be safely bounded"),
          operationId: "cap-case",
        }),
      }),
    );
  });

  it("rejects WITH queries that cannot be safely hard-capped", async () => {
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
        queryText: "with src as (select 1) insert into users select * from src",
      },
    });

    expect(query).not.toHaveBeenCalled();
    expect(view.postMessage).toHaveBeenCalledWith(
      expect.objectContaining({
        type: "queryResult",
        payload: expect.objectContaining({
          error: expect.stringContaining("cannot be safely bounded"),
        }),
      }),
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
      undefined,
      { requestToken: 1 },
    );
    expect(formatQueryResult).toHaveBeenCalledWith(
      expect.objectContaining({ rows: [{ id: 1 }] }),
      10000,
    );
  });

  it("strips trailing semicolon comments before SQL hard-cap wrapping", async () => {
    const query = vi.fn(async () => ({
      columns: ["database_name"],
      rows: [{ database_name: "postgres" }],
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
        queryText:
          "SELECT datname AS database_name FROM pg_catalog.pg_database WHERE datistemplate = false; -- trailing comment",
      },
    });

    expect(query).toHaveBeenCalledWith(
      "SELECT * FROM (SELECT datname AS database_name FROM pg_catalog.pg_database WHERE datistemplate = false) AS rapidb_query_cap LIMIT 10001",
      undefined,
      { requestToken: 1 },
    );
    expect(formatQueryResult).toHaveBeenCalledWith(
      expect.objectContaining({ rows: [{ database_name: "postgres" }] }),
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

  it("rejects readonly MSSQL queries without database-enforced permissions", async () => {
    const query = vi.fn();
    const connectionManager = {
      getConnection: vi.fn(() => ({
        id: "mssql-readonly",
        name: "Readonly SQL Server",
        type: "mssql",
        readOnly: true,
      })),
      getDriverCapabilities: vi.fn(() => ({
        readOnlyQueryGuard: () => ({ allowed: true as const }),
      })),
      isConnected: vi.fn(() => true),
      getDriver: vi.fn(() => ({ query })),
    };
    const view = {
      getActiveConnectionId: vi.fn(() => "mssql-readonly"),
      getInitialConnectionId: vi.fn(() => "mssql-readonly"),
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
      payload: { queryText: "select 1" },
    });

    expect(query).not.toHaveBeenCalled();
    expect(view.postMessage).toHaveBeenCalledWith({
      type: "queryResult",
      payload: expect.objectContaining({
        error:
          "[RapiDB] Read-only MSSQL queries require database-enforced read permissions; client-side SQL classification is not sufficient.",
      }),
    });
  });

  it("posts a query error instead of silently returning when the driver is unavailable", async () => {
    const addToHistory = vi.fn(async () => undefined);
    const connectionManager = {
      getConnection: vi.fn(() => ({
        id: "active",
        name: "Primary",
        type: "pg",
      })),
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
      undefined,
      { requestToken: 2 },
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

  it("cancels and invalidates the owned query on connection switch and disposal", async () => {
    let activeConnectionId = "conn-1";
    let resolveQuery: ((value: unknown) => void) | undefined;
    const cancelCurrentOperation = vi.fn(async () => undefined);
    const query = vi.fn(
      () =>
        new Promise((resolve) => {
          resolveQuery = resolve;
        }),
    );
    const connectionManager = {
      getConnection: vi.fn((id: string) => ({
        id,
        name: id,
        type: "pg",
        readOnly: false,
      })),
      getDriverCapabilities: vi.fn(() => ({})),
      isConnected: vi.fn(() => true),
      getDriver: vi.fn(() => ({ query, cancelCurrentOperation })),
      getQueryRowLimit: vi.fn(() => 100),
      addToHistory: vi.fn(async () => undefined),
      getSchemaAsync: vi.fn(async () => []),
    };
    const view = {
      getActiveConnectionId: vi.fn(() => activeConnectionId),
      getInitialConnectionId: vi.fn(() => "conn-1"),
      getLastQueryResult: vi.fn(() => null),
      postMessage: vi.fn(),
      setActiveConnectionId: vi.fn((id: string) => {
        activeConnectionId = id;
      }),
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
      payload: {
        queryText: "select slow",
        connectionId: "conn-1",
        operationId: "panel-1:1",
      },
    });
    await Promise.resolve();
    await controller.handleMessage({
      type: "activeConnectionChanged",
      payload: { connectionId: "conn-2" },
    });

    expect(cancelCurrentOperation).toHaveBeenCalledWith(
      expect.objectContaining({
        reason: "superseded",
        connectionId: "conn-1",
      }),
    );
    resolveQuery?.({ columns: [], rows: [], rowCount: 0, executionTimeMs: 1 });
    await first;
    expect(view.postMessage).not.toHaveBeenCalledWith(
      expect.objectContaining({ type: "queryResult" }),
    );

    const second = controller.handleMessage({
      type: "executeQuery",
      payload: {
        queryText: "select slower",
        connectionId: "conn-2",
        operationId: "panel-1:2",
      },
    });
    await Promise.resolve();
    await controller.dispose();
    expect(cancelCurrentOperation).toHaveBeenLastCalledWith(
      expect.objectContaining({
        reason: "lifecycle_shutdown",
        connectionId: "conn-2",
      }),
    );
    resolveQuery?.({ columns: [], rows: [], rowCount: 0, executionTimeMs: 1 });
    await second;
  });
});
