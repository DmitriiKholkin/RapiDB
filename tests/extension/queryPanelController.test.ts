import { beforeEach, describe, expect, it, vi } from "vitest";
import type { QueryPanelCachedResult } from "../../src/extension/panels/queryPanelController";
import { createSqlReadOnlyQueryGuard } from "../../src/extension/utils/readOnlyGuards";
import {
  MONGO_ERROR_URI,
  MONGO_NUMERIC_PASSWORD_ERROR_URIS,
  MONGO_QUERY_ERROR_URIS,
  mongoCredentialError,
} from "../support/mongoCredentialError";

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

  it.each(
    ["query", "connect"].flatMap((route) =>
      [
        MONGO_ERROR_URI,
        ...MONGO_QUERY_ERROR_URIS,
        ...MONGO_NUMERIC_PASSWORD_ERROR_URIS,
      ].map((uri) => ({
        route,
        uri,
      })),
    ),
  )("redacts upstream Mongo URI errors in the $route response: $uri", async ({
    route,
    uri,
  }) => {
    const upstream = mongoCredentialError(uri);
    const query = vi.fn(async (_queryText: string) => {
      throw upstream;
    });
    const connection = {
      id: "mongo",
      name: "Mongo",
      type: "mongodb",
      connectionUri: uri,
    };
    const connectionManager = {
      getConnection: () => connection,
      isConnected: () => route === "query",
      connectTo: vi.fn(async () => {
        throw upstream;
      }),
      getQueryRowLimit: () => 100,
      getDriver: () => ({ query }),
      addToHistory: vi.fn(async () => undefined),
      refreshSchemaCache: vi.fn(),
    };
    const view = {
      getActiveConnectionId: () => "mongo",
      getInitialConnectionId: () => "mongo",
      getLastQueryResult: () => null,
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
    const queryText = `db.runCommand({ comment: ${JSON.stringify(uri)} })`;
    await controller.handleMessage({
      type: "executeQuery",
      payload: { queryText, connectionId: "mongo" },
    });
    expect(view.postMessage).toHaveBeenCalledWith(
      expect.objectContaining({
        type: "queryResult",
        payload: expect.objectContaining({
          error: expect.stringContaining("Protocol and host list are required"),
        }),
      }),
    );
    expect(JSON.stringify(view.postMessage.mock.calls)).not.toMatch(
      /H10_MONGO_SENTINEL|H10_QUERY_SECRET|H10_SECRET|h10-user|user:123/,
    );
    if (route === "query") expect(query.mock.calls[0]?.[0]).toBe(queryText);
    expect(upstream.message).toContain(uri);
    expect(connection.connectionUri).toBe(uri);
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
      getSchemaSnapshotState: vi.fn(() => ({ status: "loaded" })),
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
      payload: { connectionId: "initial", schema: [], error: "Not connected." },
    });
  });

  it("echoes bookmark request IDs on success and failure and rejects malformed IDs before storage", async () => {
    const addBookmark = vi.fn(
      async (_connectionId: string, _queryText: string) => undefined,
    );
    const view = {
      getActiveConnectionId: () => "active",
      getInitialConnectionId: () => "initial",
      getLastQueryResult: () => null,
      postMessage: vi.fn(),
      setActiveConnectionId: vi.fn(),
      setLastQueryResult: vi.fn(),
      syncTitle: vi.fn(),
    };
    const { QueryPanelController } = await import(
      "../../src/extension/panels/queryPanelController"
    );
    const controller = new QueryPanelController({ addBookmark } as never, view);
    await controller.handleMessage({
      type: "addBookmark",
      payload: {
        queryText: "select 1",
        connectionId: "conn:1",
        requestId: "bookmark:1",
      },
    });
    expect(addBookmark).toHaveBeenCalledWith("conn:1", "select 1");
    expect(view.postMessage).toHaveBeenLastCalledWith({
      type: "bookmarkSaved",
      payload: { ok: true, requestId: "bookmark:1" },
    });
    addBookmark.mockRejectedValueOnce(new Error("Storage failed"));
    await controller.handleMessage({
      type: "addBookmark",
      payload: {
        queryText: "select 2",
        requestId: "bookmark:2",
      },
    });
    expect(view.postMessage).toHaveBeenLastCalledWith({
      type: "bookmarkSaved",
      payload: { ok: false, error: "Storage failed", requestId: "bookmark:2" },
    });
    for (const requestId of [null, 123, "", "   "]) {
      await controller.handleMessage({
        type: "addBookmark",
        payload: { queryText: "select 3", requestId },
      });
    }
    expect(addBookmark).toHaveBeenCalledTimes(2);
    await controller.handleMessage({
      type: "addBookmark",
      payload: { sql: "select 4" },
    });
    expect(view.postMessage).toHaveBeenLastCalledWith({
      type: "bookmarkSaved",
      payload: { ok: true },
    });
  });

  it.each([
    { failure: "Storage unavailable", message: "Storage unavailable" },
    { failure: { cause: new Error("Disk full") }, message: "Disk full" },
    { failure: null, message: "Unknown error" },
  ])("reports bookmark storage failures without changing SQL results and permits retry: $message", async ({
    failure,
    message,
  }) => {
    const addBookmark = vi
      .fn(async () => undefined)
      .mockRejectedValueOnce(failure);
    const cachedResult = {
      columns: ["id"],
      columnMeta: [],
      rows: [{ id: 1 }],
      rowCount: 1,
      executionTimeMs: 5,
    };
    const view = {
      getActiveConnectionId: () => "active",
      getInitialConnectionId: () => "initial",
      getLastQueryResult: () => cachedResult,
      postMessage: vi.fn(),
      setActiveConnectionId: vi.fn(),
      setLastQueryResult: vi.fn(),
      syncTitle: vi.fn(),
    };
    const { QueryPanelController } = await import(
      "../../src/extension/panels/queryPanelController"
    );
    const controller = new QueryPanelController({ addBookmark } as never, view);
    await controller.handleMessage({
      type: "addBookmark",
      payload: {
        connectionId: "explicit",
        queryText: "select 1",
        requestId: "save:1",
      },
    });
    expect(view.postMessage.mock.calls).toEqual([
      [
        {
          type: "bookmarkSaved",
          payload: { ok: false, error: message, requestId: "save:1" },
        },
      ],
    ]);
    expect(view.setLastQueryResult).not.toHaveBeenCalled();
    expect(view.setActiveConnectionId).not.toHaveBeenCalled();

    await controller.handleMessage({
      type: "addBookmark",
      payload: {
        connectionId: "explicit",
        queryText: "select 1",
        requestId: "save:2",
      },
    });
    expect(addBookmark).toHaveBeenNthCalledWith(1, "explicit", "select 1");
    expect(addBookmark).toHaveBeenNthCalledWith(2, "explicit", "select 1");
    expect(view.postMessage).toHaveBeenCalledTimes(2);
    expect(view.postMessage).toHaveBeenLastCalledWith({
      type: "bookmarkSaved",
      payload: { ok: true, requestId: "save:2" },
    });
    expect(view.setLastQueryResult).not.toHaveBeenCalled();
    expect(view.getLastQueryResult()).toBe(cachedResult);
  });

  it.each([
    "exportResultsCSV",
    "exportResultsJSON",
  ])("%s follows grid text sorting and preserves native row alignment", async (type) => {
    const { QueryPanelController } = await import(
      "../../src/extension/panels/queryPanelController"
    );
    const values = [
      "item10",
      "item2",
      "A",
      "a",
      "z",
      "z",
      "z",
      "z",
      "z",
      "z",
      "z",
      "item3",
    ];
    const cached = {
      columns: ["value"],
      columnMeta: [{ category: "text" as const }],
      rows: values.map((value) => ({ __col_0: value })),
      exportRows: values.map((_, index) => ({ __col_0: { source: index } })),
      missingCells: [{ rowIndex: 11, columnIndex: 0 }],
    };
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
    for (const [desc, indices] of [
      [false, [2, 3, 1, 11, 0, 4, 5, 6, 7, 8, 9, 10]],
      [true, [4, 5, 6, 7, 8, 9, 10, 0, 11, 1, 2, 3]],
    ] as const) {
      await controller.handleMessage({
        type,
        payload: { sort: [{ id: "__col_0", desc }] },
      });
      expect(exporter).toHaveBeenLastCalledWith(
        {
          ...cached,
          rows: indices.map((i) => cached.rows[i]),
          exportRows: indices.map((i) => cached.exportRows[i]),
          missingCells: [{ rowIndex: indices.indexOf(11), columnIndex: 0 }],
        },
        { context: undefined },
      );
    }
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
      exportRows: [
        { __col_0: { N: "2" }, __col_1: { S: "a" } },
        { __col_0: { N: "1" }, __col_1: { S: "c" } },
        { __col_0: { N: "1" }, __col_1: { S: "b" } },
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
        exportRows: [
          { __col_0: { S: "a" }, __col_1: { N: "2" } },
          { __col_0: { S: "c" }, __col_1: { N: "1" } },
          { __col_0: { S: "b" }, __col_1: { N: "1" } },
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
          exportRows: indices.map((i) => cached.exportRows[i]),
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
        exportRows: [
          { __col_0: { S: "b" }, __col_1: { N: "1" } },
          { __col_0: { S: "c" }, __col_1: { N: "1" } },
          { __col_0: { S: "a" }, __col_1: { N: "2" } },
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
        exportRows: [
          { __col_0: { S: "c" } },
          { __col_0: { S: "b" } },
          { __col_0: { S: "a" } },
        ],
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

  it.each([
    "exportResultsCSV",
    "exportResultsJSON",
  ])("%s matches exact numeric grid order while aligning native rows and missing cells", async (type) => {
    const { QueryPanelController } = await import(
      "../../src/extension/panels/queryPanelController"
    );
    const values = [
      "10",
      "2",
      "9007199254740993",
      "9007199254740992",
      "-0.00000000000000000002",
      "-0.00000000000000000001",
      "2.0",
      null,
      undefined,
    ];
    const exporter =
      type === "exportResultsCSV"
        ? exportQueryResultsAsCsv
        : exportQueryResultsAsJson;
    for (const category of ["integer", "decimal", "float"] as const) {
      const cached = {
        columns: ["value", "value"],
        columnMeta: [{ category }, { category: "text" as const }],
        rows: values.map((value, index) => ({
          __col_0: value,
          __col_1: `row-${index}`,
        })),
        exportRows: values.map((_value, index) => ({
          __col_0: { source: index },
          __col_1: { source: `row-${index}` },
        })),
        missingCells: [{ rowIndex: 8, columnIndex: 0 }],
      };
      const snapshot = structuredClone(cached);
      const controller = new QueryPanelController({} as never, {
        getActiveConnectionId: () => "active",
        getInitialConnectionId: () => "initial",
        getLastQueryResult: () => cached,
        postMessage: vi.fn(),
        setActiveConnectionId: vi.fn(),
        setLastQueryResult: vi.fn(),
        syncTitle: vi.fn(),
      });
      for (const [desc, indices] of [
        [false, [7, 8, 4, 5, 1, 6, 0, 3, 2]],
        [true, [2, 3, 0, 1, 6, 5, 4, 7, 8]],
      ] as const) {
        await controller.handleMessage({
          type,
          payload: {
            sort: [{ id: "__col_0", desc }],
            columnOrder: ["__col_1", "__col_0"],
          },
        });
        expect(exporter).toHaveBeenLastCalledWith(
          {
            ...cached,
            columnMeta: [cached.columnMeta[1], cached.columnMeta[0]],
            rows: indices.map((i) => ({
              __col_0: cached.rows[i].__col_1,
              __col_1: cached.rows[i].__col_0,
            })),
            exportRows: indices.map((i) => ({
              __col_0: cached.exportRows[i].__col_1,
              __col_1: cached.exportRows[i].__col_0,
            })),
            missingCells: [{ rowIndex: indices.indexOf(8), columnIndex: 1 }],
          },
          { context: undefined },
        );
      }
      expect(cached).toEqual(snapshot);
    }
  });

  it("exports only aggregate-capped rows from the same formatted result posted to the grid", async () => {
    const actual = await vi.importActual<
      typeof import("../../src/extension/utils/queryResultFormatting")
    >("../../src/extension/utils/queryResultFormatting");
    formatQueryResult.mockImplementation(actual.formatQueryResult);
    const first = Array.from({ length: 6000 }, (_, i) => ({ __col_0: i }));
    const second = Array.from({ length: 4001 }, (_, i) => ({ __col_0: i }));
    const raw = {
      columns: ["first"],
      rows: first,
      rowCount: 10001,
      executionTimeMs: 1,
      resultSets: [
        { id: "one", columns: ["first"], rows: first, rowCount: 6000 },
        { id: "two", columns: ["second"], rows: second, rowCount: 4001 },
      ],
    };
    const query = vi.fn(async () => raw);
    const manager = {
      getConnection: () => ({ id: "qa", type: "pg" }),
      getDriver: () => ({ query }),
      getDriverCapabilities: () => ({ boundedQueryResults: true }),
      isConnected: () => true,
      getQueryRowLimit: () => 10000,
      addToHistory: vi.fn(async () => undefined),
    };
    let cached: ReturnType<typeof actual.formatQueryResult> | null = null;
    const view = {
      getActiveConnectionId: () => "qa",
      getInitialConnectionId: () => "qa",
      getLastQueryResult: () => cached,
      postMessage: vi.fn(),
      setActiveConnectionId: vi.fn(),
      setLastQueryResult: vi.fn((value) => {
        cached = value;
      }),
      syncTitle: vi.fn(),
    };
    const { QueryPanelController } = await import(
      "../../src/extension/panels/queryPanelController"
    );
    const controller = new QueryPanelController(manager as never, view);
    await controller.handleMessage({
      type: "executeQuery",
      payload: { queryText: "SELECT first; SELECT second" },
    });
    const posted = view.postMessage.mock.calls.find(
      ([message]) => message.type === "queryResult",
    )?.[0].payload;
    expect(posted.resultSets[1]).toMatchObject({
      truncated: true,
      truncatedAt: 4000,
      rowCount: 4001,
    });
    expect(posted.resultSets[1].rows).toHaveLength(4000);
    for (const type of ["exportResultsJSON", "exportResultsCSV"]) {
      showWarningMessage.mockResolvedValueOnce("Export displayed rows");
      await controller.handleMessage({ type, payload: { resultSetIndex: 1 } });
      const exporter =
        type === "exportResultsJSON"
          ? exportQueryResultsAsJson
          : exportQueryResultsAsCsv;
      expect(exporter).toHaveBeenLastCalledWith(posted.resultSets[1], {
        context: undefined,
      });
      expect(showWarningMessage).toHaveBeenLastCalledWith(
        expect.stringContaining("first 4,000 displayed rows"),
        { modal: true },
        "Export displayed rows",
      );
    }
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
      getSchemaSnapshotState: vi.fn(() => ({ status: "loaded" })),
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

  it.each([
    { queryText: "UPDATE items SET id = id + 1", affectedRows: 3, refresh: 0 },
    {
      queryText: "CREATE TABLE items (id NUMBER)",
      affectedRows: 0,
      refresh: 1,
    },
  ])("preserves completed Oracle SQL during post-commit cleanup after a no-op Stop: $queryText", async ({
    queryText,
    affectedRows,
    refresh,
  }) => {
    const actual = await vi.importActual<
      typeof import("../../src/extension/utils/queryResultFormatting")
    >("../../src/extension/utils/queryResultFormatting");
    formatQueryResult.mockImplementation(actual.formatQueryResult);
    const result = {
      columns: [],
      rows: [],
      rowCount: affectedRows,
      affectedRows,
      executionTimeMs: 7,
    };
    let finishRelease!: () => void;
    const pendingRelease = new Promise<void>((resolve) => {
      finishRelease = resolve;
    });
    let operationActive = true;
    const committed = vi.fn();
    const breakOperation = vi.fn();
    const cancelCurrentOperation = vi.fn(async () => {
      if (operationActive) breakOperation();
    });
    const driver = {
      query: vi.fn(async () => {
        committed();
        try {
          return result;
        } finally {
          operationActive = false;
          await pendingRelease;
        }
      }),
      cancelCurrentOperation,
    };
    const connectionManager = {
      getConnection: vi.fn(() => ({
        id: "active",
        name: "Primary",
        type: "oracle",
      })),
      getDriverCapabilities: vi.fn(() => ({ boundedQueryResults: true })),
      isConnected: vi.fn(() => true),
      addToHistory: vi.fn(async () => undefined),
      getDriver: vi.fn(() => driver),
      getQueryRowLimit: vi.fn(() => 100),
      refreshSchemaCache: vi.fn(),
    };
    const view = {
      getActiveConnectionId: vi.fn(() => "active"),
      getInitialConnectionId: vi.fn(() => "active"),
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
    const running = controller.handleMessage({
      type: "executeQuery",
      payload: {
        queryText,
        operationId: "panel:run:1",
      },
    });
    await vi.waitFor(() => expect(driver.query).toHaveBeenCalledOnce());
    expect(committed).toHaveBeenCalledOnce();
    expect(operationActive).toBe(false);

    await controller.handleMessage({
      type: "cancelQuery",
      payload: { operationId: "panel:run:1" },
    });

    expect(cancelCurrentOperation).toHaveBeenCalledWith(
      expect.objectContaining({ reason: "manual", operationName: "query" }),
    );
    expect(breakOperation).not.toHaveBeenCalled();
    expect(view.postMessage).not.toHaveBeenCalled();
    finishRelease();
    await running;
    expect(view.postMessage).toHaveBeenCalledExactlyOnceWith({
      type: "queryResult",
      payload: {
        ...result,
        columnMeta: [],
        truncated: false,
        truncatedAt: 0,
        connectionId: "active",
        operationId: "panel:run:1",
        requestToken: expect.any(Number),
      },
    });
    expect(view.setLastQueryResult).toHaveBeenCalledExactlyOnceWith({
      columns: [],
      columnMeta: [],
      rows: [],
      truncated: false,
      truncatedAt: 0,
    });
    expect(connectionManager.refreshSchemaCache).toHaveBeenCalledTimes(refresh);
    if (refresh) {
      expect(connectionManager.refreshSchemaCache).toHaveBeenCalledWith(
        "active",
      );
    }
  });

  it.each([
    { cancellation: "succeeds", outcome: "error" },
    { cancellation: "succeeds", outcome: "success" },
    { cancellation: "fails", outcome: "error" },
    { cancellation: "fails", outcome: "success" },
    { cancellation: "times out", outcome: "error" },
    { cancellation: "times out", outcome: "success" },
    { cancellation: "unavailable", outcome: "error" },
    { cancellation: "unavailable", outcome: "success" },
  ])("preserves the correct $outcome outcome when manual cancellation $cancellation", async ({
    cancellation,
    outcome,
  }) => {
    const result = {
      columns: ["id"],
      rows: [{ __col_0: 1 }],
      rowCount: 1,
      executionTimeMs: 1,
    };
    let resolveQuery!: (value: typeof result) => void;
    let rejectQuery!: (error: Error) => void;
    const pendingQuery = new Promise<typeof result>((resolve, reject) => {
      resolveQuery = resolve;
      rejectQuery = reject;
    });
    const finishQuery = () => {
      if (outcome === "error") rejectQuery(new Error("Driver query error"));
      else resolveQuery(result);
    };
    let finishLateCancellation!: () => void;
    const cancelCurrentOperation = vi.fn(async () => {
      finishQuery();
      if (cancellation === "fails") throw new Error("Cancel failed");
      if (cancellation === "times out") {
        await new Promise<void>((resolve) => {
          finishLateCancellation = resolve;
        });
      }
    });
    const driver = {
      query: vi.fn(() => pendingQuery),
      ...(cancellation === "unavailable" ? {} : { cancelCurrentOperation }),
    };
    const connectionManager = {
      getConnection: () => ({ id: "active", name: "Primary", type: "pg" }),
      isConnected: () => true,
      addToHistory: vi.fn(async () => undefined),
      getDriver: () => driver,
      getQueryRowLimit: () => 100,
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
    const controller = new QueryPanelController(
      connectionManager as never,
      view,
    );
    const running = controller.handleMessage({
      type: "executeQuery",
      payload: { queryText: "select 1", operationId: "manual:1" },
    });
    await vi.waitFor(() => expect(driver.query).toHaveBeenCalledOnce());
    vi.useFakeTimers();
    try {
      const cancelling = controller.handleMessage({
        type: "cancelQuery",
        payload: { operationId: "manual:1" },
      });
      if (cancellation === "unavailable") finishQuery();
      if (cancellation === "times out") {
        await vi.advanceTimersByTimeAsync(1500);
      }
      await cancelling;
      await running;
      expect(view.postMessage).toHaveBeenCalledOnce();
      expect(view.postMessage).toHaveBeenCalledWith({
        type: "queryResult",
        payload: expect.objectContaining({
          operationId: "manual:1",
          connectionId: "active",
          ...(outcome === "error"
            ? {
                error:
                  cancellation === "succeeds"
                    ? "[RapiDB] Query failed after a cancellation request. Driver query error The operation outcome may be unknown; cancellation does not confirm rollback."
                    : "Driver query error",
              }
            : result),
        }),
      });
      expect(showWarningMessage).toHaveBeenCalledTimes(
        cancellation === "succeeds" ? 0 : 1,
      );
      if (cancellation === "times out") {
        let finishNextQuery!: (value: typeof result) => void;
        driver.query.mockImplementationOnce(
          () =>
            new Promise((resolve) => {
              finishNextQuery = resolve;
            }),
        );
        const nextRun = controller.handleMessage({
          type: "executeQuery",
          payload: { queryText: "select 2", operationId: "manual:2" },
        });
        await vi.waitFor(() => expect(driver.query).toHaveBeenCalledTimes(2));
        finishLateCancellation();
        await Promise.resolve();
        await controller.handleMessage({
          type: "cancelQuery",
          payload: { operationId: "manual:1" },
        });
        expect(cancelCurrentOperation).toHaveBeenCalledOnce();
        expect(view.postMessage).toHaveBeenCalledOnce();
        finishNextQuery(result);
        await nextRun;
        expect(view.postMessage).toHaveBeenCalledTimes(2);
        expect(view.postMessage).toHaveBeenLastCalledWith({
          type: "queryResult",
          payload: expect.objectContaining({
            ...result,
            operationId: "manual:2",
          }),
        });
      }
    } finally {
      vi.useRealTimers();
    }
  });

  it("explains the Redis query-client disconnect after a cancellation request", async () => {
    let rejectQuery!: (error: Error) => void;
    const pendingQuery = new Promise<never>((_resolve, reject) => {
      rejectQuery = reject;
    });
    const driver = {
      query: vi.fn(() => pendingQuery),
      cancelCurrentOperation: vi.fn(() => {
        rejectQuery(new Error("Disconnects client"));
      }),
    };
    const connectionManager = {
      getConnection: () => ({
        id: "redis",
        name: "Redis QA",
        type: "redis",
        readOnly: false,
      }),
      getDriverCapabilities: () => ({ boundedQueryResults: true }),
      isConnected: () => true,
      addToHistory: vi.fn(async () => undefined),
      getDriver: () => driver,
      getQueryRowLimit: () => 100,
    };
    const view = {
      getActiveConnectionId: () => "redis",
      getInitialConnectionId: () => "redis",
      getLastQueryResult: () => null,
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
    const running = controller.handleMessage({
      type: "executeQuery",
      payload: {
        queryText: "BLPOP manual_cancel_probe 15",
        operationId: "redis:cancel",
      },
    });
    await vi.waitFor(() => expect(driver.query).toHaveBeenCalledOnce());
    await controller.handleMessage({
      type: "cancelQuery",
      payload: { operationId: "redis:cancel" },
    });
    await running;

    expect(view.postMessage).toHaveBeenCalledWith({
      type: "queryResult",
      payload: expect.objectContaining({
        connectionId: "redis",
        operationId: "redis:cancel",
        error:
          "[RapiDB] Query cancellation was requested. The Redis query client was disconnected to stop the active request. The operation outcome may be unknown; cancellation does not confirm rollback.",
      }),
    });
    expect(showWarningMessage).not.toHaveBeenCalled();
  });

  it.each([
    "during cancellation",
    "after acknowledgement",
  ])("preserves SQLite unknown-outcome and session-loss diagnostics %s", async (timing) => {
    const driverError = new Error(
      "[RapiDB] SQLite operation cancelled (manual) The active SQLite request outcome is unknown (OUTCOME_UNKNOWN); if it contained a write, it may have partially or fully persisted. Refresh and verify before retrying. SQLite execution process was stopped and the connection is closed. Uncommitted transactions are rolled back. ATTACH, temporary tables and session state are lost; in-memory databases are lost. Use Connect to reconnect explicitly before continuing (:memory: starts a new empty database). Completed statements may have persisted; refresh file data before retrying.",
    );
    Object.assign(driverError, {
      code: "OUTCOME_UNKNOWN",
      executionState: "unknown",
    });
    const originalMessage = driverError.message;
    let rejectQuery!: (error: Error) => void;
    const pendingQuery = new Promise<never>((_resolve, reject) => {
      rejectQuery = reject;
    });
    let acknowledgeCancellation!: () => void;
    const pendingCancellation = new Promise<void>((resolve) => {
      acknowledgeCancellation = resolve;
    });
    const driver = {
      query: vi.fn(() => pendingQuery),
      cancelCurrentOperation: vi.fn(async () => {
        if (timing === "during cancellation") rejectQuery(driverError);
        await pendingCancellation;
      }),
    };
    const connectionManager = {
      getConnection: () => ({ id: "sqlite", name: "SQLite", type: "sqlite" }),
      getDriverCapabilities: () => ({ boundedQueryResults: true }),
      isConnected: () => true,
      addToHistory: vi.fn(async () => undefined),
      getDriver: () => driver,
      getQueryRowLimit: () => 100,
    };
    const view = {
      getActiveConnectionId: () => "sqlite",
      getInitialConnectionId: () => "sqlite",
      getLastQueryResult: () => null,
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
    const running = controller.handleMessage({
      type: "executeQuery",
      payload: {
        queryText: "UPDATE items SET id = id + 1",
        operationId: "sqlite:write",
      },
    });
    await vi.waitFor(() => expect(driver.query).toHaveBeenCalledOnce());
    const cancelling = controller.handleMessage({
      type: "cancelQuery",
      payload: { operationId: "sqlite:write" },
    });
    await vi.waitFor(() =>
      expect(driver.cancelCurrentOperation).toHaveBeenCalledOnce(),
    );
    expect(view.postMessage).not.toHaveBeenCalled();
    acknowledgeCancellation();
    await cancelling;
    if (timing === "after acknowledgement") rejectQuery(driverError);
    await running;
    expect(view.postMessage).toHaveBeenCalledExactlyOnceWith({
      type: "queryResult",
      payload: expect.objectContaining({
        connectionId: "sqlite",
        operationId: "sqlite:write",
        error: `[RapiDB] Query failed after a cancellation request. ${originalMessage} The operation outcome may be unknown; cancellation does not confirm rollback.`,
      }),
    });
    expect(view.setLastQueryResult).not.toHaveBeenCalled();
    expect(driverError.message).toBe(originalMessage);
    expect(showWarningMessage).not.toHaveBeenCalled();
  });

  it("keeps a superseding operation live when the old query and manual cancellation settle late", async () => {
    const result = {
      columns: ["id"],
      rows: [{ __col_0: 2 }],
      rowCount: 1,
      executionTimeMs: 1,
    };
    let rejectOldQuery!: (error: Error) => void;
    const oldQuery = new Promise<typeof result>((_resolve, reject) => {
      rejectOldQuery = reject;
    });
    let resolveNewQuery!: (value: typeof result) => void;
    const newQuery = new Promise<typeof result>((resolve) => {
      resolveNewQuery = resolve;
    });
    let acknowledgeOldCancellation!: () => void;
    const oldCancellation = new Promise<void>((resolve) => {
      acknowledgeOldCancellation = resolve;
    });
    const driver = {
      query: vi
        .fn()
        .mockReturnValueOnce(oldQuery)
        .mockReturnValueOnce(newQuery),
      cancelCurrentOperation: vi
        .fn(async (): Promise<void> => undefined)
        .mockImplementationOnce(() => oldCancellation),
    };
    const connectionManager = {
      getConnection: () => ({ id: "active", name: "Primary", type: "pg" }),
      isConnected: () => true,
      addToHistory: vi.fn(async () => undefined),
      getDriver: () => driver,
      getQueryRowLimit: () => 100,
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
    const controller = new QueryPanelController(
      connectionManager as never,
      view,
    );
    const oldRun = controller.handleMessage({
      type: "executeQuery",
      payload: { queryText: "select 1", operationId: "old" },
    });
    await vi.waitFor(() => expect(driver.query).toHaveBeenCalledOnce());
    const cancellingOld = controller.handleMessage({
      type: "cancelQuery",
      payload: { operationId: "old" },
    });
    const newRun = controller.handleMessage({
      type: "executeQuery",
      payload: { queryText: "select 2", operationId: "new" },
    });
    rejectOldQuery(new Error("Old driver failure (OUTCOME_UNKNOWN)"));
    acknowledgeOldCancellation();
    await cancellingOld;
    await oldRun;
    await vi.waitFor(() => expect(driver.query).toHaveBeenCalledTimes(2));
    expect(view.postMessage).not.toHaveBeenCalled();
    await controller.handleMessage({
      type: "cancelQuery",
      payload: { operationId: "old" },
    });
    expect(driver.cancelCurrentOperation).toHaveBeenCalledTimes(2);
    await controller.handleMessage({
      type: "cancelQuery",
      payload: { operationId: "new" },
    });
    expect(driver.cancelCurrentOperation).toHaveBeenCalledTimes(3);
    expect(driver.cancelCurrentOperation).toHaveBeenLastCalledWith(
      expect.objectContaining({
        reason: "manual",
        requestToken: driver.query.mock.calls[1][2].requestToken,
      }),
    );
    resolveNewQuery(result);
    await newRun;
    expect(view.postMessage).toHaveBeenCalledExactlyOnceWith({
      type: "queryResult",
      payload: expect.objectContaining({ ...result, operationId: "new" }),
    });
  });

  it("cancels a query request that is still waiting for its connection", async () => {
    let finishConnect!: () => void;
    const pendingConnect = new Promise<void>((resolve) => {
      finishConnect = resolve;
    });
    const query = vi.fn(async () => ({
      columns: [],
      rows: [],
      rowCount: 0,
      executionTimeMs: 1,
    }));
    const connectionManager = {
      getConnection: vi.fn(() => ({
        id: "active",
        name: "Primary",
        type: "pg",
      })),
      getDriverCapabilities: vi.fn(() => undefined),
      isConnected: vi.fn(() => false),
      connectTo: vi.fn(() => pendingConnect),
      addToHistory: vi.fn(async () => undefined),
      getDriver: vi.fn(() => ({ query })),
      getQueryRowLimit: vi.fn(() => 100),
    };
    const view = {
      getActiveConnectionId: vi.fn(() => "active"),
      getInitialConnectionId: vi.fn(() => "active"),
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
    const running = controller.handleMessage({
      type: "executeQuery",
      payload: {
        queryText: "select 1",
        operationId: "panel:connect:1",
      },
    });
    await vi.waitFor(() =>
      expect(connectionManager.connectTo).toHaveBeenCalledOnce(),
    );

    await controller.handleMessage({
      type: "cancelQuery",
      payload: { operationId: "panel:connect:1" },
    });

    expect(view.postMessage).toHaveBeenCalledWith(
      expect.objectContaining({
        type: "queryResult",
        payload: expect.objectContaining({
          operationId: "panel:connect:1",
          error: "[RapiDB] Query cancelled.",
        }),
      }),
    );
    finishConnect();
    await running;
    expect(query).not.toHaveBeenCalled();
  });

  it("requires explicit confirmation before exporting a truncated query result", async () => {
    showWarningMessage.mockResolvedValue("Export displayed rows");
    const cached = {
      columns: ["id"],
      rows: [{ __col_0: 1 }],
      truncated: true,
      truncatedAt: 100,
    };
    const { QueryPanelController } = await import(
      "../../src/extension/panels/queryPanelController"
    );
    const controller = new QueryPanelController({} as never, {
      getActiveConnectionId: () => "active",
      getInitialConnectionId: () => "active",
      getLastQueryResult: () => cached,
      postMessage: vi.fn(),
      setActiveConnectionId: vi.fn(),
      setLastQueryResult: vi.fn(),
      syncTitle: vi.fn(),
    });

    await controller.handleMessage({ type: "exportResultsCSV" });

    expect(showWarningMessage).toHaveBeenCalledWith(
      expect.stringContaining("only the first 1 displayed row"),
      { modal: true },
      "Export displayed rows",
    );
    expect(exportQueryResultsAsCsv).toHaveBeenCalledOnce();
  });

  it("requires explicit confirmation before exporting a partial result set", async () => {
    showWarningMessage.mockResolvedValue("Export partial result");
    const partial = {
      id: "result-2",
      columns: ["id"],
      columnMeta: [],
      rows: [{ __col_0: 1 }],
      rowCount: 2,
      partial: true,
      truncated: false,
      truncatedAt: 1,
    };
    const { QueryPanelController } = await import(
      "../../src/extension/panels/queryPanelController"
    );
    const controller = new QueryPanelController({} as never, {
      getActiveConnectionId: () => "active",
      getInitialConnectionId: () => "active",
      getLastQueryResult: () => ({
        columns: ["id"],
        rows: [{ __col_0: 1 }],
        resultSets: [partial],
      }),
      postMessage: vi.fn(),
      setActiveConnectionId: vi.fn(),
      setLastQueryResult: vi.fn(),
      syncTitle: vi.fn(),
    });

    await controller.handleMessage({ type: "exportResultsCSV" });

    expect(showWarningMessage).toHaveBeenCalledWith(
      expect.stringContaining("only rows received before the error"),
      { modal: true },
      "Export partial result",
    );
    expect(exportQueryResultsAsCsv).toHaveBeenCalledWith(partial, {
      context: undefined,
    });
  });

  it.each(
    (["csv", "json"] as const).flatMap((format) =>
      [false, true].map((withResultSets) => ({ format, withResultSets })),
    ),
  )("confirms exporting $format after a batch error (result sets: $withResultSets)", async ({
    format,
    withResultSets,
  }) => {
    const realFormatting = await vi.importActual<
      typeof import("../../src/extension/utils/queryResultFormatting")
    >("../../src/extension/utils/queryResultFormatting");
    formatQueryResult.mockImplementation(realFormatting.formatQueryResult);
    const completed = {
      id: "result-1",
      columns: ["value"],
      columnMeta: [{ category: "number" as const }],
      rows: [{ __col_0: 1 }],
      rowCount: 1,
    };
    const query = vi.fn(async () => ({
      ...completed,
      executionTimeMs: 1,
      error: "syntax error near FROM",
      ...(withResultSets ? { resultSets: [completed] } : {}),
    }));
    let cached: QueryPanelCachedResult | null = null;
    const view = {
      getActiveConnectionId: () => "mysql",
      getInitialConnectionId: () => "mysql",
      getLastQueryResult: () => cached,
      postMessage: vi.fn(),
      setActiveConnectionId: vi.fn(),
      setLastQueryResult: (result: QueryPanelCachedResult | null) => {
        cached = result;
      },
      syncTitle: vi.fn(),
    };
    const { QueryPanelController } = await import(
      "../../src/extension/panels/queryPanelController"
    );
    const controller = new QueryPanelController(
      {
        getConnection: () => ({ id: "mysql", type: "mysql" }),
        isConnected: () => true,
        getQueryRowLimit: () => 100,
        getDriver: () => ({ query }),
        addToHistory: vi.fn(async () => undefined),
        refreshSchemaCache: vi.fn(),
      } as never,
      view,
    );
    await controller.handleMessage({
      type: "executeQuery",
      payload: { queryText: "SELECT 1; SELECT FROM;" },
    });
    expect(query).toHaveBeenCalledOnce();
    expect(view.postMessage).toHaveBeenCalledWith(
      expect.objectContaining({
        type: "queryResult",
        payload: expect.objectContaining({ error: "syntax error near FROM" }),
      }),
    );
    const request = {
      type: format === "csv" ? "exportResultsCSV" : "exportResultsJSON",
    } as const;
    await controller.handleMessage(request);
    expect(showWarningMessage).toHaveBeenCalledWith(
      expect.stringContaining("only rows received before the error"),
      { modal: true },
      "Export partial result",
    );
    expect(exportQueryResultsAsCsv).not.toHaveBeenCalled();
    expect(exportQueryResultsAsJson).not.toHaveBeenCalled();

    showWarningMessage.mockResolvedValue("Export partial result");
    await controller.handleMessage(request);
    const exporter =
      format === "csv" ? exportQueryResultsAsCsv : exportQueryResultsAsJson;
    expect(exporter).toHaveBeenCalledOnce();
    expect(exporter).toHaveBeenCalledWith(
      expect.objectContaining({ rows: [{ __col_0: 1 }], partial: true }),
      { context: undefined },
    );
  });

  it("exports the requested SQL Server result set", async () => {
    const selectedSet = {
      id: "result-2",
      columns: ["second_value"],
      columnMeta: [{ category: null }],
      rows: [{ __col_0: 2 }],
      rowCount: 1,
      truncated: false,
      truncatedAt: 1,
    };
    const cached = {
      columns: ["first_value"],
      columnMeta: [{ category: null }],
      rows: [{ __col_0: 1 }],
      resultSets: [
        {
          id: "result-1",
          columns: ["first_value"],
          columnMeta: [{ category: null }],
          rows: [{ __col_0: 1 }],
          rowCount: 1,
          truncated: false,
          truncatedAt: 1,
        },
        selectedSet,
      ],
    };
    const { QueryPanelController } = await import(
      "../../src/extension/panels/queryPanelController"
    );
    const controller = new QueryPanelController({} as never, {
      getActiveConnectionId: () => "active",
      getInitialConnectionId: () => "active",
      getLastQueryResult: () => cached,
      postMessage: vi.fn(),
      setActiveConnectionId: vi.fn(),
      setLastQueryResult: vi.fn(),
      syncTitle: vi.fn(),
    });

    await controller.handleMessage({
      type: "exportResultsCSV",
      payload: { resultSetIndex: 1 },
    });

    expect(exportQueryResultsAsCsv).toHaveBeenCalledWith(selectedSet, {
      context: undefined,
    });
  });

  it.each([
    "exportResultsCSV",
    "exportResultsJSON",
  ] as const)("refuses %s when every result column is collapsed", async (type) => {
    const cached = {
      columns: ["id", "name"],
      columnMeta: [{ category: null }, { category: null }],
      rows: [{ __col_0: 1, __col_1: "Ada" }],
      truncated: true,
    };
    const { QueryPanelController } = await import(
      "../../src/extension/panels/queryPanelController"
    );
    const controller = new QueryPanelController({} as never, {
      getActiveConnectionId: () => "active",
      getInitialConnectionId: () => "active",
      getLastQueryResult: () => cached,
      postMessage: vi.fn(),
      setActiveConnectionId: vi.fn(),
      setLastQueryResult: vi.fn(),
      syncTitle: vi.fn(),
    });

    await controller.handleMessage({ type, payload: { columnOrder: [] } });

    expect(showWarningMessage).toHaveBeenCalledOnce();
    expect(showWarningMessage).toHaveBeenCalledWith(
      "[RapiDB] All result columns are collapsed. Expand at least one column to export.",
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
      getSchemaSnapshotState: vi.fn(() => ({ status: "loaded" })),
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
      getSchemaSnapshotState: vi.fn(() => ({ status: "loaded" })),
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
          truncatedAt: 1,
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
      getSchemaSnapshotState: vi.fn(() => ({ status: "loaded" })),
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
      getSchemaSnapshotState: vi.fn(() => ({ status: "loaded" })),
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
      getSchemaSnapshotState: vi.fn(() => ({ status: "loaded" })),
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

  it.each([
    {
      queryText: 'db.items.find({label:"create"})',
      fails: false,
      refresh: true,
    },
    {
      queryText: 'db.items.insertMany([{_id:"new"}])',
      fails: false,
      refresh: true,
    },
    {
      queryText: 'db.items.insertOne({_id:"new"});db.runCommand({invalid:1})',
      fails: true,
      refresh: true,
    },
  ])("refreshes MongoDB discovery after attempted native writes ($fails)", async ({
    queryText,
    fails,
    refresh,
  }) => {
    const { MongoDBDriver } = await import(
      "../../src/extension/dbDrivers/mongodb"
    );
    const connection = { id: "mongo", name: "QA", type: "mongodb" as const };
    const nativeDriver = new MongoDBDriver(connection);
    const query = vi.fn(async () => {
      if (fails) throw new Error("Later native command failed");
      return { columns: [], rows: [], rowCount: 0, executionTimeMs: 0 };
    });
    const manager = {
      getConnection: () => connection,
      isConnected: () => true,
      getDriver: () => ({ query }),
      getDriverCapabilities: () => nativeDriver.getCapabilities(),
      getQueryRowLimit: () => 100,
      addToHistory: vi.fn(async () => undefined),
      refreshSchemaCache: vi.fn(),
    };
    const view = {
      getActiveConnectionId: () => "mongo",
      getInitialConnectionId: () => "mongo",
      getLastQueryResult: () => null,
      postMessage: vi.fn(),
      setActiveConnectionId: vi.fn(),
      setLastQueryResult: vi.fn(),
      syncTitle: vi.fn(),
    };
    const { QueryPanelController } = await import(
      "../../src/extension/panels/queryPanelController"
    );
    await new QueryPanelController(manager as never, view).handleMessage({
      type: "executeQuery",
      payload: { connectionId: "mongo", queryText },
    });
    expect(query).toHaveBeenCalledOnce();
    expect(manager.refreshSchemaCache).toHaveBeenCalledTimes(refresh ? 1 : 0);
    expect(view.postMessage).toHaveBeenCalledWith(
      expect.objectContaining({
        type: "queryResult",
        payload: expect.objectContaining(
          fails ? { error: "Later native command failed" } : { rows: [] },
        ),
      }),
    );
  });

  it("blocks MySQL/MariaDB executable comments before query dispatch", async () => {
    async function runWithConnectionType(type: string, queryText: string) {
      const query = vi.fn(async () => ({
        columns: ["id"],
        rows: [{ id: 1 }],
        columnMeta: [],
        rowCount: 1,
        executionTimeMs: 2,
      }));
      const addToHistory = vi.fn(async () => undefined);
      const connectionManager = {
        getConnection: vi.fn(() => ({
          id: "active",
          name: "Primary",
          type,
        })),
        isConnected: vi.fn(() => true),
        connectTo: vi.fn(async () => undefined),
        addToHistory,
        addBookmark: vi.fn(async () => undefined),
        getDriver: vi.fn(() => ({ query })),
        getDriverCapabilities: vi.fn(() => ({ boundedQueryResults: true })),
        getQueryRowLimit: vi.fn(() => 50_000),
        getSchemaAsync: vi.fn(async () => []),
        getSchemaSnapshotState: vi.fn(() => ({ status: "loaded" })),
        refreshSchemaCache: vi.fn(),
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
      await new QueryPanelController(
        connectionManager as never,
        view,
      ).handleMessage({
        type: "executeQuery",
        payload: { queryText },
      });
      return { query, addToHistory, view };
    }

    const mysqlExecutable = await runWithConnectionType(
      "mysql",
      "SELECT 1 /*!50000 SELECT 1 */",
    );
    const mariaDbExecutable = await runWithConnectionType(
      "mysql",
      "/*M!100100 SELECT 1 */",
    );
    const mixedMysql = await runWithConnectionType(
      "mysql",
      String.raw`SELECT 'a\'b' AS "x\" /*!50000 , 1 AS y */`,
    );
    const mixedMariaDb = await runWithConnectionType(
      "mysql",
      String.raw`SELECT 'a\'b' AS "x\" /*M!100100 , 1 AS y */`,
    );
    for (const result of [
      mysqlExecutable,
      mariaDbExecutable,
      mixedMysql,
      mixedMariaDb,
    ]) {
      expect(result.query).not.toHaveBeenCalled();
      expect(result.addToHistory).not.toHaveBeenCalled();
      expect(result.view.postMessage).toHaveBeenCalledWith(
        expect.objectContaining({
          type: "queryResult",
          payload: expect.objectContaining({
            error: expect.stringContaining("server executes their contents"),
          }),
        }),
      );
    }

    const quotedText = await runWithConnectionType(
      "mysql",
      "SELECT '/*!50000 SELECT 1 */' AS comment_text",
    );
    expect(quotedText.query).toHaveBeenCalledOnce();

    for (const code of [0, 1, 8, 14, 31, 127]) {
      for (const wrapper of [
        "/*!50000 ordinary text */",
        "/*M!100100 ordinary text */",
      ]) {
        const queryText = `SELECT 1 --${String.fromCharCode(code)} ${wrapper}`;
        const result = await runWithConnectionType("mysql", queryText);
        expect(result.query).toHaveBeenCalledOnce();
        expect(result.query).toHaveBeenCalledWith(
          queryText,
          undefined,
          expect.any(Object),
        );
        expect(result.addToHistory).toHaveBeenCalledOnce();
        expect(result.view.postMessage).toHaveBeenCalledWith(
          expect.objectContaining({
            type: "queryResult",
            payload: expect.objectContaining({ rows: [{ id: 1 }] }),
          }),
        );
      }
    }

    const postgres = await runWithConnectionType(
      "pg",
      "SELECT 1 /*!50000 SELECT 1 */",
    );
    expect(postgres.query).toHaveBeenCalledOnce();
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
      getSchemaSnapshotState: vi.fn(() => ({ status: "loaded" })),
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

  it("keeps shared schema cache errors when pushing loaded schema updates", async () => {
    const partialSchema = [
      { database: "app_db", schema: "public", object: "users", columns: [] },
    ];
    const connectionManager = {
      getSchema: vi.fn(() => partialSchema),
      getSchemaSnapshotState: vi.fn(() => ({
        status: "error",
        error: "audit: permission denied for schema audit",
      })),
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

    await controller.handleSchemaLoaded("conn-1");

    expect(connectionManager.getSchemaSnapshotState).toHaveBeenCalledWith(
      "conn-1",
    );
    expect(view.postMessage).toHaveBeenCalledWith({
      type: "schema",
      payload: {
        connectionId: "conn-1",
        schema: partialSchema,
        error: "audit: permission denied for schema audit",
      },
    });
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

  it.each([
    { type: "pg", queryText: "DELETE FROM accounts", blocked: true },
    { type: "pg", queryText: "SELECT 1", blocked: false },
    { type: "mysql", queryText: "SELECT 1", blocked: false },
    { type: "mssql", queryText: "SELECT 1", blocked: true },
    { type: "oracle", queryText: "SELECT 1 FROM dual", blocked: true },
  ])("rechecks $type read-only policy after connecting ($queryText)", async ({
    type,
    queryText,
    blocked,
  }) => {
    let connection = { id: "active", name: "Primary", type, readOnly: false };
    const query = vi.fn(async () => ({ columns: [], rows: [], rowCount: 0 }));
    const manager = {
      getConnection: () => connection,
      isConnected: () => false,
      connectTo: vi.fn(async () => {
        connection = { ...connection, readOnly: true };
      }),
      getDriver: () => ({ query }),
      getDriverCapabilities: () => ({
        boundedQueryResults: true,
        readOnlyQueryGuard: createSqlReadOnlyQueryGuard(),
      }),
      getQueryRowLimit: () => 100,
      addToHistory: vi.fn(async () => undefined),
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
    await new QueryPanelController(manager as never, view).handleMessage({
      type: "executeQuery",
      payload: { queryText },
    });
    if (blocked) {
      expect(query).not.toHaveBeenCalled();
      expect(manager.addToHistory).not.toHaveBeenCalled();
      expect(view.postMessage).toHaveBeenCalledWith({
        type: "queryResult",
        payload: expect.objectContaining({
          error: expect.stringMatching(/read.only/i),
        }),
      });
    } else {
      expect(query).toHaveBeenCalledWith(queryText, undefined, {
        requestToken: 1,
        hardCap: 101,
        readOnly: true,
      });
    }
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
      getSchemaSnapshotState: vi.fn(() => ({ status: "loaded" })),
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

  it.each([
    {
      type: "mysql",
      dialect: "mysql" as const,
      queryText:
        "SELECT 1 AS $tag$; CREATE TEMPORARY TABLE rapidb_temp (id INT);",
    },
    {
      type: "sqlite",
      dialect: "sqlite" as const,
      queryText: "PRAGMA optimize",
    },
    {
      type: "pg",
      dialect: "postgresql" as const,
      queryText: "SELECT 1; $body$; DELETE FROM users",
    },
    {
      type: "oracle",
      dialect: "plsql" as const,
      queryText: "SELECT 1 FROM dual; SELECT q'[unterminated",
    },
    {
      type: "oracle",
      dialect: "plsql" as const,
      queryText:
        'SELECT app /* owner */ . "orders seq" /* sequence */ . NEXTVAL FROM dual',
    },
    {
      type: "mssql",
      dialect: "transactsql" as const,
      queryText: "SELECT 1; SELECT [unterminated",
    },
    {
      type: "mssql",
      dialect: "transactsql" as const,
      queryText: "SELECT NEXT VALUE FOR dbo.orders_seq",
    },
    {
      type: "mysql",
      dialect: "mysql" as const,
      queryText: "SELECT GET_LOCK #x\n('rapidb',0)",
    },
  ])("blocks unsafe $type read-only SQL in the query controller before connection/history", async ({
    type,
    dialect,
    queryText,
  }) => {
    const query = vi.fn(async () => ({
      columns: [],
      rows: [],
      rowCount: 0,
      executionTimeMs: 0,
    }));
    const connectionManager = {
      getConnection: vi.fn(() => ({
        id: "active",
        name: "Readonly",
        type,
        readOnly: true,
      })),
      getDriverCapabilities: vi.fn(() => ({
        readOnlyQueryGuard: createSqlReadOnlyQueryGuard(dialect),
      })),
      isConnected: vi.fn(() => false),
      connectTo: vi.fn(async () => undefined),
      addToHistory: vi.fn(async () => undefined),
      getDriver: vi.fn(() => ({ query })),
      getQueryRowLimit: vi.fn(() => 100),
    };
    const view = {
      getActiveConnectionId: vi.fn(() => "active"),
      getInitialConnectionId: vi.fn(() => "active"),
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
      payload: { queryText },
    });

    expect(connectionManager.connectTo).not.toHaveBeenCalled();
    expect(connectionManager.addToHistory).not.toHaveBeenCalled();
    expect(query).not.toHaveBeenCalled();
    expect(view.postMessage).toHaveBeenCalledWith(
      expect.objectContaining({
        type: "queryResult",
        payload: expect.objectContaining({
          error:
            type === "oracle"
              ? expect.stringContaining(
                  "Read-only Oracle query editor execution is blocked",
                )
              : "[RapiDB] Read-only SQL connections allow only read-only queries.",
        }),
      }),
    );
  });

  it.each([
    { readOnly: true, capabilitiesAvailable: false },
    { readOnly: true, capabilitiesAvailable: true },
    { readOnly: false, capabilitiesAvailable: false },
    { readOnly: false, capabilitiesAvailable: true },
  ])("enforces the Oracle editor boundary from saved settings: %j", async ({
    readOnly,
    capabilitiesAvailable,
  }) => {
    const query = vi.fn(async () => ({
      columns: ["N"],
      rows: [{ __col_0: 1 }],
      rowCount: 1,
      executionTimeMs: 1,
    }));
    const connectionManager = {
      getConnection: vi.fn(() => ({
        id: "oracle",
        name: "Oracle",
        type: "oracle",
        readOnly,
      })),
      getDriverCapabilities: vi.fn(() =>
        capabilitiesAvailable
          ? {
              readOnlyQueryGuard: () => ({ allowed: true as const }),
            }
          : undefined,
      ),
      isConnected: vi.fn(() => false),
      connectTo: vi.fn(async () => undefined),
      addToHistory: vi.fn(async () => undefined),
      getDriver: vi.fn(() => ({ query })),
      getQueryRowLimit: vi.fn(() => 100),
      refreshSchemaCache: vi.fn(),
    };
    const view = {
      getActiveConnectionId: () => "oracle",
      getInitialConnectionId: () => "oracle",
      getLastQueryResult: () => null,
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
        queryText: "SELECT audit_touch() FROM dual",
        operationId: "oracle-audit",
      },
    });
    if (readOnly) {
      expect(connectionManager.connectTo).not.toHaveBeenCalled();
      expect(connectionManager.addToHistory).not.toHaveBeenCalled();
      expect(query).not.toHaveBeenCalled();
      expect(view.postMessage).toHaveBeenCalledWith({
        type: "queryResult",
        payload: expect.objectContaining({
          error: expect.stringContaining(
            "Read-only Oracle query editor execution is blocked",
          ),
          connectionId: "oracle",
          operationId: "oracle-audit",
        }),
      });
    } else {
      expect(connectionManager.connectTo).toHaveBeenCalledOnce();
      expect(connectionManager.addToHistory).toHaveBeenCalledOnce();
      expect(query).toHaveBeenCalledOnce();
      expect(view.setLastQueryResult).toHaveBeenCalledOnce();
    }
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
      getSchemaSnapshotState: vi.fn(() => ({ status: "loaded" })),
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
      getSchemaSnapshotState: vi.fn(() => ({ status: "loaded" })),
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
      getSchemaSnapshotState: vi.fn(() => ({ status: "loaded" })),
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
    const driver = {
      query,
      cancelledTokens: [] as number[],
      async cancelCurrentOperation(context?: { requestToken?: number }) {
        this.cancelledTokens.push(context?.requestToken ?? -1);
      },
    };
    const cancelCurrentOperation = vi.spyOn(driver, "cancelCurrentOperation");

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
      getDriver: vi.fn(() => driver),
      getQueryRowLimit: vi.fn(() => 100),
      getSchemaAsync: vi.fn(async () => []),
      getSchemaSnapshotState: vi.fn(() => ({ status: "loaded" })),
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
    expect(driver.cancelledTokens).toEqual([1]);
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
      getSchemaSnapshotState: vi.fn(() => ({ status: "loaded" })),
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
      getSchemaSnapshotState: vi.fn(() => ({ status: "loaded" })),
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
      getSchemaSnapshotState: vi.fn(() => ({ status: "loaded" })),
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

  it("flags schema load failures recorded by the shared schema cache", async () => {
    const partialSchema = [{ object: "users" }];
    const connectionManager = {
      isConnected: vi.fn(() => true),
      getSchemaAsync: vi.fn(async () => partialSchema),
      getSchemaSnapshotState: vi.fn(() => ({
        status: "error",
        error: "permission denied for schema audit",
      })),
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

    await controller.handleMessage({
      type: "getSchema",
      payload: { connectionId: "conn-1" },
    });

    expect(connectionManager.getSchemaSnapshotState).toHaveBeenCalledWith(
      "conn-1",
    );
    expect(view.postMessage).toHaveBeenCalledWith({
      type: "schema",
      payload: {
        connectionId: "conn-1",
        schema: partialSchema,
        error: "permission denied for schema audit",
      },
    });
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
      payload: { connectionId: "conn-1", schema: [], error: "schema failed" },
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
      getSchemaSnapshotState: vi.fn(() => ({ status: "loaded" })),
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

  it.each([
    "unavailable",
    "fails",
    "hangs",
    "acknowledges",
  ])("retains the connection fence across switches when cancellation %s", async (cancellation) => {
    const result = { columns: [], rows: [], rowCount: 0, executionTimeMs: 1 };
    let finishQuery!: (value: typeof result) => void;
    const query = vi
      .fn()
      .mockImplementationOnce(
        () =>
          new Promise((resolve) => {
            finishQuery = resolve;
          }),
      )
      .mockResolvedValue(result);
    const cancelCurrentOperation = vi.fn(async () => {
      if (cancellation === "fails") throw new Error("Cancel failed");
      if (cancellation === "hangs") await new Promise<void>(() => undefined);
    });
    const driver = {
      query,
      ...(cancellation === "unavailable" ? {} : { cancelCurrentOperation }),
    };
    let activeConnectionId = "conn-1";
    const manager = {
      getConnection: (id: string) => ({ id, name: id, type: "pg" }),
      isConnected: () => true,
      getDriver: () => driver,
      getQueryRowLimit: () => 100,
      addToHistory: vi.fn(async () => undefined),
      getSchemaAsync: async () => [],
      getSchemaSnapshotState: vi.fn(() => ({ status: "loaded" })),
    };
    const view = {
      getActiveConnectionId: () => activeConnectionId,
      getInitialConnectionId: () => "conn-1",
      getLastQueryResult: () => null,
      postMessage: vi.fn(),
      setActiveConnectionId: (id: string) => {
        activeConnectionId = id;
      },
      setLastQueryResult: vi.fn(),
      syncTitle: vi.fn(),
    };
    const { QueryPanelController } = await import(
      "../../src/extension/panels/queryPanelController"
    );
    const controller = new QueryPanelController(manager as never, view);
    const running = controller.handleMessage({
      type: "executeQuery",
      payload: { queryText: "select 1" },
    });
    await vi.waitFor(() => expect(query).toHaveBeenCalledOnce());
    vi.useFakeTimers();
    try {
      for (const connectionId of ["conn-2", "conn-1"]) {
        const switching = controller.handleMessage({
          type: "activeConnectionChanged",
          payload: { connectionId },
        });
        await vi.advanceTimersByTimeAsync(1500);
        await switching;
      }
      const retry = controller.handleMessage({
        type: "executeQuery",
        payload: { queryText: "select 2" },
      });
      await vi.advanceTimersByTimeAsync(1500);
      await retry;
      expect(query).toHaveBeenCalledOnce();
      expect(view.postMessage).toHaveBeenCalledWith(
        expect.objectContaining({
          type: "queryResult",
          payload: expect.objectContaining({
            error: expect.stringContaining("previous query is still running"),
          }),
        }),
      );
      finishQuery(result);
      await running;
      await controller.handleMessage({
        type: "executeQuery",
        payload: { queryText: "select 3" },
      });
      expect(query).toHaveBeenCalledTimes(2);
      expect(view.setLastQueryResult).toHaveBeenCalledOnce();
    } finally {
      vi.useRealTimers();
    }
  });

  it.each([
    false,
    true,
  ])("retires a lost SQLite session without disconnecting a replacement (replaced=%s)", async (replaced) => {
    let lost = false;
    const driver = {
      isConnected: () => !lost,
      getAutomaticReconnectBlockReason: () =>
        lost ? "SQLite session was lost" : undefined,
      query: vi.fn(async () => {
        lost = true;
        throw new Error("SQLite execution stopped");
      }),
    };
    const connectionManager = {
      getConnection: () => ({ id: "sqlite", type: "sqlite", name: "SQLite" }),
      getDriverCapabilities: () => ({}),
      isConnected: () => true,
      getDriver: () => (lost && replaced ? {} : driver),
      getQueryRowLimit: () => 100,
      addToHistory: vi.fn(async () => undefined),
      disconnectFrom: vi.fn(async () => undefined),
    };
    const view = {
      getActiveConnectionId: () => "sqlite",
      getInitialConnectionId: () => "sqlite",
      getLastQueryResult: () => null,
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
      payload: { queryText: "SELECT 1", connectionId: "sqlite" },
    });
    expect(driver.query).toHaveBeenCalledOnce();
    expect(connectionManager.disconnectFrom).toHaveBeenCalledTimes(
      replaced ? 0 : 1,
    );
  });

  it("cancels and invalidates the owned query on connection switch and disposal", async () => {
    let activeConnectionId = "conn-1";
    let resolveQuery: ((value: unknown) => void) | undefined;
    const query = vi.fn(
      () =>
        new Promise((resolve) => {
          resolveQuery = resolve;
        }),
    );
    const driver = {
      query,
      cancellationReasons: [] as string[],
      async cancelCurrentOperation(context?: { reason?: string }) {
        this.cancellationReasons.push(context?.reason ?? "unknown");
      },
    };
    const cancelCurrentOperation = vi.spyOn(driver, "cancelCurrentOperation");
    const connectionManager = {
      getConnection: vi.fn((id: string) => ({
        id,
        name: id,
        type: "pg",
        readOnly: false,
      })),
      getDriverCapabilities: vi.fn(() => ({})),
      isConnected: vi.fn(() => true),
      getDriver: vi.fn(() => driver),
      getQueryRowLimit: vi.fn(() => 100),
      addToHistory: vi.fn(async () => undefined),
      getSchemaAsync: vi.fn(async () => []),
      getSchemaSnapshotState: vi.fn(() => ({ status: "loaded" })),
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
    expect(driver.cancellationReasons).toEqual([
      "superseded",
      "lifecycle_shutdown",
    ]);
    resolveQuery?.({ columns: [], rows: [], rowCount: 0, executionTimeMs: 1 });
    await second;
  });

  it.each([
    "in order",
    "in reverse order",
  ])("applies only the latest connection switch when superseded cancellations settle %s", async (settleOrder) => {
    let activeConnectionId = "conn-1";
    let resolveQuery: ((value: unknown) => void) | undefined;
    const cancellations: Array<() => void> = [];
    const driver = {
      query: vi.fn(
        () =>
          new Promise((resolve) => {
            resolveQuery = resolve;
          }),
      ),
      cancelCurrentOperation: vi.fn(
        () =>
          new Promise<void>((resolve) => {
            cancellations.push(resolve);
          }),
      ),
    };
    const connectionManager = {
      getConnection: vi.fn((id: string) => ({ id, name: id, type: "pg" })),
      getDriverCapabilities: vi.fn(() => ({})),
      isConnected: vi.fn(() => true),
      getDriver: vi.fn(() => driver),
      getQueryRowLimit: vi.fn(() => 100),
      addToHistory: vi.fn(async () => undefined),
      getSchemaAsync: vi.fn(async () => []),
      getSchemaSnapshotState: vi.fn(() => ({ status: "loaded" })),
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

    const running = controller.handleMessage({
      type: "executeQuery",
      payload: { queryText: "select slow", connectionId: "conn-1" },
    });
    await vi.waitFor(() => expect(driver.query).toHaveBeenCalledOnce());

    const switchToSecond = controller.handleMessage({
      type: "activeConnectionChanged",
      payload: { connectionId: "conn-2" },
    });
    const switchBack = controller.handleMessage({
      type: "activeConnectionChanged",
      payload: { connectionId: "conn-1" },
    });
    await vi.waitFor(() => expect(cancellations).toHaveLength(2));

    if (settleOrder === "in order") {
      cancellations[0]();
      await switchToSecond;
      cancellations[1]();
      await switchBack;
    } else {
      cancellations[1]();
      await switchBack;
      cancellations[0]();
      await switchToSecond;
    }

    expect(view.setActiveConnectionId).not.toHaveBeenCalledWith("conn-2");
    expect(activeConnectionId).toBe("conn-1");

    resolveQuery?.({ columns: [], rows: [], rowCount: 0, executionTimeMs: 1 });
    await running;

    view.setActiveConnectionId.mockClear();
    await controller.handleMessage({
      type: "activeConnectionChanged",
      payload: { connectionId: "conn-2" },
    });
    expect(view.setActiveConnectionId).toHaveBeenCalledWith("conn-2");
    expect(activeConnectionId).toBe("conn-2");
  });

  it("does not apply a pending connection switch after disposal", async () => {
    let activeConnectionId = "conn-1";
    let resolveQuery: ((value: unknown) => void) | undefined;
    const cancellations: Array<() => void> = [];
    const driver = {
      query: vi.fn(
        () =>
          new Promise((resolve) => {
            resolveQuery = resolve;
          }),
      ),
      cancelCurrentOperation: vi.fn(
        () =>
          new Promise<void>((resolve) => {
            cancellations.push(resolve);
          }),
      ),
    };
    const connectionManager = {
      getConnection: vi.fn((id: string) => ({ id, name: id, type: "pg" })),
      getDriverCapabilities: vi.fn(() => ({})),
      isConnected: vi.fn(() => true),
      getDriver: vi.fn(() => driver),
      getQueryRowLimit: vi.fn(() => 100),
      addToHistory: vi.fn(async () => undefined),
      getSchemaAsync: vi.fn(async () => []),
      getSchemaSnapshotState: vi.fn(() => ({ status: "loaded" })),
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

    const running = controller.handleMessage({
      type: "executeQuery",
      payload: { queryText: "select slow", connectionId: "conn-1" },
    });
    await vi.waitFor(() => expect(driver.query).toHaveBeenCalledOnce());

    const pendingSwitch = controller.handleMessage({
      type: "activeConnectionChanged",
      payload: { connectionId: "conn-2" },
    });
    await vi.waitFor(() => expect(cancellations).toHaveLength(1));

    const disposing = controller.dispose();
    await vi.waitFor(() => expect(cancellations).toHaveLength(2));
    view.postMessage.mockClear();
    for (const settle of cancellations) settle();
    await pendingSwitch;
    await disposing;

    expect(view.setActiveConnectionId).not.toHaveBeenCalled();
    expect(view.syncTitle).not.toHaveBeenCalled();
    expect(connectionManager.getSchemaAsync).not.toHaveBeenCalled();
    expect(view.postMessage).not.toHaveBeenCalledWith(
      expect.objectContaining({ type: "schema" }),
    );
    expect(activeConnectionId).toBe("conn-1");

    resolveQuery?.({ columns: [], rows: [], rowCount: 0, executionTimeMs: 1 });
    await running;
  });

  it("resolves schema pushes to the pending connection while a switch is in flight", async () => {
    let activeConnectionId = "conn-1";
    let resolveQuery: ((value: unknown) => void) | undefined;
    const cancellations: Array<() => void> = [];
    const driver = {
      query: vi.fn(
        () =>
          new Promise((resolve) => {
            resolveQuery = resolve;
          }),
      ),
      cancelCurrentOperation: vi.fn(
        () =>
          new Promise<void>((resolve) => {
            cancellations.push(resolve);
          }),
      ),
    };
    const connectionManager = {
      getConnection: vi.fn((id: string) => ({ id, name: id, type: "pg" })),
      getConnections: vi.fn(() => []),
      getQueryEditorPresentation: vi.fn(() => undefined),
      getDriverCapabilities: vi.fn(() => ({})),
      isConnected: vi.fn(() => true),
      getDriver: vi.fn(() => driver),
      getQueryRowLimit: vi.fn(() => 100),
      addToHistory: vi.fn(async () => undefined),
      getSchemaAsync: vi.fn(async () => []),
      getSchemaSnapshotState: vi.fn(() => ({ status: "loaded" })),
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

    const running = controller.handleMessage({
      type: "executeQuery",
      payload: { queryText: "select slow", connectionId: "conn-1" },
    });
    await vi.waitFor(() => expect(driver.query).toHaveBeenCalledOnce());

    const pendingSwitch = controller.handleMessage({
      type: "activeConnectionChanged",
      payload: { connectionId: "conn-2" },
    });
    await vi.waitFor(() => expect(cancellations).toHaveLength(1));

    controller.handleConnectionsChanged();
    await vi.waitFor(() =>
      expect(connectionManager.getSchemaAsync).toHaveBeenCalledWith("conn-2"),
    );
    expect(connectionManager.getSchemaAsync).not.toHaveBeenCalledWith("conn-1");

    cancellations[0]();
    await pendingSwitch;
    expect(activeConnectionId).toBe("conn-2");

    resolveQuery?.({ columns: [], rows: [], rowCount: 0, executionTimeMs: 1 });
    await running;
  });
});
