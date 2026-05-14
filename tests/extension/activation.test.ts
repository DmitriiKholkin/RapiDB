import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { createMockVscodeModule } from "../support/mockVscode";

const expectedCommands = [
  "rapidb.addConnection",
  "rapidb.editConnection",
  "rapidb.deleteConnection",
  "rapidb.connect",
  "rapidb.disconnect",
  "rapidb.newQuery",
  "rapidb.create",
  "rapidb.openTableData",
  "rapidb.showDDL",
  "rapidb.copyNodeName",
  "rapidb.openErd",
  "rapidb.openRoutine",
  "rapidb.openHistoryEntry",
  "rapidb.openBookmarkEntry",
  "rapidb.deleteBookmark",
  "rapidb.clearBookmarks",
  "rapidb.clearHistory",
  "rapidb.disconnectAll",
  "rapidb.refresh",
];

describe("extension activation", () => {
  let vscodeState: ReturnType<typeof createMockVscodeModule>["state"];
  let connectionManagerInstance: Record<string, unknown>;
  let connectionProviderInstances: Array<{
    refresh: ReturnType<typeof vi.fn>;
    disposable: { dispose(): void };
  }>;
  let connectionFormShow: ReturnType<typeof vi.fn>;
  let connectWithProgress: ReturnType<typeof vi.fn>;
  let queryPanelCreateOrShow: ReturnType<typeof vi.fn>;
  let queryPanelDisposeAll: ReturnType<typeof vi.fn>;
  let tablePanelCreateOrShow: ReturnType<typeof vi.fn>;
  let tablePanelDisposeAll: ReturnType<typeof vi.fn>;
  let erdPanelCreateOrShow: ReturnType<typeof vi.fn>;
  let erdPanelDisposeAll: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    vi.resetModules();
    connectionProviderInstances = [];
    connectionFormShow = vi.fn();
    connectWithProgress = vi.fn();
    queryPanelCreateOrShow = vi.fn();
    queryPanelDisposeAll = vi.fn();
    tablePanelCreateOrShow = vi.fn();
    tablePanelDisposeAll = vi.fn();
    erdPanelCreateOrShow = vi.fn();
    erdPanelDisposeAll = vi.fn();

    const vscodeMock = createMockVscodeModule();
    vscodeState = vscodeMock.state;
    vscodeState.getConfiguration.mockReturnValue({
      get: vi.fn((_section: string, fallback?: unknown) => fallback),
      update: vi.fn(),
    });
    vi.doMock("vscode", () => vscodeMock.module);

    connectionManagerInstance = {
      getConnectedCount: vi.fn(() => 2),
      onDidConnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidChangeConnections: vi.fn(() => ({ dispose: vi.fn() })),
      markSchemaScopeExpanded: vi.fn(),
      markSchemaScopeCollapsed: vi.fn(),
      ensureSchemaScopeLoading: vi.fn(),
      refreshSchemaCache: vi.fn(),
      isConnected: vi.fn(() => false),
      isConnecting: vi.fn(() => false),
      getConnection: vi.fn(() => ({
        id: "conn-1",
        name: "Primary",
        type: "pg",
      })),
      disconnectFrom: vi.fn(),
      disconnectAll: vi.fn().mockResolvedValue(undefined),
      clearBookmarks: vi.fn(),
      clearHistory: vi.fn(),
      getDriver: vi.fn(),
    };

    function ConnectionManagerMock() {
      return connectionManagerInstance;
    }

    vi.doMock("../../src/extension/connectionManager", () => ({
      ConnectionManager: ConnectionManagerMock,
    }));

    function ConnectionProviderMock() {
      const instance = {
        refresh: vi.fn(),
        disposable: { dispose: vi.fn() },
      };
      connectionProviderInstances.push(instance);
      return instance;
    }

    function HistoryProviderMock() {
      return { disposable: { dispose: vi.fn() } };
    }

    function BookmarksProviderMock() {
      return { disposable: { dispose: vi.fn() } };
    }

    vi.doMock("../../src/extension/providers/connectionProvider", () => ({
      ConnectionProvider: ConnectionProviderMock,
    }));
    vi.doMock("../../src/extension/providers/historyProvider", () => ({
      HistoryProvider: HistoryProviderMock,
    }));
    vi.doMock("../../src/extension/providers/bookmarksProvider", () => ({
      BookmarksProvider: BookmarksProviderMock,
    }));
    vi.doMock("../../src/extension/panels/connectionFormPanel", () => ({
      ConnectionFormPanel: {
        show: connectionFormShow,
      },
    }));
    vi.doMock("../../src/extension/panels/queryPanel", () => ({
      QueryPanel: {
        createOrShow: queryPanelCreateOrShow,
        disposeAll: queryPanelDisposeAll,
      },
    }));
    vi.doMock("../../src/extension/panels/tablePanel", () => ({
      TablePanel: {
        createOrShow: tablePanelCreateOrShow,
        disposeAll: tablePanelDisposeAll,
      },
    }));
    vi.doMock("../../src/extension/panels/erdPanel", () => ({
      ErdPanel: {
        createOrShow: erdPanelCreateOrShow,
        disposeAll: erdPanelDisposeAll,
      },
    }));
    vi.doMock("../../src/extension/connectionManagerPrompts", () => ({
      confirmBookmarkRemoval: vi.fn(),
      confirmConnectionRemoval: vi.fn(),
      pickConnectionWithPrompt: vi.fn(),
    }));
    vi.doMock("../../src/extension/utils/connectOrchestration", () => ({
      connectWithProgress,
    }));
  });

  afterEach(async () => {
    const extension = await import("../../src/extension/extension");
    extension.deactivate();
  });

  it("registers commands and tree views exactly once on activation", async () => {
    const extension = await import("../../src/extension/extension");
    const context = { subscriptions: [] as Array<{ dispose(): void }> };

    extension.activate(context as never);
    extension.activate(context as never);

    expect(vscodeState.createTreeView).toHaveBeenCalledTimes(3);
    expect(vscodeState.registerCommand).toHaveBeenCalledTimes(
      expectedCommands.length,
    );
    expect(
      vscodeState.registerCommand.mock.calls.map(([command]) => command),
    ).toEqual(expectedCommands);
    expect(connectionProviderInstances).toHaveLength(1);
  });

  it("tracks explorer expand and collapse events with durable schema scopes", async () => {
    const extension = await import("../../src/extension/extension");
    const context = { subscriptions: [] as Array<{ dispose(): void }> };

    extension.activate(context as never);

    const explorerView = vscodeState.treeViews.find(
      (treeView) => treeView.id === "rapidb-explorer",
    );
    if (!explorerView) {
      throw new Error("Explorer tree view was not created.");
    }

    explorerView.fireDidExpandElement({
      kind: "connectionNode_connected",
      connectionId: "conn-1",
    });
    explorerView.fireDidExpandElement({
      kind: "database",
      connectionId: "conn-1",
      database: "app_db",
    });
    explorerView.fireDidExpandElement({
      kind: "schema",
      connectionId: "conn-1",
      database: "app_db",
      schema: "public",
    });
    explorerView.fireDidExpandElement({
      kind: "table",
      connectionId: "conn-1",
      database: "app_db",
      schema: "public",
      objectName: "users",
    });

    expect(
      connectionManagerInstance.markSchemaScopeExpanded,
    ).toHaveBeenCalledTimes(3);
    expect(
      connectionManagerInstance.markSchemaScopeExpanded,
    ).toHaveBeenNthCalledWith(1, "conn-1", { kind: "connectionRoot" });
    expect(
      connectionManagerInstance.markSchemaScopeExpanded,
    ).toHaveBeenNthCalledWith(2, "conn-1", {
      kind: "database",
      database: "app_db",
    });
    expect(
      connectionManagerInstance.markSchemaScopeExpanded,
    ).toHaveBeenNthCalledWith(3, "conn-1", {
      kind: "schema",
      database: "app_db",
      schema: "public",
    });
    expect(
      connectionManagerInstance.ensureSchemaScopeLoading,
    ).toHaveBeenCalledTimes(3);
    expect(
      connectionManagerInstance.ensureSchemaScopeLoading,
    ).toHaveBeenNthCalledWith(1, "conn-1", { kind: "connectionRoot" });
    expect(
      connectionManagerInstance.ensureSchemaScopeLoading,
    ).toHaveBeenNthCalledWith(2, "conn-1", {
      kind: "database",
      database: "app_db",
    });
    expect(
      connectionManagerInstance.ensureSchemaScopeLoading,
    ).toHaveBeenNthCalledWith(3, "conn-1", {
      kind: "schema",
      database: "app_db",
      schema: "public",
    });

    explorerView.fireDidCollapseElement({
      kind: "database",
      connectionId: "conn-1",
      database: "app_db",
    });
    explorerView.fireDidCollapseElement({
      kind: "connectionNode_connected",
      connectionId: "conn-1",
    });
    explorerView.fireDidCollapseElement({
      kind: "table",
      connectionId: "conn-1",
      database: "app_db",
      schema: "public",
      objectName: "users",
    });

    expect(
      connectionManagerInstance.markSchemaScopeCollapsed,
    ).toHaveBeenCalledTimes(2);
    expect(
      connectionManagerInstance.markSchemaScopeCollapsed,
    ).toHaveBeenNthCalledWith(1, "conn-1", {
      kind: "database",
      database: "app_db",
    });
    expect(
      connectionManagerInstance.markSchemaScopeCollapsed,
    ).toHaveBeenNthCalledWith(2, "conn-1", { kind: "connectionRoot" });
  });

  it("invokes the add-connection command and refreshes the explorer when a connection is saved", async () => {
    const extension = await import("../../src/extension/extension");
    const context = { subscriptions: [] as Array<{ dispose(): void }> };
    connectionFormShow.mockResolvedValue({
      id: "conn-1",
      name: "Analytics",
      type: "pg",
    });

    extension.activate(context as never);

    const addCommand = vscodeState.registerCommand.mock.calls.find(
      ([command]) => command === "rapidb.addConnection",
    )?.[1] as (() => Promise<void>) | undefined;

    if (!addCommand) {
      throw new Error("Add connection command was not registered.");
    }

    await addCommand();

    expect(connectionFormShow).toHaveBeenCalledTimes(1);
    expect(connectionProviderInstances[0]?.refresh).toHaveBeenCalledTimes(1);
    expect(vscodeState.showInformationMessage).toHaveBeenCalledWith(
      '[RapiDB] Connection "Analytics" saved.',
    );
  });

  it("opens saved history and bookmark entries with preserved query panel options", async () => {
    const extension = await import("../../src/extension/extension");
    const context = { subscriptions: [] as Array<{ dispose(): void }> };

    extension.activate(context as never);

    const openHistoryCommand = vscodeState.registerCommand.mock.calls.find(
      ([command]) => command === "rapidb.openHistoryEntry",
    )?.[1] as
      | ((entry: { connectionId?: string; sql?: string }) => void)
      | undefined;
    const openBookmarkCommand = vscodeState.registerCommand.mock.calls.find(
      ([command]) => command === "rapidb.openBookmarkEntry",
    )?.[1] as
      | ((entry: { connectionId?: string; sql?: string }) => void)
      | undefined;

    if (!openHistoryCommand || !openBookmarkCommand) {
      throw new Error("Saved query commands were not registered.");
    }

    openHistoryCommand({ connectionId: "conn-1", sql: "select 1" });
    openHistoryCommand({ connectionId: "", sql: "select ignored" });
    openBookmarkCommand({ connectionId: "conn-1", sql: "select 2" });

    expect(queryPanelCreateOrShow).toHaveBeenCalledTimes(2);
    expect(queryPanelCreateOrShow).toHaveBeenNthCalledWith(
      1,
      context,
      connectionManagerInstance,
      "conn-1",
      "select 1",
      undefined,
      undefined,
      undefined,
    );
    expect(queryPanelCreateOrShow).toHaveBeenNthCalledWith(
      2,
      context,
      connectionManagerInstance,
      "conn-1",
      "select 2",
      true,
      false,
      true,
    );
  });

  it("confirms saved entry clearing before mutating state and preserves success messages", async () => {
    const extension = await import("../../src/extension/extension");
    const context = { subscriptions: [] as Array<{ dispose(): void }> };

    vscodeState.showWarningMessage
      .mockResolvedValueOnce(undefined)
      .mockResolvedValueOnce("Clear")
      .mockResolvedValueOnce("Clear");

    extension.activate(context as never);

    const clearBookmarksCommand = vscodeState.registerCommand.mock.calls.find(
      ([command]) => command === "rapidb.clearBookmarks",
    )?.[1] as (() => Promise<void>) | undefined;
    const clearHistoryCommand = vscodeState.registerCommand.mock.calls.find(
      ([command]) => command === "rapidb.clearHistory",
    )?.[1] as (() => Promise<void>) | undefined;

    if (!clearBookmarksCommand || !clearHistoryCommand) {
      throw new Error("Clear saved entry commands were not registered.");
    }

    await clearBookmarksCommand();
    await clearBookmarksCommand();
    await clearHistoryCommand();

    expect(connectionManagerInstance.clearBookmarks).toHaveBeenCalledTimes(1);
    expect(connectionManagerInstance.clearHistory).toHaveBeenCalledTimes(1);
    expect(vscodeState.showWarningMessage).toHaveBeenNthCalledWith(
      1,
      "[RapiDB] Clear all bookmarks?",
      { modal: true },
      "Clear",
    );
    expect(vscodeState.showWarningMessage).toHaveBeenNthCalledWith(
      2,
      "[RapiDB] Clear all bookmarks?",
      { modal: true },
      "Clear",
    );
    expect(vscodeState.showWarningMessage).toHaveBeenNthCalledWith(
      3,
      "[RapiDB] Clear all query history?",
      { modal: true },
      "Clear",
    );
    expect(vscodeState.showInformationMessage).toHaveBeenNthCalledWith(
      1,
      "[RapiDB] All bookmarks cleared.",
    );
    expect(vscodeState.showInformationMessage).toHaveBeenNthCalledWith(
      2,
      "[RapiDB] Query history cleared.",
    );
  });

  it("opens ERD with selected node scope", async () => {
    const extension = await import("../../src/extension/extension");
    const context = { subscriptions: [] as Array<{ dispose(): void }> };

    (
      connectionManagerInstance.isConnected as ReturnType<typeof vi.fn>
    ).mockReturnValue(true);

    extension.activate(context as never);

    const openErdCommand = vscodeState.registerCommand.mock.calls.find(
      ([command]) => command === "rapidb.openErd",
    )?.[1] as
      | ((node: {
          connectionId?: string;
          database?: string;
          schema?: string;
        }) => Promise<void>)
      | undefined;

    if (!openErdCommand) {
      throw new Error("Open ERD command was not registered.");
    }

    await openErdCommand({
      connectionId: "conn-1",
      database: "app_db",
      schema: "public",
    });

    expect(erdPanelCreateOrShow).toHaveBeenCalledWith(
      context,
      connectionManagerInstance,
      {
        connectionId: "conn-1",
        database: "app_db",
        schema: "public",
      },
    );
  });

  it("opens materialized views as read-only data panels", async () => {
    const extension = await import("../../src/extension/extension");
    const context = { subscriptions: [] as Array<{ dispose(): void }> };

    extension.activate(context as never);

    const openTableDataCommand = vscodeState.registerCommand.mock.calls.find(
      ([command]) => command === "rapidb.openTableData",
    )?.[1] as ((node: Record<string, unknown>) => void) | undefined;

    if (!openTableDataCommand) {
      throw new Error("Open Data command was not registered.");
    }

    openTableDataCommand({
      kind: "materializedView",
      connectionId: "conn-1",
      database: "app_db",
      schema: "public",
      objectName: "daily_users",
    });

    expect(tablePanelCreateOrShow).toHaveBeenCalledWith(
      context,
      connectionManagerInstance,
      "conn-1",
      "app_db",
      "public",
      "daily_users",
      true,
    );
  });

  it("opens views as read-only data panels", async () => {
    const extension = await import("../../src/extension/extension");
    const context = { subscriptions: [] as Array<{ dispose(): void }> };

    extension.activate(context as never);

    const openTableDataCommand = vscodeState.registerCommand.mock.calls.find(
      ([command]) => command === "rapidb.openTableData",
    )?.[1] as ((node: Record<string, unknown>) => void) | undefined;

    if (!openTableDataCommand) {
      throw new Error("Open Data command was not registered.");
    }

    openTableDataCommand({
      kind: "view",
      connectionId: "conn-1",
      database: "app_db",
      schema: "app_db",
      objectName: "active_users",
    });

    expect(tablePanelCreateOrShow).toHaveBeenCalledWith(
      context,
      connectionManagerInstance,
      "conn-1",
      "app_db",
      "app_db",
      "active_users",
      true,
    );
  });

  it("opens create templates with DB-specific formatting behavior", async () => {
    const extension = await import("../../src/extension/extension");
    const context = { subscriptions: [] as Array<{ dispose(): void }> };

    (
      connectionManagerInstance.getConnection as ReturnType<typeof vi.fn>
    ).mockImplementation((id: string) => {
      if (id === "conn-2") {
        return { id, name: "Local SQLite", type: "sqlite" };
      }

      if (id === "conn-3") {
        return { id, name: "Oracle Main", type: "oracle" };
      }

      return { id, name: "Primary", type: "pg" };
    });

    extension.activate(context as never);

    const createCommand = vscodeState.registerCommand.mock.calls.find(
      ([command]) => command === "rapidb.create",
    )?.[1] as ((node: Record<string, unknown>) => void) | undefined;

    if (!createCommand) {
      throw new Error("Create command was not registered.");
    }

    createCommand({
      kind: "connectionNode_connected",
      connectionId: "conn-1",
    });
    createCommand({
      kind: "connectionNode_disconnected",
      connectionId: "conn-2",
    });
    createCommand({
      kind: "database",
      connectionId: "conn-3",
      database: "ORCLPDB1",
    });

    expect(queryPanelCreateOrShow).toHaveBeenNthCalledWith(
      1,
      context,
      connectionManagerInstance,
      "conn-1",
      expect.stringContaining("CREATE DATABASE"),
      true,
      true,
    );
    expect(queryPanelCreateOrShow).toHaveBeenNthCalledWith(
      2,
      context,
      connectionManagerInstance,
      "conn-2",
      expect.stringContaining("ATTACH DATABASE"),
      true,
      false,
    );
    expect(queryPanelCreateOrShow).toHaveBeenNthCalledWith(
      3,
      context,
      connectionManagerInstance,
      "conn-3",
      expect.stringContaining("CREATE USER"),
      true,
      true,
    );
  });

  it("opens materialized view, function, procedure, sequence, and type definitions in the SQL editor", async () => {
    const extension = await import("../../src/extension/extension");
    const context = { subscriptions: [] as Array<{ dispose(): void }> };
    const driver = {
      getCreateTableDDL: vi
        .fn()
        .mockResolvedValue(
          'CREATE MATERIALIZED VIEW "public"."daily_users" AS SELECT 1;',
        ),
      getRoutineDefinition: vi
        .fn()
        .mockImplementation(
          async (
            _database: string,
            _schema: string,
            name: string,
            kind: string,
          ) => `CREATE ${kind.toUpperCase()} "public"."${name}" AS SELECT 1;`,
        ),
      getObjectDefinition: vi
        .fn()
        .mockImplementation(
          async (
            _database: string,
            _schema: string,
            name: string,
            kind: string,
          ) =>
            kind === "sequence"
              ? `CREATE SEQUENCE "public"."${name}" START WITH 1;`
              : `CREATE TYPE "public"."${name}" AS ENUM ('active');`,
        ),
    };

    (
      connectionManagerInstance.getDriver as ReturnType<typeof vi.fn>
    ).mockReturnValue(driver);

    extension.activate(context as never);

    const showDdlCommand = vscodeState.registerCommand.mock.calls.find(
      ([command]) => command === "rapidb.showDDL",
    )?.[1] as ((node: Record<string, unknown>) => Promise<void>) | undefined;

    if (!showDdlCommand) {
      throw new Error("Show DDL command was not registered.");
    }

    await showDdlCommand({
      kind: "materializedView",
      connectionId: "conn-1",
      database: "app_db",
      schema: "public",
      objectName: "daily_users",
    });
    await showDdlCommand({
      kind: "function",
      connectionId: "conn-1",
      database: "app_db",
      schema: "public",
      objectName: "users_total",
    });
    await showDdlCommand({
      kind: "procedure",
      connectionId: "conn-1",
      database: "app_db",
      schema: "public",
      objectName: "refresh_users",
    });
    await showDdlCommand({
      kind: "sequence",
      connectionId: "conn-1",
      database: "app_db",
      schema: "public",
      objectName: "users_id_seq",
    });
    await showDdlCommand({
      kind: "type",
      connectionId: "conn-1",
      database: "app_db",
      schema: "public",
      objectName: "user_status",
    });

    expect(driver.getCreateTableDDL).toHaveBeenCalledWith(
      "app_db",
      "public",
      "daily_users",
    );
    expect(driver.getRoutineDefinition).toHaveBeenNthCalledWith(
      1,
      "app_db",
      "public",
      "users_total",
      "function",
    );
    expect(driver.getRoutineDefinition).toHaveBeenNthCalledWith(
      2,
      "app_db",
      "public",
      "refresh_users",
      "procedure",
    );
    expect(driver.getObjectDefinition).toHaveBeenNthCalledWith(
      1,
      "app_db",
      "public",
      "users_id_seq",
      "sequence",
    );
    expect(driver.getObjectDefinition).toHaveBeenNthCalledWith(
      2,
      "app_db",
      "public",
      "user_status",
      "type",
    );
    expect(queryPanelCreateOrShow).toHaveBeenNthCalledWith(
      1,
      context,
      connectionManagerInstance,
      "conn-1",
      'CREATE MATERIALIZED VIEW "public"."daily_users" AS SELECT 1;',
      true,
      true,
    );
    expect(queryPanelCreateOrShow).toHaveBeenNthCalledWith(
      2,
      context,
      connectionManagerInstance,
      "conn-1",
      'CREATE FUNCTION "public"."users_total" AS SELECT 1;',
    );
    expect(queryPanelCreateOrShow).toHaveBeenNthCalledWith(
      3,
      context,
      connectionManagerInstance,
      "conn-1",
      'CREATE PROCEDURE "public"."refresh_users" AS SELECT 1;',
    );
    expect(queryPanelCreateOrShow).toHaveBeenNthCalledWith(
      4,
      context,
      connectionManagerInstance,
      "conn-1",
      'CREATE SEQUENCE "public"."users_id_seq" START WITH 1;',
      true,
      true,
    );
    expect(queryPanelCreateOrShow).toHaveBeenNthCalledWith(
      5,
      context,
      connectionManagerInstance,
      "conn-1",
      'CREATE TYPE "public"."user_status" AS ENUM (\'active\');',
      true,
      true,
    );
  });

  it("warns when DDL for a new schema object kind is unavailable", async () => {
    const extension = await import("../../src/extension/extension");
    const context = { subscriptions: [] as Array<{ dispose(): void }> };
    const driver = {
      getObjectDefinition: vi.fn().mockResolvedValue(null),
    };

    (
      connectionManagerInstance.getDriver as ReturnType<typeof vi.fn>
    ).mockReturnValue(driver);

    extension.activate(context as never);

    const showDdlCommand = vscodeState.registerCommand.mock.calls.find(
      ([command]) => command === "rapidb.showDDL",
    )?.[1] as ((node: Record<string, unknown>) => Promise<void>) | undefined;

    if (!showDdlCommand) {
      throw new Error("Show DDL command was not registered.");
    }

    await showDdlCommand({
      kind: "sequence",
      connectionId: "conn-1",
      database: "app_db",
      schema: "public",
      objectName: "users_id_seq",
    });

    expect(driver.getObjectDefinition).toHaveBeenCalledWith(
      "app_db",
      "public",
      "users_id_seq",
      "sequence",
    );
    expect(queryPanelCreateOrShow).not.toHaveBeenCalled();
    expect(vscodeState.showWarningMessage).toHaveBeenCalledWith(
      "[RapiDB] DDL is available only for table, view, materialized view, function, procedure, sequence, type, constraint, index, and trigger nodes.",
    );
  });

  it("opens detail-node DDL in the SQL editor", async () => {
    const extension = await import("../../src/extension/extension");
    const context = { subscriptions: [] as Array<{ dispose(): void }> };
    const driver = {
      getConstraintDDL: vi
        .fn()
        .mockResolvedValue(
          "ALTER TABLE users ADD CONSTRAINT pk_users PRIMARY KEY (id);",
        ),
      getIndexDDL: vi
        .fn()
        .mockResolvedValue("CREATE INDEX users_uid_idx ON users (uid);"),
      getTriggerDDL: vi
        .fn()
        .mockResolvedValue(
          "CREATE TRIGGER users_audit AFTER INSERT ON users BEGIN SELECT 1; END;",
        ),
    };

    (
      connectionManagerInstance.getDriver as ReturnType<typeof vi.fn>
    ).mockReturnValue(driver);

    extension.activate(context as never);

    const showDdlCommand = vscodeState.registerCommand.mock.calls.find(
      ([command]) => command === "rapidb.showDDL",
    )?.[1] as ((node: Record<string, unknown>) => Promise<void>) | undefined;

    if (!showDdlCommand) {
      throw new Error("Show DDL command was not registered.");
    }

    await showDdlCommand({
      kind: "table_detail_constraint",
      connectionId: "conn-1",
      database: "app_db",
      schema: "public",
      parentTable: "users",
      objectName: "pk_users",
    });
    await showDdlCommand({
      kind: "table_detail_index",
      connectionId: "conn-1",
      database: "app_db",
      schema: "public",
      parentTable: "users",
      objectName: "users_uid_idx",
    });
    await showDdlCommand({
      kind: "table_detail_trigger",
      connectionId: "conn-1",
      database: "app_db",
      schema: "public",
      parentTable: "users",
      objectName: "users_audit",
    });

    expect(driver.getConstraintDDL).toHaveBeenCalledWith(
      "app_db",
      "public",
      "users",
      "pk_users",
    );
    expect(driver.getIndexDDL).toHaveBeenCalledWith(
      "app_db",
      "public",
      "users",
      "users_uid_idx",
    );
    expect(driver.getTriggerDDL).toHaveBeenCalledWith(
      "app_db",
      "public",
      "users",
      "users_audit",
    );
    expect(queryPanelCreateOrShow).toHaveBeenNthCalledWith(
      1,
      context,
      connectionManagerInstance,
      "conn-1",
      "ALTER TABLE users ADD CONSTRAINT pk_users PRIMARY KEY (id);",
      true,
      true,
    );
    expect(queryPanelCreateOrShow).toHaveBeenNthCalledWith(
      2,
      context,
      connectionManagerInstance,
      "conn-1",
      "CREATE INDEX users_uid_idx ON users (uid);",
      true,
      true,
    );
    expect(queryPanelCreateOrShow).toHaveBeenNthCalledWith(
      3,
      context,
      connectionManagerInstance,
      "conn-1",
      "CREATE TRIGGER users_audit AFTER INSERT ON users BEGIN SELECT 1; END;",
      true,
      true,
    );
  });

  it("opens NoSQL DDL with non-SQL presentation hints", async () => {
    const extension = await import("../../src/extension/extension");
    const context = { subscriptions: [] as Array<{ dispose(): void }> };
    const mongoDriver = {
      getCreateTableDDL: vi
        .fn()
        .mockResolvedValue(
          'db.getSiblingDB("rapidb").createView("active_users", "users", []);',
        ),
      getIndexDDL: vi
        .fn()
        .mockResolvedValue(
          'db.getSiblingDB("rapidb").getCollection("users").createIndex({ "email": 1 }, { "name": "users_by_email" });',
        ),
    };
    const dynamoDriver = {
      getCreateTableDDL: vi
        .fn()
        .mockResolvedValue(
          'aws dynamodb create-table \\\n+  --table-name "Users" \\\n+  --attribute-definitions \'[{"AttributeName":"pk","AttributeType":"S"}]\' \\\n+  --key-schema \'[{"AttributeName":"pk","KeyType":"HASH"}]\' \\\n+  --billing-mode PAY_PER_REQUEST',
        ),
    };
    const elasticsearchDriver = {
      getCreateTableDDL: vi
        .fn()
        .mockResolvedValue(
          'PUT /users\n{\n  "settings": {\n    "number_of_shards": "1"\n  }\n}',
        ),
    };

    (
      connectionManagerInstance.getConnection as ReturnType<typeof vi.fn>
    ).mockImplementation((id: string) => {
      if (id === "conn-mongo") {
        return { id, name: "Mongo", type: "mongodb" };
      }
      if (id === "conn-ddb") {
        return { id, name: "Dynamo", type: "dynamodb" };
      }
      if (id === "conn-es") {
        return { id, name: "Elastic", type: "elasticsearch" };
      }

      return { id, name: "Primary", type: "pg" };
    });
    (
      connectionManagerInstance.getDriver as ReturnType<typeof vi.fn>
    ).mockImplementation((id: string) => {
      if (id === "conn-mongo") {
        return mongoDriver;
      }
      if (id === "conn-ddb") {
        return dynamoDriver;
      }
      if (id === "conn-es") {
        return elasticsearchDriver;
      }

      return undefined;
    });

    extension.activate(context as never);

    const showDdlCommand = vscodeState.registerCommand.mock.calls.find(
      ([command]) => command === "rapidb.showDDL",
    )?.[1] as ((node: Record<string, unknown>) => Promise<void>) | undefined;

    if (!showDdlCommand) {
      throw new Error("Show DDL command was not registered.");
    }

    await showDdlCommand({
      kind: "view",
      connectionId: "conn-mongo",
      database: "rapidb",
      schema: "rapidb",
      objectName: "active_users",
    });
    await showDdlCommand({
      kind: "table_detail_index",
      connectionId: "conn-mongo",
      database: "rapidb",
      schema: "rapidb",
      parentTable: "users",
      objectName: "users_by_email",
    });
    await showDdlCommand({
      kind: "table",
      connectionId: "conn-ddb",
      database: "us-east-1",
      schema: "us-east-1",
      objectName: "Users",
    });
    await showDdlCommand({
      kind: "table",
      connectionId: "conn-es",
      database: "default",
      schema: "indices",
      objectName: "users",
    });

    expect(queryPanelCreateOrShow).toHaveBeenNthCalledWith(
      1,
      context,
      connectionManagerInstance,
      "conn-mongo",
      'db.getSiblingDB("rapidb").createView("active_users", "users", []);',
      true,
      false,
      false,
      "javascript",
    );
    expect(queryPanelCreateOrShow).toHaveBeenNthCalledWith(
      2,
      context,
      connectionManagerInstance,
      "conn-mongo",
      'db.getSiblingDB("rapidb").getCollection("users").createIndex({ "email": 1 }, { "name": "users_by_email" });',
      true,
      false,
      false,
      "javascript",
    );
    expect(queryPanelCreateOrShow).toHaveBeenNthCalledWith(
      3,
      context,
      connectionManagerInstance,
      "conn-ddb",
      expect.stringContaining("aws dynamodb create-table"),
      true,
      false,
      false,
      "plaintext",
    );
    expect(queryPanelCreateOrShow).toHaveBeenNthCalledWith(
      4,
      context,
      connectionManagerInstance,
      "conn-es",
      expect.stringContaining("PUT /users"),
      true,
      false,
      false,
      "plaintext",
    );
  });

  it("guards Show DDL for per-index unsupported NoSQL detail nodes", async () => {
    const extension = await import("../../src/extension/extension");
    const context = { subscriptions: [] as Array<{ dispose(): void }> };
    const driver = {
      getIndexDDL: vi.fn(),
    };

    (
      connectionManagerInstance.getConnection as ReturnType<typeof vi.fn>
    ).mockReturnValue({ id: "conn-1", name: "Dynamo", type: "dynamodb" });
    (
      connectionManagerInstance.getDriver as ReturnType<typeof vi.fn>
    ).mockReturnValue(driver);

    extension.activate(context as never);

    const showDdlCommand = vscodeState.registerCommand.mock.calls.find(
      ([command]) => command === "rapidb.showDDL",
    )?.[1] as ((node: Record<string, unknown>) => Promise<void>) | undefined;

    if (!showDdlCommand) {
      throw new Error("Show DDL command was not registered.");
    }

    await showDdlCommand({
      kind: "table_detail_index",
      connectionId: "conn-1",
      database: "us-east-1",
      schema: "us-east-1",
      parentTable: "Users",
      objectName: "UsersByEmail",
      ddlSupport: "unsupported",
    });

    expect(driver.getIndexDDL).not.toHaveBeenCalled();
    expect(queryPanelCreateOrShow).not.toHaveBeenCalled();
    expect(vscodeState.showWarningMessage).toHaveBeenCalledWith(
      "[RapiDB] DDL is available only for table, view, materialized view, function, procedure, sequence, type, constraint, index, and trigger nodes.",
    );
  });

  it("guards Show DDL for Elasticsearch index detail nodes via connection-type override", async () => {
    const extension = await import("../../src/extension/extension");
    const context = { subscriptions: [] as Array<{ dispose(): void }> };
    const driver = {
      getIndexDDL: vi.fn(),
    };

    (
      connectionManagerInstance.getConnection as ReturnType<typeof vi.fn>
    ).mockReturnValue({
      id: "conn-1",
      name: "Search",
      type: "elasticsearch",
    });
    (
      connectionManagerInstance.getDriver as ReturnType<typeof vi.fn>
    ).mockReturnValue(driver);

    extension.activate(context as never);

    const showDdlCommand = vscodeState.registerCommand.mock.calls.find(
      ([command]) => command === "rapidb.showDDL",
    )?.[1] as ((node: Record<string, unknown>) => Promise<void>) | undefined;

    if (!showDdlCommand) {
      throw new Error("Show DDL command was not registered.");
    }

    await showDdlCommand({
      kind: "table_detail_index",
      connectionId: "conn-1",
      database: "default",
      schema: "indices",
      parentTable: "users",
      objectName: "users_id_idx",
    });

    expect(driver.getIndexDDL).not.toHaveBeenCalled();
    expect(queryPanelCreateOrShow).not.toHaveBeenCalled();
    expect(vscodeState.showWarningMessage).toHaveBeenCalledWith(
      "[RapiDB] DDL is available only for table, view, materialized view, function, procedure, sequence, type, constraint, index, and trigger nodes.",
    );
  });

  it("guards Show DDL for unsupported NoSQL table nodes", async () => {
    const extension = await import("../../src/extension/extension");
    const context = { subscriptions: [] as Array<{ dispose(): void }> };
    const driver = {
      getCreateTableDDL: vi.fn(),
    };

    (
      connectionManagerInstance.getConnection as ReturnType<typeof vi.fn>
    ).mockReturnValue({ id: "conn-1", name: "Cache", type: "redis" });
    (
      connectionManagerInstance.getDriver as ReturnType<typeof vi.fn>
    ).mockReturnValue(driver);

    extension.activate(context as never);

    const showDdlCommand = vscodeState.registerCommand.mock.calls.find(
      ([command]) => command === "rapidb.showDDL",
    )?.[1] as ((node: Record<string, unknown>) => Promise<void>) | undefined;

    if (!showDdlCommand) {
      throw new Error("Show DDL command was not registered.");
    }

    await showDdlCommand({
      kind: "table",
      connectionId: "conn-1",
      database: "0",
      schema: "0",
      objectName: "session:*",
    });

    expect(driver.getCreateTableDDL).not.toHaveBeenCalled();
    expect(queryPanelCreateOrShow).not.toHaveBeenCalled();
    expect(vscodeState.showWarningMessage).toHaveBeenCalledWith(
      "[RapiDB] DDL is available only for table, view, materialized view, function, procedure, sequence, type, constraint, index, and trigger nodes.",
    );
  });

  it("deactivates panels and disconnects all active connections", async () => {
    const extension = await import("../../src/extension/extension");
    const context = { subscriptions: [] as Array<{ dispose(): void }> };

    extension.activate(context as never);
    extension.deactivate();

    expect(queryPanelDisposeAll).toHaveBeenCalledTimes(1);
    expect(tablePanelDisposeAll).toHaveBeenCalledTimes(1);
    expect(erdPanelDisposeAll).toHaveBeenCalledTimes(1);
    expect(connectionManagerInstance.disconnectAll).toHaveBeenCalledTimes(1);
  });
});
