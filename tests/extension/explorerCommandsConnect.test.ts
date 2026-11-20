import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  connectWithProgress: vi.fn(),
  queryCreateOrShow: vi.fn(),
  erdCreateOrShow: vi.fn(),
  showErrorMessage: vi.fn(),
}));

vi.mock("vscode", () => ({
  window: { showErrorMessage: mocks.showErrorMessage },
}));
vi.mock("../../src/extension/utils/connectOrchestration", () => ({
  connectWithProgress: mocks.connectWithProgress,
}));
vi.mock("../../src/extension/panels/queryPanel", () => ({
  QueryPanel: { createOrShow: mocks.queryCreateOrShow },
}));
vi.mock("../../src/extension/panels/erdPanel", () => ({
  ErdPanel: { createOrShow: mocks.erdCreateOrShow },
}));
vi.mock("../../src/extension/panels/tablePanel", () => ({
  TablePanel: { createOrShow: vi.fn() },
}));

async function registerCommands(overrides: Record<string, unknown> = {}) {
  const { registerExplorerCommands } = await import(
    "../../src/extension/commands/explorerCommands"
  );
  const commands = new Map<string, (...args: unknown[]) => unknown>();
  const connectionManager = {
    isConnected: vi.fn(() => false),
    getDriver: vi.fn(() => undefined),
    getConnectedDriverSessionKey: vi.fn(() => "source-session"),
    initializeConnectionCredentialRevision: vi.fn(() => undefined),
    ...overrides,
  };
  registerExplorerCommands(
    {
      context: {} as never,
      connectionManager: connectionManager as never,
      refresh: vi.fn(),
    },
    (command, callback) => {
      commands.set(command, callback as (...args: unknown[]) => unknown);
      return { dispose: vi.fn() };
    },
  );
  return commands;
}

describe("explorer commands when connect does not complete", () => {
  beforeEach(() => {
    mocks.connectWithProgress.mockResolvedValue(false);
  });

  it("does not open a query editor for a cancelled connect", async () => {
    const commands = await registerCommands();

    await commands.get("rapidb.newQuery")?.({ connectionId: "conn-1" });

    expect(mocks.connectWithProgress).toHaveBeenCalled();
    expect(mocks.queryCreateOrShow).not.toHaveBeenCalled();
    expect(mocks.showErrorMessage).not.toHaveBeenCalled();
  });

  it("does not open an ERD for a cancelled connect", async () => {
    const commands = await registerCommands();

    await commands.get("rapidb.openErd")?.({
      connectionId: "conn-1",
      database: "app",
      schema: "public",
    });

    expect(mocks.connectWithProgress).toHaveBeenCalled();
    expect(mocks.erdCreateOrShow).not.toHaveBeenCalled();
  });
});

describe("explorer driver error diagnostics", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it.each([
    ["rapidb.newQuery", "Cannot connect"],
    ["rapidb.showDDL", "DDL error"],
    ["rapidb.openRoutine", "Cannot load function definition"],
  ])("redacts bare credentials in %s logs and notifications", async (command, heading) => {
    const password = "synthetic-driver-password-11";
    const failure = new Error(`Permission denied for ${password}`);
    const log = vi.spyOn(console, "error").mockImplementation(() => {});
    const driver = {
      getCreateTableDDL: vi.fn().mockRejectedValue(failure),
      getRoutineDefinition: vi.fn().mockRejectedValue(failure),
    };
    if (command === "rapidb.newQuery") {
      mocks.connectWithProgress.mockRejectedValueOnce(failure);
    }
    const commands = await registerCommands({
      isConnected: () => command !== "rapidb.newQuery",
      getConnection: () => ({ type: "pg" }),
      getConnectionDiagnosticContext: () => ({ password }),
      getDriver: () => driver,
      getDriverEntityManifest: () => undefined,
    });

    await commands.get(command)?.({
      connectionId: "conn-1",
      kind: command === "rapidb.openRoutine" ? "function" : "table",
      objectName: "example",
      database: "app_db",
    });

    expect(mocks.showErrorMessage).toHaveBeenCalledExactlyOnceWith(
      `[RapiDB] ${heading}: Permission denied for [REDACTED]`,
    );
    expect(log).toHaveBeenCalledExactlyOnceWith(
      expect.stringContaining("[RapiDB]"),
      expect.stringContaining("Permission denied for [REDACTED]"),
    );
    expect(JSON.stringify(log.mock.calls)).not.toContain(password);
    expect(mocks.queryCreateOrShow).not.toHaveBeenCalled();
  });
});

describe("PostgreSQL definition database routing", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it.each([
    ["rapidb.showDDL", "table", true],
    ["rapidb.showDDL", "function", false],
    ["rapidb.openRoutine", "function", false],
  ])("passes the literal database from %s (%s) into the editor", async (command, kind, forceNew) => {
    const database = '  Case:"δοκιμή"/?%  ';
    const lookup = vi.fn().mockResolvedValue("selected definition");
    const commands = await registerCommands({
      isConnected: () => true,
      getConnection: () => ({ type: "pg", database: "default_db" }),
      getDriver: () => ({
        getCreateTableDDL: lookup,
        getRoutineDefinition: lookup,
      }),
      getDriverEntityManifest: () => undefined,
      getQueryEditorPresentation: () => undefined,
    });
    await commands.get(command)?.({
      connectionId: "conn-1",
      kind,
      objectName: "example",
      database,
      schema: "public",
    });
    expect(lookup).toHaveBeenCalledExactlyOnceWith(
      database,
      "public",
      "example",
      ...(kind === "function" ? ["function", undefined] : []),
    );
    expect(mocks.queryCreateOrShow).toHaveBeenCalledExactlyOnceWith(
      expect.anything(),
      expect.anything(),
      "conn-1",
      "selected definition",
      forceNew,
      forceNew ? true : undefined,
      false,
      undefined,
      database,
      { connectionId: "conn-1", sessionKey: "source-session" },
    );
  });

  it.each([
    ["rapidb.showDDL", "table"],
    ["rapidb.openRoutine", "function"],
  ])("does not fetch from the default database when %s has no selected database", async (command, kind) => {
    const lookup = vi.fn().mockResolvedValue("default database definition");
    const commands = await registerCommands({
      isConnected: () => true,
      getConnection: () => ({ type: "pg", database: "default_db" }),
      getDriver: () => ({
        getCreateTableDDL: lookup,
        getRoutineDefinition: lookup,
      }),
      getDriverEntityManifest: () => undefined,
    });
    await commands.get(command)?.({
      connectionId: "conn-1",
      kind,
      objectName: "example",
    });
    expect(lookup).not.toHaveBeenCalled();
    expect(mocks.queryCreateOrShow).not.toHaveBeenCalled();
    expect(mocks.showErrorMessage).toHaveBeenCalledWith(
      expect.stringContaining("database is unknown"),
    );
  });
});
