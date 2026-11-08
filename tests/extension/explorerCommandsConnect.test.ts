import { beforeEach, describe, expect, it, vi } from "vitest";

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

async function registerCommands() {
  const { registerExplorerCommands } = await import(
    "../../src/extension/commands/explorerCommands"
  );
  const commands = new Map<string, (...args: unknown[]) => unknown>();
  const connectionManager = {
    isConnected: vi.fn(() => false),
    getDriver: vi.fn(() => undefined),
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
