import { beforeEach, describe, expect, it, vi } from "vitest";
import * as vscode from "vscode";
import { registerConnectionCommands } from "../../src/extension/commands/connectionCommands";
import { claimConnectFailureReport } from "../../src/extension/providers/connectionProvider";
import { RAPIDB_COMMANDS } from "../../src/shared/commandIds";

vi.mock("vscode", async () => {
  const { createMockVscodeModule } = await import("../support/mockVscode");
  return createMockVscodeModule().module;
});

function setup(attempt: Promise<void>, connects = true) {
  let connected = false;
  const connectionManager = {
    isConnected: vi.fn(() => connected),
    isConnecting: vi.fn(() => true),
    beginConnect: vi.fn(() => ({
      isNew: false,
      promise: attempt.then(() => {
        connected = connects;
      }),
    })),
    getConnection: vi.fn(() => ({ id: "conn-1", name: "Primary", type: "pg" })),
    disconnectFrom: vi.fn(async () => undefined),
  };
  const refresh = vi.fn();
  const commands = new Map<string, (...args: unknown[]) => unknown>();
  registerConnectionCommands(
    {
      context: {} as never,
      connectionManager: connectionManager as never,
      refresh,
    },
    (command, callback) => {
      commands.set(command, callback as (...args: unknown[]) => unknown);
      return { dispose() {} };
    },
  );
  const connect = commands.get(RAPIDB_COMMANDS.connect);
  if (!connect) throw new Error("Connect command was not registered");
  return {
    connectionManager,
    refresh,
    run: () => connect({ connectionId: "conn-1" }),
  };
}

describe("rapidb.connect while a connect attempt is in flight", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("waits for the pending attempt and refreshes after it connects", async () => {
    const test = setup(Promise.resolve());

    await test.run();

    expect(test.connectionManager.beginConnect).toHaveBeenCalledWith(
      "conn-1",
      "explicit",
    );
    expect(test.refresh).toHaveBeenCalledOnce();
    expect(vscode.window.showErrorMessage).not.toHaveBeenCalled();
    expect(vscode.window.showInformationMessage).not.toHaveBeenCalled();
  });

  it("tells the user when the pending attempt ends without connecting", async () => {
    const test = setup(Promise.resolve(), false);

    await test.run();

    expect(test.refresh).toHaveBeenCalledOnce();
    expect(vscode.window.showErrorMessage).not.toHaveBeenCalled();
    expect(vscode.window.showInformationMessage).toHaveBeenCalledWith(
      '[RapiDB] The pending connection to "Primary" was cancelled before it completed.',
    );
  });

  it("reports the pending attempt's failure", async () => {
    vi.spyOn(console, "error").mockImplementation(() => undefined);
    const test = setup(Promise.reject(new Error("connection refused")));

    await test.run();

    expect(vscode.window.showErrorMessage).toHaveBeenCalledWith(
      '[RapiDB] Cannot connect to "Primary": connection refused',
      "Edit Connection",
    );
  });

  it("does not report a failure the explorer already reported", async () => {
    const failure = new Error("connection refused");
    expect(claimConnectFailureReport(failure)).toBe(true);
    const test = setup(Promise.reject(failure));

    await test.run();

    expect(vscode.window.showErrorMessage).not.toHaveBeenCalled();
  });
});
