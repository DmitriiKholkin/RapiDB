import { beforeEach, describe, expect, it, vi } from "vitest";
import { registerConnectionCommands } from "../../src/extension/commands/connectionCommands";
import { connectionEditState } from "../../src/extension/connectionEditState";
import { ConnectionManager } from "../../src/extension/connectionManager";
import { VSCodeConnectionManagerStore } from "../../src/extension/connectionManagerStore";
import { ConnectionFormPanel } from "../../src/extension/panels/connectionFormPanel";
import { createWebviewShell } from "../../src/extension/panels/webviewShell";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";
import { createExtensionContextStub } from "../support/fakeConnectionManagerStore";
import type { MockWebviewPanel } from "../support/mockVscode";

vi.mock("vscode", async () => {
  const { createMockVscodeModule } = await import("../support/mockVscode");
  return createMockVscodeModule().module;
});
vi.mock("../../src/extension/panels/webviewShell", () => ({
  createWebviewShell: vi.fn(() => "<html></html>"),
}));

import * as vscode from "vscode";

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

const original: ConnectionConfig = {
  id: "a",
  name: "Original",
  type: "pg",
  host: "localhost",
  database: "app",
  username: "reader",
  useSecretStorage: true,
};

function fixture(configs = [original]) {
  let connections = structuredClone(configs);
  const context = createExtensionContextStub();
  const configuration = {
    get: (key: string) => (key === "connections" ? connections : undefined),
    inspect: () => ({ globalValue: connections }),
    update: vi.fn(async (_key: string, next: ConnectionConfig[]) => {
      connections = structuredClone(next);
    }),
  };
  vi.mocked(vscode.workspace.getConfiguration).mockReturnValue(
    configuration as never,
  );
  const store = new VSCodeConnectionManagerStore(context as never);
  const manager = new ConnectionManager(context as never, store);
  registerConnectionCommands(
    { context: context as never, connectionManager: manager, refresh: vi.fn() },
    vscode.commands.registerCommand,
  );
  const invoke = (name: string) => {
    const call = vi
      .mocked(vscode.commands.registerCommand)
      .mock.calls.find(([id]) => id === `rapidb.${name}`);
    if (!call) throw new Error(`Missing command ${name}`);
    return call[1]({ kind: "connection", connectionId: "a" }) as Promise<void>;
  };
  const openEdit = async () => {
    const count = vi.mocked(vscode.window.createWebviewPanel).mock.calls.length;
    const result = invoke("editConnection");
    await vi.waitFor(() =>
      expect(vscode.window.createWebviewPanel).toHaveBeenCalledTimes(count + 1),
    );
    const panel = vi.mocked(vscode.window.createWebviewPanel).mock.results[
      count
    ].value as MockWebviewPanel;
    return { panel, result };
  };
  return {
    context,
    store,
    manager,
    configuration,
    invoke,
    openEdit,
    connections: () => connections,
  };
}

async function save(
  panel: MockWebviewPanel,
  changes: Partial<ConnectionConfig> = {},
) {
  await panel.webview.dispatchMessage({
    type: "saveConnection",
    payload: { ...original, password: "", hasStoredSecret: true, ...changes },
  });
}

function expectConflict(panel: MockWebviewPanel, message: string) {
  expect(panel.webview.postMessage).toHaveBeenCalledWith({
    type: "saveResult",
    payload: { success: false, error: expect.stringContaining(message) },
  });
}

describe("connection edits through real commands, form, manager and VSCode store", () => {
  beforeEach(() => vi.clearAllMocks());

  it("does not resurrect a connection deleted through the tree command", async () => {
    const f = fixture();
    await f.context.secrets.store(
      "a",
      JSON.stringify({ password: "old-secret" }),
    );
    const edit = await f.openEdit();
    vi.mocked(vscode.window.showWarningMessage).mockResolvedValueOnce(
      "Delete" as never,
    );
    await f.invoke("deleteConnection");
    await save(edit.panel, { name: "Resurrected", password: "new-secret" });
    expectConflict(edit.panel, "deleted");
    expect(f.connections()).toEqual([]);
    expect(await f.context.secrets.get("a")).toBeUndefined();
    await edit.panel.webview.dispatchMessage({ type: "cancel" });
    await edit.result;
  });

  it("checks deletion again when it races save preparation", async () => {
    const f = fixture();
    await f.context.secrets.store(
      "a",
      JSON.stringify({ password: "old-secret" }),
    );
    const edit = await f.openEdit();
    const reached = deferred();
    const release = deferred();
    const get = f.context.secrets.get.bind(f.context.secrets);
    vi.spyOn(f.context.secrets, "get").mockImplementationOnce(async (id) => {
      const old = await get(id);
      reached.resolve();
      await release.promise;
      return old;
    });
    const saving = save(edit.panel);
    await reached.promise;
    vi.mocked(vscode.window.showWarningMessage).mockResolvedValueOnce(
      "Delete" as never,
    );
    await f.invoke("deleteConnection");
    release.resolve();
    await saving;
    expectConflict(edit.panel, "deleted");
    expect(f.connections()).toEqual([]);
    expect(await get("a")).toBeUndefined();
    edit.panel.dispose();
    await edit.result;
  });

  it.each([
    false,
    true,
  ])("rejects a second editor, including a secret-only change (secretOnly=%s)", async (secretOnly) => {
    const f = fixture();
    await f.context.secrets.store(
      "a",
      JSON.stringify({ password: "old-secret", futureSecret: "retain" }),
    );
    const first = await f.openEdit();
    const second = await f.openEdit();
    await save(
      first.panel,
      secretOnly ? { password: "new-secret" } : { name: "First edit" },
    );
    await first.result;
    const savedSecrets = await f.context.secrets.get("a");
    await save(second.panel, { name: "Stale edit", password: "stale-secret" });
    expectConflict(second.panel, "changed since");
    expect(f.connections()[0].name).toBe(
      secretOnly ? "Original" : "First edit",
    );
    expect(await f.context.secrets.get("a")).toBe(savedSecrets);
    expect(JSON.parse(savedSecrets ?? "null")).toEqual({
      password: secretOnly ? "new-secret" : "old-secret",
      futureSecret: "retain",
    });
    expect(
      JSON.stringify(vi.mocked(createWebviewShell).mock.calls),
    ).not.toContain("old-secret");
    second.panel.dispose();
    await second.result;
  });

  it("serializes competing saves and rejects the loser before any secret write", async () => {
    const f = fixture();
    await f.context.secrets.store(
      "a",
      JSON.stringify({ password: "old-secret" }),
    );
    const first = await f.openEdit();
    const second = await f.openEdit();
    const reached = deferred();
    const release = deferred();
    const saves = vi.spyOn(f.manager, "saveConnection");
    const storeSecret = f.context.secrets.store.bind(f.context.secrets);
    const writes = vi
      .spyOn(f.context.secrets, "store")
      .mockImplementation(async (id, value) => {
        reached.resolve();
        await release.promise;
        await storeSecret(id, value);
      });
    const savingFirst = save(first.panel, { password: "winner-secret" });
    await reached.promise;
    const savingSecond = save(second.panel, { password: "loser-secret" });
    await vi.waitFor(() => expect(saves).toHaveBeenCalledTimes(2));
    release.resolve();
    await Promise.all([savingFirst, savingSecond]);
    expectConflict(second.panel, "changed since");
    expect(writes).toHaveBeenCalledOnce();
    expect(JSON.parse((await f.context.secrets.get("a")) ?? "null")).toEqual({
      password: "winner-secret",
    });
    expect(f.connections()[0].password).toBeUndefined();
    second.panel.dispose();
    await Promise.all([first.result, second.result]);
  });

  it("allows unrelated connection edits and preserves all stored secrets on a metadata save", async () => {
    const tls = {
      mode: "mutualTls" as const,
      certFilePath: "/cert",
      keyFilePath: "/key",
    };
    const f = fixture([
      { ...original, tls },
      { ...original, id: "b" },
    ]);
    const secrets = {
      password: "db-secret",
      tlsKeyPassphrase: "tls-secret",
      futureSecret: { token: "opaque" },
    };
    await f.context.secrets.store("a", JSON.stringify(secrets));
    const edit = await f.openEdit();
    await f.manager.saveConnection({
      ...original,
      id: "b",
      name: "Independent",
    });
    await save(edit.panel, { name: "Allowed", tls });
    await edit.result;
    expect(f.connections().map(({ name }) => name)).toEqual([
      "Allowed",
      "Independent",
    ]);
    expect(JSON.parse((await f.context.secrets.get("a")) ?? "null")).toEqual(
      secrets,
    );
    expect(JSON.stringify(f.connections())).not.toContain("db-secret");
  });

  it("does not treat automatic inline-secret migration as a conflicting edit", async () => {
    const legacy = {
      ...original,
      useSecretStorage: undefined,
      password: "legacy-secret",
    };
    const f = fixture([legacy]);
    const pending = ConnectionFormPanel.show(
      f.context as never,
      f.manager,
      legacy,
    );
    await vi.waitFor(() =>
      expect(vscode.window.createWebviewPanel).toHaveBeenCalledOnce(),
    );
    f.manager.getConnections();
    await f.store.mutateConnections(() => ({ result: undefined }));
    await f.store.mutateConnections(() => ({ result: undefined }));
    const panel = vi.mocked(vscode.window.createWebviewPanel).mock.results[0]
      .value as MockWebviewPanel;
    await save(panel, { name: "After migration" });
    await pending;
    expect(f.connections()[0].name).toBe("After migration");
    expect(JSON.parse((await f.context.secrets.get("a")) ?? "null")).toEqual({
      password: "legacy-secret",
    });
  });

  it("uses create-only semantics for a new form, even if its submitted ID already exists", async () => {
    const f = fixture();
    await f.context.secrets.store(
      "a",
      JSON.stringify({ password: "keep-secret" }),
    );
    const pending = ConnectionFormPanel.show(f.context as never, f.manager);
    const panel = vi.mocked(vscode.window.createWebviewPanel).mock.results[0]
      .value as MockWebviewPanel;
    await save(panel, { password: "replacement" });
    expectConflict(panel, "already exists");
    expect(JSON.parse((await f.context.secrets.get("a")) ?? "null")).toEqual({
      password: "keep-secret",
    });
    panel.dispose();
    await pending;
  });

  it("rolls credentials back after a failed conditional write and allows retry with the original snapshot", async () => {
    const f = fixture();
    await f.context.secrets.store(
      "a",
      JSON.stringify({ password: "old-secret" }),
    );
    const precondition = {
      kind: "edit" as const,
      id: "a",
      state: connectionEditState(original, { password: "old-secret" }),
    };
    f.configuration.update.mockRejectedValueOnce(
      new Error("settings unavailable"),
    );
    await expect(
      f.manager.saveConnection(
        { ...original, name: "Changed", password: "new-secret" },
        {},
        precondition,
      ),
    ).rejects.toThrow("settings unavailable");
    expect(JSON.parse((await f.context.secrets.get("a")) ?? "null")).toEqual({
      password: "old-secret",
    });
    await f.manager.saveConnection(
      { ...original, password: "new-secret" },
      {},
      precondition,
    );
    expect(JSON.parse((await f.context.secrets.get("a")) ?? "null")).toEqual({
      password: "new-secret",
    });
  });
});
