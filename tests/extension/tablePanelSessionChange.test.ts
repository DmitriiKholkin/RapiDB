import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { PostgresDriver } from "../../src/extension/dbDrivers/postgres";
import { TablePanel } from "../../src/extension/panels/tablePanel";
import type { ColumnTypeMeta } from "../../src/shared/tableTypes";
import { parseTablePanelMessage } from "../../src/shared/webviewContracts";
import type { FakeWebviewPanelHandle } from "../workflow/bridge/fakeWebviewPanel";

type ConfigListener = (event: {
  affectsConfiguration(section: string): boolean;
}) => void;

const state = vi.hoisted(() => ({
  panel: null as FakeWebviewPanelHandle | null,
  showWarningMessage: vi.fn(),
  configListeners: new Set<ConfigListener>(),
  serializer: undefined as
    | {
        deserializeWebviewPanel(panel: unknown, saved: unknown): Promise<void>;
      }
    | undefined,
  shellStates: [] as Array<Record<string, unknown>>,
}));

vi.mock("vscode", async () => {
  const { createFakeWebviewPanel } = await import(
    "../workflow/bridge/fakeWebviewPanel"
  );
  return {
    ViewColumn: { One: 1 },
    window: {
      createWebviewPanel: () => {
        state.panel = createFakeWebviewPanel({
          viewType: "table",
          title: "Table",
        });
        return state.panel.panel;
      },
      registerWebviewPanelSerializer: (
        _viewType: string,
        serializer: typeof state.serializer,
      ) => {
        state.serializer = serializer;
        return { dispose() {} };
      },
      showWarningMessage: state.showWarningMessage,
      showErrorMessage: vi.fn(),
    },
    workspace: {
      onDidChangeConfiguration: (listener: ConfigListener) => {
        state.configListeners.add(listener);
        return { dispose: () => state.configListeners.delete(listener) };
      },
    },
    env: { clipboard: { writeText: vi.fn() } },
  };
});
vi.mock("../../src/extension/panels/webviewShell", () => ({
  APP_WEBVIEW_SHELL_LAYOUT: {},
  WEBVIEW_SCROLLBAR_STYLES: "",
  createWebviewShell: (options: { initialState: Record<string, unknown> }) => {
    state.shellStates.push(options.initialState);
    return "<html></html>";
  },
}));

const columns: ColumnTypeMeta[] = [
  {
    name: "id",
    nativeType: "integer",
    type: "integer",
    category: "integer",
    isPrimaryKey: true,
    nullable: false,
    isForeignKey: false,
    filterable: true,
    filterOperators: [],
    valueSemantics: "plain",
  },
];

interface TestConfig {
  id: string;
  name: string;
  type: string;
  host?: string;
  database?: string;
  readOnly?: boolean;
  color?: string;
  mongoConnectionMode?: string;
  connectionUri?: string;
}

const staging: TestConfig = {
  id: "pg-session",
  name: "PG",
  type: "pg",
  host: "staging.internal",
  database: "app",
};

beforeEach(() => {
  state.showWarningMessage.mockClear();
  state.shellStates = [];
});

afterEach(() => {
  TablePanel.disposeAll();
  state.configListeners.clear();
  vi.restoreAllMocks();
});

function createManager(initial: TestConfig) {
  let current: TestConfig | undefined = { ...initial };
  let connected = true;
  let generation = 0;
  let nonce: string | undefined;
  const listeners = {
    connect: new Set<() => void>(),
    disconnect: new Set<(id: string) => void>(),
    invalidate: new Set<(id: string) => void>(),
    session: new Set<(id: string) => void>(),
  };
  const driver = new PostgresDriver(initial as never);
  const describeColumns = vi
    .spyOn(driver, "describeColumns")
    .mockResolvedValue(columns);
  const subscribe =
    <T>(set: Set<T>) =>
    (listener: T) => {
      set.add(listener);
      return { dispose: () => set.delete(listener) };
    };
  const manager = {
    getDriver: () => (connected ? driver : undefined),
    getConnection: () => current,
    getDefaultPageSize: () => 25,
    getSkipTableMutationPreview: () => false,
    getQueryEditorPresentation: () => undefined,
    isConnected: () => connected,
    connectTo: async () => {
      connected = true;
      for (const listener of listeners.connect) listener();
    },
    onDidConnect: subscribe(listeners.connect),
    onDidDisconnect: subscribe(listeners.disconnect),
    onDidInvalidateConnectionMetadata: subscribe(listeners.invalidate),
    onDidRefreshSchemas: () => ({ dispose() {} }),
    getConnectionSessionGeneration: () => generation,
    getConnectionSessionNonce: () => nonce,
    onDidChangeConnectionSession: subscribe(listeners.session),
  };
  return {
    manager,
    describeColumns,
    setConnection: (next: TestConfig | undefined) => {
      current = next && { ...next };
    },
    setSessionNonce: (next: string | undefined) => {
      nonce = next;
    },
    changeSession: (nextNonce = `nonce-${generation + 1}`) => {
      generation += 1;
      nonce = nextNonce;
      for (const listener of [...listeners.session]) listener(initial.id);
    },
    configChanged: () => {
      for (const listener of [...state.configListeners]) {
        listener({
          affectsConfiguration: (section) => section === "rapidb.connections",
        });
      }
    },
    disconnect: () => {
      connected = false;
      for (const listener of listeners.invalidate) listener(initial.id);
      for (const listener of listeners.disconnect) listener(initial.id);
    },
    reconnect: () => {
      connected = true;
      for (const listener of listeners.connect) listener();
    },
  };
}

async function openDirtyPanel(config = staging, dirty = true) {
  const connection = createManager(config);
  TablePanel.createOrShow(
    {} as never,
    connection.manager as never,
    config.id,
    "app",
    "public",
    "items",
  );
  const panel = state.panel;
  if (!panel) throw new Error("Missing panel");
  const onDispose = vi.fn();
  panel.onDidDispose(onDispose);
  await panel.dispatchMessage({ type: "ready" });
  await vi.waitFor(() =>
    expect(panel.hostMessages().at(-1)?.type).toBe("tableInit"),
  );
  if (dirty) {
    await panel.dispatchMessage({
      type: "unsavedChangesState",
      payload: { hasUnsavedChanges: true },
    });
  }
  const initialState = state.shellStates.at(-1);
  if (!initialState) throw new Error("Missing initial state");
  return { ...connection, panel, onDispose, initialState };
}

function tableInits(panel: FakeWebviewPanelHandle) {
  return panel.hostMessages().filter((message) => message.type === "tableInit");
}

function lastWarning(): string {
  return String(state.showWarningMessage.mock.calls.at(-1)?.[0] ?? "");
}

describe("PNL-2 unsaved table drafts stay bound to their connection session", () => {
  it("re-attaches drafts after a plain disconnect and reconnect", async () => {
    const s = await openDirtyPanel();
    s.disconnect();
    expect(s.onDispose).not.toHaveBeenCalled();
    expect(lastWarning()).toContain("Reconnect to apply them");

    s.reconnect();
    await vi.waitFor(() =>
      expect(tableInits(s.panel).at(-1)?.payload).toMatchObject({
        intent: "connectionRefresh",
      }),
    );
    expect(s.onDispose).not.toHaveBeenCalled();
  });

  it("ignores cosmetic profile edits", async () => {
    const s = await openDirtyPanel();
    s.setConnection({ ...staging, name: "Renamed", color: "red" });
    s.configChanged();
    expect(s.onDispose).not.toHaveBeenCalled();
    expect(state.showWarningMessage).not.toHaveBeenCalled();
  });

  it.each([
    ["host", { host: "prod.internal" }],
    ["database", { database: "prod" }],
    ["type", { type: "mysql" }],
  ])("closes a dirty panel when the profile %s changes while connected", async (_field, change) => {
    const s = await openDirtyPanel();
    s.setConnection({ ...staging, ...change });
    s.configChanged();
    expect(s.onDispose).toHaveBeenCalledOnce();
    expect(lastWarning()).toContain("connection settings changed");
    expect(lastWarning()).not.toContain("Reconnect");

    s.disconnect();
    expect(s.onDispose).toHaveBeenCalledOnce();
  });

  it("closes instead of keeping open when the session already changed at disconnect", async () => {
    const s = await openDirtyPanel();
    s.setConnection({ ...staging, database: "prod" });
    s.disconnect();
    expect(s.onDispose).toHaveBeenCalledOnce();
    expect(lastWarning()).toContain("connection settings changed");
    expect(lastWarning()).not.toContain("Reconnect");
  });

  it("does not re-attach drafts to a different database after disconnect then edit", async () => {
    const s = await openDirtyPanel();
    s.disconnect();
    expect(s.onDispose).not.toHaveBeenCalled();
    const initsBefore = tableInits(s.panel).length;

    s.setConnection({ ...staging, host: "prod.internal" });
    s.reconnect();
    await vi.waitFor(() => expect(s.onDispose).toHaveBeenCalledOnce());
    expect(tableInits(s.panel)).toHaveLength(initsBefore);
    expect(lastWarning()).toContain("connection settings changed");
  });

  it("closes a kept-open panel when its connection is removed", async () => {
    const s = await openDirtyPanel();
    s.disconnect();
    expect(s.onDispose).not.toHaveBeenCalled();

    s.setConnection(undefined);
    s.configChanged();
    expect(s.onDispose).toHaveBeenCalledOnce();
    expect(lastWarning()).toContain("connection was removed");
    expect(lastWarning()).not.toContain("Reconnect");
  });

  it("closes a dirty panel when only the credentials change the session", async () => {
    const s = await openDirtyPanel();
    s.changeSession();
    expect(s.onDispose).toHaveBeenCalledOnce();
    expect(lastWarning()).toContain("connection settings changed");
  });

  it("does not re-attach drafts after a credential change while disconnected", async () => {
    const s = await openDirtyPanel();
    s.disconnect();
    expect(s.onDispose).not.toHaveBeenCalled();
    s.changeSession();
    expect(s.onDispose).toHaveBeenCalledOnce();
    expect(lastWarning()).toContain("connection settings changed");
  });

  it("does not re-attach drafts after another window changed the session", async () => {
    const s = await openDirtyPanel();
    s.disconnect();
    const initsBefore = tableInits(s.panel).length;
    s.setSessionNonce("changed-in-another-window");
    s.reconnect();
    await vi.waitFor(() => expect(s.onDispose).toHaveBeenCalledOnce());
    expect(tableInits(s.panel)).toHaveLength(initsBefore);
    expect(lastWarning()).toContain("connection settings changed");
  });

  it("closes a dirty panel without throwing when the URI credentials become ambiguous", async () => {
    const mongo = {
      ...staging,
      type: "mongodb",
      mongoConnectionMode: "uri",
      connectionUri: "mongodb://user:secret@db/app",
    };
    const s = await openDirtyPanel(mongo);
    s.setConnection({ ...mongo, connectionUri: "mongodb://user:pa/ss@db/app" });
    expect(() => s.configChanged()).not.toThrow();
    expect(s.onDispose).toHaveBeenCalledOnce();
    expect(lastWarning()).toContain("connection settings changed");
    expect(() => s.disconnect()).not.toThrow();
  });

  it("closes an initialized clean panel on a profile edit without a warning", async () => {
    const s = await openDirtyPanel(staging, false);
    s.setConnection({ ...staging, host: "prod.internal" });
    s.configChanged();
    s.changeSession();
    expect(s.onDispose).toHaveBeenCalledOnce();
    expect(state.showWarningMessage).not.toHaveBeenCalled();
  });

  it("closes a dirty panel at disconnect when its connection no longer exists", async () => {
    const s = await openDirtyPanel();
    s.setConnection(undefined);
    s.disconnect();
    expect(s.onDispose).toHaveBeenCalledOnce();
    expect(lastWarning()).toContain("connection was removed");
  });

  describe("window reload", () => {
    async function restore(
      saved: Record<string, unknown>,
      current: TestConfig | undefined,
      withDraft = true,
      nonce?: string,
    ) {
      const connection = createManager(staging);
      connection.setConnection(current);
      connection.setSessionNonce(nonce);
      TablePanel.registerSerializer({} as never, connection.manager as never);
      const { createFakeWebviewPanel } = await import(
        "../workflow/bridge/fakeWebviewPanel"
      );
      const restored = createFakeWebviewPanel({
        viewType: "table",
        title: "Table",
      });
      const onDispose = vi.fn();
      restored.onDidDispose(onDispose);
      await state.serializer?.deserializeWebviewPanel(restored.panel, {
        initialState: saved,
        ...(withDraft
          ? {
              tableDraft: {
                tableKey: JSON.stringify([
                  staging.id,
                  "app",
                  "public",
                  "items",
                ]),
                restoreState: { entries: [] },
                newRows: [{ id: { value: 7 } }],
              },
            }
          : {}),
      });
      return { onDispose, panel: restored, connection };
    }

    it("drops a restored draft when only the credentials changed the session", async () => {
      const opened = await openDirtyPanel();
      const saved = opened.initialState;
      TablePanel.disposeAll();
      state.showWarningMessage.mockClear();

      const changed = await restore(saved, staging, true, "prod-keys");
      expect(changed.onDispose).toHaveBeenCalledOnce();
      expect(lastWarning()).toContain("connection settings changed");
      expect(JSON.stringify(saved)).not.toContain("prod-keys");
    });

    it("treats a restored draft as unsaved before the webview reports it", async () => {
      const opened = await openDirtyPanel();
      const saved = opened.initialState;
      TablePanel.disposeAll();
      state.showWarningMessage.mockClear();

      const restored = await restore(saved, staging);
      expect(restored.onDispose).not.toHaveBeenCalled();
      restored.connection.setConnection({ ...staging, host: "prod.internal" });
      restored.connection.configChanged();
      expect(restored.onDispose).toHaveBeenCalledOnce();
      expect(lastWarning()).toContain("connection settings changed");
    });

    it("keeps a restored clean panel open across a disconnected profile edit and adopts the new session", async () => {
      const opened = await openDirtyPanel();
      const saved = opened.initialState;
      TablePanel.disposeAll();
      state.showWarningMessage.mockClear();

      const clean = await restore(saved, staging, false);
      clean.connection.setConnection({ ...staging, host: "fixed.internal" });
      clean.connection.configChanged();
      expect(clean.onDispose).not.toHaveBeenCalled();
      expect(state.showWarningMessage).not.toHaveBeenCalled();

      await clean.panel.dispatchMessage({ type: "ready" });
      await vi.waitFor(() =>
        expect(tableInits(clean.panel).at(-1)?.payload).toMatchObject({
          connectionSessionKey: expect.any(String),
        }),
      );
      const adoptedKey = (
        tableInits(clean.panel).at(-1)?.payload as {
          connectionSessionKey: string;
        }
      ).connectionSessionKey;
      expect(adoptedKey).not.toBe(saved.connectionSessionKey);
      expect(clean.onDispose).not.toHaveBeenCalled();

      await clean.panel.dispatchMessage({
        type: "unsavedChangesState",
        payload: { hasUnsavedChanges: true },
      });
      clean.connection.configChanged();
      expect(clean.onDispose).not.toHaveBeenCalled();
    });

    it("does not throw on ambiguous URI credentials during restore", async () => {
      const ambiguous = {
        ...staging,
        type: "mongodb",
        mongoConnectionMode: "uri",
        connectionUri: "mongodb://user:pa/ss@db/app",
      };
      const saved = {
        connectionId: staging.id,
        database: "app",
        schema: "public",
        table: "items",
        connectionSessionKey: "saved",
      };
      const withDraft = await restore(saved, ambiguous);
      expect(withDraft.onDispose).toHaveBeenCalledOnce();
      const clean = await restore(saved, ambiguous, false);
      expect(clean.onDispose).not.toHaveBeenCalled();
    });

    it("drops a restored draft whose connection settings changed", async () => {
      const opened = await openDirtyPanel();
      const saved = opened.initialState;
      expect(typeof saved.connectionSessionKey).toBe("string");
      TablePanel.disposeAll();
      state.showWarningMessage.mockClear();

      const changed = await restore(saved, { ...staging, database: "prod" });
      expect(changed.onDispose).toHaveBeenCalledOnce();
      expect(lastWarning()).toContain("connection settings changed");

      state.showWarningMessage.mockClear();
      const unchanged = await restore(saved, { ...staging, name: "Renamed" });
      expect(unchanged.onDispose).not.toHaveBeenCalled();
      expect(state.showWarningMessage).not.toHaveBeenCalled();
    });

    it("keeps a restored clean panel after a profile change and closes one for a removed connection", async () => {
      const opened = await openDirtyPanel();
      const saved = opened.initialState;
      TablePanel.disposeAll();

      const clean = await restore(saved, { ...staging, host: "other" }, false);
      expect(clean.onDispose).not.toHaveBeenCalled();

      state.showWarningMessage.mockClear();
      const removed = await restore(saved, undefined);
      expect(removed.onDispose).toHaveBeenCalledOnce();
      expect(lastWarning()).toContain("no longer exists");
    });
  });
});

describe("PNL-2 unsavedChangesState message parsing", () => {
  it.each([
    [{ type: "unsavedChangesState" }],
    [{ type: "unsavedChangesState", payload: null }],
    [{ type: "unsavedChangesState", payload: {} }],
    [{ type: "unsavedChangesState", payload: { hasUnsavedChanges: "true" } }],
    [{ type: "unsavedChangesState", payload: { hasUnsavedChanges: 1 } }],
  ])("rejects malformed message %j", (message) => {
    expect(parseTablePanelMessage(message)).toBeNull();
  });

  it("accepts a boolean flag and ignores malformed updates in the panel", async () => {
    expect(
      parseTablePanelMessage({
        type: "unsavedChangesState",
        payload: { hasUnsavedChanges: false, extra: 1 },
      }),
    ).toEqual({
      type: "unsavedChangesState",
      payload: { hasUnsavedChanges: false },
    });

    const s = await openDirtyPanel();
    await s.panel.dispatchMessage({
      type: "unsavedChangesState",
      payload: { hasUnsavedChanges: "false" },
    });
    s.disconnect();
    expect(s.onDispose).not.toHaveBeenCalled();
  });
});
