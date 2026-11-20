import { beforeEach, describe, expect, it, vi } from "vitest";
import type { ConnectionConfig } from "../../src/extension/connectionManager";
import type { IDBDriver } from "../../src/extension/dbDrivers/types";
import {
  createExtensionContextStub,
  FakeConnectionManagerStore,
} from "../support/fakeConnectionManagerStore";

const vscodeMock = vi.hoisted(() => {
  const serializers = new Map<string, unknown>();
  const createWebviewPanel = vi.fn(() => {
    const disposeListeners = new Set<() => void>();
    const messageListeners = new Set<
      (message: unknown) => void | Promise<void>
    >();
    const panel = {
      title: "",
      webview: {
        html: "",
        postMessage: vi.fn(),
        onDidReceiveMessage(
          listener: (message: unknown) => void | Promise<void>,
        ) {
          messageListeners.add(listener);
          return {
            dispose: () => {
              messageListeners.delete(listener);
            },
          };
        },
        async dispatchMessage(message: unknown) {
          for (const listener of messageListeners) {
            await listener(message);
          }
        },
      },
      onDidDispose(listener: () => void) {
        disposeListeners.add(listener);
        return {
          dispose: () => {
            disposeListeners.delete(listener);
          },
        };
      },
      dispose() {
        for (const listener of disposeListeners) {
          listener();
        }
      },
      reveal: vi.fn(),
    };
    return panel;
  });
  const registerWebviewPanelSerializer = vi.fn(
    (viewType: string, serializer: unknown) => {
      serializers.set(viewType, serializer);
      return { dispose: vi.fn() };
    },
  );
  const withProgress = vi.fn(
    (_options: unknown, run: (...args: unknown[]) => unknown) => run(),
  );

  return {
    createWebviewPanel,
    registerWebviewPanelSerializer,
    serializers,
    withProgress,
    module: {
      EventEmitter: class EventEmitter<T> {
        private readonly listeners = new Set<(value: T) => void>();
        readonly event = (listener: (value: T) => void) => {
          this.listeners.add(listener);
          return { dispose: () => this.listeners.delete(listener) };
        };
        fire(value: T) {
          for (const listener of this.listeners) listener(value);
        }
        dispose() {
          this.listeners.clear();
        }
      },
      ProgressLocation: { Notification: 15 },
      ViewColumn: { One: 1 },
      window: {
        createWebviewPanel,
        registerWebviewPanelSerializer,
        withProgress,
        showErrorMessage: vi.fn(),
        showWarningMessage: vi.fn(),
      },
      workspace: {
        onDidChangeConfiguration: vi.fn(() => ({ dispose: vi.fn() })),
      },
    },
  };
});

vi.mock("vscode", () => vscodeMock.module);
vi.mock("../../src/extension/panels/webviewShell", () => ({
  APP_WEBVIEW_SHELL_LAYOUT: {
    htmlStyles: "height: 100%; overflow: hidden;",
    bodyStyles: "height: 100%; overflow: hidden;",
    rootStyles: "height: 100vh;",
  },
  FULLSCREEN_WEBVIEW_SHELL_LAYOUT: {
    htmlStyles: "height: 100%; overflow: hidden;",
    bodyStyles: "height: 100%; overflow: hidden;",
    rootStyles: "height: 100vh; overflow: hidden;",
  },
  WEBVIEW_SCROLLBAR_STYLES: "",
  MONACO_SCROLLBAR_STYLES: "",
  createWebviewShell: vi.fn(() => "<html></html>"),
}));

function createdPanel() {
  return vscodeMock.createWebviewPanel.mock.results[0]?.value;
}

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
      for (const listener of listeners) {
        listener(value);
      }
    },
  };
}

class SessionStore extends FakeConnectionManagerStore {
  private readonly nonces = new Map<string, string>();
  readConnectionSessionNonce(id: string) {
    return this.nonces.get(id);
  }
  async writeConnectionSessionNonce(id: string, nonce: string) {
    this.nonces.set(id, nonce);
  }
}

class CredentialSessionStore extends SessionStore {
  private readonly changes = createEventSource<string>();
  readonly onDidChangeSecret = this.changes.event;
  notifySecretChange(id: string) {
    this.changes.fire(id);
  }
}

const originalTarget: ConnectionConfig = {
  id: "stable-profile",
  name: "Shared name",
  type: "pg",
  host: "original.invalid",
  database: "original",
  username: "host-test",
  password: "test-only-secret",
  useSecretStorage: false,
};

function queryDriver(database: string) {
  let connected = false;
  return {
    connect: vi.fn(async () => {
      connected = true;
    }),
    disconnect: vi.fn(async () => {
      connected = false;
    }),
    isConnected: () => connected,
    cancelCurrentOperation: vi.fn(async () => undefined),
    query: vi.fn(async (_text: string) => ({
      columns: ["database"],
      rows: [{ __col_0: database }],
      rowCount: 1,
      executionTimeMs: 1,
    })),
  };
}

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((finish) => {
    resolve = finish;
  });
  return { promise, resolve };
}

async function queryTargetFixture(store = new SessionStore()) {
  const { QueryPanel } = await import("../../src/extension/panels/queryPanel");
  const { ConnectionManager } = await import(
    "../../src/extension/connectionManager"
  );
  QueryPanel.disposeAll();
  const context = createExtensionContextStub() as never;
  const manager = new ConnectionManager(context, store);
  const drivers = vi.spyOn(
    manager as unknown as {
      createDriver(config: ConnectionConfig): IDBDriver;
    },
    "createDriver",
  );
  vi.spyOn(manager, "getSchemaAsync").mockResolvedValue([]);
  vi.spyOn(manager, "getQueryEditorPresentation").mockReturnValue(undefined);
  vi.spyOn(manager, "getDriverCapabilities").mockReturnValue({
    boundedQueryResults: true,
    tabularRead: "sql",
  });
  return { QueryPanel, manager, store, context, drivers };
}

const definitionDatabase = '  Case:"δοκιμή"/?%  ';
const definitionSchema = "private_catalog";

function definitionDriver(text: string, hold?: () => Promise<void>) {
  const lookup = async () => {
    await hold?.();
    return text;
  };
  return {
    ...queryDriver(definitionDatabase),
    getCreateTableDDL: vi.fn(lookup),
    getRoutineDefinition: vi.fn(lookup),
  };
}

async function definitionCommands(
  manager: Awaited<ReturnType<typeof queryTargetFixture>>["manager"],
  context: Awaited<ReturnType<typeof queryTargetFixture>>["context"],
) {
  const { registerExplorerCommands } = await import(
    "../../src/extension/commands/explorerCommands"
  );
  const commands = new Map<string, (...args: unknown[]) => unknown>();
  registerExplorerCommands(
    { context, connectionManager: manager, refresh: vi.fn() },
    (name, callback) => {
      commands.set(name, callback as (...args: unknown[]) => unknown);
      return { dispose: vi.fn() };
    },
  );
  return (command: "showDDL" | "openRoutine") => {
    const registered = commands.get(`rapidb.${command}`);
    if (!registered) throw new Error(`Missing ${command}`);
    return registered({
      connectionId: originalTarget.id,
      database: definitionDatabase,
      schema: definitionSchema,
      kind: command === "showDDL" ? "table" : "function",
      objectName: "selected",
      detailKey: "oid:123",
    });
  };
}

describe("Explorer definition source binding", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it.each([
    ["showDDL", "repoint"],
    ["openRoutine", "repoint"],
    ["showDDL", "reconnect"],
  ] as const)("discards an outstanding %s lookup after a same-ID %s", async (command, change) => {
    const { QueryPanel, manager, context, drivers } =
      await queryTargetFixture();
    const run = await definitionCommands(manager, context);
    const started = deferred();
    const finish = deferred();
    const oldDriver = definitionDriver("old private definition", async () => {
      started.resolve();
      await finish.promise;
    });
    const newDriver = definitionDriver("replacement private definition");
    drivers
      .mockReturnValueOnce(oldDriver as unknown as IDBDriver)
      .mockReturnValue(newDriver as unknown as IDBDriver);
    const id = originalTarget.id;
    try {
      await manager.saveConnection(originalTarget);
      await manager.connectTo(id);
      const sourceKey = manager.getQueryTargetSessionKey(id);
      const pending = run(command);
      await started.promise;
      if (change === "repoint") {
        await manager.saveConnection({
          ...originalTarget,
          host: "replacement.invalid",
        });
        expect(manager.getQueryTargetSessionKey(id)).not.toBe(sourceKey);
      } else {
        await manager.disconnectFrom(id);
        expect(manager.getQueryTargetSessionKey(id)).toBe(sourceKey);
      }
      await manager.connectTo(id);
      expect(manager.getDriver(id)).toBe(newDriver);
      finish.resolve();
      await pending;

      expect(vscodeMock.createWebviewPanel).not.toHaveBeenCalled();
      expect(
        vscodeMock.module.window.showWarningMessage,
      ).toHaveBeenCalledExactlyOnceWith(
        expect.stringContaining("Refresh the Explorer"),
      );
      expect(newDriver.getCreateTableDDL).not.toHaveBeenCalled();
      expect(newDriver.getRoutineDefinition).not.toHaveBeenCalled();
    } finally {
      finish.resolve();
      QueryPanel.disposeAll();
      await manager.dispose();
    }
  });

  it("does not read a routine from a replacement connected while Explorer awaits progress", async () => {
    const { QueryPanel, manager, context, drivers } =
      await queryTargetFixture();
    const run = await definitionCommands(manager, context);
    const connecting = deferred();
    const finishConnect = deferred();
    const progressHeld = deferred();
    const finishProgress = deferred();
    const oldDriver = definitionDriver("cancelled definition");
    oldDriver.connect.mockImplementationOnce(() => {
      connecting.resolve();
      return finishConnect.promise;
    });
    const newDriver = definitionDriver("replacement definition");
    drivers
      .mockReturnValueOnce(oldDriver as unknown as IDBDriver)
      .mockReturnValue(newDriver as unknown as IDBDriver);
    vscodeMock.withProgress.mockImplementationOnce(async (_options, run) => {
      await run();
      progressHeld.resolve();
      await finishProgress.promise;
    });
    const id = originalTarget.id;
    try {
      await manager.saveConnection(originalTarget);
      const pending = run("openRoutine");
      await connecting.promise;
      await manager.saveConnection({
        ...originalTarget,
        host: "replacement.invalid",
      });
      await progressHeld.promise;
      await manager.connectTo(id);
      finishProgress.resolve();
      finishConnect.resolve();
      await pending;

      expect(oldDriver.getRoutineDefinition).not.toHaveBeenCalled();
      expect(newDriver.getRoutineDefinition).not.toHaveBeenCalled();
      expect(vscodeMock.createWebviewPanel).not.toHaveBeenCalled();
      expect(vscodeMock.module.window.showWarningMessage).toHaveBeenCalledWith(
        expect.stringContaining("Refresh the Explorer"),
      );
    } finally {
      finishProgress.resolve();
      finishConnect.resolve();
      QueryPanel.disposeAll();
      await manager.dispose();
    }
  });

  it("does not start a routine connection after the profile changes during initial credential verification", async () => {
    const store = new CredentialSessionStore();
    const { QueryPanel, manager, context, drivers } =
      await queryTargetFixture(store);
    const run = await definitionCommands(manager, context);
    const started = deferred();
    const finish = deferred();
    const driver = definitionDriver("replacement definition");
    drivers.mockReturnValue(driver as unknown as IDBDriver);
    try {
      await manager.saveConnection({
        ...originalTarget,
        useSecretStorage: true,
      });
      const readSecret = store.getSecret.bind(store);
      vi.spyOn(store, "getSecret").mockImplementationOnce(async (key) => {
        started.resolve();
        await finish.promise;
        return readSecret(key);
      });
      const pending = run("openRoutine");
      await started.promise;
      store.setConnections([
        {
          ...originalTarget,
          useSecretStorage: true,
          host: "replacement.invalid",
        },
      ]);
      store.fireConfigurationChange("rapidb.connections");
      finish.resolve();
      await pending;

      expect(drivers).not.toHaveBeenCalled();
      expect(vscodeMock.withProgress).not.toHaveBeenCalled();
      expect(vscodeMock.createWebviewPanel).not.toHaveBeenCalled();
      expect(vscodeMock.module.window.showWarningMessage).toHaveBeenCalledWith(
        expect.stringContaining("Refresh the Explorer"),
      );
    } finally {
      finish.resolve();
      QueryPanel.disposeAll();
      await manager.dispose();
    }
  });

  it("waits for an observed credential revision to settle before opening a loaded definition", async () => {
    const store = new CredentialSessionStore();
    const { QueryPanel, manager, context, drivers } =
      await queryTargetFixture(store);
    const run = await definitionCommands(manager, context);
    const started = deferred();
    const finish = deferred();
    const readStarted = deferred();
    const finishRead = deferred();
    const recheckStarted = deferred();
    const oldDriver = definitionDriver(
      "definition with old credentials",
      async () => {
        started.resolve();
        await finish.promise;
      },
    );
    drivers.mockReturnValue(oldDriver as unknown as IDBDriver);
    const id = originalTarget.id;
    try {
      await manager.saveConnection({
        ...originalTarget,
        useSecretStorage: true,
      });
      await manager.connectTo(id);
      const key = manager.getQueryTargetSessionKey(id);
      const pending = run("showDDL");
      await started.promise;
      const { withConnectionCredentialRevision } = await import(
        "../../src/extension/connectionSecretsData"
      );
      const rotated = withConnectionCredentialRevision(
        JSON.stringify({ password: "rotated-synthetic-password" }),
        "00000000-0000-4000-8000-000000000002",
      );
      store.setSecret(id, rotated);
      vi.spyOn(store, "getSecret").mockImplementationOnce(async () => {
        readStarted.resolve();
        await finishRead.promise;
        return rotated;
      });
      const initialize =
        manager.initializeConnectionCredentialRevision.bind(manager);
      vi.spyOn(
        manager,
        "initializeConnectionCredentialRevision",
      ).mockImplementationOnce((connectionId) => {
        recheckStarted.resolve();
        return initialize(connectionId);
      });
      store.notifySecretChange(id);
      await readStarted.promise;
      finish.resolve();
      await Promise.race([recheckStarted.promise, pending]);
      expect(vscodeMock.createWebviewPanel).not.toHaveBeenCalled();
      finishRead.resolve();
      await pending;

      expect(manager.getQueryTargetSessionKey(id)).not.toBe(key);
      expect(vscodeMock.createWebviewPanel).not.toHaveBeenCalled();
      expect(vscodeMock.module.window.showWarningMessage).toHaveBeenCalledWith(
        expect.stringContaining("Refresh the Explorer"),
      );
    } finally {
      finish.resolve();
      finishRead.resolve();
      QueryPanel.disposeAll();
      await manager.dispose();
    }
  });

  it("carries the verified binding through panel creation rather than resampling the profile", async () => {
    const { QueryPanel, manager, context, drivers, store } =
      await queryTargetFixture();
    const run = await definitionCommands(manager, context);
    const { createWebviewShell } = await import(
      "../../src/extension/panels/webviewShell"
    );
    const driver = definitionDriver("select current_database()");
    drivers.mockReturnValue(driver as unknown as IDBDriver);
    const id = originalTarget.id;
    try {
      await manager.saveConnection(originalTarget);
      await manager.connectTo(id);
      const sourceKey = manager.getQueryTargetSessionKey(id);
      const create = vscodeMock.createWebviewPanel.getMockImplementation();
      vscodeMock.createWebviewPanel.mockImplementationOnce(() => {
        store.setConnections([
          { ...originalTarget, host: "replacement.invalid" },
        ]);
        store.fireConfigurationChange("rapidb.connections");
        if (!create) throw new Error("Missing webview panel fixture");
        return create();
      });

      await run("showDDL");
      const replacementKey = manager.getQueryTargetSessionKey(id);
      expect(replacementKey).not.toBe(sourceKey);
      expect(createWebviewShell).toHaveBeenLastCalledWith(
        expect.objectContaining({
          initialState: expect.objectContaining({
            postgresDatabase: definitionDatabase,
            targetBinding: { connectionId: id, sessionKey: sourceKey },
          }),
        }),
      );
      const panel = createdPanel();
      if (!panel) throw new Error("No query panel");
      await panel.webview.dispatchMessage({
        type: "executeQuery",
        payload: {
          queryText: "select current_database()",
          connectionId: id,
          targetSessionKey: replacementKey,
        },
      });
      expect(driver.query).not.toHaveBeenCalled();
      expect(panel.webview.postMessage).toHaveBeenCalledWith({
        type: "queryResult",
        payload: expect.objectContaining({
          error: expect.stringContaining("confirm"),
        }),
      });
    } finally {
      QueryPanel.disposeAll();
      await manager.dispose();
    }
  });

  it.each([
    "showDDL",
    "openRoutine",
  ] as const)("opens and restores a verified %s on its exact PostgreSQL database after a presentation edit", async (command) => {
    const { QueryPanel, manager, context, drivers } =
      await queryTargetFixture();
    const run = await definitionCommands(manager, context);
    const { createWebviewShell } = await import(
      "../../src/extension/panels/webviewShell"
    );
    const id = originalTarget.id;
    const text = "select current_database()";
    const started = deferred();
    const finish = deferred();
    const driver = definitionDriver(text, async () => {
      started.resolve();
      await finish.promise;
    });
    drivers.mockReturnValue(driver as unknown as IDBDriver);
    try {
      await manager.saveConnection(originalTarget);
      // Open Routine's ordinary cold path should connect and then read.
      if (command === "showDDL") await manager.connectTo(id);
      const key = manager.getQueryTargetSessionKey(id);
      const pending = run(command);
      await started.promise;
      await manager.saveConnection({ ...originalTarget, name: "Renamed" });
      finish.resolve();
      await pending;
      expect(
        vscodeMock.module.window.showWarningMessage,
      ).not.toHaveBeenCalled();
      expect(vscodeMock.withProgress).toHaveBeenCalledTimes(
        command === "openRoutine" ? 1 : 0,
      );
      const lookup =
        command === "showDDL"
          ? driver.getCreateTableDDL
          : driver.getRoutineDefinition;
      expect(lookup).toHaveBeenCalledExactlyOnceWith(
        definitionDatabase,
        definitionSchema,
        "selected",
        ...(command === "openRoutine" ? ["function", "oid:123"] : []),
      );
      expect(vscodeMock.createWebviewPanel).toHaveBeenCalledOnce();
      const initialState =
        vi.mocked(createWebviewShell).mock.lastCall?.[0].initialState;
      expect(initialState).toMatchObject({
        postgresDatabase: definitionDatabase,
        queryText: text,
        targetBinding: { connectionId: id, sessionKey: key },
      });

      QueryPanel.disposeAll();
      QueryPanel.registerSerializer(context, manager);
      const serializer = vscodeMock.serializers.get("rapidb.queryPanel") as {
        deserializeWebviewPanel(panel: unknown, state: unknown): Promise<void>;
      };
      const restored = vscodeMock.createWebviewPanel();
      await serializer.deserializeWebviewPanel(restored, { initialState });
      expect(restored.title).toContain(JSON.stringify(definitionDatabase));
      await restored.webview.dispatchMessage({
        type: "executeQuery",
        payload: { connectionId: id, targetSessionKey: key, queryText: text },
      });
      expect(driver.query).toHaveBeenCalledExactlyOnceWith(
        text,
        undefined,
        expect.objectContaining({ database: definitionDatabase }),
      );
    } finally {
      finish.resolve();
      QueryPanel.disposeAll();
      await manager.dispose();
    }
  });
});

describe("QueryPanel", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vscodeMock.serializers.clear();
  });

  it("requires explicit confirmation for a repointed saved query or a legacy entry", async () => {
    const { QueryPanel, manager, context, drivers } =
      await queryTargetFixture();
    const { registerSavedEntryCommands } = await import(
      "../../src/extension/commands/savedEntryCommands"
    );
    const commands = new Map<string, (...args: unknown[]) => unknown>();
    registerSavedEntryCommands(
      { context, connectionManager: manager },
      (name, run) => {
        commands.set(name, run as (...args: unknown[]) => unknown);
        return { dispose: vi.fn() };
      },
    );
    async function open(command: string, entry: unknown) {
      const run = commands.get(command);
      if (!run) throw new Error(`Missing ${command}`);
      await run(entry);
      const panel = createdPanel();
      if (!panel) throw new Error("Missing saved query panel");
      return panel;
    }
    const connectionId = originalTarget.id;
    const queryText = "select 1";
    const oldDriver = queryDriver(originalTarget.database ?? "");
    const newDriver = queryDriver("replacement");
    drivers
      .mockReturnValueOnce(oldDriver as unknown as IDBDriver)
      .mockReturnValue(newDriver as unknown as IDBDriver);
    try {
      await manager.saveConnection(originalTarget);
      const targetSessionKey = manager.getQueryTargetSessionKey(connectionId);
      QueryPanel.createOrShow(context, manager, connectionId, queryText, true);
      const originalPanel = createdPanel();
      if (!originalPanel) throw new Error("Missing original panel");
      const payload = { connectionId, queryText, targetSessionKey };
      await originalPanel.webview.dispatchMessage({
        type: "executeQuery",
        payload,
      });
      await originalPanel.webview.dispatchMessage({
        type: "addBookmark",
        payload,
      });
      await vi.waitFor(() =>
        expect(manager.getHistory()[0]?.targetSessionKey).toBe(
          targetSessionKey,
        ),
      );
      const [history] = manager.getHistory();
      const [bookmark] = manager.getBookmarks();
      expect(bookmark?.targetSessionKey).toBe(targetSessionKey);
      const bookmarkCommand = "rapidb.openBookmarkEntry";
      const historyCommand = "rapidb.openHistoryEntry";
      const verified = await open(bookmarkCommand, bookmark);
      await verified.webview.dispatchMessage({ type: "executeQuery", payload });
      expect(oldDriver.query).toHaveBeenCalledTimes(2);

      await manager.saveConnection({
        ...originalTarget,
        host: "replacement.invalid",
        database: "replacement",
      });
      const newKey = manager.getQueryTargetSessionKey(connectionId);
      expect(newKey).not.toBe(targetSessionKey);
      const replacement = { ...payload, targetSessionKey: newKey };
      for (const [command, entry] of [
        [historyCommand, history],
        [bookmarkCommand, bookmark],
        [bookmarkCommand, { ...bookmark, targetSessionKey: undefined }],
      ] as const) {
        const reopened = await open(command, entry);
        await reopened.webview.dispatchMessage({
          type: "executeQuery",
          payload: replacement,
        });
        expect(reopened.webview.postMessage).toHaveBeenCalledWith({
          type: "queryResult",
          payload: expect.objectContaining({
            error: expect.stringContaining("confirm"),
          }),
        });
      }
      expect(newDriver.query).not.toHaveBeenCalled();

      const reviewed = await open(historyCommand, history);
      await reviewed.webview.dispatchMessage({
        type: "activeConnectionChanged",
        payload: { connectionId, targetSessionKey: newKey },
      });
      await reviewed.webview.dispatchMessage({
        type: "executeQuery",
        payload: replacement,
      });
      expect(newDriver.query).toHaveBeenCalledOnce();
      await vi.waitFor(() =>
        expect(
          manager.getHistory().map((entry) => entry.targetSessionKey),
        ).toEqual([newKey, targetSessionKey]),
      );
    } finally {
      QueryPanel.disposeAll();
      await manager.dispose();
    }
  });

  it("requires confirmation after a same-engine save, keeps reconnects valid, and restores both verified and older drafts", async () => {
    const { QueryPanel, manager, store, context, drivers } =
      await queryTargetFixture();
    const { createWebviewShell } = await import(
      "../../src/extension/panels/webviewShell"
    );
    const initialState = () =>
      vi.mocked(createWebviewShell).mock.lastCall?.[0].initialState;
    const replacement = {
      ...originalTarget,
      host: "new.invalid",
      database: "new",
    };
    const id = originalTarget.id;
    const oldDriver = queryDriver("original");
    const newDriver = queryDriver("new");
    drivers
      .mockReturnValueOnce(oldDriver as unknown as IDBDriver)
      .mockReturnValue(newDriver as unknown as IDBDriver);
    let restoredFixture:
      | Awaited<ReturnType<typeof queryTargetFixture>>
      | undefined;
    try {
      await manager.saveConnection(originalTarget);
      QueryPanel.createOrShow(context, manager, id, "select 1", true);
      const panel = createdPanel();
      if (!panel) throw new Error("No query panel");
      const previousState = initialState();
      const originalKey = manager.getQueryTargetSessionKey(id);
      expect(previousState).toMatchObject({
        targetBinding: { connectionId: id, sessionKey: originalKey },
      });
      const execute = (key: string | null, operationId: string) =>
        panel.webview.dispatchMessage({
          type: "executeQuery",
          payload: {
            connectionId: id,
            queryText: "select 1",
            operationId,
            targetSessionKey: key,
          },
        });
      await execute(originalKey, "original-run");
      expect(oldDriver.query).toHaveBeenCalledOnce();
      await manager.saveConnection({ ...originalTarget, name: "Renamed" });
      expect(manager.getQueryTargetSessionKey(id)).toBe(originalKey);

      await manager.saveConnection(replacement);
      const newKey = manager.getQueryTargetSessionKey(id);
      expect(newKey).not.toBe(originalKey);
      await execute(originalKey, "old-draft");
      await execute(newKey, "unacknowledged");
      expect(newDriver.query).not.toHaveBeenCalled();
      expect(manager.getHistory()).toHaveLength(1);
      expect(panel.webview.postMessage).toHaveBeenCalledWith({
        type: "queryResult",
        payload: expect.objectContaining({
          operationId: "unacknowledged",
          error: expect.stringContaining("confirm"),
        }),
      });
      expect(JSON.stringify(panel.webview.postMessage.mock.calls)).not.toMatch(
        /test-only-secret|new\.invalid|original\.invalid/,
      );
      await panel.webview.dispatchMessage({
        type: "activeConnectionChanged",
        payload: { connectionId: id, targetSessionKey: originalKey },
      });
      await execute(newKey, "stale-confirmation");
      expect(newDriver.query).not.toHaveBeenCalled();
      await panel.webview.dispatchMessage({
        type: "activeConnectionChanged",
        payload: { connectionId: id, targetSessionKey: newKey },
      });
      await panel.webview.dispatchMessage({ type: "exportResultsCSV" });
      expect(vscodeMock.module.window.showWarningMessage).toHaveBeenCalledWith(
        "[RapiDB] No query results to export.",
      );
      await execute(newKey, "confirmed");
      expect(newDriver.query).toHaveBeenCalledOnce();
      await manager.disconnectFrom(id);
      await execute(newKey, "normal-reconnect");
      expect(newDriver.query).toHaveBeenCalledTimes(2);
      expect(manager.getQueryTargetSessionKey(id)).toBe(newKey);

      restoredFixture = await queryTargetFixture(store);
      const {
        QueryPanel: RestoredPanel,
        manager: restoredManager,
        drivers: restoredDrivers,
        context: restoredContext,
      } = restoredFixture;
      restoredDrivers.mockReturnValue(newDriver as unknown as IDBDriver);
      expect(restoredManager.getQueryTargetSessionKey(id)).toBe(newKey);
      RestoredPanel.registerSerializer(restoredContext, restoredManager);
      const serializer = vscodeMock.serializers.get("rapidb.queryPanel") as {
        deserializeWebviewPanel(panel: unknown, state: unknown): Promise<void>;
      };
      for (const [suffix, binding, allowed] of [
        ["old", { connectionId: id, sessionKey: originalKey }, false],
        ["legacy", undefined, false],
        ["verified", { connectionId: id, sessionKey: newKey }, true],
      ] as const) {
        const restored = vscodeMock.createWebviewPanel();
        await serializer.deserializeWebviewPanel(restored, {
          initialState: previousState,
          queryDraft: {
            panelId: (previousState as { panelId: string }).panelId,
            text: "select current_database()",
            activeConnectionId: id,
            ...(binding ? { targetBinding: binding } : {}),
          },
        });
        const before = newDriver.query.mock.calls.length;
        await restored.webview.dispatchMessage({
          type: "executeQuery",
          payload: {
            connectionId: id,
            targetSessionKey: newKey,
            queryText: "select current_database()",
            operationId: suffix,
          },
        });
        expect(newDriver.query).toHaveBeenCalledTimes(before + Number(allowed));
        restored.dispose();
      }
      await restoredManager.removeConnection(id);
      await restoredManager.saveConnection(replacement);
      expect(restoredManager.getQueryTargetSessionKey(id)).not.toBe(newKey);
    } finally {
      QueryPanel.disposeAll();
      await restoredFixture?.manager.dispose();
      await manager.dispose();
    }
  });

  it.each([
    false,
    true,
  ])("does not adopt a replacement driver when a deferred connect is cancelled (repoint = %s)", async (repoint) => {
    const { QueryPanel, manager, context, drivers } =
      await queryTargetFixture();
    const oldDriver = queryDriver("original");
    const replacement = queryDriver("replacement");
    const oldConnect = deferred();
    const oldConnectStarted = deferred();
    const oldCleanup = deferred();
    const newQueryStarted = deferred();
    const newQueryFinish = deferred();
    oldDriver.connect.mockImplementationOnce(() => {
      oldConnectStarted.resolve();
      return oldConnect.promise;
    });
    oldDriver.disconnect.mockImplementation(() => oldCleanup.promise);
    const replacementQuery = replacement.query.getMockImplementation();
    replacement.query.mockImplementation(async (text) => {
      newQueryStarted.resolve();
      await newQueryFinish.promise;
      if (!replacementQuery) throw new Error("No replacement query fixture");
      return replacementQuery(text);
    });
    drivers
      .mockReturnValueOnce(oldDriver as unknown as IDBDriver)
      .mockReturnValue(replacement as unknown as IDBDriver);
    const id = originalTarget.id;
    try {
      await manager.saveConnection(originalTarget);
      QueryPanel.createOrShow(context, manager, id, "select 1", true);
      const panel = createdPanel();
      if (!panel) throw new Error("No query panel");
      const send = (type: string, payload: unknown) =>
        panel.webview.dispatchMessage({ type, payload });
      const oldKey = manager.getQueryTargetSessionKey(id);
      const pending = send("executeQuery", {
        connectionId: id,
        targetSessionKey: oldKey,
        queryText: "select 1",
        operationId: "deferred-old",
      });
      await oldConnectStarted.promise;
      if (repoint) {
        await manager.saveConnection({
          ...originalTarget,
          database: "replacement",
        });
      } else {
        await manager.disconnectFrom(id);
      }
      await manager.connectTo(id);
      expect(manager.getDriver(id)).toBe(replacement);
      oldCleanup.resolve();
      await pending;
      expect(oldDriver.query).not.toHaveBeenCalled();
      expect(replacement.query).not.toHaveBeenCalled();
      expect(manager.getHistory()).toEqual([]);
      expect(panel.webview.postMessage).toHaveBeenCalledWith({
        type: "queryResult",
        payload: expect.objectContaining({
          operationId: "deferred-old",
          error: expect.stringContaining(repoint ? "confirm" : "interrupted"),
        }),
      });
      const newKey = manager.getQueryTargetSessionKey(id);
      if (repoint) {
        await send("activeConnectionChanged", {
          connectionId: id,
          targetSessionKey: newKey,
        });
      }
      const newRun = send("executeQuery", {
        connectionId: id,
        targetSessionKey: newKey,
        queryText: "select 2",
        operationId: "replacement",
      });
      await newQueryStarted.promise;
      await send("cancelQuery", {
        connectionId: id,
        operationId: "deferred-old",
      });
      expect(replacement.cancelCurrentOperation).not.toHaveBeenCalled();
      newQueryFinish.resolve();
      await newRun;
      expect(replacement.query).toHaveBeenCalledExactlyOnceWith(
        "select 2",
        undefined,
        expect.any(Object),
      );
      expect(manager.getHistory()).toEqual([
        expect.objectContaining({ sql: "select 2" }),
      ]);
    } finally {
      oldCleanup.resolve();
      oldConnect.resolve();
      newQueryFinish.resolve();
      QueryPanel.disposeAll();
      await manager.dispose();
    }
  });

  it.each([
    "faedcbea-c80b-4d2b-98b0-258b9ef97538",
    "qp_103:1",
  ])("stops only B after a webview reload leaves A running (new ID: %s)", async (operationId) => {
    const { QueryPanel, manager, context, drivers } =
      await queryTargetFixture();
    const targetA = originalTarget;
    const targetB = { ...originalTarget, id: "profile-b", database: "B" };
    const driverA = {
      ...queryDriver("A"),
      cancelCurrentOperation: undefined,
    };
    const driverB = queryDriver("B");
    const startedA = deferred();
    const finishA = deferred();
    const startedB = deferred();
    const finishB = deferred();
    const queryA = driverA.query.getMockImplementation();
    driverA.query.mockImplementationOnce(async (text) => {
      startedA.resolve();
      await finishA.promise;
      if (!queryA) throw new Error("No query fixture for A");
      return queryA(text);
    });
    driverB.query.mockImplementationOnce(async () => {
      startedB.resolve();
      await finishB.promise;
      throw new Error("Query B interrupted");
    });
    driverB.cancelCurrentOperation.mockImplementationOnce(async () => {
      finishB.resolve();
    });
    drivers
      .mockReturnValueOnce(driverA as unknown as IDBDriver)
      .mockReturnValueOnce(driverB as unknown as IDBDriver);
    try {
      await manager.saveConnection(targetA);
      await manager.saveConnection(targetB);
      QueryPanel.createOrShow(context, manager, targetA.id, "select 1", true);
      const panel = createdPanel();
      if (!panel) throw new Error("No query panel");
      const send = (type: string, payload?: unknown) =>
        panel.webview.dispatchMessage({ type, payload });
      const binding = (connectionId: string) => ({
        connectionId,
        targetSessionKey: manager.getQueryTargetSessionKey(connectionId),
      });
      const oldRun = send("executeQuery", {
        ...binding(targetA.id),
        queryText: "select 1",
        operationId: "qp_103:1",
      });
      await startedA.promise;

      // A new document requests initial state from the same panel controller.
      await send("getConnections");
      await send("activeConnectionChanged", binding(targetB.id));
      const newRun = send("executeQuery", {
        ...binding(targetB.id),
        queryText: "select 2",
        operationId,
      });
      await startedB.promise;

      // A stale Stop (or an unscoped one from an older client) must not
      // warn about A or cancel B, even if both used the same operation ID.
      await send("cancelQuery", {
        connectionId: targetA.id,
        operationId: "qp_103:1",
      });
      await send("cancelQuery", { connectionId: targetA.id, operationId });
      await send("cancelQuery", {
        connectionId: targetB.id,
        operationId: "different-operation",
      });
      await send("cancelQuery", { operationId });
      expect(driverB.cancelCurrentOperation).not.toHaveBeenCalled();
      expect(
        vscodeMock.module.window.showWarningMessage,
      ).not.toHaveBeenCalled();

      await send("cancelQuery", { connectionId: targetB.id, operationId });
      expect(driverB.cancelCurrentOperation).toHaveBeenCalledExactlyOnceWith({
        reason: "manual",
        operationName: "query",
        connectionId: targetB.id,
        requestToken: expect.any(Number),
      });
      await newRun;
      finishA.resolve();
      await oldRun;
      expect(
        panel.webview.postMessage.mock.calls
          .map(([message]: [{ type: string }]) => message)
          .filter(
            (message: { type: string }) => message.type === "queryResult",
          ),
      ).toEqual([
        {
          type: "queryResult",
          payload: expect.objectContaining({
            connectionId: targetB.id,
            operationId,
            error: expect.stringContaining("Query B interrupted"),
          }),
        },
      ]);
      expect(
        vscodeMock.module.window.showWarningMessage,
      ).not.toHaveBeenCalled();
    } finally {
      finishA.resolve();
      finishB.resolve();
      QueryPanel.disposeAll();
      await manager.dispose();
    }
  });

  it("discards a retired running query's result and schema invalidation", async () => {
    const { QueryPanel, manager, context, drivers } =
      await queryTargetFixture();
    const oldDriver = queryDriver("original");
    const replacement = queryDriver("replacement");
    const started = deferred();
    const finish = deferred();
    const oldQuery = oldDriver.query.getMockImplementation();
    oldDriver.query.mockImplementationOnce(async (text) => {
      started.resolve();
      await finish.promise;
      if (!oldQuery) throw new Error("No old query fixture");
      return oldQuery(text);
    });
    drivers
      .mockReturnValueOnce(oldDriver as unknown as IDBDriver)
      .mockReturnValue(replacement as unknown as IDBDriver);
    vi.mocked(manager.getDriverCapabilities).mockReturnValue({
      tabularRead: "sql",
      boundedQueryResults: true,
      schemaMutationGuard: (query) => query === "select 1",
    });
    const refresh = vi.spyOn(manager, "refreshSchemaCache");
    const id = originalTarget.id;
    const execute = (operationId: string, queryText: string) =>
      createdPanel()?.webview.dispatchMessage({
        type: "executeQuery",
        payload: {
          connectionId: id,
          targetSessionKey: manager.getQueryTargetSessionKey(id),
          operationId,
          queryText,
        },
      });
    try {
      await manager.saveConnection(originalTarget);
      QueryPanel.createOrShow(context, manager, id, "select 1", true);
      const panel = createdPanel();
      if (!panel) throw new Error("No query panel");
      const oldRun = execute("running-old", "select 1");
      await started.promise;
      await manager.saveConnection({
        ...originalTarget,
        database: "replacement",
      });
      await panel.webview.dispatchMessage({
        type: "activeConnectionChanged",
        payload: {
          connectionId: id,
          targetSessionKey: manager.getQueryTargetSessionKey(id),
        },
      });
      await execute("replacement", "select 2");
      finish.resolve();
      await oldRun;
      expect(replacement.query).toHaveBeenCalledOnce();
      expect(refresh).not.toHaveBeenCalled();
      expect(
        panel.webview.postMessage.mock.calls
          .map(([message]: [{ type: string }]) => message)
          .filter(
            (message: { type: string }) => message.type === "queryResult",
          ),
      ).toEqual([
        {
          type: "queryResult",
          payload: expect.objectContaining({
            operationId: "replacement",
            rows: [{ __col_0: "replacement" }],
          }),
        },
      ]);
    } finally {
      finish.resolve();
      QueryPanel.disposeAll();
      await manager.dispose();
    }
  });

  it("preserves driver-owned formatOnOpen defaults when no explicit override is provided", async () => {
    const connectionManager = {
      getConnection: vi.fn(() => ({
        id: "conn-1",
        name: "Primary",
        type: "pg",
      })),
      getQueryEditorPresentation: vi.fn(() => ({
        formatOnOpen: true,
        editorLanguage: "sql",
        sqlDialect: "postgresql",
      })),
      onDidSchemaLoad: vi.fn(() => ({ dispose: vi.fn() })),
      onDidConnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidRefreshSchemas: vi.fn(() => ({ dispose: vi.fn() })),
      getConnections: vi.fn(() => [
        { id: "conn-1", name: "Primary", type: "pg" },
      ]),
      getConnectedCount: vi.fn(() => 1),
    };

    const { createWebviewShell } = await import(
      "../../src/extension/panels/webviewShell"
    );
    const { QueryPanel } = await import(
      "../../src/extension/panels/queryPanel"
    );

    QueryPanel.createOrShow(
      { extensionUri: {} } as never,
      connectionManager as never,
      "conn-1",
      "select 1",
      true,
    );

    expect(vscodeMock.createWebviewPanel).toHaveBeenCalledWith(
      expect.any(String),
      expect.any(String),
      1,
      expect.objectContaining({
        enableScripts: true,
        retainContextWhenHidden: true,
      }),
    );

    const initialState = (createWebviewShell as ReturnType<typeof vi.fn>).mock
      .calls[0]?.[0]?.initialState;

    expect(initialState).toMatchObject({
      connectionType: "pg",
      editorPresentation: {
        formatOnOpen: true,
        editorLanguage: "sql",
        sqlDialect: "postgresql",
      },
    });
    expect(initialState.formatOnOpen).toBeUndefined();
  });

  it.each([
    "qp_42",
    "qp_701acb27-3ee6-4ff9-8424-477d65022fb2",
  ])("restores serialized query panel %s using retained webview state", async (panelId) => {
    const connectionManager = {
      getConnection: vi.fn((connectionId: string) =>
        connectionId === "conn-2"
          ? { id: "conn-2", name: "Secondary", type: "pg" }
          : { id: "conn-1", name: "Primary", type: "pg" },
      ),
      getQueryEditorPresentation: vi.fn(() => undefined),
      onDidSchemaLoad: vi.fn(() => ({ dispose: vi.fn() })),
      onDidConnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidRefreshSchemas: vi.fn(() => ({ dispose: vi.fn() })),
      getConnections: vi.fn(() => []),
    };
    const { QueryPanel } = await import(
      "../../src/extension/panels/queryPanel"
    );
    QueryPanel.disposeAll();
    QueryPanel.registerSerializer(
      { extensionUri: {} } as never,
      connectionManager as never,
    );
    const serializer = vscodeMock.serializers.get("rapidb.queryPanel") as {
      deserializeWebviewPanel(panel: unknown, state: unknown): Promise<void>;
    };
    const panel = vscodeMock.createWebviewPanel();

    await serializer.deserializeWebviewPanel(panel, {
      initialState: {
        view: "query",
        panelId,
        connectionId: "conn-1",
        queryText: "select 1",
        initialSql: "select 1",
      },
      queryDraft: {
        panelId,
        text: "select 2",
        activeConnectionId: "conn-2",
      },
    });

    expect(panel.webview.html).toBe("<html></html>");
    expect(connectionManager.getConnection).toHaveBeenCalledWith("conn-1");
    expect(panel.title).toBe("Query — Secondary");
    panel.dispose();
    QueryPanel.createOrShow(
      { extensionUri: {} } as never,
      connectionManager as never,
      "conn-1",
    );
    expect(vscodeMock.createWebviewPanel).toHaveBeenCalledTimes(2);
    QueryPanel.disposeAll();
  });

  it("restores the exact PostgreSQL target without accepting another profile from the draft or webview", async () => {
    const { QueryPanel } = await import(
      "../../src/extension/panels/queryPanel"
    );
    const { createWebviewShell } = await import(
      "../../src/extension/panels/webviewShell"
    );
    QueryPanel.disposeAll();
    const postgresDatabase = '  Case:"δοκιμή"/?%  ';
    const primary = { id: "conn-1", name: "Primary", type: "pg" };
    const secondary = { id: "conn-2", name: "Secondary", type: "pg" };
    const finishQuery = deferred();
    const query = vi.fn(async () => {
      await finishQuery.promise;
      return {
        columns: ["database"],
        rows: [{ __col_0: postgresDatabase }],
        rowCount: 1,
        executionTimeMs: 1,
      };
    });
    const context = { extensionUri: {} } as never;
    const schemaLoad = createEventSource<string>();
    const cancelCurrentOperation = vi.fn(async () => undefined);
    const driver = { query, cancelCurrentOperation };
    const connectionManager = {
      getConnection: vi.fn((id) => (id === primary.id ? primary : secondary)),
      getQueryEditorPresentation: vi.fn(() => undefined),
      onDidSchemaLoad: schemaLoad.event,
      onDidConnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidRefreshSchemas: vi.fn(() => ({ dispose: vi.fn() })),
      getConnections: vi.fn(() => [primary, secondary]),
      isConnected: vi.fn(() => true),
      getDriver: vi.fn(() => driver),
      getDriverForQuery: vi.fn(async () => driver),
      getQueryRowLimit: vi.fn(() => 100),
      getDriverCapabilities: vi.fn(() => ({ boundedQueryResults: true })),
      addToHistory: vi.fn(async () => undefined),
      addBookmark: vi.fn(async () => undefined),
      getSchema: vi.fn(),
      getSchemaAsync: vi.fn(async () => [
        {
          database: postgresDatabase,
          schema: "public",
          object: "scoped",
          columns: [],
        },
      ]),
      getSchemaSnapshotState: vi.fn(() => ({ status: "loaded" })),
    };
    QueryPanel.registerSerializer(context, connectionManager as never);
    const serializer = vscodeMock.serializers.get("rapidb.queryPanel") as {
      deserializeWebviewPanel(panel: unknown, state: unknown): Promise<void>;
    };
    const panel = vscodeMock.createWebviewPanel();
    await serializer.deserializeWebviewPanel(panel, {
      initialState: {
        view: "query",
        panelId: "qp_103",
        connectionId: primary.id,
        postgresDatabase,
        queryText: "select current_database()",
      },
      queryDraft: {
        panelId: "qp_103",
        text: "select current_database()",
        activeConnectionId: secondary.id,
      },
    });
    expect(panel.title).toBe(
      `Query — Primary / ${JSON.stringify(postgresDatabase)}`,
    );
    expect(createWebviewShell).toHaveBeenLastCalledWith(
      expect.objectContaining({
        initialState: expect.objectContaining({
          connectionId: primary.id,
          postgresDatabase,
        }),
      }),
    );
    const send = (type: string, payload?: unknown) =>
      panel.webview.dispatchMessage({ type, payload });
    await send("getConnections");
    expect(panel.webview.postMessage).toHaveBeenCalledWith({
      type: "connections",
      payload: [expect.objectContaining(primary)],
    });
    const targetSessionKey = panel.webview.postMessage.mock.calls.find(
      ([message]) => message.type === "connections",
    )?.[0].payload[0].targetSessionKey;
    expect(targetSessionKey).toMatch(/^[0-9a-f]{64}$/);
    await send("activeConnectionChanged", { connectionId: secondary.id });
    await send("getSchema", { connectionId: secondary.id });
    expect(connectionManager.getSchemaAsync).not.toHaveBeenCalled();
    await send("activeConnectionChanged", {
      connectionId: primary.id,
      targetSessionKey,
    });
    expect(connectionManager.getSchemaAsync).toHaveBeenCalledExactlyOnceWith(
      primary.id,
      postgresDatabase,
    );
    expect(connectionManager.getSchemaSnapshotState).toHaveBeenCalledWith(
      primary.id,
      {
        kind: "database",
        database: postgresDatabase,
      },
    );
    schemaLoad.fire(primary.id);
    expect(connectionManager.getSchema).not.toHaveBeenCalled();
    const execute = (connectionId?: string) =>
      send("executeQuery", {
        queryText: "select current_database()",
        connectionId,
        operationId: "qp_103:1",
        targetSessionKey,
      });
    await execute(secondary.id);
    expect(query).not.toHaveBeenCalled();
    expect(panel.webview.postMessage).toHaveBeenLastCalledWith({
      type: "queryResult",
      payload: expect.objectContaining({
        error: expect.stringContaining("pinned"),
        connectionId: secondary.id,
      }),
    });
    const running = execute();
    try {
      await vi.waitFor(() =>
        expect(query).toHaveBeenCalledExactlyOnceWith(
          "select current_database()",
          undefined,
          expect.objectContaining({ database: postgresDatabase }),
        ),
      );
      const operationId = "qp_103:1";
      await send("cancelQuery", { connectionId: secondary.id, operationId });
      expect(cancelCurrentOperation).not.toHaveBeenCalled();
      await send("cancelQuery", { connectionId: primary.id, operationId });
      expect(cancelCurrentOperation).toHaveBeenCalledExactlyOnceWith(
        expect.objectContaining({
          reason: "manual",
          connectionId: primary.id,
        }),
      );
    } finally {
      finishQuery.resolve();
      await running;
    }
    await send("addBookmark", {
      queryText: "select current_database()",
      connectionId: secondary.id,
    });
    expect(connectionManager.addBookmark).not.toHaveBeenCalled();
    await send("addBookmark", {
      queryText: "select current_database()",
      connectionId: primary.id,
      targetSessionKey,
    });
    expect(connectionManager.addBookmark).toHaveBeenCalledExactlyOnceWith(
      primary.id,
      "select current_database()",
      postgresDatabase,
      targetSessionKey,
    );
    expect(connectionManager.addToHistory).toHaveBeenCalledExactlyOnceWith(
      primary.id,
      "select current_database()",
      postgresDatabase,
      targetSessionKey,
    );

    QueryPanel.createOrShow(context, connectionManager as never, primary.id);
    expect(panel.reveal).not.toHaveBeenCalled();
    expect(vscodeMock.createWebviewPanel).toHaveBeenCalledTimes(2);

    connectionManager.getConnection.mockReturnValue({
      ...primary,
      type: "mysql",
    });
    await execute(primary.id);
    expect(query).toHaveBeenCalledOnce();
    expect(panel.webview.postMessage).toHaveBeenLastCalledWith({
      type: "queryResult",
      payload: expect.objectContaining({
        error: expect.stringContaining("unavailable"),
      }),
    });
    QueryPanel.disposeAll();
  });

  it("opens a new query when a restored panel has switched away from the requested connection", async () => {
    const connectionManager = {
      getConnection: vi.fn((connectionId: string) =>
        connectionId === "conn-2"
          ? { id: "conn-2", name: "Secondary", type: "pg" }
          : { id: "conn-1", name: "Primary", type: "pg" },
      ),
      getQueryEditorPresentation: vi.fn(() => undefined),
      onDidSchemaLoad: vi.fn(() => ({ dispose: vi.fn() })),
      onDidConnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidRefreshSchemas: vi.fn(() => ({ dispose: vi.fn() })),
      getConnections: vi.fn(() => []),
    };
    const context = { extensionUri: {} } as never;
    const { QueryPanel } = await import(
      "../../src/extension/panels/queryPanel"
    );
    QueryPanel.disposeAll();
    QueryPanel.registerSerializer(context, connectionManager as never);
    const serializer = vscodeMock.serializers.get("rapidb.queryPanel") as {
      deserializeWebviewPanel(panel: unknown, state: unknown): Promise<void>;
    };
    const restoredPanel = vscodeMock.createWebviewPanel();
    await serializer.deserializeWebviewPanel(restoredPanel, {
      initialState: {
        view: "query",
        panelId: "qp_101",
        connectionId: "conn-1",
        queryText: "select 1",
      },
      queryDraft: {
        panelId: "qp_101",
        text: "select 2",
        activeConnectionId: "conn-2",
      },
    });

    QueryPanel.createOrShow(context, connectionManager as never, "conn-1");

    expect(restoredPanel.reveal).not.toHaveBeenCalled();
    expect(vscodeMock.createWebviewPanel).toHaveBeenCalledTimes(2);
    expect(restoredPanel.title).toBe("Query — Secondary");
    QueryPanel.disposeAll();
  });

  it.each([
    "new",
    "restored",
  ])("keeps both panels registered when a legacy panel restores lazily and %s closes first", async (closedFirst) => {
    vi.resetModules();
    const context = { extensionUri: {} } as never;
    const connectionManager = {
      getConnection: vi.fn(() => ({
        id: "conn-lazy",
        name: "Primary",
        type: "pg",
      })),
      getQueryEditorPresentation: vi.fn(() => undefined),
      onDidSchemaLoad: vi.fn(() => ({ dispose: vi.fn() })),
      onDidConnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidRefreshSchemas: vi.fn(() => ({ dispose: vi.fn() })),
    };
    const { createWebviewShell } = await import(
      "../../src/extension/panels/webviewShell"
    );
    const { QueryPanel } = await import(
      "../../src/extension/panels/queryPanel"
    );
    QueryPanel.registerSerializer(context, connectionManager as never);
    const newInstance = QueryPanel.createOrShow(
      context,
      connectionManager as never,
      "conn-lazy",
    );
    const newPanel = createdPanel();
    const newPanelId = (
      createWebviewShell as ReturnType<typeof vi.fn>
    ).mock.calls.at(-1)?.[0]?.initialState?.panelId;
    expect(newPanelId).toMatch(/^qp_[0-9a-f]{8}-[0-9a-f-]{27}$/);
    const serializer = vscodeMock.serializers.get("rapidb.queryPanel") as {
      deserializeWebviewPanel(panel: unknown, state: unknown): Promise<void>;
    };
    const restoredPanel = vscodeMock.createWebviewPanel();
    await serializer.deserializeWebviewPanel(restoredPanel, {
      initialState: {
        panelId: "qp_1",
        connectionId: "conn-lazy",
        queryText: "select 1",
      },
      queryDraft: {
        panelId: "qp_1",
        text: "select 2",
        activeConnectionId: "conn-lazy",
      },
    });
    expect(createWebviewShell).toHaveBeenLastCalledWith(
      expect.objectContaining({
        initialState: expect.objectContaining({
          panelId: "qp_1",
          queryText: "select 1",
        }),
      }),
    );
    expect(
      QueryPanel.createOrShow(context, connectionManager as never, "conn-lazy"),
    ).toBe(newInstance);
    expect(newPanel.reveal).toHaveBeenCalledOnce();
    expect(vscodeMock.createWebviewPanel).toHaveBeenCalledTimes(2);

    const firstPanel = closedFirst === "new" ? newPanel : restoredPanel;
    const remainingPanel = closedFirst === "new" ? restoredPanel : newPanel;
    const remainingDispose = vi.spyOn(remainingPanel, "dispose");
    firstPanel.dispose();
    const remainingInstance = QueryPanel.createOrShow(
      context,
      connectionManager as never,
      "conn-lazy",
    );
    expect(remainingPanel.reveal).toHaveBeenCalledWith(1);
    expect(vscodeMock.createWebviewPanel).toHaveBeenCalledTimes(2);
    if (closedFirst === "new") expect(remainingInstance).not.toBe(newInstance);
    else expect(remainingInstance).toBe(newInstance);
    QueryPanel.disposeAll();
    expect(remainingDispose).toHaveBeenCalledOnce();
    expect(
      QueryPanel.createOrShow(context, connectionManager as never, "conn-lazy"),
    ).not.toBe(remainingInstance);
    expect(vscodeMock.createWebviewPanel).toHaveBeenCalledTimes(3);
    QueryPanel.disposeAll();
  });

  it("passes editor language overrides through the webview initial state", async () => {
    const connectionManager = {
      getConnection: vi.fn(() => ({
        id: "conn-1",
        name: "Mongo",
        type: "mongodb",
      })),
      getQueryEditorPresentation: vi.fn(() => ({
        formatOnOpen: false,
        editorLanguage: "javascript",
      })),
      onDidSchemaLoad: vi.fn(() => ({ dispose: vi.fn() })),
      onDidConnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidRefreshSchemas: vi.fn(() => ({ dispose: vi.fn() })),
      getConnections: vi.fn(() => [
        { id: "conn-1", name: "Mongo", type: "mongodb" },
      ]),
      getConnectedCount: vi.fn(() => 1),
    };

    const { createWebviewShell } = await import(
      "../../src/extension/panels/webviewShell"
    );
    const { QueryPanel } = await import(
      "../../src/extension/panels/queryPanel"
    );

    QueryPanel.createOrShow(
      { extensionUri: {} } as never,
      connectionManager as never,
      "conn-1",
      'db.users.createCollection("users")',
      true,
      false,
      false,
      "javascript",
    );

    expect(createWebviewShell).toHaveBeenCalledWith(
      expect.objectContaining({
        initialState: expect.objectContaining({
          connectionType: "mongodb",
          editorLanguage: "javascript",
          formatOnOpen: false,
          editorPresentation: {
            formatOnOpen: false,
            editorLanguage: "javascript",
          },
        }),
      }),
    );
  });

  it("refreshes schema in an open editor when the connection becomes available", async () => {
    let connectListener: (() => void) | undefined;
    let disconnectListener: (() => void) | undefined;
    let connected = false;

    const connectionManager = {
      getConnection: vi.fn(() => ({
        id: "conn-1",
        name: "Primary",
        type: "pg",
      })),
      getQueryEditorPresentation: vi.fn(() => undefined),
      onDidSchemaLoad: vi.fn(() => ({ dispose: vi.fn() })),
      onDidConnect: vi.fn((listener: () => void) => {
        connectListener = listener;
        return { dispose: vi.fn() };
      }),
      onDidDisconnect: vi.fn((listener: () => void) => {
        disconnectListener = listener;
        return { dispose: vi.fn() };
      }),
      onDidRefreshSchemas: vi.fn(() => ({ dispose: vi.fn() })),
      isConnected: vi.fn((id: string) => connected && id === "conn-1"),
      getSchemaAsync: vi.fn(async (id: string) => [
        {
          database: "app_db",
          schema: "public",
          object: id === "conn-1" ? "users" : "other",
          columns: [],
        },
      ]),
      getSchemaSnapshotState: vi.fn(() => ({ status: "loaded" })),
      getConnections: vi.fn(() => [
        { id: "conn-1", name: "Primary", type: "pg" },
      ]),
      getConnectedCount: vi.fn(() => (connected ? 1 : 0)),
    };

    const { QueryPanel } = await import(
      "../../src/extension/panels/queryPanel"
    );

    QueryPanel.createOrShow(
      { extensionUri: {} } as never,
      connectionManager as never,
      "conn-1",
      "select 1",
    );

    const panel = createdPanel();
    if (!panel) {
      throw new Error("Expected a webview panel to be created.");
    }

    expect(connectListener).toBeDefined();
    expect(disconnectListener).toBeDefined();

    connected = true;
    connectListener?.();

    await Promise.resolve();
    await Promise.resolve();

    expect(panel.webview.postMessage).toHaveBeenCalledWith(
      expect.objectContaining({
        type: "connections",
        payload: expect.arrayContaining([
          expect.objectContaining({ id: "conn-1", name: "Primary" }),
        ]),
      }),
    );

    expect(connectionManager.getSchemaAsync).toHaveBeenCalledWith("conn-1");

    expect(panel.webview.postMessage).toHaveBeenCalledWith(
      expect.objectContaining({
        type: "schema",
        payload: expect.objectContaining({
          connectionId: "conn-1",
          schema: expect.arrayContaining([
            expect.objectContaining({ object: "users" }),
          ]),
        }),
      }),
    );

    panel.webview.postMessage.mockClear();
    connected = false;
    disconnectListener?.();

    await Promise.resolve();
    await Promise.resolve();

    expect(panel.webview.postMessage).toHaveBeenCalledWith({
      type: "schema",
      payload: { connectionId: "conn-1", schema: [], error: "Not connected." },
    });
    connectionManager.getConnection.mockReturnValue(undefined as never);
    connectionManager.getConnections.mockReturnValue([]);
    disconnectListener?.();
    expect(panel.title).toBe("Query — Select a connection");
  });

  it("pushes incremental shared-cache schema updates and refresh reloads into the webview", async () => {
    const schemaLoad = createEventSource<string>();
    const refreshSchemas = createEventSource<void>();
    const mergedSchema = [
      {
        database: "app_db",
        schema: "public",
        object: "users",
        columns: [],
      },
      {
        database: "app_db",
        schema: "audit",
        object: "sync_events",
        columns: [],
      },
    ];
    const refreshedSchema = [
      ...mergedSchema,
      {
        database: "app_db",
        schema: "audit",
        object: "sync_event_archive",
        columns: [],
      },
    ];

    const connectionManager = {
      getConnection: vi.fn(() => ({
        id: "conn-1",
        name: "Primary",
        type: "pg",
      })),
      getQueryEditorPresentation: vi.fn(() => undefined),
      onDidSchemaLoad: schemaLoad.event,
      onDidConnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidRefreshSchemas: refreshSchemas.event,
      isConnected: vi.fn((id: string) => id === "conn-1"),
      getSchema: vi.fn(() => mergedSchema),
      getSchemaAsync: vi.fn(async () => refreshedSchema),
      getSchemaSnapshotState: vi.fn(() => ({ status: "loaded" })),
      getConnections: vi.fn(() => [
        { id: "conn-1", name: "Primary", type: "pg" },
      ]),
      getConnectedCount: vi.fn(() => 1),
    };

    const { QueryPanel } = await import(
      "../../src/extension/panels/queryPanel"
    );

    QueryPanel.createOrShow(
      { extensionUri: {} } as never,
      connectionManager as never,
      "conn-1",
      "select 1",
    );

    const panel = createdPanel();
    if (!panel) {
      throw new Error("Expected a webview panel to be created.");
    }

    panel.webview.postMessage.mockClear();

    schemaLoad.fire("conn-1");

    await Promise.resolve();
    await Promise.resolve();

    expect(connectionManager.getSchema).toHaveBeenCalledWith("conn-1");
    expect(connectionManager.getSchemaAsync).not.toHaveBeenCalled();
    expect(panel.webview.postMessage).toHaveBeenCalledWith({
      type: "schema",
      payload: { connectionId: "conn-1", schema: mergedSchema },
    });

    panel.webview.postMessage.mockClear();
    connectionManager.getSchemaAsync.mockClear();

    refreshSchemas.fire(undefined);

    await Promise.resolve();
    await Promise.resolve();

    expect(connectionManager.getSchemaAsync).toHaveBeenCalledWith("conn-1");
    expect(panel.webview.postMessage).toHaveBeenCalledWith(
      expect.objectContaining({
        type: "schema",
        payload: { connectionId: "conn-1", schema: refreshedSchema },
      }),
    );
  });

  it("disposes the controller so panel-owned queries are cancelled", async () => {
    const connectionManager = {
      getConnection: vi.fn(() => ({
        id: "conn-dispose",
        name: "Primary",
        type: "pg",
      })),
      getQueryEditorPresentation: vi.fn(() => undefined),
      onDidSchemaLoad: vi.fn(() => ({ dispose: vi.fn() })),
      onDidConnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidRefreshSchemas: vi.fn(() => ({ dispose: vi.fn() })),
    };
    const { QueryPanelController } = await import(
      "../../src/extension/panels/queryPanelController"
    );
    const disposeSpy = vi
      .spyOn(QueryPanelController.prototype, "dispose")
      .mockResolvedValue(undefined);
    const { QueryPanel } = await import(
      "../../src/extension/panels/queryPanel"
    );

    QueryPanel.createOrShow(
      { extensionUri: {} } as never,
      connectionManager as never,
      "conn-dispose",
      "select 1",
      true,
    );
    createdPanel()?.dispose();

    expect(disposeSpy).toHaveBeenCalledOnce();
    disposeSpy.mockRestore();
  });

  it("assigns distinct IDs to separate query panels", async () => {
    const connectionManager = {
      getConnection: vi.fn(() => ({
        id: "conn-namespace",
        name: "Primary",
        type: "pg",
      })),
      getQueryEditorPresentation: vi.fn(() => undefined),
      onDidSchemaLoad: vi.fn(() => ({ dispose: vi.fn() })),
      onDidConnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidRefreshSchemas: vi.fn(() => ({ dispose: vi.fn() })),
    };
    const { createWebviewShell } = await import(
      "../../src/extension/panels/webviewShell"
    );
    const { QueryPanel } = await import(
      "../../src/extension/panels/queryPanel"
    );

    QueryPanel.createOrShow(
      { extensionUri: {} } as never,
      connectionManager as never,
      "conn-namespace",
      undefined,
      true,
    );
    QueryPanel.createOrShow(
      { extensionUri: {} } as never,
      connectionManager as never,
      "conn-namespace",
      undefined,
      true,
    );

    const calls = (createWebviewShell as ReturnType<typeof vi.fn>).mock.calls;
    const firstPanelId = calls.at(-2)?.[0]?.initialState?.panelId;
    const secondPanelId = calls.at(-1)?.[0]?.initialState?.panelId;
    expect(firstPanelId).toMatch(/^qp_/);
    expect(secondPanelId).toMatch(/^qp_/);
    expect(secondPanelId).not.toBe(firstPanelId);
  });

  it("swallows postMessage failures without unhandled rejection", async () => {
    const connectionManager = {
      getConnection: vi.fn(() => ({
        id: "conn-pm",
        name: "Primary",
        type: "pg",
      })),
      getQueryEditorPresentation: vi.fn(() => undefined),
      onDidSchemaLoad: vi.fn(() => ({ dispose: vi.fn() })),
      onDidConnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidRefreshSchemas: vi.fn(() => ({ dispose: vi.fn() })),
    };
    const { QueryPanel } = await import(
      "../../src/extension/panels/queryPanel"
    );

    QueryPanel.createOrShow(
      { extensionUri: {} } as never,
      connectionManager as never,
      "conn-pm",
      "select 1",
      true,
    );
    const panel = createdPanel();
    panel.webview.postMessage.mockRejectedValueOnce(new Error("gone"));
    await panel.webview.dispatchMessage({ type: "ready" });
    await new Promise((resolve) => setTimeout(resolve, 10));

    panel.webview.postMessage.mockResolvedValueOnce(false);
    await panel.webview.dispatchMessage({ type: "ready" });
    await new Promise((resolve) => setTimeout(resolve, 10));
  });

  it("survives controller dispose rejection on panel dispose", async () => {
    const connectionManager = {
      getConnection: vi.fn(() => ({
        id: "conn-dispose-err",
        name: "Primary",
        type: "pg",
      })),
      getQueryEditorPresentation: vi.fn(() => undefined),
      onDidSchemaLoad: vi.fn(() => ({ dispose: vi.fn() })),
      onDidConnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidDisconnect: vi.fn(() => ({ dispose: vi.fn() })),
      onDidRefreshSchemas: vi.fn(() => ({ dispose: vi.fn() })),
    };
    const { QueryPanelController } = await import(
      "../../src/extension/panels/queryPanelController"
    );
    const disposeSpy = vi
      .spyOn(QueryPanelController.prototype, "dispose")
      .mockRejectedValueOnce(new Error("dispose boom"));
    const { QueryPanel } = await import(
      "../../src/extension/panels/queryPanel"
    );

    QueryPanel.createOrShow(
      { extensionUri: {} } as never,
      connectionManager as never,
      "conn-dispose-err",
      "select 1",
      true,
    );
    createdPanel()?.dispose();
    await new Promise((resolve) => setTimeout(resolve, 10));
    expect(disposeSpy).toHaveBeenCalledOnce();
    disposeSpy.mockRestore();
  });
});
