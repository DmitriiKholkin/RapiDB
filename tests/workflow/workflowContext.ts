import { promises as fs } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import userEvent from "@testing-library/user-event";
import { ConnectionManager } from "../../src/extension/connectionManager";
import {
  createExtensionContextStub,
  FakeConnectionManagerStore,
} from "../support/fakeConnectionManagerStore";
import {
  createFakeWebviewPanel,
  type FakeWebviewPanelHandle,
} from "./bridge/fakeWebviewPanel";
import {
  type BridgeSession,
  connectionFormInitialStateFor,
  erdInitialStateFor,
  queryInitialStateFor,
  startBridge,
  tableInitialStateFor,
} from "./bridge/hostWebviewBridge";
import {
  createWorkflowVscodeState,
  type WorkflowVscodeState,
} from "./bridge/workflowVscode";
import { type EngineScenario } from "./scenarios/types";
import { getSharedWorkflowState } from "./sharedState";

export interface OpenFormResult {
  session: BridgeSession;
  handle: FakeWebviewPanelHandle;
}

export interface OpenQueryResult {
  session: BridgeSession;
  handle: FakeWebviewPanelHandle;
  panel: import("../../src/extension/panels/queryPanel").QueryPanel;
}

export interface OpenTableResult {
  session: BridgeSession;
  handle: FakeWebviewPanelHandle;
}

export interface OpenErdResult {
  session: BridgeSession;
  handle: FakeWebviewPanelHandle;
}

export interface WorkflowContext {
  state: WorkflowVscodeState;
  scenario: EngineScenario;
  connectionManager: ConnectionManager;
  store: FakeConnectionManagerStore;
  user: ReturnType<typeof userEvent.setup>;
  openConnectionForm(): Promise<OpenFormResult>;
  openQueryEditor(options?: {
    initialSql?: string;
    editorLanguage?: "sql" | "javascript";
  }): Promise<OpenQueryResult>;
  openTableViewer(): Promise<OpenTableResult>;
  openErdViewer(): Promise<OpenErdResult>;
  connect(): Promise<void>;
  dispose(): Promise<void>;
}

export function ensureTempSqlitePath(): Promise<string> {
  return fs
    .mkdtemp(join(tmpdir(), "rapidb-workflow-"))
    .then((dir) => join(dir, "workflow.sqlite"));
}

export interface BootstrapOptions {
  scenario: EngineScenario;
  /**
   * State object shared with the vscode mock installed via vi.hoisted.
   * When omitted, the singleton shared state is used.
   */
  state?: { panels: unknown[]; [key: string]: unknown };
}

export async function bootstrapWorkflowContext(
  options: BootstrapOptions,
): Promise<WorkflowContext> {
  const { scenario } = options;
  const state = (options.state ?? getSharedWorkflowState()) as unknown as
    | WorkflowVscodeState
    | {
        panels: unknown[];
        showInformationMessage?: () => void;
        [key: string]: unknown;
      };

  // For a simple object from vi.hoisted, wrap it to match WorkflowVscodeState.
  const workflowState: WorkflowVscodeState =
    "createWebviewPanel" in state
      ? (state as unknown as WorkflowVscodeState)
      : createWorkflowVscodeState();
  if (!("createWebviewPanel" in state)) {
    // Copy the test's panels into the workflow state.
    workflowState.panels.length = 0;
    for (const panel of state.panels as never[]) {
      workflowState.panels.push(panel as never);
    }
    // Override createWebviewPanel to push to the test's panels array.
    (
      workflowState as unknown as { createWebviewPanel: unknown }
    ).createWebviewPanel = (...args: unknown[]) => {
      const result = (
        state as unknown as { createWebviewPanel: (...a: unknown[]) => unknown }
      ).createWebviewPanel(...args);
      (state as { panels: unknown[] }).panels.push(result);
      return result;
    };
  }

  const store = new FakeConnectionManagerStore();
  await store.saveConnections([scenario.buildConnection()]);
  store.setSkipTableMutationPreview(true);

  const context = createExtensionContextStub();
  const connectionManager = new ConnectionManager(
    context as unknown as ConstructorParameters<typeof ConnectionManager>[0],
    store,
  );

  // Override the vscode mock's createWebviewPanel for the duration of
  // this context so that panels get a real FakeWebviewPanelHandle.
  const realCreateWebviewPanel =
    workflowState.createWebviewPanel as unknown as (
      viewType: string,
      title: string,
    ) => unknown;
  const stateMut = workflowState as unknown as {
    createWebviewPanel: (viewType: string, title: string) => unknown;
  };
  stateMut.createWebviewPanel = (viewType: string, title: string) => {
    const handle = createFakeWebviewPanel({ viewType, title });
    workflowState.panels.push(handle);
    return handle.panel;
  };

  return {
    state: workflowState,
    scenario,
    connectionManager,
    store,
    user: userEvent.setup({ delay: null, pointerEventsCheck: 0 }),
    async openConnectionForm() {
      const { ConnectionFormPanel } = await import(
        "../../src/extension/panels/connectionFormPanel"
      );
      void ConnectionFormPanel.show(
        context as unknown as Parameters<typeof ConnectionFormPanel.show>[0],
        connectionManager,
      );
      const handle = workflowState.panels[workflowState.panels.length - 1];
      if (!handle) {
        throw new Error("[workflow] openConnectionForm produced no panel");
      }
      const session = await startBridge({
        state: workflowState,
        initialState: connectionFormInitialStateFor(),
      });
      return { session, handle };
    },
    async openQueryEditor(opts) {
      const { QueryPanel } = await import(
        "../../src/extension/panels/queryPanel"
      );
      const editorLanguage =
        opts?.editorLanguage ?? scenario.capabilities.driverEditorLanguage;
      const panel = QueryPanel.createOrShow(
        context as unknown as Parameters<typeof QueryPanel.createOrShow>[0],
        connectionManager,
        scenario.buildConnection().id,
        opts?.initialSql,
        true,
        undefined,
        false,
        editorLanguage,
      );
      const handle = workflowState.panels[workflowState.panels.length - 1];
      if (!handle) {
        throw new Error("[workflow] openQueryEditor produced no panel");
      }
      const session = await startBridge({
        state: workflowState,
        initialState: queryInitialStateFor(scenario.buildConnection(), {
          initialSql: opts?.initialSql,
          editorLanguage,
        }),
      });
      return { session, handle, panel };
    },
    async openTableViewer() {
      const { TablePanel } = await import(
        "../../src/extension/panels/tablePanel"
      );
      const connection = scenario.buildConnection();
      TablePanel.createOrShow(
        context as unknown as Parameters<typeof TablePanel.createOrShow>[0],
        connectionManager,
        connection.id,
        scenario.tableFixture.database,
        scenario.tableFixture.schema ?? "",
        scenario.tableFixture.table,
        false,
        "table",
      );
      const handle = workflowState.panels[workflowState.panels.length - 1];
      if (!handle) {
        throw new Error("[workflow] openTableViewer produced no panel");
      }
      const session = await startBridge({
        state: workflowState,
        initialState: tableInitialStateFor(connection, {
          database: scenario.tableFixture.database,
          schema: scenario.tableFixture.schema ?? "",
          table: scenario.tableFixture.table,
        }),
      });
      return { session, handle };
    },
    async openErdViewer() {
      const { ErdPanel } = await import("../../src/extension/panels/erdPanel");
      const connection = scenario.buildConnection();
      ErdPanel.createOrShow(
        context as unknown as Parameters<typeof ErdPanel.createOrShow>[0],
        connectionManager,
        {
          connectionId: connection.id,
          database: scenario.tableFixture.database,
          schema: scenario.tableFixture.schema,
        },
      );
      const handle = workflowState.panels[workflowState.panels.length - 1];
      if (!handle) {
        throw new Error("[workflow] openErdViewer produced no panel");
      }
      const session = await startBridge({
        state: workflowState,
        initialState: erdInitialStateFor(connection, {
          database: scenario.tableFixture.database,
          schema: scenario.tableFixture.schema,
        }),
      });
      return { session, handle };
    },
    async connect() {
      await connectionManager.connectTo(scenario.buildConnection().id);
    },
    async dispose() {
      // Restore the original createWebviewPanel.
      stateMut.createWebviewPanel = realCreateWebviewPanel;
      await connectionManager.disconnectAll();
      await connectionManager.dispose();
    },
  };
}
