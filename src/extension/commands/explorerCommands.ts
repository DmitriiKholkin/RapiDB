import * as vscode from "vscode";
import { RAPIDB_COMMANDS as CMD } from "../../shared/commandIds";
import {
  isDbObjectKind,
  isDdlOnlyDbObjectKind,
  isRoutineDbObjectKind,
} from "../../shared/dbObjectKinds";
import type { QueryEditorLanguage } from "../../shared/webviewContracts";
import type { ConnectionManager } from "../connectionManager";
import { DEFAULT_DRIVER_ENTITY_MANIFEST } from "../dbDrivers/types";
import { ErdPanel } from "../panels/erdPanel";
import { QueryPanel } from "../panels/queryPanel";
import { TablePanel } from "../panels/tablePanel";
import type { RapiDBNode } from "../providers/connectionProvider";
import { connectWithProgress } from "../utils/connectOrchestration";
import { logErrorWithContext } from "../utils/errorHandling";
import { isOpenDdlSupportedForNode } from "../utils/openDdlEligibility";
import { resolveConnectionId } from "../utils/resolveConnectionId";

/**
 * Context required for explorer commands.
 */
export interface ExplorerCommandContext {
  readonly context: vscode.ExtensionContext;
  readonly connectionManager: ConnectionManager;
  readonly refresh: () => void;
}

/**
 * Reads database and schema from a node, defaulting to empty strings.
 */
function readNodeLocation(node: RapiDBNode): {
  database: string;
  schema: string;
} {
  return {
    database: node.database ?? "",
    schema: node.schema ?? "",
  };
}

/**
 * Returns the default DDL presentation options.
 */
function getOpenDdlPresentation(): {
  formatOnOpen: boolean;
  editorLanguage?: QueryEditorLanguage;
} {
  return {
    formatOnOpen: true,
  };
}

/**
 * Ensures a connection is ready, connecting if necessary.
 *
 * @returns true if connected, false if connection failed
 */
async function ensureConnectionReady(
  connectionManager: ConnectionManager,
  connectionId: string,
  refresh: () => void,
  failureContext: string,
): Promise<boolean> {
  if (connectionManager.isConnected(connectionId)) {
    return true;
  }

  try {
    await connectWithProgress(
      connectionManager,
      connectionId,
      "RapiDB: Connecting…",
      true,
    );
    refresh();
    return true;
  } catch (err: unknown) {
    const error = logErrorWithContext(failureContext, err);
    vscode.window.showErrorMessage(`[RapiDB] Cannot connect: ${error.message}`);
    return false;
  }
}

/**
 * Opens DDL in a query panel with the specified presentation options.
 */
function openDdlInQueryPanel(
  context: vscode.ExtensionContext,
  connectionManager: ConnectionManager,
  connectionId: string,
  ddl: string,
  presentation: {
    formatOnOpen: boolean;
    editorLanguage?: QueryEditorLanguage;
  },
): void {
  if (presentation.editorLanguage) {
    QueryPanel.createOrShow(
      context,
      connectionManager,
      connectionId,
      ddl,
      true,
      presentation.formatOnOpen,
      false,
      presentation.editorLanguage,
    );
    return;
  }

  QueryPanel.createOrShow(
    context,
    connectionManager,
    connectionId,
    ddl,
    true,
    presentation.formatOnOpen,
  );
}

/**
 * Registers all explorer-related commands.
 *
 * Commands:
 * - rapidb.newQuery: Open a new query editor
 * - rapidb.openTableData: Open table data viewer
 * - rapidb.showDDL: Show DDL for an object
 * - rapidb.copyNodeName: Copy node name to clipboard
 * - rapidb.openErd: Open ERD viewer
 * - rapidb.openRoutine: Open routine definition
 */
export function registerExplorerCommands(
  ctx: ExplorerCommandContext,
  registerCommand: <TArgs extends unknown[]>(
    command: string,
    callback: (...args: TArgs) => unknown,
  ) => vscode.Disposable,
): void {
  const { context, connectionManager, refresh } = ctx;

  // ─── New Query ─────────────────────────────────────────────────────
  registerCommand(CMD.newQuery, async (node?: RapiDBNode) => {
    const connectionId = await resolveConnectionId(node, connectionManager);
    if (!connectionId) {
      return;
    }

    const connected = await ensureConnectionReady(
      connectionManager,
      connectionId,
      refresh,
      `New query connect failed for ${connectionId}`,
    );
    if (!connected) {
      return;
    }

    QueryPanel.createOrShow(
      context,
      connectionManager,
      connectionId,
      undefined,
      undefined,
      true,
    );
  });

  // ─── Open Table Data ───────────────────────────────────────────────
  registerCommand(CMD.openTableData, (node?: RapiDBNode) => {
    if (!node?.connectionId || !node.objectName) {
      return;
    }

    const isView = node.kind === "view" || node.kind === "materializedView";
    const location = readNodeLocation(node);
    TablePanel.createOrShow(
      context,
      connectionManager,
      node.connectionId,
      location.database,
      location.schema,
      node.objectName,
      isView,
      node.kind === "materializedView"
        ? "materializedView"
        : node.kind === "view"
          ? "view"
          : "table",
    );
  });

  // ─── Show DDL ──────────────────────────────────────────────────────
  registerCommand(CMD.showDDL, async (node?: RapiDBNode) => {
    if (!node?.connectionId) {
      vscode.window.showWarningMessage(
        "[RapiDB] Select a table, view, materialized view, function, procedure, sequence, type, constraint, index, or trigger node first.",
      );
      return;
    }

    const connectionType = connectionManager.getConnection(
      node.connectionId,
    )?.type;
    const entityManifest =
      connectionManager.getDriverEntityManifest(node.connectionId) ??
      DEFAULT_DRIVER_ENTITY_MANIFEST;

    if (
      !isOpenDdlSupportedForNode(node.kind, connectionType, entityManifest, {
        indexDdlSupport: node.ddlSupport,
      })
    ) {
      vscode.window.showWarningMessage(
        "[RapiDB] DDL is available only for table, view, materialized view, function, procedure, sequence, type, constraint, index, and trigger nodes.",
      );
      return;
    }

    const driver = connectionManager.getDriver(node.connectionId);
    if (!driver) {
      vscode.window.showErrorMessage("[RapiDB] Not connected. Connect first.");
      return;
    }

    try {
      let ddl: string | null = null;
      let attemptedSupportedLookup = false;
      const objectKind = isDbObjectKind(node.kind) ? node.kind : undefined;
      const location = readNodeLocation(node);

      if (
        (node.kind === "table" ||
          node.kind === "view" ||
          node.kind === "materializedView") &&
        node.objectName
      ) {
        attemptedSupportedLookup = true;
        ddl = await driver.getCreateTableDDL(
          location.database,
          location.schema,
          node.objectName,
        );
      } else if (
        objectKind &&
        isRoutineDbObjectKind(objectKind) &&
        node.objectName
      ) {
        attemptedSupportedLookup = true;
        ddl = await driver.getRoutineDefinition(
          location.database,
          location.schema,
          node.objectName,
          objectKind,
          node.detailKey,
        );
      } else if (
        objectKind &&
        isDdlOnlyDbObjectKind(objectKind) &&
        node.objectName
      ) {
        attemptedSupportedLookup = true;
        ddl = await driver.getObjectDefinition(
          location.database,
          location.schema,
          node.objectName,
          objectKind,
        );
      } else if (
        node.kind === "table_detail_constraint" &&
        node.parentTable &&
        node.objectName
      ) {
        attemptedSupportedLookup = true;
        ddl = await driver.getConstraintDDL(
          location.database,
          location.schema,
          node.parentTable,
          node.objectName,
        );
      } else if (
        node.kind === "table_detail_index" &&
        node.parentTable &&
        node.objectName
      ) {
        attemptedSupportedLookup = true;
        ddl = await driver.getIndexDDL(
          location.database,
          location.schema,
          node.parentTable,
          node.objectName,
        );
      } else if (
        node.kind === "table_detail_trigger" &&
        node.parentTable &&
        node.objectName
      ) {
        attemptedSupportedLookup = true;
        ddl = await driver.getTriggerDDL(
          location.database,
          location.schema,
          node.parentTable,
          node.objectName,
        );
      }

      if (!ddl) {
        if (attemptedSupportedLookup) {
          const kindLabel = objectKind ?? node.kind;
          const objectName =
            node.objectName ?? node.parentTable ?? "selected node";
          vscode.window.showWarningMessage(
            `[RapiDB] DDL is currently unavailable for ${kindLabel} "${objectName}". Check object permissions (for example, DBMS_METADATA access on Oracle) and retry.`,
          );
        } else {
          vscode.window.showWarningMessage(
            "[RapiDB] DDL is available only for table, view, materialized view, function, procedure, sequence, type, constraint, index, and trigger nodes.",
          );
        }
        return;
      }

      const ddlPresentation =
        connectionManager.getQueryEditorPresentation(node.connectionId) ??
        getOpenDdlPresentation();
      const ddlPresentationResolved = {
        formatOnOpen: ddlPresentation.formatOnOpen ?? false,
        editorLanguage: ddlPresentation.editorLanguage,
      };

      if (objectKind && isRoutineDbObjectKind(objectKind)) {
        QueryPanel.createOrShow(
          context,
          connectionManager,
          node.connectionId,
          ddl,
        );
      } else {
        openDdlInQueryPanel(
          context,
          connectionManager,
          node.connectionId,
          ddl,
          ddlPresentationResolved,
        );
      }
    } catch (err: unknown) {
      const error = logErrorWithContext(
        `Load DDL failed for ${node.objectName}`,
        err,
      );
      vscode.window.showErrorMessage(`[RapiDB] DDL error: ${error.message}`);
    }
  });

  // ─── Copy Node Name ────────────────────────────────────────────────
  registerCommand(CMD.copyNodeName, async (node?: RapiDBNode) => {
    const name = node?.objectName ?? node?.label?.toString();
    if (name) {
      await vscode.env.clipboard.writeText(name);
    }
  });

  // ─── Open ERD ──────────────────────────────────────────────────────
  registerCommand(CMD.openErd, async (node?: RapiDBNode) => {
    const connectionId = await resolveConnectionId(node, connectionManager);
    if (!connectionId) {
      return;
    }

    const connected = await ensureConnectionReady(
      connectionManager,
      connectionId,
      refresh,
      `Open ERD connect failed for ${connectionId}`,
    );
    if (!connected) {
      return;
    }

    if (!node?.database) {
      vscode.window.showInformationMessage(
        "[RapiDB] Please open ERD from a database or schema node.",
      );
      return;
    }

    ErdPanel.createOrShow(context, connectionManager, {
      connectionId,
      database: node.database,
      schema: node.schema,
    });
  });

  // ─── Open Routine ──────────────────────────────────────────────────
  registerCommand(CMD.openRoutine, async (node?: RapiDBNode) => {
    if (!node?.connectionId || !node.objectName) {
      return;
    }

    const objectKind = isDbObjectKind(node.kind) ? node.kind : undefined;
    if (!objectKind || !isRoutineDbObjectKind(objectKind)) {
      return;
    }

    const kind = objectKind;
    const connected = await ensureConnectionReady(
      connectionManager,
      node.connectionId,
      refresh,
      `Open routine connect failed for ${node.objectName}`,
    );
    if (!connected) {
      return;
    }

    const driver = connectionManager.getDriver(node.connectionId);
    if (!driver) {
      return;
    }

    try {
      const location = readNodeLocation(node);
      const sql = await driver.getRoutineDefinition(
        location.database,
        location.schema,
        node.objectName,
        kind,
        node.detailKey,
      );
      QueryPanel.createOrShow(
        context,
        connectionManager,
        node.connectionId,
        sql,
      );
    } catch (err: unknown) {
      const error = logErrorWithContext(
        `Load routine definition failed for ${node.objectName}`,
        err,
      );
      vscode.window.showErrorMessage(
        `[RapiDB] Cannot load ${kind} definition: ${error.message}`,
      );
    }
  });
}
