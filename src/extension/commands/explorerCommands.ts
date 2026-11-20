import * as vscode from "vscode";
import { RAPIDB_COMMANDS as CMD } from "../../shared/commandIds";
import {
  isDataDbObjectKind,
  isDbObjectKind,
  isDdlOnlyDbObjectKind,
  isRoutineDbObjectKind,
} from "../../shared/dbObjectKinds";
import { isPostgresQueryDatabase } from "../../shared/postgresQueryDatabase";
import type {
  QueryEditorLanguage,
  QueryTargetBinding,
} from "../../shared/webviewContracts";
import type { ConnectionManager } from "../connectionManager";
import { connectionSessionKey } from "../connectionManagerStore";
import { redisKeyspaceDisplayName } from "../dbDrivers/redisKeyspace";
import {
  DEFAULT_DRIVER_ENTITY_MANIFEST,
  type IDBDriver,
} from "../dbDrivers/types";
import { ErdPanel } from "../panels/erdPanel";
import { QueryPanel } from "../panels/queryPanel";
import { TablePanel } from "../panels/tablePanel";
import type { RapiDBNode } from "../providers/connectionProvider";
import { connectWithProgress } from "../utils/connectOrchestration";
import { logErrorWithContext } from "../utils/errorHandling";
import { isOpenDdlSupportedForNode } from "../utils/openDdlEligibility";
import { resolveConnectionId } from "../utils/resolveConnectionId";

export interface ExplorerCommandContext {
  readonly context: vscode.ExtensionContext;
  readonly connectionManager: ConnectionManager;
  readonly refresh: () => void;
}

function readNodeLocation(node: RapiDBNode): {
  database: string;
  schema: string;
} {
  return {
    database: node.database ?? "",
    schema: node.schema ?? "",
  };
}

function getOpenDdlPresentation(): {
  formatOnOpen: boolean;
  editorLanguage?: QueryEditorLanguage;
} {
  return {
    formatOnOpen: true,
  };
}

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
    const connected = await connectWithProgress(
      connectionManager,
      connectionId,
      "RapiDB: Connecting…",
      true,
      undefined,
      "automatic",
    );
    refresh();
    return connected;
  } catch (err: unknown) {
    const error = logErrorWithContext(
      failureContext,
      err,
      connectionManager.getConnectionDiagnosticContext?.(connectionId),
    );
    vscode.window.showErrorMessage(`[RapiDB] Cannot connect: ${error.message}`);
    return false;
  }
}

function warnDefinitionTargetChanged(): void {
  vscode.window.showWarningMessage(
    "[RapiDB] The connection changed or could not be verified while loading the definition. Refresh the Explorer and open the definition again.",
  );
}

async function loadBoundDefinition(
  connectionManager: ConnectionManager,
  connectionId: string,
  driver: IDBDriver,
  lookup: () => Promise<string | null> | undefined,
  onLoaded: (definition: string | null, binding: QueryTargetBinding) => void,
): Promise<void> {
  // Use the driver's recorded owner, not just the current profile at this ID.
  const sessionKey = connectionManager.getConnectedDriverSessionKey(
    connectionId,
    driver,
  );
  const isCurrent = () =>
    connectionManager.getConnectedDriverSessionKey(connectionId, driver) ===
    sessionKey;
  const verifying =
    connectionManager.initializeConnectionCredentialRevision(connectionId);
  if (!sessionKey || (verifying && !(await verifying)) || !isCurrent()) {
    warnDefinitionTargetChanged();
    return;
  }

  const pending = lookup();
  if (!pending) return;
  const definition = await pending;
  const rechecking =
    connectionManager.initializeConnectionCredentialRevision(connectionId);
  if ((rechecking && !(await rechecking)) || !isCurrent()) {
    warnDefinitionTargetChanged();
    return;
  }
  onLoaded(definition, { connectionId, sessionKey });
}

function openDefinitionInQueryPanel(
  context: vscode.ExtensionContext,
  connectionManager: ConnectionManager,
  targetBinding: QueryTargetBinding,
  definition: string,
  postgresDatabase?: string,
  presentation?: {
    formatOnOpen: boolean;
    editorLanguage?: QueryEditorLanguage;
  },
): void {
  QueryPanel.createOrShow(
    context,
    connectionManager,
    targetBinding.connectionId,
    definition,
    presentation !== undefined,
    presentation?.formatOnOpen,
    false,
    presentation?.editorLanguage,
    postgresDatabase,
    targetBinding,
  );
}

function requirePostgresDefinitionDatabase(database: string): boolean {
  if (isPostgresQueryDatabase(database)) return true;
  vscode.window.showErrorMessage(
    "[RapiDB] The selected PostgreSQL object's database is unknown. Refresh the Explorer and select the object again.",
  );
  return false;
}

export function registerExplorerCommands(
  ctx: ExplorerCommandContext,
  registerCommand: <TArgs extends unknown[]>(
    command: string,
    callback: (...args: TArgs) => unknown,
  ) => vscode.Disposable,
): void {
  const { context, connectionManager, refresh } = ctx;

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

  registerCommand(CMD.showDDL, async (node?: RapiDBNode) => {
    if (!node?.connectionId) {
      vscode.window.showWarningMessage(
        "[RapiDB] Select a table, view, materialized view, function, procedure, sequence, type, constraint, index, or trigger node first.",
      );
      return;
    }

    const connection = connectionManager.getConnection(node.connectionId);
    const connectionType = connection?.type;
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
    if (!connection || !driver) {
      vscode.window.showErrorMessage("[RapiDB] Not connected. Connect first.");
      return;
    }

    const location = readNodeLocation(node);
    const postgresDatabase =
      connectionType === "pg" ? location.database : undefined;
    if (
      postgresDatabase !== undefined &&
      !requirePostgresDefinitionDatabase(postgresDatabase)
    ) {
      return;
    }

    const objectKind = isDbObjectKind(node.kind) ? node.kind : undefined;
    const lookup = () => {
      if (
        (node.kind === "table" ||
          node.kind === "view" ||
          node.kind === "materializedView") &&
        node.objectName
      ) {
        return driver.getCreateTableDDL(
          location.database,
          location.schema,
          node.objectName,
        );
      }
      if (objectKind && isRoutineDbObjectKind(objectKind) && node.objectName) {
        return driver.getRoutineDefinition(
          location.database,
          location.schema,
          node.objectName,
          objectKind,
          node.detailKey,
        );
      }
      if (objectKind && isDdlOnlyDbObjectKind(objectKind) && node.objectName) {
        return driver.getObjectDefinition(
          location.database,
          location.schema,
          node.objectName,
          objectKind,
        );
      }
      if (
        node.kind === "table_detail_constraint" &&
        node.parentTable &&
        node.objectName
      ) {
        return driver.getConstraintDDL(
          location.database,
          location.schema,
          node.parentTable,
          node.objectName,
          ...(node.constraintKind ? ([node.constraintKind] as const) : []),
        );
      }
      if (
        node.kind === "table_detail_index" &&
        node.parentTable &&
        node.objectName
      ) {
        return driver.getIndexDDL(
          location.database,
          location.schema,
          node.parentTable,
          node.objectName,
        );
      }
      if (
        node.kind === "table_detail_trigger" &&
        node.parentTable &&
        node.objectName
      ) {
        return driver.getTriggerDDL(
          location.database,
          location.schema,
          node.parentTable,
          node.objectName,
        );
      }

      vscode.window.showWarningMessage(
        "[RapiDB] DDL is available only for table, view, materialized view, function, procedure, sequence, type, constraint, index, and trigger nodes.",
      );
    };

    try {
      await loadBoundDefinition(
        connectionManager,
        node.connectionId,
        driver,
        lookup,
        (ddl, binding) => {
          if (!ddl) {
            const kindLabel = objectKind ?? node.kind;
            const objectName =
              node.objectName ?? node.parentTable ?? "selected node";
            vscode.window.showWarningMessage(
              `[RapiDB] DDL is currently unavailable for ${kindLabel} "${objectName}". Check object permissions (for example, DBMS_METADATA access on Oracle) and retry.`,
            );
            return;
          }
          const ddlPresentation =
            connectionManager.getQueryEditorPresentation(node.connectionId) ??
            getOpenDdlPresentation();
          openDefinitionInQueryPanel(
            context,
            connectionManager,
            binding,
            ddl,
            postgresDatabase,
            objectKind && isRoutineDbObjectKind(objectKind)
              ? undefined
              : {
                  formatOnOpen: ddlPresentation.formatOnOpen ?? false,
                  editorLanguage: ddlPresentation.editorLanguage,
                },
          );
        },
      );
    } catch (err: unknown) {
      const error = logErrorWithContext(
        `Load DDL failed for ${node.objectName}`,
        err,
        connectionManager.getConnectionDiagnosticContext?.(
          node.connectionId,
          driver,
        ) ?? connection,
      );
      vscode.window.showErrorMessage(`[RapiDB] DDL error: ${error.message}`);
    }
  });

  registerCommand(CMD.copyNodeName, async (node?: RapiDBNode) => {
    const name =
      node?.objectName &&
      ((isDbObjectKind(node.kind) && isDataDbObjectKind(node.kind)) ||
        node.kind.startsWith("table_section_")) &&
      connectionManager.getConnection(node.connectionId)?.type === "redis"
        ? redisKeyspaceDisplayName(node.objectName)
        : (node?.objectName ?? node?.label?.toString());
    if (name) {
      await vscode.env.clipboard.writeText(name);
    }
  });

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

  registerCommand(CMD.openRoutine, async (node?: RapiDBNode) => {
    if (!node?.connectionId || !node.objectName) {
      return;
    }

    const objectKind = isDbObjectKind(node.kind) ? node.kind : undefined;
    if (!objectKind || !isRoutineDbObjectKind(objectKind)) {
      return;
    }

    const kind = objectKind;
    const location = readNodeLocation(node);
    const connection = connectionManager.getConnection(node.connectionId);
    if (!connection) return;
    const postgresDatabase =
      connection.type === "pg" ? location.database : undefined;
    if (
      postgresDatabase !== undefined &&
      !requirePostgresDefinitionDatabase(postgresDatabase)
    ) {
      return;
    }

    const { connectionId, objectName, detailKey } = node;
    let driver = connectionManager.getDriver(connectionId);

    try {
      if (!driver || !connectionManager.isConnected(connectionId)) {
        // Initialize a cold profile's credential revision before taking its
        // session key, but don't allow settings edits during that read.
        const generation =
          connectionManager.getConnectionSessionGeneration(connectionId);
        const isSameGeneration = () =>
          generation ===
          connectionManager.getConnectionSessionGeneration(connectionId);
        const settingsKey = connectionSessionKey(connection);
        const initializing =
          connectionManager.initializeConnectionCredentialRevision(
            connectionId,
          );
        if (
          !settingsKey ||
          (initializing && !(await initializing)) ||
          !isSameGeneration() ||
          settingsKey !==
            connectionSessionKey(connectionManager.getConnection(connectionId))
        ) {
          warnDefinitionTargetChanged();
          return;
        }

        const expectedKey =
          connectionManager.getQueryTargetSessionKey(connectionId);
        if (!expectedKey) {
          warnDefinitionTargetChanged();
          return;
        }
        const connected = await ensureConnectionReady(
          connectionManager,
          connectionId,
          refresh,
          `Open routine connect failed for ${objectName}`,
        );
        if (
          !isSameGeneration() ||
          expectedKey !==
            connectionManager.getQueryTargetSessionKey(connectionId)
        ) {
          warnDefinitionTargetChanged();
          return;
        }
        if (!connected) return;
        driver = connectionManager.getDriver(connectionId);
      }

      const sourceDriver = driver;
      if (!sourceDriver) {
        warnDefinitionTargetChanged();
        return;
      }
      await loadBoundDefinition(
        connectionManager,
        connectionId,
        sourceDriver,
        () =>
          sourceDriver.getRoutineDefinition(
            location.database,
            location.schema,
            objectName,
            kind,
            detailKey,
          ),
        (sql, binding) => {
          if (sql !== null) {
            openDefinitionInQueryPanel(
              context,
              connectionManager,
              binding,
              sql,
              postgresDatabase,
            );
          }
        },
      );
    } catch (err: unknown) {
      const error = logErrorWithContext(
        `Load routine definition failed for ${node.objectName}`,
        err,
        connectionManager.getConnectionDiagnosticContext?.(
          node.connectionId,
          driver,
        ),
      );
      vscode.window.showErrorMessage(
        `[RapiDB] Cannot load ${kind} definition: ${error.message}`,
      );
    }
  });
}
