import type { ConnectionConfig } from "../shared/connectionConfig";
import type { ConnectionValidationResult } from "../shared/connectionValidation";
import type { DataDbObjectKind, DbObjectKind } from "../shared/dbObjectKinds";
import type {
  ColumnTypeMeta,
  DriverTableSectionKind,
  IndexMeta,
  TableConstraintMeta,
  TriggerMeta,
} from "./dbDrivers/types";

export type { ConnectionConfig } from "../shared/connectionConfig";
export interface TestConnectionResult {
  success: boolean;
  error?: string;
  validation?: ConnectionValidationResult;
}
export interface HistoryEntry {
  id: string;
  sql: string;
  connectionId: string;
  executedAt: string;
}
export interface BookmarkEntry {
  id: string;
  sql: string;
  connectionId: string;
  savedAt: string;
}
export interface ConnectAttempt {
  promise: Promise<void>;
  isNew: boolean;
}
export interface SchemaObjectEntry {
  database: string;
  schema: string;
  object: string;
  type?: DbObjectKind;
  routineIdentity?: string;
  columns: {
    name: string;
    type: string;
  }[];
}

export interface SchemaColumnEntry {
  name: string;
  type: string;
}

export interface SchemaSnapshotObjectEntry {
  name: string;
  type: DbObjectKind;
  routineIdentity?: string;
  columns: SchemaColumnEntry[];
}

export interface SchemaSnapshotSchemaEntry {
  name: string;
  objects: SchemaSnapshotObjectEntry[];
}

export interface SchemaSnapshotDatabaseEntry {
  name: string;
  schemas: SchemaSnapshotSchemaEntry[];
}

export interface SchemaSnapshot {
  databases: SchemaSnapshotDatabaseEntry[];
}

export type SchemaLoadStatus = "idle" | "loading" | "loaded" | "error";

export type ExplorerSchemaScope =
  | { kind: "connectionRoot" }
  | { kind: "database"; database: string }
  | { kind: "schema"; database: string; schema: string };

export type SchemaScopeKey = string;

export interface RefreshSchemaRequest {
  connectionId?: string;
  reason?: "manual" | "reconnect" | "config-change";
}

export interface SchemaSnapshotState {
  snapshot: SchemaSnapshot;
  status: SchemaLoadStatus;
  isPartial: boolean;
  error?: string;
}

export interface ScopedSchemaFragment {
  database?: SchemaSnapshotDatabaseEntry;
  schema?: SchemaSnapshotSchemaEntry;
}

export interface ScopedSchemaCacheEntry extends SchemaSnapshotState {
  scope: ExplorerSchemaScope;
  key: SchemaScopeKey;
  fragment: ScopedSchemaFragment;
  generation: number;
}

export type TableDetailSectionKind = DriverTableSectionKind;

export interface TableDetailRequest {
  connectionId: string;
  database: string;
  schema: string;
  table: string;
  objectKind: DataDbObjectKind;
}

export interface TableDetailSectionState<T> {
  status: SchemaLoadStatus;
  items: T[];
  error?: string;
}

export interface TableStructureSnapshot {
  columns: TableDetailSectionState<ColumnTypeMeta>;
  constraints: TableDetailSectionState<TableConstraintMeta>;
  indexes: TableDetailSectionState<IndexMeta>;
  triggers: TableDetailSectionState<TriggerMeta>;
}

export interface TableDetailState {
  request: TableDetailRequest;
  snapshot: TableStructureSnapshot;
  status: SchemaLoadStatus;
  isPartial: boolean;
  error?: string;
}

export interface ScopeAwareConnectionManagerApi {
  ensureSchemaScopeLoading(
    connectionId: string,
    scope: ExplorerSchemaScope,
  ): void;
  getSchemaSnapshotState(
    connectionId: string,
    scope?: ExplorerSchemaScope,
  ): SchemaSnapshotState;
  markSchemaScopeExpanded(
    connectionId: string,
    scope: ExplorerSchemaScope,
  ): void;
  markSchemaScopeCollapsed(
    connectionId: string,
    scope: ExplorerSchemaScope,
  ): void;
  refreshSchemaCache(request?: RefreshSchemaRequest): void;
  getSchemaAsync(connectionId: string): Promise<SchemaObjectEntry[]>;
  ensureTableDetailLoading(request: TableDetailRequest): void;
  getTableDetailState(request: TableDetailRequest): TableDetailState;
}

export interface ConnectionManagerLifecycleApi {
  dispose(): Promise<void>;
}

/**
 * Read-only editor-presentation metadata used by panels/commands.
 * Implemented by {@link ConnectionManager} but declared as a standalone
 * interface so panels/commands can depend on the smallest possible
 * surface (no `as ConnectionManager & {...}` casts).
 */
export interface QueryEditorPresentationApi {
  getQueryEditorPresentation(
    connectionId: string,
  ): import("../shared/webviewContracts").QueryEditorPresentation | undefined;
}

/**
 * Read-only driver-entity metadata used by the explorer to decide what
 * kinds of DDL are supported for a given connection.
 */
export interface DriverMetadataApi {
  getDriverEntityManifest(
    connectionId: string,
  ): import("./dbDrivers/types").DriverEntityManifest;
}

/**
 * Read-only driver-capability metadata used to decide which read-only
 * query guard applies for a connection.
 */
export interface DriverCapabilitiesApi {
  getDriverCapabilities(
    connectionId: string,
  ): import("./dbDrivers/types").DriverCapabilities | undefined;
}

export interface StoredConnectionConfig extends ConnectionConfig {
  user?: string;
}
