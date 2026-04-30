import type { ConnectionConfig } from "../shared/connectionConfig";

export type { ConnectionConfig } from "../shared/connectionConfig";
export interface TestConnectionResult {
  success: boolean;
  error?: string;
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
  type?: "table" | "view" | "function" | "procedure";
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
  type: "table" | "view" | "function" | "procedure";
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
}

export interface StoredConnectionConfig extends ConnectionConfig {
  user?: string;
}
