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
  schema: string;
  object: string;
  type?: "table" | "view" | "function" | "procedure";
  columns: {
    name: string;
    type: string;
  }[];
}
export interface StoredConnectionConfig extends ConnectionConfig {
  user?: string;
}
