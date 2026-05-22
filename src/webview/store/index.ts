import { create } from "zustand";
import type { DbObjectKind } from "../../shared/dbObjectKinds";
import { QUERY_LIMIT_POLICY } from "../../shared/safetyContracts";
import type { QueryColumnMeta } from "../../shared/tableTypes";
import type { QueryEditorPresentation } from "../../shared/webviewContracts";

export interface QueryResult {
  columns: string[];
  columnMeta: QueryColumnMeta[];
  rows: Record<string, unknown>[];
  rowCount: number;
  executionTimeMs: number;
  error?: string;

  truncated?: boolean;

  truncatedAt?: number;
}

export type QueryStatus = "idle" | "running" | "success" | "error";

export interface QueryState {
  status: QueryStatus;
  result: QueryResult | null;
  setRunning: () => void;
  setResult: (result: QueryResult) => void;
  setError: (error: string) => void;
  reset: () => void;
}

const WEBVIEW_QUERY_RESULT_HARD_CAP = QUERY_LIMIT_POLICY.hardCap;

function normalizeQueryResult(result: QueryResult): QueryResult {
  const columnMeta = Array.isArray(result.columnMeta) ? result.columnMeta : [];
  const rows = Array.isArray(result.rows) ? result.rows : [];
  const rowCount =
    Number.isFinite(result.rowCount) && result.rowCount >= 0
      ? result.rowCount
      : rows.length;
  const rowsExceedHardCap = rows.length > WEBVIEW_QUERY_RESULT_HARD_CAP;

  if (!rowsExceedHardCap && columnMeta === result.columnMeta) {
    return result;
  }

  const cappedRows = rowsExceedHardCap
    ? rows.slice(0, WEBVIEW_QUERY_RESULT_HARD_CAP)
    : rows;
  const normalizedTruncated = result.truncated || rowsExceedHardCap;
  const normalized: QueryResult = {
    ...result,
    columnMeta,
    rows: cappedRows,
    rowCount: rowsExceedHardCap ? Math.max(rowCount, rows.length) : rowCount,
    truncated: normalizedTruncated,
  };

  if (normalizedTruncated && normalized.truncatedAt === undefined) {
    normalized.truncatedAt = cappedRows.length;
  }

  return normalized;
}

export interface ConnectionEntry {
  id: string;
  name: string;
  type: string;
  editorPresentation?: QueryEditorPresentation;
}

export interface ConnectionState {
  connections: ConnectionEntry[];
  activeConnectionId: string;
  setConnections: (conns: ConnectionEntry[]) => void;
  setActiveConnection: (id: string) => void;
}

export interface SchemaColumn {
  name: string;
  type: string;
}

export interface SchemaObject {
  database: string;
  schema: string;
  object: string;
  type?: DbObjectKind;
  columns: SchemaColumn[];
}

export interface SchemaState {
  schemaByConnection: Record<string, SchemaObject[]>;
  setSchema: (connectionId: string, schema: SchemaObject[]) => void;
  getSchema: (connectionId: string) => SchemaObject[];
}

export const useQueryStore = create<QueryState>((set) => ({
  status: "idle",
  result: null,
  setRunning: () => set({ status: "running", result: null }),
  setResult: (result) =>
    set({ status: "success", result: normalizeQueryResult(result) }),
  setError: (error) =>
    set({
      status: "error",
      result: {
        columns: [],
        columnMeta: [],
        rows: [],
        rowCount: 0,
        executionTimeMs: 0,
        error,
      },
    }),
  reset: () => set({ status: "idle", result: null }),
}));

export const useConnectionStore = create<ConnectionState>((set) => ({
  connections: [],
  activeConnectionId: "",
  setConnections: (connections) => set({ connections }),
  setActiveConnection: (id) => set({ activeConnectionId: id }),
}));

export const useSchemaStore = create<SchemaState>((set, get) => ({
  schemaByConnection: {},
  setSchema: (connectionId, schema) =>
    set((s) => ({
      schemaByConnection: { ...s.schemaByConnection, [connectionId]: schema },
    })),
  getSchema: (connectionId) => get().schemaByConnection[connectionId] ?? [],
}));
