import { create } from "zustand";
import type { QueryColumnMeta } from "../../shared/tableTypes";

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

function normalizeQueryResult(result: QueryResult): QueryResult {
  return {
    ...result,
    columnMeta: Array.isArray(result.columnMeta) ? result.columnMeta : [],
  };
}

export interface ConnectionEntry {
  id: string;
  name: string;
  type: string;
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
  type?: "table" | "view" | "function" | "procedure";
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
