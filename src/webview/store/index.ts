import { create } from "zustand";

export interface QueryResult {
  columns: string[];
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

export interface SchemaTable {
  schema: string;
  table: string;
  columns: SchemaColumn[];
}

export interface SchemaState {
  schemaByConnection: Record<string, SchemaTable[]>;
  setSchema: (connectionId: string, schema: SchemaTable[]) => void;
  getSchema: (connectionId: string) => SchemaTable[];
}

export const useQueryStore = create<QueryState>((set) => ({
  status: "idle",
  result: null,
  setRunning: () => set({ status: "running", result: null }),
  setResult: (result) => set({ status: "success", result }),
  setError: (error) =>
    set({
      status: "error",
      result: { columns: [], rows: [], rowCount: 0, executionTimeMs: 0, error },
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
    set((s) => ({ schemaByConnection: { ...s.schemaByConnection, [connectionId]: schema } })),
  getSchema: (connectionId) => get().schemaByConnection[connectionId] ?? [],
}));
