import type { ConnectionConfig } from "./connectionConfig";
import type { ConnectionType } from "./connectionTypes";

export interface WebviewMessageEnvelope<
  TType extends string = string,
  TPayload = unknown,
> {
  type: TType;
  payload?: TPayload;
}

export interface QueryInitialState {
  view: "query";
  connectionId: string;
  connectionType?: ConnectionType | "";
  initialSql?: string;
  formatOnOpen?: boolean;
  isBookmarked?: boolean;
}

export interface TableInitialState {
  view: "table";
  connectionId: string;
  database: string;
  schema: string;
  table: string;
  isView?: boolean;
  defaultPageSize?: number;
}

export interface SchemaInitialState {
  view: "schema";
  connectionId: string;
  database: string;
  schema: string;
  table: string;
}

export interface ConnectionFormInitialState {
  view: "connection";
  existing: ConnectionConfig | null;
}

export type WebviewInitialState =
  | QueryInitialState
  | TableInitialState
  | SchemaInitialState
  | ConnectionFormInitialState;