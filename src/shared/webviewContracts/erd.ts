/**
 * ERD panel types, initial state, and message parser.
 *
 * All parsers are pure; `null` indicates a parse failure.
 */
import type { PrimaryKeyRole } from "../tableTypes";
import type { PanelRetentionState, WebviewMessageEnvelope } from "./shared";
import {
  isRecord,
  parseEnvelope,
  parseRequiredPayloadRecord,
  readOptionalBoolean,
  readOptionalString,
  readRequiredString,
} from "./shared";

// ─── ERD Graph Types ────────────────────────────────────────────────────────

export interface ErdNodeColumn {
  name: string;
  type: string;
  isPrimaryKey: boolean;
  primaryKeyRole?: PrimaryKeyRole;
  isForeignKey: boolean;
  nullable: boolean;
}

export interface ErdNodePosition {
  x: number;
  y: number;
}

export interface ErdTableNode {
  id: string;
  database: string;
  schema: string;
  table: string;
  isView: boolean;
  columns: ErdNodeColumn[];
  position: ErdNodePosition;
}

export interface ErdRelationshipEdge {
  id: string;
  fromTableId: string;
  toTableId: string;
  fromColumn: string;
  toColumn: string;
  constraintName: string;
  cardinality: "one-to-one" | "many-to-one" | "unknown";
  sourceNullable: boolean;
}

export interface ErdGraphScope {
  database?: string;
  schema?: string;
}

export interface ErdGraph {
  nodes: ErdTableNode[];
  edges: ErdRelationshipEdge[];
  scope: ErdGraphScope;
}

// ─── Initial State ──────────────────────────────────────────────────────────

export interface ErdInitialState extends PanelRetentionState {
  view: "erd";
  connectionId: string;
  database?: string;
  schema?: string;
}

// ─── Initial State Parser ───────────────────────────────────────────────────

export function parseErdInitialState(
  input: Record<string, unknown>,
): ErdInitialState | null {
  const connectionId = readRequiredString(input, "connectionId");
  if (!connectionId) {
    return null;
  }
  return {
    view: "erd",
    connectionId,
    database: readOptionalString(input, "database"),
    schema: readOptionalString(input, "schema"),
  };
}

// ─── Messages ───────────────────────────────────────────────────────────────

export type ErdPanelMessage =
  | WebviewMessageEnvelope<"ready">
  | WebviewMessageEnvelope<"reload">
  | WebviewMessageEnvelope<
      "openTableData",
      { table: string; schema?: string; database?: string; isView?: boolean }
    >;

// ─── Message Parser ─────────────────────────────────────────────────────────

export function parseErdPanelMessage(input: unknown): ErdPanelMessage | null {
  const envelope = parseEnvelope(input);
  if (!envelope) {
    return null;
  }

  switch (envelope.type) {
    case "ready":
    case "reload":
      return { type: envelope.type };

    case "openTableData": {
      const payload = parseRequiredPayloadRecord(envelope);
      if (!payload) {
        return null;
      }
      const table = readRequiredString(payload, "table");
      if (!table) {
        return null;
      }
      return {
        type: envelope.type,
        payload: {
          table,
          schema: readOptionalString(payload, "schema"),
          database: readOptionalString(payload, "database"),
          isView: readOptionalBoolean(payload, "isView"),
        },
      };
    }

    default:
      return null;
  }
}
