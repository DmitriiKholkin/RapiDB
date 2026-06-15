/**
 * Webview Contracts — Barrel Re-export
 *
 * All types and parsers are defined in domain-specific modules:
 *   - shared.ts    — Core types, envelope parsers, utilities
 *   - query.ts     — Query panel types and message parser
 *   - table.ts     — Table panel types and message parser
 *   - erd.ts       — ERD panel types and message parser
 *   - connection.ts — Connection form types and message parser
 *
 * Existing imports from `"../../shared/webviewContracts"` continue
 * to work via this re-export.
 */

export type {
  ConnectionFormBrowseTarget,
  ConnectionFormExistingState,
  ConnectionFormInitialState,
  ConnectionFormPanelMessage,
  ConnectionFormSubmission,
  SanitizedConnectionConfig,
  SanitizedSshConfig,
} from "./connection";
export {
  parseConnectionFormExistingState,
  parseConnectionFormPanelMessage,
  parseConnectionFormSubmission,
} from "./connection";
export type {
  ErdGraph,
  ErdGraphScope,
  ErdInitialState,
  ErdNodeColumn,
  ErdNodePosition,
  ErdPanelMessage,
  ErdRelationshipEdge,
  ErdTableNode,
} from "./erd";
export { parseErdPanelMessage } from "./erd";
export type {
  QueryEditorLanguage,
  QueryEditorMode,
  QueryEditorPresentation,
  QueryEditorSqlDialect,
  QueryInitialState,
  QueryPanelMessage,
} from "./query";
export { parseQueryPanelMessage } from "./query";
export type {
  PanelRetentionMode,
  PanelRetentionState,
  WebviewMessageEnvelope,
} from "./shared";
export {
  isRecord,
  parseEnvelope,
  parseEnvelopeQueryPayload,
  parseEnvelopeTextPayload,
  parseOptionalPayloadRecord,
  parseRequiredPayloadRecord,
  readConnectionType,
  readOptionalBoolean,
  readOptionalNumber,
  readOptionalString,
  readPositiveInteger,
  readRequiredString,
} from "./shared";
export type {
  ApplyResultPayload,
  ApplyRowOutcome,
  ApplyRowStatus,
  RowUpdateMessagePayload,
  TableInitialState,
  TableMutationPreviewDecisionPayload,
  TableMutationPreviewKind,
  TableMutationPreviewPayload,
  TablePanelMessage,
} from "./table";
export { parseTablePanelMessage } from "./table";

// ─── Composite Types & Parsers (defined here to avoid circular deps) ───────

import type { ConnectionFormInitialState } from "./connection";
import { parseConnectionFormInitialState } from "./connection";
import type { ErdInitialState } from "./erd";
import { parseErdInitialState } from "./erd";
import type { QueryInitialState } from "./query";
import { parseQueryInitialState } from "./query";
import { isRecord } from "./shared";
import type { TableInitialState } from "./table";
import { parseTableInitialState } from "./table";

export type WebviewInitialState =
  | QueryInitialState
  | TableInitialState
  | ErdInitialState
  | ConnectionFormInitialState;

/**
 * Parses the raw initial state injected by the extension host into
 * a strongly-typed discriminated union based on the `view` field.
 * Returns `null` when the input is not a record or has an unknown
 * `view` value.
 */
export function parseWebviewInitialState(
  input: unknown,
): WebviewInitialState | null {
  if (!isRecord(input)) {
    return null;
  }
  switch (input.view) {
    case "query":
      return parseQueryInitialState(input);
    case "table":
      return parseTableInitialState(input);
    case "erd":
      return parseErdInitialState(input);
    case "connection":
      return parseConnectionFormInitialState(input);
    default:
      return null;
  }
}
