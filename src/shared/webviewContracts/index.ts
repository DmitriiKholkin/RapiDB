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
  BookmarkSavedPayload,
  QueryBookmarkPayload,
  QueryEditorLanguage,
  QueryEditorMode,
  QueryEditorPresentation,
  QueryEditorSqlDialect,
  QueryInitialState,
  QueryPanelMessage,
  QueryResultExportPayload,
} from "./query";
export { parseBookmarkSavedPayload, parseQueryPanelMessage } from "./query";
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
  ClipboardReadPayload,
  ClipboardTextPayload,
  DeleteResultPayload,
  DeleteRowOutcome,
  RowUpdateMessagePayload,
  TableInitialState,
  TableInitPayload,
  TableMutationPreviewDecisionPayload,
  TableMutationPreviewKind,
  TableMutationPreviewPayload,
  TablePanelMessage,
} from "./table";
export { parseTablePanelMessage } from "./table";

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
