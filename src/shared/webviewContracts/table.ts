/**
 * Table panel types, initial state, and message parser.
 *
 * All parsers are pure; `null` indicates a parse failure.
 */

import type { ColumnTypeMeta } from "../tableTypes";
import type { PanelRetentionState, WebviewMessageEnvelope } from "./shared";
import {
  isRecord,
  parseEnvelope,
  parseEnvelopeTextPayload,
  parseOptionalPayloadRecord,
  parseRequiredPayloadRecord,
  readOptionalBoolean,
  readOptionalNumber,
  readOptionalString,
  readPositiveInteger,
  readRequiredString,
} from "./shared";

// ─── Apply Result Types ─────────────────────────────────────────────────────

export type ApplyRowStatus =
  | "applied"
  | "not_applied"
  | "unknown"
  | "skipped"
  | "prevalidation_failed"
  | "verification_failed";

export interface ApplyRowOutcome {
  rowIndex: number;
  success: boolean;
  status: ApplyRowStatus;
  message?: string;
  columns?: string[];
}

export interface ApplyResultPayload {
  operationId?: string;
  success: boolean;
  error?: string;
  /** Whether a failed apply may have persisted any write. */
  changesPossible?: boolean;
  /** Whether write execution/settlement is uncertain (not a post-commit read failure). */
  outcomeUnknown?: boolean;
  warning?: string;
  failedRows?: number[];
  rowOutcomes?: ApplyRowOutcome[];
  /** Whether at least one insert is known to have completed, not merely possibly applied. */
  insertApplied?: boolean;
  insertRowOutcomes?: ApplyRowOutcome[];
}

// ─── Mutation Preview Types ─────────────────────────────────────────────────

export interface RowUpdateMessagePayload {
  primaryKeys: Record<string, unknown>;
  changes: Record<string, unknown>;
  originalValues?: Record<string, unknown>;
}

export type TableMutationPreviewKind =
  | "applyChanges"
  | "insertRow"
  | "deleteRows";

export interface TableMutationPreviewPayload {
  operationId: string;
  previewToken: string;
  kind: TableMutationPreviewKind;
  title: string;
  text: string;
  contentType: "application/sql" | "application/json" | "text/plain";
  sql: string;
  statementCount: number;
}

export interface TableMutationPreviewDecisionPayload {
  operationId: string;
  previewToken: string;
}

export interface ClipboardReadPayload {
  requestId: string;
  recipient: string;
}

export interface ClipboardTextPayload extends ClipboardReadPayload {
  text: string;
}

export interface TableMutationResultPayload {
  operationId: string;
  success: boolean;
  error?: string;
  status?: "prevalidation_failed";
  columns?: string[];
}

export interface DeleteRowOutcome {
  rowIndex: number;
  primaryKeys: Record<string, unknown>;
  success: boolean;
  status: "deleted" | "notfound" | "failed" | "unknown" | "skipped";
  message?: string;
}

export interface DeleteResultPayload {
  operationId?: string;
  success: boolean;
  affectedRows: number;
  rowOutcomes: DeleteRowOutcome[];
  /** A read is needed to reconcile a committed or possibly applied delete. */
  changesPossible: boolean;
  /** A delete write may still settle after the first refresh; excludes post-commit verification failures. */
  outcomeUnknown: boolean;
  error?: string;
}

// ─── Initial State ──────────────────────────────────────────────────────────

export interface TableInitialState extends PanelRetentionState {
  view: "table";
  connectionId: string;
  database: string;
  schema: string;
  table: string;
  displayTableName?: string;
  isView?: boolean;
  objectKind?: "table" | "view" | "materializedView";
  connectionReadOnly?: boolean;
  mongoRowIdentity?: boolean;
  defaultPageSize?: number;
}

// ─── Initial State Parser ───────────────────────────────────────────────────

export function parseTableInitialState(
  input: Record<string, unknown>,
): TableInitialState | null {
  const connectionId = readRequiredString(input, "connectionId");
  const database = readOptionalString(input, "database");
  const schema = readOptionalString(input, "schema");
  const table = readRequiredString(input, "table");
  const objectKind = input.objectKind;
  if (
    !connectionId ||
    database === undefined ||
    schema === undefined ||
    !table
  ) {
    return null;
  }
  if (
    objectKind !== undefined &&
    objectKind !== "table" &&
    objectKind !== "view" &&
    objectKind !== "materializedView"
  ) {
    return null;
  }
  return {
    view: "table",
    connectionId,
    database,
    schema,
    table,
    displayTableName: readOptionalString(input, "displayTableName"),
    isView: readOptionalBoolean(input, "isView"),
    objectKind,
    connectionReadOnly: readOptionalBoolean(input, "connectionReadOnly"),
    mongoRowIdentity: readOptionalBoolean(input, "mongoRowIdentity"),
    defaultPageSize: readOptionalNumber(input, "defaultPageSize"),
  };
}

// ─── Messages ───────────────────────────────────────────────────────────────

export type TablePanelMessage =
  | WebviewMessageEnvelope<"ready">
  | WebviewMessageEnvelope<
      "fetchPage",
      {
        fetchId?: number;
        page?: number | string;
        pageSize?: number | string;
        filters?: unknown;
        sort?: unknown;
      }
    >
  | WebviewMessageEnvelope<
      "applyChanges",
      {
        operationId: string;
        updates?: RowUpdateMessagePayload[];
        insertValues?: Record<string, unknown>[];
      }
    >
  | WebviewMessageEnvelope<
      "insertRow",
      { operationId: string; values?: Record<string, unknown> }
    >
  | WebviewMessageEnvelope<
      "deleteRows",
      {
        operationId: string;
        primaryKeysList?: Array<Record<string, unknown>>;
      }
    >
  | WebviewMessageEnvelope<
      "exportCSV",
      {
        sort?: unknown;
        filters?: unknown[];
        limitToPage?: { page: number; pageSize: number };
        columnOrder?: string[];
      }
    >
  | WebviewMessageEnvelope<
      "exportJSON",
      {
        sort?: unknown;
        filters?: unknown[];
        limitToPage?: { page: number; pageSize: number };
        columnOrder?: string[];
      }
    >
  | WebviewMessageEnvelope<
      "confirmMutationPreview",
      TableMutationPreviewDecisionPayload
    >
  | WebviewMessageEnvelope<
      "cancelMutationPreview",
      TableMutationPreviewDecisionPayload
    >
  | WebviewMessageEnvelope<"readClipboard", ClipboardReadPayload>
  | WebviewMessageEnvelope<"writeClipboard", { text: string }>;

// ─── Parser Helpers ─────────────────────────────────────────────────────────

function isRowUpdateMessagePayload(
  value: unknown,
): value is RowUpdateMessagePayload {
  return (
    isRecord(value) &&
    isRecord(value.primaryKeys) &&
    isRecord(value.changes) &&
    (value.originalValues === undefined || isRecord(value.originalValues))
  );
}

function isArrayOfRecords(
  value: unknown,
): value is Array<Record<string, unknown>> {
  return Array.isArray(value) && value.every(isRecord);
}

function readLimitToPage(
  value: unknown,
): { page: number; pageSize: number } | undefined {
  if (!isRecord(value)) {
    return undefined;
  }
  const page = readPositiveInteger(value.page);
  const pageSize = readPositiveInteger(value.pageSize);
  if (page === undefined || pageSize === undefined) {
    return undefined;
  }
  return { page, pageSize };
}

function readOperationId(payload: Record<string, unknown>): string | null {
  return readRequiredString(payload, "operationId");
}

function readClipboardRequest(
  payload: Record<string, unknown>,
): ClipboardReadPayload | null {
  const requestId = readRequiredString(payload, "requestId");
  const recipient = readRequiredString(payload, "recipient");
  return requestId && recipient ? { requestId, recipient } : null;
}

// ─── Message Parser ─────────────────────────────────────────────────────────

export function parseTablePanelMessage(
  input: unknown,
): TablePanelMessage | null {
  const envelope = parseEnvelope(input);
  if (!envelope) {
    return null;
  }

  switch (envelope.type) {
    case "ready":
      return { type: envelope.type };

    case "fetchPage": {
      const payload = parseOptionalPayloadRecord(envelope);
      if (!payload) {
        return null;
      }
      return {
        type: envelope.type,
        payload: {
          fetchId: readOptionalNumber(payload, "fetchId"),
          page: payload.page as number | string | undefined,
          pageSize: payload.pageSize as number | string | undefined,
          filters: payload.filters,
          sort: payload.sort,
        },
      };
    }

    case "applyChanges": {
      const payload = parseRequiredPayloadRecord(envelope);
      if (!payload) {
        return null;
      }
      const operationId = readOperationId(payload);
      if (!operationId) {
        return null;
      }
      const updates = payload.updates;
      const insertValues = payload.insertValues;
      if (
        updates !== undefined &&
        (!Array.isArray(updates) ||
          updates.some((item) => !isRowUpdateMessagePayload(item)))
      ) {
        return null;
      }
      if (
        insertValues !== undefined &&
        !isArrayOfRecords(insertValues as unknown[])
      ) {
        return null;
      }
      return {
        type: envelope.type,
        payload: {
          operationId,
          updates: updates as RowUpdateMessagePayload[] | undefined,
          insertValues: insertValues as Record<string, unknown>[] | undefined,
        },
      };
    }

    case "insertRow": {
      const payload = parseRequiredPayloadRecord(envelope);
      if (!payload) {
        return null;
      }
      const operationId = readOperationId(payload);
      if (!operationId) {
        return null;
      }
      const values = payload.values;
      if (values !== undefined && !isRecord(values)) {
        return null;
      }
      return {
        type: envelope.type,
        payload: {
          operationId,
          values: (values as Record<string, unknown> | undefined) ?? {},
        },
      };
    }

    case "deleteRows": {
      const payload = parseRequiredPayloadRecord(envelope);
      if (!payload) {
        return null;
      }
      const operationId = readOperationId(payload);
      if (!operationId) {
        return null;
      }
      const primaryKeysList = payload.primaryKeysList;
      if (primaryKeysList !== undefined && !isArrayOfRecords(primaryKeysList)) {
        return null;
      }
      return {
        type: envelope.type,
        payload: { operationId, primaryKeysList },
      };
    }

    case "exportCSV":
    case "exportJSON": {
      const payload = parseOptionalPayloadRecord(envelope);
      if (!payload) {
        return null;
      }
      if (payload.filters !== undefined && !Array.isArray(payload.filters)) {
        return null;
      }
      const columnOrder = payload.columnOrder;
      if (
        columnOrder !== undefined &&
        (!Array.isArray(columnOrder) ||
          !columnOrder.every((name) => typeof name === "string"))
      ) {
        return null;
      }
      return {
        type: envelope.type,
        payload: {
          sort: payload.sort,
          filters: payload.filters as unknown[] | undefined,
          limitToPage: readLimitToPage(payload.limitToPage),
          columnOrder: columnOrder as string[] | undefined,
        },
      };
    }

    case "confirmMutationPreview":
    case "cancelMutationPreview": {
      const payload = parseRequiredPayloadRecord(envelope);
      if (!payload) {
        return null;
      }
      const previewToken = readRequiredString(payload, "previewToken");
      const operationId = readOperationId(payload);
      return previewToken && operationId
        ? { type: envelope.type, payload: { previewToken, operationId } }
        : null;
    }

    case "readClipboard": {
      const payload = parseRequiredPayloadRecord(envelope);
      const request = payload ? readClipboardRequest(payload) : null;
      return request ? { type: envelope.type, payload: request } : null;
    }

    case "writeClipboard": {
      const payload = parseEnvelopeTextPayload(envelope);
      return payload ? { type: envelope.type, payload } : null;
    }

    default:
      return null;
  }
}
export interface TableInitPayload {
  /** connectionRefresh fences reads even when columns match; it is not a user verification Refresh. Omitted by older hosts: genuine initialization. */
  intent?: "initialize" | "metadataRefresh" | "connectionRefresh";
  columns: ColumnTypeMeta[];
  primaryKeyColumns: string[];
  isView?: boolean;
  connectionReadOnly?: boolean;
}
