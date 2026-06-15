/**
 * Table panel types, initial state, and message parser.
 *
 * All parsers are pure; `null` indicates a parse failure.
 */

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
  success: boolean;
  error?: string;
  warning?: string;
  failedRows?: number[];
  rowOutcomes?: ApplyRowOutcome[];
  insertApplied?: boolean;
}

// ─── Mutation Preview Types ─────────────────────────────────────────────────

export interface RowUpdateMessagePayload {
  primaryKeys: Record<string, unknown>;
  changes: Record<string, unknown>;
}

export type TableMutationPreviewKind =
  | "applyChanges"
  | "insertRow"
  | "deleteRows";

export interface TableMutationPreviewPayload {
  previewToken: string;
  kind: TableMutationPreviewKind;
  title: string;
  text: string;
  contentType: "application/sql" | "application/json" | "text/plain";
  sql: string;
  statementCount: number;
}

export interface TableMutationPreviewDecisionPayload {
  previewToken: string;
}

// ─── Initial State ──────────────────────────────────────────────────────────

export interface TableInitialState extends PanelRetentionState {
  view: "table";
  connectionId: string;
  database: string;
  schema: string;
  table: string;
  isView?: boolean;
  connectionReadOnly?: boolean;
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
  if (
    !connectionId ||
    database === undefined ||
    schema === undefined ||
    !table
  ) {
    return null;
  }
  return {
    view: "table",
    connectionId,
    database,
    schema,
    table,
    isView: readOptionalBoolean(input, "isView"),
    connectionReadOnly: readOptionalBoolean(input, "connectionReadOnly"),
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
        updates?: RowUpdateMessagePayload[];
        insertValues?: Record<string, unknown>;
      }
    >
  | WebviewMessageEnvelope<"insertRow", { values?: Record<string, unknown> }>
  | WebviewMessageEnvelope<
      "deleteRows",
      { primaryKeysList?: Array<Record<string, unknown>> }
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
  | WebviewMessageEnvelope<"readClipboard">
  | WebviewMessageEnvelope<"writeClipboard", { text: string }>;

// ─── Parser Helpers ─────────────────────────────────────────────────────────

function isRowUpdateMessagePayload(
  value: unknown,
): value is RowUpdateMessagePayload {
  return (
    isRecord(value) && isRecord(value.primaryKeys) && isRecord(value.changes)
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

function readColumnOrder(value: unknown): string[] | undefined {
  if (!Array.isArray(value)) {
    return undefined;
  }
  const strings = value.filter((v): v is string => typeof v === "string");
  return strings.length > 0 ? strings : undefined;
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
      const updates = payload.updates;
      const insertValues = payload.insertValues;
      if (
        updates !== undefined &&
        (!Array.isArray(updates) ||
          updates.some((item) => !isRowUpdateMessagePayload(item)))
      ) {
        return null;
      }
      if (insertValues !== undefined && !isRecord(insertValues)) {
        return null;
      }
      return {
        type: envelope.type,
        payload: {
          updates: updates as RowUpdateMessagePayload[] | undefined,
          insertValues: insertValues as Record<string, unknown> | undefined,
        },
      };
    }

    case "insertRow": {
      const payload = parseRequiredPayloadRecord(envelope);
      if (!payload) {
        return null;
      }
      const values = payload.values;
      if (values !== undefined && !isRecord(values)) {
        return null;
      }
      return {
        type: envelope.type,
        payload: {
          values: (values as Record<string, unknown> | undefined) ?? {},
        },
      };
    }

    case "deleteRows": {
      const payload = parseRequiredPayloadRecord(envelope);
      if (!payload) {
        return null;
      }
      const primaryKeysList = payload.primaryKeysList;
      if (primaryKeysList !== undefined && !isArrayOfRecords(primaryKeysList)) {
        return null;
      }
      return {
        type: envelope.type,
        payload: { primaryKeysList },
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
      return {
        type: envelope.type,
        payload: {
          sort: payload.sort,
          filters: payload.filters as unknown[] | undefined,
          limitToPage: readLimitToPage(payload.limitToPage),
          columnOrder: readColumnOrder(payload.columnOrder),
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
      return previewToken
        ? { type: envelope.type, payload: { previewToken } }
        : null;
    }

    case "readClipboard":
      return { type: envelope.type };

    case "writeClipboard": {
      const payload = parseEnvelopeTextPayload(envelope);
      return payload ? { type: envelope.type, payload } : null;
    }

    default:
      return null;
  }
}
