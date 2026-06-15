/**
 * Core types and parser utilities shared across all panel parsers.
 *
 * All public functions are pure: same input -> same output, no I/O,
 * no mutation of arguments. Parser failure is communicated by a
 * `null` return value.
 */
import type { ConnectionType } from "../connectionTypes";
import { CONNECTION_TYPES } from "../connectionTypes";

export interface WebviewMessageEnvelope<
  TType extends string = string,
  TPayload = unknown,
> {
  type: TType;
  payload?: TPayload;
}

export type PanelRetentionMode = "retain" | "rehydrate";

export interface PanelRetentionState {
  panelRetentionMode?: PanelRetentionMode;
}

type UnknownRecord = Record<string, unknown>;

/** True for plain objects (no `null`, no arrays). */
export function isRecord(value: unknown): value is UnknownRecord {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

export function readRequiredString(
  record: UnknownRecord,
  key: string,
): string | null {
  const value = record[key];
  return typeof value === "string" ? value : null;
}

export function readOptionalString(
  record: UnknownRecord,
  key: string,
): string | undefined {
  const value = record[key];
  return typeof value === "string" ? value : undefined;
}

export function readOptionalBoolean(
  record: UnknownRecord,
  key: string,
): boolean | undefined {
  const value = record[key];
  return typeof value === "boolean" ? value : undefined;
}

/** Reads a finite number; accepts numeric and non-empty numeric strings. */
export function readOptionalNumber(
  record: UnknownRecord,
  key: string,
): number | undefined {
  const value = record[key];
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string" && value.trim() !== "") {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : undefined;
  }
  return undefined;
}

/** Reads a positive integer; accepts numeric and non-empty numeric strings. */
export function readPositiveInteger(value: unknown): number | undefined {
  if (typeof value === "number") {
    return Number.isInteger(value) && value > 0 ? value : undefined;
  }
  if (typeof value === "string" && value.trim() !== "") {
    const parsed = Number(value);
    return Number.isInteger(parsed) && parsed > 0 ? parsed : undefined;
  }
  return undefined;
}

export function readConnectionType(
  value: unknown,
): ConnectionType | "" | undefined {
  if (value === "") {
    return "";
  }
  return typeof value === "string" &&
    CONNECTION_TYPES.includes(value as ConnectionType)
    ? (value as ConnectionType)
    : undefined;
}

export function parseEnvelope(input: unknown): WebviewMessageEnvelope | null {
  if (!isRecord(input)) {
    return null;
  }
  const type = readRequiredString(input, "type");
  if (!type) {
    return null;
  }
  return { type, payload: input.payload };
}

export function parseRequiredPayloadRecord(
  envelope: WebviewMessageEnvelope,
): UnknownRecord | null {
  return isRecord(envelope.payload) ? envelope.payload : null;
}

export function parseOptionalPayloadRecord(
  envelope: WebviewMessageEnvelope,
): UnknownRecord | null {
  if (envelope.payload === undefined || envelope.payload === null) {
    return {};
  }
  return isRecord(envelope.payload) ? envelope.payload : null;
}

export function parseEnvelopeTextPayload(
  envelope: WebviewMessageEnvelope,
): { text: string } | null {
  const payload = parseRequiredPayloadRecord(envelope);
  if (!payload) {
    return null;
  }
  const text = readRequiredString(payload, "text");
  return text !== null ? { text } : null;
}

export function parseEnvelopeQueryPayload(
  envelope: WebviewMessageEnvelope,
): { queryText: string; sql: string; connectionId?: string } | null {
  const payload = parseRequiredPayloadRecord(envelope);
  if (!payload) {
    return null;
  }
  const queryText =
    readRequiredString(payload, "queryText") ??
    readRequiredString(payload, "sql");
  if (!queryText) {
    return null;
  }
  return {
    queryText,
    sql: queryText,
    connectionId: readOptionalString(payload, "connectionId"),
  };
}
