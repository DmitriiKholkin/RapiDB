/**
 * Connection validation — public types, policies, and facade.
 *
 * The implementation is split across focused modules:
 *   - issues.ts        : Issue factories and the typed `push` accumulator
 *   - primitives.ts    : Pure helpers (hasValue, isSatisfied, ...)
 *   - ssh-validation.ts: SSH-specific rules
 *   - tls-validation.ts: TLS-specific rules
 *   - sqlite-validation.ts: SQLite WAL mode rules
 *   - mongo-uri.ts     : MongoDB URI helpers (SRV / multi-host detection)
 *
 * This file is the only public entry point. It preserves the original
 * `validateConnectionConfig(config)` behavior 1:1.
 */
import type { ConnectionConfig } from "../connectionConfig";
import {
  getConnectionTlsSupport,
  isConnectionTlsEnabled,
} from "../connectionConfig";
import type { ConnectionType } from "../connectionTypes";

import type { ConnectionValidationIssue } from "./issues";
import {
  buildAnyOfIssue,
  buildRequiredIssue,
  formatIssuesAsMessage,
} from "./issues";
import { hasValue, isSatisfied } from "./primitives";
import { buildSqliteValidationIssues } from "./sqlite-validation";
import { buildSshValidationIssues } from "./ssh-validation";
import { buildTlsValidationIssues } from "./tls-validation";

export type { ConnectionValidationIssue };

export interface ConnectionValidationResult {
  valid: boolean;
  message?: string;
  missingRequired: string[];
  missingAnyOf: string[][];
  issues: ConnectionValidationIssue[];
}

type ValidationPolicy = {
  readonly required: readonly (keyof ConnectionConfig)[];
  readonly anyOf?: readonly (readonly (keyof ConnectionConfig)[])[];
};

/** Static, per-driver validation policy. Data-only; safe to export as-is. */
export const CONNECTION_VALIDATION_POLICY: Readonly<
  Record<ConnectionType, ValidationPolicy>
> = {
  pg: { required: ["name", "type", "host", "database", "username"] },
  mysql: { required: ["name", "type", "host", "database", "username"] },
  sqlite: { required: ["name", "type", "filePath"] },
  mssql: { required: ["name", "type", "host", "database"] },
  oracle: { required: ["name", "type"], anyOf: [["serviceName", "database"]] },
  mongodb: {
    required: ["name", "type"],
    anyOf: [["connectionUri", "uri"], ["host"]],
  },
  redis: { required: ["name", "type"], anyOf: [["connectionUri"], ["host"]] },
  elasticsearch: {
    required: ["name", "type"],
    anyOf: [["connectionUri", "endpoint", "cloudId"]],
  },
  dynamodb: { required: ["name", "type", "awsRegion"] },
};

/** Sentinel returned when `type` is missing or unknown. */
const TYPE_REQUIRED_RESULT: ConnectionValidationResult = {
  valid: false,
  message: "Missing required fields: type.",
  missingRequired: ["type"],
  missingAnyOf: [],
  issues: [
    {
      code: "required",
      fields: ["type"],
      message: 'Field "type" is required.',
    },
  ],
};

/** Extracts the per-driver policy or returns the "type required" sentinel. */
function resolvePolicy(
  config: Partial<ConnectionConfig>,
): { policy: ValidationPolicy; type: ConnectionType } | null {
  const type = config.type;
  if (!type || !(type in CONNECTION_VALIDATION_POLICY)) {
    return null;
  }
  return { policy: CONNECTION_VALIDATION_POLICY[type as ConnectionType], type };
}

/** Collects the list of required fields that are empty/missing. */
function collectMissingRequired(
  policy: ValidationPolicy,
  config: Partial<ConnectionConfig>,
): string[] {
  return policy.required
    .filter((field) => !hasValue(config[field]))
    .map((field) => String(field));
}

/**
 * Returns the anyOf groups that are completely missing, or `[]` if at
 * least one group is fully satisfied (or no groups are defined).
 */
function collectMissingAnyOf(
  policy: ValidationPolicy,
  config: Partial<ConnectionConfig>,
): string[][] {
  const anyOfGroups = policy.anyOf ?? [];
  if (anyOfGroups.length === 0) {
    return [];
  }
  if (anyOfGroups.some((group) => isSatisfied(config, group))) {
    return [];
  }
  return anyOfGroups.map((group) => group.map((field) => String(field)));
}

/**
 * Validates a partial connection config and returns a structured result.
 * Pure function: same input -> same output, no side effects.
 */
export function validateConnectionConfig(
  config: Partial<ConnectionConfig>,
): ConnectionValidationResult {
  const resolved = resolvePolicy(config);
  if (!resolved) {
    return TYPE_REQUIRED_RESULT;
  }

  const { policy } = resolved;
  const missingRequired = collectMissingRequired(policy, config);
  const missingAnyOf = collectMissingAnyOf(policy, config);

  // Compose all issues in one flat list. The order is stable so message
  // generation is deterministic for tests.
  const issues: ConnectionValidationIssue[] = [
    ...missingRequired.map(buildRequiredIssue),
    ...missingAnyOf.map(buildAnyOfIssue),
    ...buildSqliteValidationIssues(config),
    ...buildSshValidationIssues(config),
    ...buildTlsValidationIssues(config),
  ];

  if (issues.length === 0) {
    return { valid: true, missingRequired: [], missingAnyOf: [], issues: [] };
  }

  return {
    valid: false,
    message: formatIssuesAsMessage(missingRequired, missingAnyOf, issues),
    missingRequired,
    missingAnyOf,
    issues,
  };
}

// Re-export primitives used by other modules that may want to extend
// validation (kept here to keep the public surface stable).
export { getConnectionTlsSupport, isConnectionTlsEnabled };
