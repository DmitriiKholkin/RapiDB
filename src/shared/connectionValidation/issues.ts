/**
 * Validation issue types and message-formatting helpers.
 * Pure functions only — no I/O, no mutation of arguments.
 */

export type ConnectionValidationIssueCode = "required" | "anyOf" | "invalid";

export interface ConnectionValidationIssue {
  code: ConnectionValidationIssueCode;
  fields: string[];
  message: string;
}

function createIssue(
  code: ConnectionValidationIssueCode,
  fields: string[],
  message: string,
): ConnectionValidationIssue {
  return { code, fields, message };
}

/** Builds a "required" issue for a single field. */
export function buildRequiredIssue(field: string): ConnectionValidationIssue {
  return createIssue("required", [field], `Field "${field}" is required.`);
}

/**
 * Builds a "required" issue that groups multiple sub-fields under a
 * shared parent (e.g. SSH sub-fields grouped under `"ssh"`). This
 * preserves the legacy convention where SSH-related issues share a
 * single `["ssh"]` field group.
 */
export function buildRequiredGroupIssue(
  fields: string[],
  message: string,
): ConnectionValidationIssue {
  return createIssue("required", fields, message);
}

/** Builds an "anyOf" issue for a group of alternative fields. */
export function buildAnyOfIssue(
  group: readonly string[],
): ConnectionValidationIssue {
  return createIssue(
    "anyOf",
    [...group],
    `At least one of "${group.join('", "')}" is required.`,
  );
}

/**
 * Builds a generic "invalid" issue for an arbitrary field list.
 * Use for value-format problems (regex mismatches, ranges, ...).
 */
export function buildInvalidIssue(
  fields: string[],
  message: string,
): ConnectionValidationIssue {
  return createIssue("invalid", fields, message);
}

/**
 * Concatenates the human-readable summary for a validation result.
 * Exported as a pure helper so it is trivially unit-testable.
 */
export function formatIssuesAsMessage(
  missingRequired: readonly string[],
  missingAnyOf: readonly string[][],
  invalidIssues: readonly ConnectionValidationIssue[],
): string {
  const parts: string[] = [];

  if (missingRequired.length > 0) {
    parts.push(`Missing required fields: ${missingRequired.join(", ")}.`);
  }

  if (missingAnyOf.length > 0) {
    for (const group of missingAnyOf) {
      parts.push(`Provide at least one of: ${group.join(" | ")}.`);
    }
  }

  for (const issue of invalidIssues) {
    parts.push(issue.message);
  }

  return parts.join(" ");
}
