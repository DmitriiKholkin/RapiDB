import type { ConnectionConfig } from "./connectionConfig";
import type { ConnectionType } from "./connectionTypes";

export interface ConnectionValidationIssue {
  code: "required" | "anyOf";
  fields: string[];
  message: string;
}

export interface ConnectionValidationResult {
  valid: boolean;
  message?: string;
  missingRequired: string[];
  missingAnyOf: string[][];
  issues: ConnectionValidationIssue[];
}

type ValidationPolicy = {
  required: readonly (keyof ConnectionConfig)[];
  anyOf?: readonly (readonly (keyof ConnectionConfig)[])[];
};

export const CONNECTION_VALIDATION_POLICY: Readonly<
  Record<ConnectionType, ValidationPolicy>
> = {
  pg: {
    required: ["name", "type", "host", "database", "username"],
  },
  mysql: {
    required: ["name", "type", "host", "database", "username"],
  },
  sqlite: {
    required: ["name", "type", "filePath"],
  },
  mssql: {
    required: ["name", "type", "host", "database"],
  },
  oracle: {
    required: ["name", "type"],
    anyOf: [["serviceName", "database"]],
  },
  mongodb: {
    required: ["name", "type"],
    anyOf: [["connectionUri", "uri"], ["host"]],
  },
  redis: {
    required: ["name", "type"],
    anyOf: [["connectionUri"], ["host"]],
  },
  elasticsearch: {
    required: ["name", "type"],
    anyOf: [["connectionUri", "endpoint", "cloudId"]],
  },
  dynamodb: {
    required: ["name", "type", "awsRegion"],
  },
};

function hasValue(value: unknown): boolean {
  if (typeof value === "string") {
    return value.trim().length > 0;
  }

  return value !== undefined && value !== null;
}

function isSatisfied(
  config: Partial<ConnectionConfig>,
  fields: readonly (keyof ConnectionConfig)[],
): boolean {
  for (const field of fields) {
    if (hasValue(config[field])) {
      return true;
    }
  }

  return false;
}

function buildValidationMessage(
  missingRequired: readonly string[],
  missingAnyOf: readonly string[][],
): string {
  const parts: string[] = [];

  if (missingRequired.length > 0) {
    parts.push(`Missing required fields: ${missingRequired.join(", ")}.`);
  }

  if (missingAnyOf.length > 0) {
    parts.push(
      ...missingAnyOf.map(
        (group) => `Provide at least one of: ${group.join(" | ")}.`,
      ),
    );
  }

  return parts.join(" ");
}

export function validateConnectionConfig(
  config: Partial<ConnectionConfig>,
): ConnectionValidationResult {
  const type = config.type;
  if (!type || !(type in CONNECTION_VALIDATION_POLICY)) {
    return {
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
  }

  const policy = CONNECTION_VALIDATION_POLICY[type as ConnectionType];

  const missingRequired = policy.required
    .filter((field) => !hasValue(config[field]))
    .map((field) => String(field));

  const anyOfGroups = policy.anyOf ?? [];
  const missingAnyOf =
    anyOfGroups.length > 0 &&
    !anyOfGroups.some((group) => isSatisfied(config, group))
      ? anyOfGroups.map((group) => group.map((field) => String(field)))
      : [];

  const issues: ConnectionValidationIssue[] = [
    ...missingRequired.map((field) => ({
      code: "required" as const,
      fields: [field],
      message: `Field "${field}" is required.`,
    })),
    ...missingAnyOf.map((group) => ({
      code: "anyOf" as const,
      fields: group,
      message: `At least one of "${group.join('", "')}" is required.`,
    })),
  ];

  if (issues.length === 0) {
    return {
      valid: true,
      missingRequired: [],
      missingAnyOf: [],
      issues: [],
    };
  }

  return {
    valid: false,
    message: buildValidationMessage(missingRequired, missingAnyOf),
    missingRequired,
    missingAnyOf,
    issues,
  };
}
