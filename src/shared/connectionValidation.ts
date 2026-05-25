import type { ConnectionConfig } from "./connectionConfig";
import type { ConnectionType } from "./connectionTypes";

export interface ConnectionValidationIssue {
  code: "required" | "anyOf" | "invalid";
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
  invalidIssues: readonly ConnectionValidationIssue[],
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

  if (invalidIssues.length > 0) {
    parts.push(...invalidIssues.map((issue) => issue.message));
  }

  return parts.join(" ");
}

function isPositiveInteger(value: unknown): value is number {
  return typeof value === "number" && Number.isInteger(value) && value > 0;
}

function isSshFingerprintSha256(value: string | undefined): boolean {
  if (!value) {
    return false;
  }

  return /^SHA256:[A-Za-z0-9+/]+={0,2}$/.test(value.trim());
}

function resolveSshHostVerificationMode(
  config: Partial<ConnectionConfig>,
): NonNullable<ConnectionConfig["sshHostVerificationMode"]> {
  return config.sshHostVerificationMode === "trustOnFirstUse"
    ? "trustOnFirstUse"
    : "manual";
}

function hasMongoSrvUri(value: string | undefined): boolean {
  return typeof value === "string" && value.trim().startsWith("mongodb+srv://");
}

function hasMongoMultiHostUri(value: string | undefined): boolean {
  if (typeof value !== "string") {
    return false;
  }

  const normalized = value.trim();
  const schemeIndex = normalized.indexOf("://");
  if (schemeIndex < 0) {
    return false;
  }

  const authorityStart = schemeIndex + 3;
  const pathStart = normalized.indexOf("/", authorityStart);
  const authority =
    pathStart >= 0
      ? normalized.slice(authorityStart, pathStart)
      : normalized.slice(authorityStart);
  const hosts = authority.includes("@")
    ? authority.slice(authority.lastIndexOf("@") + 1)
    : authority;

  return hosts.includes(",");
}

function buildSshValidationIssues(
  config: Partial<ConnectionConfig>,
): ConnectionValidationIssue[] {
  if (config.sshEnabled !== true) {
    return [];
  }

  const issues: ConnectionValidationIssue[] = [];

  if (config.type === "sqlite") {
    issues.push({
      code: "invalid",
      fields: ["sshEnabled"],
      message: "SSH is not supported for sqlite connections.",
    });
    return issues;
  }

  if (!isPositiveInteger(config.sshPort)) {
    issues.push({
      code: "required",
      fields: ["sshPort"],
      message: 'Field "sshPort" is required and must be a positive integer.',
    });
  }

  if (!hasValue(config.sshHost)) {
    issues.push({
      code: "required",
      fields: ["sshHost"],
      message: 'Field "sshHost" is required when SSH is enabled.',
    });
  }

  if (!hasValue(config.sshUsername)) {
    issues.push({
      code: "required",
      fields: ["sshUsername"],
      message: 'Field "sshUsername" is required when SSH is enabled.',
    });
  }

  if (!hasValue(config.sshAuthMethod)) {
    issues.push({
      code: "required",
      fields: ["sshAuthMethod"],
      message: 'Field "sshAuthMethod" is required when SSH is enabled.',
    });
  }

  const hostVerificationMode = resolveSshHostVerificationMode(config);
  if (!hasValue(config.sshHostFingerprintSha256)) {
    if (hostVerificationMode === "manual") {
      issues.push({
        code: "required",
        fields: ["sshHostFingerprintSha256"],
        message:
          'Field "sshHostFingerprintSha256" is required when SSH host verification is manual.',
      });
    }
  } else if (!isSshFingerprintSha256(config.sshHostFingerprintSha256)) {
    issues.push({
      code: "invalid",
      fields: ["sshHostFingerprintSha256"],
      message:
        'Field "sshHostFingerprintSha256" must use the OpenSSH SHA256 fingerprint format.',
    });
  }

  if (config.sshAuthMethod === "password") {
    if (!hasValue(config.sshPassword)) {
      issues.push({
        code: "required",
        fields: ["sshPassword"],
        message: 'Field "sshPassword" is required for password SSH auth.',
      });
    }
  } else if (config.sshAuthMethod === "privateKey") {
    if (!hasValue(config.sshPrivateKey)) {
      issues.push({
        code: "required",
        fields: ["sshPrivateKey"],
        message: 'Field "sshPrivateKey" is required for private key SSH auth.',
      });
    }
  }

  if (config.type === "mongodb") {
    const mongoUri =
      typeof config.connectionUri === "string" && config.connectionUri.trim()
        ? config.connectionUri
        : config.uri;

    if (hasMongoSrvUri(mongoUri)) {
      issues.push({
        code: "invalid",
        fields: ["connectionUri", "uri"],
        message: "MongoDB over SSH does not support mongodb+srv URIs in v1.",
      });
    }

    if (hasMongoMultiHostUri(mongoUri)) {
      issues.push({
        code: "invalid",
        fields: ["connectionUri", "uri"],
        message: "MongoDB over SSH supports only single-host URIs in v1.",
      });
    }

    if (config.directConnection === false) {
      issues.push({
        code: "invalid",
        fields: ["directConnection"],
        message: "MongoDB over SSH requires a direct connection in v1.",
      });
    }
  }

  return issues;
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
    ...buildSshValidationIssues(config),
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
    message: buildValidationMessage(missingRequired, missingAnyOf, issues),
    missingRequired,
    missingAnyOf,
    issues,
  };
}
