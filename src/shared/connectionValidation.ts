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

type ConditionalIssueRule = {
  when: (config: Partial<ConnectionConfig>) => boolean;
  issue: ConnectionValidationIssue;
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
  return typeof value === "string"
    ? value.trim().length > 0
    : value !== undefined && value !== null;
}

function isSatisfied(
  config: Partial<ConnectionConfig>,
  fields: readonly (keyof ConnectionConfig)[],
): boolean {
  return fields.some((field) => hasValue(config[field]));
}

function createValidationIssue(
  code: ConnectionValidationIssue["code"],
  fields: string[],
  message: string,
): ConnectionValidationIssue {
  return { code, fields, message };
}

function createRequiredIssue(field: string): ConnectionValidationIssue {
  return createValidationIssue(
    "required",
    [field],
    `Field "${field}" is required.`,
  );
}

function createAnyOfIssue(group: readonly string[]): ConnectionValidationIssue {
  return createValidationIssue(
    "anyOf",
    [...group],
    `At least one of "${group.join('", "')}" is required.`,
  );
}

function buildValidationMessage(
  missingRequired: readonly string[],
  missingAnyOf: readonly string[][],
  invalidIssues: readonly ConnectionValidationIssue[],
): string {
  const parts: string[] = [];

  const appendMessages = (messages: readonly string[]) => {
    parts.push(...messages);
  };

  if (missingRequired.length > 0) {
    appendMessages([`Missing required fields: ${missingRequired.join(", ")}.`]);
  }

  if (missingAnyOf.length > 0) {
    appendMessages(
      missingAnyOf.map(
        (group) => `Provide at least one of: ${group.join(" | ")}.`,
      ),
    );
  }

  if (invalidIssues.length > 0) {
    appendMessages(invalidIssues.map((issue) => issue.message));
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

function resolveSqliteWalMode(
  config: Partial<ConnectionConfig>,
): NonNullable<ConnectionConfig["sqliteWalMode"]> | undefined {
  if (config.type !== "sqlite") {
    return undefined;
  }

  return config.sqliteWalMode === "off" ? "off" : "auto";
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

function collectConditionalIssues(
  config: Partial<ConnectionConfig>,
  rules: readonly ConditionalIssueRule[],
): ConnectionValidationIssue[] {
  return rules.filter((rule) => rule.when(config)).map((rule) => rule.issue);
}

function buildSshValidationIssues(
  config: Partial<ConnectionConfig>,
): ConnectionValidationIssue[] {
  if (config.sshEnabled !== true) {
    return [];
  }

  const issues: ConnectionValidationIssue[] = [];
  const pushIssue = (
    code: ConnectionValidationIssue["code"],
    fields: string[],
    message: string,
  ) => {
    issues.push(createValidationIssue(code, fields, message));
  };

  if (config.type === "sqlite") {
    pushIssue(
      "invalid",
      ["sshEnabled"],
      "SSH is not supported for sqlite connections.",
    );
    return issues;
  }

  issues.push(
    ...collectConditionalIssues(config, [
      {
        when: (candidate) => !isPositiveInteger(candidate.sshPort),
        issue: createValidationIssue(
          "required",
          ["sshPort"],
          'Field "sshPort" is required and must be a positive integer.',
        ),
      },
      {
        when: (candidate) => !hasValue(candidate.sshHost),
        issue: createValidationIssue(
          "required",
          ["sshHost"],
          'Field "sshHost" is required when SSH is enabled.',
        ),
      },
      {
        when: (candidate) => !hasValue(candidate.sshUsername),
        issue: createValidationIssue(
          "required",
          ["sshUsername"],
          'Field "sshUsername" is required when SSH is enabled.',
        ),
      },
      {
        when: (candidate) => !hasValue(candidate.sshAuthMethod),
        issue: createValidationIssue(
          "required",
          ["sshAuthMethod"],
          'Field "sshAuthMethod" is required when SSH is enabled.',
        ),
      },
    ]),
  );

  const hostVerificationMode = resolveSshHostVerificationMode(config);
  if (!hasValue(config.sshHostFingerprintSha256)) {
    if (hostVerificationMode === "manual") {
      pushIssue(
        "required",
        ["sshHostFingerprintSha256"],
        'Field "sshHostFingerprintSha256" is required when SSH host verification is manual.',
      );
    }
  } else if (!isSshFingerprintSha256(config.sshHostFingerprintSha256)) {
    pushIssue(
      "invalid",
      ["sshHostFingerprintSha256"],
      'Field "sshHostFingerprintSha256" must use the OpenSSH SHA256 fingerprint format.',
    );
  }

  issues.push(
    ...collectConditionalIssues(config, [
      {
        when: (candidate) =>
          candidate.sshAuthMethod === "password" &&
          !hasValue(candidate.sshPassword),
        issue: createValidationIssue(
          "required",
          ["sshPassword"],
          'Field "sshPassword" is required for password SSH auth.',
        ),
      },
      {
        when: (candidate) =>
          candidate.sshAuthMethod === "privateKey" &&
          !hasValue(candidate.sshPrivateKey),
        issue: createValidationIssue(
          "required",
          ["sshPrivateKey"],
          'Field "sshPrivateKey" is required for private key SSH auth.',
        ),
      },
    ]),
  );

  if (config.type === "mongodb") {
    const mongoUri =
      typeof config.connectionUri === "string" && config.connectionUri.trim()
        ? config.connectionUri
        : config.uri;

    if (hasMongoSrvUri(mongoUri)) {
      pushIssue(
        "invalid",
        ["connectionUri", "uri"],
        "MongoDB over SSH does not support mongodb+srv URIs in v1.",
      );
    }

    if (hasMongoMultiHostUri(mongoUri)) {
      pushIssue(
        "invalid",
        ["connectionUri", "uri"],
        "MongoDB over SSH supports only single-host URIs in v1.",
      );
    }

    if (config.directConnection === false) {
      pushIssue(
        "invalid",
        ["directConnection"],
        "MongoDB over SSH requires a direct connection in v1.",
      );
    }
  }

  return issues;
}

function buildSqliteValidationIssues(
  config: Partial<ConnectionConfig>,
): ConnectionValidationIssue[] {
  if (config.sqliteWalMode === undefined) {
    return [];
  }

  if (config.type !== "sqlite") {
    return [
      {
        code: "invalid",
        fields: ["sqliteWalMode"],
        message:
          'Field "sqliteWalMode" is supported only for sqlite connections.',
      },
    ];
  }

  const normalizedWalMode = resolveSqliteWalMode(config);
  if (normalizedWalMode === undefined) {
    return [];
  }

  return config.sqliteWalMode === normalizedWalMode
    ? []
    : [
        {
          code: "invalid",
          fields: ["sqliteWalMode"],
          message: 'Field "sqliteWalMode" must be either "auto" or "off".',
        },
      ];
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
    ...missingRequired.map(createRequiredIssue),
    ...missingAnyOf.map(createAnyOfIssue),
    ...buildSqliteValidationIssues(config),
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
