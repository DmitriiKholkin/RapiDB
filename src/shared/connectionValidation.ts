import { ConnectionString } from "mongodb-connection-string-url";
import {
  type ConnectionConfig,
  getConnectionTlsSupport,
  getElasticsearchTlsEndpointError,
  isConnectionTlsEnabled,
} from "./connectionConfig";
import type { ConnectionType } from "./connectionTypes";
import {
  AMBIGUOUS_URI_CREDENTIALS_MESSAGE,
  hasAmbiguousUriCredentials,
} from "./connectionUriSecurity";
import {
  getMongoConnectionUri,
  normalizeMongoConnectionConfig,
  resolveMongoConnectionMode,
} from "./mongoConnection";

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
  issues: readonly ConnectionValidationIssue[],
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

  const extraConditionalIssues = issues.filter((issue) => {
    if (issue.code === "required") {
      return !(
        issue.fields.length === 1 && missingRequired.includes(issue.fields[0])
      );
    }
    if (issue.code === "anyOf") {
      return !missingAnyOf.some(
        (group) =>
          group.length === issue.fields.length &&
          group.every((field) => issue.fields.includes(field)),
      );
    }
    return false;
  });
  appendMessages(extraConditionalIssues.map((issue) => issue.message));

  const invalidIssues = issues.filter((issue) => issue.code === "invalid");
  if (invalidIssues.length > 0) {
    appendMessages(invalidIssues.map((issue) => issue.message));
  }

  return parts.join(" ");
}

function isValidConnectionPort(value: unknown): value is number {
  return (
    typeof value === "number" &&
    Number.isInteger(value) &&
    value >= 1 &&
    value <= 65535
  );
}

export function isConnectionPortRelevant(
  config: Partial<ConnectionConfig>,
): boolean {
  switch (config.type) {
    case "sqlite":
    case "dynamodb":
      return false;
    case "mongodb":
      return (
        config.mongoScheme !== "mongodb+srv" &&
        !hasValue(config.connectionUri) &&
        !hasValue(config.uri)
      );
    case "redis":
      return !hasValue(config.connectionUri);
    case "elasticsearch":
      return (
        !hasValue(config.connectionUri) &&
        !hasValue(config.endpoint) &&
        !hasValue(config.cloudId)
      );
    default:
      return true;
  }
}

export function buildConnectionPortValidationIssues(
  config: Partial<ConnectionConfig>,
): ConnectionValidationIssue[] {
  const issues: ConnectionValidationIssue[] = [];
  if (
    isConnectionPortRelevant(config) &&
    config.port !== undefined &&
    !isValidConnectionPort(config.port)
  ) {
    issues.push(
      createValidationIssue(
        "invalid",
        ["port"],
        "Database port must be an integer between 1 and 65535.",
      ),
    );
  }
  if (
    config.type !== "sqlite" &&
    config.ssh?.port !== undefined &&
    !isValidConnectionPort(config.ssh.port)
  ) {
    issues.push(
      createValidationIssue(
        "invalid",
        ["ssh"],
        "SSH port must be an integer between 1 and 65535.",
      ),
    );
  }
  return issues;
}

function isSshFingerprintSha256(value: string | undefined): boolean {
  if (!value) {
    return false;
  }

  return /^SHA256:[A-Za-z0-9+/]+={0,2}$/.test(value.trim());
}

function resolveSshHostVerificationMode(
  config: Partial<ConnectionConfig>,
): NonNullable<ConnectionConfig["ssh"]>["hostVerificationMode"] {
  return config.ssh?.hostVerificationMode === "trustOnFirstUse"
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

  const authority = normalized.slice(schemeIndex + 3).split(/[/?#]/, 1)[0];
  const hosts = authority.includes("@")
    ? authority.slice(authority.lastIndexOf("@") + 1)
    : authority;

  return hosts.includes(",");
}

function isValidMongoHostEntry(value: string): boolean {
  const hostEntry = value.trim();
  if (!hostEntry || /[\s/?#@\\]/.test(hostEntry)) return false;

  if (hostEntry.startsWith("[")) {
    const match = /^\[([^\]]+)\](?::(\d+))?$/.exec(hostEntry);
    if (!match) return false;
    const port = match[2];
    if (
      port &&
      (!Number.isSafeInteger(Number(port)) ||
        Number(port) < 1 ||
        Number(port) > 65535)
    ) {
      return false;
    }
    try {
      new URL(`mongodb://${hostEntry}/`);
      return true;
    } catch {
      return false;
    }
  }

  const colonCount = hostEntry.match(/:/g)?.length ?? 0;
  if (colonCount > 1) {
    try {
      new URL(`mongodb://[${hostEntry}]/`);
      return true;
    } catch {
      return false;
    }
  }

  let hostname = hostEntry;
  if (colonCount === 1) {
    const portSeparator = hostEntry.lastIndexOf(":");
    const port = hostEntry.slice(portSeparator + 1);
    if (!/^\d+$/.test(port)) return false;
    const portNumber = Number(port);
    if (
      !Number.isSafeInteger(portNumber) ||
      portNumber < 1 ||
      portNumber > 65535
    ) {
      return false;
    }
    hostname = hostEntry.slice(0, portSeparator);
  }
  if (!hostname) return false;

  try {
    return new URL(`mongodb://${hostname}/`).hostname.length > 0;
  } catch {
    return false;
  }
}

function isValidMongoUriHostEntry(value: string): boolean {
  const colonCount = value.match(/:/g)?.length ?? 0;
  return (
    (colonCount <= 1 || value.startsWith("[")) && isValidMongoHostEntry(value)
  );
}

function isValidMongoSrvHostname(value: string): boolean {
  const host = value.trim();
  if (!host || /[\s/?#@\\:[\]]/.test(host)) return false;

  let hostname: string;
  try {
    hostname = new URL(`mongodb://${host}/`).hostname.toLowerCase();
  } catch {
    return false;
  }
  if (hostname.startsWith("[") || hostname.length > 253) return false;

  const normalized = hostname.endsWith(".") ? hostname.slice(0, -1) : hostname;
  if (!normalized) return false;
  const labels = normalized.split(".");
  if (labels.every((label) => /^\d+$/.test(label))) return false;
  return labels.every((label) =>
    /^[a-z\d](?:[a-z\d-]{0,61}[a-z\d])?$/.test(label),
  );
}

function collectConditionalIssues(
  config: Partial<ConnectionConfig>,
  rules: readonly ConditionalIssueRule[],
): ConnectionValidationIssue[] {
  return rules.filter((rule) => rule.when(config)).map((rule) => rule.issue);
}

export function buildSshValidationIssues(
  config: Partial<ConnectionConfig>,
  options: {
    hasStoredPassword?: boolean;
    hasStoredPrivateKey?: boolean;
  } = {},
): ConnectionValidationIssue[] {
  if (!config.ssh) {
    return [];
  }

  const ssh = config.ssh;
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
      ["ssh"],
      "SSH is not supported for sqlite connections.",
    );
    return issues;
  }

  issues.push(
    ...collectConditionalIssues(config, [
      {
        when: () => !hasValue(ssh.host),
        issue: createValidationIssue(
          "required",
          ["ssh"],
          'Field "ssh.host" is required when SSH is enabled.',
        ),
      },
      {
        when: () => !hasValue(ssh.username),
        issue: createValidationIssue(
          "required",
          ["ssh"],
          'Field "ssh.username" is required when SSH is enabled.',
        ),
      },
      {
        when: () => !hasValue(ssh.authMethod),
        issue: createValidationIssue(
          "required",
          ["ssh"],
          'Field "ssh.authMethod" is required when SSH is enabled.',
        ),
      },
    ]),
  );

  const hostVerificationMode = resolveSshHostVerificationMode(config);
  if (!hasValue(ssh.hostFingerprintSha256)) {
    if (hostVerificationMode === "manual") {
      pushIssue(
        "required",
        ["ssh"],
        'Field "ssh.hostFingerprintSha256" is required when SSH host verification is manual.',
      );
    }
  } else if (!isSshFingerprintSha256(ssh.hostFingerprintSha256)) {
    pushIssue(
      "invalid",
      ["ssh"],
      'Field "ssh.hostFingerprintSha256" must use the OpenSSH SHA256 fingerprint format.',
    );
  }

  issues.push(
    ...collectConditionalIssues(config, [
      {
        when: () =>
          ssh.authMethod === "password" &&
          !hasValue(ssh.password) &&
          !options.hasStoredPassword,
        issue: createValidationIssue(
          "required",
          ["ssh"],
          'Field "ssh.password" is required for password SSH auth.',
        ),
      },
      {
        when: () =>
          ssh.authMethod === "privateKey" &&
          !hasValue(ssh.privateKey) &&
          !options.hasStoredPrivateKey,
        issue: createValidationIssue(
          "required",
          ["ssh"],
          'Field "ssh.privateKey" is required for private key SSH auth.',
        ),
      },
    ]),
  );

  if (config.type === "mongodb") {
    const mongoUri =
      typeof config.connectionUri === "string" && config.connectionUri.trim()
        ? config.connectionUri
        : config.uri;
    const mongoConnectionMode = resolveMongoConnectionMode(config);
    const isSrvFieldsConnection =
      mongoConnectionMode === "fields" && config.mongoScheme === "mongodb+srv";

    if (hasMongoSrvUri(mongoUri) || isSrvFieldsConnection) {
      pushIssue(
        "invalid",
        isSrvFieldsConnection
          ? ["mongoScheme", "ssh"]
          : ["connectionUri", "uri"],
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

    if (mongoConnectionMode === "fields" && config.host?.includes(",")) {
      pushIssue(
        "invalid",
        ["host", "ssh"],
        "MongoDB over SSH supports only single-host connections in v1.",
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

function buildTlsValidationIssues(
  config: Partial<ConnectionConfig>,
): ConnectionValidationIssue[] {
  const tlsConfig = config.tls;
  if (!tlsConfig || !config.type) {
    return [];
  }

  const support = getConnectionTlsSupport(config.type);
  if (!support) {
    return isConnectionTlsEnabled(tlsConfig.mode)
      ? [
          createValidationIssue(
            "invalid",
            ["tls"],
            `TLS configuration is not supported for ${config.type} connections.`,
          ),
        ]
      : [];
  }

  const issues: ConnectionValidationIssue[] = [];
  const pushInvalidIssue = (fields: string[], message: string) => {
    issues.push(createValidationIssue("invalid", fields, message));
  };

  const endpointError = getElasticsearchTlsEndpointError(config);
  if (endpointError) {
    pushInvalidIssue(
      [
        "tls.mode",
        config.connectionUri !== undefined ? "connectionUri" : "endpoint",
      ],
      endpointError,
    );
  }

  if (!support.modes.includes(tlsConfig.mode)) {
    pushInvalidIssue(
      ["tls.mode"],
      `TLS mode "${tlsConfig.mode}" is not supported for ${config.type} connections.`,
    );
  }

  if (tlsConfig.mode === "mutualTls") {
    if (!support.supportsClientCertificate) {
      pushInvalidIssue(
        ["tls.certFilePath"],
        `Client certificates are not supported for ${config.type} connections.`,
      );
    }
    if (!support.supportsClientKey) {
      pushInvalidIssue(
        ["tls.keyFilePath"],
        `Client keys are not supported for ${config.type} connections.`,
      );
    }
    if (!hasValue(tlsConfig.certFilePath)) {
      issues.push(
        createValidationIssue(
          "required",
          ["tls.certFilePath"],
          'Field "tls.certFilePath" is required for mutual TLS.',
        ),
      );
    }
    if (!hasValue(tlsConfig.keyFilePath)) {
      issues.push(
        createValidationIssue(
          "required",
          ["tls.keyFilePath"],
          'Field "tls.keyFilePath" is required for mutual TLS.',
        ),
      );
    }
  }

  if (hasValue(tlsConfig.caFilePath) && !support.supportsCaFile) {
    pushInvalidIssue(
      ["tls.caFilePath"],
      `CA certificate files are not supported for ${config.type} connections.`,
    );
  }

  if (
    hasValue(tlsConfig.serverNameOverride) &&
    !support.supportsServerNameOverride
  ) {
    pushInvalidIssue(
      ["tls.serverNameOverride"],
      `TLS server name overrides are not supported for ${config.type} connections.`,
    );
  }

  if (
    hasValue(tlsConfig.keyPassphrase) &&
    !support.supportsClientKeyPassphrase
  ) {
    pushInvalidIssue(
      ["tls.keyPassphrase"],
      `TLS client key passphrases are not supported for ${config.type} connections.`,
    );
  }

  return issues;
}

export function validateConnectionConfig(
  input: Partial<ConnectionConfig>,
  options: {
    hasStoredAwsKeyPair?: boolean;
    hasStoredSshPassword?: boolean;
    hasStoredSshPrivateKey?: boolean;
  } = {},
): ConnectionValidationResult {
  const config = normalizeMongoConnectionConfig(input);
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
    ...buildConnectionPortValidationIssues(config),
    ...buildMongoConnectionValidationIssues(config),
    ...buildRedisConnectionValidationIssues(config),
    ...buildDynamoValidationIssues(config, {
      hasStoredAwsKeyPair: options.hasStoredAwsKeyPair,
    }),
    ...buildSqliteValidationIssues(config),
    ...buildSshValidationIssues(config, {
      hasStoredPassword: options.hasStoredSshPassword,
      hasStoredPrivateKey: options.hasStoredSshPrivateKey,
    }),
    ...buildTlsValidationIssues(config),
    ...buildUriCredentialValidationIssues(config),
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

export function buildUriCredentialValidationIssues(
  config: Partial<ConnectionConfig>,
): ConnectionValidationIssue[] {
  return (["connectionUri", "uri", "endpoint", "awsEndpoint"] as const)
    .filter((field) => hasAmbiguousUriCredentials(config[field]))
    .map((field) =>
      createValidationIssue(
        "invalid",
        [field],
        AMBIGUOUS_URI_CREDENTIALS_MESSAGE,
      ),
    );
}

export function buildMongoConnectionValidationIssues(
  config: Partial<ConnectionConfig>,
): ConnectionValidationIssue[] {
  if (config.type !== "mongodb") return [];
  const issues: ConnectionValidationIssue[] = [];
  const invalid = (field: string, message: string) =>
    issues.push(createValidationIssue("invalid", [field], message));
  const mongoConnectionMode = resolveMongoConnectionMode(config);
  if (mongoConnectionMode === "uri") {
    const uri = (getMongoConnectionUri(config) ?? "").trim();
    const isSrvUri = /^mongodb\+srv:\/\//i.test(uri);
    let parsedUri: ConnectionString | undefined;
    try {
      parsedUri = new ConnectionString(uri);
    } catch {
      invalid(
        "connectionUri",
        isSrvUri
          ? "Enter a valid MongoDB SRV URI with one hostname and no port."
          : "Enter a valid MongoDB URI starting with mongodb:// or mongodb+srv://.",
      );
    }
    if (parsedUri?.hosts.some((host) => !isValidMongoUriHostEntry(host))) {
      invalid(
        "connectionUri",
        "Enter valid MongoDB hosts; each port must be between 1 and 65535, and IPv6 addresses must be enclosed in brackets.",
      );
    }
    if (parsedUri?.isSRV) {
      if (parsedUri.hosts.some((host) => !isValidMongoSrvHostname(host))) {
        invalid(
          "connectionUri",
          "MongoDB SRV requires a DNS hostname and does not accept IP addresses.",
        );
      }
      const directConnectionEnabled = parsedUri.searchParams
        .getAll("directConnection")
        .some((value) => value.toLowerCase() === "true");
      if (directConnectionEnabled || config.directConnection === true) {
        invalid(
          "connectionUri",
          "MongoDB SRV URIs cannot enable directConnection.",
        );
      }
    }
  } else if (mongoConnectionMode === "fields") {
    const mongoHosts = config.host?.split(",").map((host) => host.trim());
    if (
      !config.host?.trim() ||
      mongoHosts?.some((host) => !isValidMongoHostEntry(host))
    )
      invalid(
        "host",
        "Enter valid MongoDB hostnames or IP addresses, optionally with a numeric port from 1 to 65535. Use bracketed IPv6 when specifying a port.",
      );
    if (config.mongoAuthMethod === "password" && !config.username?.trim())
      invalid(
        "username",
        "Username is required for MongoDB password authentication.",
      );
    if (
      config.mongoScheme === "mongodb+srv" &&
      /[:,[\]]/.test(config.host ?? "")
    )
      invalid("host", "MongoDB SRV requires one DNS hostname without a port.");
    if (
      config.mongoScheme === "mongodb+srv" &&
      mongoHosts?.some((host) => !isValidMongoSrvHostname(host))
    )
      invalid(
        "host",
        "MongoDB SRV requires a DNS hostname and does not accept IP addresses.",
      );
    if (
      config.mongoScheme === "mongodb+srv" &&
      config.directConnection === true
    )
      invalid(
        "directConnection",
        "Direct connection is not supported with MongoDB SRV.",
      );
    if (config.directConnection === true && config.host?.includes(","))
      invalid(
        "directConnection",
        "Direct connection requires a single MongoDB host.",
      );
  }
  return issues;
}

export function buildRedisConnectionValidationIssues(
  config: Partial<ConnectionConfig>,
): ConnectionValidationIssue[] {
  if (config.type !== "redis") return [];
  const uri = config.connectionUri?.trim();
  if (!uri) {
    if (!config.host?.trim()) {
      return [
        createValidationIssue(
          "anyOf",
          ["connectionUri", "host"],
          "Enter a Redis URI or host.",
        ),
      ];
    }
    const database = config.database?.trim();
    if (!database) return [];
    const databaseMatch = /^(?:db)?(\d+)$/.exec(database);
    if (!databaseMatch || !Number.isSafeInteger(Number(databaseMatch[1]))) {
      return [
        createValidationIssue(
          "invalid",
          ["database"],
          "Redis database must be a non-negative safe integer or dbN.",
        ),
      ];
    }
    return [];
  }
  try {
    const parsed = new URL(uri);
    if (
      (parsed.protocol !== "redis:" && parsed.protocol !== "rediss:") ||
      !parsed.hostname
    ) {
      throw new Error("Unsupported Redis URI");
    }
    const database = parsed.pathname.startsWith("/")
      ? parsed.pathname.slice(1)
      : parsed.pathname;
    const databaseMatch = /^(?:db)?(\d+)$/.exec(database);
    if (
      database &&
      (!databaseMatch || !Number.isSafeInteger(Number(databaseMatch[1])))
    ) {
      return [
        createValidationIssue(
          "invalid",
          ["connectionUri"],
          "Redis URI database must be a non-negative safe integer or dbN.",
        ),
      ];
    }
    return [];
  } catch {
    // Return the same actionable URI error as for unsupported schemes.
  }
  return [
    createValidationIssue(
      "invalid",
      ["connectionUri"],
      "Enter a valid Redis URI starting with redis:// or rediss:// and including a host.",
    ),
  ];
}

export function buildDynamoValidationIssues(
  config: Partial<ConnectionConfig>,
  options: { hasStoredAwsKeyPair?: boolean } = {},
): ConnectionValidationIssue[] {
  if (config.type !== "dynamodb") return [];

  const hasAccessKeyId = hasValue(config.awsAccessKeyId);
  const hasSecretAccessKey = hasValue(config.awsSecretAccessKey);
  const hasSessionToken = hasValue(config.awsSessionToken);
  const issues: ConnectionValidationIssue[] = [];
  if (hasAccessKeyId !== hasSecretAccessKey) {
    issues.push(
      createValidationIssue(
        "invalid",
        ["awsAccessKeyId", "awsSecretAccessKey"],
        "Enter both AWS Access Key ID and Secret Access Key, or leave both blank to use a profile or the default credential chain.",
      ),
    );
  }
  if (hasSessionToken && !hasAccessKeyId && !options.hasStoredAwsKeyPair) {
    issues.push(
      createValidationIssue(
        "invalid",
        ["awsSessionToken", "awsAccessKeyId", "awsSecretAccessKey"],
        "An AWS Session Token requires both Access Key ID and Secret Access Key.",
      ),
    );
  }
  return issues;
}
