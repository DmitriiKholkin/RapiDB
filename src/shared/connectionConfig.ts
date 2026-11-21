import type { ConnectionType } from "./connectionTypes";

export type ConnectionSshAuthMethod = "password" | "privateKey";
export type ConnectionSshHostVerificationMode = "manual" | "trustOnFirstUse";

export interface ConnectionSshConfig {
  host?: string;
  port?: number;
  username?: string;
  authMethod?: ConnectionSshAuthMethod;
  hostVerificationMode?: ConnectionSshHostVerificationMode;
  password?: string;
  privateKey?: string;
  passphrase?: string;
  hostFingerprintSha256?: string;
}
export const CONNECTION_TLS_MODES = [
  "disabled",
  "requireTrustServerCertificate",
  "requireVerifyCa",
  "requireVerifyFull",
  "mutualTls",
] as const;

export type ConnectionTlsMode = (typeof CONNECTION_TLS_MODES)[number];

export interface ConnectionTlsConfig {
  mode: ConnectionTlsMode;
  caFilePath?: string;
  certFilePath?: string;
  keyFilePath?: string;
  keyPassphrase?: string;
  serverNameOverride?: string;
}

export interface ConnectionTlsSupport {
  modes: readonly ConnectionTlsMode[];
  supportsCaFile: boolean;
  supportsClientCertificate: boolean;
  supportsClientKey: boolean;
  supportsClientKeyPassphrase: boolean;
  supportsServerNameOverride: boolean;
}

const FULL_TLS_SUPPORT: ConnectionTlsSupport = {
  modes: [
    "disabled",
    "requireTrustServerCertificate",
    "requireVerifyCa",
    "requireVerifyFull",
    "mutualTls",
  ],
  supportsCaFile: true,
  supportsClientCertificate: true,
  supportsClientKey: true,
  supportsClientKeyPassphrase: true,
  supportsServerNameOverride: true,
};

const MSSQL_TLS_SUPPORT: ConnectionTlsSupport = {
  modes: ["disabled", "requireVerifyFull", "requireTrustServerCertificate"],
  supportsCaFile: false,
  supportsClientCertificate: false,
  supportsClientKey: false,
  supportsClientKeyPassphrase: false,
  supportsServerNameOverride: true,
};

export const CONNECTION_TLS_SUPPORT_BY_TYPE: Readonly<
  Partial<Record<ConnectionType, ConnectionTlsSupport>>
> = {
  pg: FULL_TLS_SUPPORT,
  mysql: FULL_TLS_SUPPORT,
  mssql: MSSQL_TLS_SUPPORT,
  mongodb: FULL_TLS_SUPPORT,
  redis: FULL_TLS_SUPPORT,
  elasticsearch: FULL_TLS_SUPPORT,
};

export type AwsCredentialMode = "static" | "profile" | "default";

export interface ConnectionConfig {
  id: string;
  name: string;
  type: ConnectionType;
  readOnly?: boolean;
  host?: string;
  port?: number;
  database?: string;
  username?: string;
  password?: string;
  filePath?: string;
  tls?: ConnectionTlsConfig;
  folder?: string;
  serviceName?: string;
  connectionUri?: string;
  replicaSet?: string;
  directConnection?: boolean;
  awsCredentialMode?: AwsCredentialMode;
  awsProfile?: string;
  endpoint?: string;
  apiKey?: string;
  cloudId?: string;
  uri?: string;
  mongoUriHasCredentials?: boolean;
  mongoScheme?: "mongodb" | "mongodb+srv";
  mongoConnectionMode?: "uri" | "fields";
  mongoAuthMethod?: "none" | "password";
  authMechanism?: "SCRAM-SHA-1" | "SCRAM-SHA-256";
  authSource?: string;
  awsRegion?: string;
  awsAccessKeyId?: string;
  awsSecretAccessKey?: string;
  awsSessionToken?: string;
  awsSessionTokenRequired?: boolean;
  awsEndpoint?: string;
  /** Explicit HTTPS STS endpoint for AWS account verification (DynamoDB). */
  awsStsEndpoint?: string;
  ssh?: ConnectionSshConfig;
  useSecretStorage?: boolean;
  /** Internal link to the Secret Storage document for this saved profile. */
  secretStorageProfile?: string;
  color?: string;
}

export function hasAwsCredentialMaterial(
  config: Pick<
    ConnectionConfig,
    "awsAccessKeyId" | "awsSecretAccessKey" | "awsSessionToken"
  >,
): boolean {
  return [
    config.awsAccessKeyId,
    config.awsSecretAccessKey,
    config.awsSessionToken,
  ].some((value) => typeof value === "string" && value.trim() !== "");
}

export function resolveAwsCredentialMode(
  config: Partial<ConnectionConfig>,
): AwsCredentialMode | undefined {
  if (config.awsCredentialMode !== undefined) return config.awsCredentialMode;
  if (hasAwsCredentialMaterial(config)) return "static";
  // Older versions could store a static pair alongside a profile name, or
  // leave no credential fields at all in settings. Only a known plaintext
  // profile without any key material is unambiguous.
  if (config.useSecretStorage === false) {
    return typeof config.awsProfile === "string" && config.awsProfile.trim()
      ? "profile"
      : "default";
  }
}

export function getConnectionTlsSupport(
  type: ConnectionType,
): ConnectionTlsSupport | undefined {
  return CONNECTION_TLS_SUPPORT_BY_TYPE[type];
}

export function resolveConnectionTlsMode(
  config: Pick<ConnectionConfig, "tls">,
): ConnectionTlsMode {
  return config.tls?.mode ?? "disabled";
}

export function isConnectionTlsEnabled(mode: ConnectionTlsMode): boolean {
  return mode !== "disabled";
}

export function getElasticsearchTlsEndpointError(
  config: Partial<ConnectionConfig>,
): string | undefined {
  if (
    config.type !== "elasticsearch" ||
    config.cloudId ||
    !isConnectionTlsEnabled(resolveConnectionTlsMode(config))
  ) {
    return undefined;
  }
  const endpoint = config.connectionUri ?? config.endpoint;
  if (endpoint === undefined) return undefined;
  try {
    if (new URL(endpoint).protocol === "https:") return undefined;
  } catch {}
  return "Elasticsearch TLS requires an HTTPS connection URI or endpoint.";
}

export function normalizeConnectionTlsConfig(
  config: Pick<ConnectionConfig, "tls">,
): ConnectionTlsConfig | undefined {
  if (config.tls?.mode && config.tls.mode !== "disabled") {
    return config.tls;
  }

  return undefined;
}
