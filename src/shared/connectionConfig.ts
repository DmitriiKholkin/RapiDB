import type { ConnectionType } from "./connectionTypes";

export type ConnectionSshAuthMethod = "password" | "privateKey";
export type ConnectionSshHostVerificationMode = "manual" | "trustOnFirstUse";
export type SQLiteWalMode = "auto" | "off";
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
  ssl?: boolean;
  rejectUnauthorized?: boolean;
  tls?: ConnectionTlsConfig;
  folder?: string;
  serviceName?: string;
  connectionUri?: string;
  replicaSet?: string;
  directConnection?: boolean;
  sqliteWalMode?: SQLiteWalMode;
  awsProfile?: string;
  endpoint?: string;
  apiKey?: string;
  cloudId?: string;
  uri?: string;
  authSource?: string;
  awsRegion?: string;
  awsAccessKeyId?: string;
  awsSecretAccessKey?: string;
  awsSessionToken?: string;
  awsEndpoint?: string;
  sshEnabled?: boolean;
  sshHost?: string;
  sshPort?: number;
  sshUsername?: string;
  sshAuthMethod?: ConnectionSshAuthMethod;
  sshHostVerificationMode?: ConnectionSshHostVerificationMode;
  sshPassword?: string;
  sshPrivateKey?: string;
  sshPassphrase?: string;
  sshHostFingerprintSha256?: string;
  useSecretStorage?: boolean;
  color?: string;
}

export function getConnectionTlsSupport(
  type: ConnectionType,
): ConnectionTlsSupport | undefined {
  return CONNECTION_TLS_SUPPORT_BY_TYPE[type];
}

export function resolveConnectionTlsMode(
  config: Pick<ConnectionConfig, "ssl" | "rejectUnauthorized" | "tls">,
): ConnectionTlsMode {
  if (config.tls?.mode) {
    return config.tls.mode;
  }

  if (config.ssl === true) {
    return config.rejectUnauthorized === false
      ? "requireTrustServerCertificate"
      : "requireVerifyFull";
  }

  return "disabled";
}

export function isConnectionTlsEnabled(mode: ConnectionTlsMode): boolean {
  return mode !== "disabled";
}

export function normalizeConnectionTlsConfig(
  config: Pick<ConnectionConfig, "ssl" | "rejectUnauthorized" | "tls">,
): ConnectionTlsConfig | undefined {
  if (config.tls?.mode) {
    return config.tls;
  }

  if (config.ssl !== true) {
    return undefined;
  }

  return {
    mode:
      config.rejectUnauthorized === false
        ? "requireTrustServerCertificate"
        : "requireVerifyFull",
  };
}

export function deriveLegacyConnectionTlsFlags(
  tls: ConnectionTlsConfig | undefined,
): Pick<ConnectionConfig, "ssl" | "rejectUnauthorized"> {
  if (!tls || tls.mode === "disabled") {
    return {
      ssl: false,
      rejectUnauthorized: undefined,
    };
  }

  return {
    ssl: true,
    rejectUnauthorized: tls.mode !== "requireTrustServerCertificate",
  };
}
