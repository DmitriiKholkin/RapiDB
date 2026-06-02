import { readFileSync } from "node:fs";
import * as tls from "node:tls";
import {
  type ConnectionConfig,
  type ConnectionTlsConfig,
  normalizeConnectionTlsConfig,
} from "../../shared/connectionConfig";
import { getTlsServername } from "../driverRuntimeConfig";

export interface ResolvedConnectionTlsSettings {
  config: ConnectionTlsConfig;
  rejectUnauthorized: boolean;
  skipHostnameVerification: boolean;
  servername?: string;
  ca?: Buffer;
  cert?: Buffer;
  key?: Buffer;
  passphrase?: string;
  checkServerIdentity?: typeof tls.checkServerIdentity;
}

function readOptionalTlsFile(filePath: string | undefined): Buffer | undefined {
  if (typeof filePath !== "string") {
    return undefined;
  }

  const normalized = filePath.trim();
  return normalized ? readFileSync(normalized) : undefined;
}

export function resolveConnectionTlsSettings(
  config: ConnectionConfig,
): ResolvedConnectionTlsSettings | undefined {
  const tlsConfig = normalizeConnectionTlsConfig(config);
  if (!tlsConfig || tlsConfig.mode === "disabled") {
    return undefined;
  }

  const skipHostnameVerification =
    tlsConfig.mode === "requireVerifyCa" ||
    tlsConfig.mode === "requireTrustServerCertificate";
  const rejectUnauthorized = tlsConfig.mode !== "requireTrustServerCertificate";

  return {
    config: tlsConfig,
    rejectUnauthorized,
    skipHostnameVerification,
    servername: getTlsServername(config),
    ca: readOptionalTlsFile(tlsConfig.caFilePath),
    cert: readOptionalTlsFile(tlsConfig.certFilePath),
    key: readOptionalTlsFile(tlsConfig.keyFilePath),
    passphrase:
      typeof tlsConfig.keyPassphrase === "string" &&
      tlsConfig.keyPassphrase.length > 0
        ? tlsConfig.keyPassphrase
        : undefined,
    checkServerIdentity: skipHostnameVerification ? () => undefined : undefined,
  };
}
