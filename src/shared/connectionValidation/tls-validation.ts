/**
 * TLS config validation rules. Pure, returns a list of issues.
 *
 * Each rule below corresponds 1:1 to a branch in the legacy
 * `buildTlsValidationIssues` to guarantee identical behavior.
 */
import {
  type ConnectionConfig,
  type ConnectionTlsConfig,
  getConnectionTlsSupport,
  isConnectionTlsEnabled,
} from "../connectionConfig";
import type { ConnectionType } from "../connectionTypes";
import type { ConnectionValidationIssue } from "./issues";
import { buildInvalidIssue, buildRequiredIssue } from "./issues";
import { hasValue } from "./primitives";

/** Format used for `tls.<field>` issue keys. */
const TLS_FIELD = "tls";

/**
 * Validates the TLS sub-config of `config`. Returns an empty array
 * when TLS is absent or when there is no `config.type` to compare
 * support against.
 */
export function buildTlsValidationIssues(
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
          buildInvalidIssue(
            [TLS_FIELD],
            `TLS configuration is not supported for ${config.type} connections.`,
          ),
        ]
      : [];
  }

  const issues: ConnectionValidationIssue[] = [];

  // Mode must be in the per-driver allow-list.
  if (!support.modes.includes(tlsConfig.mode)) {
    issues.push(
      buildInvalidIssue(
        ["tls.mode"],
        `TLS mode "${tlsConfig.mode}" is not supported for ${config.type} connections.`,
      ),
    );
  }

  // mutualTls has additional requirements: client cert + key, and the
  // driver must support each independently.
  if (tlsConfig.mode === "mutualTls") {
    for (const unsupportedRule of unsupportedMutualTlsRules(
      config.type,
      support,
      tlsConfig,
    )) {
      issues.push(unsupportedRule);
    }
  }

  // Optional features gated by per-driver capability flags.
  issues.push(...unsupportedOptionalTlsRules(config.type, support, tlsConfig));

  return issues;
}

type TlsSupportShape = NonNullable<ReturnType<typeof getConnectionTlsSupport>>;

/** Issues raised when a feature isn't supported by the driver. */
function unsupportedMutualTlsRules(
  type: ConnectionType,
  support: TlsSupportShape,
  tls: ConnectionTlsConfig,
): ConnectionValidationIssue[] {
  const issues: ConnectionValidationIssue[] = [];

  if (!support.supportsClientCertificate) {
    issues.push(
      buildInvalidIssue(
        ["tls.certFilePath"],
        `Client certificates are not supported for ${type} connections.`,
      ),
    );
  }
  if (!support.supportsClientKey) {
    issues.push(
      buildInvalidIssue(
        ["tls.keyFilePath"],
        `Client keys are not supported for ${type} connections.`,
      ),
    );
  }
  if (!hasValue(tls.certFilePath)) {
    issues.push(buildRequiredIssue("tls.certFilePath"));
  }
  if (!hasValue(tls.keyFilePath)) {
    issues.push(buildRequiredIssue("tls.keyFilePath"));
  }
  return issues;
}

/** Optional-field issues — only fire when the user actually filled the field. */
function unsupportedOptionalTlsRules(
  type: ConnectionType,
  support: TlsSupportShape,
  tls: ConnectionTlsConfig,
): ConnectionValidationIssue[] {
  const issues: ConnectionValidationIssue[] = [];

  if (hasValue(tls.caFilePath) && !support.supportsCaFile) {
    issues.push(
      buildInvalidIssue(
        ["tls.caFilePath"],
        `CA certificate files are not supported for ${type} connections.`,
      ),
    );
  }
  if (hasValue(tls.serverNameOverride) && !support.supportsServerNameOverride) {
    issues.push(
      buildInvalidIssue(
        ["tls.serverNameOverride"],
        `TLS server name overrides are not supported for ${type} connections.`,
      ),
    );
  }
  if (hasValue(tls.keyPassphrase) && !support.supportsClientKeyPassphrase) {
    issues.push(
      buildInvalidIssue(
        ["tls.keyPassphrase"],
        `TLS client key passphrases are not supported for ${type} connections.`,
      ),
    );
  }
  return issues;
}
