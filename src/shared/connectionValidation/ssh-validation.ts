/**
 * SSH config validation rules. Pure, returns a list of issues.
 *
 * Behavior is identical to the legacy `buildSshValidationIssues`:
 *   - SSH on SQLite is rejected unconditionally.
 *   - When SSH is configured, the required fields are checked in a
 *     fixed order.
 *   - For `mongodb` over SSH, the URI must not be SRV nor multi-host,
 *     and `directConnection` must be `true`.
 */
import type {
  ConnectionConfig,
  ConnectionSshConfig,
} from "../connectionConfig";
import type { ConnectionValidationIssue } from "./issues";
import { buildInvalidIssue, buildRequiredGroupIssue } from "./issues";
import { hasMongoMultiHostUri, hasMongoSrvUri } from "./mongo-uri";
import { hasValue, isPositiveInteger } from "./primitives";

const SSH_FINGERPRINT_RE = /^SHA256:[A-Za-z0-9+/]+={0,2}$/;

type SshHostVerificationMode = NonNullable<
  ConnectionSshConfig["hostVerificationMode"]
>;

/** Resolves the effective host verification mode (defaults to "manual"). */
function resolveHostVerificationMode(
  config: Partial<ConnectionConfig>,
): SshHostVerificationMode {
  return config.ssh?.hostVerificationMode === "trustOnFirstUse"
    ? "trustOnFirstUse"
    : "manual";
}

/** True when `value` matches the OpenSSH SHA256 fingerprint format. */
export function isSshFingerprintSha256(value: string | undefined): boolean {
  if (!value) {
    return false;
  }
  return SSH_FINGERPRINT_RE.test(value.trim());
}

/** Picks the `connectionUri` (preferred) or the legacy `uri` for Mongo. */
function pickMongoUri(config: Partial<ConnectionConfig>): string | undefined {
  const explicit = config.connectionUri;
  if (typeof explicit === "string" && explicit.trim()) {
    return explicit;
  }
  return config.uri;
}

/**
 * Validates `config.ssh` and returns 0..N issues. Empty array if
 * SSH is not configured at all.
 */
export function buildSshValidationIssues(
  config: Partial<ConnectionConfig>,
): ConnectionValidationIssue[] {
  if (!config.ssh) {
    return [];
  }

  const ssh = config.ssh;
  const issues: ConnectionValidationIssue[] = [];

  // SQLite doesn't have TCP sockets; SSH is nonsensical there.
  if (config.type === "sqlite") {
    issues.push(
      buildInvalidIssue(
        ["ssh"],
        "SSH is not supported for sqlite connections.",
      ),
    );
    return issues;
  }

  // Required scalar fields. Each rule is a self-contained `(when, message)`
  // pair to avoid 4-level nesting in the legacy implementation.
  //
  // NOTE: All SSH issues in the legacy code use `fields: ["ssh"]` (not
  // the specific dotted path) and `code: "required"` — preserved here
  // for byte-for-byte behavioral parity.
  const requiredFieldRules: ReadonlyArray<{
    readonly when: () => boolean;
    readonly message: string;
  }> = [
    {
      when: () => !isPositiveInteger(ssh.port),
      message: 'Field "ssh.port" is required and must be a positive integer.',
    },
    {
      when: () => !hasValue(ssh.host),
      message: 'Field "ssh.host" is required when SSH is enabled.',
    },
    {
      when: () => !hasValue(ssh.username),
      message: 'Field "ssh.username" is required when SSH is enabled.',
    },
    {
      when: () => !hasValue(ssh.authMethod),
      message: 'Field "ssh.authMethod" is required when SSH is enabled.',
    },
  ];

  for (const rule of requiredFieldRules) {
    if (rule.when()) {
      issues.push(buildRequiredGroupIssue(["ssh"], rule.message));
    }
  }

  // Host fingerprint: required for `manual` mode; must be a valid SHA256
  // string when provided. The legacy implementation uses code="required"
  // with fields=["ssh"] (not "ssh.hostFingerprintSha256") — preserved
  // for byte-for-byte behavioral parity.
  const hostVerificationMode = resolveHostVerificationMode(config);
  if (!hasValue(ssh.hostFingerprintSha256)) {
    if (hostVerificationMode === "manual") {
      issues.push(
        buildRequiredGroupIssue(
          ["ssh"],
          'Field "ssh.hostFingerprintSha256" is required when SSH host verification is manual.',
        ),
      );
    }
  } else if (!isSshFingerprintSha256(ssh.hostFingerprintSha256)) {
    issues.push(
      buildInvalidIssue(
        ["ssh"],
        'Field "ssh.hostFingerprintSha256" must use the OpenSSH SHA256 fingerprint format.',
      ),
    );
  }

  // Auth-method-specific required fields. Same `fields: ["ssh"]` and
  // `code: "required"` convention as the requiredFieldRules above.
  if (ssh.authMethod === "password" && !hasValue(ssh.password)) {
    issues.push(
      buildRequiredGroupIssue(
        ["ssh"],
        'Field "ssh.password" is required for password SSH auth.',
      ),
    );
  }
  if (ssh.authMethod === "privateKey" && !hasValue(ssh.privateKey)) {
    issues.push(
      buildRequiredGroupIssue(
        ["ssh"],
        'Field "ssh.privateKey" is required for private key SSH auth.',
      ),
    );
  }

  // MongoDB-over-SSH has additional constraints.
  if (config.type === "mongodb") {
    const mongoUri = pickMongoUri(config);
    if (hasMongoSrvUri(mongoUri)) {
      issues.push(
        buildInvalidIssue(
          ["connectionUri", "uri"],
          "MongoDB over SSH does not support mongodb+srv URIs in v1.",
        ),
      );
    }
    if (hasMongoMultiHostUri(mongoUri)) {
      issues.push(
        buildInvalidIssue(
          ["connectionUri", "uri"],
          "MongoDB over SSH supports only single-host URIs in v1.",
        ),
      );
    }
    if (config.directConnection === false) {
      issues.push(
        buildInvalidIssue(
          ["directConnection"],
          "MongoDB over SSH requires a direct connection in v1.",
        ),
      );
    }
  }

  return issues;
}
