/**
 * Shared type and parsing utilities for connection secret storage.
 *
 * Extracted to eliminate duplication between ConnectionManager and
 * ConnectionFormPanel. Both modules previously contained identical
 * StoredConnectionSecrets type definitions and parseStoredConnectionSecrets
 * implementations.
 */

/** Shape of secrets persisted in VS Code SecretStorage per connection. */
export type StoredConnectionSecrets = {
  password?: string;
  apiKey?: string;
  awsAccessKeyId?: string;
  awsSecretAccessKey?: string;
  awsSessionToken?: string;
  connectionUri?: string;
  uri?: string;
  endpoint?: string;
  awsEndpoint?: string;
  sshPassword?: string;
  sshPrivateKey?: string;
  sshPassphrase?: string;
  tlsKeyPassphrase?: string;
};

export const CONNECTION_SECRET_KEYS: readonly (keyof StoredConnectionSecrets)[] =
  [
    "password",
    "apiKey",
    "awsAccessKeyId",
    "awsSecretAccessKey",
    "awsSessionToken",
    "connectionUri",
    "uri",
    "endpoint",
    "awsEndpoint",
    "sshPassword",
    "sshPrivateKey",
    "sshPassphrase",
    "tlsKeyPassphrase",
  ];

/**
 * Safely extract a string field from a parsed JSON record.
 * Returns the original string if it is non-empty, otherwise undefined.
 */
function extractStringField(
  record: Record<string, unknown>,
  key: string,
): string | undefined {
  const value = record[key];
  if (typeof value !== "string") {
    return undefined;
  }
  return value.length > 0 ? value : undefined;
}

/**
 * Parse a JSON-serialized secret blob into a structured object.
 *
 * Falls back to treating the entire value as a legacy plaintext password
 * if the value is not valid JSON or does not match the expected shape.
 *
 * @param value - Raw string from SecretStorage (may be undefined)
 * @returns Parsed secrets, or `{}` if input is empty/undefined
 */
export function parseStoredConnectionSecrets(
  value: string | undefined,
): StoredConnectionSecrets {
  if (!value) {
    return {};
  }

  try {
    const parsed = JSON.parse(value) as Record<string, unknown>;
    if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
      const secrets: StoredConnectionSecrets = {};
      for (const key of CONNECTION_SECRET_KEYS) {
        const extracted = extractStringField(parsed, key);
        if (extracted !== undefined) {
          secrets[key] = extracted;
        }
      }
      return secrets;
    }
  } catch {
    // JSON parse failed — treat as legacy plaintext password
  }

  return { password: value };
}

export interface StoredConnectionSecretDocument {
  known: StoredConnectionSecrets;
  record: Record<string, unknown>;
  isLegacy: boolean;
}

export function parseStoredConnectionSecretDocument(
  value: string | undefined,
): StoredConnectionSecretDocument {
  if (!value) {
    return { known: {}, record: {}, isLegacy: false };
  }

  try {
    const parsed = JSON.parse(value) as unknown;
    if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
      return {
        known: parseStoredConnectionSecrets(value),
        record: { ...(parsed as Record<string, unknown>) },
        isLegacy: false,
      };
    }
  } catch {
    // Legacy values are intentionally retained byte-for-byte as passwords.
  }

  return {
    known: { password: value },
    record: {},
    isLegacy: true,
  };
}

/**
 * Serialize a secrets object for storage.
 * Filters out undefined fields and returns undefined if the object is empty.
 */
export function serializeStoredConnectionSecrets(
  secrets: StoredConnectionSecrets,
): string | undefined {
  const filtered = Object.fromEntries(
    Object.entries(secrets).filter(([, value]) => typeof value === "string"),
  );

  return Object.keys(filtered).length > 0
    ? JSON.stringify(filtered)
    : undefined;
}
