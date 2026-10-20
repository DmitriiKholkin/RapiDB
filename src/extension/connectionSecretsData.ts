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
  } catch {}

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
  } catch {}

  return {
    known: { password: value },
    record: {},
    isLegacy: true,
  };
}

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
