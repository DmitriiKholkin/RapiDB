import { describe, expect, it } from "vitest";
import {
  extractCredentialBearingUriSecret,
  sanitizeCredentialBearingUri,
  sanitizePersistedConnectionConfig,
  serializeConnectionSecretsForStoredConfig,
  trimOptionalSecretValue,
} from "../../src/extension/connectionSecrets";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";

describe("connection URI secret handling", () => {
  it("moves query and fragment credentials out of persisted config", () => {
    const uri =
      "https://cluster.example.com/api?region=us-east-1&api_key=secret#token=fragment-secret";

    expect(sanitizeCredentialBearingUri(uri)).toBe(
      "https://cluster.example.com/api?region=us-east-1",
    );
    expect(extractCredentialBearingUriSecret(uri)).toBe(uri);

    const persisted = sanitizePersistedConnectionConfig({
      id: "elastic-secret-uri",
      name: "Elastic",
      type: "elasticsearch",
      endpoint: uri,
    } as ConnectionConfig);
    expect(persisted.endpoint).toBe(
      "https://cluster.example.com/api?region=us-east-1",
    );
    expect(persisted.useSecretStorage).toBe(true);
  });

  it("preserves harmless URI formatting exactly", () => {
    expect(sanitizeCredentialBearingUri("https://cluster.example.com")).toBe(
      "https://cluster.example.com",
    );
  });
});

describe("connection secret persistence", () => {
  it("preserves whitespace-significant secret values byte-for-byte", () => {
    expect(trimOptionalSecretValue("  password  ")).toBe("  password  ");
    expect(trimOptionalSecretValue("\nprivate-key\n")).toBe("\nprivate-key\n");
  });

  it("preserves unknown fields while updating known credentials", () => {
    const previous = JSON.stringify({
      password: "old",
      futureCredential: { version: 2, value: ["keep", null] },
    });
    const serialized = serializeConnectionSecretsForStoredConfig(
      {
        id: "conn-1",
        name: "Primary",
        type: "pg",
        host: "localhost",
        database: "app",
        username: "postgres",
        password: "  new password  ",
        useSecretStorage: true,
      },
      previous,
    );

    expect(JSON.parse(serialized ?? "{}")).toEqual({
      password: "  new password  ",
      futureCredential: { version: 2, value: ["keep", null] },
    });
  });

  it("keeps an unchanged secret document exactly as stored", () => {
    const previous = '{ "futureCredential": true, "password": "same" }';
    expect(
      serializeConnectionSecretsForStoredConfig(
        {
          id: "conn-1",
          name: "Primary",
          type: "pg",
          password: "same",
          useSecretStorage: true,
        },
        previous,
      ),
    ).toBe(previous);
  });
});
