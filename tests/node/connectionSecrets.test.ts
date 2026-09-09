import { describe, expect, it } from "vitest";
import {
  extractCredentialBearingUriSecret,
  sanitizeCredentialBearingUri,
  sanitizePersistedConnectionConfig,
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
