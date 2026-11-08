import { describe, expect, it } from "vitest";
import {
  connectionEditState,
  preserveTrustedSshFingerprint,
} from "../../src/extension/connectionEditState";
import type { ConnectionConfig } from "../../src/extension/connectionManagerModels";

const PIN = "SHA256:AbCdEfGhIjKlMnOpQrStUvWxYz0123456789+/";

function profile(
  ssh: Partial<NonNullable<ConnectionConfig["ssh"]>>,
): ConnectionConfig {
  return {
    id: "conn-1",
    name: "Tunnelled",
    type: "pg",
    host: "db.internal",
    database: "app",
    username: "qa",
    useSecretStorage: true,
    ssh: {
      host: "bastion.example.com",
      port: 22,
      username: "tunnel",
      authMethod: "privateKey",
      hostVerificationMode: "trustOnFirstUse",
      ...ssh,
    },
  };
}

describe("preserveTrustedSshFingerprint", () => {
  const stored = profile({ hostFingerprintSha256: PIN });
  const expected = connectionEditState(stored, {});

  it.each([
    ["host", { host: "new-bastion.example.com" }],
    ["port", { port: 2222 }],
    ["username", { username: "other" }],
  ])("drops a resubmitted pin after the SSH %s changes", (_field, change) => {
    const result = preserveTrustedSshFingerprint(
      profile({ hostFingerprintSha256: PIN, ...change }),
      expected,
      stored,
    );

    expect(result.ssh?.hostFingerprintSha256).toBeUndefined();
  });

  it("keeps the pin for an unchanged SSH target", () => {
    const result = preserveTrustedSshFingerprint(
      profile({ hostFingerprintSha256: PIN }),
      expected,
      stored,
    );

    expect(result.ssh?.hostFingerprintSha256).toBe(PIN);
  });

  it("keeps a pin added while the editor was open for an unchanged target", () => {
    const result = preserveTrustedSshFingerprint(
      profile({}),
      connectionEditState(profile({}), {}),
      stored,
    );

    expect(result.ssh?.hostFingerprintSha256).toBe(PIN);
  });

  it("does not restore a pin added while the editor was open after a target change", () => {
    const result = preserveTrustedSshFingerprint(
      profile({ host: "new-bastion.example.com" }),
      connectionEditState(profile({}), {}),
      stored,
    );

    expect(result.ssh?.hostFingerprintSha256).toBeUndefined();
  });
});
