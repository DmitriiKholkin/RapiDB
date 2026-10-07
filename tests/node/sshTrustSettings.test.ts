import { describe, expect, it } from "vitest";
import type { ConnectionConfig } from "../../src/extension/connectionManagerModels";
import { SshService } from "../../src/extension/services/sshService";
import { buildConnectionSshSettings } from "../../src/extension/sshConnectionHelper";

const config: ConnectionConfig = {
  id: "ssh-trust-settings",
  name: "SSH trust settings",
  type: "pg",
  ssh: {
    host: "bastion.internal",
    username: "tunnel",
    authMethod: "password",
    password: "secret",
  },
};

describe.each([
  ["helper (also used by the facade)", buildConnectionSshSettings],
  [
    "service",
    (input: ConnectionConfig) =>
      new SshService().buildConnectionSshSettings(input),
  ],
] as const)("SSH trust defaults: %s", (_name, buildSettings) => {
  it.each([
    undefined,
    "manual",
  ] as const)("requires a pin for mode %s", (hostVerificationMode) => {
    expect(() =>
      buildSettings({
        ...config,
        ssh: { ...config.ssh, hostVerificationMode },
      }),
    ).toThrow("SSH settings are incomplete");
  });

  it("uses manual verification by default when a pin is supplied", () => {
    expect(
      buildSettings({
        ...config,
        ssh: { ...config.ssh, hostFingerprintSha256: " SHA256:pinned " },
      }),
    ).toMatchObject({
      hostVerificationMode: "manual",
      fingerprintSha256: "SHA256:pinned",
    });
  });

  it("allows an absent pin only for explicitly selected TOFU", () => {
    expect(
      buildSettings({
        ...config,
        ssh: { ...config.ssh, hostVerificationMode: "trustOnFirstUse" },
      }),
    ).toMatchObject({
      hostVerificationMode: "trustOnFirstUse",
      fingerprintSha256: undefined,
    });
  });

  it("keeps an existing TOFU pin", () => {
    expect(
      buildSettings({
        ...config,
        ssh: {
          ...config.ssh,
          hostVerificationMode: "trustOnFirstUse",
          hostFingerprintSha256: " SHA256:pinned ",
        },
      }),
    ).toMatchObject({
      hostVerificationMode: "trustOnFirstUse",
      fingerprintSha256: "SHA256:pinned",
    });
  });
});
