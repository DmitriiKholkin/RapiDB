import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import type { ConnectionConfig } from "../../src/extension/connectionManagerModels";
import {
  createExtensionContextStub,
  FakeConnectionManagerStore,
} from "../support/fakeConnectionManagerStore";
import { MockEventEmitter } from "../support/mockVscode";
import { SSH_HOST_FINGERPRINT_SHA256 } from "../support/sshFingerprints";

vi.mock("vscode", () => ({
  EventEmitter: MockEventEmitter,
  ProgressLocation: { Window: 10 },
  window: {},
}));

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

let buildSettings: (input: ConnectionConfig) => unknown;
let disposeManager: () => Promise<void>;

beforeAll(async () => {
  const { ConnectionManager } = await import(
    "../../src/extension/connectionManager"
  );
  const manager = new ConnectionManager(
    createExtensionContextStub() as never,
    new FakeConnectionManagerStore(),
  );
  const resolver = manager as unknown as {
    buildConnectionSshSettings(input: ConnectionConfig): unknown;
  };
  buildSettings = (input) => resolver.buildConnectionSshSettings(input);
  disposeManager = () => manager.dispose();
});

afterAll(async () => {
  await disposeManager();
});

describe("SSH trust defaults", () => {
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
        ssh: {
          ...config.ssh,
          hostFingerprintSha256: ` ${SSH_HOST_FINGERPRINT_SHA256} `,
        },
      }),
    ).toMatchObject({
      hostVerificationMode: "manual",
      fingerprintSha256: SSH_HOST_FINGERPRINT_SHA256,
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
          hostFingerprintSha256: ` ${SSH_HOST_FINGERPRINT_SHA256} `,
        },
      }),
    ).toMatchObject({
      hostVerificationMode: "trustOnFirstUse",
      fingerprintSha256: SSH_HOST_FINGERPRINT_SHA256,
    });
  });
});
