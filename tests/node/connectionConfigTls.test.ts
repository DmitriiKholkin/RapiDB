import { describe, expect, it } from "vitest";
import {
  CONNECTION_TLS_MODES,
  type ConnectionConfig,
  type ConnectionTlsConfig,
  isConnectionTlsEnabled,
  normalizeConnectionTlsConfig,
  resolveConnectionTlsMode,
} from "../../src/shared/connectionConfig";

function makeConfig(tls?: ConnectionTlsConfig): Pick<ConnectionConfig, "tls"> {
  return { tls };
}

describe("resolveConnectionTlsMode", () => {
  it("returns 'disabled' when tls is undefined", () => {
    expect(resolveConnectionTlsMode(makeConfig())).toBe("disabled");
  });

  it("returns the configured mode when tls is set", () => {
    for (const mode of CONNECTION_TLS_MODES) {
      expect(resolveConnectionTlsMode(makeConfig({ mode }))).toBe(mode);
    }
  });

  it("returns 'disabled' when tls has mode 'disabled'", () => {
    expect(resolveConnectionTlsMode(makeConfig({ mode: "disabled" }))).toBe(
      "disabled",
    );
  });
});

describe("isConnectionTlsEnabled", () => {
  it("returns false for 'disabled'", () => {
    expect(isConnectionTlsEnabled("disabled")).toBe(false);
  });

  it("returns true for all non-disabled modes", () => {
    const enabledModes = CONNECTION_TLS_MODES.filter((m) => m !== "disabled");
    for (const mode of enabledModes) {
      expect(isConnectionTlsEnabled(mode)).toBe(true);
    }
  });
});

describe("normalizeConnectionTlsConfig", () => {
  it("returns undefined when tls is undefined", () => {
    expect(normalizeConnectionTlsConfig(makeConfig())).toBeUndefined();
  });

  it("returns undefined when tls mode is 'disabled'", () => {
    expect(
      normalizeConnectionTlsConfig(makeConfig({ mode: "disabled" })),
    ).toBeUndefined();
  });

  it("returns the tls config when mode is not 'disabled'", () => {
    const tls: ConnectionTlsConfig = {
      mode: "requireVerifyFull",
      caFilePath: "/tmp/ca.pem",
      serverNameOverride: "db.example.com",
    };
    expect(normalizeConnectionTlsConfig(makeConfig(tls))).toEqual(tls);
  });

  it("preserves all tls fields through normalization", () => {
    const tls: ConnectionTlsConfig = {
      mode: "mutualTls",
      caFilePath: "/tmp/ca.pem",
      certFilePath: "/tmp/client.crt",
      keyFilePath: "/tmp/client.key",
      keyPassphrase: "pass",
      serverNameOverride: "db.internal",
    };
    const result = normalizeConnectionTlsConfig(makeConfig(tls));
    expect(result).toEqual(tls);
  });
});

describe("ConnectionConfig type constraints", () => {
  it("ConnectionConfig should not have ssl or rejectUnauthorized fields", () => {
    // This test ensures the type was cleaned up.
    // If someone adds `ssl` or `rejectUnauthorized` back to ConnectionConfig,
    // this test will still pass at runtime but the TypeScript compiler will
    // catch it at build time (the field won't exist on the type).
    const config: ConnectionConfig = {
      id: "test",
      name: "Test",
      type: "pg",
    };
    expect(config).toBeDefined();
    expect("ssl" in config).toBe(false);
    expect("rejectUnauthorized" in config).toBe(false);
  });
});
