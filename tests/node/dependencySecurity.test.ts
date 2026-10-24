import { readFileSync } from "node:fs";
import { createRequire } from "node:module";
import { describe, expect, it } from "vitest";

const require = createRequire(import.meta.url);

describe("release dependency security constraints", () => {
  it("keeps all locked MSAL copies outside the auth-code disclosure range", () => {
    const lock = JSON.parse(
      readFileSync(new URL("../../package-lock.json", import.meta.url), "utf8"),
    ) as { packages: Record<string, { version?: string }> };
    const versions = Object.entries(lock.packages)
      .filter(([path]) => path.endsWith("node_modules/@azure/msal-node"))
      .map(([, entry]) => entry.version ?? "");
    expect(versions.length).toBeGreaterThan(0);
    for (const version of versions) {
      const [major, minor] = version.split(".").map(Number);
      expect(major < 5 || major > 7 || (major === 7 && minor >= 1)).toBe(true);
    }
  });

  it.each([
    "tedious",
    "@vscode/vsce",
  ])("loads the pinned Azure/MSAL stack used by %s and generates valid UUIDs", (parent) => {
    const parentRequire = createRequire(require.resolve(parent));
    const identityRequire = createRequire(
      parentRequire.resolve("@azure/identity"),
    );
    const { ClientSecretCredential } = parentRequire("@azure/identity");
    const { CryptoProvider, ConfidentialClientApplication } =
      identityRequire("@azure/msal-node");
    const id = new CryptoProvider().createNewGuid();
    expect(id).toMatch(
      /^[\da-f]{8}-[\da-f]{4}-4[\da-f]{3}-[89ab][\da-f]{3}-[\da-f]{12}$/i,
    );
    expect(new ClientSecretCredential(id, id, "test-secret")).toBeDefined();
    expect(
      new ConfidentialClientApplication({
        auth: { clientId: id, clientSecret: "test-secret" },
      }),
    ).toBeDefined();
  });
});
