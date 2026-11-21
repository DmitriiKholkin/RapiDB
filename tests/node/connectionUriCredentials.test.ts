import { describe, expect, it } from "vitest";
import { restoreCredentialBearingUri } from "../../src/extension/connectionSecrets";

describe("credential-bearing connection URI editing", () => {
  it("replaces old URI credentials when a complete new credential URI is provided", () => {
    const previous =
      "https://old-user:old-pass@elastic.local/base?api_key=old-secret";
    const replacement =
      "https://new-user:new-pass@elastic.local/other?api_key=new-secret";
    expect(restoreCredentialBearingUri(replacement, previous, true)).toBe(
      replacement,
    );
  });

  it("does not carry old userinfo or secret query options to a changed destination", () => {
    const submitted =
      "https://elastic.local/other?mode=fast&API_KEY=new-secret";
    expect(
      restoreCredentialBearingUri(
        submitted,
        "https://u:p@elastic.local/base?mode=slow&api_key=old-secret&token=old-token",
        true,
      ),
    ).toBe(submitted);
  });

  it("keeps one explicit secret query option regardless of key casing at the same destination", () => {
    const submitted = "https://elastic.local/base?mode=fast&API_KEY=new-secret";
    expect(
      restoreCredentialBearingUri(
        submitted,
        "https://u:p@elastic.local/base?mode=fast&api_key=old-secret",
        true,
      ),
    ).toBe(submitted);
  });

  it("restores a query secret before the URI fragment at the same destination", () => {
    expect(
      restoreCredentialBearingUri(
        "https://elastic.local/base?mode=fast#dashboard",
        "https://u:p@elastic.local/base?mode=fast&api_key=old-secret#dashboard",
        true,
      ),
    ).toBe(
      "https://u:p@elastic.local/base?mode=fast&api_key=old-secret#dashboard",
    );
  });
});
