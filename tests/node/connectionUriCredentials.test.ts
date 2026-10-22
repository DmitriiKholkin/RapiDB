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

  it("does not append a duplicate secret query option with different key casing", () => {
    expect(
      restoreCredentialBearingUri(
        "https://elastic.local/other?API_KEY=new-secret",
        "https://u:p@elastic.local/base?api_key=old-secret",
        true,
      ),
    ).toBe("https://u:p@elastic.local/other?API_KEY=new-secret");
  });

  it("keeps query secrets before a URI fragment when editing the endpoint", () => {
    expect(
      restoreCredentialBearingUri(
        "https://elastic.local/edited?mode=fast#dashboard",
        "https://u:p@elastic.local/base?api_key=old-secret&keep=yes#section",
        true,
      ),
    ).toBe(
      "https://u:p@elastic.local/edited?mode=fast&api_key=old-secret#dashboard",
    );
  });
});
