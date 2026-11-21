import { describe, expect, it } from "vitest";
import type { StoredConnectionConfig } from "../../src/extension/connectionManagerModels";
import {
  assertConnectionSecretBinding,
  bindConnectionSecret,
  connectionSecretTarget,
} from "../../src/extension/connectionSecretBinding";

const profile: StoredConnectionConfig = {
  id: "dynamo-sts",
  name: "DynamoDB",
  type: "dynamodb",
  awsRegion: "us-east-1",
  awsCredentialMode: "static",
  useSecretStorage: true,
  secretStorageProfile: "00000000-0000-4000-8000-000000000001",
};
const vpce = "https://vpce-0abc-def.sts.us-east-1.vpce.amazonaws.com";

describe("Secret binding of the DynamoDB STS endpoint", () => {
  it("keeps the target of profiles without an STS endpoint", () => {
    expect(connectionSecretTarget({ ...profile, awsStsEndpoint: "" })).toBe(
      connectionSecretTarget(profile),
    );
  });

  it("binds saved credentials to the STS endpoint", () => {
    const withSts = { ...profile, awsStsEndpoint: vpce };
    const other = {
      ...profile,
      awsStsEndpoint: "https://vpce-0abc-xyz.sts.us-east-1.vpce.amazonaws.com",
    };
    expect(connectionSecretTarget(withSts)).not.toBe(
      connectionSecretTarget(profile),
    );
    expect(connectionSecretTarget(withSts)).not.toBe(
      connectionSecretTarget(other),
    );

    const document = bindConnectionSecret(
      profile,
      JSON.stringify({ awsAccessKeyId: "AKIA", awsSecretAccessKey: "s" }),
    );
    expect(() =>
      assertConnectionSecretBinding(profile, document),
    ).not.toThrow();
    // Changing (or adding) the STS endpoint does not inherit the secret.
    expect(() => assertConnectionSecretBinding(withSts, document)).toThrow(
      "do not match",
    );
    const rebound = bindConnectionSecret(withSts, document);
    expect(() => assertConnectionSecretBinding(withSts, rebound)).not.toThrow();
    expect(() => assertConnectionSecretBinding(other, rebound)).toThrow(
      "do not match",
    );
    expect(() => assertConnectionSecretBinding(profile, rebound)).toThrow(
      "do not match",
    );
  });
});
