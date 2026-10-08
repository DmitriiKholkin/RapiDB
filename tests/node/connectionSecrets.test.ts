import { describe, expect, it } from "vitest";
import {
  extractCredentialBearingUriSecret,
  hasConnectionConfigSecrets,
  sanitizeConnectionConfigForResponse,
  sanitizeCredentialBearingUri,
  sanitizePersistedConnectionConfig,
  serializeConnectionSecretsForStoredConfig,
  trimOptionalSecretValue,
} from "../../src/extension/connectionSecrets";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";
import { validateConnectionConfig } from "../../src/shared/connectionValidation";

describe("connection URI secret handling", () => {
  it.each([
    ["redis://user:p@ss@host:6379/0", "redis://host:6379/0"],
    [
      "mongodb://user:p%40%2F%3F%23%20ss@host1:27017,host2:27017/db?replicaSet=rs",
      "mongodb://host1:27017,host2:27017/db?replicaSet=rs",
    ],
  ])("removes complete userinfo from %s and preserves the secret on resave", (uri, redacted) => {
    const config: ConnectionConfig = {
      id: "uri",
      name: "URI",
      type: "mongodb",
      connectionUri: uri,
    };
    const persisted = sanitizePersistedConnectionConfig(config);
    expect(persisted.connectionUri).toBe(redacted);
    expect(validateConnectionConfig(persisted).valid).toBe(true);
    expect(extractCredentialBearingUriSecret(uri)).toBe(uri);
    const secret = serializeConnectionSecretsForStoredConfig(config, undefined);
    expect(JSON.parse(secret!)).toEqual({ connectionUri: uri });
    expect(serializeConnectionSecretsForStoredConfig(persisted, secret)).toBe(
      secret,
    );
  });

  it.each([
    "/",
    "?",
    "#",
    " ",
    "\t",
    "\n",
    "\\",
  ])("rejects ambiguous userinfo containing %j without echoing secrets", (delimiter) => {
    const uri = `mongodb://user:pa${delimiter}ss@host/db`;
    expect(() => sanitizeCredentialBearingUri(uri)).toThrow(
      "Ambiguous URI credentials",
    );
    expect(() => extractCredentialBearingUriSecret(uri)).toThrow(
      "Ambiguous URI credentials",
    );
    for (const useSecretStorage of [undefined, false, true]) {
      for (const field of [
        "connectionUri",
        "uri",
        "endpoint",
        "awsEndpoint",
      ] as const) {
        const config: ConnectionConfig = {
          id: "uri",
          name: "URI",
          type: "mongodb",
          host: "host",
          [field]: uri,
          useSecretStorage,
        };
        const validation = validateConnectionConfig(config);
        expect(validation.valid).toBe(false);
        expect(validation.message).not.toContain(uri);
        expect(() => sanitizePersistedConnectionConfig(config)).toThrow(
          "Ambiguous URI credentials",
        );
      }
    }
  });

  it.each([
    "redis://user:123/ss@host/0",
    "redis://user:p@ss/word@host/0",
    "https://host:443/path@name",
    "redis://user name@host/0",
    "mongodb://us/er:password@host/db",
    "mongodb://us?er:password@host/db",
    "mongodb://us#er:password@host/db",
  ])("rejects ambiguous URI %s rather than guessing a host or password", (uri) => {
    expect(() => sanitizeCredentialBearingUri(uri)).toThrow(
      "Ambiguous URI credentials",
    );
  });

  it.each([
    "https://host/path@name?mode=read&column=name&label=example#section",
    "mongodb://host1:27017,host2:27017/db?replicaSet=rs",
    "redis://host:6379/0",
    "https://host/path%40name?label=user%40example.com",
    "https://host:443/path%40name",
  ])("preserves harmless URI %s", (uri) => {
    expect(sanitizeCredentialBearingUri(uri)).toBe(uri);
    expect(extractCredentialBearingUriSecret(uri)).toBeUndefined();
  });

  it.each([
    "auth",
    "key",
    "sig",
    "%61uth",
    "KEY",
    "SIG",
  ])("stores %s query and fragment credentials only in Secret Storage", (key) => {
    for (const delimiter of ["?", "#"]) {
      const uri = `https://host/${delimiter}${key}=secret`;
      const config: ConnectionConfig = {
        id: "generic-uri-secret",
        name: "Endpoint",
        type: "elasticsearch",
        endpoint: uri,
      };
      const persisted = sanitizePersistedConnectionConfig(config);
      expect(persisted.endpoint).toBe("https://host/");
      expect(persisted.useSecretStorage).toBe(true);
      expect(extractCredentialBearingUriSecret(uri)).toBe(uri);
      const secret = serializeConnectionSecretsForStoredConfig(
        config,
        undefined,
      );
      expect(JSON.parse(secret ?? "{}")).toEqual({ endpoint: uri });
      expect(serializeConnectionSecretsForStoredConfig(persisted, secret)).toBe(
        secret,
      );
    }
  });

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

  it.each([
    "proxyPassword",
    "tlsCertificateKeyFilePassword",
    "passphrase",
    "PROXYPASSWORD",
    "tlsCertificateKeyFile%50assword",
  ])("redacts supported MongoDB URI credential %s and preserves it across resaves", (key) => {
    const uri = `mongodb://db.internal/app?replicaSet=rs&${key}=uri-secret&tls=true`;
    const redacted = "mongodb://db.internal/app?replicaSet=rs&tls=true";
    const config: ConnectionConfig = {
      id: "mongo-uri-credential",
      name: "MongoDB",
      type: "mongodb",
      connectionUri: uri,
      useSecretStorage: true,
    };
    const persisted = sanitizePersistedConnectionConfig(config);
    expect(persisted.connectionUri).toBe(redacted);
    expect(hasConnectionConfigSecrets(config)).toBe(true);
    const secret = serializeConnectionSecretsForStoredConfig(config, undefined);
    expect(JSON.parse(secret ?? "{}")).toEqual({ connectionUri: uri });
    expect(serializeConnectionSecretsForStoredConfig(persisted, secret)).toBe(
      secret,
    );
    expect(
      sanitizeConnectionConfigForResponse({
        ...config,
        useSecretStorage: false,
      }).connectionUri,
    ).toBe(redacted);
    expect(
      sanitizeCredentialBearingUri(`mongodb://host/app#${key}=uri-secret`),
    ).toBe("mongodb://host/app");
  });

  it("preserves the explicit plaintext opt-out for unambiguous credentials", () => {
    const config: ConnectionConfig = {
      id: "plaintext",
      name: "Redis",
      type: "redis",
      connectionUri: "redis://user:p%40ss@host:6379/0",
      useSecretStorage: false,
    };
    expect(sanitizePersistedConnectionConfig(config)).toEqual(config);
  });
});

describe("connection secret persistence", () => {
  it.each([
    undefined,
    false,
    true,
  ])("sanitizes outward metadata independently of storage mode %s without changing the input", (useSecretStorage) => {
    const config: ConnectionConfig = {
      id: "outward",
      name: "Outward",
      type: "dynamodb",
      useSecretStorage,
      password: " db-secret ",
      apiKey: "api-secret",
      awsAccessKeyId: "access-secret",
      awsSecretAccessKey: "aws-secret",
      awsSessionToken: "token-secret",
      connectionUri: "mongodb://user:uri-secret@host/db",
      uri: "redis://user:alias-secret@host/0",
      endpoint: "https://host/?api_key=endpoint-secret&region=us",
      awsEndpoint: "https://host/#token=fragment-secret",
      ssh: {
        host: "bastion",
        port: 22,
        username: "user",
        authMethod: "privateKey",
        password: "ssh-secret",
        privateKey: "private-secret",
        passphrase: "passphrase-secret",
      },
      tls: {
        mode: "mutualTls",
        certFilePath: "/cert",
        keyFilePath: "/key",
        keyPassphrase: "tls-secret",
      },
    };
    const original = structuredClone(config);
    expect(hasConnectionConfigSecrets(config)).toBe(true);
    const response = sanitizeConnectionConfigForResponse(config);
    expect(hasConnectionConfigSecrets(response)).toBe(false);
    expect(JSON.stringify(response)).not.toContain("secret");
    expect(response).toMatchObject({
      id: "outward",
      useSecretStorage,
      connectionUri: "mongodb://host/db",
      uri: "redis://host/0",
      endpoint: "https://host/?region=us",
      awsEndpoint: "https://host/",
      ssh: { host: "bastion", authMethod: "privateKey" },
      tls: { keyFilePath: "/key" },
    });
    expect(config).toEqual(original);
    if (useSecretStorage === false)
      expect(sanitizePersistedConnectionConfig(config)).toEqual(original);
  });

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
