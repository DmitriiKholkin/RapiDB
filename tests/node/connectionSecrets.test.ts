import { describe, expect, it } from "vitest";
import {
  extractCredentialBearingUriSecret,
  hasConnectionConfigSecrets,
  resolveStoredCredentialBearingUri,
  sanitizeConnectionConfigForResponse,
  sanitizeCredentialBearingUri,
  sanitizePersistedConnectionConfig,
  serializeConnectionSecretsForStoredConfig,
  shouldForceSecretStorage,
  shouldKeepHandWrittenSecretsInSettings,
  trimOptionalSecretValue,
} from "../../src/extension/connectionSecrets";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";
import { validateConnectionConfig } from "../../src/shared/connectionValidation";

describe("connection URI secret handling", () => {
  it("resolves a stored URI secret only for a matching credential-free settings value", () => {
    const secret = "redis://user:old@cache.local:6379/0";
    expect(
      resolveStoredCredentialBearingUri("redis://cache.local:6379/0", secret),
    ).toBe(secret);
    for (const value of [
      "redis://other.local:6379/0",
      "redis://user:new@cache.local:6379/0",
      undefined,
    ]) {
      expect(resolveStoredCredentialBearingUri(value, secret)).toBe(value);
    }
    expect(
      resolveStoredCredentialBearingUri(
        "redis://cache.local:6379/0",
        "redis://user:pa/ss@cache.local:6379/0",
      ),
    ).toBe("redis://cache.local:6379/0");
  });

  it("retains a mismatched URI secret only when migration asks for it", () => {
    const previous = JSON.stringify({
      connectionUri: "redis://user:old@old.local:6379/0",
    });
    const config: ConnectionConfig = {
      id: "redis",
      name: "Redis",
      type: "redis",
      connectionUri: "redis://new.local:6379/0",
      useSecretStorage: true,
    };
    expect(
      serializeConnectionSecretsForStoredConfig(config, previous),
    ).toBeUndefined();
    expect(
      serializeConnectionSecretsForStoredConfig(config, previous, {
        retainCredentialBearingUris: true,
      }),
    ).toBe(previous);
  });

  it("removes only explicitly cleared key passphrases and accepts replacements", () => {
    const previous = JSON.stringify({
      password: "db-secret",
      sshPrivateKey: "key",
      sshPassphrase: "old-ssh",
      tlsKeyPassphrase: "old-tls",
      futureSecret: "preserved",
    });
    const config: ConnectionConfig = {
      id: "keys",
      name: "Keys",
      type: "pg",
      ssh: { authMethod: "privateKey" },
      tls: { mode: "mutualTls" },
    };
    const unchanged = JSON.parse(
      serializeConnectionSecretsForStoredConfig(config, previous) ?? "{}",
    );
    expect(unchanged.sshPassphrase).toBe("old-ssh");
    expect(unchanged.tlsKeyPassphrase).toBe("old-tls");
    const cleared = JSON.parse(
      serializeConnectionSecretsForStoredConfig(config, previous, {
        sshPassphrase: true,
        tlsKeyPassphrase: true,
      }) ?? "{}",
    );
    expect(cleared).toEqual({
      password: "db-secret",
      sshPrivateKey: "key",
      futureSecret: "preserved",
    });
    const replacement = JSON.parse(
      serializeConnectionSecretsForStoredConfig(
        {
          ...config,
          ssh: { ...config.ssh, passphrase: "new-ssh" },
          tls: { mode: "mutualTls", keyPassphrase: "new-tls" },
        },
        previous,
        { sshPassphrase: true, tlsKeyPassphrase: true },
      ) ?? "{}",
    );
    expect(replacement).toMatchObject({
      sshPassphrase: "new-ssh",
      tlsKeyPassphrase: "new-tls",
    });
  });
  it.each([
    ["redis://user:p@ss@host:6379/0", "redis://host:6379/0"],
    ["user:uri-secret@host/db", "host/db"],
    ["//user:uri-secret@host/db", "//host/db"],
    [
      "mongodb://host/db?authMechanism=MONGODB-AWS&authMechanismProperties=AWS_SESSION_TOKEN%3ASESSION_SECRET&retryWrites=true",
      "mongodb://host/db?authMechanism=MONGODB-AWS&retryWrites=true",
    ],
    [
      "mongodb://host/db?auth%4dechanismProperties=AWS_SESSION_TOKEN%3ASESSION_SECRET",
      "mongodb://host/db",
    ],
    [
      "mongodb://host/db?AUTHMECHANISMPROPERTIES=AWS_SESSION_TOKEN%3ASESSION_SECRET",
      "mongodb://host/db",
    ],
    [
      "mongodb://user:p%40%2F%3F%23%20ss@host1:27017,host2:27017/db?replicaSet=rs",
      "mongodb://host1:27017,host2:27017/db?replicaSet=rs",
    ],
  ])("removes complete userinfo from %s and preserves the secret on resave", (uri, redacted) => {
    const config: ConnectionConfig = {
      id: "uri",
      name: "URI",
      type: uri.startsWith("redis://") ? "redis" : "mongodb",
      connectionUri: uri,
      useSecretStorage: true,
    };
    const persisted = sanitizePersistedConnectionConfig(config);
    expect(persisted.connectionUri).toBe(redacted);
    expect(validateConnectionConfig(persisted).valid).toBe(
      /^(?:mongodb|redis):\/\//.test(uri),
    );
    expect(extractCredentialBearingUriSecret(uri)).toBe(uri);
    const secret = serializeConnectionSecretsForStoredConfig(config, undefined);
    expect(JSON.parse(secret ?? "{}")).toEqual({ connectionUri: uri });
    expect(serializeConnectionSecretsForStoredConfig(persisted, secret)).toBe(
      secret,
    );
  });

  it.each(
    ["/", "?", "#", " ", "\t", "\n", "\\"].flatMap((delimiter) =>
      ["mongodb://", "jdbc:postgresql://", "", "//"].map(
        (prefix) => `${prefix}user:pa${delimiter}ss@host/db`,
      ),
    ),
  )("rejects ambiguous userinfo %j without echoing secrets", (uri) => {
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
    "jdbc:postgresql://user:uri-secret@host/db",
    "alice:supersecret://tail@host/db",
    "outer:middle:inner://user:uri-secret@host/db",
  ])("rejects credential-bearing nested schemes without retaining a possible password: %s", (uri) => {
    for (const useSecretStorage of [undefined, false, true]) {
      for (const field of [
        "connectionUri",
        "uri",
        "endpoint",
        "awsEndpoint",
      ] as const) {
        const config: ConnectionConfig = {
          id: "nested-scheme",
          name: "Nested scheme",
          type: "mongodb",
          [field]: uri,
          useSecretStorage,
        };
        const validation = validateConnectionConfig(config);
        expect(validation.valid).toBe(false);
        expect(validation.message).not.toContain(uri);
        expect(() => sanitizePersistedConnectionConfig(config)).toThrow(
          "Ambiguous URI credentials",
        );
        expect(() => sanitizeConnectionConfigForResponse(config)).toThrow(
          "Ambiguous URI credentials",
        );
        expect(() =>
          serializeConnectionSecretsForStoredConfig(config, undefined),
        ).toThrow("Ambiguous URI credentials");
      }
    }
  });

  it.each([
    "redis://user:123/ss@host/0",
    "jdbc:postgresql://user:uri/secret@host/db",
    "user:uri/secret@host/db",
    "//user:uri/secret@host/db",
    "redis://user:p@ss/word@host/0",
    "https://host:443/path@name",
    "redis://user name@host/0",
    "mongodb://us/er:password@host/db",
    "mongodb://us?er:password@host/db",
    "mongodb://us#er:password@host/db",
    "mongodb://admin:pa/ss@db/app",
    "redis://user:123?ss@host/0",
    "redis://cache.local:6379/0?client_name=a:x@b",
    "redis://user:123?s=s@host/0",
    "mongodb://user:1234/db?x=1@evil.example.com",
  ])("rejects ambiguous URI %s rather than guessing a host or password", (uri) => {
    expect(() => sanitizeCredentialBearingUri(uri)).toThrow(
      "Ambiguous URI credentials",
    );
  });

  it.each([
    "https://host/path@name?mode=read&column=name&label=example#section",
    "jdbc:postgresql://host:5432/db",
    "host:5432/db",
    "//host:5432/db",
    "mongodb://host1:27017,host2:27017/db?replicaSet=rs",
    "redis://host:6379/0",
    "https://host/path%40name?label=user%40example.com",
    "https://host:443/path%40name",
    "redis://cache.local:6379/0?client_name=a@b",
    "https://es.example.com:9243/?x=a@b",
    "mongodb://host1:27017,host2:27017/db?appName=a@b",
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
        useSecretStorage: true,
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
      useSecretStorage: true,
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
    "authMechanismProperties",
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

  it.each([
    ["user:uri-secret@host/db", "host/db"],
    ["//user:uri-secret@host/db", "//host/db"],
  ])("protects nonstandard userinfo in every URI field: %s", (uri, redacted) => {
    for (const field of [
      "connectionUri",
      "uri",
      "endpoint",
      "awsEndpoint",
    ] as const) {
      for (const useSecretStorage of [undefined, false, true]) {
        const config: ConnectionConfig = {
          id: "nonstandard-uri",
          name: "Nonstandard URI",
          type: "mongodb",
          [field]: uri,
          useSecretStorage,
        };
        expect(hasConnectionConfigSecrets(config)).toBe(true);
        expect(sanitizeConnectionConfigForResponse(config)[field]).toBe(
          redacted,
        );
        expect(sanitizePersistedConnectionConfig(config)[field]).toBe(
          useSecretStorage === true ? redacted : uri,
        );
        expect(sanitizePersistedConnectionConfig(config).useSecretStorage).toBe(
          useSecretStorage ?? false,
        );
        expect(
          JSON.parse(
            serializeConnectionSecretsForStoredConfig(config, undefined) ??
              "{}",
          ),
        ).toEqual({
          [field]: uri,
        });
      }
    }
  });
});

describe("connection secret persistence", () => {
  const handWrittenSecretConfigs: ConnectionConfig[] = [
    {
      id: "pg",
      name: "pg",
      type: "pg",
      host: "db",
      password: "hand-password",
    },
    {
      id: "es",
      name: "es",
      type: "elasticsearch",
      endpoint: "https://es",
      apiKey: "hand-api-key",
    },
    {
      id: "ddb",
      name: "ddb",
      type: "dynamodb",
      awsRegion: "us-east-1",
      awsAccessKeyId: "hand-access",
      awsSecretAccessKey: "hand-secret",
      awsSessionToken: "hand-token",
    },
    {
      id: "ssh",
      name: "ssh",
      type: "mysql",
      host: "db",
      ssh: {
        host: "bastion",
        port: 22,
        username: "tunnel",
        authMethod: "privateKey",
        privateKey: "hand-key",
        passphrase: "hand-passphrase",
      },
    },
    {
      id: "ssh-password",
      name: "ssh-password",
      type: "mssql",
      host: "db",
      ssh: {
        host: "bastion",
        port: 22,
        username: "tunnel",
        authMethod: "password",
        password: "hand-ssh-password",
      },
    },
    {
      id: "tls",
      name: "tls",
      type: "oracle",
      host: "db",
      tls: {
        mode: "mutualTls",
        certFilePath: "/cert",
        keyFilePath: "/key",
        keyPassphrase: "hand-tls",
      },
    },
    {
      id: "uri",
      name: "uri",
      type: "mongodb",
      mongoConnectionMode: "uri",
      connectionUri: "mongodb://user:hand-uri@host/db",
    },
    {
      id: "redis",
      name: "redis",
      type: "redis",
      uri: "redis://user:hand-uri@host/0",
    },
    {
      id: "ddb-endpoint",
      name: "ddb-endpoint",
      type: "dynamodb",
      awsRegion: "us-east-1",
      awsEndpoint: "https://user:hand-uri@host/",
    },
  ];

  it.each(
    handWrittenSecretConfigs,
  )("keeps hand-written $id secrets in settings without a storage flag", (config) => {
    expect(shouldKeepHandWrittenSecretsInSettings(config)).toBe(true);
    expect(shouldForceSecretStorage(config)).toBe(false);
    expect(sanitizePersistedConnectionConfig(config)).toEqual({
      ...config,
      useSecretStorage: false,
    });
    for (const useSecretStorage of [false, true]) {
      expect(
        shouldKeepHandWrittenSecretsInSettings({ ...config, useSecretStorage }),
      ).toBe(false);
    }
  });

  it("treats ambiguous hand-written URI credentials as plaintext secrets", () => {
    const config: ConnectionConfig = {
      id: "ambiguous",
      name: "ambiguous",
      type: "mongodb",
      connectionUri: "mongodb://admin:pa/ss@db/app",
    };
    expect(shouldKeepHandWrittenSecretsInSettings(config)).toBe(true);
  });

  it.each([
    "pg",
    "elasticsearch",
    "dynamodb",
  ] as const)("does not force Secret Storage for flagless %s connections without secrets", (type) => {
    const config: ConnectionConfig = {
      id: type,
      name: type,
      type,
      host: "db",
      ssh: {
        host: "bastion",
        port: 22,
        username: "tunnel",
        authMethod: "privateKey",
      },
      tls: { mode: "mutualTls", certFilePath: "/cert", keyFilePath: "/key" },
    };
    expect(shouldKeepHandWrittenSecretsInSettings(config)).toBe(false);
    expect(shouldForceSecretStorage(config)).toBe(false);
    expect(sanitizePersistedConnectionConfig(config)).toEqual(config);
  });

  it.each(
    handWrittenSecretConfigs,
  )("stores every $id secret only by the explicit flag", (config) => {
    expect(
      sanitizePersistedConnectionConfig({ ...config, useSecretStorage: false }),
    ).toEqual({ ...config, useSecretStorage: false });
    const persisted = sanitizePersistedConnectionConfig({
      ...config,
      useSecretStorage: true,
    });
    expect(persisted.useSecretStorage).toBe(true);
    expect(hasConnectionConfigSecrets(persisted)).toBe(false);
    expect(
      serializeConnectionSecretsForStoredConfig(
        { ...config, useSecretStorage: true },
        undefined,
      ),
    ).toBeDefined();
  });

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
