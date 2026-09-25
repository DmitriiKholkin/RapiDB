import { describe, expect, it } from "vitest";
import { ConnectionValidationService } from "../../src/extension/services/connectionValidationService";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";

const service = new ConnectionValidationService();

describe("ConnectionValidationService", () => {
  it("summarizes required and anyOf fields once while preserving structured issues", () => {
    const result = service.validate({ type: "oracle", port: 0 });
    expect(result.message).toBe(
      "Missing required fields: name. Provide at least one of: serviceName | database. Database port must be an integer between 1 and 65535.",
    );
    expect(result.missingRequired).toEqual(["name"]);
    expect(result.missingAnyOf).toEqual([["serviceName", "database"]]);
    expect(result.issues).toEqual([
      {
        code: "required",
        fields: ["name"],
        message: 'Field "name" is required.',
      },
      {
        code: "anyOf",
        fields: ["serviceName", "database"],
        message: 'At least one of "serviceName", "database" is required.',
      },
      {
        code: "invalid",
        fields: ["port"],
        message: "Database port must be an integer between 1 and 65535.",
      },
    ]);
    expect(service.validate({ type: "pg" }).message).toBe(
      "Missing required fields: name, host, database, username.",
    );
  });

  const portConfig: ConnectionConfig = {
    id: "ports",
    name: "Ports",
    type: "pg",
    host: "localhost",
    database: "app",
    username: "user",
  };
  const portSsh: NonNullable<ConnectionConfig["ssh"]> = {
    host: "bastion",
    username: "tunnel",
    authMethod: "password",
    password: "secret",
    hostVerificationMode: "trustOnFirstUse",
  };

  it.each([
    0,
    -1,
    1.5,
    65536,
    70000,
    NaN,
    Infinity,
    -Infinity,
    null,
    "abc",
    "5432",
  ])("rejects invalid provided database and SSH ports: %s", (port) => {
    for (const type of [
      "pg",
      "mysql",
      "mssql",
      "oracle",
      "mongodb",
      "redis",
      "elasticsearch",
    ] as const) {
      const result = service.validate({
        ...portConfig,
        type,
        port: port as number,
      });
      expect(result.issues).toContainEqual(
        expect.objectContaining({ code: "invalid", fields: ["port"] }),
      );
      expect(result.message).toContain(
        "Database port must be an integer between 1 and 65535",
      );
    }
    const result = service.validate({
      ...portConfig,
      ssh: { ...portSsh, port: port as number },
    });
    expect(result.valid).toBe(false);
    expect(result.message).toContain(
      "SSH port must be an integer between 1 and 65535",
    );
  });

  it.each([
    undefined,
    1,
    22,
    5432,
    65535,
  ])("accepts absent/default and boundary ports: %s", (port) => {
    expect(
      service.validate({ ...portConfig, port, ssh: { ...portSsh, port } })
        .valid,
    ).toBe(true);
  });

  it.each([
    { type: "sqlite", filePath: "/tmp/app.db" },
    { type: "dynamodb", awsRegion: "us-east-1" },
    { type: "mongodb", connectionUri: "mongodb://host" },
    { type: "mongodb", uri: "mongodb://host" },
    { type: "redis", connectionUri: "redis://host" },
    { type: "elasticsearch", endpoint: "https://host" },
    { type: "elasticsearch", cloudId: "deployment:ZXM=" },
  ] as const)("ignores unused database ports: %j", (config) => {
    expect(service.validate({ ...portConfig, ...config, port: 0 }).valid).toBe(
      true,
    );
  });

  it("accepts minimal valid configs for all 9 drivers", () => {
    const scenarios: ConnectionConfig[] = [
      {
        id: "pg-1",
        name: "PG",
        type: "pg",
        host: "localhost",
        database: "app",
        username: "postgres",
      },
      {
        id: "mysql-1",
        name: "MySQL",
        type: "mysql",
        host: "localhost",
        database: "app",
        username: "root",
      },
      {
        id: "sqlite-1",
        name: "SQLite",
        type: "sqlite",
        filePath: "/tmp/test.db",
      },
      {
        id: "mssql-1",
        name: "MSSQL",
        type: "mssql",
        host: "localhost",
        database: "app",
      },
      {
        id: "oracle-1",
        name: "Oracle",
        type: "oracle",
        serviceName: "FREEPDB1",
      },
      {
        id: "mongodb-1",
        name: "MongoDB",
        type: "mongodb",
        host: "localhost",
      },
      {
        id: "redis-1",
        name: "Redis",
        type: "redis",
        connectionUri: "redis://localhost:6379",
      },
      {
        id: "elasticsearch-1",
        name: "Elasticsearch",
        type: "elasticsearch",
        endpoint: "https://cluster.example.com",
      },
      {
        id: "dynamodb-1",
        name: "DynamoDB",
        type: "dynamodb",
        awsRegion: "us-east-1",
      },
    ];

    for (const scenario of scenarios) {
      expect(service.validate(scenario).valid).toBe(true);
    }
  });

  it("returns missing field details for invalid configs", () => {
    const scenarios: Array<{
      config: Partial<ConnectionConfig>;
      expectedRequired?: string[];
      expectedAnyOf?: string[];
    }> = [
      {
        config: { type: "pg", name: "PG" },
        expectedRequired: ["host", "database", "username"],
      },
      {
        config: { type: "mysql", name: "MySQL" },
        expectedRequired: ["host", "database", "username"],
      },
      {
        config: { type: "sqlite", name: "SQLite" },
        expectedRequired: ["filePath"],
      },
      {
        config: { type: "mssql", name: "MSSQL" },
        expectedRequired: ["host", "database"],
      },
      {
        config: { type: "oracle", name: "Oracle" },
        expectedAnyOf: ["serviceName", "database"],
      },
      {
        config: { type: "mongodb", name: "MongoDB" },
        expectedAnyOf: ["connectionUri", "uri"],
      },
      {
        config: { type: "redis", name: "Redis" },
        expectedAnyOf: ["connectionUri"],
      },
      {
        config: { type: "elasticsearch", name: "ES" },
        expectedAnyOf: ["connectionUri", "endpoint", "cloudId"],
      },
      {
        config: { type: "dynamodb", name: "DDB" },
        expectedRequired: ["awsRegion"],
      },
    ];

    for (const scenario of scenarios) {
      const result = service.validate(scenario.config);
      expect(result.valid).toBe(false);
      expect(result.message).toBeTruthy();

      if (scenario.expectedRequired) {
        expect(result.missingRequired).toEqual(
          expect.arrayContaining(scenario.expectedRequired),
        );
      }

      if (scenario.expectedAnyOf) {
        expect(result.missingAnyOf).toEqual(
          expect.arrayContaining([
            expect.arrayContaining(scenario.expectedAnyOf),
          ]),
        );
      }
    }
  });

  it("returns a required type issue when type is missing or unsupported", () => {
    const missingType = service.validate({ name: "No Type" });
    const unsupportedType = service.validate({
      name: "Unknown",
      type: "snowflake" as never,
    });

    for (const result of [missingType, unsupportedType]) {
      expect(result.valid).toBe(false);
      expect(result.missingRequired).toEqual(["type"]);
      expect(result.issues).toEqual([
        expect.objectContaining({
          code: "required",
          fields: ["type"],
        }),
      ]);
    }
  });

  it("supports key precedence aliases for MongoDB and Elasticsearch", () => {
    const mongodbWithUriAlias = service.validate({
      type: "mongodb",
      name: "MongoDB",
      uri: "mongodb://localhost:27017/app",
      database: "admin",
      authSource: "legacy-admin",
    });
    const elasticsearchWithCloudId = service.validate({
      type: "elasticsearch",
      name: "Elasticsearch",
      cloudId: "deployment:ZXM=",
    });

    expect(mongodbWithUriAlias.valid).toBe(true);
    expect(elasticsearchWithCloudId.valid).toBe(true);
  });

  it("accepts sqlite WAL mode defaults and rejects invalid values", () => {
    const defaultWalMode = service.validate({
      id: "sqlite-default-wal",
      name: "SQLite Default WAL",
      type: "sqlite",
      filePath: "/tmp/default-wal.db",
    });
    const explicitWalOff = service.validate({
      id: "sqlite-wal-off",
      name: "SQLite WAL Off",
      type: "sqlite",
      filePath: "/tmp/wal-off.db",
      sqliteWalMode: "off",
    });
    const invalidWalMode = service.validate({
      id: "sqlite-invalid-wal",
      name: "SQLite Invalid WAL",
      type: "sqlite",
      filePath: "/tmp/invalid-wal.db",
      sqliteWalMode: "always" as ConnectionConfig["sqliteWalMode"],
    });

    expect(defaultWalMode.valid).toBe(true);
    expect(explicitWalOff.valid).toBe(true);
    expect(invalidWalMode.valid).toBe(false);
    expect(invalidWalMode.issues).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          code: "invalid",
          fields: ["sqliteWalMode"],
        }),
      ]),
    );
  });

  it("rejects sqlite WAL mode for non-sqlite connections", () => {
    const result = service.validate({
      id: "pg-with-sqlite-wal",
      name: "PG",
      type: "pg",
      host: "localhost",
      database: "app",
      username: "postgres",
      sqliteWalMode: "auto",
    });

    expect(result.valid).toBe(false);
    expect(result.issues).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          code: "invalid",
          fields: ["sqliteWalMode"],
        }),
      ]),
    );
  });

  it("requires SSH fingerprint and auth secrets when SSH is enabled", () => {
    const result = service.validate({
      id: "pg-ssh-missing",
      name: "PG over SSH",
      type: "pg",
      host: "db.internal",
      database: "app",
      username: "postgres",
      ssh: {
        host: "bastion.example.com",
        port: 22,
        username: "tunnel",
        authMethod: "password",
      },
    });

    expect(result.valid).toBe(false);
    expect(result.issues).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          code: "required",
          fields: ["ssh"],
        }),
      ]),
    );
  });

  it.each([
    "connectionUri",
    "uri",
  ] as const)("checks only the MongoDB authority for multiple hosts via %s", (field) => {
    const ssh: ConnectionConfig["ssh"] = {
      host: "bastion.example.com",
      port: 22,
      username: "tunnel",
      authMethod: "password",
      password: "secret",
      hostVerificationMode: "trustOnFirstUse",
    };
    const scenarios = [
      ["mongodb://host:27017?readPreferenceTags=dc:ny,rack:1", true],
      ["mongodb://host:27017#one,two", true],
      ["mongodb://host:27017/db,other?tags=a,b", true],
      ["mongodb://u%2Cser:p%40%2F%3F%23@[::1]:27017?tags=a,b", true],
      ["mongodb://user,name:password@[2001:db8::1]:27017", true],
      ["mongodb://host1,host2?tags=a,b", false],
      ["mongodb://u%40:p%2C@[::1]:27017,[::2]:27018#fragment", false],
      ["mongodb://host1,host2/path@host", false],
    ] as const;
    for (const [uri, valid] of scenarios) {
      const result = service.validate({
        name: "Mongo SSH",
        type: "mongodb",
        [field]: uri,
        ssh,
      });
      expect(result.valid, uri).toBe(valid);
      if (!valid) expect(result.message).toContain("single-host");
    }
  });

  it("rejects unsupported MongoDB SSH topologies", () => {
    const result = service.validate({
      id: "mongo-ssh-invalid",
      name: "Mongo SSH",
      type: "mongodb",
      connectionUri: "mongodb://db1.internal:27017,db2.internal:27017/app",
      directConnection: false,
      ssh: {
        host: "bastion.example.com",
        port: 22,
        username: "tunnel",
        authMethod: "password",
        password: "ssh-secret",
        hostFingerprintSha256: "SHA256:AbCdEfGhIjKlMnOpQrStUvWxYz0123456789+/",
      },
    });

    expect(result.valid).toBe(false);
    expect(result.issues).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          code: "invalid",
          fields: ["connectionUri", "uri"],
        }),
        expect.objectContaining({
          code: "invalid",
          fields: ["directConnection"],
        }),
      ]),
    );
  });

  it("allows trust-on-first-use SSH verification without a pre-entered fingerprint", () => {
    const result = service.validate({
      id: "pg-ssh-tofu",
      name: "PG SSH TOFU",
      type: "pg",
      host: "db.internal",
      database: "app",
      username: "postgres",
      ssh: {
        host: "bastion.example.com",
        port: 22,
        username: "tunnel",
        authMethod: "password",
        hostVerificationMode: "trustOnFirstUse",
        password: "ssh-secret",
      },
    });

    expect(result.valid).toBe(true);
  });
});
