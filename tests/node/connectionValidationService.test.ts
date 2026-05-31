import { describe, expect, it } from "vitest";
import { ConnectionValidationService } from "../../src/extension/services/connectionValidationService";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";

const service = new ConnectionValidationService();

describe("ConnectionValidationService", () => {
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
      sshEnabled: true,
      sshHost: "bastion.example.com",
      sshPort: 22,
      sshUsername: "tunnel",
      sshAuthMethod: "password",
    });

    expect(result.valid).toBe(false);
    expect(result.issues).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          code: "required",
          fields: ["sshHostFingerprintSha256"],
        }),
        expect.objectContaining({
          code: "required",
          fields: ["sshPassword"],
        }),
      ]),
    );
  });

  it("rejects unsupported MongoDB SSH topologies", () => {
    const result = service.validate({
      id: "mongo-ssh-invalid",
      name: "Mongo SSH",
      type: "mongodb",
      connectionUri: "mongodb://db1.internal:27017,db2.internal:27017/app",
      directConnection: false,
      sshEnabled: true,
      sshHost: "bastion.example.com",
      sshPort: 22,
      sshUsername: "tunnel",
      sshAuthMethod: "password",
      sshPassword: "ssh-secret",
      sshHostFingerprintSha256: "SHA256:AbCdEfGhIjKlMnOpQrStUvWxYz0123456789+/",
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
      sshEnabled: true,
      sshHost: "bastion.example.com",
      sshPort: 22,
      sshUsername: "tunnel",
      sshAuthMethod: "password",
      sshHostVerificationMode: "trustOnFirstUse",
      sshPassword: "ssh-secret",
    });

    expect(result.valid).toBe(true);
  });
});
