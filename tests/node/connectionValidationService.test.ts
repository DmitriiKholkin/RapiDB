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
      authDatabase: "admin",
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
});
