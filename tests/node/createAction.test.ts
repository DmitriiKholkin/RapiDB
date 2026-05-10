import { describe, expect, it } from "vitest";
import {
  composeCreateAwareConnectionContextValue,
  composeCreateAwareDatabaseContextValue,
  generateCreateDatabaseTemplate,
  generateCreateSchemaTemplate,
  getCreateCapabilityPolicy,
} from "../../src/extension/utils/createAction";
import {
  CONNECTION_TYPES,
  type ConnectionType,
} from "../../src/shared/connectionTypes";

describe("create action policy", () => {
  it("defines create policy for all connection types", () => {
    expect(CONNECTION_TYPES).toHaveLength(9);

    const policies = CONNECTION_TYPES.map((type) => [
      type,
      getCreateCapabilityPolicy(type),
    ]);

    expect(policies).toEqual(
      expect.arrayContaining([
        [
          "pg",
          {
            connectionCreateDb: true,
            databaseCreateSchema: true,
          },
        ],
        [
          "mysql",
          {
            connectionCreateDb: true,
            databaseCreateSchema: false,
          },
        ],
        [
          "sqlite",
          {
            connectionCreateDb: "limited_attach",
            databaseCreateSchema: false,
          },
        ],
        [
          "mssql",
          {
            connectionCreateDb: true,
            databaseCreateSchema: true,
          },
        ],
        [
          "oracle",
          {
            connectionCreateDb: false,
            databaseCreateSchema: "create_user",
          },
        ],
        [
          "mongodb",
          {
            connectionCreateDb: false,
            databaseCreateSchema: false,
          },
        ],
        [
          "redis",
          {
            connectionCreateDb: false,
            databaseCreateSchema: false,
          },
        ],
        [
          "elasticsearch",
          {
            connectionCreateDb: false,
            databaseCreateSchema: false,
          },
        ],
        [
          "dynamodb",
          {
            connectionCreateDb: false,
            databaseCreateSchema: false,
          },
        ],
      ]),
    );
  });

  it("generates database-create templates for supported types with expected formatting policy", () => {
    const expectations: Array<[ConnectionType, boolean, string]> = [
      ["pg", true, "CREATE DATABASE"],
      ["mysql", true, "CREATE DATABASE"],
      ["sqlite", false, "ATTACH DATABASE"],
      ["mssql", true, "CREATE DATABASE"],
    ];

    for (const [connectionType, formatOnOpen, token] of expectations) {
      const template = generateCreateDatabaseTemplate(connectionType);
      expect(template).toBeDefined();
      expect(template?.formatOnOpen).toBe(formatOnOpen);
      expect(template?.script).toContain(token);
    }

    expect(generateCreateDatabaseTemplate("oracle")).toBeUndefined();
    expect(generateCreateDatabaseTemplate("mongodb")).toBeUndefined();
    expect(generateCreateDatabaseTemplate("redis")).toBeUndefined();
    expect(generateCreateDatabaseTemplate("elasticsearch")).toBeUndefined();
    expect(generateCreateDatabaseTemplate("dynamodb")).toBeUndefined();
  });

  it("generates schema-create templates only where policy allows", () => {
    const pg = generateCreateSchemaTemplate("pg");
    expect(pg?.formatOnOpen).toBe(true);
    expect(pg?.script).toContain("CREATE SCHEMA");

    const mssql = generateCreateSchemaTemplate("mssql");
    expect(mssql?.formatOnOpen).toBe(true);
    expect(mssql?.script).toContain("CREATE SCHEMA");

    const oracle = generateCreateSchemaTemplate("oracle");
    expect(oracle?.formatOnOpen).toBe(true);
    expect(oracle?.script).toContain("CREATE USER");

    expect(generateCreateSchemaTemplate("mysql")).toBeUndefined();
    expect(generateCreateSchemaTemplate("sqlite")).toBeUndefined();
    expect(generateCreateSchemaTemplate("mongodb")).toBeUndefined();
    expect(generateCreateSchemaTemplate("redis")).toBeUndefined();
    expect(generateCreateSchemaTemplate("elasticsearch")).toBeUndefined();
    expect(generateCreateSchemaTemplate("dynamodb")).toBeUndefined();
  });

  it("maps create-aware tree context values for connection and database nodes", () => {
    expect(
      composeCreateAwareConnectionContextValue(
        "connectionNode_connected",
        "pg",
      ),
    ).toBe("connectionNode_connected_canCreateDatabase");
    expect(
      composeCreateAwareConnectionContextValue(
        "connectionNode_disconnected",
        "redis",
      ),
    ).toBe("connectionNode_disconnected_noCreateDatabase");

    expect(composeCreateAwareDatabaseContextValue("mssql")).toBe(
      "database_canCreateSchema",
    );
    expect(composeCreateAwareDatabaseContextValue("mysql")).toBe(
      "database_noCreateSchema",
    );
  });

  it("includes selected database context in schema templates", () => {
    const pg = generateCreateSchemaTemplate("pg", "tenant_db");
    expect(pg?.script).toContain("\\c tenant_db");
    expect(pg?.script).toContain("CREATE SCHEMA app;");

    const mssql = generateCreateSchemaTemplate("mssql", "TenantDb");
    expect(mssql?.script).toContain("USE [TenantDb];");
    expect(mssql?.script).toContain("CREATE SCHEMA [app];");

    const oracle = generateCreateSchemaTemplate("oracle", "ORCLPDB1");
    expect(oracle?.script).toContain("-- Database: ORCLPDB1");
    expect(oracle?.script).toContain("CREATE USER app IDENTIFIED BY");
  });

  it("keeps schema templates valid without selected database", () => {
    const pg = generateCreateSchemaTemplate("pg");
    expect(pg?.script).not.toContain("\\c");

    const mssql = generateCreateSchemaTemplate("mssql");
    expect(mssql?.script).not.toContain("USE [");

    const oracle = generateCreateSchemaTemplate("oracle");
    expect(oracle?.script).not.toContain("-- Database:");
  });
});
