import type { ConnectionType } from "../../shared/connectionTypes";

type ConnectionCreateDatabasePolicy =
  | false
  | true
  | "limited_attach"
  | "mongo_shell";
type DatabaseCreateSchemaPolicy = false | true | "create_user";

export type CreateCapabilityPolicy = {
  connectionCreateDb: ConnectionCreateDatabasePolicy;
  databaseCreateSchema: DatabaseCreateSchemaPolicy;
};

export type CreateTemplate = {
  script: string;
  formatOnOpen: boolean;
};

export type CreateConnectionNodeKind =
  | "connectionNode_connected"
  | "connectionNode_disconnected";

const CREATE_POLICY: Readonly<Record<ConnectionType, CreateCapabilityPolicy>> =
  {
    pg: {
      connectionCreateDb: true,
      databaseCreateSchema: true,
    },
    mysql: {
      connectionCreateDb: true,
      databaseCreateSchema: false,
    },
    sqlite: {
      connectionCreateDb: "limited_attach",
      databaseCreateSchema: false,
    },
    mssql: {
      connectionCreateDb: true,
      databaseCreateSchema: true,
    },
    oracle: {
      connectionCreateDb: false,
      databaseCreateSchema: "create_user",
    },
    mongodb: {
      connectionCreateDb: "mongo_shell",
      databaseCreateSchema: false,
    },
    redis: {
      connectionCreateDb: false,
      databaseCreateSchema: false,
    },
    elasticsearch: {
      connectionCreateDb: false,
      databaseCreateSchema: false,
    },
    dynamodb: {
      connectionCreateDb: false,
      databaseCreateSchema: false,
    },
  };

export function getCreateCapabilityPolicy(
  connectionType: ConnectionType,
): CreateCapabilityPolicy {
  return CREATE_POLICY[connectionType];
}

function assertNever(value: never): never {
  throw new Error(`Unhandled connection type: ${String(value)}`);
}

export function composeCreateAwareConnectionContextValue(
  kind: CreateConnectionNodeKind,
  connectionType: ConnectionType,
): string {
  const policy = getCreateCapabilityPolicy(connectionType);
  const suffix =
    policy.connectionCreateDb === false
      ? "noCreateDatabase"
      : "canCreateDatabase";
  return `${kind}_${suffix}`;
}

export function composeCreateAwareDatabaseContextValue(
  connectionType?: ConnectionType,
): string {
  if (!connectionType) {
    return "database_noCreateSchema";
  }

  const policy = getCreateCapabilityPolicy(connectionType);
  const suffix =
    policy.databaseCreateSchema === false
      ? "noCreateSchema"
      : "canCreateSchema";
  return `database_${suffix}`;
}

export function generateCreateDatabaseTemplate(
  connectionType: ConnectionType,
): CreateTemplate | undefined {
  switch (connectionType) {
    case "pg":
      return {
        script: [
          "-- PostgreSQL",
          "CREATE DATABASE app_db;",
          "",
          "-- Optional: create schema after connecting to the new database",
          "-- CREATE SCHEMA app;",
        ].join("\n"),
        formatOnOpen: true,
      };
    case "mysql":
      return {
        script: ["-- MySQL / MariaDB", "CREATE DATABASE app_db;"].join("\n"),
        formatOnOpen: true,
      };
    case "sqlite":
      return {
        script: [
          "SQLite does not support CREATE DATABASE.",
          "Create a new file and optionally attach it in the current session:",
          "ATTACH DATABASE '/absolute/path/app.db' AS app;",
        ].join("\n"),
        formatOnOpen: false,
      };
    case "mssql":
      return {
        script: ["-- SQL Server (MSSQL)", "CREATE DATABASE [app_db];"].join(
          "\n",
        ),
        formatOnOpen: true,
      };
    case "oracle":
      return undefined;
    case "mongodb":
      return {
        script:
          'command {"database":"app_db","command":{"create":"sample_collection"}}',
        formatOnOpen: false,
      };
    case "redis":
      return undefined;
    case "elasticsearch":
      return undefined;
    case "dynamodb":
      return undefined;
  }

  return assertNever(connectionType);
}

export function generateCreateSchemaTemplate(
  connectionType: ConnectionType,
): CreateTemplate | undefined {
  switch (connectionType) {
    case "pg":
      return {
        script: ["-- PostgreSQL", "CREATE SCHEMA app;"].join("\n"),
        formatOnOpen: true,
      };
    case "mysql":
      return undefined;
    case "sqlite":
      return undefined;
    case "mssql":
      return {
        script: ["-- SQL Server (MSSQL)", "CREATE SCHEMA [app];"].join("\n"),
        formatOnOpen: true,
      };
    case "oracle":
      return {
        script: [
          "-- Oracle creates a schema through a user",
          'CREATE USER app IDENTIFIED BY "ChangeMe123!";',
          "GRANT CONNECT, RESOURCE TO app;",
        ].join("\n"),
        formatOnOpen: true,
      };
    case "mongodb":
      return undefined;
    case "redis":
      return undefined;
    case "elasticsearch":
      return undefined;
    case "dynamodb":
      return undefined;
  }

  return assertNever(connectionType);
}
