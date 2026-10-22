import {
  isConnectionTlsEnabled,
  resolveConnectionTlsMode,
} from "../shared/connectionConfig";
import { resolveMongoRemoteTarget } from "../shared/mongoConnection";
import type { ConnectionConfig } from "./connectionManagerModels";
import { DynamoDBDriver } from "./dbDrivers/dynamodb";
import { ElasticsearchDriver } from "./dbDrivers/elasticsearch";
import { MongoDBDriver } from "./dbDrivers/mongodb";
import { MSSQLDriver } from "./dbDrivers/mssql";
import { MySQLDriver } from "./dbDrivers/mysql";
import { OracleDriver } from "./dbDrivers/oracle";
import { PostgresDriver } from "./dbDrivers/postgres";
import { RedisDriver } from "./dbDrivers/redis";
import { SQLiteDriver } from "./dbDrivers/sqlite";
import type { DriverTimeoutSettingsProvider } from "./dbDrivers/timeout";
import { createTimeoutAwareDriver } from "./dbDrivers/timeout";
import type { IDBDriver } from "./dbDrivers/types";

export class ConnectionDriverFactory {
  constructor(
    private readonly getTimeoutSettings: DriverTimeoutSettingsProvider,
  ) {}

  createDriver(config: ConnectionConfig): IDBDriver {
    const timeoutSettingsProvider = this.getTimeoutSettings;

    const driver = (() => {
      switch (config.type) {
        case "mysql":
          return new MySQLDriver(config, timeoutSettingsProvider);
        case "pg":
          return new PostgresDriver(config, timeoutSettingsProvider);
        case "sqlite":
          return new SQLiteDriver(config, timeoutSettingsProvider);
        case "mssql":
          return new MSSQLDriver(config, timeoutSettingsProvider);
        case "oracle":
          return new OracleDriver(config, timeoutSettingsProvider);
        case "mongodb":
          return new MongoDBDriver(config);
        case "redis":
          return new RedisDriver(config, timeoutSettingsProvider);
        case "elasticsearch":
          return new ElasticsearchDriver(config, timeoutSettingsProvider);
        case "dynamodb":
          return new DynamoDBDriver(config);
        default: {
          const unknownType: never = config.type;
          throw new Error(`[RapiDB] Unknown driver type: ${unknownType}`);
        }
      }
    })();

    return createTimeoutAwareDriver(driver, timeoutSettingsProvider);
  }

  resolveMongoRemoteTarget(config: ConnectionConfig): {
    host: string;
    port: number;
  } {
    return resolveMongoRemoteTarget(config);
  }

  resolveRedisRemoteTarget(config: ConnectionConfig): {
    host: string;
    port: number;
  } {
    if (config.connectionUri) {
      const parsed = new URL(config.connectionUri);
      return {
        host: parsed.hostname.replace(/^\[|\]$/g, ""),
        port: parsed.port ? Number.parseInt(parsed.port, 10) : 6379,
      };
    }

    return {
      host: config.host?.trim() || "127.0.0.1",
      port: config.port ?? 6379,
    };
  }

  resolveUrlRemoteTarget(
    value: string | undefined,
    defaultPort: number,
  ): { host: string; port: number } | undefined {
    if (!value) {
      return undefined;
    }

    const url = new URL(value);
    return {
      host: url.hostname,
      port:
        url.port.length > 0
          ? Number.parseInt(url.port, 10)
          : url.protocol === "https:"
            ? 443
            : url.protocol === "http:"
              ? 80
              : defaultPort,
    };
  }

  resolveElasticsearchRemoteTarget(
    config: ConnectionConfig,
  ): { host: string; port: number } | undefined {
    return (
      this.resolveUrlRemoteTarget(
        config.connectionUri ?? config.endpoint,
        isConnectionTlsEnabled(resolveConnectionTlsMode(config)) ? 443 : 9200,
      ) ??
      (config.host?.trim()
        ? {
            host: config.host.trim(),
            port: config.port ?? 9200,
          }
        : undefined)
    );
  }
}
