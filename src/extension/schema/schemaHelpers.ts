import { resolveMongoDatabaseName } from "../../shared/mongoDatabase";
import { getConfiguredRedisDatabaseName } from "../../shared/redisDatabase";
import type { ConnectionConfig } from "../connectionManagerModels";
import {
  DEFAULT_DRIVER_ENTITY_MANIFEST,
  type DriverEntityManifest,
  type IDBDriver,
} from "../dbDrivers/types";

export function resolveDriverEntityManifest(
  driver: IDBDriver | undefined,
): DriverEntityManifest {
  return driver?.getEntityManifest?.() ?? DEFAULT_DRIVER_ENTITY_MANIFEST;
}

export function getConfiguredDefaultDatabaseName(
  config: ConnectionConfig,
): string {
  if (config.type === "mongodb") {
    return resolveMongoDatabaseName(config);
  }

  if (config.type === "elasticsearch") {
    return "default";
  }

  if (config.type === "dynamodb") {
    return config.awsRegion || "default";
  }

  if (config.type === "oracle") {
    return config.serviceName || config.database || "";
  }

  if (config.type === "redis") {
    return getConfiguredRedisDatabaseName(config);
  }

  return (
    config.database || config.serviceName || (config.filePath ? "main" : "")
  );
}
