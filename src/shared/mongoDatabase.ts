import type { ConnectionConfig } from "./connectionConfig";
import {
  assertMongoUriOptionNameEncoding,
  decodeMongoUriDatabasePath,
  getMongoConnectionUri,
  normalizeMongoConnectionConfig,
  parseMongoUriPathAndQuery,
} from "./mongoConnection";

export function resolveMongoDatabaseName(
  input: Pick<
    ConnectionConfig,
    "database" | "connectionUri" | "uri" | "mongoConnectionMode"
  >,
): string {
  const config = normalizeMongoConnectionConfig({ ...input, type: "mongodb" });
  if (config.database?.trim()) return config.database;

  const uri = getMongoConnectionUri(config);
  if (!uri) return "admin";

  const parsed = parseMongoUriPathAndQuery(uri);
  const database = decodeMongoUriDatabasePath(parsed.pathname);
  assertMongoUriOptionNameEncoding(uri);
  if (database) return database;

  for (const [key, value] of parsed.searchParams) {
    if (key.toLowerCase() === "dbname" && value) return value;
  }
  return "admin";
}
