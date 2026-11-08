import type { ConnectionConfig } from "./connectionConfig";
import {
  getMongoConnectionUri,
  normalizeMongoConnectionConfig,
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

  const match = /^mongodb(?:\+srv)?:\/\/[^/?#]*(.*)$/.exec(uri);
  if (!match) throw new Error("Invalid MongoDB connection URI.");
  const parsed = new URL(`mongodb://placeholder${match[1]}`);
  const database = decodeURIComponent(parsed.pathname.replace(/^\//, ""));
  if (database) return database;

  for (const [key, value] of parsed.searchParams) {
    if (key.toLowerCase() === "dbname" && value) return value;
  }
  return "admin";
}
