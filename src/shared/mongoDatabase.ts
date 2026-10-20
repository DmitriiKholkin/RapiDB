import type { ConnectionConfig } from "./connectionConfig";

/** Resolve the operation database, independently of the authentication source. */
export function resolveMongoDatabaseName(
  config: Pick<ConnectionConfig, "database" | "connectionUri" | "uri">,
): string {
  if (config.database?.trim()) return config.database;

  const uri = config.connectionUri ?? config.uri;
  if (!uri) return "admin";

  // Only parse the path/query, never MongoDB's authority: WHATWG URL does not
  // support MongoDB seed lists. A placeholder also works for SRV and IPv6 URIs
  // and preserves the SDK's path normalization and single percent-decoding.
  const match = /^mongodb(?:\+srv)?:\/\/[^/?#]*(.*)$/.exec(uri);
  if (!match) throw new Error("Invalid MongoDB connection URI.");
  const parsed = new URL(`mongodb://placeholder${match[1]}`);
  const database = decodeURIComponent(parsed.pathname.replace(/^\//, ""));
  if (database) return database;

  // The SDK also accepts dbName as a case-insensitive URI option. A path
  // database takes precedence; authSource must never select the operation DB.
  for (const [key, value] of parsed.searchParams) {
    if (key.toLowerCase() === "dbname" && value) return value;
  }
  return "admin";
}
