export function normalizeRedisLogicalDatabaseName(name: string): string {
  const match = /^(?:db)?(\d+)$/.exec(name.trim());
  if (!match) return name;

  const index = Number(match[1]);
  return Number.isSafeInteger(index) ? `db${index}` : name;
}

export function getConfiguredRedisDatabaseName(config: {
  database?: string;
  connectionUri?: string;
}): string {
  let selected = config.database?.trim();
  if (!selected && config.connectionUri) {
    try {
      selected = new URL(config.connectionUri).pathname.slice(1);
    } catch {
      return "";
    }
  }
  return normalizeRedisLogicalDatabaseName(selected || "0");
}
