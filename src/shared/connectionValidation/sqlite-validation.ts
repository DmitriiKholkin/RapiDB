/**
 * SQLite-specific validation: `sqliteWalMode` only makes sense for
 * SQLite connections and must be either `"auto"` or `"off"`.
 */
import type { ConnectionConfig } from "../connectionConfig";
import type { ConnectionValidationIssue } from "./issues";
import { buildInvalidIssue } from "./issues";

type SqliteWalMode = NonNullable<ConnectionConfig["sqliteWalMode"]>;

/** Resolves the effective WAL mode (defaults to `"auto"`). */
function resolveSqliteWalMode(
  config: Partial<ConnectionConfig>,
): SqliteWalMode | undefined {
  if (config.type !== "sqlite") {
    return undefined;
  }
  return config.sqliteWalMode === "off" ? "off" : "auto";
}

export function buildSqliteValidationIssues(
  config: Partial<ConnectionConfig>,
): ConnectionValidationIssue[] {
  if (config.sqliteWalMode === undefined) {
    return [];
  }

  if (config.type !== "sqlite") {
    return [
      buildInvalidIssue(
        ["sqliteWalMode"],
        'Field "sqliteWalMode" is supported only for sqlite connections.',
      ),
    ];
  }

  const normalizedWalMode = resolveSqliteWalMode(config);
  if (normalizedWalMode === undefined) {
    return [];
  }

  return config.sqliteWalMode === normalizedWalMode
    ? []
    : [
        buildInvalidIssue(
          ["sqliteWalMode"],
          'Field "sqliteWalMode" must be either "auto" or "off".',
        ),
      ];
}
