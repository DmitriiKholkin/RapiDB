import { format as formatSql } from "sql-formatter";
import type { ConnectionType } from "../../shared/connectionTypes";

function formatterLanguageForConnection(
  connectionType: ConnectionType | undefined,
): "postgresql" | "mysql" | "transactsql" | "sqlite" | "plsql" | "sql" {
  switch (connectionType) {
    case "pg":
      return "postgresql";
    case "mysql":
      return "mysql";
    case "mssql":
      return "transactsql";
    case "sqlite":
      return "sqlite";
    case "oracle":
      return "plsql";
    default:
      return "sql";
  }
}

function ensureStatementTerminator(sql: string): string {
  const trimmed = sql.trim();
  if (trimmed === "") {
    return trimmed;
  }
  return trimmed.endsWith(";") ? trimmed : `${trimmed};`;
}

export function formatMutationPreviewSql(
  statements: readonly string[],
  connectionType: ConnectionType | undefined,
): string {
  const language = formatterLanguageForConnection(connectionType);

  return statements
    .map((statement) => ensureStatementTerminator(statement))
    .filter((statement) => statement.length > 0)
    .map((statement) => {
      try {
        return formatSql(statement, {
          language,
          tabWidth: 2,
          keywordCase: "upper",
          linesBetweenQueries: 2,
        }).trim();
      } catch {
        return statement.trim();
      }
    })
    .join("\n\n");
}
