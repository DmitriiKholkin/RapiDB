import type { QueryEditorSqlDialect } from "../../shared/webviewContracts";
import type { ConnectionManager } from "../connectionManager";
import type {
  DriverCapabilities,
  ReadOnlyQueryDecision,
  ReadOnlyQueryGuard,
} from "../dbDrivers/types";

const ALLOWED_READ_ONLY_QUERY: ReadOnlyQueryDecision = { allowed: true };

const SQL_READ_ONLY_QUERY_REASON =
  "[RapiDB] Read-only SQL connections allow only read-only queries.";

const SQLITE_READ_ONLY_PRAGMAS = new Set([
  "application_id",
  "auto_vacuum",
  "busy_timeout",
  "cache_size",
  "cache_spill",
  "collation_list",
  "compile_options",
  "count_changes",
  "data_version",
  "database_list",
  "encoding",
  "foreign_key_check",
  "foreign_key_list",
  "foreign_keys",
  "freelist_count",
  "function_list",
  "hard_heap_limit",
  "ignore_check_constraints",
  "index_info",
  "index_list",
  "index_xinfo",
  "integrity_check",
  "journal_mode",
  "journal_size_limit",
  "legacy_alter_table",
  "locking_mode",
  "max_page_count",
  "module_list",
  "optimize",
  "page_count",
  "page_size",
  "pragma_list",
  "query_only",
  "quick_check",
  "read_uncommitted",
  "recursive_triggers",
  "reverse_unordered_selects",
  "schema_version",
  "secure_delete",
  "short_column_names",
  "shrink_memory",
  "soft_heap_limit",
  "synchronous",
  "table_info",
  "table_list",
  "table_xinfo",
  "threads",
  "trusted_schema",
  "user_version",
  "wal_autocheckpoint",
]);

const SQLITE_PRAGMA_ARGUMENT_SAFE_ALLOWLIST = new Set([
  "foreign_key_check",
  "foreign_key_list",
  "index_info",
  "index_xinfo",
  "integrity_check",
  "quick_check",
  "table_info",
  "table_list",
  "table_xinfo",
]);

const DIALECT_ALLOWLISTS: Readonly<
  Partial<Record<QueryEditorSqlDialect, ReadonlySet<string>>>
> = {
  postgresql: new Set(["show", "table", "values", "explain"]),
  mysql: new Set(["show", "describe", "desc", "explain"]),
  sqlite: new Set(["pragma", "values", "explain"]),
  transactsql: new Set(["values"]),
  plsql: new Set([]),
  sql: new Set(["values", "explain"]),
};

const SQL_MUTATION_KEYWORDS = new Set([
  "alter",
  "analyze",
  "attach",
  "backup",
  "begin",
  "call",
  "checkpoint",
  "cluster",
  "comment",
  "commit",
  "copy",
  "create",
  "delete",
  "detach",
  "drop",
  "exec",
  "execute",
  "grant",
  "insert",
  "kill",
  "lock",
  "merge",
  "pragma",
  "refresh",
  "reindex",
  "release",
  "replace",
  "reset",
  "restore",
  "revoke",
  "rollback",
  "savepoint",
  "set",
  "start",
  "truncate",
  "update",
  "upsert",
  "use",
  "vacuum",
]);

type ConnectionManagerLike = {
  getConnection?: ConnectionManager["getConnection"];
  getDriverCapabilities?: ConnectionManager["getDriverCapabilities"];
  getDriver?: ConnectionManager["getDriver"];
};

export function allowReadOnlyQuery(): ReadOnlyQueryDecision {
  return ALLOWED_READ_ONLY_QUERY;
}

export function denyReadOnlyQuery(reason: string): ReadOnlyQueryDecision {
  return { allowed: false, reason };
}

export function isConnectionReadOnly(
  connectionManager: ConnectionManagerLike,
  connectionId: string,
): boolean {
  return connectionManager.getConnection?.(connectionId)?.readOnly === true;
}

export function assertConnectionWritable(
  connectionManager: ConnectionManagerLike,
  connectionId: string,
  operationName: string,
): void {
  if (!isConnectionReadOnly(connectionManager, connectionId)) {
    return;
  }

  const label =
    connectionManager.getConnection?.(connectionId)?.name?.trim() ||
    connectionId;
  throw new Error(
    `[RapiDB] Cannot ${operationName}: connection "${label}" is read-only.`,
  );
}

export function decideReadOnlyQueryExecution(
  connectionManager: ConnectionManagerLike,
  connectionId: string,
  queryText: string,
): ReadOnlyQueryDecision {
  if (!isConnectionReadOnly(connectionManager, connectionId)) {
    return allowReadOnlyQuery();
  }

  return resolveReadOnlyQueryGuard(connectionManager, connectionId)(queryText);
}

export function sqlReadOnlyQueryGuard(
  queryText: string,
  dialect: QueryEditorSqlDialect = "sql",
): ReadOnlyQueryDecision {
  const statements = sanitizeSqlForReadOnlyClassification(queryText)
    .split(";")
    .map((statement) => statement.trim())
    .filter((statement) => statement.length > 0);

  if (statements.length === 0) {
    return denyReadOnlyQuery(SQL_READ_ONLY_QUERY_REASON);
  }

  return statements.every((statement) =>
    isReadOnlySqlStatement(statement, dialect),
  )
    ? allowReadOnlyQuery()
    : denyReadOnlyQuery(SQL_READ_ONLY_QUERY_REASON);
}

export function createSqlReadOnlyQueryGuard(
  dialect: QueryEditorSqlDialect = "sql",
): ReadOnlyQueryGuard {
  return (queryText: string) => sqlReadOnlyQueryGuard(queryText, dialect);
}

function resolveReadOnlyQueryGuard(
  connectionManager: ConnectionManagerLike,
  connectionId: string,
): ReadOnlyQueryGuard {
  const capabilities =
    connectionManager.getDriverCapabilities?.(connectionId) ??
    resolveDriverCapabilities(connectionManager, connectionId);

  return capabilities?.readOnlyQueryGuard ?? sqlReadOnlyQueryGuard;
}

function resolveDriverCapabilities(
  connectionManager: ConnectionManagerLike,
  connectionId: string,
): DriverCapabilities | undefined {
  const driver = connectionManager.getDriver?.(connectionId);
  return driver?.getCapabilities?.();
}

function sanitizeSqlForReadOnlyClassification(queryText: string): string {
  let sanitized = "";

  for (let index = 0; index < queryText.length; ) {
    const char = queryText[index];
    const next = queryText[index + 1];

    if (char === "-" && next === "-") {
      sanitized += " ";
      index += 2;
      while (index < queryText.length && queryText[index] !== "\n") {
        index += 1;
      }
      continue;
    }

    if (char === "/" && next === "*") {
      sanitized += " ";
      index += 2;
      while (index < queryText.length) {
        if (queryText[index] === "*" && queryText[index + 1] === "/") {
          index += 2;
          break;
        }
        index += 1;
      }
      continue;
    }

    if (char === "'") {
      sanitized += " ";
      index += 1;
      while (index < queryText.length) {
        if (queryText[index] === "'") {
          if (queryText[index + 1] === "'") {
            index += 2;
            continue;
          }
          index += 1;
          break;
        }
        index += 1;
      }
      continue;
    }

    if (char === '"' || char === "`") {
      sanitized += " ";
      const quote = char;
      index += 1;
      while (index < queryText.length) {
        if (queryText[index] === quote) {
          if (queryText[index + 1] === quote) {
            index += 2;
            continue;
          }
          index += 1;
          break;
        }
        index += 1;
      }
      continue;
    }

    if (char === "[") {
      sanitized += " ";
      index += 1;
      while (index < queryText.length) {
        if (queryText[index] === "]") {
          if (queryText[index + 1] === "]") {
            index += 2;
            continue;
          }
          index += 1;
          break;
        }
        index += 1;
      }
      continue;
    }

    const dollarQuoteTag = readDollarQuoteTag(queryText, index);
    if (dollarQuoteTag) {
      sanitized += " ";
      index += dollarQuoteTag.length;
      const closingIndex = queryText.indexOf(dollarQuoteTag, index);
      if (closingIndex === -1) {
        break;
      }
      index = closingIndex + dollarQuoteTag.length;
      continue;
    }

    sanitized += char;
    index += 1;
  }

  return sanitized;
}

function readDollarQuoteTag(queryText: string, index: number): string | null {
  if (queryText[index] !== "$") {
    return null;
  }

  const match = /^(\$[A-Za-z_][A-Za-z0-9_]*\$|\$\$)/.exec(
    queryText.slice(index),
  );
  return match?.[0] ?? null;
}

function tokenizeSql(statement: string): string[] {
  return statement.match(/[A-Za-z_][A-Za-z0-9_$]*|[(),]/g) ?? [];
}

function isReadOnlySqlStatement(
  statement: string,
  dialect: QueryEditorSqlDialect,
): boolean {
  const tokens = tokenizeSql(statement);
  if (tokens.length === 0) {
    return false;
  }

  if (isReadOnlySqlTokens(tokens)) {
    return true;
  }

  return isDialectReadOnlySqlStatement(statement, tokens, dialect);
}

function isReadOnlySqlTokens(tokens: string[]): boolean {
  if (tokens.length === 0) {
    return false;
  }

  const firstToken = tokens[0]?.toLowerCase();
  if (firstToken === "select") {
    return isReadOnlySelectTokens(tokens);
  }

  if (firstToken === "with") {
    return isReadOnlyCteTokens(tokens);
  }

  return false;
}

function isDialectReadOnlySqlStatement(
  statement: string,
  tokens: string[],
  dialect: QueryEditorSqlDialect,
): boolean {
  const firstToken = tokens[0]?.toLowerCase();
  if (!firstToken) {
    return false;
  }

  const allowlist = DIALECT_ALLOWLISTS[dialect];
  if (!allowlist?.has(firstToken)) {
    return false;
  }

  switch (firstToken) {
    case "show":
      return isReadOnlyShowStatement(tokens);
    case "describe":
    case "desc":
      return isReadOnlyDescribeStatement(tokens, dialect);
    case "pragma":
      return dialect === "sqlite" && isReadOnlySqlitePragma(statement, tokens);
    case "table":
      return dialect === "postgresql" && tokens.length >= 2;
    case "values":
      return isReadOnlyValuesTokens(tokens);
    case "explain":
      return isReadOnlyExplainStatement(tokens, dialect);
    default:
      return false;
  }
}

function isReadOnlyShowStatement(tokens: string[]): boolean {
  return tokens.length >= 2;
}

function isReadOnlyDescribeStatement(
  tokens: string[],
  dialect: QueryEditorSqlDialect,
): boolean {
  if (dialect !== "mysql") {
    return false;
  }

  if (tokens.length < 2) {
    return false;
  }

  const secondToken = tokens[1]?.toLowerCase();
  if (secondToken === "select") {
    return isReadOnlySelectTokens(tokens.slice(1));
  }

  return true;
}

function isReadOnlyValuesTokens(tokens: string[]): boolean {
  if (tokens.length < 2 || tokens[1] !== "(") {
    return false;
  }

  let depth = 0;
  for (const token of tokens) {
    if (token === "(") {
      depth += 1;
      continue;
    }
    if (token === ")") {
      depth -= 1;
      if (depth < 0) {
        return false;
      }
      continue;
    }

    if (SQL_MUTATION_KEYWORDS.has(token.toLowerCase())) {
      return false;
    }
  }

  return depth === 0;
}

function isReadOnlyExplainStatement(
  tokens: string[],
  dialect: QueryEditorSqlDialect,
): boolean {
  let index = 1;

  if (tokens[index]?.toLowerCase() === "query") {
    if (dialect !== "sqlite" || tokens[index + 1]?.toLowerCase() !== "plan") {
      return false;
    }
    index += 2;
  }

  while (index < tokens.length) {
    const keyword = tokens[index]?.toLowerCase();
    if (!keyword) {
      return false;
    }

    if (keyword === "analyze" || keyword === "plan") {
      return false;
    }

    if (keyword === "format") {
      index += 1;
      while (index < tokens.length && tokens[index] !== "(") {
        const nextKeyword = tokens[index]?.toLowerCase();
        if (
          nextKeyword === "select" ||
          nextKeyword === "with" ||
          nextKeyword === "table" ||
          nextKeyword === "values"
        ) {
          break;
        }
        index += 1;
      }
      continue;
    }

    return isReadOnlySqlTokens(tokens.slice(index));
  }

  return false;
}

function isReadOnlySqlitePragma(statement: string, tokens: string[]): boolean {
  if (tokens.length < 2) {
    return false;
  }

  const normalizedStatement = statement.toLowerCase().trim();
  if (normalizedStatement.includes("=")) {
    return false;
  }

  const pragmaName = tokens[1]?.toLowerCase();
  if (!pragmaName || !SQLITE_READ_ONLY_PRAGMAS.has(pragmaName)) {
    return false;
  }

  const hasPragmaArguments = new RegExp(
    `^pragma\\s+(?:[a-z_][a-z0-9_]*\\.)?${pragmaName}\\s*\\(`,
    "i",
  ).test(normalizedStatement);

  if (
    hasPragmaArguments &&
    !SQLITE_PRAGMA_ARGUMENT_SAFE_ALLOWLIST.has(pragmaName)
  ) {
    return false;
  }

  return true;
}

function isReadOnlyCteTokens(tokens: string[]): boolean {
  let index = 1;

  if (tokens[index]?.toLowerCase() === "recursive") {
    index += 1;
  }

  while (index < tokens.length) {
    let columnListDepth = 0;
    while (index < tokens.length) {
      const token = tokens[index];
      if (token === "(") {
        columnListDepth += 1;
      } else if (token === ")") {
        if (columnListDepth === 0) {
          return false;
        }
        columnListDepth -= 1;
      } else if (columnListDepth === 0 && token.toLowerCase() === "as") {
        break;
      }
      index += 1;
    }

    if (tokens[index]?.toLowerCase() !== "as") {
      return false;
    }

    index += 1;
    if (tokens[index]?.toLowerCase() === "not") {
      index += 1;
    }
    if (tokens[index]?.toLowerCase() === "materialized") {
      index += 1;
    }
    if (tokens[index] !== "(") {
      return false;
    }

    const bodyStart = index + 1;
    let depth = 1;
    index += 1;
    while (index < tokens.length && depth > 0) {
      if (tokens[index] === "(") {
        depth += 1;
      } else if (tokens[index] === ")") {
        depth -= 1;
      }
      index += 1;
    }

    if (depth !== 0) {
      return false;
    }

    if (!isReadOnlySqlTokens(tokens.slice(bodyStart, index - 1))) {
      return false;
    }

    if (tokens[index] === ",") {
      index += 1;
      continue;
    }

    return isReadOnlySqlTokens(tokens.slice(index));
  }

  return false;
}

function isReadOnlySelectTokens(tokens: string[]): boolean {
  let depth = 0;

  for (let index = 0; index < tokens.length; index += 1) {
    const token = tokens[index];
    if (token === "(") {
      depth += 1;
      continue;
    }
    if (token === ")") {
      depth = Math.max(0, depth - 1);
      continue;
    }
    if (depth > 0) {
      continue;
    }

    const keyword = token.toLowerCase();
    if (keyword === "into") {
      return false;
    }
    if (keyword === "for" && isSelectLockingClause(tokens, index)) {
      return false;
    }
    if (SQL_MUTATION_KEYWORDS.has(keyword)) {
      return false;
    }
  }

  return true;
}

function isSelectLockingClause(tokens: string[], index: number): boolean {
  const nextToken = tokens[index + 1]?.toLowerCase();
  if (nextToken === "update" || nextToken === "share") {
    return true;
  }

  if (
    nextToken === "no" &&
    tokens[index + 2]?.toLowerCase() === "key" &&
    tokens[index + 3]?.toLowerCase() === "update"
  ) {
    return true;
  }

  return nextToken === "key" && tokens[index + 2]?.toLowerCase() === "share";
}
