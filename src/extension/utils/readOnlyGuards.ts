import type { QueryEditorSqlDialect } from "../../shared/webviewContracts";
import type { ConnectionManager } from "../connectionManager";
import type {
  DriverCapabilities,
  ReadOnlyQueryDecision,
  ReadOnlyQueryGuard,
} from "../dbDrivers/types";
import { splitMySQLScript } from "./mysqlScriptSplit";
import {
  hasPostgresDollarQuoteTag,
  isAmbiguousMysqlDashComment,
  isMysqlDashCommentStart,
  oracleAlternativeQuoteEnd,
  readPostgresDollarQuoteTag,
  toPostgresWireText,
} from "./sqlStatementScan";

const ALLOWED_READ_ONLY_QUERY: ReadOnlyQueryDecision = { allowed: true };

const SQL_READ_ONLY_QUERY_REASON =
  "[RapiDB] Read-only SQL connections allow only read-only queries.";

export const ORACLE_READ_ONLY_ENFORCEMENT_MESSAGE =
  "[RapiDB] Read-only Oracle query editor execution is blocked: autonomous transactions can commit writes even inside SET TRANSACTION READ ONLY. Use database-enforced read-only permissions with a regular connection to run queries.";

const EXECUTABLE_MYSQL_COMMENT_SENTINEL =
  "\u0000__rapidb-executable-comment__\u0000";
const AMBIGUOUS_SQL_SYNTAX_SENTINEL = "\u0000__rapidb-ambiguous-syntax__\u0000";

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
  "index_list",
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

const SQL_EXPRESSION_CONTEXT_PREVIOUS_TOKENS = new Set([
  "where",
  "and",
  "or",
  "by",
  "having",
  "=",
  "<",
  ">",
  "+",
  "-",
  "/",
  "%",
]);

const SEMICOLON_SEPARATED_STATEMENT_DIALECTS: ReadonlySet<QueryEditorSqlDialect> =
  new Set(["postgresql", "mysql", "sqlite"]);

const SQL_EXPRESSION_OPERATOR_WORDS = new Set([
  "and",
  "or",
  "is",
  "isnull",
  "notnull",
  "in",
  "not",
  "between",
  "like",
  "ilike",
  "similar",
  "collate",
  "at",
  "escape",
  "glob",
  "regexp",
  "rlike",
  "div",
  "mod",
  "xor",
]);

const TRANSACTION_CONTROL_KEYWORDS: Readonly<
  Partial<Record<QueryEditorSqlDialect, ReadonlySet<string>>>
> = {
  postgresql: new Set([
    "abort",
    "begin",
    "commit",
    "end",
    "execute",
    "prepare",
    "release",
    "rollback",
    "savepoint",
    "set",
    "start",
  ]),
  mysql: new Set([
    "begin",
    "commit",
    "release",
    "rollback",
    "savepoint",
    "set",
    "start",
  ]),
};

const TRANSACTION_CONTROL_IDENTIFIER_PREVIOUS_TOKENS = new Set([
  "from",
  "join",
  "as",
  "on",
  "then",
  "else",
  "when",
  "character",
  "char",
  "describe",
  "desc",
  "table",
]);

const TRANSACTION_CONTROL_IDENTIFIER_NEXT_TOKENS = new Set([
  "asc",
  "desc",
  "nulls",
  "from",
  "as",
  "on",
  "where",
  "using",
  "then",
  "else",
  "when",
  "end",
  "group",
  "order",
  "limit",
  "offset",
  "union",
  "except",
  "intersect",
  "join",
  "inner",
  "left",
  "right",
  "full",
  "cross",
  "natural",
  "having",
  "window",
  "over",
  "filter",
]);

const POSTGRES_EXPLAIN_OPTIONS = new Set([
  "buffers",
  "costs",
  "format",
  "generic_plan",
  "memory",
  "serialize",
  "settings",
  "summary",
  "timing",
  "verbose",
  "wal",
]);

const TSQL_STATEMENT_KEYWORDS = new Set([
  "shutdown",
  "dbcc",
  "reconfigure",
  "deny",
  "waitfor",
  "enable",
  "disable",
  "writetext",
  "updatetext",
  "receive",
  "send",
  "move",
  "open",
  "close",
  "deallocate",
]);

const MYSQL_DELIMITER_DIRECTIVE = /(?:^|[\r\n])\s*DELIMITER\b/i;

const SQL_COLUMN_LIST_PREVIOUS_TOKENS = new Set([
  "select",
  "distinct",
  ",",
  "as",
]);

const SQL_COLUMN_LIST_NEXT_TOKENS = new Set(["from", ",", "as", ")"]);

const SELECT_INTO_STATEMENT_KEYWORDS = new Set([
  "select",
  "insert",
  "merge",
  "update",
  "delete",
]);

const SQL_MUTATING_FUNCTIONS = new Set([
  "nextval",
  "setval",
  "pg_terminate_backend",
  "pg_cancel_backend",
  "pg_reload_conf",
  "pg_notify",
  "pg_create_logical_replication_slot",
  "pg_create_physical_replication_slot",
  "pg_create_restore_point",
  "pg_drop_replication_slot",
  "pg_replication_slot_advance",
  "pg_logical_slot_get_changes",
  "pg_logical_slot_get_binary_changes",
  "pg_logical_emit_message",
  "pg_switch_wal",
  "pg_switch_xlog",
  "pg_stat_statements_reset",
  "pg_copy_logical_replication_slot",
  "pg_copy_physical_replication_slot",
  "pg_rotate_logfile",
  "pg_promote",
  "pg_wal_replay_pause",
  "pg_wal_replay_resume",
  "pg_xlog_replay_pause",
  "pg_xlog_replay_resume",
  "pg_backup_start",
  "pg_backup_stop",
  "pg_start_backup",
  "pg_stop_backup",
  "pg_log_backend_memory_contexts",
  "pg_advisory_lock",
  "pg_advisory_xact_lock",
  "pg_try_advisory_lock",
  "pg_try_advisory_xact_lock",
  "pg_advisory_unlock",
  "pg_advisory_unlock_all",
  "dblink",
  "dblink_exec",
  "dblink_connect",
  "dblink_disconnect",
  "dblink_send_query",
  "dblink_get_result",
  "set_config",
  "lo_create",
  "lo_creat",
  "lo_import",
  "lo_unlink",
  "lo_export",
  "lo_open",
  "lo_from_bytea",
  "lo_put",
  "lo_write",
  "lowrite",
  "lo_truncate",
  "lo_lseek",
]);

const SQL_MUTATING_FUNCTION_PREFIXES = [
  "dblink_",
  "pg_advisory_",
  "pg_stat_reset",
  "pg_replication_origin_",
];
const MYSQL_SESSION_MUTATING_FUNCTIONS = new Set([
  "get_lock",
  "release_lock",
  "release_all_locks",
]);

function isMutatingFunctionCall(
  keyword: string,
  nextToken: string | undefined,
): boolean {
  if (nextToken !== "(") {
    return false;
  }
  if (SQL_MUTATING_FUNCTIONS.has(keyword)) {
    return true;
  }
  return SQL_MUTATING_FUNCTION_PREFIXES.some((prefix) =>
    keyword.startsWith(prefix),
  );
}

function isSqlMutationKeywordAt(
  tokens: string[],
  index: number,
  dialect?: QueryEditorSqlDialect,
): boolean {
  const keyword = tokens[index]?.toLowerCase() ?? "";
  if (!SQL_MUTATION_KEYWORDS.has(keyword)) {
    return false;
  }
  if (keyword === "replace" && tokens[index + 1] === "(") {
    return false;
  }
  return dialect && SEMICOLON_SEPARATED_STATEMENT_DIALECTS.has(dialect)
    ? isNestedStatementStartPosition(tokens, index)
    : isSqlStatementStartPosition(tokens, index);
}

function isNestedStatementStartPosition(
  tokens: string[],
  index: number,
): boolean {
  if (index === 0) {
    return true;
  }
  if (tokens[index - 1] !== "(") {
    return false;
  }
  const next = tokens[index + 1]?.toLowerCase();
  return (
    next === undefined ||
    (/^[\p{L}\p{Nl}_(*@]/u.test(next) &&
      !SQL_EXPRESSION_OPERATOR_WORDS.has(next))
  );
}

function isSqlStatementStartPosition(tokens: string[], index: number): boolean {
  const previous = tokens[index - 1]?.toLowerCase();
  const next = tokens[index + 1]?.toLowerCase();
  if (previous === "." && /^[\p{L}\p{Nl}_]/u.test(tokens[index - 2] ?? "")) {
    return false;
  }
  if (previous && SQL_EXPRESSION_CONTEXT_PREVIOUS_TOKENS.has(previous)) {
    return false;
  }
  if (previous === "(" && (next === ")" || next === "," || next === "as")) {
    return false;
  }
  return !(
    previous &&
    SQL_COLUMN_LIST_PREVIOUS_TOKENS.has(previous) &&
    (next === undefined || SQL_COLUMN_LIST_NEXT_TOKENS.has(next))
  );
}

function isKnownMutatingFunctionName(
  keyword: string,
  dialect: QueryEditorSqlDialect,
): boolean {
  return (
    SQL_MUTATING_FUNCTIONS.has(keyword) ||
    SQL_MUTATING_FUNCTION_PREFIXES.some((prefix) =>
      keyword.startsWith(prefix),
    ) ||
    (dialect === "mysql" &&
      (MYSQL_SESSION_MUTATING_FUNCTIONS.has(keyword) ||
        keyword === "last_insert_id"))
  );
}

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

  if (connectionManager.getConnection?.(connectionId)?.type === "oracle") {
    return denyReadOnlyQuery(ORACLE_READ_ONLY_ENFORCEMENT_MESSAGE);
  }

  return resolveReadOnlyQueryGuard(connectionManager, connectionId)(queryText);
}

export function sqlReadOnlyQueryGuard(
  queryText: string,
  dialect: QueryEditorSqlDialect = "sql",
): ReadOnlyQueryDecision {
  if (dialect === "plsql") {
    return denyReadOnlyQuery(ORACLE_READ_ONLY_ENFORCEMENT_MESSAGE);
  }
  const statements = readOnlyClassifiedStatements(queryText, dialect);
  if (
    !statements ||
    (dialect === "mysql" &&
      !matchesMysqlDriverStatements(queryText, statements.length))
  ) {
    return denyReadOnlyQuery(SQL_READ_ONLY_QUERY_REASON);
  }
  return allowReadOnlyQuery();
}

function readOnlyClassifiedStatements(
  queryText: string,
  dialect: QueryEditorSqlDialect,
): string[] | undefined {
  const sanitized = sanitizeSqlForReadOnlyClassification(queryText, dialect);
  if (
    sanitized.includes(EXECUTABLE_MYSQL_COMMENT_SENTINEL) ||
    sanitized.includes(AMBIGUOUS_SQL_SYNTAX_SENTINEL)
  ) {
    return undefined;
  }
  const statements = sanitized
    .split(";")
    .map((statement) => statement.trim())
    .filter((statement) => statement.length > 0);

  return statements.length > 0 &&
    statements.every((statement) => isReadOnlySqlStatement(statement, dialect))
    ? statements
    : undefined;
}

function matchesMysqlDriverStatements(
  queryText: string,
  statementCount: number,
): boolean {
  let driverStatements: string[];
  try {
    driverStatements = splitMySQLScript(queryText);
  } catch {
    return false;
  }
  return (
    driverStatements.length === statementCount &&
    driverStatements.every(
      (statement) =>
        readOnlyClassifiedStatements(statement, "mysql")?.length === 1,
    )
  );
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

function sqlLineCommentEnd(
  queryText: string,
  index: number,
  dialect: QueryEditorSqlDialect,
): number | null | undefined {
  let start: number;
  if (queryText[index] === "#") {
    if (dialect === "sql") return null;
    if (dialect !== "mysql") return undefined;
    start = index + 1;
  } else if (queryText[index] === "-" && queryText[index + 1] === "-") {
    if (
      (dialect === "mysql" || dialect === "sql") &&
      !isMysqlDashCommentStart(queryText, index)
    ) {
      return dialect === "sql" || isAmbiguousMysqlDashComment(queryText, index)
        ? null
        : undefined;
    }
    start = index + 2;
  } else {
    return undefined;
  }
  const newline = queryText.indexOf("\n", start);
  const lineEnd = newline < 0 ? queryText.length : newline;
  if (dialect === "mysql" || dialect === "sqlite") return lineEnd;
  const carriageReturn = queryText.indexOf("\r", start);
  if (carriageReturn < 0 || carriageReturn >= lineEnd) return lineEnd;
  if (dialect === "postgresql") return carriageReturn;
  return carriageReturn + 1 === lineEnd ? carriageReturn : null;
}

function skipSqlTrivia(
  queryText: string,
  start: number,
  dialect: QueryEditorSqlDialect,
): number {
  let pos = start;
  while (pos < queryText.length) {
    const char = queryText[pos];
    const next = queryText[pos + 1];
    const lineCommentEnd = sqlLineCommentEnd(queryText, pos, dialect);
    if (lineCommentEnd === null) break;
    if (lineCommentEnd !== undefined) {
      pos = lineCommentEnd;
      continue;
    }
    if (char === "/" && next === "*") {
      pos += 2;
      while (pos < queryText.length) {
        if (queryText[pos] === "*" && queryText[pos + 1] === "/") {
          pos += 2;
          break;
        }
        pos += 1;
      }
      continue;
    }
    if (/\s/.test(char)) {
      pos += 1;
      continue;
    }
    break;
  }
  return pos;
}

function sanitizeSqlForReadOnlyClassification(
  rawQueryText: string,
  dialect: QueryEditorSqlDialect = "sql",
): string {
  if (dialect === "mysql" && MYSQL_DELIMITER_DIRECTIVE.test(rawQueryText)) {
    return AMBIGUOUS_SQL_SYNTAX_SENTINEL;
  }
  const queryText =
    dialect === "postgresql" ? toPostgresWireText(rawQueryText) : rawQueryText;
  let sanitized = "";

  for (let index = 0; index < queryText.length; ) {
    const char = queryText[index];
    const next = queryText[index + 1];

    if (dialect === "plsql") {
      try {
        const alternativeQuoteEnd = oracleAlternativeQuoteEnd(queryText, index);
        if (alternativeQuoteEnd !== undefined) {
          sanitized += " ";
          index = alternativeQuoteEnd;
          continue;
        }
      } catch {
        return AMBIGUOUS_SQL_SYNTAX_SENTINEL;
      }
    }

    const lineCommentEnd = sqlLineCommentEnd(queryText, index, dialect);
    if (lineCommentEnd === null) {
      return AMBIGUOUS_SQL_SYNTAX_SENTINEL;
    }
    if (lineCommentEnd !== undefined) {
      sanitized += " ";
      index = lineCommentEnd;
      continue;
    }

    if (char === "/" && next === "*") {
      if (
        dialect === "mysql" &&
        /^\/\*(?:!|M!)/.test(queryText.slice(index, index + 4))
      ) {
        sanitized += ` ${EXECUTABLE_MYSQL_COMMENT_SENTINEL} `;
      } else {
        sanitized += " ";
      }
      index += 2;
      let closed = false;
      while (index < queryText.length) {
        if (queryText[index] === "/" && queryText[index + 1] === "*") {
          return AMBIGUOUS_SQL_SYNTAX_SENTINEL;
        }
        if (queryText[index] === "*" && queryText[index + 1] === "/") {
          index += 2;
          closed = true;
          break;
        }
        index += 1;
      }
      if (!closed) {
        return AMBIGUOUS_SQL_SYNTAX_SENTINEL;
      }
      continue;
    }

    if (char === "'") {
      sanitized += " __rapidb_quoted_value__ ";
      index += 1;
      let closed = false;
      while (index < queryText.length) {
        if (
          supportsBackslashEscapedQuotes(dialect) &&
          queryText[index] === "\\" &&
          queryText[index + 1] === "'"
        ) {
          return AMBIGUOUS_SQL_SYNTAX_SENTINEL;
        }
        if (queryText[index] === "'") {
          if (queryText[index + 1] === "'") {
            index += 2;
            continue;
          }
          index += 1;
          closed = true;
          break;
        }
        index += 1;
      }
      if (!closed) {
        return AMBIGUOUS_SQL_SYNTAX_SENTINEL;
      }
      continue;
    }

    if (char === '"' || char === "`") {
      if (
        char === '"' &&
        index >= 2 &&
        queryText[index - 1] === "&" &&
        /[uU]/.test(queryText[index - 2])
      ) {
        return AMBIGUOUS_SQL_SYNTAX_SENTINEL;
      }
      const quote = char;
      index += 1;
      let inner = "";
      let closed = false;
      while (index < queryText.length) {
        if (
          dialect === "mysql" &&
          queryText[index] === "\\" &&
          queryText[index + 1] === quote
        ) {
          return AMBIGUOUS_SQL_SYNTAX_SENTINEL;
        }
        if (queryText[index] === quote) {
          if (queryText[index + 1] === quote) {
            inner += quote;
            index += 2;
            continue;
          }
          index += 1;
          closed = true;
          break;
        }
        inner += queryText[index];
        index += 1;
      }
      if (!closed) {
        return AMBIGUOUS_SQL_SYNTAX_SENTINEL;
      }
      const lowered = inner.toLowerCase();
      const lookahead = skipSqlTrivia(queryText, index, dialect);
      if (
        isKnownMutatingFunctionName(lowered, dialect) &&
        queryText[lookahead] === "("
      ) {
        sanitized += ` ${lowered} (`;
      } else if (dialect === "plsql") {
        sanitized +=
          inner === "NEXTVAL" ? " NEXTVAL " : " __rapidb_quoted_identifier__ ";
      } else if (dialect === "mysql") {
        sanitized += " __rapidb_quoted_value__ ";
      } else {
        sanitized += " __rapidb_quoted_identifier__ ";
      }
      continue;
    }

    if (char === "[" && (dialect === "transactsql" || dialect === "sqlite")) {
      index += 1;
      let inner = "";
      let closed = false;
      while (index < queryText.length) {
        if (queryText[index] === "]") {
          if (queryText[index + 1] === "]") {
            inner += "]";
            index += 2;
            continue;
          }
          index += 1;
          closed = true;
          break;
        }
        inner += queryText[index];
        index += 1;
      }
      if (!closed) {
        return AMBIGUOUS_SQL_SYNTAX_SENTINEL;
      }
      const lowered = inner.toLowerCase();
      const lookahead = skipSqlTrivia(queryText, index, dialect);
      if (
        isKnownMutatingFunctionName(lowered, dialect) &&
        queryText[lookahead] === "("
      ) {
        sanitized += ` ${lowered} (`;
      } else {
        sanitized += " __rapidb_quoted_identifier__ ";
      }
      continue;
    }

    const dollarQuoteTag =
      dialect === "postgresql"
        ? readPostgresDollarQuoteTag(queryText, index)
        : null;
    if (dollarQuoteTag) {
      sanitized += " __rapidb_quoted_value__ ";
      index += dollarQuoteTag.length;
      const closingIndex = queryText.indexOf(dollarQuoteTag, index);
      if (closingIndex === -1) {
        return AMBIGUOUS_SQL_SYNTAX_SENTINEL;
      }
      index = closingIndex + dollarQuoteTag.length;
      continue;
    }

    sanitized += char;
    index += 1;
  }

  return sanitized;
}

function supportsBackslashEscapedQuotes(
  dialect: QueryEditorSqlDialect,
): boolean {
  return dialect === "mysql" || dialect === "postgresql";
}

export function mayChangeDatabaseSchema(
  queryText: string,
  dialect: QueryEditorSqlDialect = "mysql",
): boolean {
  const sanitizedQuery = sanitizeSqlForReadOnlyClassification(
    queryText,
    dialect,
  );
  if (
    sanitizedQuery.includes(EXECUTABLE_MYSQL_COMMENT_SENTINEL) ||
    sanitizedQuery.includes(AMBIGUOUS_SQL_SYNTAX_SENTINEL)
  ) {
    return true;
  }
  if (
    /\b(?:CREATE|ALTER|DROP|TRUNCATE|RENAME|COMMENT|GRANT|REVOKE|ATTACH|DETACH|SP_RENAME)\b|\bIMPORT\s+FOREIGN\s+SCHEMA\b/i.test(
      sanitizedQuery,
    )
  ) {
    return true;
  }
  if (
    (dialect === "postgresql" || dialect === "transactsql") &&
    sanitizedQuery
      .split(";")
      .some((statement) =>
        createsTableFromSelect(tokenizeSql(statement, dialect)),
      )
  ) {
    return true;
  }

  return (
    /^\s*DO\b/i.test(sanitizedQuery) && hasPostgresDollarQuoteTag(queryText)
  );
}

function createsTableFromSelect(tokens: string[]): boolean {
  let statementKeyword: string | undefined;
  for (const token of tokens) {
    const keyword = token.toLowerCase();
    if (SELECT_INTO_STATEMENT_KEYWORDS.has(keyword)) {
      statementKeyword = keyword;
    } else if (keyword === "into" && statementKeyword === "select") {
      return true;
    }
  }
  return false;
}

function tokenizeSql(
  statement: string,
  dialect: QueryEditorSqlDialect,
): string[] {
  const pattern =
    dialect === "transactsql" || dialect === "plsql"
      ? /[\p{L}\p{Nl}_][\p{L}\p{Nl}\p{N}\p{M}_$#]*|[0-9]+(?:\.[0-9]*)?(?:[eE][+-]?[0-9]+)?|[(),.]|[^\s[\]]/gu
      : /[\p{L}\p{Nl}_][\p{L}\p{Nl}\p{N}\p{M}_$]*|[0-9]+(?:\.[0-9]*)?(?:[eE][+-]?[0-9]+)?|[(),.]|[^\s[\]]/gu;
  return statement.match(pattern) ?? [];
}

function isReadOnlySqlStatement(
  statement: string,
  dialect: QueryEditorSqlDialect,
): boolean {
  const tokens = tokenizeSql(statement, dialect);
  if (tokens.length === 0) {
    return false;
  }
  if (dialect === "mysql" && hasMysqlSessionSideEffect(statement, tokens)) {
    return false;
  }
  if (hasTransactionControlStatement(tokens, dialect)) {
    return false;
  }
  if (
    (dialect === "transactsql" || dialect === "mysql") &&
    hasSequenceAdvance(tokens, dialect)
  ) {
    return false;
  }
  if (dialect === "transactsql" && hasTsqlStatementStarter(tokens)) {
    return false;
  }

  if (isReadOnlySqlTokens(tokens, dialect)) {
    return true;
  }

  return isDialectReadOnlySqlStatement(statement, tokens, dialect);
}

function hasTransactionControlStatement(
  tokens: string[],
  dialect: QueryEditorSqlDialect,
): boolean {
  const keywords = TRANSACTION_CONTROL_KEYWORDS[dialect];
  if (keywords === undefined) {
    return false;
  }
  let caseDepth = 0;
  for (let index = 0; index < tokens.length; index += 1) {
    const keyword = tokens[index]?.toLowerCase() ?? "";
    const previous = tokens[index - 1]?.toLowerCase();
    if (keyword === "case" && previous !== "." && previous !== "as") {
      caseDepth += 1;
      continue;
    }
    if (
      keyword === "end" &&
      caseDepth > 0 &&
      previous !== "." &&
      previous !== "as"
    ) {
      caseDepth -= 1;
      continue;
    }
    if (
      index > 0 &&
      keywords.has(keyword) &&
      isTransactionControlStatementAt(tokens, index)
    ) {
      return true;
    }
  }
  return false;
}

function isTransactionControlStatementAt(
  tokens: string[],
  index: number,
): boolean {
  const keyword = tokens[index]?.toLowerCase();
  const previous = tokens[index - 1]?.toLowerCase();
  const next = tokens[index + 1]?.toLowerCase();
  if (keyword === "start") {
    return next === "transaction";
  }
  if (
    previous &&
    TRANSACTION_CONTROL_IDENTIFIER_PREVIOUS_TOKENS.has(previous)
  ) {
    return false;
  }
  if (
    next !== undefined &&
    (TRANSACTION_CONTROL_IDENTIFIER_NEXT_TOKENS.has(next) ||
      (SQL_EXPRESSION_OPERATOR_WORDS.has(next) && next !== "and") ||
      !/^[\p{L}\p{Nl}_(@]/u.test(next))
  ) {
    return false;
  }
  return isSqlStatementStartPosition(tokens, index);
}

function hasTsqlStatementStarter(tokens: string[]): boolean {
  return tokens.some(
    (token, index) =>
      index > 0 &&
      TSQL_STATEMENT_KEYWORDS.has(token.toLowerCase()) &&
      isSqlStatementStartPosition(tokens, index),
  );
}

function hasSequenceAdvance(
  tokens: string[],
  dialect: QueryEditorSqlDialect,
): boolean {
  for (let index = 0; index + 1 < tokens.length; index += 1) {
    if (
      tokens[index]?.toLowerCase() === "next" &&
      tokens[index + 1]?.toLowerCase() === "value" &&
      tokens[index + 2]?.toLowerCase() === "for"
    ) {
      return true;
    }
    if (
      dialect === "mysql" &&
      tokens[index] === "." &&
      tokens[index + 1]?.toLowerCase() === "nextval"
    ) {
      return true;
    }
  }
  return false;
}

function hasMysqlSessionSideEffect(
  statement: string,
  tokens: string[],
): boolean {
  if (statement.includes(":=")) {
    return true;
  }
  const lastInsertIdCall = /\blast_insert_id\s*\(/gi;
  while (true) {
    const lastInsertIdMatch = lastInsertIdCall.exec(statement);
    if (lastInsertIdMatch === null) {
      break;
    }
    let argumentStart = lastInsertIdMatch.index + lastInsertIdMatch[0].length;
    while (/\s/.test(statement[argumentStart] ?? "")) {
      argumentStart += 1;
    }
    if (statement[argumentStart] !== ")") {
      return true;
    }
  }
  for (let index = 0; index < tokens.length; index += 1) {
    const keyword = tokens[index]?.toLowerCase();
    if (
      MYSQL_SESSION_MUTATING_FUNCTIONS.has(keyword ?? "") &&
      tokens[index + 1] === "("
    ) {
      return true;
    }
  }
  return false;
}

export function isReadOnlySqlTokens(
  tokens: string[],
  dialect?: QueryEditorSqlDialect,
): boolean {
  if (tokens.length === 0) {
    return false;
  }

  const firstToken = tokens[0]?.toLowerCase();
  if (firstToken === "select") {
    return isReadOnlySelectTokens(tokens, dialect);
  }

  if (firstToken === "with") {
    return isReadOnlyCteTokens(tokens, dialect);
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
      return (
        dialect === "postgresql" &&
        tokens.length >= 2 &&
        isReadOnlySelectTokens(tokens, dialect)
      );
    case "values":
      return isReadOnlyValuesTokens(tokens, dialect);
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
  if (
    secondToken === "select" ||
    secondToken === "with" ||
    secondToken === "analyze" ||
    secondToken === "format"
  ) {
    return isReadOnlyExplainStatement(tokens, dialect);
  }

  return true;
}

function isReadOnlyValuesTokens(
  tokens: string[],
  dialect: QueryEditorSqlDialect,
): boolean {
  if (tokens.length < 2 || tokens[1] !== "(") {
    return false;
  }

  let depth = 0;
  for (let index = 0; index < tokens.length; index += 1) {
    const token = tokens[index];
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

    const keyword = token.toLowerCase();
    if (isSqlMutationKeywordAt(tokens, index, dialect)) {
      return false;
    }
    if (isMutatingFunctionCall(keyword, tokens[index + 1])) {
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

  if (dialect === "postgresql" && tokens[index] === "(") {
    const optionsEnd = postgresExplainOptionsEnd(tokens, index + 1);
    return (
      optionsEnd !== undefined &&
      isReadOnlySqlTokens(tokens.slice(optionsEnd), dialect)
    );
  }

  while (index < tokens.length) {
    const keyword = tokens[index]?.toLowerCase();
    if (!keyword) {
      return false;
    }

    if (keyword === "analyze" || keyword === "plan") {
      return false;
    }

    if (keyword === "verbose" && dialect === "postgresql") {
      index += 1;
      continue;
    }

    if (keyword === "format") {
      index += 1;
      while (index < tokens.length && tokens[index] !== "(") {
        const nextKeyword = tokens[index]?.toLowerCase();
        if (nextKeyword === "into") {
          return false;
        }
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

    return isReadOnlySqlTokens(tokens.slice(index), dialect);
  }

  return false;
}

function postgresExplainOptionsEnd(
  tokens: string[],
  start: number,
): number | undefined {
  let index = start;
  while (true) {
    const option = tokens[index]?.toLowerCase();
    if (!option || !POSTGRES_EXPLAIN_OPTIONS.has(option)) {
      return undefined;
    }
    index += 1;
    const value = tokens[index];
    if (value !== undefined && value !== "," && value !== ")") {
      if (!/^[\p{L}\p{N}_]/u.test(value)) {
        return undefined;
      }
      index += 1;
    }
    if (tokens[index] === ",") {
      index += 1;
      continue;
    }
    return tokens[index] === ")" ? index + 1 : undefined;
  }
}

function isReadOnlySqlitePragma(statement: string, tokens: string[]): boolean {
  if (tokens.length < 2) {
    return false;
  }

  const normalizedStatement = statement.toLowerCase().trim();
  if (normalizedStatement.includes("=")) {
    return false;
  }

  const nameIndex = tokens[2] === "." ? 3 : 1;
  const pragmaName = tokens[nameIndex]?.toLowerCase();
  if (!pragmaName || !SQLITE_READ_ONLY_PRAGMAS.has(pragmaName)) {
    return false;
  }

  const nextToken = tokens[nameIndex + 1];
  if (nextToken !== undefined && nextToken !== "(") {
    return false;
  }
  const hasPragmaArguments = nextToken === "(";

  if (
    hasPragmaArguments &&
    !SQLITE_PRAGMA_ARGUMENT_SAFE_ALLOWLIST.has(pragmaName)
  ) {
    return false;
  }

  return true;
}

function isReadOnlyCteTokens(
  tokens: string[],
  dialect?: QueryEditorSqlDialect,
): boolean {
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

    if (!isReadOnlySqlTokens(tokens.slice(bodyStart, index - 1), dialect)) {
      return false;
    }

    if (tokens[index] === ",") {
      index += 1;
      continue;
    }

    return isReadOnlySqlTokens(tokens.slice(index), dialect);
  }

  return false;
}

function isReadOnlySelectTokens(
  tokens: string[],
  dialect?: QueryEditorSqlDialect,
): boolean {
  for (let index = 0; index < tokens.length; index += 1) {
    const token = tokens[index];
    if (token === "(" || token === ")" || token === ",") {
      continue;
    }

    const keyword = token.toLowerCase();
    if (keyword === "into") {
      return false;
    }
    if (keyword === "for" && isSelectLockingClause(tokens, index)) {
      return false;
    }
    if (keyword === "lock" && isSelectLockingClause(tokens, index)) {
      return false;
    }
    if (isSqlMutationKeywordAt(tokens, index, dialect)) {
      return false;
    }
    if (isMutatingFunctionCall(keyword, tokens[index + 1])) {
      return false;
    }
  }

  return true;
}

function isSelectLockingClause(tokens: string[], index: number): boolean {
  const nextToken = tokens[index + 1]?.toLowerCase();
  if (tokens[index]?.toLowerCase() === "lock") {
    return (
      nextToken === "in" &&
      tokens[index + 2]?.toLowerCase() === "share" &&
      tokens[index + 3]?.toLowerCase() === "mode"
    );
  }
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
