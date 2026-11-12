import type { DdlOnlyDbObjectKind } from "../../shared/dbObjectKinds";
import { normalizeNumericToken } from "../../shared/numericNormalization";
import type { QueryResultSet } from "../../shared/webviewContracts";
import type { ConnectionConfig } from "../connectionManager";
import { BaseDBDriver } from "./BaseDBDriver";
import { queryCollectionLimit } from "./boundedQueryRows";
import { literalContainsPattern } from "./literalContains";
import { MutationNotExecutedError } from "./mutationExecutionState";
import { openSQLiteDatabase, type SQLiteDatabase } from "./sqliteRuntime";
import {
  type DriverTimeoutSettingsProvider,
  throwIfTransactionCancelled,
} from "./timeout";
import {
  TransactionIdentityStore,
  TransactionVerificationError,
  verifyTransaction,
} from "./transactionVerification";
import type {
  ColumnMeta,
  ColumnTypeMeta,
  DatabaseInfo,
  DriverEntityManifest,
  FilterConditionResult,
  FilterOperator,
  GeneratedKind,
  PersistedEditCheckOptions,
  PersistedEditCheckResult,
  QueryExecutionOptions,
  QueryResult,
  SchemaInfo,
  TableInfo,
  TypeCategory,
  ValueSemantics,
} from "./types";
import { assertTransactionAffectedRows } from "./types";

const SQLITE_ENTITY_MANIFEST: DriverEntityManifest = {
  dbObjectKinds: ["table", "view"],
  tableSections: {
    columns: "supported",
    constraints: "supported",
    indexes: "supported",
    triggers: "supported",
  },
  tableSectionOverridesByObjectKind: {
    view: {
      constraints: "not_applicable",
      indexes: "not_applicable",
    },
  },
};

type SqlStatementKind = "select" | "dml";
interface SQLiteTableXInfoRow {
  name: string;
  type: string;
  notnull: number;
  dflt_value: string | null;
  pk: number;
  hidden: number;
}
interface SQLiteGeneratedColumnDetail {
  expression?: string;
  generatedKind?: GeneratedKind;
}
interface SQLiteDatabaseListRow {
  seq: number;
  name: string;
  file: string | null;
}
interface SQLiteTableMetadata {
  rows: SQLiteTableXInfoRow[];
  foreignKeyColumns: Set<string>;
  autoIncrementColumns: Set<string>;
  generatedColumns: Map<string, SQLiteGeneratedColumnDetail>;
}
const SQLITE_SELECT_STARTERS = new Set([
  "SELECT",
  "PRAGMA",
  "EXPLAIN",
  "VALUES",
]);
const SQLITE_MAX_RETAINED_RESULT_SETS = 100;
const SQLITE_MAX_RETAINED_RESULT_COLUMNS = 4096;
const SQLITE_DML_STARTERS = new Set(["INSERT", "UPDATE", "DELETE", "REPLACE"]);
const SQLITE_TIME_LITERAL_RE =
  /^(\d{2}):(\d{2})(?::(\d{2})(?:\.\d+)?)?(?:\s*(Z|[+-](\d{2}):(\d{2})))?$/i;
const SQLITE_DATETIME_LITERAL_RE =
  /^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?(?: ?(?:Z|[+-]\d{2}:\d{2}))?$/i;
const SQLITE_INT64_MIN = -(1n << 63n);
const SQLITE_INT64_MAX = (1n << 63n) - 1n;
const SQLITE_JS_SAFE_INTEGER_MIN = BigInt(Number.MIN_SAFE_INTEGER);
const SQLITE_JS_SAFE_INTEGER_MAX = BigInt(Number.MAX_SAFE_INTEGER);

function exactSqliteIntegerFilterToken(value: string): string {
  const match = /^([+-]?)(\d*)(?:\.(\d*))?(?:[eE]([+-]?\d+))?$/.exec(value);
  if (!match || !(match[2] || match[3])) return value;
  const digits = `${match[2]}${match[3] ?? ""}`.replace(/^0+/, "");
  if (!digits) return "0";
  const exponent = Number(match[4] ?? "0");
  const shift = exponent - (match[3]?.length ?? 0);
  if (!Number.isSafeInteger(shift)) return value;
  let integer: string;
  if (shift >= 0) {
    if (digits.length + shift > 19) return value;
    integer = digits + "0".repeat(shift);
  } else {
    const cut = digits.length + shift;
    if (cut <= 0 || /[1-9]/.test(digits.slice(cut))) return value;
    integer = digits.slice(0, cut);
    if (integer.length > 19) return value;
  }
  const signed = `${match[1] === "-" ? "-" : ""}${integer}`;
  const number = BigInt(signed);
  return number >= SQLITE_INT64_MIN && number <= SQLITE_INT64_MAX
    ? number.toString()
    : value;
}

function normalizeSqliteIntegerTrailingDot(value: string): string {
  return value.replace(/^(\s*[+-]?\d+)\.(?=([eE][+-]?\d+)?\s*$)/, "$1");
}
function findClosingSqliteParen(sql: string, openIndex: number): number {
  let depth = 0;
  let quote: "'" | '"' | "`" | "]" | null = null;
  let lineComment = false;
  let blockComment = false;
  for (let index = openIndex; index < sql.length; index += 1) {
    const char = sql[index];
    const next = sql[index + 1];
    if (lineComment) {
      if (char === "\n" || char === "\r") lineComment = false;
      continue;
    }
    if (blockComment) {
      if (char === "*" && next === "/") {
        blockComment = false;
        index += 1;
      }
      continue;
    }
    if (quote) {
      if (char === quote && next === quote) index += 1;
      else if (char === quote) quote = null;
      continue;
    }
    if (char === "-" && next === "-") {
      lineComment = true;
      index += 1;
      continue;
    }
    if (char === "/" && next === "*") {
      blockComment = true;
      index += 1;
      continue;
    }
    if (char === "'" || char === '"' || char === "`") {
      quote = char;
      continue;
    }
    if (char === "[") {
      quote = "]";
      continue;
    }
    if (char === "(") depth += 1;
    else if (char === ")" && --depth === 0) return index;
  }
  return -1;
}

function extractSqliteCheckConstraints(
  sql: string,
): Array<{ name?: string; expression: string }> {
  const checks: Array<{ name?: string; expression: string }> = [];
  let quote: "'" | '"' | "`" | "]" | null = null;
  let lineComment = false;
  let blockComment = false;
  for (let index = 0; index < sql.length; index += 1) {
    const char = sql[index];
    const next = sql[index + 1];
    if (lineComment) {
      if (char === "\n" || char === "\r") lineComment = false;
      continue;
    }
    if (blockComment) {
      if (char === "*" && next === "/") {
        blockComment = false;
        index += 1;
      }
      continue;
    }
    if (quote) {
      if (char === quote && next === quote) index += 1;
      else if (char === quote) quote = null;
      continue;
    }
    if (char === "-" && next === "-") {
      lineComment = true;
      index += 1;
      continue;
    }
    if (char === "/" && next === "*") {
      blockComment = true;
      index += 1;
      continue;
    }
    if (char === "'" || char === '"' || char === "`") {
      quote = char;
      continue;
    }
    if (char === "[") {
      quote = "]";
      continue;
    }
    if (
      sql.slice(index, index + 5).toUpperCase() !== "CHECK" ||
      /[\w$]/.test(sql[index - 1] ?? "") ||
      /[\w$]/.test(sql[index + 5] ?? "")
    ) {
      continue;
    }
    const openIndex = sql.indexOf("(", index + 5);
    if (openIndex < 0 || sql.slice(index + 5, openIndex).trim() !== "")
      continue;
    const closeIndex = findClosingSqliteParen(sql, openIndex);
    if (closeIndex < 0) continue;

    const prefix = sql.slice(0, index);
    const nameMatch =
      /\bCONSTRAINT\s+("(?:[^"]|"")+"|`(?:[^`]|``)+`|\[(?:[^\]]|\]\])+\]|[^\s(),]+)\s*$/i.exec(
        prefix,
      );
    const rawName = nameMatch?.[1];
    const name = rawName
      ? (rawName[0] === '"' || rawName[0] === "`" || rawName[0] === "["
          ? rawName.slice(1, -1)
          : rawName
        )
          .replace(/""/g, '"')
          .replace(/``/g, "`")
          .replace(/\]\]/g, "]")
      : undefined;
    checks.push({
      ...(name ? { name } : {}),
      expression: sql.slice(openIndex + 1, closeIndex).trim(),
    });
    index = closeIndex;
  }
  return checks;
}

function approximateNumericFilterTolerance(rawValue: string): number {
  const fraction = /\.(\d+)/.exec(rawValue)?.[1].length ?? 0;
  const precision = Math.min(Math.max(fraction + 2, 6), 12);
  return 10 ** -precision;
}
function sqliteDeclaredTypeBase(typeName: string): string {
  return typeName.toUpperCase().trim().split("(")[0].trim();
}
function classifySql(sql: string): SqlStatementKind {
  const start = skipSqlTrivia(sql, 0);
  const leadingKeyword = readSqlKeyword(sql, start);
  if (!leadingKeyword) return "dml";
  if (leadingKeyword.keyword === "WITH") {
    const mainKeyword = sqliteWithMainKeyword(sql, leadingKeyword.next);
    return mainKeyword && SQLITE_SELECT_STARTERS.has(mainKeyword)
      ? "select"
      : "dml";
  }
  return SQLITE_SELECT_STARTERS.has(leadingKeyword.keyword) ? "select" : "dml";
}
function isSQLiteDmlStatement(sql: string): boolean {
  const leadingKeyword = readSqlKeyword(sql, 0);
  const keyword =
    leadingKeyword?.keyword === "WITH"
      ? sqliteWithMainKeyword(sql, leadingKeyword.next)
      : leadingKeyword?.keyword;
  return keyword !== undefined && SQLITE_DML_STARTERS.has(keyword);
}
// Statement boundaries follow SQLite's sqlite3_complete() state machine:
// a CREATE [TEMP] TRIGGER statement only ends at a `;` that directly follows
// `; END` (whitespace and comments aside), so `;` inside the trigger body,
// CASE ... END and columns named END/BEGIN do not split it.
type SQLiteScanState =
  | "start"
  | "normal"
  | "explain"
  | "create"
  | "trigger"
  | "semi"
  | "end";
type SQLiteScanToken =
  | "semi"
  | "other"
  | "explain"
  | "create"
  | "temp"
  | "trigger"
  | "end";
// Mirrors trans[][] in SQLite's complete.c (whitespace/comments: no change).
function nextSQLiteScanState(
  state: SQLiteScanState,
  token: SQLiteScanToken,
): SQLiteScanState {
  if (token === "semi") {
    return state === "trigger" || state === "semi" ? "semi" : "start";
  }
  switch (state) {
    case "start":
      return token === "explain" || token === "create" ? token : "normal";
    case "explain":
      if (token === "other") return "explain";
      return token === "create" ? "create" : "normal";
    case "create":
      if (token === "temp") return "create";
      return token === "trigger" ? "trigger" : "normal";
    case "normal":
      return "normal";
    case "semi":
      return token === "end" ? "end" : "trigger";
    case "trigger":
    case "end":
      return "trigger";
  }
}
const SQLITE_WORD_CHAR_RE = /[A-Za-z0-9_$\u0080-\uffff]/;
// tokenize.c's CC_SPACE, not JavaScript whitespace (e.g. NBSP is an ID char).
const SQLITE_SPACE_RE = /[\t\n\f\r ]/;
// Trim leading BOM trivia for keyword checks, but never a BOM ending an ID.
const SQLITE_TRIM_TRIVIA_RE = /^[\t\n\f\r \uFEFF]+|[\t\n\f\r ]+$/g;
function sqliteScanKeywordToken(word: string): SQLiteScanToken {
  switch (word.toUpperCase()) {
    case "EXPLAIN":
      return "explain";
    case "CREATE":
      return "create";
    case "TEMP":
    case "TEMPORARY":
      return "temp";
    case "TRIGGER":
      return "trigger";
    case "END":
      return "end";
    default:
      return "other";
  }
}
export function splitSQLiteScript(sql: string): string[] {
  const stmts: string[] = [];
  let buf = "";
  let i = 0;
  const n = sql.length;
  let state: SQLiteScanState = "start";
  const advance = (token: SQLiteScanToken) => {
    state = nextSQLiteScanState(state, token);
  };
  const keepsComments = () =>
    state === "trigger" || state === "semi" || state === "end";
  while (i < n) {
    if (sql[i] === "-" && sql[i + 1] === "-") {
      const commentStart = i;
      while (i < n && sql[i] !== "\n") i++;
      buf += keepsComments() ? sql.slice(commentStart, i) : " ";
      continue;
    }
    if (sql[i] === "/" && sql[i + 1] === "*") {
      const commentStart = i;
      i += 2;
      while (i < n) {
        if (sql[i] === "*" && sql[i + 1] === "/") {
          i += 2;
          break;
        }
        i++;
      }
      buf += keepsComments() ? sql.slice(commentStart, i) : " ";
      continue;
    }
    if (sql[i] === "'" || sql[i] === '"' || sql[i] === "`" || sql[i] === "[") {
      const q = sql[i];
      const closing = q === "[" ? "]" : q;
      buf += sql[i++];
      while (i < n) {
        const c = sql[i];
        buf += c;
        i++;
        if (c === closing) {
          if (closing !== "]" && sql[i] === closing) {
            buf += sql[i++];
          } else {
            break;
          }
        }
      }
      advance("other");
      continue;
    }
    if (sql[i] === ";") {
      i++;
      advance("semi");
      if (state !== "start") {
        buf += ";";
        continue;
      }
      const stmt = buf.replace(SQLITE_TRIM_TRIVIA_RE, "");
      if (stmt) stmts.push(stmt);
      buf = "";
      continue;
    }
    // tokenize.c's CC_BOM is TK_SPACE only at the start of a token.
    // Keep FEFF in the word loop below: IdChar consumes it inside identifiers.
    if (sql[i] === "\uFEFF") {
      buf += sql[i++];
      continue;
    }
    if (SQLITE_WORD_CHAR_RE.test(sql[i])) {
      const wordStart = i;
      while (i < n && SQLITE_WORD_CHAR_RE.test(sql[i])) i++;
      const word = sql.slice(wordStart, i);
      buf += word;
      advance(sqliteScanKeywordToken(word));
      continue;
    }
    const current = sql[i++];
    buf += current;
    if (!SQLITE_SPACE_RE.test(current)) advance("other");
  }
  const tail = buf.replace(SQLITE_TRIM_TRIVIA_RE, "");
  if (tail) stmts.push(tail);
  return stmts;
}
function skipSqlTrivia(sql: string, start: number): number {
  let index = start;
  while (index < sql.length) {
    if (/\s/.test(sql[index])) {
      index++;
      continue;
    }
    if (sql[index] === "-" && sql[index + 1] === "-") {
      index += 2;
      while (index < sql.length && sql[index] !== "\n") {
        index++;
      }
      continue;
    }
    if (sql[index] === "/" && sql[index + 1] === "*") {
      index += 2;
      while (index < sql.length) {
        if (sql[index] === "*" && sql[index + 1] === "/") {
          index += 2;
          break;
        }
        index++;
      }
      continue;
    }
    break;
  }
  return index;
}
function readSqlKeyword(
  sql: string,
  start: number,
): {
  keyword: string;
  next: number;
} | null {
  const index = skipSqlTrivia(sql, start);
  const match = /^[A-Za-z_][A-Za-z0-9_]*/.exec(sql.slice(index));
  if (!match) return null;
  return {
    keyword: match[0].toUpperCase(),
    next: index + match[0].length,
  };
}
function nextSqlKeyword(
  sql: string,
  start: number,
): {
  keyword: string;
  next: number;
} | null {
  let index = start;
  while (index < sql.length) {
    index = skipSqlTrivia(sql, index);
    if (index >= sql.length) return null;
    const current = sql[index];
    if (
      current === '"' ||
      current === "'" ||
      current === "`" ||
      current === "["
    ) {
      index = skipQuotedSql(sql, index);
      continue;
    }
    const keyword = readSqlKeyword(sql, index);
    if (keyword) return keyword;
    index++;
  }
  return null;
}
function skipQuotedSql(sql: string, start: number): number {
  const opener = sql[start];
  const closer = opener === "[" ? "]" : opener;
  let index = start + 1;
  while (index < sql.length) {
    const current = sql[index];
    if (current === closer) {
      if (closer !== "]" && sql[index + 1] === closer) {
        index += 2;
        continue;
      }
      return index + 1;
    }
    index++;
  }
  return sql.length;
}
function skipBalancedParens(sql: string, start: number): number {
  if (sql[start] !== "(") return start;
  let depth = 0;
  let index = start;
  while (index < sql.length) {
    const current = sql[index];
    if (
      current === "'" ||
      current === '"' ||
      current === "`" ||
      current === "["
    ) {
      index = skipQuotedSql(sql, index);
      continue;
    }
    if (current === "-" && sql[index + 1] === "-") {
      index = skipSqlTrivia(sql, index);
      continue;
    }
    if (current === "/" && sql[index + 1] === "*") {
      index = skipSqlTrivia(sql, index);
      continue;
    }
    if (current === "(") {
      depth++;
    } else if (current === ")") {
      depth--;
      if (depth === 0) {
        return index + 1;
      }
    }
    index++;
  }
  return sql.length;
}
function skipSqlIdentifier(sql: string, start: number): number {
  const index = skipSqlTrivia(sql, start);
  const current = sql[index];
  if (
    current === '"' ||
    current === "'" ||
    current === "`" ||
    current === "["
  ) {
    return skipQuotedSql(sql, index);
  }
  const match = /^[A-Za-z_][A-Za-z0-9_]*/.exec(sql.slice(index));
  return match ? index + match[0].length : index;
}
function parseSqliteTriggerHeader(
  sql: string,
): Pick<import("./types").TriggerMeta, "timing" | "events"> {
  const unknown = () => ({
    timing: "unknown" as const,
    events: ["unknown" as const],
  });
  let token = readSqlKeyword(sql, 0);
  if (token?.keyword !== "CREATE") return unknown();
  token = readSqlKeyword(sql, token.next);
  if (token?.keyword === "TEMP" || token?.keyword === "TEMPORARY")
    token = readSqlKeyword(sql, token.next);
  if (token?.keyword !== "TRIGGER") return unknown();
  let index = token.next;
  const ifToken = readSqlKeyword(sql, index);
  const notToken = ifToken && readSqlKeyword(sql, ifToken.next);
  const existsToken = notToken && readSqlKeyword(sql, notToken.next);
  if (
    ifToken?.keyword === "IF" &&
    notToken?.keyword === "NOT" &&
    existsToken?.keyword === "EXISTS"
  )
    index = existsToken.next;
  const skipName = (start: number) => {
    const nameStart = skipSqlTrivia(sql, start);
    if (/["'`[]/.test(sql[nameStart] ?? "")) {
      return skipQuotedSql(sql, nameStart);
    }
    const match = /^[A-Za-z_\u0080-\uffff][A-Za-z0-9_$\u0080-\uffff]*/.exec(
      sql.slice(nameStart),
    );
    return nameStart + (match?.[0].length ?? 0);
  };
  index = skipName(index);
  const dot = skipSqlTrivia(sql, index);
  if (sql[dot] === ".") index = skipName(dot + 1);
  let timing: import("./types").TriggerMeta["timing"] = "before";
  token = readSqlKeyword(sql, index);
  if (token?.keyword === "BEFORE" || token?.keyword === "AFTER") {
    timing = token.keyword === "BEFORE" ? "before" : "after";
    token = readSqlKeyword(sql, token.next);
  } else if (token?.keyword === "INSTEAD") {
    token = readSqlKeyword(sql, token.next);
    if (token?.keyword !== "OF") return unknown();
    timing = "instead_of";
    token = readSqlKeyword(sql, token.next);
  }
  const event = token?.keyword.toLowerCase();
  if (event !== "delete" && event !== "insert" && event !== "update")
    return unknown();
  return { timing, events: [event] };
}
function sqliteColumnDefinitionName(definition: string): string | null {
  const trimmed = definition.trim();
  if (!trimmed) {
    return null;
  }
  if (/^(?:constraint|primary|unique|check|foreign)\b/i.test(trimmed)) {
    return null;
  }
  const first = trimmed[0];
  if (first === '"' || first === "`" || first === "[") {
    const closer = first === "[" ? "]" : first;
    let value = "";
    let index = 1;
    while (index < trimmed.length) {
      const current = trimmed[index];
      const next = trimmed[index + 1];
      if (current === closer) {
        if (closer !== "]" && next === closer) {
          value += closer;
          index += 2;
          continue;
        }
        return value;
      }
      value += current;
      index++;
    }
    return null;
  }
  const match = /^[A-Za-z_\u0080-\uffff][A-Za-z0-9_$\u0080-\uffff]*/.exec(
    trimmed,
  );
  return match?.[0] ?? null;
}
function sqliteAsciiLower(value: string): string {
  return value.replace(/[A-Z]/g, (char) => char.toLowerCase());
}
function stripSqliteComments(sql: string): string {
  let stripped = "";
  let index = 0;
  while (index < sql.length) {
    const current = sql[index];
    if (
      current === '"' ||
      current === "'" ||
      current === "`" ||
      current === "["
    ) {
      const end = skipQuotedSql(sql, index);
      stripped += sql.slice(index, end);
      index = end;
      continue;
    }
    if (
      (current === "-" && sql[index + 1] === "-") ||
      (current === "/" && sql[index + 1] === "*")
    ) {
      index = skipSqlTrivia(sql, index);
      stripped += " ";
      continue;
    }
    stripped += current;
    index++;
  }
  return stripped;
}
function sqliteSplitCreateDefinitions(rawCreateSql: string): string[] {
  const createSql = stripSqliteComments(rawCreateSql);
  let openParenIndex = -1;
  for (let index = 0; index < createSql.length; ) {
    const current = createSql[index];
    if (
      current === '"' ||
      current === "'" ||
      current === "`" ||
      current === "["
    ) {
      index = skipQuotedSql(createSql, index);
      continue;
    }
    if (current === "(") {
      openParenIndex = index;
      break;
    }
    index++;
  }
  if (openParenIndex < 0) {
    return [];
  }
  const closeParenIndex = skipBalancedParens(createSql, openParenIndex) - 1;
  if (closeParenIndex <= openParenIndex) {
    return [];
  }
  const body = createSql.slice(openParenIndex + 1, closeParenIndex);
  const definitions: string[] = [];
  let depth = 0;
  let start = 0;
  let index = 0;
  while (index < body.length) {
    const current = body[index];
    if (
      current === '"' ||
      current === "'" ||
      current === "`" ||
      current === "["
    ) {
      index = skipQuotedSql(body, index);
      continue;
    }
    if (current === "(") {
      depth++;
      index++;
      continue;
    }
    if (current === ")") {
      depth = Math.max(0, depth - 1);
      index++;
      continue;
    }
    if (current === "," && depth === 0) {
      const definition = body.slice(start, index).trim();
      if (definition) {
        definitions.push(definition);
      }
      start = index + 1;
    }
    index++;
  }
  const tail = body.slice(start).trim();
  if (tail) {
    definitions.push(tail);
  }
  return definitions;
}
function sqliteGeneratedKindFromHidden(
  hidden: number,
): GeneratedKind | undefined {
  if (hidden === 2) {
    return "virtual";
  }
  if (hidden === 3) {
    return "stored";
  }
  return undefined;
}
function parseSqliteGeneratedColumns(
  createSql: string,
): Map<string, SQLiteGeneratedColumnDetail> {
  const details = new Map<string, SQLiteGeneratedColumnDetail>();
  for (const definition of sqliteSplitCreateDefinitions(createSql)) {
    const columnName = sqliteColumnDefinitionName(definition);
    if (!columnName) {
      continue;
    }
    const asMatch = /\bAS\s*\(/i.exec(definition);
    if (!asMatch) {
      continue;
    }
    const openParenIndex = definition.indexOf("(", asMatch.index);
    if (openParenIndex < 0) {
      continue;
    }
    const closeParenIndex = skipBalancedParens(definition, openParenIndex) - 1;
    if (closeParenIndex <= openParenIndex) {
      continue;
    }
    const expression = definition
      .slice(openParenIndex + 1, closeParenIndex)
      .trim();
    const suffix = definition.slice(closeParenIndex + 1);
    details.set(sqliteAsciiLower(columnName), {
      expression: expression || undefined,
      generatedKind: /\bSTORED\b/i.test(suffix)
        ? "stored"
        : /\bVIRTUAL\b/i.test(suffix)
          ? "virtual"
          : undefined,
    });
  }
  return details;
}
function sqliteWithMainKeyword(sql: string, start: number): string | undefined {
  let index = skipSqlTrivia(sql, start);
  const maybeRecursive = readSqlKeyword(sql, index);
  if (maybeRecursive?.keyword === "RECURSIVE") {
    index = maybeRecursive.next;
  }
  while (index < sql.length) {
    index = skipSqlIdentifier(sql, index);
    if (index >= sql.length) return undefined;
    index = skipSqlTrivia(sql, index);
    if (sql[index] === "(") {
      index = skipBalancedParens(sql, index);
      index = skipSqlTrivia(sql, index);
    }
    const asKeyword = readSqlKeyword(sql, index);
    if (asKeyword?.keyword !== "AS") return undefined;
    index = skipSqlTrivia(sql, asKeyword.next);
    const materializedKeyword = readSqlKeyword(sql, index);
    if (materializedKeyword?.keyword === "NOT") {
      const nextKeyword = readSqlKeyword(sql, materializedKeyword.next);
      if (nextKeyword?.keyword === "MATERIALIZED") {
        index = skipSqlTrivia(sql, nextKeyword.next);
      }
    } else if (materializedKeyword?.keyword === "MATERIALIZED") {
      index = skipSqlTrivia(sql, materializedKeyword.next);
    }
    if (sql[index] !== "(") return undefined;
    index = skipBalancedParens(sql, index);
    index = skipSqlTrivia(sql, index);
    if (sql[index] === ",") {
      index++;
      continue;
    }
    break;
  }
  return readSqlKeyword(sql, index)?.keyword;
}
function canSQLiteStatementReturnRows(sql: string): boolean {
  let keyword = nextSqlKeyword(sql, 0);
  while (keyword) {
    if (keyword.keyword === "RETURNING") {
      return true;
    }
    keyword = nextSqlKeyword(sql, keyword.next);
  }
  return false;
}
function normalizeSqliteTimeLiteral(value: string): string | null {
  const trimmed = value.trim();
  const match = SQLITE_TIME_LITERAL_RE.exec(trimmed);
  if (!match) return null;
  const hours = Number(match[1]);
  const minutes = Number(match[2]);
  const seconds = Number(match[3] ?? "00");
  if (
    hours > 24 ||
    minutes > 59 ||
    seconds > 59 ||
    Number(match[5] ?? 0) > 14 ||
    Number(match[6] ?? 0) > 59
  )
    return null;
  return match[3] ? trimmed : `${trimmed.slice(0, 5)}:00${trimmed.slice(5)}`;
}
function sqliteTimeKeyExpression(column: string): string {
  return `COALESCE(RAPIDB_TIME_KEY(${column}), RAPIDB_TIME_KEY(TIME(${column})))`;
}
function normalizeSqliteDatetimeLiteral(value: string): string | null {
  const trimmed = value.trim().replace(/ ([+-]\d{2}:\d{2})$/, "$1");
  if (!SQLITE_DATETIME_LITERAL_RE.test(trimmed)) return null;
  const normalized = trimmed.replace("T", " ");
  const match =
    /^(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2})(?::(\d{2})(?:\.\d+)?)?(?: ?(?:Z|[+-](\d{2}):(\d{2})))?$/i.exec(
      normalized,
    );
  if (!match) return null;
  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  if (month < 1 || month > 12 || day < 1) return null;
  const leapYear = year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0);
  const daysInMonth = [
    31,
    leapYear ? 29 : 28,
    31,
    30,
    31,
    30,
    31,
    31,
    30,
    31,
    30,
    31,
  ];
  if (day > daysInMonth[month - 1]) return null;
  const hours = Number(match[4]);
  const minutes = Number(match[5]);
  const seconds = Number(match[6] ?? "00");
  if (hours > 23 || minutes > 59 || seconds > 59) return null;
  const offsetHours = Number(match[7] ?? "00");
  const offsetMinutes = Number(match[8] ?? "00");
  if (offsetHours > 14 || offsetMinutes > 59) return null;
  return normalized;
}
function sqliteDatetimeKeyExpression(column: string): string {
  return `COALESCE(RAPIDB_DATETIME_KEY(${column}), RAPIDB_DATETIME_KEY(DATETIME(${column})))`;
}
function parseSqliteBlobDisplayValue(val: string): Uint8Array | null {
  try {
    const parsed: unknown = JSON.parse(val.trim());
    if (Array.isArray(parsed)) {
      if (
        parsed.length > 0 &&
        parsed.every(
          (v) =>
            typeof v === "number" && Number.isInteger(v) && v >= 0 && v <= 255,
        )
      ) {
        return new Uint8Array(parsed as number[]);
      }
      return null;
    }
    if (parsed !== null && typeof parsed === "object") {
      const entries = Object.entries(parsed as Record<string, unknown>);
      if (
        entries.length > 0 &&
        entries.every(
          ([k, v]) =>
            /^\d+$/.test(k) &&
            typeof v === "number" &&
            Number.isInteger(v) &&
            v >= 0 &&
            v <= 255,
        )
      ) {
        const maxIdx = Math.max(...entries.map(([k]) => Number(k)));
        const bytes = new Uint8Array(maxIdx + 1);
        for (const [k, v] of entries) {
          bytes[Number(k)] = v as number;
        }
        return bytes;
      }
    }
    return null;
  } catch {
    return null;
  }
}
function invalidSqliteTemporalFilterError(
  columnName: string,
  typeName: "time" | "datetime",
): Error {
  return new Error(
    `[RapiDB Filter] Column ${columnName} expects a valid ${typeName} value.`,
  );
}
function quoteSqliteIdentifier(identifier: string): string {
  return `"${identifier.replace(/"/g, '""')}"`;
}

class SQLiteRollbackFailure extends AggregateError {
  constructor(operationError: unknown, rollbackError: unknown) {
    super(
      [operationError, rollbackError],
      "SQLite operation and rollback both failed; the connection session is unsafe.",
    );
    this.name = "SQLiteRollbackFailure";
  }
}

export class SQLiteCoreDriver extends BaseDBDriver {
  protected override getQueryEditorSqlDialect() {
    return "sqlite" as const;
  }

  private db: SQLiteDatabase | null = null;
  private readonly config: ConnectionConfig;
  constructor(
    config: ConnectionConfig,
    timeoutSettingsProvider?: DriverTimeoutSettingsProvider,
    private readonly runtimeTargets?: readonly string[],
  ) {
    super(timeoutSettingsProvider);
    this.config = config;
  }
  async connect(): Promise<void> {
    if (!this.config.filePath) {
      throw new Error("[RapiDB] SQLite requires a filePath");
    }
    if (this.db !== null) {
      try {
        this.db.close();
      } catch {}
      this.db = null;
    }
    this.db = await openSQLiteDatabase(
      {
        filePath: this.config.filePath,
        readOnly: this.config.readOnly,
      },
      this.runtimeTargets,
    );
  }
  private requireDb(): SQLiteDatabase {
    if (!this.db?.isOpen) {
      throw new Error("[RapiDB] SQLite connection is not open");
    }
    return this.db;
  }
  private listAttachedDatabases(): SQLiteDatabaseListRow[] {
    return this.requireDb().all(
      "PRAGMA database_list",
    ) as SQLiteDatabaseListRow[];
  }
  private resolveSqliteSchema(database: string, schema: string): string {
    const available = new Map(
      this.listAttachedDatabases()
        .map((entry) => entry.name)
        .filter(
          (name): name is string => typeof name === "string" && name.length > 0,
        )
        .map((name) => [sqliteAsciiLower(name), name]),
    );
    available.set("temp", available.get("temp") ?? "temp");
    const lookup = (name: string) =>
      available.get(sqliteAsciiLower(name)) ??
      available.get(sqliteAsciiLower(name.trim()));
    if (schema?.trim()) {
      const resolved = lookup(schema);
      if (!resolved) {
        throw new Error(
          `[RapiDB] SQLite schema "${schema}" is not attached to this connection`,
        );
      }
      return resolved;
    }
    return (database?.trim() && lookup(database)) || "main";
  }
  private readTableMetadata(
    database: string,
    schema: string,
    table: string,
  ): SQLiteTableMetadata {
    const db = this.requireDb();
    const sqliteSchema = this.resolveSqliteSchema(database, schema);
    const safeName = table.replace(/"/g, '""');
    const rows = (
      db.all(
        `PRAGMA ${quoteSqliteIdentifier(sqliteSchema)}.table_xinfo("${safeName}")`,
      ) as unknown as SQLiteTableXInfoRow[]
    ).filter((row) => row.hidden !== 1);
    const fkRows = db.all(
      `PRAGMA ${quoteSqliteIdentifier(sqliteSchema)}.foreign_key_list("${safeName}")`,
    ) as {
      from: string;
    }[];
    const autoIncrementColumns = new Set<string>();
    let createSql = "";
    try {
      const master = db.all(
        `SELECT sql FROM ${quoteSqliteIdentifier(sqliteSchema)}.sqlite_master WHERE type='table' AND name=?`,
        [table],
      ) as {
        sql: string;
      }[];
      createSql = master[0]?.sql ?? "";
      for (const definition of sqliteSplitCreateDefinitions(createSql)) {
        const columnName = sqliteColumnDefinitionName(definition);
        if (!columnName) continue;
        const nameEnd = /^["`[]/.test(definition)
          ? skipQuotedSql(definition, 0)
          : columnName.length;
        if (
          /^\s*INTEGER\b[^,)]*\bPRIMARY\s+KEY\b[^,)]*\bAUTOINCREMENT\b/i.test(
            definition.slice(nameEnd),
          )
        )
          autoIncrementColumns.add(sqliteAsciiLower(columnName));
      }
    } catch {}
    const generatedColumns = parseSqliteGeneratedColumns(createSql);
    for (const row of rows) {
      const generatedKind = sqliteGeneratedKindFromHidden(row.hidden);
      if (!generatedKind) {
        continue;
      }
      const key = sqliteAsciiLower(row.name);
      const detail = generatedColumns.get(key) ?? {};
      generatedColumns.set(key, {
        ...detail,
        generatedKind: detail.generatedKind ?? generatedKind,
      });
    }
    return {
      rows,
      foreignKeyColumns: new Set(
        fkRows.map((row) => sqliteAsciiLower(row.from)),
      ),
      autoIncrementColumns,
      generatedColumns,
    };
  }
  private toColumnMeta(
    row: SQLiteTableXInfoRow,
    metadata: SQLiteTableMetadata,
  ): ColumnMeta {
    const generatedDetail = metadata.generatedColumns.get(
      sqliteAsciiLower(row.name),
    );
    const isComputed = generatedDetail !== undefined;
    const defaultValue = !isComputed
      ? (row.dflt_value ?? undefined)
      : undefined;
    return {
      name: row.name,
      type: row.type.toLowerCase() || "text",
      nullable: row.notnull === 0,
      defaultValue,
      identityGeneration: metadata.autoIncrementColumns.has(
        sqliteAsciiLower(row.name),
      )
        ? "auto_increment"
        : undefined,
      isComputed,
      computedExpression: generatedDetail?.expression,
      generatedKind: generatedDetail?.generatedKind,
      isPersisted:
        generatedDetail?.generatedKind === undefined
          ? undefined
          : generatedDetail.generatedKind === "stored",
      isPrimaryKey: row.pk > 0,
      primaryKeyOrdinal: row.pk > 0 ? row.pk : undefined,
      isForeignKey: metadata.foreignKeyColumns.has(sqliteAsciiLower(row.name)),
    };
  }
  async disconnect(): Promise<void> {
    this.db?.close();
    this.db = null;
  }
  isConnected(): boolean {
    return this.db?.isOpen ?? false;
  }

  getEntityManifest(): DriverEntityManifest {
    return SQLITE_ENTITY_MANIFEST;
  }

  async listDatabases(): Promise<DatabaseInfo[]> {
    return this.listAttachedDatabases().map((database) => ({
      name: database.name,
      schemas: [],
    }));
  }
  async listSchemas(database: string): Promise<SchemaInfo[]> {
    return [{ name: this.resolveSqliteSchema(database, "") }];
  }
  async listObjects(database: string, schema: string): Promise<TableInfo[]> {
    const sqliteSchema = this.resolveSqliteSchema(database, schema);
    const rows = this.requireDb().all(`SELECT name, type
       FROM ${quoteSqliteIdentifier(sqliteSchema)}.sqlite_master
       WHERE type IN ('table','view')
         AND name NOT LIKE 'sqlite!_%' ESCAPE '!'
       ORDER BY type DESC, name`) as {
      name: string;
      type: string;
    }[];
    return rows.map((r) => ({
      schema: sqliteSchema,
      name: r.name,
      type: r.type as TableInfo["type"],
    }));
  }
  async describeTable(
    database: string,
    schema: string,
    table: string,
  ): Promise<ColumnMeta[]> {
    const metadata = this.readTableMetadata(database, schema, table);
    return metadata.rows.map((row) => this.toColumnMeta(row, metadata));
  }
  override async describeColumns(
    database: string,
    schema: string,
    table: string,
  ): Promise<ColumnTypeMeta[]> {
    const metadata = this.readTableMetadata(database, schema, table);
    return metadata.rows.map((row) =>
      this.enrichColumn(this.toColumnMeta(row, metadata)),
    );
  }
  async getTableRowIdentityOrderExpressions(
    _database: string,
    schema: string,
    table: string,
    columns: readonly ColumnTypeMeta[],
  ): Promise<string[]> {
    const master = schema
      ? `${quoteSqliteIdentifier(schema)}.sqlite_master`
      : "sqlite_master";
    const result = await this.query(
      `SELECT type FROM ${master} WHERE type IN ('table','view') AND name = ?`,
      [table],
    );
    if (result.rows[0]?.__col_0 !== "table") {
      return [];
    }
    const columnNames = new Set(
      columns.map((column) => column.name.toLowerCase()),
    );
    const alias = ["rowid", "_rowid_", "oid"].find(
      (candidate) => !columnNames.has(candidate),
    );
    return alias ? [alias] : [];
  }
  async query(
    sql: string,
    params?: unknown[],
    operationContext?: QueryExecutionOptions & { deadline?: number },
  ): Promise<QueryResult> {
    const db = this.requireDb();
    const deadlineExceeded = () =>
      operationContext?.deadline !== undefined &&
      Date.now() >= operationContext.deadline;
    if (deadlineExceeded()) {
      throw new MutationNotExecutedError(
        "SQLite query deadline exceeded before execution. The query was NOT_EXECUTED (not executed).",
      );
    }
    const statements = splitSQLiteScript(sql);
    const transactional =
      statements.length > 0 &&
      statements.every((statement) =>
        /^(?:SELECT|WITH|INSERT|UPDATE|DELETE|REPLACE|CREATE|ALTER|DROP|EXPLAIN|VALUES)\b/i.test(
          statement,
        ),
      );
    if (transactional) db.exec("SAVEPOINT rapidb_query_deadline");
    try {
      const result = await this.executeQuery(sql, params, operationContext);
      if (transactional) {
        if (deadlineExceeded()) {
          throw new Error("SQLite query deadline exceeded before completion.");
        }
        db.exec("RELEASE SAVEPOINT rapidb_query_deadline");
      }
      if (db.inTransaction) {
        throw new Error(
          "Unfinished SQLite transaction was rolled back. Execute BEGIN/SAVEPOINT and COMMIT/ROLLBACK/RELEASE in the same script. Earlier commits are not undone.",
        );
      }
      return result;
    } catch (error) {
      if (db.inTransaction) {
        try {
          if (transactional) {
            db.exec("ROLLBACK TO SAVEPOINT rapidb_query_deadline");
            db.exec("RELEASE SAVEPOINT rapidb_query_deadline");
          } else {
            db.exec("ROLLBACK");
          }
        } catch (rollbackError) {
          throw new SQLiteRollbackFailure(error, rollbackError);
        }
        if (transactional) {
          if (error instanceof TransactionVerificationError) throw error;
          throw new MutationNotExecutedError(
            error instanceof Error ? error.message : String(error),
            error,
          );
        }
      }
      throw error;
    }
  }
  private async executeQuery(
    sql: string,
    params?: unknown[],
    operationContext?: QueryExecutionOptions,
  ): Promise<QueryResult> {
    const hardCap = queryCollectionLimit(operationContext?.hardCap);
    const start = Date.now();
    if (params && params.length > 0) {
      return this._executeSingle(sql, params, start, hardCap);
    }
    const stmts = splitSQLiteScript(sql);
    if (stmts.length === 0) {
      return { columns: [], rows: [], rowCount: 0, executionTimeMs: 0 };
    }
    if (stmts.length === 1) {
      return this._executeSingle(stmts[0], [], start, hardCap);
    }
    return this._executeScript(stmts, start, hardCap);
  }
  private _executeSingle(
    sql: string,
    params: unknown[],
    start: number,
    hardCap?: number,
    onResultSet?: () => void,
  ): QueryResult {
    const kind = classifySql(sql);
    const isDml = isSQLiteDmlStatement(sql);
    const db = this.requireDb();
    if (hardCap !== undefined) {
      const result = db.queryBounded(sql, params, hardCap, onResultSet);
      const changes =
        result.changes === undefined ? undefined : isDml ? result.changes : 0;
      const affectedRows =
        changes ??
        (isDml && canSQLiteStatementReturnRows(sql)
          ? Number(
              (db.get("SELECT changes() AS affected") as { affected: number })
                .affected,
            )
          : undefined);
      return {
        columns: result.columns,
        columnMeta: result.columnMeta,
        rows: result.rows.map((row) =>
          Object.fromEntries(row.map((value, i) => [`__col_${i}`, value])),
        ),
        rowCount: changes ?? result.rowCount,
        ...(affectedRows !== undefined ? { affectedRows } : {}),
        truncated: result.truncated,
        executionTimeMs: Date.now() - start,
      };
    }
    if (kind === "select") {
      try {
        const { columns, columnMeta, rows: rawRows } = db.allRaw(sql, params);
        const rows = rawRows.map((row) =>
          Object.fromEntries(row.map((value, i) => [`__col_${i}`, value])),
        );
        return {
          columns,
          columnMeta,
          rows,
          rowCount: rows.length,
          executionTimeMs: Date.now() - start,
        };
      } catch (error) {
        const msg = error instanceof Error ? error.message : String(error);
        if (!/does not return data/i.test(msg)) {
          throw error;
        }
      }
    }
    if (canSQLiteStatementReturnRows(sql)) {
      const { columns, columnMeta, rows: rawRows } = db.allRaw(sql, params);
      const rows = rawRows.map((row) =>
        Object.fromEntries(
          row.map((value, index) => [`__col_${index}`, value]),
        ),
      );
      return {
        columns,
        columnMeta,
        rows,
        rowCount: rows.length,
        executionTimeMs: Date.now() - start,
      };
    }
    const info = db.run(sql, params);
    const changes = isDml ? info.changes : 0;
    return {
      columns: [],
      rows: [],
      rowCount: changes,
      executionTimeMs: Date.now() - start,
      affectedRows: changes,
    };
  }
  private _executeScript(
    stmts: string[],
    start: number,
    hardCap?: number,
  ): QueryResult {
    const resultSets: QueryResultSet[] = [];
    let retainedRows = 0;
    let retainedColumns = 0;
    let resultSetsTruncated = false;
    let totalAffected = 0;
    for (const stmt of stmts) {
      const remainingHardCap =
        hardCap === undefined ? undefined : Math.max(1, hardCap - retainedRows);
      const r = this._executeSingle(stmt, [], start, remainingHardCap);
      totalAffected += r.affectedRows ?? 0;
      if (r.columns.length === 0) continue;
      if (
        resultSets.length >= SQLITE_MAX_RETAINED_RESULT_SETS ||
        retainedColumns + r.columns.length > SQLITE_MAX_RETAINED_RESULT_COLUMNS
      ) {
        resultSetsTruncated = true;
        continue;
      }

      const remainingRows =
        hardCap === undefined
          ? r.rows.length
          : Math.max(0, hardCap - retainedRows);
      const rows = r.rows.slice(0, remainingRows);
      const truncated = r.truncated === true || rows.length < r.rows.length;
      resultSets.push({
        id: `result-${resultSets.length + 1}`,
        columns: r.columns,
        columnMeta: r.columnMeta ?? [],
        rows,
        rowCount: r.rowCount,
        truncated,
      });
      retainedRows += rows.length;
      retainedColumns += r.columns.length;
    }

    const firstResultSet = resultSets[0];
    return {
      columns: firstResultSet?.columns ?? [],
      columnMeta: firstResultSet?.columnMeta ?? [],
      rows: firstResultSet?.rows ?? [],
      rowCount:
        resultSets.length > 0
          ? resultSets.reduce(
              (total, resultSet) => total + resultSet.rowCount,
              0,
            )
          : totalAffected,
      executionTimeMs: Date.now() - start,
      ...(resultSets.length > 0 ? { resultSets } : {}),
      ...(resultSets.some((resultSet) => resultSet.truncated)
        ? { truncated: true }
        : {}),
      ...(resultSetsTruncated ? { resultSetsTruncated: true } : {}),
      ...(resultSets.length === 0 && totalAffected > 0
        ? { affectedRows: totalAffected }
        : {}),
    };
  }
  override getCapabilities() {
    return { ...super.getCapabilities(), boundedQueryResults: true };
  }
  async getIndexes(
    database: string,
    schema: string,
    table: string,
  ): Promise<import("./types").IndexMeta[]> {
    const safeTable = table.replace(/"/g, '""');
    const db = this.requireDb();
    const sqliteSchema = this.resolveSqliteSchema(database, schema);
    const idxList = db.all(
      `PRAGMA ${quoteSqliteIdentifier(sqliteSchema)}.index_list("${safeTable}")`,
    ) as {
      name: string;
      unique: number;
      origin: string;
      partial?: number;
    }[];
    const result: import("./types").IndexMeta[] = [];
    let indexSqlByName: Map<string, string | null> | undefined;
    for (const idx of idxList) {
      const safeName = idx.name.replace(/"/g, '""');
      const keys = (
        db.all(
          `PRAGMA ${quoteSqliteIdentifier(sqliteSchema)}.index_xinfo("${safeName}")`,
        ) as {
          seqno: number;
          cid: number;
          name: string | null;
          key: number;
        }[]
      ).filter((column) => column.key === 1);
      const hasExpression = keys.some(
        (column) => column.cid < 0 || column.name === null,
      );
      let expressionTerms: string[] = [];
      if (hasExpression) {
        indexSqlByName ??= new Map(
          (
            db.all(
              `SELECT name, sql FROM ${quoteSqliteIdentifier(sqliteSchema)}.sqlite_master WHERE type='index' AND tbl_name = ? COLLATE NOCASE`,
              [table],
            ) as { name: string; sql: string | null }[]
          ).map((row) => [row.name, row.sql]),
        );
        const terms = sqliteSplitCreateDefinitions(
          indexSqlByName.get(idx.name) ?? "",
        );
        if (terms.length === keys.length) {
          // Preserve the complete indexed term. ASC/DESC can be unquoted
          // column operands (a + desc), not necessarily sort-order suffixes.
          expressionTerms = terms;
        }
      }
      result.push({
        name: idx.name,
        columns: keys.map((column, position) =>
          column.cid >= 0 && column.name !== null
            ? column.name
            : expressionTerms[position] || "<expression>",
        ),
        unique: idx.unique === 1,
        primary: idx.origin === "pk",
        ...(idx.partial === 1 ? { partial: true } : {}),
        ...(hasExpression ? { expression: true } : {}),
      });
    }
    return result;
  }
  async getForeignKeys(
    database: string,
    schema: string,
    table: string,
  ): Promise<import("./types").ForeignKeyMeta[]> {
    const safeTable = table.replace(/"/g, '""');
    const db = this.requireDb();
    const sqliteSchema = this.resolveSqliteSchema(database, schema);
    const rows = db.all(
      `PRAGMA ${quoteSqliteIdentifier(sqliteSchema)}.foreign_key_list("${safeTable}")`,
    ) as {
      from: string;
      table: string;
      to: string | null;
      id: number;
      seq: number;
    }[];
    const primaryKeys = new Map<string, string[]>();
    const primaryKeyColumns = (referencedTable: string): string[] => {
      let columns = primaryKeys.get(referencedTable);
      if (!columns) {
        columns = (
          db.all(
            `PRAGMA ${quoteSqliteIdentifier(sqliteSchema)}.table_info("${referencedTable.replace(/"/g, '""')}")`,
          ) as { name: string; pk: number }[]
        )
          .filter((column) => column.pk > 0)
          .sort((left, right) => left.pk - right.pk)
          .map((column) => column.name);
        primaryKeys.set(referencedTable, columns);
      }
      return columns;
    };
    return rows.map((r) => ({
      constraintName: `fk_${table}_${r.id}`,
      column: r.from,
      referencedSchema: sqliteSchema,
      referencedTable: r.table,
      referencedColumn: r.to ?? primaryKeyColumns(r.table)[r.seq] ?? "",
    }));
  }
  async getConstraints(
    database: string,
    schema: string,
    table: string,
  ): Promise<import("./types").TableConstraintMeta[]> {
    const constraints = await super.getConstraints(database, schema, table);
    const sqliteSchema = this.resolveSqliteSchema(database, schema);
    const row = this.requireDb().get(
      `SELECT sql FROM ${quoteSqliteIdentifier(sqliteSchema)}.sqlite_master WHERE type='table' AND name=?`,
      [table],
    ) as { sql: string | null } | null;
    const createSql = row?.sql ?? "";
    let unnamedIndex = 1;
    for (const check of extractSqliteCheckConstraints(createSql)) {
      constraints.push({
        name: check.name ?? `check_${table}_${unnamedIndex++}`,
        kind: "check",
        columns: [],
        checkExpression: check.expression,
        source: "catalog",
      });
    }
    return constraints;
  }
  async getTriggers(
    database: string,
    schema: string,
    table: string,
  ): Promise<import("./types").TriggerMeta[] | null> {
    const sqliteSchema = this.resolveSqliteSchema(database, schema);
    const rows = this.requireDb().all(
      `SELECT name, sql FROM ${quoteSqliteIdentifier(sqliteSchema)}.sqlite_master WHERE type='trigger' AND tbl_name = ? COLLATE NOCASE ORDER BY name`,
      [table],
    ) as { name: string; sql: string | null }[];
    return rows.map((row) => ({
      name: row.name,
      ...parseSqliteTriggerHeader(row.sql ?? ""),
      orientation: "row",
      enabled: true,
      definition: row.sql ?? undefined,
    }));
  }
  override async getConstraintDDL(
    database: string,
    schema: string,
    table: string,
    constraintName: string,
  ): Promise<string> {
    const constraint = (
      await this.getConstraints(database, schema, table)
    ).find((entry) => entry.name === constraintName);
    if (!constraint) {
      throw new Error(`Constraint "${constraintName}" not found`);
    }

    const tableDdl = await this.getCreateTableDDL(database, schema, table);
    return `-- SQLite stores table constraints inside CREATE TABLE statements.\n-- Constraint: ${constraint.name}\n${tableDdl}`;
  }
  override async getIndexDDL(
    database: string,
    schema: string,
    table: string,
    indexName: string,
  ): Promise<string> {
    const sqliteSchema = this.resolveSqliteSchema(database, schema);
    const row = this.requireDb().get(
      `SELECT sql FROM ${quoteSqliteIdentifier(sqliteSchema)}.sqlite_master WHERE type='index' AND name = ?`,
      [indexName],
    ) as { sql: string | null } | null;
    if (row?.sql) {
      return row.sql.endsWith(";") ? row.sql : `${row.sql};`;
    }

    const index = (await this.getIndexes(database, schema, table)).find(
      (entry) => entry.name === indexName,
    );
    if (!index) {
      throw new Error(`Index "${indexName}" not found`);
    }

    if (index.primary) {
      const tableDdl = await this.getCreateTableDDL(database, schema, table);
      return `-- SQLite stores PRIMARY KEY indexes inside CREATE TABLE statements.\n-- Index: ${index.name}\n${tableDdl}`;
    }

    return super.getIndexDDL(database, schema, table, indexName);
  }
  override async getTriggerDDL(
    database: string,
    schema: string,
    table: string,
    triggerName: string,
  ): Promise<string> {
    const sqliteSchema = this.resolveSqliteSchema(database, schema);
    const row = this.requireDb().get(
      `SELECT sql FROM ${quoteSqliteIdentifier(sqliteSchema)}.sqlite_master WHERE type='trigger' AND tbl_name = ? COLLATE NOCASE AND name = ?`,
      [table, triggerName],
    ) as { sql: string | null } | null;
    const sql = row?.sql?.trim();
    if (!sql) {
      throw new Error(`Trigger "${triggerName}" not found`);
    }
    return sql.endsWith(";") ? sql : `${sql};`;
  }
  async getCreateTableDDL(
    database: string,
    schema: string,
    table: string,
  ): Promise<string> {
    const sqliteSchema = this.resolveSqliteSchema(database, schema);
    const row = this.requireDb().get(
      `SELECT sql FROM ${quoteSqliteIdentifier(sqliteSchema)}.sqlite_master WHERE type IN ('table','view') AND name = ?`,
      [table],
    ) as {
      sql: string;
    } | null;
    return row?.sql ?? `-- DDL not available for "${table}"`;
  }
  async getObjectDefinition(
    _database: string,
    _schema: string,
    _name: string,
    _kind: DdlOnlyDbObjectKind,
  ): Promise<string | null> {
    return null;
  }
  async getRoutineDefinition(
    _database: string,
    _schema: string,
    name: string,
    kind: "function" | "procedure",
    _routineIdentity?: string,
  ): Promise<string> {
    return `-- SQLite does not support stored ${kind}s.\n-- Object: ${name}`;
  }
  async runTransaction(
    operations: import("./types").TransactionOperation[],
    context?: import("./types").TransactionContext,
    scope?: import("./types").TransactionOptions,
  ): Promise<void> {
    throwIfTransactionCancelled(context);
    const db = this.requireDb();
    db.exec("BEGIN TRANSACTION");
    try {
      const identities = new TransactionIdentityStore();
      for (const [index, op] of operations.entries()) {
        throwIfTransactionCancelled(context);
        const returning =
          op.captureIdentity && !op.captureIdentity.sql
            ? this._executeSingle(op.sql, op.params ?? [], Date.now(), 2)
            : undefined;
        const affected = returning
          ? (returning.affectedRows ?? returning.rowCount)
          : db.run(op.sql, op.params ?? []).changes;
        assertTransactionAffectedRows(op, affected);
        await identities.capture(
          index,
          op,
          returning?.rows ?? [],
          async (sql, params) =>
            this._executeSingle(sql, params, Date.now(), 2).rows,
        );
      }
      await verifyTransaction(
        this,
        identities.resolve(scope?.verifications),
        async (verification) =>
          this._executeSingle(
            verification.sql,
            verification.params,
            Date.now(),
            2,
          ).rows,
        context,
      );
      throwIfTransactionCancelled(context);
      db.exec("COMMIT");
    } catch (error) {
      if (db.inTransaction) {
        try {
          db.exec("ROLLBACK");
        } catch (rollbackError) {
          throw new SQLiteRollbackFailure(error, rollbackError);
        }
        if (error instanceof TransactionVerificationError) {
          error.rollbackConfirmed = true;
          throw error;
        }
        throw new MutationNotExecutedError(
          error instanceof Error ? error.message : String(error),
          error,
        );
      }
      throw error;
    }
  }
  mapTypeCategory(nativeType: string): TypeCategory {
    const ct = nativeType.toUpperCase().trim();
    const base = sqliteDeclaredTypeBase(nativeType);
    if (base === "" || base === "TEXT") return "text";
    if (base === "JSON") return "json";
    if (base === "UUID") return "uuid";
    if (
      base === "INTEGER" ||
      base === "INT" ||
      base === "BIGINT" ||
      base === "SMALLINT" ||
      base === "TINYINT" ||
      base === "MEDIUMINT"
    )
      return "integer";
    if (base === "REAL" || base === "DOUBLE" || base === "FLOAT")
      return "float";
    if (base === "NUMERIC" || base === "DECIMAL") return "decimal";
    if (base === "BOOLEAN" || base === "BOOL") return "boolean";
    if (base === "BLOB") return "binary";
    if (base === "DATE") return "date";
    if (base === "TIME") return "time";
    if (base === "DATETIME" || base === "TIMESTAMP") return "datetime";
    if (ct.includes("INT")) return "integer";
    if (ct.includes("CHAR") || ct.includes("TEXT") || ct.includes("CLOB"))
      return "text";
    if (ct.includes("REAL") || ct.includes("FLOA") || ct.includes("DOUB"))
      return "float";
    if (ct.includes("BLOB")) return "binary";
    return "other";
  }
  protected getValueSemantics(
    nativeType: string,
    _category: TypeCategory,
  ): ValueSemantics {
    const base = sqliteDeclaredTypeBase(nativeType);
    return base === "BOOLEAN" || base === "BOOL" ? "boolean" : "plain";
  }
  isDatetimeWithTime(nativeType: string): boolean {
    const base = sqliteDeclaredTypeBase(nativeType);
    return base === "DATETIME" || base === "TIMESTAMP";
  }
  override coerceInputValue(value: unknown, column: ColumnTypeMeta): unknown {
    if (
      typeof value === "boolean" &&
      (this.hasBooleanSemantics(column) ||
        column.category === "boolean" ||
        ["BOOLEAN", "BOOL"].includes(sqliteDeclaredTypeBase(column.nativeType)))
    ) {
      return value ? 1 : 0;
    }
    if (typeof value === "string" && this.hasBooleanSemantics(column)) {
      const normalized = this.parseBooleanInput(value);
      if (normalized !== null) {
        return normalized ? 1 : 0;
      }
    }
    if (typeof value === "string" && column.category === "binary") {
      const parsedBlob = parseSqliteBlobDisplayValue(value);
      if (parsedBlob !== null) {
        return parsedBlob;
      }
    }
    return super.coerceInputValue(value, column);
  }
  override formatOutputValue(value: unknown, column: ColumnTypeMeta): unknown {
    if (this.hasBooleanSemantics(column)) {
      if (value === 1 || value === "1") return true;
      if (value === 0 || value === "0") return false;
    }
    return super.formatOutputValue(value, column);
  }
  formatTableDataValue(
    value: unknown,
    column: ColumnTypeMeta,
    _projectionValues: Record<string, unknown>,
    _columnIndex: number,
    _columns: readonly ColumnTypeMeta[] = [],
  ): unknown {
    if (
      this.isDatetimeWithTime(column.nativeType) &&
      typeof value === "string"
    ) {
      return value;
    }
    return this.formatOutputValue(value, column);
  }
  override checkPersistedEdit(
    column: ColumnTypeMeta,
    expectedValue: unknown,
    options?: PersistedEditCheckOptions,
  ): PersistedEditCheckResult | null {
    if (column.category === "integer") {
      return this.checkExactNumericPersistedEdit(
        column,
        expectedValue,
        { precision: null, scale: null },
        options,
      );
    }
    if (column.category === "decimal") {
      return this.checkExactNumericPersistedEdit(
        column,
        expectedValue,
        { precision: null, scale: null },
        options,
      );
    }
    if (column.category === "float") {
      return this.checkApproximateNumericPersistedEdit(
        column,
        expectedValue,
        15,
        options,
      );
    }
    if (column.category === "boolean") {
      return this.checkBooleanPersistedEdit(column, expectedValue, options);
    }
    if (column.category === "binary") {
      const normalized =
        typeof expectedValue === "string"
          ? (parseSqliteBlobDisplayValue(expectedValue) ?? expectedValue)
          : expectedValue;
      return this.checkBinaryPersistedEdit(column, normalized, options);
    }
    if (column.category === "json") {
      return this.checkJsonPersistedEdit(column, expectedValue, options);
    }
    if (column.category === "uuid") {
      return this.checkUuidPersistedEdit(column, expectedValue, options);
    }
    if (
      column.category === "date" ||
      column.category === "time" ||
      column.category === "datetime"
    ) {
      return {
        ok: true,
        shouldVerify: false,
      };
    }
    if (column.category === "text") {
      return this.checkTextPersistedEdit(column, expectedValue, options);
    }
    return null;
  }
  override normalizeFilterValue(
    column: ColumnTypeMeta,
    operator: FilterOperator,
    value: string | [string, string] | undefined,
  ): string | [string, string] | undefined {
    if (
      column.category === "integer" &&
      !this.hasBooleanSemantics(column) &&
      ["eq", "neq", "gt", "gte", "lt", "lte", "between", "in"].includes(
        operator,
      )
    ) {
      if (Array.isArray(value)) {
        value = value.map(normalizeSqliteIntegerTrailingDot) as [
          string,
          string,
        ];
      } else if (typeof value === "string") {
        value =
          operator === "in"
            ? value.split(",").map(normalizeSqliteIntegerTrailingDot).join(",")
            : normalizeSqliteIntegerTrailingDot(value);
      }
    }
    if (
      this.isNumericCategory(column.category) &&
      column.category !== "decimal" &&
      typeof value === "string" &&
      (operator === "eq" || operator === "neq" || operator === "in")
    ) {
      try {
        return super.normalizeFilterValue(column, operator, value);
      } catch (error) {
        const trimmed = value.trim();
        if (trimmed === "") {
          throw error;
        }
        if (operator === "in") {
          const parts = trimmed
            .split(",")
            .map((part) => part.trim())
            .filter(Boolean);
          if (parts.length === 0) {
            throw error;
          }
          return parts
            .map((part) =>
              this.hasBooleanSemantics(column)
                ? part
                : (normalizeNumericToken(part) ?? part),
            )
            .join(", ");
        }
        return trimmed;
      }
    }
    const normalized = super.normalizeFilterValue(column, operator, value);
    if (normalized === undefined) {
      return normalized;
    }
    if (column.category === "time") {
      if (operator === "between" && Array.isArray(normalized)) {
        const startLiteral = normalizeSqliteTimeLiteral(normalized[0]);
        const endLiteral = normalizeSqliteTimeLiteral(normalized[1]);
        if (!startLiteral || !endLiteral) {
          throw invalidSqliteTemporalFilterError(column.name, "time");
        }
        return [startLiteral, endLiteral];
      }
      if (operator === "in" && typeof normalized === "string") {
        const parts = normalized
          .split(",")
          .map((part) => part.trim())
          .filter(Boolean);
        if (parts.length === 0) {
          throw invalidSqliteTemporalFilterError(column.name, "time");
        }
        const literals = parts.map((part) => {
          const literal = normalizeSqliteTimeLiteral(part);
          if (!literal) {
            throw invalidSqliteTemporalFilterError(column.name, "time");
          }
          return literal;
        });
        return literals.join(", ");
      }
      if (
        ["eq", "neq", "gt", "gte", "lt", "lte"].includes(operator) &&
        typeof normalized === "string"
      ) {
        const literal = normalizeSqliteTimeLiteral(normalized);
        if (literal) {
          return literal;
        }
        if (operator !== "eq" && operator !== "neq") {
          throw invalidSqliteTemporalFilterError(column.name, "time");
        }
        return normalized;
      }
    }
    if (column.category === "datetime") {
      if (operator === "between" && Array.isArray(normalized)) {
        const startLiteral = normalizeSqliteDatetimeLiteral(normalized[0]);
        const endLiteral = normalizeSqliteDatetimeLiteral(normalized[1]);
        if (!startLiteral || !endLiteral) {
          throw invalidSqliteTemporalFilterError(column.name, "datetime");
        }
        return [startLiteral, endLiteral];
      }
      if (operator === "in" && typeof normalized === "string") {
        const parts = normalized
          .split(",")
          .map((part) => part.trim())
          .filter(Boolean);
        if (parts.length === 0) {
          throw invalidSqliteTemporalFilterError(column.name, "datetime");
        }
        const literals = parts.map((part) => {
          const literal = normalizeSqliteDatetimeLiteral(part);
          if (!literal) {
            throw invalidSqliteTemporalFilterError(column.name, "datetime");
          }
          return literal;
        });
        return literals.join(", ");
      }
      if (
        ["eq", "neq", "gt", "gte", "lt", "lte"].includes(operator) &&
        typeof normalized === "string"
      ) {
        const literal = normalizeSqliteDatetimeLiteral(normalized);
        if (literal) {
          return literal;
        }
        if (operator !== "eq" && operator !== "neq") {
          throw invalidSqliteTemporalFilterError(column.name, "datetime");
        }
        return normalized;
      }
    }
    return normalized;
  }
  override buildFilterCondition(
    column: ColumnTypeMeta,
    operator: FilterOperator,
    value: string | [string, string] | undefined,
    _paramIndex: number,
  ): FilterConditionResult | null {
    const preamble = this.createFilterConditionPreamble(
      column,
      operator,
      value,
    );
    if (!preamble) return null;
    if (preamble.kind === "resolved") return preamble.condition;
    const col = preamble.columnSql;
    const val = preamble.value;
    if (column.category === "array") {
      if (operator !== "like" && operator !== "ilike") {
        return null;
      }
      const arrayValue = typeof val === "string" ? val : val[0];
      return {
        sql: `${col} LIKE ? ESCAPE '!'`,
        params: [literalContainsPattern(arrayValue)],
      };
    }
    if (
      column.category === "binary" &&
      typeof val === "string" &&
      (operator === "eq" || operator === "neq")
    ) {
      const sqlOp = operator === "neq" ? "!=" : "=";
      return {
        sql: `${col} ${sqlOp} ?`,
        params: [this.coerceInputValue(val, column)],
      };
    }
    if (
      column.category === "decimal" &&
      ["eq", "neq", "gt", "gte", "lt", "lte", "between", "in"].includes(
        operator,
      )
    ) {
      const normalized = super.normalizeFilterValue(column, operator, val);
      if (operator === "between" && Array.isArray(normalized)) {
        return { sql: `${col} BETWEEN ? AND ?`, params: normalized };
      }
      if (typeof normalized === "string") {
        if (operator === "in") {
          const parts = normalized.split(",").map((part) => part.trim());
          return {
            sql: `${col} IN (${parts.map(() => "?").join(", ")})`,
            params: parts,
          };
        }
        if (["eq", "neq", "gt", "gte", "lt", "lte"].includes(operator)) {
          return {
            sql: `${col} ${this.sqlOperator(operator)} ?`,
            params: [normalized],
          };
        }
      }
    }
    if (
      this.hasBooleanSemantics(column) &&
      (operator === "eq" || operator === "neq")
    ) {
      const strVal = (typeof val === "string" ? val : val[0]).toLowerCase();
      if (strVal === "true" || strVal === "false") {
        const boolVal = strVal === "true" ? 1 : 0;
        const op = operator === "neq" ? "!=" : "=";
        return { sql: `${col} ${op} ?`, params: [boolVal] };
      }
    }
    if (column.category === "integer" && !this.hasBooleanSemantics(column)) {
      if (operator === "between" && Array.isArray(val)) {
        return {
          sql: `${col} BETWEEN ? AND ?`,
          params: val.map(exactSqliteIntegerFilterToken),
        };
      }
      if (operator === "in" && typeof val === "string") {
        const parts = val
          .split(",")
          .map((part) => exactSqliteIntegerFilterToken(part.trim()));
        return {
          sql: `${col} IN (${parts.map(() => "?").join(", ")})`,
          params: parts,
        };
      }
      if (
        typeof val === "string" &&
        ["eq", "neq", "gt", "gte", "lt", "lte"].includes(operator)
      ) {
        return {
          sql: `${col} ${this.sqlOperator(operator)} ?`,
          params: [exactSqliteIntegerFilterToken(val)],
        };
      }
    }
    if (
      this.isNumericCategory(column.category) &&
      typeof val === "string" &&
      (operator === "eq" || operator === "neq") &&
      Number.isNaN(Number(val))
    ) {
      const sqlOp = operator === "neq" ? "!=" : this.sqlOperator(operator);
      return { sql: `${col} ${sqlOp} ?`, params: [val] };
    }
    if (
      this.isNumericCategory(column.category) &&
      typeof val === "string" &&
      !Number.isNaN(Number(val)) &&
      val !== ""
    ) {
      if (
        column.category === "float" &&
        (operator === "eq" || operator === "neq")
      ) {
        const numericValue = Number(val);
        if (!Number.isFinite(numericValue)) {
          const sqlOp = operator === "neq" ? "!=" : this.sqlOperator(operator);
          return { sql: `${col} ${sqlOp} ?`, params: [numericValue] };
        }
        const tolerance = approximateNumericFilterTolerance(val);
        const toleranceExpr = "MAX(?, ABS(?) * ?)";
        const deltaExpr = `ABS(CAST(${col} AS REAL) - ?)`;
        return {
          sql:
            operator === "neq"
              ? `${deltaExpr} >= ${toleranceExpr}`
              : `${deltaExpr} < ${toleranceExpr}`,
          params: [numericValue, tolerance, numericValue, tolerance],
        };
      }
      const sqlOp = this.sqlOperator(operator);
      if (column.category === "integer" && /^[+-]?\d+$/.test(val)) {
        const big = BigInt(val);
        if (
          big > SQLITE_JS_SAFE_INTEGER_MAX ||
          big < SQLITE_JS_SAFE_INTEGER_MIN ||
          big > SQLITE_INT64_MAX ||
          big < SQLITE_INT64_MIN
        ) {
          return { sql: `${col} ${sqlOp} ?`, params: [val] };
        }
      }
      return { sql: `${col} ${sqlOp} ?`, params: [Number(val)] };
    }
    if (column.category === "date") {
      if (operator === "between" && Array.isArray(val)) {
        return {
          sql: `DATE(${col}) BETWEEN DATE(?) AND DATE(?)`,
          params: [val[0], val[1]],
        };
      }
      if (typeof val === "string" && operator === "in") {
        const parts = val
          .split(",")
          .map((part) => part.trim())
          .filter(Boolean);
        return {
          sql: `DATE(${col}) IN (${parts.map(() => "DATE(?)").join(", ")})`,
          params: parts,
        };
      }
      if (
        typeof val === "string" &&
        ["eq", "neq", "gt", "gte", "lt", "lte"].includes(operator)
      ) {
        const sqlOp = operator === "neq" ? "!=" : this.sqlOperator(operator);
        return { sql: `DATE(${col}) ${sqlOp} DATE(?)`, params: [val] };
      }
    }
    if (column.category === "time") {
      if (operator === "between" && Array.isArray(val)) {
        const startLiteral = normalizeSqliteTimeLiteral(val[0]);
        const endLiteral = normalizeSqliteTimeLiteral(val[1]);
        if (!startLiteral || !endLiteral) {
          throw invalidSqliteTemporalFilterError(column.name, "time");
        }
        return {
          sql: `${sqliteTimeKeyExpression(col)} BETWEEN RAPIDB_TIME_KEY(?) AND RAPIDB_TIME_KEY(?)`,
          params: [startLiteral, endLiteral],
        };
      }
      if (operator === "in" && typeof val === "string") {
        const parts = val
          .split(",")
          .map((part) => part.trim())
          .filter(Boolean)
          .map((part) => {
            const literal = normalizeSqliteTimeLiteral(part);
            if (!literal) {
              throw invalidSqliteTemporalFilterError(column.name, "time");
            }
            return literal;
          });
        if (parts.length === 0) {
          throw invalidSqliteTemporalFilterError(column.name, "time");
        }
        return {
          sql: `${sqliteTimeKeyExpression(col)} IN (${parts.map(() => "RAPIDB_TIME_KEY(?)").join(", ")})`,
          params: parts,
        };
      }
      if (
        ["eq", "neq", "gt", "gte", "lt", "lte"].includes(operator) &&
        typeof val === "string"
      ) {
        const literal = normalizeSqliteTimeLiteral(val);
        if (literal) {
          const sqlOp = operator === "neq" ? "!=" : this.sqlOperator(operator);
          return {
            sql: `${sqliteTimeKeyExpression(col)} ${sqlOp} RAPIDB_TIME_KEY(?)`,
            params: [literal],
          };
        }
        if (operator === "eq" || operator === "neq") {
          return {
            sql: `${col} ${operator === "neq" ? "<>" : "="} ?`,
            params: [val],
          };
        }
        throw invalidSqliteTemporalFilterError(column.name, "time");
      }
    }
    if (column.category === "datetime") {
      if (operator === "between" && Array.isArray(val)) {
        const startLiteral = normalizeSqliteDatetimeLiteral(val[0]);
        const endLiteral = normalizeSqliteDatetimeLiteral(val[1]);
        if (!startLiteral || !endLiteral) {
          throw invalidSqliteTemporalFilterError(column.name, "datetime");
        }
        return {
          sql: `${sqliteDatetimeKeyExpression(col)} BETWEEN RAPIDB_DATETIME_KEY(?) AND RAPIDB_DATETIME_KEY(?)`,
          params: [startLiteral, endLiteral],
        };
      }
      if (operator === "in" && typeof val === "string") {
        const parts = val
          .split(",")
          .map((part) => part.trim())
          .filter(Boolean)
          .map((part) => {
            const literal = normalizeSqliteDatetimeLiteral(part);
            if (!literal) {
              throw invalidSqliteTemporalFilterError(column.name, "datetime");
            }
            return literal;
          });
        return {
          sql: `${sqliteDatetimeKeyExpression(col)} IN (${parts
            .map(() => "RAPIDB_DATETIME_KEY(?)")
            .join(", ")})`,
          params: parts,
        };
      }
      if (
        ["eq", "neq", "gt", "gte", "lt", "lte"].includes(operator) &&
        typeof val === "string"
      ) {
        const literal = normalizeSqliteDatetimeLiteral(val);
        if (literal) {
          const sqlOp = operator === "neq" ? "!=" : this.sqlOperator(operator);
          return {
            sql: `${sqliteDatetimeKeyExpression(col)} ${sqlOp} RAPIDB_DATETIME_KEY(?)`,
            params: [literal],
          };
        }
        if (operator === "eq" || operator === "neq") {
          return {
            sql: `${col} ${operator === "neq" ? "<>" : "="} ?`,
            params: [val],
          };
        }
        throw invalidSqliteTemporalFilterError(column.name, "datetime");
      }
    }
    if (operator === "between" && Array.isArray(val)) {
      return { sql: `${col} BETWEEN ? AND ?`, params: [val[0], val[1]] };
    }
    if (operator === "in" && typeof val === "string") {
      const parts = val.split(",").map((s) => s.trim());
      return {
        sql: `${col} IN (${parts.map(() => "?").join(", ")})`,
        params: parts,
      };
    }
    const v = typeof val === "string" ? val : val[0];
    if (operator === "like" || operator === "ilike") {
      return {
        sql: `${col} LIKE ? ESCAPE '!'`,
        params: [literalContainsPattern(v)],
      };
    }
    return { sql: `${col} LIKE ?`, params: [`%${v}%`] };
  }
}
