import type { ConnectionType } from "../../shared/connectionTypes";

const POSTGRES_IDENTIFIER_CONTINUATION = /[A-Za-z0-9_$\u0080-\u{10FFFF}]/u;
const POSTGRES_DOLLAR_QUOTE_TAG =
  /^(\$\$|\$[A-Za-z_\u0080-\u{10FFFF}][A-Za-z0-9_\u0080-\u{10FFFF}]*\$)/u;

export function toPostgresWireText(sql: string): string {
  return Buffer.from(sql, "utf8").toString("utf8");
}

export function readPostgresDollarQuoteTag(
  sql: string,
  start: number,
): string | null {
  if (sql[start] !== "$") return null;

  if (start > 0) {
    let previousStart = start - 1;
    const previousCodeUnit = sql.charCodeAt(previousStart);
    if (
      previousCodeUnit >= 0xdc00 &&
      previousCodeUnit <= 0xdfff &&
      previousStart > 0
    ) {
      const highSurrogate = sql.charCodeAt(previousStart - 1);
      if (highSurrogate >= 0xd800 && highSurrogate <= 0xdbff) {
        previousStart -= 1;
      }
    }
    if (
      POSTGRES_IDENTIFIER_CONTINUATION.test(sql.slice(previousStart, start))
    ) {
      return null;
    }
  }

  return POSTGRES_DOLLAR_QUOTE_TAG.exec(sql.slice(start))?.[0] ?? null;
}

export function hasPostgresDollarQuoteTag(rawSql: string): boolean {
  const sql = toPostgresWireText(rawSql);
  for (let index = 0; index < sql.length; index += 1) {
    if (sql[index] === "$" && readPostgresDollarQuoteTag(sql, index)) {
      return true;
    }
  }
  return false;
}

const SQL_LEXER_WHITESPACE = /[ \t\n\r\f\v]/;

export function isMysqlDashCommentStart(sql: string, index: number): boolean {
  if (sql[index] !== "-" || sql[index + 1] !== "-") return false;
  if (index + 2 >= sql.length) return true;
  const following = sql.charCodeAt(index + 2);
  return following <= 0x20 || following === 0x7f;
}

export function isAmbiguousMysqlDashComment(
  sql: string,
  index: number,
): boolean {
  return (
    sql[index] === "-" &&
    sql[index + 1] === "-" &&
    sql.charCodeAt(index + 2) >= 0x80
  );
}

export function hasExecutableMysqlComment(sql: string): boolean {
  return (
    scanForExecutableMysqlComment(sql, true, true) ||
    scanForExecutableMysqlComment(sql, false, false) ||
    scanForExecutableMysqlComment(sql, true, false)
  );
}

function scanForExecutableMysqlComment(
  sql: string,
  backslashEscapesSingleQuotes: boolean,
  backslashEscapesDoubleQuotes: boolean,
): boolean {
  let index = 0;
  while (index < sql.length) {
    const char = sql[index];
    const next = sql[index + 1];
    if (char === "'" || char === '"' || char === "`") {
      index += 1;
      while (index < sql.length) {
        if (
          sql[index] === "\\" &&
          ((char === "'" && backslashEscapesSingleQuotes) ||
            (char === '"' && backslashEscapesDoubleQuotes))
        ) {
          index += 2;
          continue;
        }
        if (sql[index] === char) {
          if (sql[index + 1] === char) {
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

    if (isMysqlDashCommentStart(sql, index) || char === "#") {
      while (index < sql.length && sql[index] !== "\n" && sql[index] !== "\r") {
        index += 1;
      }
      continue;
    }

    if (char === "/" && next === "*") {
      if (/^\/\*(?:!|M!)/i.test(sql.slice(index))) return true;
      const end = sql.indexOf("*/", index + 2);
      if (end < 0) return false;
      index = end + 2;
      continue;
    }
    index += 1;
  }
  return false;
}

export interface SqlCapToken {
  text: string;
  start: number;
  end: number;
  kind: "word" | "quoted" | "literal" | "symbol";
  depth: number;
}

export function oracleAlternativeQuoteEnd(
  sql: string,
  start: number,
): number | undefined {
  if (
    !/[nq]/i.test(sql[start]) ||
    (start > 0 && /[A-Za-z0-9_$#\u0080-\uffff]/.test(sql[start - 1]))
  )
    return undefined;
  const prefix = /^(?:nq|q)'/i.exec(sql.slice(start));
  if (!prefix) return undefined;
  const opener = sql[start + prefix[0].length];
  const pairs: Record<string, string> = {
    "[": "]",
    "(": ")",
    "{": "}",
    "<": ">",
  };
  const closer = pairs[opener] ?? opener;
  const end = closer
    ? sql.indexOf(`${closer}'`, start + prefix[0].length + 1)
    : -1;
  if (end < 0)
    throw new Error(
      "Unterminated Oracle alternative-quoted string. Close the string and retry.",
    );
  return end + 2;
}

export function scanSqlForCap(
  rawSql: string,
  dialect: ConnectionType,
): SqlCapToken[] {
  const sql = dialect === "pg" ? toPostgresWireText(rawSql) : rawSql;
  const tokens: SqlCapToken[] = [];
  let index = 0;
  let depth = 0;
  const fail = (detail: string): never => {
    throw new Error(detail);
  };
  while (index < sql.length) {
    const start = index;
    const char = sql[index];
    const next = sql[index + 1];
    if (SQL_LEXER_WHITESPACE.test(char)) {
      index++;
      continue;
    }
    const dashComment =
      dialect === "mysql"
        ? isMysqlDashCommentStart(sql, index)
        : char === "-" && next === "-";
    if (dashComment || (dialect === "mysql" && char === "#")) {
      while (
        index < sql.length &&
        sql[index] !== "\n" &&
        (dialect === "sqlite" || dialect === "mysql" || sql[index] !== "\r")
      )
        index++;
      continue;
    }
    if (char === "/" && next === "*") {
      if (dialect === "mysql" && /^\/\*(?:!|M!)/i.test(sql.slice(index))) {
        fail(
          "Executable MySQL/MariaDB comments are unsupported. Expand their SQL and execute one statement at a time.",
        );
      }
      index += 2;
      let comments = 1;
      while (index < sql.length && comments) {
        if (sql.startsWith("*/", index)) {
          comments--;
          index += 2;
        } else if (
          (dialect === "pg" || dialect === "mssql") &&
          sql.startsWith("/*", index)
        ) {
          comments++;
          index += 2;
        } else index++;
      }
      if (comments) {
        fail("Unterminated SQL comment. Close the comment and retry.");
      }
      continue;
    }
    let kind: SqlCapToken["kind"] = "symbol";
    const alternativeEnd =
      dialect === "oracle" ? oracleAlternativeQuoteEnd(sql, index) : undefined;
    const dollarTag =
      dialect === "pg" ? readPostgresDollarQuoteTag(sql, index) : null;
    if (dollarTag) {
      const end = sql.indexOf(dollarTag, index + dollarTag.length);
      if (end < 0) {
        fail("Unterminated dollar-quoted string. Close the string and retry.");
      }
      index = end + dollarTag.length;
      kind = "literal";
    } else if (alternativeEnd !== undefined) {
      index = alternativeEnd;
      kind = "literal";
    } else if (
      char === "'" ||
      char === '"' ||
      ((dialect === "mysql" || dialect === "sqlite") && char === "`") ||
      ((dialect === "mssql" || dialect === "sqlite") && char === "[")
    ) {
      const closer = char === "[" ? "]" : char;
      kind = char === "'" ? "literal" : "quoted";
      const previous = tokens.at(-1);
      const escapeString =
        dialect === "pg" &&
        char === "'" &&
        previous?.text.toLowerCase() === "e" &&
        previous?.end === index;
      index++;
      let closed = false;
      while (index < sql.length) {
        if (
          sql[index] === "\\" &&
          ((char === "'" && (dialect === "mysql" || dialect === "pg")) ||
            (char === '"' && dialect === "mysql"))
        ) {
          if (!escapeString) {
            fail(
              "Session-dependent backslash quoting is unsupported. Use doubled quotes (or PostgreSQL E/dollar quoting) and retry.",
            );
          }
          index += 2;
        } else if (sql[index] === closer) {
          index++;
          if (
            sql[index] === closer &&
            !(dialect === "sqlite" && closer === "]")
          ) {
            index++;
          } else {
            closed = true;
            break;
          }
        } else index++;
      }
      if (!closed) fail("Unterminated SQL quote. Close the quote and retry.");
    } else if (/[A-Za-z_\u0080-\uffff]/.test(char)) {
      index++;
      while (
        index < sql.length &&
        /[A-Za-z0-9_$\u0080-\uffff]/.test(sql[index])
      )
        index++;
      kind = "word";
    } else {
      index++;
    }
    if (kind === "symbol" && char === ")") depth--;
    if (depth < 0) {
      fail("Unbalanced SQL parentheses. Check the statement and retry.");
    }
    tokens.push({
      text: sql.slice(start, index),
      start,
      end: index,
      kind,
      depth,
    });
    if (kind === "symbol" && char === "(") depth++;
  }
  if (depth !== 0) {
    fail("Unbalanced SQL parentheses. Check the statement and retry.");
  }
  return tokens;
}
