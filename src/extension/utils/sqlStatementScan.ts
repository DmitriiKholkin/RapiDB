import type { ConnectionType } from "../../shared/connectionTypes";

export interface SqlCapToken {
  text: string;
  start: number;
  end: number;
  kind: "word" | "quoted" | "literal" | "symbol";
  depth: number;
}

/** Shared with the Oracle script splitter so literal semicolons stay opaque. */
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

/**
 * Lexical boundaries for the editor's conservative cap rewrites, not a SQL
 * parser. Unlike the driver script splitters this retains source positions and
 * fails closed on ambiguous/session-dependent quoting and executable comments.
 */
export function scanSqlForCap(
  sql: string,
  dialect: ConnectionType,
): SqlCapToken[] {
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
    if (/\s/.test(char)) {
      index++;
      continue;
    }
    const dashComment =
      char === "-" &&
      next === "-" &&
      (dialect !== "mysql" || !sql[index + 2] || /\s/.test(sql[index + 2]));
    if (dashComment || (dialect === "mysql" && char === "#")) {
      // SQLite/MySQL and their driver splitters terminate these comments at
      // LF, not bare CR. Treating CR as a boundary can append a LIMIT that the
      // driver/server then consumes as part of the comment.
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
      dialect === "pg"
        ? /^(\$[A-Za-z_][A-Za-z0-9_]*\$|\$\$)/.exec(sql.slice(index))?.[0]
        : undefined;
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
      // PostgreSQL E strings have a fixed escape convention. Ordinary PG and
      // MySQL strings depend on session settings; don't guess those settings.
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
