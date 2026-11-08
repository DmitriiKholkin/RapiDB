import { isMysqlDashCommentStart } from "./sqlStatementScan";

const NOT_PART_OF_NAME = String.raw`(?<![\w$.@\u0080-\uffff])`;
const STATEMENT_START = new RegExp(
  `(?:^|[;:]|${NOT_PART_OF_NAME}(?:BEGIN|ATOMIC|THEN|ELSE|DO|LOOP|REPEAT))$`,
  "i",
);
const HANDLER_CONDITION = String.raw`(?:SQLSTATE(?:\s+VALUE)?\s*'[^']*'|NOT\s+FOUND|SQLWARNING|SQLEXCEPTION|\d+|\x60[^\x60]*\x60|[A-Za-z_$\u0080-\uffff][\w$\u0080-\uffff]*)`;
const HANDLER_BODY_START = new RegExp(
  String.raw`${NOT_PART_OF_NAME}HANDLER\s+FOR\s+${HANDLER_CONDITION}(?:\s*,\s*${HANDLER_CONDITION})*$`,
  "i",
);
// BEGIN and END are non-reserved, so after an operator or clause keyword
// they are column references such as `SET end = 1`, not block delimiters.
const OPERAND_POSITION = new RegExp(
  String.raw`(?:[=<>!+\-*%&|^~,(.]|(?<!\*)\/|${NOT_PART_OF_NAME}(?:SET|SELECT|WHERE|AND|OR|NOT|XOR|ON|BY|AS|VALUES|IS|LIKE|IN|OUT|INOUT|BETWEEN|WHEN|THEN|ELSE|CASE|UNTIL|RETURN|DIV|MOD|REGEXP|RLIKE|INTERVAL|DISTINCT|HAVING|FROM|JOIN|INTO|UPDATE|TABLE|COLUMN|DECLARE|USING))$`,
  "i",
);
const ROUTINE_HEADER = new RegExp(
  String.raw`^\s*(?:CREATE|ALTER)\b[\s\S]*?${NOT_PART_OF_NAME}(?:PROCEDURE|FUNCTION|TRIGGER|EVENT)(?![\w$\u0080-\uffff])`,
  "i",
);
const NOT_ATOMIC_AFTER_BEGIN =
  /(?:\s|\/\*[\s\S]*?\*\/)+NOT(?:\s|\/\*[\s\S]*?\*\/)+ATOMIC\b/iy;

export function splitMySQLScript(sql: string): string[] {
  const stmts: string[] = [];
  let delim = ";";
  let i = 0;
  const n = sql.length;
  let buf = "";
  const blocks: Array<"block" | "case"> = [];
  function isWordChar(p: number): boolean {
    if (p < 0 || p >= n) return false;
    const ch = sql[p];
    return /[A-Za-z0-9_$\u0080-\uffff]/.test(ch);
  }
  function isKeywordAt(kw: string): boolean {
    return (
      !isWordChar(i - 1) &&
      sql[i - 1] !== "." &&
      sql[i - 1] !== "@" &&
      sql.slice(i, i + kw.length).toUpperCase() === kw &&
      (!isWordChar(i + kw.length) || sql.startsWith(delim, i + kw.length)) &&
      sql[i + kw.length] !== "."
    );
  }
  function consumeKeyword(kw: string): boolean {
    if (!isKeywordAt(kw)) return false;
    buf += sql.slice(i, i + kw.length);
    i += kw.length;
    return true;
  }
  function isStatementStart(): boolean {
    if (blocks[blocks.length - 1] === "case") return false;
    const before = buf.trimEnd();
    return STATEMENT_START.test(before) || HANDLER_BODY_START.test(before);
  }
  function isOperandPosition(): boolean {
    return OPERAND_POSITION.test(buf.trimEnd());
  }
  while (i < n) {
    const atLineStart = i === 0 || sql[i - 1] === "\n";
    if (atLineStart && /^DELIMITER\b/i.test(sql.slice(i))) {
      while (i < n && sql[i] !== " " && sql[i] !== "\t") i++;
      while (i < n && (sql[i] === " " || sql[i] === "\t")) i++;
      let nd = "";
      while (i < n && sql[i] !== "\r" && sql[i] !== "\n") {
        nd += sql[i++];
      }
      if (nd) delim = nd;
      while (i < n && sql[i] !== "\n") i++;
      if (i < n) i++;
      continue;
    }
    if (isMysqlDashCommentStart(sql, i) || sql[i] === "#") {
      while (i < n && sql[i] !== "\n") i++;
      buf += " ";
      continue;
    }
    if (sql[i] === "/" && sql[i + 1] === "*") {
      const start = i;
      const executable =
        sql[i + 2] === "!" ||
        sql[i + 2] === "+" ||
        (sql[i + 2] === "M" && sql[i + 3] === "!");
      i += 2;
      let closed = false;
      while (i < n) {
        if (sql[i] === "*" && sql[i + 1] === "/") {
          i += 2;
          closed = true;
          break;
        }
        i++;
      }
      if (!closed) {
        throw new Error(
          "Unterminated MySQL block comment; script was not submitted.",
        );
      }
      buf += executable ? sql.slice(start, i) : " ";
      continue;
    }
    if (sql[i] === "'" || sql[i] === '"' || sql[i] === "`") {
      const q = sql[i];
      buf += q;
      i++;
      while (i < n) {
        const c = sql[i];
        if (c === "\\" && q !== "`") {
          buf += c + (sql[i + 1] ?? "");
          i += 2;
          continue;
        }
        if (c === q && sql[i + 1] === q) {
          buf += c + c;
          i += 2;
          continue;
        }
        if (c === q) {
          buf += c;
          i++;
          break;
        }
        buf += c;
        i++;
      }
      continue;
    }
    if (isKeywordAt("BEGIN")) {
      NOT_ATOMIC_AFTER_BEGIN.lastIndex = i + 5;
      const opensBlock =
        blocks.length > 0
          ? isStatementStart()
          : (buf.trim() === "" && NOT_ATOMIC_AFTER_BEGIN.test(sql)) ||
            (!isOperandPosition() && ROUTINE_HEADER.test(buf));
      consumeKeyword("BEGIN");
      if (opensBlock) blocks.push("block");
      continue;
    }
    if (isKeywordAt("CASE")) {
      const statement = blocks.length > 0 && isStatementStart();
      consumeKeyword("CASE");
      blocks.push(statement ? "block" : "case");
      continue;
    }
    if (blocks.length > 0 && !isOperandPosition() && consumeKeyword("END")) {
      const closer = /^\s+(?:IF|CASE|WHILE|LOOP|REPEAT|FOR)\b/i.exec(
        sql.slice(i, i + 64),
      );
      if (closer) {
        buf += closer[0];
        i += closer[0].length;
      }
      blocks.pop();
      continue;
    }
    const opener =
      blocks.length > 0
        ? ["IF", "LOOP", "REPEAT", "WHILE", "FOR"].find(isKeywordAt)
        : undefined;
    if (opener && isStatementStart()) {
      consumeKeyword(opener);
      blocks.push("block");
      continue;
    }
    if (blocks.length === 0 && sql.startsWith(delim, i)) {
      const s = buf.trim();
      if (s) stmts.push(s);
      buf = "";
      i += delim.length;
      while (
        i < n &&
        (sql[i] === " " ||
          sql[i] === "\t" ||
          sql[i] === "\r" ||
          sql[i] === "\n")
      ) {
        i++;
      }
      continue;
    }
    buf += sql[i++];
  }
  const s = buf.trim();
  if (s) stmts.push(s);
  return stmts;
}
