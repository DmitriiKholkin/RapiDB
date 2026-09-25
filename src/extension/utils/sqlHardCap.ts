import type { ConnectionType } from "../../shared/connectionTypes";
import type { SqlHardCapRewriteDecision } from "../../shared/safetyContracts";
import { isReadOnlySqlTokens } from "./readOnlyGuards";
import { type SqlCapToken, scanSqlForCap } from "./sqlStatementScan";

const SQL_TYPES = new Set<ConnectionType>([
  "pg",
  "mysql",
  "sqlite",
  "mssql",
  "oracle",
]);
const NON_RESULT_STARTERS = new Set([
  "insert",
  "update",
  "delete",
  "merge",
  "replace",
  "create",
  "alter",
  "drop",
  "truncate",
  "grant",
  "revoke",
  "comment",
  "begin",
  "commit",
  "rollback",
  "savepoint",
  "release",
  "set",
  "reset",
  "use",
  "vacuum",
  "analyze",
  "reindex",
  "attach",
  "detach",
  "start",
  "rename",
]);
const word = (token: SqlCapToken | undefined): string =>
  token?.kind === "word" ? token.text.toLowerCase() : "";
const classificationTokens = (tokens: SqlCapToken[]): string[] =>
  tokens.map((token, index) => {
    if (token.kind === "quoted") return "identifier";
    if (token.kind === "literal") return "literal";
    if (word(token) === "replace" && tokens[index + 1]?.text === "(")
      return "identifier";
    return token.text;
  });

// A derived table rejects duplicate output names in MySQL and renames them in
// SQLite. Preserve the original projection when LIMIT has a literal row count.
function integerAt(
  tokens: SqlCapToken[],
  index: number,
):
  | {
      value: bigint;
      start: number;
      end: number;
      next: number;
    }
  | undefined {
  const start = tokens[index]?.start;
  if (start === undefined) return undefined;
  let text = "";
  let end = start;
  if (["-", "+"].includes(tokens[index].text)) {
    text = tokens[index].text;
    end = tokens[index++].end;
  }
  const digitStart = index;
  while (tokens[index]?.kind === "symbol" && /^\d$/.test(tokens[index].text)) {
    if (index > digitStart && tokens[index].start !== end) break;
    text += tokens[index].text;
    end = tokens[index++].end;
  }
  if (index === digitStart) return undefined;
  return { value: BigInt(text), start, end, next: index };
}

function applyLiteralLimit(
  sql: string,
  tokens: SqlCapToken[],
  sqlStart: number,
  hardCap: number,
): string | undefined {
  const top = tokens.filter((token) => token.depth === tokens[0].depth);
  // SQLite forbids LIMIT when the rightmost compound operand is VALUES.
  if (top.some((token) => word(token) === "values")) return undefined;
  const limit = top.findIndex((token) => word(token) === "limit");
  if (limit < 0) return `${sql} LIMIT ${hardCap}`;
  let count = integerAt(top, limit + 1);
  if (!count) return undefined;
  let next = count.next;
  if (top[next]?.text === ",") {
    count = integerAt(top, next + 1);
    if (!count) return undefined;
    next = count.next;
  } else if (word(top[next]) === "offset") {
    const offset = integerAt(top, next + 1);
    if (!offset) return undefined;
    next = offset.next;
  }
  if (next !== top.length) return undefined;
  const capped =
    count.value < 0n || count.value > BigInt(hardCap) ? hardCap : count.value;
  return `${sql.slice(0, count.start - sqlStart)}${capped}${sql.slice(count.end - sqlStart)}`;
}
const MSSQL_BATCH_WORDS = new Set([
  "select",
  "with",
  "begin",
  "if",
  "while",
  "declare",
  "dbcc",
  "print",
  "waitfor",
  "receive",
  "readtext",
  "go",
  "exec",
  "execute",
]);

function isSubquerySelect(tokens: SqlCapToken[], index: number): boolean {
  const token = tokens[index];
  if (token.depth <= tokens[0].depth) return false;
  for (let i = index - 1; i >= 0; i--) {
    if (tokens[i].text === "(" && tokens[i].depth === token.depth - 1) {
      // These positions require an expression/table operand; a parenthesized
      // standalone T-SQL statement cannot start there. Do not infer from depth
      // alone: `SELECT 1 (SELECT ... )` can be a second statement.
      return (
        [
          "exists",
          "in",
          "from",
          "join",
          "apply",
          "select",
          "distinct",
          "all",
          "then",
          "else",
        ].includes(word(tokens[i - 1])) ||
        ["=", "<", ">", ",", "+", "-", "*", "/", "%"].includes(
          tokens[i - 1]?.text,
        )
      );
    }
  }
  return false;
}

function sqlitePragmaSelect(tokens: SqlCapToken[]): string | undefined {
  // SQLite exposes these read-only metadata PRAGMAs as table-valued functions.
  // This does not turn assignment PRAGMAs (journal_mode etc.) into reads.
  const withArgument = new Set([
    "table_info",
    "table_xinfo",
    "index_list",
    "index_info",
    "index_xinfo",
    "foreign_key_list",
  ]);
  const withoutArgument = new Set([
    "database_list",
    "compile_options",
    "function_list",
    "module_list",
    "pragma_list",
    "collation_list",
  ]);
  const argument = (token: SqlCapToken | undefined): string | undefined => {
    if (!token || token.kind === "symbol") return undefined;
    if (token.kind === "literal") return token.text;
    let value = token.text;
    if (token.kind === "quoted") {
      const quote = value[0];
      value = value.slice(1, -1);
      if (quote !== "[") value = value.split(quote + quote).join(quote);
    }
    return `'${value.replace(/'/g, "''")}'`;
  };
  let index = 1;
  let schema: string | undefined;
  if (tokens[index + 1]?.text === ".") {
    schema = argument(tokens[index]);
    if (!schema) return undefined;
    index += 2;
  }
  const name = word(tokens[index++]);
  if (withoutArgument.has(name) && index === tokens.length && !schema) {
    return `SELECT * FROM pragma_${name}()`;
  }
  if (!withArgument.has(name)) return undefined;
  const marker = tokens[index++];
  const value = argument(tokens[index++]);
  if (!value) return undefined;
  if (marker?.text === "(") {
    if (tokens[index++]?.text !== ")") return undefined;
  } else if (marker?.text !== "=") return undefined;
  if (index !== tokens.length) return undefined;
  return `SELECT * FROM pragma_${name}(${value}${schema ? `, ${schema}` : ""})`;
}

export interface SqlHardCapRewrite {
  queryText: string;
  decision: SqlHardCapRewriteDecision;
  error?: string;
}

/** Only rewrites a documented subset; the server remains the syntax validator. */
export function applyHardCapToSqlQuery(
  queryText: string,
  connectionType: ConnectionType | undefined,
  hardCap: number,
): SqlHardCapRewrite {
  if (!connectionType || !SQL_TYPES.has(connectionType)) {
    return {
      queryText,
      decision: { applied: false, reason: "unsupported_connection" },
    };
  }
  const reject = (detail: string): SqlHardCapRewrite => ({
    queryText,
    decision: { applied: false, reason: "unsupported_sql" },
    error: `[RapiDB] This query cannot be safely bounded by the configured row limit. ${detail}`,
  });
  if (!Number.isSafeInteger(hardCap) || hardCap < 1) {
    return reject("The row limit must be a positive integer.");
  }
  let tokens: SqlCapToken[];
  try {
    tokens = scanSqlForCap(queryText, connectionType);
  } catch (error) {
    return reject((error as Error).message);
  }
  // Empty separators (including SQL Server's ;WITH convention) are harmless.
  while (tokens[0]?.text === ";") tokens.shift();
  while (tokens.at(-1)?.text === ";") tokens.pop();
  if (tokens.some((token) => token.kind === "symbol" && token.text === ";")) {
    if (connectionType !== "mssql") {
      const statements: SqlHardCapRewrite[] = [];
      let start = 0;
      // Preflight the complete script before the controller can execute any
      // statement. Each result is bounded; transaction semantics stay native.
      for (const token of tokens) {
        if (token.kind !== "symbol" || token.text !== ";") continue;
        if (token.depth !== 0)
          return reject(
            "Procedural scripts require a driver-level result cap.",
          );
        const part = queryText.slice(start, token.start);
        if (scanSqlForCap(part, connectionType).length) {
          const result = applyHardCapToSqlQuery(part, connectionType, hardCap);
          if (result.error) return result;
          statements.push(result);
        }
        start = token.end;
      }
      const last = applyHardCapToSqlQuery(
        queryText.slice(start),
        connectionType,
        hardCap,
      );
      if (last.error) return last;
      statements.push(last);
      return {
        queryText: statements
          .map((statement) => statement.queryText)
          .join("\n;\n"),
        decision: statements.some((statement) => statement.decision.applied)
          ? { applied: true }
          : { applied: false, reason: "non_limitable_statement" },
      };
    }
    return reject(
      "Multiple statements and procedural scripts are unsupported here. Select and run one statement at a time, so no earlier statement executes before a cap error.",
    );
  }
  if (!tokens.length) return reject("Enter one SQL statement.");
  let sqlStart = 0;
  let sql = queryText.slice(sqlStart, tokens[tokens.length - 1].end);
  // Drop leading empty separators without losing comments on ordinary queries.
  if (queryText.slice(0, tokens[0].start).includes(";")) {
    sqlStart = tokens[0].start;
    sql = queryText.slice(sqlStart, tokens[tokens.length - 1].end);
  }
  // Remove only parentheses enclosing the entire query (not set operands).
  while (
    tokens[0]?.text === "(" &&
    tokens.at(-1)?.text === ")" &&
    !tokens.slice(1, -1).some((token) => token.depth === tokens[0].depth)
  ) {
    tokens = tokens.slice(1, -1);
    if (!tokens.length) return reject("Enter a SELECT inside the parentheses.");
    sqlStart = tokens[0].start;
    sql = queryText.slice(sqlStart, tokens[tokens.length - 1].end);
  }
  let first = word(tokens[0]);
  if (
    connectionType === "mysql" &&
    first === "set" &&
    word(tokens[1]) === "statement"
  ) {
    // MariaDB SET STATEMENT scopes settings to an arbitrary statement; unlike
    // ordinary SET it can return an unbounded result. Bound that statement,
    // retaining the settings and native mutation semantics.
    const forIndex = tokens.findIndex(
      (token) => token.depth === tokens[0].depth && word(token) === "for",
    );
    if (forIndex < 0 || !tokens[forIndex + 1])
      return reject(
        "SET STATEMENT requires FOR followed by a supported statement.",
      );
    const innerStart = tokens[forIndex + 1].start;
    const inner = applyHardCapToSqlQuery(
      queryText.slice(innerStart, tokens[tokens.length - 1].end),
      connectionType,
      hardCap,
    );
    if (inner.error) return { ...inner, queryText };
    return {
      queryText: queryText.slice(sqlStart, innerStart) + inner.queryText,
      decision: inner.decision,
    };
  }
  if (
    connectionType === "pg" &&
    first === "show" &&
    tokens.length === 2 &&
    word(tokens[1]) === "all"
  ) {
    return {
      queryText: `SELECT name, setting, description FROM pg_catalog.pg_settings ORDER BY name LIMIT ${hardCap}`,
      decision: { applied: true },
    };
  }
  if (connectionType === "sqlite" && first === "pragma") {
    const metadata = sqlitePragmaSelect(tokens);
    if (metadata)
      return {
        queryText: `${metadata} LIMIT ${hardCap}`,
        decision: { applied: true },
      };
  }
  let ctePrefix = "";
  if (first === "with") {
    // Reuse the read-only guard's CTE structure check, using the dialect-aware
    // lexer above instead of erasing quotes/comments and splitting on ';'.
    // PostgreSQL permits data-modifying CTEs at statement scope; retaining that
    // prefix lets the mutation finish while bounding only the final SELECT.
    if (
      connectionType !== "pg" &&
      (!isReadOnlySqlTokens(
        classificationTokens(tokens).map((token) =>
          connectionType === "sqlite" && token.toLowerCase() === "values"
            ? "select"
            : token,
        ),
      ) ||
        tokens.some(
          (token, index) =>
            !(word(token) === "replace" && tokens[index + 1]?.text === "(") &&
            [
              "insert",
              "update",
              "delete",
              "merge",
              "replace",
              "into",
              "returning",
              "output",
              "exec",
              "execute",
              "call",
            ].includes(word(token)),
        ))
    ) {
      return reject(
        "Only read-only WITH … SELECT is supported. Remove write CTEs/RETURNING and run mutations separately; CTE bodies must be SELECT queries.",
      );
    }
    const baseDepth = tokens[0].depth;
    const mainIndex = tokens.findIndex(
      (token) => token.depth === baseDepth && word(token) === "select",
    );
    if (
      mainIndex < 0 ||
      tokens
        .slice(0, mainIndex)
        .some(
          (token) =>
            token.depth === baseDepth &&
            ["insert", "update", "delete", "merge"].includes(word(token)),
        )
    ) {
      return reject(
        "The WITH clause must end in a standalone SELECT. Simplify the CTE and retry.",
      );
    }
    if (
      (connectionType === "mssql" || connectionType === "oracle") &&
      (word(tokens[1]) === "recursive" ||
        tokens.some(
          (token, index) =>
            word(token) === "with" && tokens[index - 1]?.text === "(",
        ))
    ) {
      return reject(
        "This dialect requires top-level WITH without the RECURSIVE keyword or nested WITH clauses. Use its native recursive CTE syntax.",
      );
    }
    ctePrefix = queryText.slice(sqlStart, tokens[mainIndex].start);
    sqlStart = tokens[mainIndex].start;
    sql = queryText.slice(sqlStart, tokens[tokens.length - 1].end);
    tokens = tokens.slice(mainIndex);
    first = "select";
  }
  if (
    !["select", "values", "table"].includes(first) &&
    tokens[0].text !== "("
  ) {
    if (
      connectionType === "pg" &&
      ["insert", "update", "delete"].includes(first) &&
      tokens.some(
        (token) =>
          token.depth === tokens[0].depth && word(token) === "returning",
      )
    ) {
      // PostgreSQL executes data-modifying CTEs to completion even when the
      // consumer stops early. Never put a limit on the mutation itself.
      let name = "rapidb_returning_cap";
      while (tokens.some((token) => token.text.toLowerCase().includes(name)))
        name += "_";
      return {
        queryText: `WITH ${name} AS (${sql}) SELECT * FROM ${name} LIMIT ${hardCap}`,
        decision: { applied: true },
      };
    }
    if (
      !NON_RESULT_STARTERS.has(first) ||
      (connectionType === "mysql" && first === "analyze") ||
      tokens.some((token) =>
        ["returning", "output", "call", "exec", "execute"].includes(
          word(token),
        ),
      )
    ) {
      return reject(
        "This result-producing command is unsupported. Run a separate SELECT for results; omit RETURNING/OUTPUT from mutations.",
      );
    }
    // T-SQL permits batches without semicolons. Do not pass through a hidden
    // SELECT/CTE or procedural batch after a nominal DML/DDL statement.
    if (
      connectionType === "mssql" &&
      tokens.some(
        (token, index) =>
          (word(token) === "select" && !isSubquerySelect(tokens, index)) ||
          (token.depth === tokens[0].depth &&
            MSSQL_BATCH_WORDS.has(word(token)) &&
            token !== tokens[0]),
      )
    ) {
      return reject(
        "SQL Server batches and top-level SELECT sources in mutations or DDL require a driver-level result cap. Operand subqueries are supported.",
      );
    }
    if (connectionType === "oracle" && first === "begin") {
      return reject(
        "PL/SQL blocks may return implicit results. Run a standalone SQL statement instead.",
      );
    }
    return {
      queryText,
      decision: { applied: false, reason: "non_limitable_statement" },
    };
  }
  if (first === "table" && connectionType !== "pg") {
    return reject("Use SELECT * FROM the table for this dialect.");
  }
  if (
    first === "select" &&
    (tokens.some(
      (token, index) =>
        token.depth === tokens[0].depth &&
        (word(token) === "into" ||
          (word(token) === "for" &&
            ["update", "share", "no", "key"].includes(
              word(tokens[index + 1]),
            )) ||
          (word(token) === "lock" && word(tokens[index + 1]) === "in")),
    ) ||
      (connectionType === "mssql" &&
        !isReadOnlySqlTokens(classificationTokens(tokens))))
  ) {
    return reject(
      "SELECT INTO and locking/mutating SELECT forms are unsupported. Run a read-only SELECT for capped results.",
    );
  }
  if (connectionType === "mssql") {
    // T-SQL does not require statement separators. Without a full T-SQL parser
    // a nested SELECT might instead be an appended parenthesized statement.
    // CTE bodies are outside this final SELECT and remain supported.
    if (
      tokens.some(
        (token, index) =>
          index > 0 &&
          word(token) === "select" &&
          !isSubquerySelect(tokens, index),
      )
    ) {
      return reject(
        "Additional SELECTs in set operations, ambiguous subquery positions or parenthesized batches are unsupported by the SQL Server TOP rewrite. Use top-level CTEs or operand subqueries (EXISTS, IN, FROM/JOIN/APPLY or comparisons).",
      );
    }
    const baseDepth = tokens[0].depth;
    const topLevel = tokens.filter((token) => token.depth === baseDepth);
    const modifier = ["distinct", "all"].includes(word(tokens[1]))
      ? tokens[1]
      : tokens[0];
    const topIndex = tokens.indexOf(modifier) + 1;
    const existingTop = word(tokens[topIndex]) === "top";
    if (
      first !== "select" ||
      topLevel
        .slice(1)
        .some(
          (token) =>
            (MSSQL_BATCH_WORDS.has(word(token)) && word(token) !== "with") ||
            ["union", "except", "intersect", "for"].includes(word(token)),
        )
    ) {
      return reject(
        "This SQL Server SELECT shape is unsupported (set operations or FOR). Literal TOP and ordinary OFFSET/FETCH are supported; other shapes require a driver-level result cap.",
      );
    }
    const offsetIndex = topLevel.findIndex((token) => word(token) === "offset");
    if (offsetIndex >= 0 && !existingTop) {
      const fetchIndex = topLevel.findIndex((token) => word(token) === "fetch");
      if (fetchIndex < 0 && ["row", "rows"].includes(word(topLevel.at(-1)))) {
        return {
          queryText: `${ctePrefix}${sql} FETCH NEXT ${hardCap} ROWS ONLY`,
          decision: { applied: true },
        };
      }
      const count = integerAt(topLevel, fetchIndex + 2);
      if (
        fetchIndex > offsetIndex &&
        ["first", "next"].includes(word(topLevel[fetchIndex + 1])) &&
        count &&
        count.value >= 0n &&
        ["row", "rows"].includes(word(topLevel[count.next])) &&
        word(topLevel[count.next + 1]) === "only" &&
        count.next + 2 === topLevel.length
      ) {
        sql = `${sql.slice(0, count.start - sqlStart)}${count.value > BigInt(hardCap) ? hardCap : count.value}${sql.slice(count.end - sqlStart)}`;
        return { queryText: ctePrefix + sql, decision: { applied: true } };
      }
      return reject(
        "OFFSET pagination requires a final ROWS clause or a literal FETCH FIRST/NEXT count. Expressions in FETCH require a driver-level result cap.",
      );
    }
    if (offsetIndex >= 0 || topLevel.some((token) => word(token) === "fetch")) {
      return reject(
        "TOP cannot be combined with OFFSET/FETCH in the same SELECT.",
      );
    }
    if (existingTop) {
      const parenthesized = tokens[topIndex + 1]?.text === "(";
      const count = integerAt(tokens, topIndex + (parenthesized ? 2 : 1));
      if (
        !count ||
        count.value < 0n ||
        (parenthesized && tokens[count.next]?.text !== ")") ||
        topLevel.some((token) => ["percent", "ties"].includes(word(token)))
      ) {
        return reject(
          "TOP expressions, PERCENT and WITH TIES require a driver-level result cap.",
        );
      }
      sql = `${sql.slice(0, count.start - sqlStart)}${count.value > BigInt(hardCap) ? hardCap : count.value}${sql.slice(count.end - sqlStart)}`;
      return { queryText: ctePrefix + sql, decision: { applied: true } };
    }
    const insertion = modifier.end - sqlStart;
    sql = `${sql.slice(0, insertion)} TOP (${hardCap})${sql.slice(insertion)}`;
  } else if (connectionType === "oracle") {
    sql = `SELECT * FROM (${sql}) rapidb_query_cap FETCH FIRST ${hardCap} ROWS ONLY`;
  } else if (
    (connectionType === "mysql" || connectionType === "sqlite") &&
    first === "select"
  ) {
    sql =
      applyLiteralLimit(sql, tokens, sqlStart, hardCap) ??
      `SELECT * FROM (${sql}) AS rapidb_query_cap LIMIT ${hardCap}`;
  } else {
    sql = `SELECT * FROM (${sql}) AS rapidb_query_cap LIMIT ${hardCap}`;
  }
  return { queryText: ctePrefix + sql, decision: { applied: true } };
}
