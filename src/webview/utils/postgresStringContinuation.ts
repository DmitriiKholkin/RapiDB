function endOfQuote(
  sql: string,
  start: number,
  quote: string,
  backslashEscapes: boolean,
): number {
  for (let i = start; i < sql.length; i++) {
    if (backslashEscapes && sql[i] === "\\") {
      i++;
    } else if (sql[i] === quote) {
      if (sql[i + 1] !== quote) return i + 1;
      i++;
    }
  }
  return sql.length;
}

function endOfLineComment(sql: string, start: number): number {
  while (start < sql.length && sql[start] !== "\r" && sql[start] !== "\n") {
    start++;
  }
  return start;
}

function hasContinuationAt(sql: string, start: number): boolean {
  let newline = false;
  for (let i = start; i < sql.length; i++) {
    const char = sql[i];
    if (char === "\r" || char === "\n") {
      newline = true;
    } else if (char === "-" && sql[i + 1] === "-") {
      i = endOfLineComment(sql, i + 2) - 1;
    } else if (!/[ \t\f\v]/.test(char)) {
      return newline && char === "'";
    }
  }
  return false;
}

function scanForContinuation(
  sql: string,
  standardConformingStrings: boolean,
): boolean {
  // Match whole unquoted identifiers so an embedded "$tag$" or "e" is not
  // mistaken for the start of a dollar-quoted or escape string.
  const token =
    /--|\/\*|(?:e|b|x|u&)?'|"|\$(?:[a-z_\u0080-\uffff][a-z0-9_\u0080-\uffff]*)?\$|[a-z_\u0080-\uffff][a-z0-9_$\u0080-\uffff]*/gi;
  const blockMarker = /\/\*|\*\//g;

  for (let match = token.exec(sql); match; match = token.exec(sql)) {
    const text = match[0];
    if (text === "--") {
      token.lastIndex = endOfLineComment(sql, token.lastIndex);
    } else if (text === "/*") {
      blockMarker.lastIndex = token.lastIndex;
      let depth = 1;
      while (depth > 0) {
        const marker = blockMarker.exec(sql);
        if (!marker) break;
        depth += marker[0] === "/*" ? 1 : -1;
      }
      token.lastIndex = depth === 0 ? blockMarker.lastIndex : sql.length;
    } else if (text[0] === "$") {
      const close = sql.indexOf(text, token.lastIndex);
      token.lastIndex = close === -1 ? sql.length : close + text.length;
    } else if (text === '"') {
      token.lastIndex = endOfQuote(sql, token.lastIndex, '"', false);
    } else if (text.endsWith("'")) {
      const backslashEscapes =
        text.toLowerCase() === "e'" ||
        (text === "'" && !standardConformingStrings);
      token.lastIndex = endOfQuote(sql, token.lastIndex, "'", backslashEscapes);
      // A newline must be outside the literal. PostgreSQL permits line
      // comments in this gap, but a block comment ends the continuation.
      if (hasContinuationAt(sql, token.lastIndex)) return true;
    }
  }
  return false;
}

export function hasPostgresStringContinuation(sql: string): boolean {
  if (!sql.includes("'") || !/[\r\n]/.test(sql)) return false;
  return (
    scanForContinuation(sql, true) ||
    // Monaco does not know the session's standard_conforming_strings setting.
    (sql.includes("\\") && scanForContinuation(sql, false))
  );
}
