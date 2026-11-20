import { oracleAlternativeQuoteEnd } from "../utils/sqlStatementScan";

export interface QuestionMarkPlaceholderOptions {
  dialect?: "pg" | "oracle" | "mssql" | "mysql";
  backslashEscapes?: boolean;
  hashLineComments?: boolean;
  dashCommentRequiresWhitespace?: boolean;
}

function backslashEscapesInQuote(
  sql: string,
  start: number,
  quote: "'" | '"' | "`",
  options: QuestionMarkPlaceholderOptions,
): boolean {
  switch (options.dialect) {
    case "pg":
      return (
        quote === "'" &&
        /[eE]/.test(sql[start - 1] ?? "") &&
        !/[A-Za-z0-9_$\u0080-\uffff]/.test(sql[start - 2] ?? "")
      );
    case "oracle":
    case "mssql":
      return false;
    case "mysql":
      // Backticks quote identifiers, not strings, in both MySQL modes.
      return quote !== "`" && (options.backslashEscapes ?? true);
    default:
      return options.backslashEscapes ?? true;
  }
}

export function questionMarkPlaceholderOffsets(
  sql: string,
  options: QuestionMarkPlaceholderOptions = {},
): number[] {
  const offsets: number[] = [];
  let quote: "'" | '"' | "`" | null = null;
  let bracketIdentifier = false;
  let lineComment = false;
  let blockComment = false;
  let backslashEscapes = false;

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
    if (bracketIdentifier) {
      if (char === "]" && next === "]") index += 1;
      else if (char === "]") bracketIdentifier = false;
      continue;
    }
    if (quote) {
      if (backslashEscapes && char === "\\") index += 1;
      else if (char === quote && next === quote) index += 1;
      else if (char === quote) {
        const continuation = quoteContinuation(sql, index, quote, options);
        if (continuation >= 0) index = continuation;
        else quote = null;
      }
      continue;
    }

    if (options.dialect === "oracle") {
      const end = oracleAlternativeQuoteEnd(sql, index);
      if (end !== undefined) {
        index = end - 1;
        continue;
      }
    }

    if (
      char === "-" &&
      next === "-" &&
      (!options.dashCommentRequiresWhitespace ||
        /\s/.test(sql[index + 2] ?? ""))
    ) {
      lineComment = true;
      index += 1;
    } else if (char === "/" && next === "*") {
      blockComment = true;
      index += 1;
    } else if (options.hashLineComments && char === "#") {
      lineComment = true;
    } else if (char === "[") {
      bracketIdentifier = true;
    } else if (char === "'" || char === '"' || char === "`") {
      quote = char;
      backslashEscapes = backslashEscapesInQuote(sql, index, char, options);
    } else if (char === "?") {
      offsets.push(index);
    }
  }

  return offsets;
}

export interface IndexedPlaceholderOffset {
  start: number;
  end: number;
  index: number;
  text: string;
}

export type IndexedPlaceholderOptions = QuestionMarkPlaceholderOptions;

function postgresStringContinuationQuote(sql: string, start: number): number {
  let index = start;
  let newline = false;
  while (index < sql.length) {
    const char = sql[index];
    if (/[ \t\n\r\f\v]/.test(char)) {
      if (char === "\n" || char === "\r") newline = true;
      index += 1;
    } else if (char === "-" && sql[index + 1] === "-") {
      index += 2;
      while (index < sql.length && sql[index] !== "\n" && sql[index] !== "\r") {
        index += 1;
      }
    } else {
      return newline && char === "'" ? index : -1;
    }
  }
  return -1;
}

function quoteContinuation(
  sql: string,
  index: number,
  quote: "'" | '"' | "`",
  options: QuestionMarkPlaceholderOptions,
): number {
  return options.dialect === "pg" && quote === "'"
    ? postgresStringContinuationQuote(sql, index + 1)
    : -1;
}

export function indexedPlaceholderOffsets(
  sql: string,
  marker: "$" | ":",
  options: IndexedPlaceholderOptions = {},
): IndexedPlaceholderOffset[] {
  const offsets: IndexedPlaceholderOffset[] = [];
  let quote: "'" | '"' | "`" | null = null;
  let bracketIdentifier = false;
  let lineComment = false;
  let blockCommentDepth = 0;
  let backslashEscapes = false;

  for (let index = 0; index < sql.length; index += 1) {
    const char = sql[index];
    const next = sql[index + 1];

    if (lineComment) {
      if (char === "\n" || char === "\r") lineComment = false;
      continue;
    }
    if (blockCommentDepth > 0) {
      if (char === "*" && next === "/") {
        blockCommentDepth -= 1;
        index += 1;
      } else if (options.dialect === "pg" && char === "/" && next === "*") {
        blockCommentDepth += 1;
        index += 1;
      }
      continue;
    }
    if (bracketIdentifier) {
      if (char === "]" && next === "]") index += 1;
      else if (char === "]") bracketIdentifier = false;
      continue;
    }
    if (quote) {
      if (backslashEscapes && char === "\\") index += 1;
      else if (char === quote && next === quote) index += 1;
      else if (char === quote) {
        const continuation = quoteContinuation(sql, index, quote, options);
        if (continuation >= 0) index = continuation;
        else quote = null;
      }
      continue;
    }

    if (options.dialect === "oracle") {
      const end = oracleAlternativeQuoteEnd(sql, index);
      if (end !== undefined) {
        index = end - 1;
        continue;
      }
    }

    if (
      char === "-" &&
      next === "-" &&
      (!options.dashCommentRequiresWhitespace ||
        /\s/.test(sql[index + 2] ?? ""))
    ) {
      lineComment = true;
      index += 1;
    } else if (char === "/" && next === "*") {
      blockCommentDepth = 1;
      index += 1;
    } else if (options.hashLineComments && char === "#") {
      lineComment = true;
    } else if (char === "[" && options.dialect !== "pg") {
      bracketIdentifier = true;
    } else if (char === "'" || char === '"' || char === "`") {
      quote = char;
      backslashEscapes = backslashEscapesInQuote(sql, index, char, options);
    } else if (char === "$") {
      const dollarQuote =
        (options.dialect === undefined || options.dialect === "pg") &&
        !/[A-Za-z0-9_$\u0080-\uffff]/.test(sql[index - 1] ?? "")
          ? /^\$(?:[A-Za-z_\u0080-\uffff][A-Za-z0-9_\u0080-\uffff]*)?\$/.exec(
              sql.slice(index),
            )?.[0]
          : undefined;
      if (dollarQuote) {
        const closing = sql.indexOf(dollarQuote, index + dollarQuote.length);
        index = closing >= 0 ? closing + dollarQuote.length - 1 : sql.length;
        continue;
      }
      const match =
        marker === "$"
          ? /^\$(\d+)(?![A-Za-z0-9_$\u0080-\uffff])/.exec(sql.slice(index))
          : null;
      if (/[A-Za-z0-9_$\u0080-\uffff]/.test(sql[index - 1] ?? "")) continue;
      if (match) {
        const text = match[0];
        offsets.push({
          start: index,
          end: index + text.length,
          index: Number.parseInt(match[1], 10) - 1,
          text,
        });
        index += text.length - 1;
      }
    } else if (marker === ":" && char === ":") {
      if (/[A-Za-z0-9_$#:\u0080-\uffff]/.test(sql[index - 1] ?? "")) continue;
      const match = /^:(\d+)(?![A-Za-z0-9_$#\u0080-\uffff])/.exec(
        sql.slice(index),
      );
      if (match) {
        const text = match[0];
        offsets.push({
          start: index,
          end: index + text.length,
          index: Number.parseInt(match[1], 10) - 1,
          text,
        });
        index += text.length - 1;
      }
    }
  }

  return offsets;
}

export function replaceIndexedPlaceholders(
  sql: string,
  offsets: readonly IndexedPlaceholderOffset[],
  replacement: (placeholder: IndexedPlaceholderOffset) => string,
): string {
  if (offsets.length === 0) return sql;
  let result = "";
  let start = 0;
  for (const placeholder of offsets) {
    result += sql.slice(start, placeholder.start);
    result += replacement(placeholder);
    start = placeholder.end;
  }
  return result + sql.slice(start);
}

export function replaceQuestionMarkPlaceholders(
  sql: string,
  offsets: readonly number[],
  replacement: (index: number) => string,
): string {
  if (offsets.length === 0) return sql;
  let result = "";
  let start = 0;
  offsets.forEach((offset, index) => {
    result += sql.slice(start, offset);
    result += replacement(index);
    start = offset + 1;
  });
  return result + sql.slice(start);
}
