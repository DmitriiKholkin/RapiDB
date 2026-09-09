export interface QuestionMarkPlaceholderOptions {
  hashLineComments?: boolean;
  dashCommentRequiresWhitespace?: boolean;
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
      if (char === "\\") index += 1;
      else if (char === quote && next === quote) index += 1;
      else if (char === quote) quote = null;
      continue;
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

export function indexedPlaceholderOffsets(
  sql: string,
  marker: "$" | ":",
  options: QuestionMarkPlaceholderOptions = {},
): IndexedPlaceholderOffset[] {
  const offsets: IndexedPlaceholderOffset[] = [];
  let quote: "'" | '"' | "`" | null = null;
  let bracketIdentifier = false;
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
    if (bracketIdentifier) {
      if (char === "]" && next === "]") index += 1;
      else if (char === "]") bracketIdentifier = false;
      continue;
    }
    if (quote) {
      if (char === "\\") index += 1;
      else if (char === quote && next === quote) index += 1;
      else if (char === quote) quote = null;
      continue;
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
    } else if (char === "$") {
      const dollarQuote = /^\$[A-Za-z_][A-Za-z0-9_]*\$|^\$\$/.exec(
        sql.slice(index),
      )?.[0];
      if (dollarQuote) {
        const closing = sql.indexOf(dollarQuote, index + dollarQuote.length);
        if (closing >= 0) {
          index = closing + dollarQuote.length - 1;
          continue;
        }
      }
      const match = marker === "$" ? /^\$(\d+)/.exec(sql.slice(index)) : null;
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
      const match = /^:(\d+)/.exec(sql.slice(index));
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
