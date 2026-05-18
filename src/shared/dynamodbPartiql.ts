function pushStatement(statements: string[], statement: string): void {
  const trimmed = statement.trim();
  if (trimmed.length > 0) {
    statements.push(trimmed);
  }
}

export function splitDynamoPartiqlStatements(sql: string): string[] {
  const statements: string[] = [];
  let current = "";
  let index = 0;
  let inSingleQuote = false;
  let inDoubleQuote = false;
  let parenDepth = 0;
  let bracketDepth = 0;
  let braceDepth = 0;
  let setDepth = 0;

  while (index < sql.length) {
    const char = sql[index];
    const next = sql[index + 1];

    if (inSingleQuote) {
      current += char;
      if (char === "'" && next === "'") {
        current += next;
        index += 2;
        continue;
      }
      if (char === "'") {
        inSingleQuote = false;
      }
      index += 1;
      continue;
    }

    if (inDoubleQuote) {
      current += char;
      if (char === '"' && next === '"') {
        current += next;
        index += 2;
        continue;
      }
      if (char === '"') {
        inDoubleQuote = false;
      }
      index += 1;
      continue;
    }

    if (char === "-" && next === "-") {
      const newlineIndex = sql.indexOf("\n", index + 2);
      if (newlineIndex === -1) {
        current += sql.slice(index);
        break;
      }
      current += sql.slice(index, newlineIndex + 1);
      index = newlineIndex + 1;
      continue;
    }

    if (char === "/" && next === "*") {
      const commentEnd = sql.indexOf("*/", index + 2);
      if (commentEnd === -1) {
        current += sql.slice(index);
        break;
      }
      current += sql.slice(index, commentEnd + 2);
      index = commentEnd + 2;
      continue;
    }

    if (char === "'") {
      inSingleQuote = true;
      current += char;
      index += 1;
      continue;
    }

    if (char === '"') {
      inDoubleQuote = true;
      current += char;
      index += 1;
      continue;
    }

    if (char === "<" && next === "<") {
      setDepth += 1;
      current += "<<";
      index += 2;
      continue;
    }

    if (char === ">" && next === ">") {
      setDepth = Math.max(0, setDepth - 1);
      current += ">>";
      index += 2;
      continue;
    }

    if (char === "(") {
      parenDepth += 1;
    } else if (char === ")") {
      parenDepth = Math.max(0, parenDepth - 1);
    } else if (char === "[") {
      bracketDepth += 1;
    } else if (char === "]") {
      bracketDepth = Math.max(0, bracketDepth - 1);
    } else if (char === "{") {
      braceDepth += 1;
    } else if (char === "}") {
      braceDepth = Math.max(0, braceDepth - 1);
    } else if (
      char === ";" &&
      parenDepth === 0 &&
      bracketDepth === 0 &&
      braceDepth === 0 &&
      setDepth === 0
    ) {
      pushStatement(statements, current);
      current = "";
      index += 1;
      continue;
    }

    current += char;
    index += 1;
  }

  pushStatement(statements, current);
  return statements;
}
