/**
 * Convert a JSON-syntax array literal (e.g. `[13000.0, "x", null]`) into a
 * PostgreSQL array literal (e.g. `{13000.0,"x",NULL}`).
 *
 * The converter uses a small tokenizer that mirrors the JSON grammar but
 * preserves every numeric token verbatim, so trailing zeros in stored
 * `numeric[]` values round-trip through the user-facing JSON dialog and
 * the actual SQL parameter that we send to PostgreSQL.
 */
const JSON_STRING_RE = /"(?:\\.|[^"\\])*"/y;
const JSON_NUMBER_RE = /-?(?:\d+(?:\.\d+)?|\.\d+)(?:[eE][+-]?\d+)?/y;
const JSON_KEYWORD_RE = /\b(?:true|false|null)\b/y;

function escapePgString(token: string): string {
  let out = "";
  for (const ch of token) {
    switch (ch) {
      case "\\":
        out += "\\\\";
        break;
      case '"':
        out += '\\"';
        break;
      case "\n":
        out += "\\n";
        break;
      case "\r":
        out += "\\r";
        break;
      case "\t":
        out += "\\t";
        break;
      case "\b":
        out += "\\b";
        break;
      case "\f":
        out += "\\f";
        break;
      case "\v":
        out += "\\v";
        break;
      default:
        out += ch;
        break;
    }
  }
  return `"${out}"`;
}

function jsonArrayToPgArrayInner(
  text: string,
  position: { pos: number },
): string {
  const skipWhitespace = (): void => {
    while (position.pos < text.length) {
      const ch = text[position.pos];
      if (ch === " " || ch === "\t" || ch === "\n" || ch === "\r") {
        position.pos++;
        continue;
      }
      break;
    }
  };

  skipWhitespace();
  if (text[position.pos] !== "[") {
    throw new Error("Expected '[' at start of JSON array");
  }
  position.pos++;
  const parts: string[] = [];
  skipWhitespace();
  if (text[position.pos] === "]") {
    position.pos++;
    return "{}";
  }
  while (position.pos < text.length) {
    skipWhitespace();
    const ch = text[position.pos];
    if (ch === "]") {
      position.pos++;
      return `{${parts.join(",")}}`;
    }
    if (ch === "{") {
      const inner = jsonObjectToPgValue(text, position);
      parts.push(inner);
    } else if (ch === "[") {
      const inner = jsonArrayToPgArrayInner(text, position);
      parts.push(inner);
    } else if (ch === '"') {
      JSON_STRING_RE.lastIndex = position.pos;
      const match = JSON_STRING_RE.exec(text);
      if (!match || match.index !== position.pos) {
        throw new Error("Invalid JSON string in array");
      }
      position.pos += match[0].length;
      const parsed = JSON.parse(match[0]) as string;
      parts.push(escapePgString(parsed));
    } else if (ch === "t" || ch === "f" || ch === "n") {
      JSON_KEYWORD_RE.lastIndex = position.pos;
      const match = JSON_KEYWORD_RE.exec(text);
      if (!match || match.index !== position.pos) {
        throw new Error("Invalid JSON keyword in array");
      }
      position.pos += match[0].length;
      if (match[0] === "null") {
        parts.push("NULL");
      } else if (match[0] === "true") {
        parts.push("true");
      } else {
        parts.push("false");
      }
    } else if (ch === "-" || (ch >= "0" && ch <= "9")) {
      JSON_NUMBER_RE.lastIndex = position.pos;
      const match = JSON_NUMBER_RE.exec(text);
      if (!match || match.index !== position.pos) {
        throw new Error("Invalid JSON number in array");
      }
      position.pos += match[0].length;
      parts.push(match[0]);
    } else {
      throw new Error(`Unexpected character '${ch}' in JSON array`);
    }
    skipWhitespace();
    if (text[position.pos] === ",") {
      position.pos++;
    }
  }
  throw new Error("Unterminated JSON array");
}

function jsonObjectToPgValue(text: string, position: { pos: number }): string {
  const skipWhitespace = (): void => {
    while (position.pos < text.length) {
      const ch = text[position.pos];
      if (ch === " " || ch === "\t" || ch === "\n" || ch === "\r") {
        position.pos++;
        continue;
      }
      break;
    }
  };

  if (text[position.pos] !== "{") {
    throw new Error("Expected '{' at start of JSON object");
  }
  position.pos++;
  const parts: string[] = [];
  skipWhitespace();
  if (text[position.pos] === "}") {
    position.pos++;
    return '"{}"';
  }
  while (position.pos < text.length) {
    skipWhitespace();
    if (text[position.pos] === "}") {
      position.pos++;
      return escapePgString(`{${parts.join(",")}}`);
    }
    if (text[position.pos] === '"') {
      JSON_STRING_RE.lastIndex = position.pos;
      const match = JSON_STRING_RE.exec(text);
      if (!match || match.index !== position.pos) {
        throw new Error("Invalid JSON string in object");
      }
      position.pos += match[0].length;
      const key = JSON.parse(match[0]) as string;
      skipWhitespace();
      if (text[position.pos] !== ":") {
        throw new Error("Expected ':' after JSON object key");
      }
      position.pos++;
      skipWhitespace();
      const valueCh = text[position.pos];
      let valueText = "";
      if (valueCh === "{") {
        valueText = jsonObjectToPgValue(text, position);
      } else if (valueCh === "[") {
        valueText = jsonArrayToPgArrayInner(text, position);
      } else if (valueCh === '"') {
        JSON_STRING_RE.lastIndex = position.pos;
        const valueMatch = JSON_STRING_RE.exec(text);
        if (!valueMatch || valueMatch.index !== position.pos) {
          throw new Error("Invalid JSON string value");
        }
        position.pos += valueMatch[0].length;
        valueText = escapePgString(JSON.parse(valueMatch[0]) as string);
      } else if (valueCh === "t" || valueCh === "f" || valueCh === "n") {
        JSON_KEYWORD_RE.lastIndex = position.pos;
        const valueMatch = JSON_KEYWORD_RE.exec(text);
        if (!valueMatch || valueMatch.index !== position.pos) {
          throw new Error("Invalid JSON keyword value");
        }
        position.pos += valueMatch[0].length;
        valueText =
          valueMatch[0] === "null"
            ? "NULL"
            : valueMatch[0] === "true"
              ? "true"
              : "false";
      } else if (valueCh === "-" || (valueCh >= "0" && valueCh <= "9")) {
        JSON_NUMBER_RE.lastIndex = position.pos;
        const valueMatch = JSON_NUMBER_RE.exec(text);
        if (!valueMatch || valueMatch.index !== position.pos) {
          throw new Error("Invalid JSON number value");
        }
        position.pos += valueMatch[0].length;
        valueText = valueMatch[0];
      } else {
        throw new Error(`Unexpected character '${valueCh}' in JSON object`);
      }
      parts.push(`${escapePgString(key)}:${valueText}`);
    } else {
      throw new Error(
        `Unexpected character '${text[position.pos]}' in JSON object`,
      );
    }
    skipWhitespace();
    if (text[position.pos] === ",") {
      position.pos++;
    }
  }
  throw new Error("Unterminated JSON object");
}

export function jsonArrayLiteralToPgArrayLiteral(input: string): string {
  const trimmed = input.trim();
  if (trimmed === "") {
    return "{}";
  }
  if (trimmed.startsWith("{")) {
    return trimmed;
  }
  if (!trimmed.startsWith("[")) {
    return trimmed;
  }
  const position = { pos: 0 };
  return jsonArrayToPgArrayInner(trimmed, position);
}
