const JSON_NUMBER_RE = /-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?/y;
const JSON_KEYWORDS = ["true", "false", "null"] as const;

type JsonTokenKind = "value" | "{" | "}" | "[" | "]" | "," | ":";

interface JsonToken {
  kind: JsonTokenKind;
  text: string;
  offset: number;
  isString: boolean;
}

class JsonFormatError extends Error {}

function describeToken(token: JsonToken | undefined): string {
  return token
    ? `Unexpected token ${token.text.slice(0, 20)} at position ${token.offset}`
    : "Unexpected end of JSON input";
}

function readTokenAt(
  input: string,
  pos: number,
  re: RegExp,
): string | undefined {
  re.lastIndex = pos;
  return re.exec(input)?.[0];
}

// A repeated regexp group can overflow irregexp's stack on large valid strings.
// Scan code units instead; JSON permits lone surrogates, but not raw controls.
function readStringAt(input: string, start: number): string {
  for (let pos = start + 1; pos < input.length; pos++) {
    const code = input.charCodeAt(pos);
    if (code === 0x22) return input.slice(start, pos + 1);
    if (code < 0x20) {
      throw new JsonFormatError(`Invalid control character at position ${pos}`);
    }
    if (code !== 0x5c) continue;
    const escapeCode = input[++pos];
    if (escapeCode === "u") {
      for (let digit = 0; digit < 4; digit++) {
        const hex = input.charCodeAt(++pos);
        if (
          !(
            (hex >= 0x30 && hex <= 0x39) ||
            (hex >= 0x41 && hex <= 0x46) ||
            (hex >= 0x61 && hex <= 0x66)
          )
        ) {
          throw new JsonFormatError(
            `Invalid Unicode escape at position ${pos}`,
          );
        }
      }
    } else if (escapeCode === undefined || !'"\\/bfnrt'.includes(escapeCode)) {
      throw new JsonFormatError(`Invalid escape at position ${pos}`);
    }
  }
  throw new JsonFormatError("Unexpected end of JSON string");
}

function tokenize(input: string): JsonToken[] {
  const tokens: JsonToken[] = [];
  let pos = 0;
  while (pos < input.length) {
    const ch = input[pos];
    if (ch === " " || ch === "\t" || ch === "\n" || ch === "\r") {
      pos++;
      continue;
    }
    if (
      ch === "{" ||
      ch === "}" ||
      ch === "[" ||
      ch === "]" ||
      ch === "," ||
      ch === ":"
    ) {
      tokens.push({ kind: ch, text: ch, offset: pos, isString: false });
      pos++;
      continue;
    }
    let text: string | undefined;
    if (ch === '"') {
      text = readStringAt(input, pos);
    } else if (ch === "-" || (ch >= "0" && ch <= "9")) {
      text = readTokenAt(input, pos, JSON_NUMBER_RE);
    } else {
      text = JSON_KEYWORDS.find((keyword) => input.startsWith(keyword, pos));
    }
    if (!text) {
      throw new JsonFormatError(`Unexpected token ${ch} at position ${pos}`);
    }
    tokens.push({ kind: "value", text, offset: pos, isString: ch === '"' });
    pos += text.length;
  }
  return tokens;
}

function validate(tokens: readonly JsonToken[]): void {
  const stack: ("{" | "[")[] = [];
  let pos = 0;
  const consume = (predicate: (token: JsonToken) => boolean): JsonToken => {
    const token = tokens[pos];
    if (!token || !predicate(token)) {
      throw new JsonFormatError(describeToken(token));
    }
    pos++;
    return token;
  };
  const expectKey = () => {
    consume((token) => token.isString);
    consume((token) => token.kind === ":");
  };

  let needValue = true;
  while (true) {
    if (needValue) {
      const token = consume(
        (t) => t.kind === "value" || t.kind === "{" || t.kind === "[",
      );
      if (token.kind !== "{" && token.kind !== "[") {
        needValue = false;
      } else {
        stack.push(token.kind);
        const close = token.kind === "{" ? "}" : "]";
        if (tokens[pos]?.kind === close) {
          pos++;
          stack.pop();
          needValue = false;
        } else if (token.kind === "{") {
          expectKey();
        }
      }
      continue;
    }
    const container = stack[stack.length - 1];
    if (!container) {
      break;
    }
    const close = container === "{" ? "}" : "]";
    const token = consume((t) => t.kind === "," || t.kind === close);
    if (token.kind === ",") {
      if (container === "{") {
        expectKey();
      }
      needValue = true;
    } else {
      stack.pop();
    }
  }
  if (pos !== tokens.length) {
    throw new JsonFormatError(describeToken(tokens[pos]));
  }
}

/**
 * Pretty-prints JSON with 2-space indentation without round-tripping through
 * JSON.parse, so number literals, duplicate keys and string escapes are kept
 * byte-for-byte. Input must be strictly valid JSON.
 */
export function formatJsonPreservingTokens(
  input: string,
): { result: string } | { error: string } {
  if (!input.trim()) {
    return { result: input };
  }
  try {
    const tokens = tokenize(input);
    validate(tokens);

    // Pretty-printing has quadratic expansion at extreme nesting depths. Bound
    // allocations before creating indentation, while allowing large flat input.
    const maxLength = Math.max(16 * 1024 * 1024, input.length * 4);
    const parts: string[] = [];
    let length = 0;
    let indent = 0;
    const reserve = (size: number) => {
      if (length + size > maxLength) {
        throw new JsonFormatError(
          "Formatted JSON exceeds the safe output size limit",
        );
      }
      length += size;
    };
    const append = (text: string) => {
      reserve(text.length);
      parts.push(text);
    };
    const indentAfter = (text: string) => {
      reserve(text.length + indent * 2);
      parts.push(text, "  ".repeat(indent));
    };
    for (let i = 0; i < tokens.length; i++) {
      const { kind, text } = tokens[i];
      if (kind === "{" || kind === "[") {
        const next = tokens[i + 1]?.kind;
        if (next === "}" || next === "]") {
          append(text);
          append(tokens[++i].text);
          continue;
        }
        indent++;
        indentAfter(`${text}\n`);
      } else if (kind === "}" || kind === "]") {
        indent--;
        indentAfter("\n");
        append(text);
      } else if (kind === ",") {
        indentAfter(",\n");
      } else if (kind === ":") {
        append(": ");
      } else {
        append(text);
      }
    }
    return { result: parts.join("") };
  } catch (err: unknown) {
    return { error: err instanceof Error ? err.message : String(err) };
  }
}
