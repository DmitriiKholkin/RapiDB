/**
 * Token-preserving JSON helpers.
 *
 * JavaScript's built-in `JSON.parse` collapses every JSON number to a
 * JS `Number`, which silently drops trailing zeros (e.g. `13000.0`
 * becomes `13000`). To round-trip a JSON document without losing that
 * information, we need to keep the original numeric token text.
 *
 * The two helpers below are intentionally narrow: they accept arbitrary
 * JSON text, produce a structural value where every numeric token is
 * kept verbatim (as a `__raw` string), and serialize the structure back
 * to JSON text in a stable key order. They are only used by the
 * persisted-edit verification pipeline and the cell-display fallback,
 * never for general-purpose JSON handling.
 */

const RAW_NUMERIC_TOKEN_RE = /^-?(?:\d+(?:\.\d+)?|\.\d+)(?:[eE][+-]?\d+)?$/;

const JSON_STRING_RE = /"(?:\\.|[^"\\])*"/y;
const JSON_NUMBER_RE = /-?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?/y;
const JSON_KEYWORD_RE = /\b(?:true|false|null)\b/y;

const RAW_NUMBER_TAG = "__rapidbRawNumber" as const;

export interface RawNumberToken {
  readonly [RAW_NUMBER_TAG]: true;
  readonly raw: string;
}

export type CanonicalJsonValue =
  | null
  | boolean
  | string
  | RawNumberToken
  | CanonicalJsonValue[]
  | { [key: string]: CanonicalJsonValue };

function isRawNumberToken(value: unknown): value is RawNumberToken {
  return (
    typeof value === "object" &&
    value !== null &&
    (value as Record<string, unknown>)[RAW_NUMBER_TAG] === true &&
    typeof (value as Record<string, unknown>).raw === "string"
  );
}

function tokenize(input: string): string[] | null {
  const tokens: string[] = [];
  let pos = 0;
  while (pos < input.length) {
    const ch = input[pos];
    if (ch === " " || ch === "\t" || ch === "\n" || ch === "\r") {
      pos++;
      continue;
    }
    if (ch === '"') {
      JSON_STRING_RE.lastIndex = pos;
      const match = JSON_STRING_RE.exec(input);
      if (!match || match.index !== pos) {
        return null;
      }
      tokens.push(match[0]);
      pos += match[0].length;
      continue;
    }
    if (ch === "-" || (ch >= "0" && ch <= "9")) {
      JSON_NUMBER_RE.lastIndex = pos;
      const match = JSON_NUMBER_RE.exec(input);
      if (!match || match.index !== pos) {
        return null;
      }
      tokens.push(match[0]);
      pos += match[0].length;
      continue;
    }
    if (ch === "t" || ch === "f" || ch === "n") {
      JSON_KEYWORD_RE.lastIndex = pos;
      const match = JSON_KEYWORD_RE.exec(input);
      if (!match || match.index !== pos) {
        return null;
      }
      tokens.push(match[0]);
      pos += match[0].length;
      continue;
    }
    if (
      ch === "[" ||
      ch === "]" ||
      ch === "{" ||
      ch === "}" ||
      ch === "," ||
      ch === ":"
    ) {
      tokens.push(ch);
      pos++;
      continue;
    }
    return null;
  }
  return tokens;
}

class JsonTokenParser {
  private pos = 0;
  constructor(private readonly tokens: string[]) {}

  get position(): number {
    return this.pos;
  }

  parseValue(): CanonicalJsonValue | undefined {
    const token = this.tokens[this.pos];
    if (token === undefined) {
      return undefined;
    }
    if (token === "{") {
      return this.parseObject();
    }
    if (token === "[") {
      return this.parseArray();
    }
    if (token === "true") {
      this.pos++;
      return true;
    }
    if (token === "false") {
      this.pos++;
      return false;
    }
    if (token === "null") {
      this.pos++;
      return null;
    }
    if (RAW_NUMERIC_TOKEN_RE.test(token)) {
      this.pos++;
      return { [RAW_NUMBER_TAG]: true, raw: token };
    }
    if (token.startsWith('"')) {
      this.pos++;
      try {
        return JSON.parse(token) as string;
      } catch {
        return undefined;
      }
    }
    return undefined;
  }

  private parseObject(): CanonicalJsonValue | undefined {
    if (this.tokens[this.pos] !== "{") {
      return undefined;
    }
    this.pos++;
    const obj: Record<string, CanonicalJsonValue> = {};
    if (this.tokens[this.pos] === "}") {
      this.pos++;
      return obj;
    }
    while (this.pos < this.tokens.length) {
      const keyToken = this.tokens[this.pos];
      if (!keyToken?.startsWith('"')) {
        return undefined;
      }
      this.pos++;
      let key: string;
      try {
        key = JSON.parse(keyToken) as string;
      } catch {
        return undefined;
      }
      if (this.tokens[this.pos] !== ":") {
        return undefined;
      }
      this.pos++;
      const value = this.parseValue();
      if (value === undefined) {
        return undefined;
      }
      obj[key] = value;
      if (this.tokens[this.pos] === ",") {
        this.pos++;
        continue;
      }
      if (this.tokens[this.pos] === "}") {
        this.pos++;
        return obj;
      }
      return undefined;
    }
    return undefined;
  }

  private parseArray(): CanonicalJsonValue | undefined {
    if (this.tokens[this.pos] !== "[") {
      return undefined;
    }
    this.pos++;
    const arr: CanonicalJsonValue[] = [];
    if (this.tokens[this.pos] === "]") {
      this.pos++;
      return arr;
    }
    while (this.pos < this.tokens.length) {
      const value = this.parseValue();
      if (value === undefined) {
        return undefined;
      }
      arr.push(value);
      if (this.tokens[this.pos] === ",") {
        this.pos++;
        continue;
      }
      if (this.tokens[this.pos] === "]") {
        this.pos++;
        return arr;
      }
      return undefined;
    }
    return undefined;
  }
}

export function parseJsonPreservingRawNumbers(
  text: string,
): CanonicalJsonValue | undefined {
  const tokens = tokenize(text);
  if (tokens === null) {
    return undefined;
  }
  const parser = new JsonTokenParser(tokens);
  const value = parser.parseValue();
  if (value === undefined || parser.position !== tokens.length) {
    return undefined;
  }
  return value;
}

export function serializeCanonicalJson(value: CanonicalJsonValue): string {
  if (value === null) {
    return "null";
  }
  if (typeof value === "boolean") {
    return value ? "true" : "false";
  }
  if (typeof value === "string") {
    return JSON.stringify(value);
  }
  if (isRawNumberToken(value)) {
    return value.raw;
  }
  if (Array.isArray(value)) {
    return `[${value.map((entry) => serializeCanonicalJson(entry)).join(",")}]`;
  }
  const entries = Object.keys(value)
    .sort()
    .map(
      (key) =>
        `${JSON.stringify(key)}:${serializeCanonicalJson((value as Record<string, CanonicalJsonValue>)[key])}`,
    );
  return `{${entries.join(",")}}`;
}

export function canonicalizeJsonPreservingRawNumbers(
  text: string,
): string | null {
  const parsed = parseJsonPreservingRawNumbers(text);
  if (parsed === undefined) {
    return null;
  }
  return serializeCanonicalJson(parsed);
}
