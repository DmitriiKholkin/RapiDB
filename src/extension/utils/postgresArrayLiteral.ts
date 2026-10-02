import {
  hasPostgresArrayBounds,
  parsePostgresArrayLiteral,
} from "./postgresArrayParser";

// JSON numbers are never converted to JS Number: send their exact tokens to PG.
const JSON_STRING_RE = /"(?:\\.|[^"\\])*"/y;
const JSON_NUMBER_RE = /-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?/y;
const JSON_KEYWORD_RE = /(?:true|false|null)/y;

function escapePgString(value: string): string {
  // PG array escapes quote/backslash, not JSON control sequences. Real controls
  // inside a quoted element must be sent unchanged (\n would store the letter n).
  return `"${value.replace(/["\\]/g, (ch) => `\\${ch}`)}"`;
}

class JsonArrayConverter {
  private pos = 0;
  constructor(private readonly text: string) {}

  private fail(): never {
    throw new Error(`Invalid JSON array literal at position ${this.pos}`);
  }

  private skipWhitespace(): void {
    while (this.pos < this.text.length && /[ \t\n\r]/.test(this.text[this.pos]))
      this.pos++;
  }

  private readToken(pattern: RegExp): string {
    pattern.lastIndex = this.pos;
    const match = pattern.exec(this.text);
    if (!match) return this.fail();
    this.pos += match[0].length;
    return match[0];
  }

  private readString(): string {
    return JSON.parse(this.readToken(JSON_STRING_RE)) as string;
  }

  private readValue(): string {
    this.skipWhitespace();
    const ch = this.text[this.pos];
    if (ch === "[") return this.readContainer(false);
    if (ch === "{") return this.readContainer(true);
    if (ch === '"') return escapePgString(this.readString());
    if (ch === "-" || (ch >= "0" && ch <= "9"))
      return this.readToken(JSON_NUMBER_RE);
    if (ch === "t" || ch === "f" || ch === "n") {
      const token = this.readToken(JSON_KEYWORD_RE);
      return token === "null" ? "NULL" : token;
    }
    return this.fail();
  }

  private readContainer(object: boolean): string {
    const start = this.pos++;
    const end = object ? "}" : "]";
    const parts: string[] = [];
    this.skipWhitespace();
    if (this.text[this.pos] !== end) {
      while (this.pos < this.text.length) {
        this.skipWhitespace();
        if (object) {
          if (this.text[this.pos] !== '"') this.fail();
          this.readString();
          this.skipWhitespace();
          if (this.text[this.pos++] !== ":") this.fail();
        }
        parts.push(this.readValue());
        this.skipWhitespace();
        if (this.text[this.pos] === end) break;
        if (this.text[this.pos++] !== ",") this.fail();
        // readValue/readString rejects trailing commas and missing values.
      }
    }
    if (this.text[this.pos++] !== end) this.fail();
    // Objects are scalar JSON-text elements, including all their nested JSON
    // arrays/nulls/strings. Escape once for the surrounding PG array grammar.
    return object
      ? escapePgString(this.text.slice(start, this.pos))
      : `{${parts.join(",")}}`;
  }

  convert(): string {
    const result = this.readValue();
    this.skipWhitespace();
    if (this.pos !== this.text.length) this.fail();
    return result;
  }
}

/** Convert JSON-syntax arrays to PG array text while preserving numeric tokens. */
export function jsonArrayLiteralToPgArrayLiteral(input: string): string {
  const trimmed = input.trim();
  if (trimmed === "") return "{}";
  if (trimmed.startsWith("{") || hasPostgresArrayBounds(trimmed)) {
    parsePostgresArrayLiteral(trimmed);
    return trimmed;
  }
  if (!trimmed.startsWith("[")) return trimmed;
  const result = new JsonArrayConverter(trimmed).convert();
  parsePostgresArrayLiteral(result);
  return result;
}
