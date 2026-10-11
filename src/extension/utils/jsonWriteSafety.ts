import { compareNumericTokens } from "../../shared/numericNormalization";

export class JsonNumericPrecisionError extends Error {
  constructor(token: string) {
    super(
      `JSON number ${token.slice(0, 100)} cannot be written without losing numeric precision. Use a string or a supported exact numeric type.`,
    );
    this.name = "JsonNumericPrecisionError";
  }
}

/** Parse JSON for Number-based SDKs, refusing silent numeric changes.
 * Ordinary decimals follow decimal JSON round-trip semantics (0.1 is allowed).
 * Integers are compared with the actual IEEE-754 integer, also safe for BSON.
 */
export function parseJsonForWrite(text: string): unknown {
  const parsed: unknown = JSON.parse(text);
  // Syntax has already been validated. Match strings atomically so quoted
  // numbers/escaped keys are never mistaken for numeric tokens. Check every
  // token, including duplicate object keys, before returning the parsed value.
  const tokens = /"(?:\\.|[^"\\])*"|-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?/g;
  for (const match of text.matchAll(tokens)) {
    const token = match[0];
    if (token.startsWith('"')) continue;
    const number = Number(token);
    const actual = Number.isFinite(number)
      ? Number.isInteger(number)
        ? BigInt(number).toString()
        : String(number)
      : null;
    if (actual === null || compareNumericTokens(token, actual) !== 0) {
      throw new JsonNumericPrecisionError(token);
    }
  }
  return parsed;
}
