import { compareNumericTokens } from "../../shared/numericNormalization";

export class JsonNumericPrecisionError extends Error {
  constructor(token: string) {
    super(
      `JSON number ${token.slice(0, 100)} cannot be written without losing numeric precision. Use a string or a supported exact numeric type.`,
    );
    this.name = "JsonNumericPrecisionError";
  }
}

export function parseJsonForWrite(text: string): unknown {
  const parsed: unknown = JSON.parse(text);
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
