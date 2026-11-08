import { compareNumericTokens } from "../../shared/numericNormalization";

export class JsonNumericPrecisionError extends Error {
  constructor(token: string) {
    super(
      `JSON number ${token.slice(0, 100)} cannot be written without losing numeric precision. Use a string or a supported exact numeric type.`,
    );
    this.name = "JsonNumericPrecisionError";
  }
}

export function isLosslessJsonNumberToken(token: string): boolean {
  const number = Number(token);
  if (!Number.isFinite(number)) return false;
  if (
    Number.isInteger(number) &&
    compareNumericTokens(token, BigInt(number).toString()) === 0
  )
    return true;
  const shortest = String(number);
  return (
    /[.eE]/.test(token) &&
    /[.e]/.test(shortest) &&
    compareNumericTokens(token, shortest) === 0
  );
}

export function isRoundTripJsonNumberToken(token: string): boolean {
  return (
    isLosslessJsonNumberToken(token) &&
    compareNumericTokens(token, String(Number(token))) === 0
  );
}

export function parseJsonForWrite(
  text: string,
  isSafeToken: (token: string) => boolean = isLosslessJsonNumberToken,
): unknown {
  const parsed: unknown = JSON.parse(text);
  const tokens = /"(?:\\.|[^"\\])*"|-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?/g;
  for (const match of text.matchAll(tokens)) {
    const token = match[0];
    if (token.startsWith('"')) continue;
    if (!isSafeToken(token)) {
      throw new JsonNumericPrecisionError(token);
    }
  }
  return parsed;
}
