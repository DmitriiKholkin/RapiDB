import { compareExactNumericValues } from "./exactNumericComparison";

export const DYNAMODB_NATIVE_CELL_VALUE_PREFIX =
  "DynamoDB native value (nested sets/binary): ";

export type DynamoDbSetSubtype = "string" | "number" | "binary";
const NUMBER_RE = /^([+-]?)(?:(\d+)(?:\.(\d+))?|\.(\d+))(?:[eE]([+-]?\d+))?$/;
const NUMBER_SET_INPUT_ERROR =
  "Enter a valid DynamoDB number set containing at least one number.";
const MAX_NUMBER_PRECISION = 38;
const MIN_NUMBER_ORDER = -130;
const MAX_NUMBER_ORDER = 125;
const MAX_NUMBER_DIGITS = "9".repeat(MAX_NUMBER_PRECISION);
const BASE64_ALPHABET =
  "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

class DynamoDbSetInputError extends Error {}

export function hasDuplicateDynamoDbNumbers(
  entries: readonly string[],
): boolean {
  const sorted = [...entries].sort(compareExactNumericValues);
  return sorted.some(
    (entry, index) =>
      index > 0 && compareExactNumericValues(entry, sorted[index - 1]) === 0,
  );
}

export function parseDynamoDbNumberSetJson(text: string): string[] {
  // Quote bare JSON numbers before parsing so even large values stay exact.
  const quoted = text.replace(
    /"(?:\\.|[^"\\])*"|-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?/g,
    (token) => (token.startsWith('"') ? token : `"${token}"`),
  );
  let parsed: unknown;
  try {
    parsed = JSON.parse(quoted);
  } catch {
    throw new DynamoDbSetInputError(NUMBER_SET_INPUT_ERROR);
  }
  return parseDynamoDbNumberSetEntries(parsed);
}

export function parseDynamoDbNumberSetInput(text: string): string[] {
  const trimmed = text.trim();
  if (trimmed.startsWith("<<"))
    return parseDynamoDbSetLiteral(trimmed, "number");
  if (trimmed.startsWith("[")) return parseDynamoDbNumberSetJson(trimmed);

  // Preserve the existing plain comma-separated form.
  const entries = trimmed
    .replace(/>>$/, "")
    .replace(/\]$/, "")
    .split(",")
    .map((entry) => entry.trim())
    .filter(Boolean);
  if (entries.some((entry) => entry.startsWith("+")))
    throw new DynamoDbSetInputError(NUMBER_SET_INPUT_ERROR);
  return parseDynamoDbNumberSetEntries(entries);
}

export function parseDynamoDbNumberSetEntries(values: unknown): string[] {
  if (
    !Array.isArray(values) ||
    values.length === 0 ||
    !values.every((entry): entry is string => typeof entry === "string")
  )
    throw new DynamoDbSetInputError(NUMBER_SET_INPUT_ERROR);

  const comparisonTokens = values.map((entry) => {
    const match = NUMBER_RE.exec(entry);
    if (!match) throw new DynamoDbSetInputError(NUMBER_SET_INPUT_ERROR);
    const [, sign, integer = "", fraction, leadingDotFraction, rawExponent] =
      match;
    const digits = integer + (fraction ?? leadingDotFraction ?? "");
    const firstNonzero = digits.search(/[1-9]/);
    if (firstNonzero === -1) return "0";

    // The bound allows the coefficient's decimal position to cancel even a
    // large exponent. Only convert the small, exact integer needed for this.
    const decimalOffset = integer.length - firstNonzero - 1;
    const exponentDigits = rawExponent?.replace(/^[+-]?0*/, "") ?? "";
    const exponentMagnitude =
      exponentDigits.length >
      String(Math.abs(decimalOffset) - MIN_NUMBER_ORDER).length
        ? Infinity
        : Number(exponentDigits);
    const order =
      decimalOffset +
      (rawExponent?.startsWith("-") ? -exponentMagnitude : exponentMagnitude);
    if (order < MIN_NUMBER_ORDER)
      throw new DynamoDbSetInputError(
        "DynamoDB number set value underflows the minimum nonzero magnitude (1e-130).",
      );

    let lastNonzero = digits.length - 1;
    while (digits[lastNonzero] === "0") lastNonzero--;
    const precision = lastNonzero - firstNonzero + 1;
    if (
      order > MAX_NUMBER_ORDER ||
      (order === MAX_NUMBER_ORDER &&
        precision > MAX_NUMBER_PRECISION &&
        digits.startsWith(MAX_NUMBER_DIGITS, firstNonzero))
    )
      throw new DynamoDbSetInputError(
        "DynamoDB number set value overflows the maximum magnitude.",
      );
    if (precision > MAX_NUMBER_PRECISION)
      throw new DynamoDbSetInputError(
        "DynamoDB number set values may have at most 38 significant digits.",
      );

    // Compare bounded, exact tokens; keep the original spelling for writes.
    return `${sign}${digits.slice(firstNonzero, lastNonzero + 1)}e${order - precision + 1}`;
  });
  if (hasDuplicateDynamoDbNumbers(comparisonTokens))
    throw new DynamoDbSetInputError(
      "DynamoDB number set values must be unique.",
    );
  return values;
}

function isCanonicalBase64(value: string): boolean {
  if (value.length % 4 !== 0 || !/^[A-Za-z0-9+/]*={0,2}$/.test(value))
    return false;
  if (value.endsWith("=="))
    return (BASE64_ALPHABET.indexOf(value.at(-3) ?? "") & 15) === 0;
  if (value.endsWith("="))
    return (BASE64_ALPHABET.indexOf(value.at(-2) ?? "") & 3) === 0;
  return true;
}

export function parseDynamoDbBinarySetEntries(
  values: unknown,
  format: "cell" | "envelope",
): string[] {
  const fail = (): never => {
    throw new DynamoDbSetInputError(
      format === "envelope"
        ? "Enter a non-empty DynamoDB binary set of canonical base64 strings in the AttributeValue envelope."
        : "Enter a non-empty DynamoDB binary set of hex or canonical base64 strings.",
    );
  };
  if (!Array.isArray(values) || values.length === 0) return fail();
  const base64Values = values.map((value: unknown) => {
    if (typeof value !== "string") return fail();
    const entry = format === "cell" ? value.trim() : value;
    if (format === "cell" && /^(?:0x|\\x)/i.test(entry)) {
      const hex = entry.slice(2);
      if (hex.length % 2 !== 0 || !/^[a-f\d]*$/i.test(hex)) return fail();
      return btoa(
        hex.replace(/../g, (byte) =>
          String.fromCharCode(Number.parseInt(byte, 16)),
        ),
      );
    }
    // An explicit empty string is empty binary; whitespace alone is not.
    if ((!entry && value) || !isCanonicalBase64(entry)) return fail();
    return entry;
  });
  if (new Set(base64Values).size !== base64Values.length) {
    throw new DynamoDbSetInputError(
      "DynamoDB binary set values must be unique.",
    );
  }
  return base64Values;
}

export function parseDynamoDbBinarySetInput(text: string): string[] {
  const trimmed = text.trim();
  if (trimmed.startsWith("<<"))
    return parseDynamoDbSetLiteral(trimmed, "binary");
  let parsed: unknown;
  try {
    parsed = JSON.parse(trimmed);
  } catch {}
  return parseDynamoDbBinarySetEntries(parsed, "cell");
}

function isNonEmptyStringArray(value: unknown): value is string[] {
  return (
    Array.isArray(value) &&
    value.length > 0 &&
    value.every((entry) => typeof entry === "string")
  );
}

export function parseDynamoDbStringSetInput(text: string): string[] | null {
  const trimmed = text.trim();
  if (trimmed.startsWith("<<"))
    return parseDynamoDbSetLiteral(trimmed, "string");
  try {
    const parsed: unknown = JSON.parse(trimmed);
    return isNonEmptyStringArray(parsed) ? parsed : null;
  } catch {
    return null;
  }
}

export function parseDynamoDbSetLiteral(
  text: string,
  subtype: DynamoDbSetSubtype,
): string[] {
  const value = text.trim();
  const fail = (): never => {
    throw new Error(`Enter a valid DynamoDB ${subtype} set literal.`);
  };
  if (!value.startsWith("<<") || !value.endsWith(">>")) return fail();
  const body = value.slice(2, -2);
  const entries: string[] = [];
  let pos = 0;
  while (pos < body.length) {
    while (/\s/.test(body[pos] ?? "") && pos < body.length) pos++;
    let entry = "";
    const quote = body[pos];
    if (quote === "'" || quote === '"') {
      const start = pos++;
      let closed = false;
      while (pos < body.length) {
        const ch = body[pos++];
        if (ch === quote) {
          if (quote === "'" && body[pos] === "'") {
            entry += "'";
            pos++;
            continue;
          }
          closed = true;
          break;
        }
        if (quote === '"' && ch === "\\") {
          if (pos >= body.length) return fail();
          entry += ch + body[pos++];
        } else entry += ch;
      }
      if (!closed) return fail();
      if (quote === '"') {
        try {
          entry = JSON.parse(body.slice(start, pos)) as string;
        } catch {
          return fail();
        }
      }
    } else {
      const start = pos;
      while (pos < body.length && body[pos] !== ",") pos++;
      entry = body.slice(start, pos).trim();
      if (!entry || subtype === "string") return fail();
    }
    entries.push(entry);
    while (/\s/.test(body[pos] ?? "") && pos < body.length) pos++;
    if (pos === body.length) break;
    if (body[pos++] !== "," || body.slice(pos).trim() === "") return fail();
  }
  if (entries.length === 0) return fail();
  if (subtype === "number") return parseDynamoDbNumberSetEntries(entries);
  if (subtype === "binary")
    return parseDynamoDbBinarySetEntries(entries, "cell");
  return entries;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function isAttributeValue(value: unknown): boolean {
  if (!isRecord(value) || Object.keys(value).length !== 1) return false;
  const [tag] = Object.keys(value);
  const content = value[tag];
  switch (tag) {
    case "S":
      return typeof content === "string";
    case "N":
      return typeof content === "string" && NUMBER_RE.test(content);
    case "B":
      return typeof content === "string" && isCanonicalBase64(content);
    case "BOOL":
      return typeof content === "boolean";
    case "NULL":
      return content === true;
    case "SS":
      return isNonEmptyStringArray(content);
    case "NS":
      parseDynamoDbNumberSetEntries(content);
      return true;
    case "BS":
      parseDynamoDbBinarySetEntries(content, "envelope");
      return true;
    case "L":
      return Array.isArray(content) && content.every(isAttributeValue);
    case "M":
      return (
        isRecord(content) && Object.values(content).every(isAttributeValue)
      );
    default:
      return false;
  }
}

export function getDynamoDbNativeCellValidationError(
  text: string,
  nativeType: string,
): string | null | undefined {
  const tags: Record<string, string> = {
    map: "M",
    list: "L",
    "string set": "SS",
    "number set": "NS",
    "binary set": "BS",
  };
  const expectedTag = Object.hasOwn(tags, nativeType)
    ? tags[nativeType]
    : undefined;
  if (!expectedTag) return undefined;
  const trimmed = text.trim();
  if (trimmed.startsWith(DYNAMODB_NATIVE_CELL_VALUE_PREFIX)) {
    try {
      const value: unknown = JSON.parse(
        trimmed.slice(DYNAMODB_NATIVE_CELL_VALUE_PREFIX.length),
      );
      if (
        isRecord(value) &&
        Object.hasOwn(value, expectedTag) &&
        isAttributeValue(value)
      )
        return null;
    } catch (error) {
      if (error instanceof DynamoDbSetInputError) return error.message;
    }
    return `Enter a valid DynamoDB ${nativeType} AttributeValue envelope.`;
  }
  if (nativeType === "string set") {
    try {
      return parseDynamoDbStringSetInput(trimmed) === null
        ? "Enter a DynamoDB string set as a <<'...'>> literal or a non-empty JSON array of strings."
        : null;
    } catch (error) {
      return error instanceof Error
        ? error.message
        : "Enter a valid DynamoDB set literal.";
    }
  }
  if (nativeType === "number set") {
    try {
      parseDynamoDbNumberSetInput(trimmed);
      return null;
    } catch (error) {
      return error instanceof Error
        ? error.message
        : "Enter a valid DynamoDB number set.";
    }
  }
  if (nativeType === "binary set") {
    try {
      parseDynamoDbBinarySetInput(trimmed);
      return null;
    } catch (error) {
      return error instanceof Error
        ? error.message
        : "Enter a valid binary set.";
    }
  }
  if (nativeType === "list") {
    try {
      if (Array.isArray(JSON.parse(trimmed))) return null;
    } catch {}
    return "Enter a JSON array before applying this DynamoDB List value.";
  }
  return undefined;
}
