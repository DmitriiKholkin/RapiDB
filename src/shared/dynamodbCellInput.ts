import { compareExactNumericValues } from "./exactNumericComparison";

export const DYNAMODB_NATIVE_CELL_VALUE_PREFIX =
  "DynamoDB native value (nested sets/binary): ";

export type DynamoDbSetSubtype = "string" | "number" | "binary";
const NUMBER_RE = /^[+-]?(?:\d+(?:\.\d+)?|\.\d+)(?:[eE][+-]?\d+)?$/;
const BASE64_ALPHABET =
  "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

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
  const values: unknown = JSON.parse(text);
  if (
    !Array.isArray(values) ||
    !values.length ||
    !values.every(
      (value) => typeof value === "number" || typeof value === "string",
    )
  )
    throw new Error(
      "Enter a valid DynamoDB number set containing at least one number.",
    );
  const tokens =
    text.match(
      /"(?:\\.|[^"\\])*"|-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?/g,
    ) ?? [];
  const entries = tokens.map((token) =>
    token.startsWith('"') ? (JSON.parse(token) as string) : token,
  );
  if (!entries.every((entry) => NUMBER_RE.test(entry)))
    throw new Error(
      "Enter a valid DynamoDB number set containing at least one number.",
    );
  if (hasDuplicateDynamoDbNumbers(entries))
    throw new Error("DynamoDB number set values must be unique.");
  return entries;
}

function isCanonicalBase64(value: string): boolean {
  if (
    !/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(
      value,
    )
  )
    return false;
  if (value.endsWith("=="))
    return (BASE64_ALPHABET.indexOf(value.at(-3) ?? "") & 15) === 0;
  if (value.endsWith("="))
    return (BASE64_ALPHABET.indexOf(value.at(-2) ?? "") & 3) === 0;
  return true;
}

function isBinaryInput(value: string): boolean {
  if (/^(?:0x|\\x)/i.test(value)) {
    const hex = value.slice(2);
    return hex.length % 2 === 0 && /^[a-f\d]*$/i.test(hex);
  }
  return (
    value.length > 0 &&
    value.length % 4 === 0 &&
    /^[A-Za-z0-9+/]+={0,2}$/.test(value)
  );
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
    if (subtype === "number" && !NUMBER_RE.test(entry)) return fail();
    if (subtype === "binary" && !isBinaryInput(entry.trim())) return fail();
    entries.push(entry);
    while (/\s/.test(body[pos] ?? "") && pos < body.length) pos++;
    if (pos === body.length) break;
    if (body[pos++] !== "," || body.slice(pos).trim() === "") return fail();
  }
  if (subtype === "number" && hasDuplicateDynamoDbNumbers(entries))
    throw new Error("DynamoDB number set values must be unique.");
  return entries.length > 0 ? entries : fail();
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
      return (
        Array.isArray(content) &&
        content.length > 0 &&
        content.every((v) => typeof v === "string")
      );
    case "NS":
      return (
        Array.isArray(content) &&
        content.length > 0 &&
        content.every((v) => typeof v === "string" && NUMBER_RE.test(v)) &&
        !hasDuplicateDynamoDbNumbers(content)
      );
    case "BS":
      return (
        Array.isArray(content) &&
        content.length > 0 &&
        new Set(content).size === content.length &&
        content.every((v) => typeof v === "string" && isCanonicalBase64(v))
      );
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
        isAttributeValue(value) &&
        isRecord(value) &&
        Object.hasOwn(value, expectedTag)
      )
        return null;
    } catch {}
    return `Enter a valid DynamoDB ${nativeType} AttributeValue envelope.`;
  }
  if (nativeType.endsWith(" set") && trimmed.startsWith("<<")) {
    try {
      parseDynamoDbSetLiteral(
        trimmed,
        nativeType.split(" ")[0] as DynamoDbSetSubtype,
      );
      return null;
    } catch (error) {
      return error instanceof Error
        ? error.message
        : "Enter a valid DynamoDB set literal.";
    }
  }
  if (nativeType === "number set" && trimmed.startsWith("[")) {
    try {
      parseDynamoDbNumberSetJson(trimmed);
      return null;
    } catch (error) {
      return error instanceof Error
        ? error.message
        : "Enter a valid DynamoDB number set.";
    }
  }
  if (nativeType === "binary set") {
    try {
      const values: unknown = JSON.parse(trimmed);
      if (
        Array.isArray(values) &&
        values.length > 0 &&
        values.every(
          (value) => typeof value === "string" && isBinaryInput(value.trim()),
        )
      )
        return null;
    } catch {}
    return "Enter a non-empty DynamoDB binary set of hex or base64 strings.";
  }
  if (nativeType === "list") {
    try {
      if (Array.isArray(JSON.parse(trimmed))) return null;
    } catch {}
    return "Enter a JSON array before applying this DynamoDB List value.";
  }
  return undefined;
}
