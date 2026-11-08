interface ExactNumericValue {
  sign: -1 | 0 | 1;
  digits: string;
  decimalPosition: bigint;
  special?: "negativeInfinity" | "positiveInfinity" | "nan";
}

const EXACT_NUMERIC_RE =
  /^([+-]?)(?:(\d+)(?:\.(\d*))?|\.(\d+))(?:[eE]([+-]?\d+))?$/;
const FORMATTED_NUMERIC_RE =
  /^([+-]?)\s*(\p{Sc}?)\s*([+-]?)\s*([\d.,][\d.,'\s]*)\s*(\p{Sc}?)\s*([+-]?)$/u;

function parseExactNumericValue(value: unknown): ExactNumericValue | null {
  let text: string;
  if (typeof value === "bigint") {
    text = value.toString();
  } else if (typeof value === "number") {
    text = value.toString();
  } else if (typeof value === "string") {
    text = value.trim();
  } else {
    return null;
  }

  const specialValue = text.toLowerCase();
  if (["-infinity", "-inf"].includes(specialValue)) {
    return {
      sign: -1,
      digits: "",
      decimalPosition: 0n,
      special: "negativeInfinity",
    };
  }
  if (["infinity", "+infinity", "inf", "+inf"].includes(specialValue)) {
    return {
      sign: 1,
      digits: "",
      decimalPosition: 0n,
      special: "positiveInfinity",
    };
  }
  if (["nan", "+nan", "-nan"].includes(specialValue)) {
    return { sign: 0, digits: "", decimalPosition: 0n, special: "nan" };
  }

  if (!EXACT_NUMERIC_RE.test(text)) {
    text = normalizeFormattedNumericValue(text) ?? "";
  }

  const match = EXACT_NUMERIC_RE.exec(text);
  if (!match) return null;

  const integerPart = match[2] ?? "0";
  const fractionalPart = match[3] ?? match[4] ?? "";
  const rawDigits = `${integerPart}${fractionalPart}`;
  const leadingZeroes = rawDigits.length - rawDigits.replace(/^0+/, "").length;
  const digits = rawDigits.slice(leadingZeroes) || "0";
  const sign = digits === "0" ? 0 : match[1] === "-" ? -1 : 1;
  const exponent = BigInt(match[5] ?? "0");

  return {
    sign,
    digits,
    decimalPosition: BigInt(integerPart.length - leadingZeroes) + exponent,
  };
}

function normalizeFormattedNumericValue(value: string): string | null {
  const trimmed = value.trim();
  const parenthesized = trimmed.startsWith("(") && trimmed.endsWith(")");
  const inner = parenthesized ? trimmed.slice(1, -1).trim() : trimmed;
  const match = FORMATTED_NUMERIC_RE.exec(inner);
  if (!match) return null;
  const signs = [match[1], match[3], match[6]].filter(Boolean);
  if (signs.length > 1 || (parenthesized && signs.length > 0)) return null;
  if (match[2] && match[5]) return null;
  const negative = parenthesized || signs[0] === "-";
  const digitsAndSeparators = match[4].replace(/[^\d.,]/g, "");
  if (!/\d/.test(digitsAndSeparators)) return null;

  const lastDot = digitsAndSeparators.lastIndexOf(".");
  const lastComma = digitsAndSeparators.lastIndexOf(",");
  const hasDot = lastDot >= 0;
  const hasComma = lastComma >= 0;
  let decimalSeparator: "." | "," | null = null;
  if (hasDot && hasComma) {
    decimalSeparator = lastDot > lastComma ? "." : ",";
  } else if (hasDot || hasComma) {
    const separator = hasDot ? "." : ",";
    const lastSeparator = hasDot ? lastDot : lastComma;
    const integerDigits = digitsAndSeparators.slice(0, lastSeparator);
    const digitsAfter = digitsAndSeparators.slice(lastSeparator + 1).length;
    const grouping =
      digitsAfter === 0 ||
      integerDigits.includes(separator) ||
      (/^[1-9]\d{0,2}$/.test(integerDigits) && digitsAfter === 3);
    if (!grouping) {
      decimalSeparator = separator;
    }
  }

  let normalized: string;
  if (decimalSeparator) {
    const decimalIndex = digitsAndSeparators.lastIndexOf(decimalSeparator);
    const integerPart = digitsAndSeparators
      .slice(0, decimalIndex)
      .replace(/[^\d]/g, "");
    const fractionalPart = digitsAndSeparators
      .slice(decimalIndex + 1)
      .replace(/[^\d]/g, "");
    if (fractionalPart.length === 0) return null;
    normalized = `${integerPart || "0"}.${fractionalPart}`;
  } else {
    normalized = digitsAndSeparators.replace(/[^\d]/g, "");
  }

  return `${negative ? "-" : ""}${normalized}`;
}

function compareMagnitude(
  left: ExactNumericValue,
  right: ExactNumericValue,
): number {
  if (left.decimalPosition !== right.decimalPosition) {
    return left.decimalPosition < right.decimalPosition ? -1 : 1;
  }

  const maxLength = Math.max(left.digits.length, right.digits.length);
  for (let index = 0; index < maxLength; index += 1) {
    const leftDigit = left.digits.charCodeAt(index) || 48;
    const rightDigit = right.digits.charCodeAt(index) || 48;
    if (leftDigit !== rightDigit) {
      return leftDigit < rightDigit ? -1 : 1;
    }
  }

  return 0;
}

function compareFallbackValues(left: unknown, right: unknown): number {
  const leftText = String(left);
  const rightText = String(right);
  return leftText < rightText ? -1 : leftText > rightText ? 1 : 0;
}

export function compareExactNumericValues(
  left: unknown,
  right: unknown,
): number {
  if (left === right) return 0;
  if (left === null || left === undefined) {
    return right === null || right === undefined ? 0 : -1;
  }
  if (right === null || right === undefined) return 1;

  const leftNumeric = parseExactNumericValue(left);
  const rightNumeric = parseExactNumericValue(right);
  if (!leftNumeric && !rightNumeric) {
    return compareFallbackValues(left, right);
  }
  if (!leftNumeric) return 1;
  if (!rightNumeric) return -1;

  const specialRank = (numeric: ExactNumericValue): number => {
    switch (numeric.special) {
      case "negativeInfinity":
        return 0;
      case "positiveInfinity":
        return 2;
      case "nan":
        return 3;
      default:
        return 1;
    }
  };
  const leftRank = specialRank(leftNumeric);
  const rightRank = specialRank(rightNumeric);
  if (leftRank !== rightRank) return leftRank < rightRank ? -1 : 1;
  if (leftNumeric.special || rightNumeric.special) return 0;

  if (leftNumeric.sign !== rightNumeric.sign) {
    return leftNumeric.sign < rightNumeric.sign ? -1 : 1;
  }
  if (leftNumeric.sign === 0) return 0;

  const magnitude = compareMagnitude(leftNumeric, rightNumeric);
  return magnitude === 0 ? 0 : leftNumeric.sign === 1 ? magnitude : -magnitude;
}
