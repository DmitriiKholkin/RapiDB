// Supported currency codes; never interpret arbitrary three-letter text as money.
const CURRENCY_CODES = new Set([
  "USD",
  "EUR",
  "GBP",
  "JPY",
  "CHF",
  "CAD",
  "AUD",
  "NZD",
  "CNY",
  "HKD",
  "SGD",
  "INR",
  "RUB",
  "BRL",
  "MXN",
  "ZAR",
  "KRW",
  "SEK",
  "NOK",
  "DKK",
  "PLN",
  "CZK",
  "HUF",
  "TRY",
  "ILS",
  "AED",
  "SAR",
]);

const CURRENCY_AFFIX_CODE_POINT_RE = /^[\p{Sc}\s]$/u;

function stripCurrencyAffixes(rawValue: string): string {
  let value = rawValue.trim();
  value = value.replace(/^[\p{Sc}\s]+/gu, "");
  let end = value.length;
  // Scan suffix code points once. An unanchored greedy suffix regexp retries
  // every position in long interior whitespace/currency runs before a digit.
  while (end > 0) {
    let start = end - 1;
    const last = value.charCodeAt(start);
    if (last >= 0xdc00 && last <= 0xdfff && start > 0) {
      const preceding = value.charCodeAt(start - 1);
      if (preceding >= 0xd800 && preceding <= 0xdbff) start -= 1;
    }
    if (!CURRENCY_AFFIX_CODE_POINT_RE.test(value.slice(start, end))) break;
    end = start;
  }
  value = value.slice(0, end);
  if (
    /^[A-Za-z]{3}(?=\s|[+-]?\d|\.)/.test(value) &&
    CURRENCY_CODES.has(value.slice(0, 3).toUpperCase())
  ) {
    value = value.slice(3).trim();
  }
  if (
    // Fixed-width suffix: no variable-length run can be retried quadratically.
    /(?:\d|\.)[A-Za-z]{3}$/.test(value) &&
    CURRENCY_CODES.has(value.slice(-3).toUpperCase())
  ) {
    value = value.slice(0, -3).trim();
  }
  return value;
}

function stripAllCurrencySymbols(value: string): string {
  return value.replace(/\p{Sc}/gu, "");
}

const NUMERIC_FILTER_TOKEN_RE =
  /^[+-]?(?:\d+(?:\.\d+)?|\.\d+)(?:[eE][+-]?\d+)?$/;
const GROUPED_NUMBER_RE =
  /^[+-]?(?:\d{1,3}(?:,\d{3})+)(?:\.\d+)?(?:[eE][+-]?\d+)?$/;
const APOSTROPHE_GROUPED_RE =
  /^[+-]?(?:\d{1,3}(?:'\d{3})+)(?:\.\d+)?(?:[eE][+-]?\d+)?$/;

export function normalizeNumericToken(
  rawValue: string,
  requireFiniteNumber = true,
): string | null {
  let value = rawValue.trim();
  if (value === "") return null;

  let isNegative = false;
  const wrappedNegative = /^\((.*)\)$/.exec(value);
  if (wrappedNegative) {
    isNegative = true;
    value = wrappedNegative[1].trim();
  }

  value = stripCurrencyAffixes(value);
  value = value.replace(/\s+/g, "");
  value = stripAllCurrencySymbols(value);

  if (value.includes("'")) {
    if (!APOSTROPHE_GROUPED_RE.test(value)) {
      return null;
    }
    value = value.replace(/'/g, "");
  }

  if (value.includes(",")) {
    if (!GROUPED_NUMBER_RE.test(value)) {
      return null;
    }
    value = value.replace(/,/g, "");
  }

  if (isNegative) {
    value = `-${value.replace(/^[+-]/, "")}`;
  }

  if (!NUMERIC_FILTER_TOKEN_RE.test(value)) {
    return null;
  }

  // Exact DB numerics can exceed the JS Number range without being infinite.
  return !requireFiniteNumber || Number.isFinite(Number(value)) ? value : null;
}

// Comparison-only work limits; normalization's existing exact/money contracts
// remain unchanged. Oversized comparison inputs are invalid, never rounded.
export const NUMERIC_COMPARISON_LIMITS = {
  maxTokenLength: 65_536,
  maxExponentDigits: 8_192,
} as const;

function skipLeadingZeros(digits: string, start = 0): number {
  while (start < digits.length && digits.charCodeAt(start) === 48) start += 1;
  return start;
}

/** Compare bounded decimal tokens without rounding or expanding exponents. */
export function compareNumericTokens(
  left: string,
  right: string,
): number | null {
  // Check both operands before trimming, normalization, allocation or BigInt.
  if (
    left.length > NUMERIC_COMPARISON_LIMITS.maxTokenLength ||
    right.length > NUMERIC_COMPARISON_LIMITS.maxTokenLength
  )
    return null;
  const parse = (raw: string) => {
    let token = normalizeNumericToken(raw, false);
    if (token === null) {
      // Only the complete plain trailing-dot grammar gets this extension.
      // Never repair a malformed decimal or alter money/grouping normalization.
      const plain = raw.trim();
      if (!/^[+-]?\d+\.(?:[eE][+-]?\d+)?$/.test(plain)) return null;
      const dot = plain.indexOf(".");
      token = plain.slice(0, dot) + plain.slice(dot + 1);
    }
    const match = /^([+-]?)(\d*)(?:\.(\d+))?(?:[eE]([+-]?\d+))?$/.exec(token);
    if (!match) return null;
    const exponent = match[4] ?? "0";
    const exponentSignLength =
      exponent[0] === "+" || exponent[0] === "-" ? 1 : 0;
    const exponentStart = skipLeadingZeros(exponent, exponentSignLength);
    if (
      exponent.length - exponentStart >
      NUMERIC_COMPARISON_LIMITS.maxExponentDigits
    )
      return null;
    const exponentDigits = exponent.slice(exponentStart) || "0";
    const signedExponent =
      exponent[0] === "-" ? `-${exponentDigits}` : exponentDigits;
    const digits = `${match[2]}${match[3] ?? ""}`;
    const start = skipLeadingZeros(digits);
    if (start === digits.length) return { sign: 0, digits: "", magnitude: 0n };
    let end = digits.length;
    // One backwards pass. An unanchored /0+$/ retries long internal zero runs
    // at every position and becomes quadratic on e.g. 1 + zeros + 1.
    while (end > start && digits.charCodeAt(end - 1) === 48) end -= 1;
    return {
      sign: match[1] === "-" ? -1 : 1,
      digits: digits.slice(start, end),
      magnitude:
        BigInt(signedExponent) +
        BigInt(digits.length - start) -
        BigInt(match[3]?.length ?? 0),
    };
  };
  const a = parse(left);
  const b = parse(right);
  if (!a || !b) return null;
  if (a.sign !== b.sign) return a.sign < b.sign ? -1 : 1;
  if (a.sign === 0) return 0;
  if (a.magnitude !== b.magnitude) {
    return (a.magnitude < b.magnitude ? -1 : 1) * a.sign;
  }
  // Virtual right-padding is bounded by the input significands, never exponents.
  for (let i = 0; i < Math.max(a.digits.length, b.digits.length); i += 1) {
    const x = a.digits[i] ?? "0";
    const y = b.digits[i] ?? "0";
    if (x !== y) return (x < y ? -1 : 1) * a.sign;
  }
  return 0;
}
