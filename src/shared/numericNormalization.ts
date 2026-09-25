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

function stripCurrencyAffixes(rawValue: string): string {
  let value = rawValue.trim();
  value = value.replace(/^[\p{Sc}\s]+/gu, "");
  value = value.replace(/[\p{Sc}\s]+$/gu, "");
  if (
    /^[A-Za-z]{3}(?=\s|[+-]?\d|\.)/.test(value) &&
    CURRENCY_CODES.has(value.slice(0, 3).toUpperCase())
  ) {
    value = value.slice(3).trim();
  }
  if (
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
