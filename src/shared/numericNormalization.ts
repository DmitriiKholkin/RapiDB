const _CURRENCY_AFFIX_RE = /[\p{Sc}\s]+/gu;

function stripCurrencyAffixes(rawValue: string): string {
  let value = rawValue.trim();
  value = value.replace(/^[\p{Sc}\s]+/gu, "");
  value = value.replace(/[\p{Sc}\s]+$/gu, "");
  if (/^[A-Za-z]{3}(?=\s|[+-]?\d|\.)/.test(value)) {
    value = value.slice(3).trim();
  }
  if (/(?:\d|\.)[A-Za-z]{3}$/.test(value)) {
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

export function normalizeNumericToken(rawValue: string): string | null {
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

  const parsed = Number(value);
  return Number.isFinite(parsed) ? value : null;
}
