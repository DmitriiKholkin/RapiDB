import type { TypeCategory } from "../types";

function oracleFloatPrecision(nativeType?: string): number | null {
  if (!nativeType) return null;

  const normalized = nativeType.toUpperCase().trim();
  if (normalized === "BINARY_FLOAT") return 7;
  if (normalized === "BINARY_DOUBLE") return 15;

  const match = /^FLOAT(?:\((\d+)\))?$/.exec(normalized);
  if (!match?.[1]) return null;

  const precision = Number.parseInt(match[1], 10);
  if (precision <= 24) return 7;
  if (precision <= 53) return 15;
  return null;
}

export function formatScalarValueForDisplay(
  value: unknown,
  category?: TypeCategory,
  nativeType?: string,
): string {
  if (
    category === "float" &&
    typeof value === "number" &&
    Number.isFinite(value)
  ) {
    const precision = oracleFloatPrecision(nativeType);
    if (precision !== null) {
      return Number.parseFloat(value.toPrecision(precision)).toString();
    }
  }

  return String(value);
}
