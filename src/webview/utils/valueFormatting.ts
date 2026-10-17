const BINDATA_LITERAL_RE =
  /^(?:new\s+)?BinData\(\s*(\d+)\s*,\s*(["'])([A-Za-z0-9+/=]*)\2\s*\)$/i;

export function formatScalarValueForDisplay(value: unknown): string {
  if (value === null || value === undefined) {
    return "";
  }

  if (
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "boolean" ||
    typeof value === "bigint"
  ) {
    return String(value);
  }

  if (value instanceof Date) {
    return Number.isNaN(value.getTime()) ? String(value) : value.toISOString();
  }

  if (typeof value === "object") {
    const driverSnapshot = value as {
      __rapidbSqlVariant?: unknown;
      __rapidbSpatial?: unknown;
      displayValue?: unknown;
    };
    if (
      (driverSnapshot.__rapidbSqlVariant === 1 ||
        driverSnapshot.__rapidbSpatial === 1) &&
      typeof driverSnapshot.displayValue === "string"
    ) {
      return driverSnapshot.displayValue;
    }
    try {
      return JSON.stringify(value);
    } catch {
      return String(value);
    }
  }

  return String(value);
}

export function formatBinaryValueForViewer(value: unknown): string {
  const formatted = formatScalarValueForDisplay(value);
  const match = BINDATA_LITERAL_RE.exec(formatted);
  if (!match) {
    return formatted;
  }
  return `BinData(${match[1]}, ${match[2]}${match[3]}${match[2]})`;
}
