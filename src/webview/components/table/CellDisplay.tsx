// biome-ignore lint/correctness/noUnusedImports: React needed for JSX
import React from "react";
import type { TypeCategory } from "../../../shared/tableTypes";
import { getCategoryPresentation } from "../../types";
import { formatScalarValueForDisplay } from "../../utils/valueFormatting";

const PENDING_COLOR = "var(--vscode-editorWarning-foreground, #cca700)";

/** Display a cell value with category-aware formatting. */
export function CellDisplay({
  value,
  isPending,
  isBoolean,
  category,
}: {
  value: unknown;
  isPending: boolean;
  isBoolean?: boolean;
  category?: TypeCategory;
}) {
  if (value === null || value === undefined) {
    return <span style={{ fontStyle: "italic", opacity: 0.45 }}>NULL</span>;
  }

  const categoryColor = category
    ? getCategoryPresentation(category).foreground
    : undefined;
  const resolvedColor = isPending ? PENDING_COLOR : categoryColor;

  // Boolean display
  if (isBoolean || category === "boolean") {
    const boolVal = coerceBoolDisplay(value);
    if (boolVal !== null) {
      return (
        <span
          style={{
            color: resolvedColor ?? getCategoryPresentation("boolean").foreground,
            fontWeight: 500,
          }}
        >
          {boolVal ? "true" : "false"}
        </span>
      );
    }
  }

  const str = formatScalarValueForDisplay(value);

  // Binary display (hex prefix)
  if (category === "binary" && str.startsWith("\\x")) {
    return (
      <span
        style={{
          color: resolvedColor,
          opacity: 0.85,
        }}
      >
        {str}
      </span>
    );
  }

  // JSON display (truncated)
  if (category === "json" && str.length > 0) {
    return (
      <span
        style={{
          color: resolvedColor,
          opacity: 0.85,
        }}
        title={str}
      >
        {str.length > 100 ? `${str.slice(0, 97)}…` : str}
      </span>
    );
  }

  // UUID — monospace inherent, but subtle color
  if (category === "uuid") {
    return (
      <span
        style={{
          color: resolvedColor,
          opacity: 0.85,
        }}
      >
        {str}
      </span>
    );
  }

  // Numeric — right-aligned is handled by parent; here just style
  if (
    category === "integer" ||
    category === "float" ||
    category === "decimal"
  ) {
    return (
      <span
        style={{
          color: resolvedColor,
        }}
      >
        {str}
      </span>
    );
  }

  // Default text
  return (
    <span
      style={{
        color: resolvedColor,
      }}
    >
      {str}
    </span>
  );
}

/**
 * Normalize a raw cell value to a boolean for display.
 * Returns null if not representable as boolean.
 */
function coerceBoolDisplay(value: unknown): boolean | null {
  if (typeof value === "boolean") return value;
  if (value === 1 || value === "1" || value === "true") return true;
  if (value === 0 || value === "0" || value === "false") return false;
  return null;
}
