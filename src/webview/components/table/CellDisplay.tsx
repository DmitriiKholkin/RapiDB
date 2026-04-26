import React from "react";
import type { TypeCategory } from "../../../shared/tableTypes";
import { getCategoryPresentation } from "../../types";
import { formatScalarValueForDisplay } from "../../utils/valueFormatting";
const PENDING_COLOR = "var(--vscode-editorWarning-foreground, #cca700)";
export function CellDisplay({
  value,
  isPending,
  category,
}: {
  value: unknown;
  isPending: boolean;
  category?: TypeCategory;
}) {
  if (value === null || value === undefined) {
    return <span style={{ fontStyle: "italic", opacity: 0.45 }}>NULL</span>;
  }
  const categoryColor = category
    ? getCategoryPresentation(category).foreground
    : undefined;
  const resolvedColor = isPending ? PENDING_COLOR : categoryColor;
  const str = formatScalarValueForDisplay(value);
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
  const singleLineStr = str.replace(/\r?\n/g, " ");
  return (
    <span
      style={{
        color: resolvedColor,
        whiteSpace: "pre",
      }}
    >
      {singleLineStr}
    </span>
  );
}
