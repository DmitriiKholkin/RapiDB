import React from "react";
import type { TypeCategory } from "../../../shared/tableTypes";
import { getCategoryPresentation } from "../../types";
import {
  formatBinaryValueForViewer,
  formatScalarValueForDisplay,
} from "../../utils/valueFormatting";

const PENDING_COLOR = "var(--vscode-editorWarning-foreground, #cca700)";
export function CellDisplay({
  value,
  isPending,
  category,
  nativeType,
}: {
  value: unknown;
  isPending: boolean;
  category?: TypeCategory;
  nativeType?: string;
}) {
  if (value === null || value === undefined) {
    return <span style={{ fontStyle: "italic", opacity: 0.45 }}>NULL</span>;
  }
  const categoryColor = category
    ? getCategoryPresentation(category).foreground
    : undefined;
  const resolvedColor = isPending ? PENDING_COLOR : categoryColor;
  const str = formatScalarValueForDisplay(value);

  if (category === "binary") {
    const binaryStr = formatBinaryValueForViewer(value);
    return (
      <span
        style={{
          color: resolvedColor,
          opacity: 0.85,
        }}
      >
        {binaryStr}
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
