import React from "react";
import type { TypeCategory } from "../../../shared/tableTypes";
import { getCategoryPresentation } from "../../types";
import { CELL_PREVIEW_NOTICE, getCellPreview } from "../../utils/cellPreview";

const PENDING_COLOR = "var(--vscode-editorWarning-foreground, #cca700)";
export function CellDisplay({
  value,
  isPending,
  category,
  nativeType: _nativeType,
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
  const preview = getCellPreview(value, category);
  const str = preview.text;
  const title = preview.truncated ? CELL_PREVIEW_NOTICE : undefined;

  if (category === "binary") {
    return (
      <span
        title={title}
        style={{
          color: resolvedColor,
          opacity: 0.85,
        }}
      >
        {str}
      </span>
    );
  }

  if (category === "uuid") {
    return (
      <span
        title={title}
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
        title={title}
        style={{
          color: resolvedColor,
        }}
      >
        {str}
      </span>
    );
  }
  const singleLineStr = str.replace(/\r\n|\r|\n/g, "↵");
  return (
    <span
      title={title}
      style={{
        color: resolvedColor,
        whiteSpace: "pre",
      }}
    >
      {singleLineStr}
    </span>
  );
}
