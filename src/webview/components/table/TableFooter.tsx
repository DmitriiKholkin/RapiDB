import React from "react";
import { PAGE_SIZES, tableButtonStyle } from "./tableViewHelpers";

interface TableFooterProps {
  page: number;
  pageSize: number;
  totalPages: number;
  onNextPage: () => void;
  onPreviousPage: () => void;
  onPageSizeChange: (pageSize: number) => void;
}

export function TableFooter({
  page,
  pageSize,
  totalPages,
  onNextPage,
  onPreviousPage,
  onPageSizeChange,
}: TableFooterProps) {
  return (
    <div
      style={{
        height: 34,
        flexShrink: 0,
        display: "flex",
        alignItems: "center",
        gap: 8,
        padding: "0 12px",
        borderTop: "1px solid var(--vscode-panel-border)",
        background: "var(--vscode-editorGroupHeader-tabsBackground)",
        fontSize: 12,
      }}
    >
      <button
        type="button"
        style={tableButtonStyle("ghost", page <= 1)}
        disabled={page <= 1}
        onClick={onPreviousPage}
      >
        ← Prev
      </button>
      <span style={{ opacity: 0.7 }}>
        Page {page} of {totalPages}
      </span>
      <button
        type="button"
        style={tableButtonStyle("ghost", page >= totalPages)}
        disabled={page >= totalPages}
        onClick={onNextPage}
      >
        Next →
      </button>
      <div style={{ flex: 1 }} />
      <span style={{ opacity: 0.6 }}>Rows per page:</span>
      <select
        aria-label="Rows per page"
        value={pageSize}
        onChange={(event) => onPageSizeChange(Number(event.target.value))}
        style={{
          padding: "2px 4px",
          fontSize: 12,
          background:
            "var(--vscode-dropdown-background, var(--vscode-input-background))",
          color: "var(--vscode-dropdown-foreground, var(--vscode-foreground))",
          border:
            "1px solid var(--vscode-dropdown-border, var(--vscode-panel-border))",
          borderRadius: 2,
          fontFamily: "inherit",
          cursor: "pointer",
        }}
      >
        {PAGE_SIZES.map((size) => (
          <option key={size} value={size}>
            {size}
          </option>
        ))}
      </select>
    </div>
  );
}
