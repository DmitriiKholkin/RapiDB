import React from "react";
import { Icon } from "../../Icon";

interface GridOverlayProps {
  loading: boolean;
  rowsLength: number;
  hasDraftRow: boolean;
  pasteErrors: Array<{
    rowIndex: number;
    columnIndex: number;
    columnName: string;
    message: string;
  }>;
  onDismissPasteErrors: () => void;
}

export function GridOverlay({
  loading,
  rowsLength,
  hasDraftRow,
  pasteErrors,
  onDismissPasteErrors,
}: GridOverlayProps) {
  return (
    <>
      {!loading && rowsLength === 0 && !hasDraftRow && (
        <div
          style={{
            display: "flex",
            flexDirection: "column",
            alignItems: "center",
            justifyContent: "center",
            height: 200,
            opacity: 0.4,
            userSelect: "none",
          }}
        >
          <Icon name="inbox" size={28} style={{ opacity: 0.4 }} />
          <div style={{ fontSize: 13, marginTop: 8 }}>No rows found</div>
        </div>
      )}

      {pasteErrors.length > 0 && (
        <div
          style={{
            position: "absolute",
            bottom: 0,
            left: 0,
            right: 0,
            padding: "8px 12px",
            background:
              "var(--vscode-inputValidation-errorBackground, rgba(200,50,50,0.15))",
            borderTop:
              "1px solid var(--vscode-inputValidation-errorBorder, rgba(200,50,50,0.5))",
            color: "var(--vscode-errorForeground)",
            fontSize: 12,
            zIndex: 10,
            display: "flex",
            alignItems: "flex-start",
            gap: 8,
          }}
        >
          <Icon
            name="error"
            size={14}
            style={{ flexShrink: 0, marginTop: 2 }}
          />
          <div style={{ flex: 1 }}>
            <div style={{ fontWeight: 600, marginBottom: 4 }}>
              Paste failed: {pasteErrors.length} error
              {pasteErrors.length > 1 ? "s" : ""}
            </div>
            <div style={{ opacity: 0.9, maxHeight: 60, overflow: "auto" }}>
              {pasteErrors.slice(0, 3).map((error) => (
                <div
                  key={`${error.rowIndex}-${error.columnIndex}-${error.columnName}`}
                  style={{ marginBottom: 2 }}
                >
                  {error.columnName
                    ? `Row ${error.rowIndex + 1}, ${error.columnName}: `
                    : `Row ${error.rowIndex + 1}: `}
                  {error.message}
                </div>
              ))}
              {pasteErrors.length > 3 && (
                <div style={{ opacity: 0.7, fontStyle: "italic" }}>
                  ...and {pasteErrors.length - 3} more error
                  {pasteErrors.length - 3 > 1 ? "s" : ""}
                </div>
              )}
            </div>
          </div>
          <button
            type="button"
            onClick={onDismissPasteErrors}
            style={{
              background: "transparent",
              border: "none",
              color: "var(--vscode-errorForeground)",
              cursor: "pointer",
              padding: "2px 6px",
              fontSize: 14,
              opacity: 0.7,
            }}
            title="Dismiss"
          >
            ×
          </button>
        </div>
      )}
    </>
  );
}
