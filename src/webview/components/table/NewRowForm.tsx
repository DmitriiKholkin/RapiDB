// biome-ignore lint/style/useImportType: React used as value in refs
import React from "react";
import type { ColumnMeta } from "../../types";
import { NULL_SENTINEL, placeholderForCategory } from "../../types";
import { Icon } from "../Icon";

export function NewRowForm({
  columns,
  newRow,
  setNewRow,
  inserting,
  onInsert,
  onCancel,
}: {
  columns: ColumnMeta[];
  newRow: Record<string, unknown>;
  setNewRow: (row: Record<string, unknown>) => void;
  inserting: boolean;
  onInsert: () => void;
  onCancel: () => void;
}) {
  const btnBase: React.CSSProperties = {
    padding: "3px 10px",
    fontSize: 12,
    borderRadius: 2,
    cursor: "pointer",
    fontFamily: "inherit",
    whiteSpace: "nowrap",
  };

  return (
    <div
      style={{
        flexShrink: 0,
        padding: "8px 12px",
        borderBottom: "1px solid var(--vscode-panel-border)",
        background:
          "var(--vscode-list-inactiveSelectionBackground, rgba(128,128,128,0.1))",
        display: "flex",
        flexWrap: "wrap",
        gap: 8,
        alignItems: "flex-end",
      }}
    >
      <span
        style={{
          fontSize: 11,
          opacity: 0.6,
          alignSelf: "center",
          marginRight: 4,
        }}
      >
        New row:
      </span>
      {columns.map((col) => {
        const isNull = newRow[col.name] === NULL_SENTINEL;
        const rawVal = newRow[col.name];

        return (
          <div
            key={col.name}
            style={{ display: "flex", flexDirection: "column", gap: 2 }}
          >
            <span style={{ fontSize: 10, opacity: 0.6 }}>
              {col.name}
              {col.isPrimaryKey && (
                <Icon
                  name="key"
                  size={10}
                  color="var(--vscode-charts-yellow, #cca700)"
                  style={{ marginLeft: 3 }}
                  title="Primary Key"
                />
              )}
            </span>
            <div style={{ display: "flex", alignItems: "center", gap: 2 }}>
              {col.isBoolean || col.category === "boolean" ? (
                <select
                  value={isNull ? "" : String(rawVal ?? "false")}
                  disabled={isNull}
                  onChange={(e) =>
                    setNewRow({ ...newRow, [col.name]: e.target.value })
                  }
                  style={{
                    width: 120,
                    padding: "3px 6px",
                    fontSize: 12,
                    background: "var(--vscode-input-background)",
                    color: isNull
                      ? "var(--vscode-disabledForeground)"
                      : "var(--vscode-input-foreground)",
                    border: "1px solid var(--vscode-input-border)",
                    borderRadius: 2,
                    fontFamily: "inherit",
                    opacity: isNull ? 0.55 : 1,
                    boxSizing: "border-box" as const,
                  }}
                >
                  <option value="false">false</option>
                  <option value="true">true</option>
                </select>
              ) : (
                <input
                  value={isNull ? "" : String(rawVal ?? "")}
                  disabled={isNull}
                  onChange={(e) =>
                    setNewRow({ ...newRow, [col.name]: e.target.value })
                  }
                  placeholder={
                    isNull
                      ? "NULL"
                      : placeholderForCategory(col.category, col.isBoolean)
                  }
                  style={{
                    width: 120,
                    padding: "3px 6px",
                    fontSize: 12,
                    background: "var(--vscode-input-background)",
                    color: isNull
                      ? "var(--vscode-disabledForeground)"
                      : "var(--vscode-input-foreground)",
                    border: "1px solid var(--vscode-input-border)",
                    borderRadius: 2,
                    fontFamily: "inherit",
                    opacity: isNull ? 0.55 : 1,
                    fontStyle: isNull ? "italic" : "normal",
                    boxSizing: "border-box" as const,
                  }}
                />
              )}
              {col.nullable && (
                <button
                  type="button"
                  onMouseDown={(e) => e.preventDefault()}
                  onClick={() =>
                    setNewRow({
                      ...newRow,
                      [col.name]: isNull ? "" : NULL_SENTINEL,
                    })
                  }
                  title={
                    isNull ? "Remove NULL — set to empty" : "Set field to NULL"
                  }
                  style={{
                    flexShrink: 0,
                    height: 24,
                    padding: "0 5px",
                    fontSize: 9,
                    fontStyle: "italic",
                    fontFamily: "inherit",
                    background: isNull
                      ? "var(--vscode-button-background)"
                      : "transparent",
                    color: isNull
                      ? "var(--vscode-button-foreground)"
                      : "var(--vscode-badge-foreground)",
                    border: "none",
                    borderRadius: 2,
                    cursor: "pointer",
                    letterSpacing: "0.02em",
                    opacity: isNull ? 1 : 0.5,
                  }}
                >
                  NULL
                </button>
              )}
            </div>
          </div>
        );
      })}
      <button
        type="button"
        style={{
          ...btnBase,
          alignSelf: "flex-end",
          background: "var(--vscode-button-background)",
          color: "var(--vscode-button-foreground)",
          border: "none",
          opacity: inserting ? 0.45 : 1,
          cursor: inserting ? "default" : "pointer",
        }}
        disabled={inserting}
        onClick={onInsert}
      >
        {inserting ? "Inserting…" : "Insert"}
      </button>
      <button
        type="button"
        style={{
          ...btnBase,
          alignSelf: "flex-end",
          background: "transparent",
          color: "var(--vscode-foreground)",
          border: "1px solid var(--vscode-panel-border)",
        }}
        onClick={onCancel}
      >
        Cancel
      </button>
    </div>
  );
}
