import React from "react";
import {
  type ColumnTypeMeta as ColumnMeta,
  NULL_SENTINEL,
} from "../../../shared/tableTypes";
import { placeholderForCategory } from "../../types";
import { Icon } from "../Icon";

function omitKey(
  row: Record<string, unknown>,
  columnName: string,
): Record<string, unknown> {
  return Object.fromEntries(
    Object.entries(row).filter(([key]) => key !== columnName),
  );
}

function clearsNullToEmptyString(column: ColumnMeta): boolean {
  const nativeType = column.nativeType.toLowerCase();
  return (
    column.category === "text" ||
    nativeType.startsWith("set") ||
    column.type.toLowerCase().startsWith("set") ||
    (nativeType.startsWith("enum") && nativeType.includes("''"))
  );
}

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
        const canInsert = true;
        const isNull = newRow[col.name] === NULL_SENTINEL;
        const rawVal = newRow[col.name];

        return (
          <div
            key={col.name}
            style={{ display: "flex", flexDirection: "column", gap: 2 }}
          >
            <span style={{ fontSize: 10, opacity: canInsert ? 0.6 : 0.35 }}>
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
              <input
                aria-label={`New value for ${col.name}`}
                value={isNull ? "" : String(rawVal ?? "")}
                disabled={isNull || !canInsert}
                onChange={(e) => {
                  if (!canInsert) return;
                  const next = e.target.value;
                  if (next === "" && !clearsNullToEmptyString(col)) {
                    setNewRow(omitKey(newRow, col.name));
                    return;
                  }
                  setNewRow({ ...newRow, [col.name]: next });
                }}
                placeholder={
                  isNull ? "NULL" : placeholderForCategory(col.category)
                }
                style={{
                  width: 120,
                  padding: "3px 6px",
                  fontSize: 12,
                  background: "var(--vscode-input-background)",
                  color:
                    isNull || !canInsert
                      ? "var(--vscode-disabledForeground)"
                      : "var(--vscode-input-foreground)",
                  border: "1px solid var(--vscode-input-border)",
                  borderRadius: 2,
                  fontFamily: "inherit",
                  opacity: isNull || !canInsert ? 0.55 : 1,
                  fontStyle: isNull || !canInsert ? "italic" : "normal",
                  boxSizing: "border-box" as const,
                }}
              />
              {col.nullable && (
                <button
                  type="button"
                  disabled={!canInsert}
                  onMouseDown={(e) => e.preventDefault()}
                  onClick={() =>
                    canInsert &&
                    setNewRow(
                      isNull
                        ? omitKey(newRow, col.name)
                        : {
                            ...newRow,
                            [col.name]: NULL_SENTINEL,
                          },
                    )
                  }
                  title={
                    !canInsert
                      ? "This column is read-only"
                      : isNull
                        ? "Remove NULL"
                        : "Set field to NULL"
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
                    color: !canInsert
                      ? "var(--vscode-disabledForeground)"
                      : isNull
                        ? "var(--vscode-button-foreground)"
                        : "var(--vscode-badge-foreground)",
                    border: "none",
                    borderRadius: 2,
                    cursor: canInsert ? "pointer" : "default",
                    letterSpacing: "0.02em",
                    opacity: !canInsert ? 0.35 : isNull ? 1 : 0.5,
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
