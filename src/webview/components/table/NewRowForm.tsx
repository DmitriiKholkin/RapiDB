import React from "react";
import {
  type ColumnTypeMeta as ColumnMeta,
  NULL_SENTINEL,
} from "../../../shared/tableTypes";
import { Icon } from "../Icon";

function omitKey(
  row: Record<string, unknown>,
  columnName: string,
): Record<string, unknown> {
  return Object.fromEntries(
    Object.entries(row).filter(([key]) => key !== columnName),
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
        display: "grid",
        gap: 8,
      }}
    >
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(auto-fill, minmax(220px, 1fr))",
          gap: 8,
          alignItems: "end",
        }}
      >
        {columns.map((col) => {
          const canInsert = true;
          const includedInInsert = Object.hasOwn(newRow, col.name);
          const isNull = newRow[col.name] === NULL_SENTINEL;
          const rawVal = newRow[col.name];

          return (
            <div
              key={col.name}
              style={{
                display: "flex",
                flexDirection: "column",
                gap: 2,
                minWidth: 0,
              }}
            >
              <span style={{ fontSize: 12, opacity: canInsert ? 0.6 : 0.35 }}>
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
                  type="checkbox"
                  aria-label={`Include ${col.name} in insert`}
                  checked={includedInInsert}
                  disabled={!canInsert}
                  onChange={(event) => {
                    if (!canInsert) {
                      return;
                    }

                    if (event.target.checked) {
                      setNewRow({ ...newRow, [col.name]: "" });
                      return;
                    }

                    setNewRow(omitKey(newRow, col.name));
                  }}
                  style={{
                    margin: 0,
                    width: 20,
                    height: 20,
                    cursor: canInsert ? "pointer" : "default",
                    flexShrink: 0,
                  }}
                />
                <input
                  aria-label={`New value for ${col.name}`}
                  value={isNull ? "" : String(rawVal ?? "")}
                  disabled={isNull || !canInsert || !includedInInsert}
                  onChange={(e) => {
                    if (!canInsert || !includedInInsert) return;
                    const next = e.target.value;
                    setNewRow({ ...newRow, [col.name]: next });
                  }}
                  placeholder={isNull ? "NULL" : "value"}
                  style={{
                    width: "100%",
                    minWidth: 0,
                    padding: "3px 6px",
                    fontSize: 12,
                    background: "var(--vscode-input-background)",
                    color:
                      isNull || !canInsert || !includedInInsert
                        ? "var(--vscode-disabledForeground)"
                        : "var(--vscode-input-foreground)",
                    border: "1px solid var(--vscode-input-border)",
                    borderRadius: 2,
                    fontFamily: "inherit",
                    opacity:
                      isNull || !canInsert || !includedInInsert ? 0.55 : 1,
                    fontStyle:
                      isNull || !canInsert || !includedInInsert
                        ? "italic"
                        : "normal",
                    boxSizing: "border-box" as const,
                  }}
                />
                {col.nullable && (
                  <button
                    type="button"
                    disabled={!canInsert || !includedInInsert}
                    onMouseDown={(e) => e.preventDefault()}
                    onClick={() =>
                      canInsert &&
                      includedInInsert &&
                      setNewRow(
                        isNull
                          ? {
                              ...newRow,
                              [col.name]: "",
                            }
                          : {
                              ...newRow,
                              [col.name]: NULL_SENTINEL,
                            },
                      )
                    }
                    title={
                      !canInsert
                        ? "This column is read-only"
                        : !includedInInsert
                          ? "Enable this field for insert first"
                          : isNull
                            ? "Replace NULL with empty value"
                            : "Set field to NULL"
                    }
                    style={{
                      flexShrink: 0,
                      height: 20,
                      padding: "0 5px",
                      fontSize: 12,
                      fontStyle: "italic",
                      fontFamily: "inherit",
                      background: isNull
                        ? "var(--vscode-button-foreground)"
                        : "var(--vscode-button-background)",
                      color:
                        !canInsert || !includedInInsert
                          ? "var(--vscode-disabledForeground)"
                          : isNull
                            ? "var(--vscode-button-background)"
                            : "var(--vscode-button-foreground)",
                      border: "none",
                      borderRadius: 2,
                      cursor:
                        canInsert && includedInInsert ? "pointer" : "default",
                      letterSpacing: "0.02em",
                      opacity:
                        !canInsert || !includedInInsert
                          ? 0.35
                          : isNull
                            ? 1
                            : 0.5,
                    }}
                  >
                    NULL
                  </button>
                )}
              </div>
            </div>
          );
        })}
      </div>
      <div
        style={{
          display: "flex",
          gap: 8,
          justifyContent: "flex-end",
          alignItems: "center",
        }}
      >
        <button
          type="button"
          style={{
            ...btnBase,
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
            background: "transparent",
            color: "var(--vscode-foreground)",
            border: "1px solid var(--vscode-panel-border)",
          }}
          onClick={onCancel}
        >
          Cancel
        </button>
      </div>
    </div>
  );
}
