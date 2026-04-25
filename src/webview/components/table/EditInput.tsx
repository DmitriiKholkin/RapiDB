import React, { useLayoutEffect, useRef, useState } from "react";
import { NULL_SENTINEL, type TypeCategory } from "../../../shared/tableTypes";
import { placeholderForCategory } from "../../types";
import { formatScalarValueForDisplay } from "../../utils/valueFormatting";

export function EditInput({
  initial,
  nullable,
  category,
  readOnly = false,
  onCommit,
  onCancel,
}: {
  initial: string;
  nullable: boolean;
  category?: TypeCategory;
  readOnly?: boolean;
  onCommit: (v: string) => void;
  onCancel: () => void;
}) {
  const isInitiallyNull = initial === NULL_SENTINEL;
  const [isNull, setIsNull] = useState(isInitiallyNull);
  const [val, setVal] = useState(isInitiallyNull ? "" : initial);
  const ref = useRef<HTMLInputElement>(null);

  useLayoutEffect(() => {
    ref.current?.focus();
    if (ref.current instanceof HTMLInputElement) {
      ref.current.select();
    }
  }, []);

  const commit = () => onCommit(isNull ? NULL_SENTINEL : val);

  const inputStyle: React.CSSProperties = {
    flex: 1,
    minWidth: 0,
    height: "100%",
    padding: "0px",
    fontSize: 12,
    fontFamily: "inherit",
    background: "var(--vscode-input-background)",
    color: isNull
      ? "var(--vscode-disabledForeground)"
      : "var(--vscode-input-foreground)",
    border: "0px",
    outline: "none",
    boxSizing: "border-box" as const,
    fontStyle: isNull ? "italic" : "normal",
  };

  const nullBtnStyle: React.CSSProperties = {
    flexShrink: 0,
    height: "100%",
    padding: "0 5px",
    fontSize: 9,
    fontStyle: "italic",
    fontFamily: "inherit",
    background: "var(--vscode-input-foreground)",
    color: "var(--vscode-input-background)",
    border: "0px",
    cursor: "pointer",
    letterSpacing: "0.02em",
  };

  return (
    <div
      style={{
        display: "flex",
        alignItems: "center",
        width: "100%",
        height: "100%",
      }}
    >
      <input
        ref={ref}
        aria-label="Cell value"
        value={val}
        readOnly={readOnly}
        onChange={(e) => {
          if (isNull) {
            setIsNull(false);
          }
          setVal(e.target.value);
        }}
        onKeyDown={(e) => {
          if (e.key === "Enter") {
            e.preventDefault();
            commit();
          }
          if (e.key === "Escape") {
            e.preventDefault();
            onCancel();
          }
        }}
        onBlur={commit}
        onClick={(e) => e.stopPropagation()}
        placeholder={
          isNull
            ? "NULL"
            : category
              ? placeholderForCategory(category)
              : undefined
        }
        style={inputStyle}
      />
      {nullable && !readOnly && (
        <button
          type="button"
          data-null-btn="1"
          onMouseDown={(e) => e.preventDefault()}
          onClick={() => onCommit(NULL_SENTINEL)}
          title="Set field to NULL"
          style={nullBtnStyle}
        >
          NULL
        </button>
      )}
    </div>
  );
}

/**
 * Convert a cell value to an initial string for the edit input.
 */
export function valueToEditString(value: unknown): string {
  if (value == null) return NULL_SENTINEL;
  return formatScalarValueForDisplay(value);
}
