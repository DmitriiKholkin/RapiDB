// biome-ignore lint/style/useImportType: React used as value in JSX refs
import React, { useLayoutEffect, useRef, useState } from "react";
import {
  NULL_SENTINEL,
  type TypeCategory,
} from "../../../shared/tableTypes";
import { placeholderForCategory } from "../../types";
import { formatScalarValueForDisplay } from "../../utils/valueFormatting";

const ROW_H = 26;

export function EditInput({
  initial,
  nullable,
  isBoolean,
  category,
  onCommit,
  onCancel,
}: {
  initial: string;
  nullable: boolean;
  isBoolean?: boolean;
  category?: TypeCategory;
  onCommit: (v: string) => void;
  onCancel: () => void;
}) {
  const isInitiallyNull = initial === NULL_SENTINEL;
  const [isNull, setIsNull] = useState(isInitiallyNull);
  const [val, setVal] = useState(isInitiallyNull ? "" : initial);
  const ref = useRef<HTMLInputElement | HTMLSelectElement>(null);

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
    height: ROW_H - 4,
    padding: "0 4px",
    fontSize: 12,
    fontFamily: "inherit",
    background: "var(--vscode-input-background)",
    color: isNull
      ? "var(--vscode-disabledForeground)"
      : "var(--vscode-input-foreground)",
    border: "1px solid var(--vscode-focusBorder)",
    borderRadius: 2,
    outline: "none",
    boxSizing: "border-box" as const,
    opacity: isNull ? 0.5 : 1,
    fontStyle: isNull ? "italic" : "normal",
  };

  const nullBtnStyle: React.CSSProperties = {
    flexShrink: 0,
    height: "100%",
    padding: "0 5px",
    fontSize: 9,
    fontStyle: "italic",
    fontFamily: "inherit",
    background: "transparent",
    color: "var(--vscode-badge-foreground)",
    border: "none",
    borderRadius: 2,
    cursor: "pointer",
    letterSpacing: "0.02em",
    opacity: 0.5,
  };

  // Boolean columns → select dropdown
  if (isBoolean || category === "boolean") {
    return (
      <div
        style={{ display: "flex", alignItems: "center", gap: 2, width: "100%" }}
      >
        <select
          ref={ref as React.RefObject<HTMLSelectElement>}
          value={isNull ? "" : val}
          onChange={(e) => {
            const next = e.target.value;
            if (next === "") {
              setIsNull(true);
              setVal("");
              return;
            }
            setIsNull(false);
            setVal(next);
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
          style={{
            ...inputStyle,
            cursor: "pointer",
            WebkitAppearance: "none",
          }}
        >
          {nullable && <option value="" />}
          <option value="false">false</option>
          <option value="true">true</option>
        </select>
        {nullable && (
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

  // Default: text input
  return (
    <div
      style={{ display: "flex", alignItems: "center", gap: 2, width: "100%" }}
    >
      <input
        ref={ref as React.RefObject<HTMLInputElement>}
        value={val}
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
              ? placeholderForCategory(category, false)
              : undefined
        }
        style={inputStyle}
      />
      {nullable && (
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
export function valueToEditString(
  value: unknown,
  isBoolean: boolean,
): string {
  if (isBoolean && value !== null && value !== undefined) {
    if (value === true || value === 1 || value === "1") return "true";
    if (value === false || value === 0 || value === "0") return "false";
    return String(value);
  }
  if (value == null) return NULL_SENTINEL;
  return formatScalarValueForDisplay(value);
}
