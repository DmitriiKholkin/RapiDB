import React, { useLayoutEffect, useRef, useState } from "react";
import { NULL_SENTINEL, type TypeCategory } from "../../../shared/tableTypes";
import { placeholderForCategory } from "../../types";
import { buildButtonStyle } from "../../utils/buttonStyles";
import { buildTextInputStyle } from "../../utils/controlStyles";
import { formatScalarValueForDisplay } from "../../utils/valueFormatting";
export function EditInput({
  initial,
  nullable,
  category,
  suppressPlaceholder = false,
  showDefaultButton = false,
  onSetDefault,
  readOnly = false,
  onCommit,
  onCancel,
}: {
  initial: string;
  nullable: boolean;
  category?: TypeCategory;
  suppressPlaceholder?: boolean;
  showDefaultButton?: boolean;
  onSetDefault?: () => void;
  readOnly?: boolean;
  onCommit: (v: string) => void;
  onCancel: () => void;
}) {
  const isInitiallyNull = initial === NULL_SENTINEL;
  const [isNull, setIsNull] = useState(false);
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
    ...buildTextInputStyle("sm"),
    flex: 1,
    minWidth: 0,
    height: "100%",
    fontSize: 12,
    color: isNull
      ? "var(--vscode-disabledForeground)"
      : "var(--vscode-input-foreground)",
    border: "0px",
    boxSizing: "border-box" as const,
    fontStyle: isNull ? "italic" : "normal",
  };
  const nullBtnStyle: React.CSSProperties = {
    ...buildButtonStyle("ghost", { size: "sm" }),
    flexShrink: 0,
    height: "100%",
    padding: "0 5px",
    fontSize: 9,
    fontStyle: "italic",
    background: "var(--vscode-input-foreground)",
    color: "var(--vscode-input-background)",
    border: "0px",
    letterSpacing: "0em",
  };
  return (
    <div
      style={{
        display: "flex",
        gap: 2,
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
          suppressPlaceholder
            ? undefined
            : isNull
              ? "NULL"
              : category
                ? placeholderForCategory(category)
                : undefined
        }
        style={inputStyle}
      />
      {showDefaultButton && !readOnly && onSetDefault && (
        <button
          type="button"
          onMouseDown={(e) => e.preventDefault()}
          onClick={onSetDefault}
          title="Set field to DEFAULT (omit from insert)"
          style={nullBtnStyle}
        >
          DEF
        </button>
      )}
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
export function valueToEditString(value: unknown): string {
  if (value == null) return NULL_SENTINEL;
  return formatScalarValueForDisplay(value);
}
