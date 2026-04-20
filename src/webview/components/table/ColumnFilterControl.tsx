// biome-ignore lint/correctness/noUnusedImports: <explanation>
import React, { type CSSProperties, useEffect, useRef, useState } from "react";
import {
  type ColumnTypeMeta as ColumnMeta,
  defaultFilterOperator,
  type FilterDraft,
  type FilterOperator,
  type ScalarFilterOperator,
} from "../../../shared/tableTypes";
import { placeholderForCategory } from "../../types";

const CLEAR_FILTER = "__clear__";

const OPERATOR_META: Record<FilterOperator, { token: string; label: string }> =
  {
    eq: { token: "=", label: "Equals" },
    neq: { token: "!=", label: "Not equal" },
    gt: { token: ">", label: "Greater than" },
    gte: { token: ">=", label: "Greater than or equal" },
    lt: { token: "<", label: "Less than" },
    lte: { token: "<=", label: "Less than or equal" },
    between: { token: "..", label: "Between" },
    like: { token: "~", label: "Contains" },
    ilike: { token: "~*", label: "Contains (case-insensitive)" },
    in: { token: "in", label: "In list" },
    is_null: { token: "N", label: "Is NULL" },
    is_not_null: { token: "!N", label: "Is NOT NULL" },
  };

const rootStyle: CSSProperties = {
  position: "relative",
  display: "flex",
  alignItems: "center",
  gap: 2,
  height: "100%",
};

const triggerStyle: CSSProperties = {
  flexShrink: 0,
  width: 22,
  height: "100%",
  padding: 0,
  borderRadius: 2,
  border: "1px solid var(--vscode-panel-border)",
  background: "var(--vscode-editor-background)",
  color: "var(--vscode-foreground)",
  fontFamily: "inherit",
  fontSize: 9,
  fontWeight: 700,
  cursor: "pointer",
  lineHeight: 1,
};

const inputStyle: CSSProperties = {
  flex: 1,
  minWidth: 0,
  height: "100%",
  padding: "0 4px",
  fontSize: 11,
  background: "var(--vscode-input-background)",
  color: "var(--vscode-input-foreground)",
  border: "1px solid transparent",
  borderRadius: 2,
  fontFamily: "inherit",
  boxSizing: "border-box",
};

const lockedInputStyle: CSSProperties = {
  color: "var(--vscode-disabledForeground)",
  opacity: 0.55,
  fontStyle: "italic",
  cursor: "default",
};

const menuStyle: CSSProperties = {
  position: "absolute",
  top: "calc(100% + 2px)",
  left: 0,
  minWidth: 188,
  padding: 4,
  display: "flex",
  flexDirection: "column",
  gap: 2,
  borderRadius: 4,
  background: "var(--vscode-menu-background, var(--vscode-editor-background))",
  border: "1px solid var(--vscode-widget-border, var(--vscode-panel-border))",
  boxShadow: "0 8px 20px rgba(0, 0, 0, 0.22)",
  zIndex: 6,
};

const menuItemStyle: CSSProperties = {
  width: "100%",
  padding: "4px 6px",
  display: "flex",
  alignItems: "center",
  gap: 8,
  border: "none",
  borderRadius: 3,
  background: "transparent",
  color: "inherit",
  fontFamily: "inherit",
  fontSize: 11,
  textAlign: "left",
  cursor: "pointer",
};

const tokenStyle: CSSProperties = {
  width: 24,
  flexShrink: 0,
  textAlign: "center",
  fontSize: 9,
  fontWeight: 700,
  letterSpacing: "0.02em",
};

function isScalarOperator(
  operator: FilterOperator,
): operator is ScalarFilterOperator {
  return (
    operator !== "between" &&
    operator !== "is_null" &&
    operator !== "is_not_null"
  );
}

function isInputOperator(
  operator: FilterOperator,
): operator is ScalarFilterOperator | "between" {
  return operator !== "is_null" && operator !== "is_not_null";
}

function getAvailableOperators(column: ColumnMeta): FilterOperator[] {
  return column.filterOperators.filter((operator) =>
    isInputOperator(operator) ? column.filterable : true,
  );
}

function getPreferredOperator(column: ColumnMeta): FilterOperator | null {
  const operators = getAvailableOperators(column);
  if (operators.length === 0) {
    return null;
  }

  if (column.filterable) {
    const defaultOperator = defaultFilterOperator(column);
    if (operators.includes(defaultOperator)) {
      return defaultOperator;
    }

    const firstScalar = operators.find(isScalarOperator);
    if (firstScalar) {
      return firstScalar;
    }

    if (operators.includes("between")) {
      return "between";
    }
  }

  return null;
}

function nextDraftForOperator(
  operator: FilterOperator,
  draft: FilterDraft | undefined,
): FilterDraft {
  switch (operator) {
    case "is_null":
    case "is_not_null":
      return { operator };
    case "between": {
      if (draft?.operator === "between") {
        return draft;
      }

      const start =
        draft && isScalarOperator(draft.operator) ? draft.value : "";
      return { operator, value: [start, ""] };
    }
    default: {
      if (draft?.operator === operator) {
        return draft;
      }

      const value =
        draft?.operator === "between"
          ? draft.value[0]
          : draft && isScalarOperator(draft.operator)
            ? draft.value
            : "";
      return { operator, value };
    }
  }
}

interface ColumnFilterControlProps {
  column: ColumnMeta;
  draft?: FilterDraft;
  onChange: (nextDraft: FilterDraft | undefined) => void;
}

export function ColumnFilterControl({
  column,
  draft,
  onChange,
}: ColumnFilterControlProps) {
  const [menuOpen, setMenuOpen] = useState(false);
  const rootRef = useRef<HTMLDivElement>(null);

  const availableOperators = getAvailableOperators(column);
  const normalizedDraft =
    draft && availableOperators.includes(draft.operator) ? draft : undefined;
  const selectedOperator =
    normalizedDraft?.operator ?? getPreferredOperator(column);
  const selectedMeta = selectedOperator
    ? OPERATOR_META[selectedOperator]
    : { token: "x", label: "No filter" };
  const hasMenu = availableOperators.length > 0;
  const isBetween = selectedOperator === "between";
  const isNullabilityOperator =
    selectedOperator === "is_null" || selectedOperator === "is_not_null";
  const scalarOperator =
    selectedOperator && isScalarOperator(selectedOperator)
      ? selectedOperator
      : null;

  useEffect(() => {
    if (!menuOpen) {
      return;
    }

    const handlePointerDown = (event: MouseEvent) => {
      if (!rootRef.current?.contains(event.target as Node)) {
        setMenuOpen(false);
      }
    };

    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        setMenuOpen(false);
      }
    };

    document.addEventListener("mousedown", handlePointerDown);
    document.addEventListener("keydown", handleKeyDown);

    return () => {
      document.removeEventListener("mousedown", handlePointerDown);
      document.removeEventListener("keydown", handleKeyDown);
    };
  }, [menuOpen]);

  const scalarValue =
    scalarOperator && normalizedDraft?.operator === scalarOperator
      ? normalizedDraft.value
      : "";
  const betweenValue =
    normalizedDraft?.operator === "between" ? normalizedDraft.value : ["", ""];
  const scalarPlaceholder =
    scalarOperator === "in"
      ? "value1, value2"
      : placeholderForCategory(column.category, column.isBoolean);

  const handleOperatorSelect = (
    operator: FilterOperator | typeof CLEAR_FILTER,
  ) => {
    setMenuOpen(false);

    if (operator === CLEAR_FILTER) {
      onChange(undefined);
      return;
    }

    onChange(nextDraftForOperator(operator, normalizedDraft));
  };

  return (
    <div ref={rootRef} style={rootStyle}>
      <button
        type="button"
        aria-label={`${column.name} filter operator`}
        aria-expanded={hasMenu ? menuOpen : undefined}
        aria-haspopup={hasMenu ? "menu" : undefined}
        disabled={!hasMenu}
        onClick={() => {
          if (hasMenu) {
            setMenuOpen((open) => !open);
          }
        }}
        title={selectedMeta.label}
        style={{
          ...triggerStyle,
          opacity: hasMenu ? 1 : 0.4,
          cursor: hasMenu ? "pointer" : "default",
        }}
      >
        {selectedMeta.token}
      </button>

      {menuOpen && (
        <div
          role="menu"
          aria-label={`${column.name} filter operators`}
          style={menuStyle}
        >
          <button
            type="button"
            role="menuitemradio"
            aria-checked={normalizedDraft === undefined}
            onClick={() => handleOperatorSelect(CLEAR_FILTER)}
            style={{
              ...menuItemStyle,
              background:
                normalizedDraft === undefined
                  ? "var(--vscode-list-activeSelectionBackground, rgba(128,128,128,0.16))"
                  : "transparent",
            }}
          >
            <span style={tokenStyle}>x</span>
            <span>No filter</span>
          </button>

          {availableOperators.map((operator) => {
            const meta = OPERATOR_META[operator];
            const isSelected = normalizedDraft?.operator === operator;
            return (
              <button
                key={operator}
                type="button"
                role="menuitemradio"
                aria-checked={isSelected}
                onClick={() => handleOperatorSelect(operator)}
                style={{
                  ...menuItemStyle,
                  background: isSelected
                    ? "var(--vscode-list-activeSelectionBackground, rgba(128,128,128,0.16))"
                    : "transparent",
                }}
              >
                <span style={tokenStyle}>{meta.token}</span>
                <span>{meta.label}</span>
              </button>
            );
          })}
        </div>
      )}

      {isBetween ? (
        <div
          style={{
            display: "flex",
            gap: 2,
            flex: 1,
            minWidth: 0,
            height: "100%",
          }}
        >
          <input
            aria-label={`${column.name} filter start`}
            value={betweenValue[0]}
            onChange={(event) =>
              onChange({
                operator: "between",
                value: [event.target.value, betweenValue[1]],
              })
            }
            placeholder="from"
            style={inputStyle}
          />
          <input
            aria-label={`${column.name} filter end`}
            value={betweenValue[1]}
            onChange={(event) =>
              onChange({
                operator: "between",
                value: [betweenValue[0], event.target.value],
              })
            }
            placeholder="to"
            style={inputStyle}
          />
        </div>
      ) : (
        <input
          aria-label={`${column.name} filter value`}
          value={isNullabilityOperator ? "NULL" : scalarValue}
          disabled={!scalarOperator || isNullabilityOperator}
          onChange={(event) => {
            if (!scalarOperator) {
              return;
            }

            onChange({
              operator: scalarOperator,
              value: event.target.value,
            });
          }}
          placeholder={scalarOperator ? scalarPlaceholder : ""}
          style={{
            ...inputStyle,
            ...(!scalarOperator || isNullabilityOperator
              ? lockedInputStyle
              : null),
          }}
        />
      )}
    </div>
  );
}
