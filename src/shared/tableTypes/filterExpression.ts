/**
 * Filter expression builder, draft normalization, and untrusted-input parser.
 *
 * Public surface (re-exported from the parent module):
 *   - buildFilterExpressionFromDraft
 *   - serializeFilterDrafts
 *   - deriveApplicableFilterDrafts
 *   - buildFilterExpression
 *   - coerceFilterExpressions
 */

import { valueFilterOperator } from "./columnFormatter";
import type {
  ColumnTypeMeta,
  FilterDraft,
  FilterDraftColumn,
  FilterDraftMap,
  FilterExpression,
  FilterOperator,
  ScalarFilterOperator,
} from "./types";
import { NULL_SENTINEL, SCALAR_FILTER_OPERATORS } from "./types";

// ─── Primitives ────────────────────────────────────────────────────────────

function trimOrNull(rawValue: string): string | null {
  const trimmed = rawValue.trim();
  return trimmed === "" ? null : trimmed;
}

function normalizeBetweenValues(
  rawValues: readonly [string, string],
): [string, string] | null {
  const start = trimOrNull(rawValues[0]);
  const end = trimOrNull(rawValues[1]);
  return start && end ? [start, end] : null;
}

function hasOperator(
  column: Pick<FilterDraftColumn, "filterOperators">,
  operator: FilterOperator,
): boolean {
  return column.filterOperators.includes(operator);
}

// ─── Draft → Expression ────────────────────────────────────────────────────

/**
 * Converts a single draft (or `null`/`undefined`) into a typed filter
 * expression. Returns `null` when the draft is incompatible with the
 * column (not filterable, missing value, ...).
 */
export function buildFilterExpressionFromDraft(
  column: FilterDraftColumn,
  draft: FilterDraft | null | undefined,
): FilterExpression | null {
  if (!draft) {
    return null;
  }

  switch (draft.operator) {
    case "is_null":
    case "is_not_null":
      return hasOperator(column, draft.operator)
        ? { column: column.name, operator: draft.operator }
        : null;

    case "between": {
      if (!column.filterable || !hasOperator(column, "between")) {
        return null;
      }
      const value = normalizeBetweenValues(draft.value);
      if (!value) {
        return null;
      }
      return { column: column.name, operator: "between", value };
    }

    default: {
      if (!column.filterable || !hasOperator(column, draft.operator)) {
        return null;
      }
      const value = trimOrNull(draft.value);
      if (!value) {
        return null;
      }
      return { column: column.name, operator: draft.operator, value };
    }
  }
}

/**
 * Serializes a `FilterDraftMap` into the array form consumed by
 * the data layer. Invalid drafts are silently dropped.
 */
export function serializeFilterDrafts(
  columns: readonly FilterDraftColumn[],
  drafts: FilterDraftMap | null | undefined,
): FilterExpression[] {
  if (!drafts) {
    return [];
  }
  return columns.flatMap<FilterExpression>((column) => {
    const expression = buildFilterExpressionFromDraft(
      column,
      drafts[column.name],
    );
    return expression ? [expression] : [];
  });
}

/**
 * Returns a new draft map containing only drafts that survive
 * `buildFilterExpressionFromDraft`. Used to keep form state in sync
 * with the filterable column set.
 */
export function deriveApplicableFilterDrafts(
  columns: readonly FilterDraftColumn[],
  drafts: FilterDraftMap | null | undefined,
): FilterDraftMap {
  if (!drafts) {
    return {};
  }
  return columns.reduce<FilterDraftMap>((activeDrafts, column) => {
    const draft = drafts[column.name];
    if (!draft || !buildFilterExpressionFromDraft(column, draft)) {
      return activeDrafts;
    }
    activeDrafts[column.name] = draft;
    return activeDrafts;
  }, {});
}

// ─── Raw value → Expression ────────────────────────────────────────────────

/**
 * Builds a filter expression from a raw user-typed value. Empty or
 * whitespace-only values return `null`. The `NULL_SENTINEL` is
 * translated to an `is_null` expression.
 */
export function buildFilterExpression(
  column: Pick<
    ColumnTypeMeta,
    "name" | "category" | "filterable" | "filterOperators"
  >,
  rawValue: string,
): FilterExpression | null {
  const value = trimOrNull(rawValue);
  if (!value) {
    return null;
  }
  if (value === NULL_SENTINEL) {
    return buildFilterExpressionFromDraft(column, { operator: "is_null" });
  }
  const operator = valueFilterOperator(column);
  if (!operator) {
    return null;
  }
  return buildFilterExpressionFromDraft(column, { operator, value });
}

// ─── Untrusted input → Expression[] ────────────────────────────────────────

/** True when `value` is a plain object with no `null` prototype tricks. */
function isRecordLike(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function parseBetweenValue(raw: unknown): { value: [string, string] } | null {
  if (!Array.isArray(raw) || raw.length !== 2) {
    return null;
  }
  if (typeof raw[0] !== "string" || typeof raw[1] !== "string") {
    return null;
  }
  const normalized = normalizeBetweenValues([raw[0], raw[1]]);
  return normalized ? { value: normalized } : null;
}

function parseScalarFilter(
  columnName: string,
  operator: unknown,
  value: unknown,
): FilterExpression | null {
  if (
    typeof operator !== "string" ||
    !SCALAR_FILTER_OPERATORS.has(operator as ScalarFilterOperator)
  ) {
    return null;
  }
  if (typeof value !== "string") {
    return null;
  }
  const trimmed = trimOrNull(value);
  if (!trimmed) {
    return null;
  }
  return {
    column: columnName,
    operator: operator as ScalarFilterOperator,
    value: trimmed,
  };
}

/**
 * Coerces an untrusted input (e.g. a message payload) into a
 * `FilterExpression[]`. Any malformed entry is dropped silently —
 * the caller is expected to surface a UX error if needed.
 *
 * Returns `[]` when `rawFilters` is not an array.
 */
export function coerceFilterExpressions(
  rawFilters: unknown,
): FilterExpression[] {
  if (!Array.isArray(rawFilters)) {
    return [];
  }

  return rawFilters.flatMap<FilterExpression>((rawFilter) => {
    if (!isRecordLike(rawFilter)) {
      return [];
    }
    const columnName =
      typeof rawFilter.column === "string" ? rawFilter.column : null;
    if (!columnName) {
      return [];
    }
    const operator = rawFilter.operator;

    if (operator === "is_null" || operator === "is_not_null") {
      return [{ column: columnName, operator }];
    }

    if (operator === "between") {
      const parsed = parseBetweenValue(rawFilter.value);
      return parsed
        ? [{ column: columnName, operator, value: parsed.value }]
        : [];
    }

    const scalar = parseScalarFilter(columnName, operator, rawFilter.value);
    return scalar ? [scalar] : [];
  });
}
