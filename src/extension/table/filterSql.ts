import type {
  ColumnTypeMeta,
  FilterExpression,
  IDBDriver,
} from "../dbDrivers/types";

export function buildWhere(
  drv: IDBDriver,
  filters: FilterExpression[],
  cols: ColumnTypeMeta[],
): { clause: string; params: unknown[] } {
  if (filters.length === 0) return { clause: "", params: [] };

  validateFilterExpressions(filters, cols);

  const colMap = new Map(cols.map((c) => [c.name, c]));
  const params: unknown[] = [];
  const conditions: string[] = [];

  for (const f of filters) {
    const meta = colMap.get(f.column);
    if (!meta) {
      throw new Error(`[RapiDB Filter] Unknown filter column ${f.column}.`);
    }

    const result = normalizeFilterCondition(drv, meta, f, params.length + 1);
    if (!result) {
      throw new Error(
        `[RapiDB Filter] Column ${f.column} could not build ${f.operator} filter.`,
      );
    }
    conditions.push(result.sql);
    params.push(...result.params);
  }

  if (conditions.length === 0) return { clause: "", params: [] };
  return { clause: `WHERE ${conditions.join(" AND ")}`, params };
}

/**
 * Validate filters against discovered metadata without producing SQL. Native
 * NoSQL table-page readers use this same policy before dispatching a read.
 */
export function validateFilterExpressions(
  filters: FilterExpression[],
  cols: ColumnTypeMeta[],
): void {
  if (!Array.isArray(filters)) {
    throw new Error("[RapiDB Filter] Filters must be an array.");
  }

  const colMap = new Map(cols.map((column) => [column.name, column]));
  for (const expression of filters as unknown[]) {
    if (
      expression === null ||
      typeof expression !== "object" ||
      Array.isArray(expression)
    ) {
      throw new Error("[RapiDB Filter] Invalid filter expression.");
    }

    const filter = expression as Record<string, unknown>;
    const columnName = filter.column;
    const column =
      typeof columnName === "string" ? colMap.get(columnName) : undefined;
    if (!column) {
      throw new Error(
        `[RapiDB Filter] Unknown filter column ${String(columnName)}.`,
      );
    }

    const operator = filter.operator;
    const operatorLabel =
      typeof operator === "string" ? operator : String(operator);
    if (isNullFilterOperator(operator)) {
      if (!column.filterOperators.includes(operator)) {
        throw unsupportedFilterOperatorError(column.name, operator);
      }
      if (filter.value !== undefined) {
        throw new Error(
          `[RapiDB Filter] Column ${column.name} does not accept a value for ${operator} filters.`,
        );
      }
      continue;
    }

    if (!column.filterable) {
      throw new Error(
        `[RapiDB Filter] Column ${column.name} is not filterable.`,
      );
    }

    if (
      typeof operator !== "string" ||
      !column.filterOperators.includes(operator as FilterExpression["operator"])
    ) {
      throw unsupportedFilterOperatorError(column.name, operatorLabel);
    }

    if (!Object.hasOwn(filter, "value")) {
      throw new Error(
        `[RapiDB Filter] Column ${column.name} requires a value for ${operatorLabel} filters.`,
      );
    }

    const value = filter.value;
    if (operator === "between") {
      if (
        !Array.isArray(value) ||
        value.length !== 2 ||
        typeof value[0] !== "string" ||
        typeof value[1] !== "string"
      ) {
        throw new Error(
          `[RapiDB Filter] Column ${column.name} expects two string values for between filters.`,
        );
      }
      if (value.some((part: string) => part.trim() === "")) {
        throw requiredFilterValueError(column.name);
      }
      continue;
    }

    if (typeof value !== "string") {
      throw new Error(
        `[RapiDB Filter] Column ${column.name} expects a string value for ${operatorLabel} filters.`,
      );
    }
    if (value.trim() === "") {
      throw requiredFilterValueError(column.name);
    }
    if (
      operator === "in" &&
      value.split(",").every((entry) => entry.trim() === "")
    ) {
      throw requiredFilterValueError(column.name);
    }
  }
}

function normalizeFilterCondition(
  drv: IDBDriver,
  column: ColumnTypeMeta,
  filter: FilterExpression,
  paramIndex: number,
) {
  const filterValue = "value" in filter ? filter.value : undefined;
  const normalizedValue = isNullFilterOperator(filter.operator)
    ? undefined
    : drv.normalizeFilterValue(column, filter.operator, filterValue);

  return drv.buildFilterCondition(
    column,
    filter.operator,
    normalizedValue,
    paramIndex,
  );
}

function isNullFilterOperator(
  operator: unknown,
): operator is "is_null" | "is_not_null" {
  return operator === "is_null" || operator === "is_not_null";
}

function unsupportedFilterOperatorError(
  columnName: string,
  operator: string,
): Error {
  return new Error(
    `[RapiDB Filter] Column ${columnName} does not support ${operator} filters.`,
  );
}

function requiredFilterValueError(columnName: string): Error {
  return new Error(
    `[RapiDB Filter] Column ${columnName} expects a filter value.`,
  );
}
