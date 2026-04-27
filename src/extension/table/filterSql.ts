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

  const colMap = new Map(cols.map((c) => [c.name, c]));
  const params: unknown[] = [];
  const conditions: string[] = [];

  for (const f of filters) {
    const meta = colMap.get(f.column);
    if (!meta) continue;

    const result = normalizeFilterCondition(drv, meta, f, params.length + 1);
    if (result) {
      conditions.push(result.sql);
      params.push(...result.params);
    }
  }

  if (conditions.length === 0) return { clause: "", params: [] };
  return { clause: `WHERE ${conditions.join(" AND ")}`, params };
}

function normalizeFilterCondition(
  drv: IDBDriver,
  column: ColumnTypeMeta,
  filter: FilterExpression,
  paramIndex: number,
) {
  if (filter.operator === "is_null" || filter.operator === "is_not_null") {
    if (!column.filterOperators.includes(filter.operator)) {
      throw unsupportedFilterOperatorError(column.name, filter.operator);
    }

    return drv.buildFilterCondition(
      column,
      filter.operator,
      undefined,
      paramIndex,
    );
  }

  if (!column.filterable) return null;

  if (!column.filterOperators.includes(filter.operator)) {
    throw unsupportedFilterOperatorError(column.name, filter.operator);
  }

  if (!("value" in filter)) {
    return null;
  }

  const normalizedValue = drv.normalizeFilterValue(
    column,
    filter.operator,
    filter.value,
  );

  return drv.buildFilterCondition(
    column,
    filter.operator,
    normalizedValue,
    paramIndex,
  );
}

function unsupportedFilterOperatorError(
  columnName: string,
  operator: FilterExpression["operator"],
): Error {
  return new Error(
    `[RapiDB Filter] Column ${columnName} does not support ${operator} filters.`,
  );
}
