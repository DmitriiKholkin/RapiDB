import type { ColumnTypeMeta, IDBDriver } from "../dbDrivers/types";

export function coerceRecord(
  drv: IDBDriver,
  record: Record<string, unknown>,
  colMap: Map<string, ColumnTypeMeta>,
): Record<string, unknown> {
  return Object.fromEntries(
    Object.entries(record).map(([k, v]) => {
      const meta = colMap.get(k);
      return [k, meta ? drv.coerceInputValue(v, meta) : v];
    }),
  );
}

export function writableEntries(
  values: Record<string, unknown>,
  colMap: Map<string, ColumnTypeMeta>,
): Array<[string, unknown]> {
  return Object.entries(values).filter(([columnName, value]) => {
    if (value === undefined) return false;
    return isWritableColumn(colMap.get(columnName));
  });
}

export function filterWritableRecord(
  record: Record<string, unknown>,
  colMap: Map<string, ColumnTypeMeta>,
): Record<string, unknown> {
  return Object.fromEntries(writableEntries(record, colMap));
}

export function assertExactPrimaryKeyShape(
  primaryKeyValues: Record<string, unknown>,
  columns: readonly ColumnTypeMeta[],
  operation: "update" | "delete" = "update",
): void {
  const expected = columns
    .filter((column) => column.isPrimaryKey)
    .map((column) => column.name);
  const provided = Object.keys(primaryKeyValues);
  const expectedSet = new Set(expected);
  if (
    expected.length === 0 ||
    provided.length !== expected.length ||
    provided.some(
      (columnName) =>
        !expectedSet.has(columnName) ||
        primaryKeyValues[columnName] === null ||
        primaryKeyValues[columnName] === undefined,
    )
  ) {
    throw new Error(
      operation === "delete"
        ? "Delete requires the full primary key for the target row, with no NULL or missing values."
        : "Update requires the full primary key for the target row, with no NULL or missing values.",
    );
  }
}

export function coercePrimaryKeyValues(
  driver: IDBDriver,
  values: Record<string, unknown>,
  columns: readonly ColumnTypeMeta[],
  operation: "update" | "delete" = "update",
): Record<string, unknown> {
  assertExactPrimaryKeyShape(values, columns, operation);
  const columnMap = new Map(columns.map((column) => [column.name, column]));
  const coerced = Object.fromEntries(
    Object.entries(values).map(([name, value]) => {
      const column = columnMap.get(name);
      if (!column) throw new Error("Unknown primary key column.");
      return [
        name,
        driver.coercePrimaryKeyValue
          ? driver.coercePrimaryKeyValue(value, column)
          : driver.coerceInputValue(value, column),
      ];
    }),
  );
  // Driver coercion can turn display values (including sentinels) into NULL.
  assertExactPrimaryKeyShape(coerced, columns, operation);
  return coerced;
}

export function buildUpdateRowSql(
  drv: IDBDriver,
  database: string,
  schema: string,
  table: string,
  pkValues: Record<string, unknown>,
  changes: Record<string, unknown>,
  cols: ColumnTypeMeta[],
  originalValues: Record<string, unknown> = {},
): { sql: string; params: unknown[] } | null {
  const qt = drv.qualifiedTableName(database, schema, table);
  const colMap = new Map(cols.map((c) => [c.name, c]));

  const coercedChanges = coerceRecord(
    drv,
    filterWritableRecord(changes, colMap),
    colMap,
  );
  if (Object.keys(coercedChanges).length === 0) return null;
  const coercedPk = coercePrimaryKeyValues(drv, pkValues, cols);
  const coercedOriginalValues = coerceRecord(
    drv,
    filterWritableRecord(originalValues, colMap),
    colMap,
  );

  const setCols = Object.keys(coercedChanges);
  const pkCols = Object.keys(coercedPk);

  const params: unknown[] = [];

  const setParts = setCols.map((c) => {
    params.push(coercedChanges[c]);
    const meta = colMap.get(c);
    return meta
      ? drv.buildSetExpr(meta, params.length)
      : `${drv.quoteIdentifier(c)} = ?`;
  });

  const whereParts = pkCols.map((c) => {
    params.push(coercedPk[c]);
    const meta = colMap.get(c);
    const placeholder = meta
      ? drv.buildInsertValueExpr(meta, params.length)
      : "?";
    return `${drv.quoteIdentifier(c)} = ${placeholder}`;
  });
  for (const [columnName, value] of Object.entries(coercedOriginalValues)) {
    const identifier = drv.quoteIdentifier(columnName);
    if (value === null) {
      whereParts.push(`${identifier} IS NULL`);
      continue;
    }
    params.push(value);
    const meta = colMap.get(columnName);
    whereParts.push(
      meta && drv.buildOriginalValueComparison
        ? drv.buildOriginalValueComparison(meta, params.length)
        : `${identifier} = ${meta ? drv.buildInsertValueExpr(meta, params.length) : "?"}`,
    );
  }

  return {
    sql: `UPDATE ${qt} SET ${setParts.join(", ")} WHERE ${whereParts.join(" AND ")}`,
    params,
  };
}

function isWritableColumn(
  column: ColumnTypeMeta | undefined,
): column is ColumnTypeMeta {
  return !!column;
}
