import { isColumnReadOnly } from "../../shared/tableTypes";
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

export function unknownSqlMutationColumns(
  driver: IDBDriver,
  values: Record<string, unknown>,
  colMap: Map<string, ColumnTypeMeta>,
): string[] {
  if (driver.getCapabilities?.().tabularRead !== "sql") return [];
  return Object.entries(values)
    .filter(([name, value]) => value !== undefined && !colMap.has(name))
    .map(([name]) => name);
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
  onPreviewColumns?: (columns: Array<ColumnTypeMeta | undefined>) => void,
): { sql: string; params: unknown[] } | null {
  const qt = drv.qualifiedTableName(database, schema, table);
  const colMap = new Map(cols.map((c) => [c.name, c]));
  const unknownColumns = unknownSqlMutationColumns(drv, changes, colMap);
  if (unknownColumns.length > 0) {
    throw new Error(
      `Update contains unknown column${unknownColumns.length === 1 ? "" : "s"} ${unknownColumns.map((name) => `"${name}"`).join(", ")}. Refresh the table schema and try again.`,
    );
  }

  const coercedChanges = coerceRecord(
    drv,
    filterWritableRecord(changes, colMap),
    colMap,
  );
  if (Object.keys(coercedChanges).length === 0) return null;
  const coercedPk = coercePrimaryKeyValues(drv, pkValues, cols);
  const canUseSqlOriginalCoercion =
    drv.getCapabilities?.().tabularRead === "sql";
  const coercedOriginalValues = Object.fromEntries(
    writableEntries(originalValues, colMap).map(([columnName, value]) => {
      const column = colMap.get(columnName);
      return [
        columnName,
        column && canUseSqlOriginalCoercion && drv.coerceOriginalValue
          ? drv.coerceOriginalValue(value, column)
          : column
            ? drv.coerceInputValue(value, column)
            : value,
      ];
    }),
  );

  const setCols = Object.keys(coercedChanges);
  const pkCols = Object.keys(coercedPk);

  const params: unknown[] = [];
  const previewColumns: Array<ColumnTypeMeta | undefined> = [];
  const tableAlias = drv.getUpdateTableAlias?.() ?? undefined;
  const updateTarget = tableAlias
    ? `${qt} ${drv.quoteIdentifier(tableAlias)}`
    : qt;

  const setParts = setCols.map((c) => {
    const meta = colMap.get(c);
    if (meta && drv.buildMutationValueExpr) {
      const mutationValue = drv.buildMutationValueExpr(
        meta,
        coercedChanges[c],
        "update",
        coercedOriginalValues[c],
        params.length + 1,
      );
      if (mutationValue) {
        params.push(...mutationValue.params);
        previewColumns.push(
          ...(mutationValue.previewColumns ??
            mutationValue.params.map(() => meta)),
        );
        return `${drv.quoteIdentifier(c)} = ${mutationValue.sql}`;
      }
    }
    params.push(coercedChanges[c]);
    previewColumns.push(meta);
    return meta
      ? drv.buildSetExpr(meta, params.length)
      : `${drv.quoteIdentifier(c)} = ?`;
  });

  const whereParts = pkCols.map((c) => {
    params.push(coercedPk[c]);
    const meta = colMap.get(c);
    previewColumns.push(meta);
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
    const meta = colMap.get(columnName);
    const expandedComparison =
      meta && drv.buildOriginalValueComparisonWithParams
        ? drv.buildOriginalValueComparisonWithParams(
            meta,
            value,
            params.length + 1,
            tableAlias,
          )
        : null;
    if (expandedComparison) {
      params.push(...expandedComparison.params);
      previewColumns.push(
        ...(expandedComparison.previewColumns ??
          expandedComparison.params.map(() => meta)),
      );
      whereParts.push(expandedComparison.sql);
      continue;
    }
    params.push(value);
    previewColumns.push(meta);
    whereParts.push(
      meta && drv.buildOriginalValueComparison
        ? drv.buildOriginalValueComparison(meta, params.length)
        : `${identifier} = ${meta ? drv.buildInsertValueExpr(meta, params.length) : "?"}`,
    );
  }

  onPreviewColumns?.(previewColumns);
  return {
    sql: `UPDATE ${updateTarget} SET ${setParts.join(", ")} WHERE ${whereParts.join(" AND ")}`,
    params,
  };
}

function isWritableColumn(
  column: ColumnTypeMeta | undefined,
): column is ColumnTypeMeta {
  return !!column && !isColumnReadOnly(column);
}
