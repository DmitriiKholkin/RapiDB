import type {
  ColumnTypeMeta,
  IDBDriver,
} from "../dbDrivers/types";

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

export function buildUpdateRowSql(
  drv: IDBDriver,
  database: string,
  schema: string,
  table: string,
  pkValues: Record<string, unknown>,
  changes: Record<string, unknown>,
  cols: ColumnTypeMeta[],
): { sql: string; params: unknown[] } | null {
  const qt = drv.qualifiedTableName(database, schema, table);
  const colMap = new Map(cols.map((c) => [c.name, c]));

  const coercedChanges = coerceRecord(
    drv,
    filterWritableRecord(changes, colMap),
    colMap,
  );
  const coercedPk = coerceRecord(drv, pkValues, colMap);

  const setCols = Object.keys(coercedChanges);
  const pkCols = Object.keys(coercedPk);
  if (setCols.length === 0 || pkCols.length === 0) return null;

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

  return {
    sql: `UPDATE ${qt} SET ${setParts.join(", ")} WHERE ${whereParts.join(" AND ")}`,
    params,
  };
}

function filterWritableRecord(
  record: Record<string, unknown>,
  colMap: Map<string, ColumnTypeMeta>,
): Record<string, unknown> {
  return Object.fromEntries(writableEntries(record, colMap));
}

function isWritableColumn(
  column: ColumnTypeMeta | undefined,
): column is ColumnTypeMeta {
  return !!column && column.editable && !column.isAutoIncrement;
}