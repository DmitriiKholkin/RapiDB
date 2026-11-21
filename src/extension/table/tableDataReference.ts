import type { ColumnTypeMeta, IDBDriver } from "../dbDrivers/types";

/** A FROM / UPDATE / DELETE target, not an identifier for qualifying columns. */
export function tableDataReference(
  driver: IDBDriver,
  database: string,
  schema: string,
  table: string,
  columns: readonly ColumnTypeMeta[],
): string {
  return (
    driver.getTableDataReference?.(database, schema, table, columns) ??
    driver.qualifiedTableName(database, schema, table)
  );
}
