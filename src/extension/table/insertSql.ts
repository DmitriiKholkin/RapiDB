import type {
  ColumnTypeMeta,
  IDBDriver,
  TransactionOperation,
} from "../dbDrivers/types";
import { writableEntries } from "./updateSql";

export function buildInsertRowOperation(
  drv: IDBDriver,
  database: string,
  schema: string,
  table: string,
  values: Record<string, unknown>,
  cols: ColumnTypeMeta[],
): TransactionOperation {
  const qt = drv.qualifiedTableName(database, schema, table);
  const colMap = new Map(cols.map((column) => [column.name, column]));
  const entries = writableEntries(values, colMap);

  if (entries.length === 0) {
    return {
      sql: buildDefaultValuesInsertSql(drv, qt, cols),
      params: [],
    };
  }

  const columnNames: string[] = [];
  const valueExpressions: string[] = [];
  const params: unknown[] = [];

  for (const [columnName, rawValue] of entries) {
    const column = colMap.get(columnName);
    if (!column) {
      continue;
    }

    columnNames.push(drv.quoteIdentifier(columnName));
    valueExpressions.push(drv.buildInsertValueExpr(column, params.length + 1));
    params.push(drv.coerceInputValue(rawValue, column));
  }

  if (columnNames.length === 0) {
    throw new Error(
      "Insert failed: no writable values were provided for this table.",
    );
  }

  return {
    sql: `INSERT INTO ${qt} (${columnNames.join(", ")}) VALUES (${valueExpressions.join(", ")})`,
    params,
  };
}

function buildDefaultValuesInsertSql(
  drv: IDBDriver,
  qualifiedTable: string,
  columns: readonly ColumnTypeMeta[],
): string {
  return drv.buildInsertDefaultValuesSql(qualifiedTable, columns);
}
