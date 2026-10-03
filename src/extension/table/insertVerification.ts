import { canonicalizeExactNumeric } from "../dbDrivers/numericUtils";
import type {
  ColumnTypeMeta,
  IDBDriver,
  TransactionOperation,
  TransactionVerification,
} from "../dbDrivers/types";
import {
  mssqlInsertIdentityExpression,
  oracleInsertIdentity,
} from "./insertIdentitySql";
import { buildInsertRowOperation } from "./insertSql";
import { writableEntries } from "./updateSql";

/** Keep requested values, not bind/coerced values, as the verification baseline. */
export function prepareInsertVerification(
  driver: IDBDriver,
  backend: string | undefined,
  database: string,
  schema: string,
  table: string,
  requested: Record<string, unknown>,
  columns: ColumnTypeMeta[],
  operation: TransactionOperation,
): TransactionVerification | undefined {
  const columnMap = new Map(columns.map((column) => [column.name, column]));
  const keys = columns.filter((column) => column.isPrimaryKey);
  const writable = writableEntries(requested, columnMap);
  const values = writable.flatMap(([name, expectedValue]) => {
    const column = columnMap.get(name);
    // NULL is a request to generate a key for SQLite's INTEGER rowid alias and
    // MySQL AUTO_INCREMENT, not a request to persist a NULL primary key.
    if (
      column?.isPrimaryKey &&
      driver.coerceInputValue(expectedValue, column) === null &&
      ((backend === "sqlite" &&
        keys.length === 1 &&
        column.nativeType.trim().toUpperCase() === "INTEGER") ||
        (backend === "mysql" && column.identityGeneration === "auto_increment"))
    )
      return [];
    return column &&
      driver.checkPersistedEdit(column, expectedValue)?.shouldVerify
      ? [{ column, expectedValue }]
      : [];
  });
  if (!values.length) return undefined;
  if (!keys.length) {
    throw new Error(
      "INSERT verification requires a reliable primary key; no data was written.",
    );
  }
  const qualified = driver.qualifiedTableName(database, schema, table);
  const keyValues = keys.map((column) =>
    requested[column.name] === undefined
      ? null
      : driver.coerceInputValue(requested[column.name], column),
  );
  const generated = keys.some(
    (column, index) =>
      requested[column.name] === undefined || keyValues[index] === null,
  );
  const supportsReturning =
    backend === "sqlite" ||
    backend === "pg" ||
    backend === "mssql" ||
    backend === "oracle";
  const mysqlAutoKey =
    backend === "mysql"
      ? keys.find((column) => column.identityGeneration === "auto_increment")
      : undefined;
  const captureKeys = supportsReturning || Boolean(mysqlAutoKey);
  if (captureKeys || generated) {
    if (
      backend === "sqlite" ||
      backend === "pg" ||
      backend === "mssql" ||
      backend === "oracle"
    ) {
      Object.assign(
        operation,
        buildInsertRowOperation(
          driver,
          database,
          schema,
          table,
          requested,
          columns,
          { backend, columns: keys },
        ),
      );
    } else if (backend === "mysql") {
      if (!mysqlAutoKey) {
        throw new Error(
          "INSERT verification cannot capture this generated MySQL primary key; no data was written.",
        );
      }
      operation.captureIdentity = {
        // Resolve all key components from this packet's auto key; reject ambiguity.
        mysqlInsertId: {
          sql: `SELECT ${keys.map((column, index) => `${driver.quoteIdentifier(column.name)} AS ${driver.quoteIdentifier(`__col_${index}`)}`).join(", ")} FROM ${qualified} WHERE ${driver.quoteIdentifier(mysqlAutoKey.name)} = ${driver.buildInsertValueExpr(mysqlAutoKey, 1)} LIMIT 2`,
          // Resolve zero from this operation's session SQL mode, not planning metadata.
          ...(writable.some(
            ([name, value]) =>
              name === mysqlAutoKey.name &&
              canonicalizeExactNumeric(
                driver.coerceInputValue(value, mysqlAutoKey),
                null,
              )?.canonical === "0",
          )
            ? {
                zero: {
                  columnName: mysqlAutoKey.name,
                  keyIndex: keys.indexOf(mysqlAutoKey),
                  guardSql: `SELECT 1 FROM ${qualified} WHERE ${driver.quoteIdentifier(mysqlAutoKey.name)} = ${driver.buildInsertValueExpr(mysqlAutoKey, 1)} LIMIT 1 FOR UPDATE`,
                },
              }
            : {}),
        },
      };
      operation.expectedAffectedRows = 1;
    } else {
      throw new Error(
        "INSERT verification cannot capture this generated primary key on this backend; no data was written. Provide the primary key explicitly.",
      );
    }
  }
  if (backend === "mysql" && !captureKeys) {
    operation.captureIdentity = {
      mysqlKeyGuard: {
        sql: `SELECT 1 FROM ${qualified} WHERE ${keys.map((column, index) => `${driver.quoteIdentifier(column.name)} = ${driver.buildInsertValueExpr(column, index + 1)}`).join(" AND ")} LIMIT 1 FOR UPDATE`,
        params: keyValues,
      },
    };
    operation.expectedAffectedRows = 1;
  }
  return {
    rowIndex: 0,
    mutation: "insert",
    sql: `SELECT ${values.map(({ column }, index) => `${driver.quoteIdentifier(column.name)} AS ${driver.quoteIdentifier(`__col_${index}`)}`).join(", ")} FROM ${qualified} WHERE ${keys
      .map((column, index) => {
        const name = driver.quoteIdentifier(column.name);
        const expression =
          captureKeys && backend === "oracle"
            ? oracleInsertIdentity(column, name).expression
            : captureKeys && backend === "mssql"
              ? mssqlInsertIdentityExpression(column, name)
              : name;
        const bind =
          captureKeys && backend === "mssql"
            ? "?"
            : driver.buildInsertValueExpr(column, index + 1);
        return `${expression} ${backend === "sqlite" ? "IS" : "="} ${bind}`;
      })
      .join(" AND ")}`,
    params: captureKeys ? keys.map(() => null) : keyValues,
    values,
    ...(captureKeys
      ? {
          identity: {
            operationIndex: 0,
            parameterIndexes: keys.map((_, index) => index),
            ...(backend === "sqlite" ? { allowNull: true } : {}),
          },
        }
      : {}),
  };
}
