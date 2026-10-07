import { isServerGeneratedColumn } from "../../shared/tableTypes";
import type {
  ColumnTypeMeta,
  IDBDriver,
  TransactionOperation,
} from "../dbDrivers/types";
import {
  mssqlInsertIdentityCollation,
  mssqlInsertIdentityExpression,
  mssqlInsertIdentityType,
  oracleInsertIdentity,
} from "./insertIdentitySql";
import { writableEntries } from "./updateSql";

export interface InsertReturning {
  backend: "sqlite" | "pg" | "mssql" | "oracle";
  columns: ColumnTypeMeta[];
}

export function buildInsertRowOperation(
  drv: IDBDriver,
  database: string,
  schema: string,
  table: string,
  values: Record<string, unknown>,
  cols: ColumnTypeMeta[],
  returning?: InsertReturning,
): TransactionOperation {
  const qt = drv.qualifiedTableName(database, schema, table);
  const colMap = new Map(cols.map((column) => [column.name, column]));
  const entries = writableEntries(values, colMap);

  if (entries.length === 0) {
    return withReturning(
      drv,
      {
        sql: buildDefaultValuesInsertSql(drv, qt, cols),
        params: [],
      },
      returning,
      (output) => `INSERT INTO ${qt} ${output} DEFAULT VALUES`,
      { database, schema, table },
    );
  }

  const columnNames: string[] = [];
  const valueExpressions: string[] = [];
  const params: unknown[] = [];

  for (const [columnName, rawValue] of entries) {
    const column = colMap.get(columnName);
    if (!column) {
      continue;
    }

    const value = drv.coerceInputValue(rawValue, column);

    columnNames.push(drv.quoteIdentifier(columnName));
    if (value === null) {
      // Inline NULL to avoid backend-specific NULL parameter typing issues.
      valueExpressions.push("NULL");
      continue;
    }

    valueExpressions.push(drv.buildInsertValueExpr(column, params.length + 1));
    params.push(value);
  }

  if (columnNames.length === 0) {
    throw new Error(
      "Insert failed: no writable values were provided for this table.",
    );
  }

  return withReturning(
    drv,
    {
      sql: `INSERT INTO ${qt} (${columnNames.join(", ")}) VALUES (${valueExpressions.join(", ")})`,
      params,
    },
    returning,
    (output) =>
      `INSERT INTO ${qt} (${columnNames.join(", ")}) ${output} VALUES (${valueExpressions.join(", ")})`,
    { database, schema, table },
  );
}

/** Render identity clauses structurally; never search inside quoted identifiers. */
function withReturning(
  driver: IDBDriver,
  operation: TransactionOperation,
  returning: InsertReturning | undefined,
  renderOutput: (output: string) => string,
  target: { database: string; schema: string; table: string },
): TransactionOperation {
  if (!returning) return operation;
  const { backend, columns } = returning;
  const aliases = columns.map((_, index) =>
    driver.quoteIdentifier(`__col_${index}`),
  );
  operation.captureIdentity = {};
  if (backend === "mssql") {
    operation.captureIdentity.mssqlInsertTarget = target;
    const declarations = columns.map((column, index) => {
      const identityType = mssqlInsertIdentityType(column);
      const collation = mssqlInsertIdentityCollation(
        column.collation,
        identityType,
      );
      return `${aliases[index]} ${identityType}${collation}`;
    });
    const output = columns
      .map((column) => `INSERTED.${driver.quoteIdentifier(column.name)}`)
      .join(", ");
    operation.sql = `DECLARE @__rapidb_identity TABLE (${declarations.join(", ")});\n${renderOutput(`OUTPUT ${output} INTO @__rapidb_identity (${aliases.join(", ")})`)}`;
    const select = columns.map((column, index) => {
      const expression = mssqlInsertIdentityExpression(column, aliases[index]);
      return expression === aliases[index]
        ? expression
        : `${expression} AS ${aliases[index]}`;
    });
    operation.captureIdentity.mssqlSelect = `SELECT ${select.join(", ")} FROM @__rapidb_identity;`;
  } else if (backend === "oracle") {
    const types: Array<"string" | "buffer"> = [];
    const expressions = columns.map((column) => {
      const identity = oracleInsertIdentity(
        column,
        driver.quoteIdentifier(column.name),
      );
      types.push(identity.outType);
      return identity.expression;
    });
    const start = operation.params?.length ?? 0;
    operation.sql += ` RETURNING ${expressions.join(", ")} INTO ${columns.map((_, index) => `:${start + index + 1}`).join(", ")}`;
    operation.captureIdentity.oracleOutTypes = types;
  } else {
    operation.sql += ` RETURNING ${columns.map((column, index) => `${driver.quoteIdentifier(column.name)} AS ${aliases[index]}`).join(", ")}`;
  }
  operation.expectedAffectedRows = 1;
  return operation;
}

function buildDefaultValuesInsertSql(
  drv: IDBDriver,
  qualifiedTable: string,
  columns: readonly ColumnTypeMeta[],
): string {
  return drv.buildInsertDefaultValuesSql(
    qualifiedTable,
    columns.filter((column) => !isServerGeneratedColumn(column)),
  );
}
