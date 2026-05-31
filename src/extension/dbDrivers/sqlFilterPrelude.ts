import type {
  ColumnTypeMeta,
  FilterConditionResult,
  FilterOperator,
} from "./types";

export interface SqlFilterPreambleInput {
  column: ColumnTypeMeta;
  operator: FilterOperator;
  value: string | [string, string] | undefined;
  quoteIdentifier: (identifier: string) => string;
}

export type SqlFilterPreambleResult =
  | {
      kind: "resolved";
      condition: FilterConditionResult;
    }
  | {
      kind: "ready";
      columnSql: string;
      value: string | [string, string];
    }
  | null;

export function createSqlFilterPreamble({
  column,
  operator,
  value,
  quoteIdentifier,
}: SqlFilterPreambleInput): SqlFilterPreambleResult {
  const columnSql = quoteIdentifier(column.name);
  switch (operator) {
    case "is_null":
      return {
        kind: "resolved",
        condition: { sql: `${columnSql} IS NULL`, params: [] },
      };
    case "is_not_null":
      return {
        kind: "resolved",
        condition: { sql: `${columnSql} IS NOT NULL`, params: [] },
      };
    default:
      if (!column.filterable || value === undefined) {
        return null;
      }

      return {
        kind: "ready",
        columnSql,
        value: typeof value === "string" ? value.trim() : value,
      };
  }
}
