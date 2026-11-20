import type { MssqlSqlVariantRowIdentity } from "../../shared/mssqlSqlVariantRowIdentity";
import type { MySQLTimestampRowIdentity } from "../../shared/mysqlTimestampRowIdentity";
import type { OracleTemporalRowIdentity } from "../../shared/oracleTemporalRowIdentity";
import type { PostgresArrayRowIdentity } from "../../shared/postgresArrayRowIdentity";
import type { PostgresIntervalRowIdentity } from "../../shared/postgresIntervalRowIdentity";
import type { SqlDeleteObservation } from "../../shared/sqlDeleteObservation";
import type { SQLiteRowIdentity } from "../../shared/sqliteRowIdentity";
import type { ApplyResultPayload } from "../../shared/webviewContracts";
import type { SQLiteAttachmentTarget } from "../dbDrivers/sqliteAttachmentTarget";
import type {
  ColumnTypeMeta,
  MongoValueTypeHint,
  MutationSqlExpression,
  TransactionOperation,
  TransactionVerification,
} from "../dbDrivers/types";

export interface SortConfig {
  column: string;
  direction: "asc" | "desc";
}

export interface TablePage {
  columns: ColumnTypeMeta[];
  rows: Record<string, unknown>[];
  mongoIdTypes?: Array<"objectId" | "string" | null>;
  mongoValueTypes?: Array<Record<string, MongoValueTypeHint>>;
  rowValueTypes?: Array<Record<string, MongoValueTypeHint>>;
  sqliteRowIdentities?: SQLiteRowIdentity[];
  postgresArrayRowIdentities?: PostgresArrayRowIdentity[];
  postgresIntervalRowIdentities?: PostgresIntervalRowIdentity[];
  mysqlTimestampRowIdentities?: MySQLTimestampRowIdentity[];
  mssqlSqlVariantRowIdentities?: MssqlSqlVariantRowIdentity[];
  oracleTemporalRowIdentities?: OracleTemporalRowIdentity[];
  sqlDeleteObservations?: Array<SqlDeleteObservation | null>;
  totalCount: number;
  executionTimeMs?: number;
}

export interface RowUpdate {
  primaryKeys: Record<string, unknown>;
  changes: Record<string, unknown>;
  originalValues?: Record<string, unknown>;
  originalTypes?: Record<string, MongoValueTypeHint>;
}

export type ApplyResult = ApplyResultPayload;

export interface PreparedInsertPlan {
  connectionId: string;
  database: string;
  schema: string;
  table: string;
  sqliteAttachmentTarget?: SQLiteAttachmentTarget;
  mode?: "sql" | "driver";
  values?: Record<string, unknown>;
  operation: TransactionOperation;
  previewStatements: string[];
  verificationCriteria: Record<string, unknown> | null;
  verification?: TransactionVerification;
}

export interface PreparedDeletePlan {
  connectionId: string;
  database: string;
  schema: string;
  table: string;
  sqliteAttachmentTarget?: SQLiteAttachmentTarget;
  mode?: "sql" | "driver";
  executionMode: "sequential" | "transaction";
  primaryKeyValuesList?: Record<string, unknown>[];
  rowIdentities?: Record<string, unknown>[];
  operations: TransactionOperation[];
  previewStatements: string[];
  verificationCriteriaList: Record<string, unknown>[];
}

export interface VerificationTarget {
  rowIndex: number;
  primaryKeys: Record<string, unknown>;
  writtenKeyComparisons?: Record<string, MutationSqlExpression>;
  capturedIdentity?: {
    columns: ColumnTypeMeta[];
    operationIndex: number;
    sqlite: boolean;
    sqliteNullKeyColumns?: string[];
    oracle?: boolean;
  };
  values: Array<{
    column: ColumnTypeMeta;
    expectedValue: unknown;
    originalValue?: unknown;
  }>;
}

export interface PreparedApplyPlan {
  connectionId: string;
  database: string;
  schema: string;
  table: string;
  sqliteAttachmentTarget?: SQLiteAttachmentTarget;
  mode?: "sql" | "driver";
  cols: ColumnTypeMeta[];
  updates: RowUpdate[];
  operations: TransactionOperation[];
  previewStatements: string[];
  skippedRows: number[];
  verificationTargets: VerificationTarget[];
}

export type PreparedApplyPlanResult =
  | {
      executable: false;
      result: ApplyResult;
    }
  | {
      executable: true;
      plan: PreparedApplyPlan;
    };

export interface TableColumnsProvider {
  getColumns(
    connectionId: string,
    database: string,
    schema: string,
    table: string,
  ): Promise<ColumnTypeMeta[]>;
}
