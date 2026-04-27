import type { ApplyResultPayload } from "../../shared/webviewContracts";
import type { ColumnTypeMeta, TransactionOperation } from "../dbDrivers/types";

export interface SortConfig {
  column: string;
  direction: "asc" | "desc";
}

export interface TablePage {
  columns: ColumnTypeMeta[];
  rows: Record<string, unknown>[];
  totalCount: number;
}

export interface RowUpdate {
  primaryKeys: Record<string, unknown>;
  changes: Record<string, unknown>;
}

export type ApplyResult = ApplyResultPayload;

export interface PreparedInsertPlan {
  connectionId: string;
  database: string;
  schema: string;
  table: string;
  operation: TransactionOperation;
  previewStatements: string[];
  verificationCriteria: Record<string, unknown> | null;
}

export interface PreparedDeletePlan {
  connectionId: string;
  database: string;
  schema: string;
  table: string;
  executionMode: "sequential" | "transaction";
  operations: TransactionOperation[];
  previewStatements: string[];
  verificationCriteriaList: Record<string, unknown>[];
}

export interface VerificationTarget {
  rowIndex: number;
  primaryKeys: Record<string, unknown>;
  values: Array<{
    column: ColumnTypeMeta;
    expectedValue: unknown;
  }>;
}

export interface PreparedApplyPlan {
  connectionId: string;
  database: string;
  schema: string;
  table: string;
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
