export interface QueryLimitPolicy {
  hardCap: number;
}

export const QUERY_LIMIT_POLICY: QueryLimitPolicy = Object.freeze({
  hardCap: 10000,
});

export type OperationCancellationReason =
  | "timeout"
  | "superseded"
  | "manual"
  | "lifecycle_shutdown"
  | "late_settlement_after_timeout";

export interface OperationCancellationContext {
  reason: OperationCancellationReason;
  operationName: string;
  connectionId?: string;
  requestToken?: number;
  supersededByRequestToken?: number;
  timeoutKind?: "connection" | "dbOperation";
}

export interface QueryExecutionCancellationHandle {
  requestToken: number;
  connectionId: string;
  operationName: string;
  supportsCancellation: boolean;
  cancel(context: OperationCancellationContext): Promise<void>;
}

export interface ElasticsearchReadBudget {
  hardCap: number;
}

export const ELASTICSEARCH_READ_BUDGET: ElasticsearchReadBudget = Object.freeze(
  {
    hardCap: 10000,
  },
);

export interface SqlHardCapRewriteDecision {
  applied: boolean;
  reason?:
    | "unsupported_connection"
    | "non_limitable_statement"
    | "unsafe_with_clause";
}

export interface RedisReadBudget {
  maxScanKeys: number;
  maxValueReads: number;
  parallelValueReads: number;
}

export const REDIS_READ_BUDGET: RedisReadBudget = Object.freeze({
  maxScanKeys: 5000,
  maxValueReads: 2000,
  parallelValueReads: 24,
});

export interface ProblemDetails {
  type: string;
  title: string;
  status: number;
  detail?: string;
  instance?: string;
  [key: string]: unknown;
}

export interface ConnectionSecretUpdateTransaction {
  connectionId: string;
  useSecretStorage: boolean;
  previousSecretSnapshot?: string;
  nextSecretSnapshot?: string;
}
