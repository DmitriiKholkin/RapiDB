import type { OperationCancellationContext } from "../../shared/safetyContracts";
import type { ConnectionConfig } from "../connectionManager";
import { SQLiteCoreDriver } from "./sqliteCore";
import { SQLiteWorkerClient } from "./sqliteWorkerClient";
import type { SQLiteWorkerMethod } from "./sqliteWorkerProtocol";
import type { DriverTimeoutSettingsProvider } from "./timeout";
import type {
  QueryExecutionOptions,
  QueryResult,
  TransactionContext,
  TransactionOperation,
  TransactionOptions,
} from "./types";

/** Pure coercion/filter/preview behavior is inherited; all DB work uses IPC. */
export class SQLiteDriver extends SQLiteCoreDriver {
  readonly driverTimeoutsManagedInternally = true;
  private readonly worker = new SQLiteWorkerClient();
  constructor(
    private readonly connectionConfig: ConnectionConfig,
    settings?: DriverTimeoutSettingsProvider,
  ) {
    super(connectionConfig, settings);
  }
  override connect(): Promise<void> {
    if (!this.connectionConfig.filePath)
      return Promise.reject(new Error("[RapiDB] SQLite requires a filePath"));
    return this.worker.open(
      this.connectionConfig,
      this.getConnectionTimeoutMs(),
    );
  }
  override disconnect(): Promise<void> {
    return this.worker.close();
  }
  override isConnected(): boolean {
    return this.worker.connected;
  }
  getAutomaticReconnectBlockReason(): string | undefined {
    if (!this.worker.sessionLost) return undefined;
    const recovery =
      this.connectionConfig.filePath === ":memory:"
        ? "This starts a new empty in-memory database."
        : "File data is retained, but ATTACH, temporary tables and session state must be recreated; refresh data before retrying mutations.";
    return `[RapiDB] SQLite session was lost. Automatic reconnect is disabled. Use Connect to reconnect explicitly. ${recovery}`;
  }
  cancelCurrentOperation(
    context?: OperationCancellationContext,
  ): Promise<void> {
    return this.worker.cancel(context);
  }
  private call<M extends SQLiteWorkerMethod>(
    method: M,
    args: Parameters<SQLiteCoreDriver[M]>,
  ): ReturnType<SQLiteCoreDriver[M]> {
    return this.worker.call(
      method,
      args,
      this.getDbOperationTimeoutMs(),
    ) as ReturnType<SQLiteCoreDriver[M]>;
  }
  override listDatabases() {
    return this.call("listDatabases", []);
  }
  override listSchemas(...args: Parameters<SQLiteCoreDriver["listSchemas"]>) {
    return this.call("listSchemas", args);
  }
  override listObjects(...args: Parameters<SQLiteCoreDriver["listObjects"]>) {
    return this.call("listObjects", args);
  }
  override describeTable(
    ...args: Parameters<SQLiteCoreDriver["describeTable"]>
  ) {
    return this.call("describeTable", args);
  }
  override describeColumns(
    ...args: Parameters<SQLiteCoreDriver["describeColumns"]>
  ) {
    return this.call("describeColumns", args);
  }
  override getIndexes(...args: Parameters<SQLiteCoreDriver["getIndexes"]>) {
    return this.call("getIndexes", args);
  }
  override getForeignKeys(
    ...args: Parameters<SQLiteCoreDriver["getForeignKeys"]>
  ) {
    return this.call("getForeignKeys", args);
  }
  override getConstraints(
    ...args: Parameters<SQLiteCoreDriver["getConstraints"]>
  ) {
    return this.call("getConstraints", args);
  }
  override getTriggers(...args: Parameters<SQLiteCoreDriver["getTriggers"]>) {
    return this.call("getTriggers", args);
  }
  override getConstraintDDL(
    ...args: Parameters<SQLiteCoreDriver["getConstraintDDL"]>
  ) {
    return this.call("getConstraintDDL", args);
  }
  override getIndexDDL(...args: Parameters<SQLiteCoreDriver["getIndexDDL"]>) {
    return this.call("getIndexDDL", args);
  }
  override getTriggerDDL(
    ...args: Parameters<SQLiteCoreDriver["getTriggerDDL"]>
  ) {
    return this.call("getTriggerDDL", args);
  }
  override getCreateTableDDL(
    ...args: Parameters<SQLiteCoreDriver["getCreateTableDDL"]>
  ) {
    return this.call("getCreateTableDDL", args);
  }
  override getObjectDefinition(
    ...args: Parameters<SQLiteCoreDriver["getObjectDefinition"]>
  ) {
    return this.call("getObjectDefinition", args);
  }
  override getRoutineDefinition(
    ...args: Parameters<SQLiteCoreDriver["getRoutineDefinition"]>
  ) {
    return this.call("getRoutineDefinition", args);
  }
  override query(
    sql: string,
    params?: unknown[],
    context?: QueryExecutionOptions,
  ): Promise<QueryResult> {
    return this.worker.call(
      "query",
      [sql, params, context],
      this.getDbOperationTimeoutMs(),
      context?.requestToken,
    );
  }
  override runTransaction(
    operations: TransactionOperation[],
    context?: TransactionContext,
    scope?: TransactionOptions,
  ): Promise<void> {
    return this.worker.call(
      "runTransaction",
      [operations, undefined, scope],
      this.getDbOperationTimeoutMs(),
      undefined,
      context?.signal,
      context?.deadline,
    );
  }
}
