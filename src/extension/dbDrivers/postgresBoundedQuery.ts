import { Query, type QueryArrayConfig, type QueryArrayResult } from "pg";
import pgPackage from "pg/package.json";
import { BoundedQueryRows } from "./boundedQueryRows";
import { PostgresQueryResultBudget } from "./postgresQueryResultBudget";

type Result = QueryArrayResult<unknown[]> & {
  parseRow(fields: readonly unknown[]): unknown[];
};
type Completion = (error: Error | null, result?: Result) => void;
export type BoundedPgResultField = {
  name: string;
  dataTypeID?: number;
};
export type BoundedPgResultSet = {
  fields: BoundedPgResultField[];
  rows: unknown[][];
  rowCount: number;
  truncated: boolean;
  truncatedBySize?: boolean;
  completed: boolean;
  partial?: boolean;
};

export const POSTGRES_MAX_RETAINED_RESULT_SETS = 100;
export const POSTGRES_MAX_RETAINED_RESULT_COLUMNS = 4096;

const POSTGRES_AFFECTED_ROW_COMMANDS: ReadonlySet<string> = new Set([
  "INSERT",
  "UPDATE",
  "DELETE",
  "MERGE",
]);

// Command tags count only the top-level statement (never trigger work), so a
// script's DML total is the sum of its DML tags; SELECT/SHOW tags are skipped.
export function addPostgresAffectedRows(
  total: number | undefined,
  result: { command?: string | null; rowCount?: number | null },
): number | undefined {
  if (!result.command || !POSTGRES_AFFECTED_ROW_COMMANDS.has(result.command))
    return total;
  return (total ?? 0) + (result.rowCount ?? 0);
}

const StreamingQueryBase = Query as unknown as new (
  config: QueryArrayConfig,
  callback: Completion,
) => Query<unknown[]> & {
  _result: Result;
  _results: Result | Result[];
  _accumulateRows: boolean | Completion;
  _canceledDueToError: Error | false;
  _checkForMultirow(): void;
  handleCommandComplete(message: { text?: string }, connection: object): void;
  handleError(error: Error, connection: object): void;
  handleReadyForQuery(connection: object): void;
  handleRowDescription(message: unknown): void;
  handleDataRow(message: unknown): void;
};

const SUPPORTED_PG_VERSION = "8.21.0";

function assertStreamingCompatibility(
  query: InstanceType<typeof StreamingQueryBase>,
): void {
  const incompatible = (detail: string): never => {
    throw new Error(
      `[RapiDB] Cannot safely bound PostgreSQL query results: incompatible pg ${pgPackage.version} (${detail}). Expected the JavaScript Query implementation from pg ${SUPPORTED_PG_VERSION}; query was not submitted. No fully buffered fallback is allowed.`,
    );
  };
  if (pgPackage.version !== SUPPORTED_PG_VERSION)
    incompatible("unvalidated version");
  if (query._canceledDueToError !== false)
    incompatible("unexpected parser-error state");
  for (const method of [
    "_checkForMultirow",
    "handleRowDescription",
    "handleDataRow",
    "handleCommandComplete",
    "handleReadyForQuery",
    "submit",
  ]) {
    if (typeof Reflect.get(StreamingQueryBase.prototype, method) !== "function")
      incompatible(`missing Query.${method}`);
  }
  const result = query._result;
  if (
    !result ||
    query._results !== result ||
    !Array.isArray(result.rows) ||
    result.rows.length !== 0 ||
    !Array.isArray(result.fields) ||
    result.fields.length !== 0 ||
    Reflect.get(result, "rowAsArray") !== true ||
    result.command !== null ||
    result.rowCount !== null
  )
    incompatible("unexpected _result/_results shape");
  for (const method of [
    "addFields",
    "parseRow",
    "addRow",
    "addCommandComplete",
  ]) {
    if (typeof Reflect.get(result, method) !== "function")
      incompatible(`missing Result.${method}`);
  }
  try {
    const probe = new StreamingQueryBase(
      { text: "", rowMode: "array" },
      () => {},
    );
    let emitted = 0;
    probe.on("row", () => emitted++);
    probe.handleRowDescription({ fields: [] });
    if (typeof probe._accumulateRows !== "function")
      throw new Error("missing _accumulateRows callback switch");
    probe._accumulateRows = false;
    probe.handleDataRow({ fields: [] });
    if (emitted !== 1 || probe._result.rows.length !== 0)
      throw new Error("_accumulateRows does not disable row buffering");
  } catch (error) {
    incompatible(
      `row-buffering probe failed: ${error instanceof Error ? error.message : String(error)}`,
    );
  }
}

export class BoundedPostgresQuery extends StreamingQueryBase {
  readonly retained: BoundedQueryRows<unknown[]>;
  readonly resultSets: BoundedPgResultSet[] = [];
  resultSetsTruncated = false;
  affectedRows: number | undefined;
  private activeResultSet: BoundedPgResultSet | undefined;
  private activeResultSetCompleted = false;
  private retainedRowCount = 0;
  private seenResultSetCount = 0;
  private retainedColumnCount = 0;

  constructor(
    text: string,
    values: unknown[],
    limit: number,
    complete: Completion,
    private readonly budget = new PostgresQueryResultBudget(),
  ) {
    super({ text, values, rowMode: "array" }, complete);
    assertStreamingCompatibility(this);
    this.retained = new BoundedQueryRows(limit);
  }

  override handleDataRow(message: { fields: unknown[] }): void {
    const output = this.activeResultSet;
    if (!output || this._canceledDueToError !== false) return;
    let row: unknown[] | undefined;
    if (!output.truncated && this.retainedRowCount < this.retained.limit) {
      const wireBytes = this.budget.wireRowSize(message.fields);
      if (wireBytes === undefined) {
        output.truncatedBySize = true;
      } else {
        // Match pg's parser-error handling, which reports the error at
        // ReadyForQuery. A size limit is NOT a parser/execution error.
        let decoded: unknown[];
        try {
          decoded = this._result.parseRow(message.fields);
        } catch (error) {
          this._canceledDueToError = error as Error;
          return;
        }
        if (this.budget.retainRow(decoded, wireBytes)) row = decoded;
        else output.truncatedBySize = true;
      }
    }
    output.rowCount++;
    if (row) {
      this.retained.add(row);
      this.retainedRowCount++;
    } else {
      // Keep a prefix in each set. Drain/count subsequent rows and continue
      // executing the script without decoding data that won't be displayed.
      this.retained.rowCount++;
      output.truncated = true;
    }
  }

  override _checkForMultirow(): void {
    const previous = this._result;
    super._checkForMultirow();
    this._results = this._result;
    if (previous !== this._result) {
      this.retained.reset();
      this.activeResultSet = undefined;
      this.activeResultSetCompleted = false;
    }
  }

  override handleRowDescription(message: unknown): void {
    const fields =
      (message as { fields?: BoundedPgResultField[] }).fields ?? [];
    this.seenResultSetCount++;
    this.activeResultSet = undefined;
    this.activeResultSetCompleted = false;
    if (
      this.seenResultSetCount > POSTGRES_MAX_RETAINED_RESULT_SETS ||
      this.retainedColumnCount + fields.length >
        POSTGRES_MAX_RETAINED_RESULT_COLUMNS ||
      !this.budget.retainFields(fields)
    ) {
      this._checkForMultirow();
      this.resultSetsTruncated = true;
      this._accumulateRows = false;
      return;
    }
    super.handleRowDescription(message);
    const output: BoundedPgResultSet = {
      fields: fields.map(({ name, dataTypeID }) => ({ name, dataTypeID })),
      rows: this.retained.rows,
      rowCount: 0,
      truncated: false,
      completed: false,
    };
    this.resultSets.push(output);
    this.retainedColumnCount += fields.length;
    this.activeResultSet = output;
    this.activeResultSetCompleted = false;
    this._accumulateRows = false;
  }

  override handleCommandComplete(
    message: { text?: string },
    connection: object,
  ): void {
    super.handleCommandComplete(message, connection);
    this.affectedRows = addPostgresAffectedRows(
      this.affectedRows,
      this._result,
    );
    if (this.activeResultSet) {
      const decodedCompletely = this._canceledDueToError === false;
      this.activeResultSetCompleted = decodedCompletely;
      this.activeResultSet.completed = decodedCompletely;
      if (!decodedCompletely) this.activeResultSet.partial = true;
      this.activeResultSet.truncated =
        this.activeResultSet.rowCount > this.activeResultSet.rows.length;
    }
  }

  override handleReadyForQuery(connection: object): void {
    super.handleReadyForQuery(connection);
  }

  override handleError(error: Error, connection: object): void {
    super.handleError(error, connection);
  }

  markIncompleteResultPartial(): void {
    if (this.activeResultSet && !this.activeResultSetCompleted) {
      this.activeResultSet.partial = true;
    }
  }

  markAllResultSetsPartial(): void {
    for (const resultSet of this.resultSets) resultSet.partial = true;
  }

  get hasRecoverableResult(): boolean {
    return this.resultSets.some(
      (resultSet) => resultSet.completed || resultSet.rowCount > 0,
    );
  }

  get currentResult(): Result {
    return this._result;
  }
}
