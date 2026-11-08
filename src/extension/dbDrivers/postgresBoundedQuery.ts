import { Query, type QueryArrayConfig, type QueryArrayResult } from "pg";
import pgPackage from "pg/package.json";
import { BoundedQueryRows } from "./boundedQueryRows";

type Result = QueryArrayResult<unknown[]>;
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
  completed: boolean;
  partial?: boolean;
};

export const POSTGRES_MAX_RETAINED_RESULT_SETS = 100;
export const POSTGRES_MAX_RETAINED_RESULT_COLUMNS = 4096;

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
  private readonly resultSetByBuilder = new WeakMap<
    object,
    BoundedPgResultSet
  >();
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
  ) {
    super({ text, values, rowMode: "array" }, complete);
    assertStreamingCompatibility(this);
    this.retained = new BoundedQueryRows(limit);
    this.on("row", (row, result) => {
      const output = this.resultSetByBuilder.get(result as object);
      if (!output) return;
      output.rowCount++;
      if (this.retainedRowCount < this.retained.limit) {
        this.retained.add(row);
        this.retainedRowCount++;
      } else {
        this.retained.rowCount++;
        output.truncated = true;
      }
      output.rows = this.retained.rows;
    });
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
    super.handleRowDescription(message);
    const fields =
      (message as { fields?: BoundedPgResultField[] }).fields ?? [];
    this.seenResultSetCount++;
    this.activeResultSet = undefined;
    this.activeResultSetCompleted = false;
    if (
      this.seenResultSetCount > POSTGRES_MAX_RETAINED_RESULT_SETS ||
      this.retainedColumnCount + fields.length >
        POSTGRES_MAX_RETAINED_RESULT_COLUMNS
    ) {
      this.resultSetsTruncated = true;
      this._accumulateRows = false;
      return;
    }
    const output: BoundedPgResultSet = {
      fields: fields.map((field) => ({ ...field })),
      rows: this.retained.rows,
      rowCount: 0,
      truncated: false,
      completed: false,
    };
    this.resultSets.push(output);
    this.retainedColumnCount += fields.length;
    this.resultSetByBuilder.set(this._result, output);
    this.activeResultSet = output;
    this.activeResultSetCompleted = false;
    this._accumulateRows = false;
  }

  override handleCommandComplete(
    message: { text?: string },
    connection: object,
  ): void {
    super.handleCommandComplete(message, connection);
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
