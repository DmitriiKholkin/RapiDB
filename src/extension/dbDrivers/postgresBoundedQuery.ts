import { Query, type QueryArrayConfig, type QueryArrayResult } from "pg";
import { BoundedQueryRows } from "./boundedQueryRows";

type Result = QueryArrayResult<unknown[]>;
type Completion = (error: Error | null, result?: Result) => void;

// pg's public Query event API avoids row buffering only without a callback.
// Client.query_timeout installs a callback even for event consumers. This small
// adapter targets the pinned pg version: force event-only row handling and keep
// only the last command's metadata, rather than an ever-growing _results array.
// Protocol-level regression tests exercise the real pg Query implementation.
const StreamingQueryBase = Query as unknown as new (
  config: QueryArrayConfig,
  callback: Completion,
) => Query<unknown[]> & {
  _result: Result;
  _results: Result | Result[];
  _accumulateRows: boolean;
  _checkForMultirow(): void;
  handleRowDescription(message: unknown): void;
};

export class BoundedPostgresQuery extends StreamingQueryBase {
  readonly retained: BoundedQueryRows<unknown[]>;

  constructor(
    text: string,
    values: unknown[],
    limit: number,
    complete: Completion,
  ) {
    super({ text, values, rowMode: "array" }, complete);
    this.retained = new BoundedQueryRows(limit);
    this.on("row", (row) => this.retained.add(row));
  }

  override _checkForMultirow(): void {
    const previous = this._result;
    super._checkForMultirow();
    this._results = this._result;
    if (previous !== this._result) this.retained.reset();
  }

  override handleRowDescription(message: unknown): void {
    super.handleRowDescription(message);
    this._accumulateRows = false;
  }
}
