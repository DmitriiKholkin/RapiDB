import { Query, type QueryArrayConfig, type QueryArrayResult } from "pg";
import pgPackage from "pg/package.json";
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
  _accumulateRows: boolean | Completion;
  _checkForMultirow(): void;
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
  // Private hooks are not a pg compatibility promise. A dependency upgrade must
  // be reviewed against the protocol tests before extending this allowlist.
  if (pgPackage.version !== SUPPORTED_PG_VERSION)
    incompatible("unvalidated version");
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
  // _accumulateRows does not exist until RowDescription in the pinned version.
  // Probe that switch on a disposable Query, without a connection or SQL, so a
  // renamed/ignored switch (including pg-native) cannot silently buffer rows.
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

  constructor(
    text: string,
    values: unknown[],
    limit: number,
    complete: Completion,
  ) {
    super({ text, values, rowMode: "array" }, complete);
    assertStreamingCompatibility(this);
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
