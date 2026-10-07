import {
  Connection,
  type FieldPacket,
  type Connection as NativeMysqlConnection,
  type Query,
  type QueryOptions,
} from "mysql2";
import mysqlPackage from "mysql2/package.json";

const SUPPORTED_MYSQL_VERSION = "3.24.4";

/** Construct and validate a detached command: native.query() can send SQL immediately. */
export function createMysqlStreamingQuery(
  native: NativeMysqlConnection,
  options: QueryOptions,
): Query {
  const incompatible = (detail: string): never => {
    throw new Error(
      `[RapiDB] Cannot safely bound MySQL query results: incompatible mysql2 ${mysqlPackage.version} (${detail}). Expected mysql2 ${SUPPORTED_MYSQL_VERSION}; query was not submitted. No fully buffered fallback is allowed.`,
    );
  };
  // Private protocol state is not a compatibility promise. Review upgrades
  // against the installed-protocol tests before extending this allowlist.
  if (mysqlPackage.version !== SUPPORTED_MYSQL_VERSION)
    incompatible("unvalidated version");
  if (
    !native ||
    typeof native.query !== "function" ||
    !native.config ||
    typeof native.config !== "object"
  )
    incompatible("missing native connection query/config");
  if (typeof Connection.createQuery !== "function")
    incompatible("missing Connection.createQuery");
  // mysql2's declarations omit the options/config overload of this public factory.
  const createQuery = Connection.createQuery as unknown as (
    options: QueryOptions,
    values: undefined,
    callback: undefined,
    config: NativeMysqlConnection["config"],
  ) => Query;
  const command = createQuery(options, undefined, undefined, native.config);
  if (!command || typeof command !== "object")
    incompatible("unexpected callback-free Query shape");
  for (const method of [
    "execute",
    "start",
    "resultsetHeader",
    "readField",
    "fieldsEOF",
    "row",
    "doneInsert",
    "done",
    "on",
    "off",
  ]) {
    if (typeof Reflect.get(command, method) !== "function")
      incompatible(`missing Query.${method}`);
  }
  if (
    Reflect.get(command, "onResult") !== undefined ||
    Reflect.get(command, "next") !== null ||
    Reflect.get(command, "_resultIndex") !== 0 ||
    Reflect.get(command, "_currentRows") !== null ||
    Reflect.get(command, "_currentFields") !== null ||
    Reflect.get(command, "_rowParser") !== null ||
    Reflect.get(command, "_queryOptions")?.rowsAsArray !== true ||
    !Array.isArray(Reflect.get(command, "_rows")) ||
    Reflect.get(command, "_rows").length !== 0 ||
    !Array.isArray(Reflect.get(command, "_fields")) ||
    Reflect.get(command, "_fields").length !== 0
  )
    incompatible("unexpected callback-free Query shape");
  return command;
}

/**
 * mysql2's callback-free command emits rows without retaining them, but still
 * appends an empty row array / OK packet and field metadata for every result.
 * At the fields event readField holds the current fields in a local variable;
 * doneInsert likewise holds its OK packet locally before emitting result.
 * Retain only the field slot and rebase the next result's index.
 * This adapter is specific to the pinned mysql2 Query protocol implementation;
 * it must only be used on a validated command without a callback or other
 * result consumers.
 */
export function discardMysqlResultHistory(
  command: Query,
  fields: FieldPacket[] | undefined,
): void {
  const native = command as Query & {
    _rows: unknown[];
    _fields: (FieldPacket[] | undefined)[];
    _resultIndex: number;
  };
  native._rows.length = 0;
  native._fields.length = 0;
  native._fields.push(fields);
  native._resultIndex = 0;
}
