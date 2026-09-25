import type { FieldPacket, Query } from "mysql2";

/**
 * mysql2's callback-free command emits rows without retaining them, but still
 * appends an empty row array / OK packet and field metadata for every result.
 * At the fields event the current fields have already been read into local
 * parser state. Retain only that slot and rebase the next result's index.
 * This adapter is specific to the pinned mysql2 Query protocol implementation;
 * it must only be used on a command without a callback or other result consumers.
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
