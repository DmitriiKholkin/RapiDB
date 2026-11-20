export const SQLITE_ATTACHMENT_CHANGED =
  "The SQLite attachment target changed or was detached. Close and reopen the Table Viewer to work with the current attachment. Pending edits were not transferred.";

export interface SQLiteAttachmentTarget {
  /** An SQLite alias, normalized with SQLite's ASCII case folding. */
  schema: string;
  /** Opaque worker-local identity of this attachment, not its filename. */
  generation: string;
}

export function sqliteTableSchema(database: string, schema: string): string {
  return schema || database || "main";
}

export function sqliteAttachmentSchema(
  database: string,
  schema: string,
): string | undefined {
  const target = sqliteTableSchema(database, schema);
  return target.toLowerCase() === "main" ? undefined : target;
}
