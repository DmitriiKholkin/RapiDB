import type { TransactionOperation } from "./types";

/** Catalog visibility must be proven: an empty sys.triggers result without VIEW
 * DEFINITION is not evidence that the target has no triggers. USE is local to
 * this request's batch, so both permission checks and catalogs use the target DB.
 */
export function mssqlInsertGuard(
  target: NonNullable<
    TransactionOperation["captureIdentity"]
  >["mssqlInsertTarget"],
): { sql: string; params: [string, string, string] } {
  if (!target)
    throw new Error(
      "INSERT verification cannot inspect this MSSQL OUTPUT target. Recreate the insert plan before retrying.",
    );
  const quote = (name: string) => `[${name.replace(/]/g, "]]")}]`;
  const localName = `${quote(target.schema)}.${quote(target.table)}`;
  const qualified = target.database
    ? `${quote(target.database)}.${localName}`
    : localName;
  return {
    sql: `${target.database ? `USE ${quote(target.database)};\n` : ""}DECLARE @__rapidb_schema nvarchar(128) = @p1, @__rapidb_table nvarchar(128) = @p2, @__rapidb_name nvarchar(517) = @p3;
DECLARE @__rapidb_target int;
SELECT TOP (1) 1 AS __rapidb_lock FROM ${qualified} WITH (TABLOCKX, HOLDLOCK) OPTION (EXPAND VIEWS);
SET @__rapidb_target = (
  SELECT o.object_id FROM sys.objects o
  JOIN sys.schemas s ON s.schema_id = o.schema_id
  WHERE s.name = @__rapidb_schema AND o.name = @__rapidb_table AND o.type IN ('U', 'V')
);
IF @__rapidb_target IS NULL OR ISNULL(HAS_PERMS_BY_NAME(@__rapidb_name, 'OBJECT', 'VIEW DEFINITION'), 0) <> 1
  THROW 50001, 'INSERT verification cannot inspect trigger metadata. Grant VIEW DEFINITION on the target table/view or use an explicit SQL transaction.', 1;
IF EXISTS (
  SELECT 1 FROM sys.triggers t
  JOIN sys.trigger_events e ON e.object_id = t.object_id
  WHERE t.parent_id = @__rapidb_target AND t.parent_class = 1
    AND t.is_instead_of_trigger = 1 AND t.is_disabled = 0
    AND e.type_desc = 'INSERT'
)
  THROW 50001, 'INSERT verification does not support enabled INSTEAD OF INSERT triggers: OUTPUT can report rows that were never inserted. Use an explicit SQL transaction and verify the trigger effects, or disable/remove that trigger before using table inserts.', 1;
IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = @__rapidb_target AND type = 'V')
  THROW 50001, 'INSERT verification cannot reliably pin MSSQL view trigger metadata. Insert into the base table or use an explicit SQL transaction and verify the trigger effects.', 1;`,
    params: [target.schema, target.table, localName],
  };
}
