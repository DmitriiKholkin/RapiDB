import type { ColumnTypeMeta } from "../dbDrivers/types";

/** The same representation is used by RETURNING and the final identity predicate.
 * Never round a key through a JS Date/number or an implicit NLS conversion. */
export function oracleInsertIdentity(
  column: ColumnTypeMeta,
  name: string,
): {
  expression: string;
  outType: "string" | "buffer";
} {
  const native = column.nativeType.toUpperCase().trim();
  let expression = name;
  if (native === "BINARY_FLOAT" || native === "BINARY_DOUBLE") {
    const converter =
      native === "BINARY_FLOAT"
        ? "CAST_FROM_BINARY_FLOAT"
        : "CAST_FROM_BINARY_DOUBLE";
    expression = `RAWTOHEX(UTL_RAW.${converter}(${name}))`;
  } else if (
    column.category === "integer" ||
    column.category === "decimal" ||
    column.category === "float"
  ) {
    expression = `TO_CHAR(${name}, 'TM9', 'NLS_NUMERIC_CHARACTERS=''.,''')`;
  } else if (native === "DATE") {
    expression = `TO_CHAR(${name}, 'SYYYY-MM-DD HH24:MI:SS', 'NLS_DATE_LANGUAGE=American')`;
  } else if (native.startsWith("TIMESTAMP")) {
    // Preserve timezone-independent instants at full 9-digit precision.
    const timestamp = native.includes("TIME ZONE")
      ? `SYS_EXTRACT_UTC(CAST(${name} AS TIMESTAMP(9) WITH TIME ZONE))`
      : name;
    expression = `TO_CHAR(${timestamp}, 'SYYYY-MM-DD HH24:MI:SS.FF9', 'NLS_DATE_LANGUAGE=American')`;
  } else if (column.category === "interval") {
    expression = `TO_CHAR(${name})`;
  } else if (native === "ROWID") {
    expression = `ROWIDTOCHAR(${name})`;
  } else if (native === "UROWID") {
    expression = `CAST(${name} AS VARCHAR2(4000))`;
  } else if (native === "BOOLEAN") {
    expression = `CASE WHEN ${name} THEN '1' WHEN NOT ${name} THEN '0' END`;
  } else if (column.category === "binary") {
    return { expression: name, outType: "buffer" };
  } else if (column.category !== "text" && column.category !== "uuid") {
    throw new Error(
      `INSERT verification cannot capture Oracle primary key type ${column.nativeType}; no data was written.`,
    );
  }
  return { expression, outType: "string" };
}

export function mssqlInsertIdentityExpression(
  column: ColumnTypeMeta,
  name: string,
): string {
  const native = column.nativeType.toLowerCase().trim();
  if (native === "hierarchyid") return `${name}.ToString()`;
  if (native === "sql_variant") {
    // Include type properties as well as value bytes: equal-looking values of
    // different variant types must not redirect verification to a different row.
    const properties = [
      "BaseType",
      "Precision",
      "Scale",
      "MaxLength",
      "Collation",
    ];
    return `CONCAT(${properties.map((property) => `CONVERT(nvarchar(128), SQL_VARIANT_PROPERTY(${name}, '${property}'))`).join(", ':', ")}, ':', CONVERT(varchar(max), CONVERT(varbinary(8000), ${name}), 2))`;
  }
  if (column.category === "float")
    return `CONVERT(varchar(32), CONVERT(varbinary(8), ${name}), 2)`;
  if (column.category === "decimal") return `CONVERT(varchar(100), ${name}, 2)`;
  if (
    column.category === "date" ||
    column.category === "time" ||
    column.category === "datetime"
  )
    return `CONVERT(nvarchar(64), ${name}, 127)`;
  return name;
}

/**
 * SQL Server's COLLATE grammar accepts a bare collation name, not a delimited
 * identifier. Keep the catalog collation on OUTPUT INTO columns to preserve
 * character encoding and comparison semantics, but only interpolate names
 * that are a single safe collation-name token.
 */
export function mssqlInsertIdentityCollation(
  collation: string | undefined,
  identityType: string,
): string {
  if (collation === undefined) return "";
  if (
    typeof collation !== "string" ||
    !/^[A-Za-z][A-Za-z0-9_]*$/.test(collation) ||
    collation.toLowerCase() === "database_default" ||
    !/^(?:n?varchar|n?char)\(\d+\)$/i.test(identityType)
  ) {
    throw new Error(
      "INSERT verification cannot capture MSSQL primary-key collation metadata; no data was written.",
    );
  }
  return ` COLLATE ${collation}`;
}

export function mssqlInsertIdentityType(column: ColumnTypeMeta): string {
  const native = column.nativeType.trim();
  const type = /^(?:timestamp|rowversion)$/i.test(native)
    ? "binary(8)"
    : /^sysname$/i.test(native)
      ? "nvarchar(128)"
      : native;
  // Aliases have already been resolved through sys.types. This grammar covers
  // SQL Server's indexable scalar built-ins, not arbitrary catalog text as SQL.
  // LOB/XML/spatial/MAX types cannot be PKs. Comparable hierarchyid/sql_variant can.
  if (
    !/^(?:bigint|int|integer|smallint|tinyint|bit|float(?:\(\d+\))?|real|money|smallmoney|uniqueidentifier|hierarchyid|sql_variant|date|datetime|smalldatetime|(?:datetime2|datetimeoffset|time)(?:\(\d+\))?|(?:decimal|numeric)(?:\(\d+(?:\s*,\s*\d+)?\))?|(?:n?varchar|n?char|varbinary|binary)(?:\(\d+\))?)$/i.test(
      type,
    )
  ) {
    throw new Error(
      `INSERT verification cannot capture MSSQL primary key type ${native}; no data was written.`,
    );
  }
  return type;
}
