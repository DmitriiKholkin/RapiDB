import { createHash } from "node:crypto";
import type { ConnectionType } from "../../shared/connectionTypes";
import type { SqlDeleteObservation } from "../../shared/sqlDeleteObservation";
import { mssqlSqlVariantPrimaryKeyExpression } from "./mssqlSqlVariantIdentity";
import type { ColumnTypeMeta, IDBDriver } from "./types";

interface SqlDeleteSnapshot {
  readonly expressions: readonly string[];
  readonly shape: string;
  readonly valuePattern: RegExp;
  readonly oracleLobLock?: boolean;
}

export function isSqlDeleteBackend(type: ConnectionType | undefined): boolean {
  return (
    type === "pg" ||
    type === "sqlite" ||
    type === "mysql" ||
    type === "mssql" ||
    type === "oracle"
  );
}

/** Build the same server-side expression for the page read and the DELETE.
 * Never reconstruct evidence from the formatted grid, a second read, or a
 * subset of columns. Non-version snapshots include the key and NULLs.
 */
export function buildSqlDeleteSnapshot(
  backend: ConnectionType | undefined,
  driver: Pick<IDBDriver, "quoteIdentifier" | "buildTablePrimaryKeyExpr">,
  columns: readonly ColumnTypeMeta[],
): SqlDeleteSnapshot | undefined {
  if (!columns.some((column) => column.isPrimaryKey)) return undefined;
  const shape = createHash("sha256")
    .update(JSON.stringify([backend, columns]))
    .digest("hex");
  if (backend === "pg") {
    // xmin alone is not unique, e.g. a replacement written in the same
    // transaction. tableoid scopes ctid for partitions and inheritance.
    return {
      shape,
      expressions: [
        `(tableoid::pg_catalog.text || ':' || xmin::pg_catalog.text || ':' || ctid::pg_catalog.text) COLLATE pg_catalog."C"`,
      ],
      valuePattern: /^\d+:\d+:\(\d+,\d+\)$/,
    };
  }
  if (backend === "mssql") {
    const version = columns.find((column) =>
      ["rowversion", "timestamp"].includes(typeName(column)),
    );
    if (version && !version.nullable) {
      // Legacy WRITETEXT/UPDATETEXT do not advance rowversion. Guard the
      // contents of those columns as well, rather than trusting the version.
      const legacyLobs = columns.filter((column) =>
        ["text", "ntext", "image"].includes(typeName(column)),
      );
      return {
        shape,
        expressions: [
          `CONVERT(varchar(16), CONVERT(binary(8), ${driver.quoteIdentifier(version.name)}), 2)`,
          ...legacyLobs.map((column) =>
            mssqlLeaf(driver.quoteIdentifier(column.name), typeName(column)),
          ),
        ],
        valuePattern: /^(?:[0-9a-f]{16}|[0-9a-f]{64})$/i,
      };
    }
  }

  const combine = (left: string, right: string): string => {
    if (backend === "sqlite") return `RAPIDB_DELETE_HASH(${left} || ${right})`;
    return `SHA2(CONCAT(${left}, ${right}), 256)`;
  };
  const leaves: string[] = [];
  for (const column of columns) {
    const name = driver.quoteIdentifier(column.name);
    const type = typeName(column);
    const expression =
      backend === "sqlite"
        ? sqliteLeaf(name, driver.buildTablePrimaryKeyExpr?.(column))
        : backend === "mysql"
          ? mysqlLeaf(name, type, column)
          : backend === "mssql"
            ? mssqlLeaf(name, type)
            : backend === "oracle"
              ? oracleLeaf(name, type)
              : undefined;
    if (!expression) return undefined;
    leaves.push(expression);
  }
  return {
    shape,
    // Oracle and older SQL Server can treat NULL as empty when concatenating
    // strings. Never turn a missing column digest into a valid row digest.
    expressions:
      backend === "oracle" || backend === "mssql"
        ? leaves
        : [balanced(leaves, combine)],
    valuePattern: /^[0-9a-f]{64}$/i,
    ...(backend === "oracle" &&
    columns.some((column) => isOracleLob(typeName(column)))
      ? { oracleLobLock: true }
      : {}),
  };
}

const NULL_DIGEST = `'${"0".repeat(64)}'`;

function typeName(column: ColumnTypeMeta): string {
  return (column.baseType ?? column.nativeType)
    .replace(/\([^)]*\)/g, "")
    .trim()
    .toLowerCase();
}

function balanced(
  leaves: string[],
  combine: (left: string, right: string) => string,
): string {
  if (leaves.length === 1) return leaves[0];
  const mid = Math.floor(leaves.length / 2);
  return combine(
    balanced(leaves.slice(0, mid), combine),
    balanced(leaves.slice(mid), combine),
  );
}

function sqliteLeaf(
  name: string,
  expr: string | null | undefined,
): string | undefined {
  return expr
    ? `RAPIDB_DELETE_HASH(CASE WHEN typeof(${name}) = 'real' THEN ${name} ELSE COALESCE(${expr}, 'n:') END)`
    : undefined;
}

function mysqlLeaf(
  name: string,
  type: string,
  column: ColumnTypeMeta,
): string | undefined {
  let bytes: string;
  if (type === "timestamp") {
    bytes = `CAST(CAST(UNIX_TIMESTAMP(${name}) AS DECIMAL(20,6)) AS BINARY)`;
  } else if (column.category === "spatial") {
    bytes = `CONCAT(ST_SRID(${name}), ':', ST_AsWKB(${name}))`;
  } else if (["float", "double", "double precision", "real"].includes(type)) {
    // Widen FLOAT before server-side rendering, without the display precision
    // of a FLOAT field or a JS float round trip.
    bytes = `CAST((${name} * 1e0) AS BINARY)`;
  } else if (["enum", "set"].includes(type)) {
    bytes = `CAST((${name} + 0) AS BINARY)`;
  } else if (
    /^(?:tinyint|smallint|mediumint|int|integer|bigint|decimal|numeric|bit|year)(?: unsigned)?(?: zerofill)?$/.test(
      type,
    ) ||
    /^(?:var)?(?:char|binary)$|^(?:tiny|medium|long)?(?:text|blob)$/.test(
      type,
    ) ||
    ["date", "datetime", "time"].includes(type)
  ) {
    // CHAR padding is fixed by the declared type; BINARY preserves case,
    // accents, spaces, and trailing zero bytes for variable-width values.
    bytes = `CAST(${name} AS BINARY)`;
  } else {
    // In particular, MySQL's textual JSON can erase native JSON scalar types.
    return nullOnly(name);
  }
  return `CASE WHEN ${name} IS NULL THEN ${NULL_DIGEST} ELSE SHA2(${bytes}, 256) END`;
}

function mssqlHash(bytes: string): string {
  return `CONVERT(varchar(64), HASHBYTES('SHA2_256', ${bytes}), 2)`;
}

function mssqlLeaf(name: string, type: string): string {
  let bytes: string;
  switch (type) {
    case "sql_variant":
      bytes = `CONVERT(varbinary(max), ${mssqlSqlVariantPrimaryKeyExpression(name)})`;
      break;
    case "geometry":
    case "geography":
      bytes = `CONVERT(binary(4), ${name}.STSrid) + ${name}.AsBinaryZM()`;
      break;
    case "text":
      bytes = `CONVERT(varbinary(max), CONVERT(varchar(max), ${name}))`;
      break;
    case "ntext":
    case "xml":
      bytes = `CONVERT(varbinary(max), CONVERT(nvarchar(max), ${name}))`;
      break;
    default:
      if (
        !/^(?:bigint|binary|bit|char|date|datetime|datetime2|datetimeoffset|decimal|float|image|int|money|nchar|numeric|nvarchar|real|rowversion|smalldatetime|smallint|smallmoney|time|timestamp|tinyint|uniqueidentifier|varbinary|varchar)$/.test(
          type,
        )
      )
        return nullOnly(name);
      bytes = `CONVERT(varbinary(max), ${name})`;
  }
  return `CASE WHEN ${name} IS NULL THEN ${NULL_DIGEST} ELSE ${mssqlHash(bytes)} END`;
}

function oracleHash(value: string): string {
  return `RAWTOHEX(STANDARD_HASH(${value}, 'SHA256'))`;
}

function isOracleLob(type: string): boolean {
  return ["clob", "nclob", "blob"].includes(type);
}

function oracleLeaf(name: string, type: string): string | undefined {
  const onNull = `CASE WHEN ${name} IS NULL THEN ${NULL_DIGEST}`;
  let value: string;
  if (isOracleLob(type)) {
    // STANDARD_HASH cannot hash a LOB and DBMS_CRYPTO may require grants.
    // Only use SQL SUBSTR where the whole value fits even with multibyte LOBs.
    const length = `DBMS_LOB.GETLENGTH(${name})`;
    const limit = type === "blob" ? 2000 : 500;
    return `${onNull} WHEN ${length} = 0 THEN '${"1".repeat(64)}' WHEN ${length} <= ${limit} THEN ${oracleHash(`DBMS_LOB.SUBSTR(${name}, ${limit}, 1)`)} END`;
  }
  if (/^(?:n?char|n?varchar2|raw|rowid|urowid)$/.test(type)) {
    value = name;
  } else if (
    /^(?:number|float|binary_float|binary_double|date|timestamp(?: with (?:local )?time zone)?|interval (?:year to month|day to second))$/.test(
      type,
    )
  ) {
    // DUMP preserves native bytes: no loss of BC era, fractional precision,
    // timezone attributes, or exact numerics via node-oracledb conversion.
    value = `DUMP(${name}, 16)`;
  } else {
    return type === "long" || type === "long raw" ? undefined : nullOnly(name);
  }
  return `${onNull} ELSE ${oracleHash(value)} END`;
}

function nullOnly(name: string): string {
  return `CASE WHEN ${name} IS NULL THEN ${NULL_DIGEST} END`;
}

export function readSqlDeleteObservations(
  snapshot: SqlDeleteSnapshot,
  rows: readonly Record<string, unknown>[],
  index: number,
): Array<SqlDeleteObservation | null> {
  return rows.map((row) => {
    const values = snapshot.expressions.map(
      (_, offset) => row[`__col_${index + offset}`],
    );
    return values.every(
      (value): value is string =>
        typeof value === "string" && snapshot.valuePattern.test(value),
    )
      ? { shape: snapshot.shape, values }
      : null;
  });
}

export function sqlDeleteSnapshotComparison(
  backend: ConnectionType | undefined,
  expression: string,
  paramIndex: number,
): string {
  if (backend === "mysql")
    return `CAST((${expression}) AS BINARY) = CAST(? AS BINARY)`;
  const param =
    backend === "pg"
      ? `$${paramIndex}`
      : backend === "oracle"
        ? `:${paramIndex}`
        : "?";
  return `(${expression}) = ${param}`;
}
