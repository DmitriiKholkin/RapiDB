import type { ConnectionConfig, ConnectionManager } from "./connectionManager";
import type { ColumnMeta } from "./dbDrivers/types";

export interface ColumnDef {
  name: string;
  type: string;
  nullable: boolean;
  isPrimaryKey: boolean;
  isForeignKey: boolean;
  defaultValue?: string;
  isBoolean?: boolean;
}

export interface Filter {
  column: string;
  value: string;
}

export interface SortConfig {
  column: string;
  direction: "asc" | "desc";
}

export interface TablePage {
  columns: ColumnDef[];
  rows: Record<string, unknown>[];
  totalCount: number;
}

export const ISO_DATETIME_RE =
  /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})?$/;

const DATE_ONLY_RE = /^\d{4}-\d{2}-\d{2}$/;

export const DATETIME_SQL_RE =
  /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d+)?([+-]\d{2}:\d{2})?$/;

const PG_GEOMETRIC_TYPES = new Set([
  "point",
  "line",
  "lseg",
  "box",
  "path",
  "polygon",
  "circle",
]);

const MYSQL_SPATIAL_TYPES = new Set([
  "point",
  "linestring",
  "polygon",
  "multipoint",
  "multilinestring",
  "multipolygon",
  "geometrycollection",
  "geometry",
]);

function isMysqlSpatialType(colType: string): boolean {
  return MYSQL_SPATIAL_TYPES.has(colType.toLowerCase().split("(")[0].trim());
}

function isOracleNonFilterable(colType: string): boolean {
  const ct = colType.toLowerCase().split("(")[0].trim();
  return (
    ct === "xmltype" ||
    ct === "blob" ||
    ct === "bfile" ||
    ct === "long raw" ||
    ct === "long" ||
    ct === "raw"
  );
}

function isMssqlNonFilterable(colType: string): boolean {
  const ct = colType.toLowerCase().split("(")[0].trim();
  return (
    ct === "image" ||
    ct === "binary" ||
    ct === "varbinary" ||
    ct === "geography" ||
    ct === "geometry"
  );
}

export function isBooleanLikeType(
  colType: string,
  dbType: ConnectionConfig["type"],
): boolean {
  const ct = colType.toLowerCase();
  const base = ct.split("(")[0].trim();
  if (dbType === "pg") return base === "boolean" || base === "bool";
  if (dbType === "mssql") return base === "bit";
  if (dbType === "mysql") {
    if (base === "bool" || base === "boolean") return true;
    if (base === "tinyint" && ct.includes("(1)")) return true;
    if (base === "bit" && ct.includes("(1)")) return true;
    return false;
  }
  if (dbType === "sqlite") return base === "boolean" || base === "bool";
  return false;
}

function parseMysqlSpatialToWkt(val: string): string {
  const trimmed = val.trim();

  if (
    /^(POINT|LINESTRING|POLYGON|MULTI\w+|GEOMETRYCOLLECTION|GEOMETRY)\s*\(/i.test(
      trimmed,
    )
  ) {
    return trimmed;
  }

  let parsed: unknown;
  try {
    parsed = JSON.parse(trimmed);
  } catch {
    throw new Error(
      "Cannot parse spatial value. Use WKT, e.g. POINT(1 2) or POLYGON((0 0, 1 0, 1 1, 0 0)).",
    );
  }

  try {
    return mysqlSpatialJsonToWkt(parsed);
  } catch (e: any) {
    throw new Error(
      `Cannot convert spatial value to WKT: ${e?.message ?? String(e)}. ` +
        "Use WKT, e.g. POINT(1 2) or POLYGON((0 0, 1 0, 1 1, 0 0)).",
    );
  }
}

function mysqlSpatialJsonToWkt(obj: unknown): string {
  if (obj === null || typeof obj !== "object") {
    throw new Error("Invalid spatial JSON");
  }

  if (!Array.isArray(obj)) {
    const o = obj as Record<string, unknown>;
    if (typeof o.x === "number" && typeof o.y === "number") {
      return `POINT(${o.x} ${o.y})`;
    }
    throw new Error("Unknown spatial object format");
  }

  const arr = obj as unknown[];
  if (arr.length === 0) {
    throw new Error("Empty spatial array");
  }

  const first = arr[0];

  if (
    first !== null &&
    typeof first === "object" &&
    !Array.isArray(first) &&
    typeof (first as any).x === "number"
  ) {
    const pts = (arr as Array<{ x: number; y: number }>)
      .map((p) => `${p.x} ${p.y}`)
      .join(", ");
    return `LINESTRING(${pts})`;
  }

  if (Array.isArray(first)) {
    const rings = (arr as Array<Array<{ x: number; y: number }>>)
      .map((ring) => `(${ring.map((p) => `${p.x} ${p.y}`).join(", ")})`)
      .join(", ");
    return `POLYGON(${rings})`;
  }

  throw new Error("Unrecognised spatial JSON structure");
}

function isDatetimeWithTime(
  colType: string,
  dbType: ConnectionConfig["type"],
): boolean {
  const ct = colType.toLowerCase();
  if (dbType === "pg") {
    return (
      ct.startsWith("timestamp") ||
      ct === "timetz" ||
      ct === "time with time zone" ||
      ct === "time"
    );
  }
  if (dbType === "mysql") {
    return ct === "datetime" || ct === "timestamp";
  }
  if (dbType === "mssql") {
    return (
      ct === "datetime" ||
      ct === "datetime2" ||
      ct === "smalldatetime" ||
      ct === "datetimeoffset" ||
      ct === "time" ||
      ct.startsWith("datetime")
    );
  }
  if (dbType === "oracle") {
    return ct === "date" || ct.startsWith("timestamp");
  }
  if (dbType === "sqlite") {
    return (
      ct === "datetime" ||
      ct === "timestamp" ||
      ct.startsWith("datetime") ||
      ct.startsWith("timestamp")
    );
  }
  return false;
}

export function formatDatetimeForDisplay(val: unknown): string | null {
  if (val instanceof Date) {
    if (isNaN(val.getTime())) {
      return null;
    }
    const pad = (n: number) => String(n).padStart(2, "0");
    const ms = val.getUTCMilliseconds();
    const frac = ms > 0 ? `.${String(ms).padStart(3, "0")}` : "";
    return (
      `${val.getUTCFullYear()}-${pad(val.getUTCMonth() + 1)}-${pad(val.getUTCDate())} ` +
      `${pad(val.getUTCHours())}:${pad(val.getUTCMinutes())}:${pad(val.getUTCSeconds())}${frac}`
    );
  }

  if (typeof val === "string") {
    const m =
      /^(\d{4}-\d{2}-\d{2}) (\d{2}:\d{2}:\d{2})(\.\d+)?([+-]\d{2}(:\d{2})?|Z)?$/.exec(
        val,
      );
    if (m) {
      const [, date, time, rawFrac, tz] = m;
      let fracStr = "";
      if (rawFrac && rawFrac.length > 1) {
        const digits = rawFrac.slice(1).slice(0, 3).padEnd(3, "0");
        const msNum = parseInt(digits, 10);
        if (msNum > 0) {
          fracStr = `.${String(msNum).padStart(3, "0").replace(/0+$/, "")}`;
        }
      }
      return `${date} ${time}${fracStr}${tz ?? ""}`;
    }
  }

  return null;
}

function isoToLocalDateStr(iso: string): string | null {
  const d = new Date(iso);
  if (isNaN(d.getTime())) {
    return null;
  }
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, "0");
  const day = String(d.getUTCDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

function normalizeCidrValue(val: string): string {
  const m = val
    .trim()
    .match(/^(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})\/(\d{1,2})$/);
  if (!m) return val;
  const prefix = parseInt(m[2], 10);
  if (prefix < 0 || prefix > 32) return val;
  const parts = m[1].split(".").map(Number);
  if (parts.some((p) => p < 0 || p > 255)) return val;
  const mask =
    prefix === 0 ? 0 : ((0xffffffff << (32 - prefix)) & 0xffffffff) >>> 0;
  const ipNum =
    (((parts[0] << 24) | (parts[1] << 16) | (parts[2] << 8) | parts[3]) >>> 0) &
    mask;
  const masked = [
    (ipNum >>> 24) & 0xff,
    (ipNum >>> 16) & 0xff,
    (ipNum >>> 8) & 0xff,
    ipNum & 0xff,
  ].join(".");
  return `${masked}/${prefix}`;
}

function jsonToPgCircle(val: string): string {
  const trimmed = val.trim();
  if (!trimmed.startsWith("{")) return trimmed;
  try {
    const obj = JSON.parse(trimmed) as Record<string, unknown>;
    if (
      typeof obj.x === "number" &&
      typeof obj.y === "number" &&
      typeof obj.radius === "number"
    ) {
      return `<(${obj.x},${obj.y}),${obj.radius}>`;
    }
  } catch {}
  return trimmed;
}

function coerceValue(
  value: unknown,
  dbType: ConnectionConfig["type"],
  colType?: string,
): unknown {
  if (value === null || value === undefined || value === "") {
    return value;
  }
  if (typeof value !== "string") {
    return value;
  }

  if (colType && isBooleanLikeType(colType, dbType)) {
    const lower = value.toLowerCase();
    if (lower === "true" || lower === "1") {
      return dbType === "pg" || dbType === "mssql" ? true : 1;
    }
    if (lower === "false" || lower === "0") {
      return dbType === "pg" || dbType === "mssql" ? false : 0;
    }
  }

  if (
    dbType === "mysql" &&
    colType &&
    colType.toLowerCase().startsWith("bit")
  ) {
    const n = parseInt(value, 10);
    if (!isNaN(n)) return n;
  }

  if (
    dbType === "pg" &&
    colType &&
    (colType.endsWith("[]") || colType === "ARRAY" || colType.startsWith("_"))
  ) {
    if (value.startsWith("[") && value.endsWith("]")) {
      try {
        const parsed = JSON.parse(value);
        if (Array.isArray(parsed)) {
          return parsed;
        }
      } catch {}
    }
  }

  if (dbType === "pg" && colType && colType.startsWith("interval")) {
    if (value.startsWith("{")) {
      try {
        const obj = JSON.parse(value) as Record<string, number>;
        let iso = "P";
        if (obj.years) iso += `${obj.years}Y`;
        if (obj.months) iso += `${obj.months}M`;
        if (obj.days) iso += `${obj.days}D`;
        const hasTime = obj.hours || obj.minutes || obj.seconds;
        if (hasTime) {
          iso += "T";
          if (obj.hours) iso += `${obj.hours}H`;
          if (obj.minutes) iso += `${obj.minutes}M`;
          if (obj.seconds) iso += `${obj.seconds}S`;
        }
        if (iso === "P") iso = "P0D";
        return iso;
      } catch {}
    }

    return value;
  }

  if (ISO_DATETIME_RE.test(value)) {
    if (dbType === "pg" && colType && colType === "date") {
      return isoToLocalDateStr(value) ?? value;
    }

    if (dbType === "oracle" && colType) {
      const ct = colType.toLowerCase();
      if (ct === "date" || ct.startsWith("timestamp")) {
        const sqlStr = value
          .replace("T", " ")
          .replace(/Z$/, "")
          .replace(/([+-]\d{2}):(\d{2})$/, " $1:$2");
        return sqlStr;
      }
    }

    if (dbType === "mysql" || dbType === "mssql") {
      const d = new Date(value);
      if (!isNaN(d.getTime())) {
        const pad = (n: number) => String(n).padStart(2, "0");
        const ms = d.getUTCMilliseconds();
        const fracSec = ms > 0 ? `.${String(ms).padStart(3, "0")}` : "";
        return (
          `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())} ` +
          `${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}:${pad(d.getUTCSeconds())}${fracSec}`
        );
      }
    }

    return value;
  }

  if (DATE_ONLY_RE.test(value) && dbType === "oracle" && colType) {
    const ct = colType.toLowerCase();
    if (ct === "date" || ct.startsWith("timestamp")) {
      return `${value} 00:00:00`;
    }
  }

  if (DATETIME_SQL_RE.test(value) && colType) {
    const ct = colType.toLowerCase();
    if (dbType === "oracle" && (ct === "date" || ct.startsWith("timestamp"))) {
      return value;
    }
    if (dbType === "mysql" || dbType === "mssql") {
      return value;
    }
    return value;
  }

  if (dbType === "pg" && colType) {
    const ct = colType.toLowerCase().split("(")[0].trim();
    if (ct === "cidr") {
      return normalizeCidrValue(value);
    }
    if (ct === "circle") {
      return jsonToPgCircle(value);
    }
  }

  return value;
}

function isNumericCompareUnsafe(
  colType: string,
  dbType: ConnectionConfig["type"],
): boolean {
  const ct = colType.toLowerCase().split("(")[0].trim();

  if (dbType === "pg") {
    return (
      ct === "date" ||
      ct === "time" ||
      ct === "timetz" ||
      ct === "boolean" ||
      ct === "bool" ||
      ct === "uuid" ||
      ct === "inet" ||
      ct === "cidr" ||
      ct === "macaddr" ||
      ct === "macaddr8" ||
      ct === "bit" ||
      ct === "varbit" ||
      ct === "oid" ||
      ct === "xid" ||
      ct === "cid" ||
      colType.toLowerCase().endsWith("[]") ||
      colType.toLowerCase().startsWith("_") ||
      colType.toLowerCase() === "array"
    );
  }

  if (dbType === "mssql") {
    return (
      ct === "bit" ||
      ct === "date" ||
      ct === "time" ||
      ct === "datetime" ||
      ct === "datetime2" ||
      ct === "smalldatetime" ||
      ct === "datetimeoffset"
    );
  }

  if (dbType === "mysql") {
    return (
      ct === "date" || ct === "datetime" || ct === "timestamp" || ct === "year"
    );
  }

  if (dbType === "oracle") {
    return ct === "date" || ct.startsWith("timestamp");
  }

  return false;
}

function buildEffectiveOrderBy(
  sort: SortConfig | null,
  cols: ColumnDef[],
  type: ConnectionConfig["type"],
): string {
  if (sort) {
    const col = quoteId(sort.column, type);
    const dir = sort.direction === "desc" ? "DESC" : "ASC";
    return `ORDER BY ${col} ${dir}`;
  }
  const pkCols = cols.filter((c) => c.isPrimaryKey);
  if (pkCols.length === 0) {
    return "";
  }
  return `ORDER BY ${pkCols.map((c) => quoteId(c.name, type)).join(", ")}`;
}

function coerceRecord(
  record: Record<string, unknown>,
  dbType: ConnectionConfig["type"],
  cols?: ColumnDef[],
): Record<string, unknown> {
  const colMap = new Map(cols?.map((c) => [c.name, c.type]) ?? []);
  return Object.fromEntries(
    Object.entries(record).map(([k, v]) => [
      k,
      coerceValue(v, dbType, colMap.get(k)),
    ]),
  );
}

function quoteId(name: string, type: ConnectionConfig["type"]): string {
  switch (type) {
    case "mysql":
      return `\`${name.replace(/`/g, "``")}\``;
    case "mssql":
      return `[${name.replace(/]/g, "]]")}]`;
    default:
      return `"${name.replace(/"/g, '""')}"`;
  }
}

function qualifiedTable(
  database: string,
  schema: string,
  table: string,
  type: ConnectionConfig["type"],
): string {
  const q = (n: string) => quoteId(n, type);
  if (type === "mysql") {
    return database ? `${q(database)}.${q(table)}` : q(table);
  }
  if (type === "sqlite") {
    return q(table);
  }
  if (type === "oracle") {
    return schema
      ? `${q(schema.toUpperCase())}.${q(table.toUpperCase())}`
      : q(table.toUpperCase());
  }

  const parts: string[] = [];
  if (database && type === "mssql") {
    parts.push(q(database));
  }
  if (schema) {
    parts.push(q(schema));
  }
  parts.push(q(table));
  return parts.join(".");
}

const NULL_SENTINEL = "\x00__NULL__\x00";

function buildWhere(
  filters: Filter[],
  type: ConnectionConfig["type"],
  cols: ColumnDef[],
  startIndex = 1,
): { clause: string; params: unknown[] } {
  if (filters.length === 0) {
    return { clause: "", params: [] };
  }

  const params: unknown[] = [];
  const conditions = filters
    .filter((f) => f.value.trim() !== "")
    .map((f): string | null => {
      const col = quoteId(f.column, type);
      const val = f.value.trim();

      if (val === NULL_SENTINEL) {
        return `${col} IS NULL`;
      }

      const colDef = cols.find((c) => c.name === f.column);
      const colType = colDef?.type.toLowerCase() ?? "";

      if (type === "oracle" && isOracleNonFilterable(colType)) {
        return null;
      }
      if (type === "mssql" && isMssqlNonFilterable(colType)) {
        return null;
      }

      const isDateCol = colType === "date";
      const isTextual =
        colType.includes("json") ||
        colType.includes("text") ||
        colType.includes("char") ||
        colType.includes("clob") ||
        colType.includes("string") ||
        colType === "xml" ||
        colType.startsWith("enum") ||
        colType.startsWith("set(") ||
        colType.includes("blob") ||
        colType === "uniqueidentifier";
      const isPgGeometric =
        type === "pg" && PG_GEOMETRIC_TYPES.has(colType.split("(")[0].trim());

      if (type === "pg" && isDateCol) {
        let dateVal: string | null = null;
        if (DATE_ONLY_RE.test(val)) {
          dateVal = val;
        } else if (ISO_DATETIME_RE.test(val)) {
          dateVal = isoToLocalDateStr(val);
        }
        if (dateVal) {
          params.push(dateVal);
          return `${col} = $${startIndex + params.length - 1}::date`;
        }
      }

      if (
        type === "pg" &&
        (colType.startsWith("timestamp") ||
          colType === "timetz" ||
          colType === "time with time zone")
      ) {
        params.push(`${val}%`);
        return `CAST(${col} AS TEXT) ILIKE $${startIndex + params.length - 1}`;
      }

      if (type === "mysql" && isMysqlSpatialType(colType)) {
        params.push(`%${val}%`);
        return `ST_AsText(${col}) LIKE ?`;
      }

      if (colDef && isBooleanLikeType(colDef.type, type)) {
        const lower = val.toLowerCase();
        if (lower === "true" || lower === "false") {
          const isTrue = lower === "true";
          if (type === "pg") {
            params.push(isTrue);
            return `${col} = $${startIndex + params.length - 1}`;
          }
          params.push(isTrue ? 1 : 0);
          return `${col} = ?`;
        }
      }

      const numericUnsafe = isNumericCompareUnsafe(colType, type);

      if (
        !isTextual &&
        !isPgGeometric &&
        !numericUnsafe &&
        !isNaN(Number(val)) &&
        val !== ""
      ) {
        params.push(val);
        if (type === "pg") {
          return `${col} = $${startIndex + params.length - 1}`;
        }
        return `${col} = ?`;
      }

      let finalVal = val;
      if (ISO_DATETIME_RE.test(val)) {
        finalVal = val
          .replace(/(\.\d*?[1-9])0+(?=[Z+-]|$)/, "$1")
          .replace(/\.0+(?=[Z+-]|$)/, "")
          .replace("T", "%")
          .replace("Z", "%")
          .replace(/[+-]\d{2}:\d{2}$/, "%");
      } else if (DATETIME_SQL_RE.test(val)) {
        finalVal = val;
      }

      if (type === "pg") {
        params.push(`%${finalVal}%`);
        return `CAST(${col} AS TEXT) ILIKE $${startIndex + params.length - 1}`;
      }
      if (type === "oracle") {
        const isTs = colType.startsWith("timestamp");
        const isOraDateTime = colType === "date" || isTs;

        if (isOraDateTime) {
          if (colType === "date") {
            if (DATE_ONLY_RE.test(val)) {
              const nextDay = (() => {
                const [y, mo, d] = val.split("-").map(Number);
                const dt = new Date(Date.UTC(y, mo - 1, d + 1));
                return `${dt.getUTCFullYear()}-${String(dt.getUTCMonth() + 1).padStart(2, "0")}-${String(dt.getUTCDate()).padStart(2, "0")}`;
              })();
              params.push(val, nextDay);
              return `(${col} >= TO_DATE(?, 'YYYY-MM-DD') AND ${col} < TO_DATE(?, 'YYYY-MM-DD'))`;
            } else if (ISO_DATETIME_RE.test(val)) {
              const d = new Date(val);
              if (!isNaN(d.getTime())) {
                params.push(d);
                return `${col} = ?`;
              }
            } else if (DATETIME_SQL_RE.test(val)) {
              const wallClock = val.replace(" ", "T");
              const d = new Date(wallClock);
              if (!isNaN(d.getTime())) {
                params.push(d);
                return `${col} = ?`;
              }
            }
          } else {
            const isTzAware =
              colType.includes("with time zone") ||
              colType.includes("with local time zone");

            let toCharExpr: string;
            if (isTzAware) {
              toCharExpr = `TO_CHAR(${col} AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.FF3')`;
            } else {
              toCharExpr = `TO_CHAR(${col}, 'YYYY-MM-DD HH24:MI:SS.FF3')`;
            }

            const bare = val
              .replace(/Z$/i, "")
              .replace(/[+-]\d{2}:\d{2}$/, "")
              .replace("T", " ");
            params.push(`${bare}%`);
            return `${toCharExpr} LIKE ?`;
          }
        }

        params.push(`%${finalVal.toUpperCase()}%`);
        if (colType.includes("clob")) {
          return `UPPER(DBMS_LOB.SUBSTR(${col}, 4000, 1)) LIKE ?`;
        }
        return `UPPER(CAST(${col} AS VARCHAR2(4000))) LIKE ?`;
      }
      if (type === "mssql") {
        if (isDateCol) {
          let dateStr: string | null = null;
          if (DATE_ONLY_RE.test(val)) {
            dateStr = val;
          } else if (ISO_DATETIME_RE.test(val)) {
            dateStr = isoToLocalDateStr(val);
          }
          if (dateStr) {
            params.push(dateStr);
            return `CONVERT(NVARCHAR(10), ${col}, 23) = ?`;
          }
        }
        if (isDatetimeWithTime(colType, "mssql")) {
          const mssqlDtVal = DATETIME_SQL_RE.test(finalVal)
            ? `${finalVal}%`
            : `%${finalVal}%`;
          params.push(mssqlDtVal);
          return `CONVERT(NVARCHAR(23), ${col}, 121) LIKE ?`;
        }
        const mssqlVal = DATETIME_SQL_RE.test(finalVal)
          ? `${finalVal}%`
          : `%${finalVal}%`;
        params.push(mssqlVal);
        return `CAST(${col} AS NVARCHAR(MAX)) LIKE ?`;
      }
      const mysqlVal = DATETIME_SQL_RE.test(finalVal)
        ? `${finalVal}%`
        : `%${finalVal}%`;
      params.push(mysqlVal);
      return `CAST(${col} AS CHAR) LIKE ?`;
    })
    .filter((c): c is string => c !== null);

  if (conditions.length === 0) {
    return { clause: "", params: [] };
  }
  return { clause: `WHERE ${conditions.join(" AND ")}`, params };
}

export class TableDataService {
  constructor(private readonly cm: ConnectionManager) {}

  private readonly _colCache = new Map<string, ColumnDef[]>();

  private colCacheKey(
    connectionId: string,
    database: string,
    schema: string,
    table: string,
  ): string {
    return `${connectionId}::${database}::${schema}::${table}`;
  }

  clearForConnection(connectionId: string): void {
    for (const key of this._colCache.keys()) {
      if (key.startsWith(`${connectionId}::`)) {
        this._colCache.delete(key);
      }
    }
  }

  private conn(id: string) {
    const cfg = this.cm.getConnection(id);
    const drv = this.cm.getDriver(id);
    if (!cfg || !drv) {
      throw new Error(`[RapiDB] Not connected: ${id}`);
    }
    return { cfg, drv };
  }

  async getColumns(
    connectionId: string,
    database: string,
    schema: string,
    table: string,
  ): Promise<ColumnDef[]> {
    const key = this.colCacheKey(connectionId, database, schema, table);
    const cached = this._colCache.get(key);
    if (cached) {
      return cached;
    }

    const { cfg, drv } = this.conn(connectionId);
    const cols = await drv.describeTable(database, schema, table);
    const result = cols.map((c: ColumnMeta) => ({
      name: c.name,
      type: c.type,
      nullable: c.nullable,
      isPrimaryKey: c.isPrimaryKey,
      isForeignKey: c.isForeignKey,
      defaultValue: c.defaultValue,
      isBoolean: isBooleanLikeType(c.type, cfg.type),
    }));
    this._colCache.set(key, result);
    return result;
  }

  async getPage(
    connectionId: string,
    database: string,
    schema: string,
    table: string,
    page: number,
    pageSize: number,
    filters: Filter[],
    sort: SortConfig | null = null,
  ): Promise<TablePage> {
    const { cfg, drv } = this.conn(connectionId);
    const t = cfg.type;
    const qt = qualifiedTable(database, schema, table, t);
    const cols = await this.getColumns(connectionId, database, schema, table);

    const { clause: where, params: whereParams } = buildWhere(
      filters,
      t,
      cols,
      1,
    );
    const offset = (page - 1) * pageSize;
    const effectiveOrderBy = buildEffectiveOrderBy(sort, cols, t);

    let totalCount = 0;
    try {
      const countSql = `SELECT COUNT(*) AS cnt FROM ${qt} ${where}`;
      const countRes = await drv.query(countSql, whereParams);

      const countRow = countRes.rows[0] as Record<string, unknown> | undefined;

      totalCount = Number(
        countRow?.__col_0 ??
          countRow?.cnt ??
          countRow?.CNT ??
          countRow?.count ??
          0,
      );
    } catch {}

    let dataSql: string;
    let dataParams: unknown[];

    if (t === "mssql") {
      const orderByMssql = effectiveOrderBy || "ORDER BY (SELECT NULL)";
      dataSql = `SELECT * FROM ${qt} ${where} ${orderByMssql} OFFSET ? ROWS FETCH NEXT ? ROWS ONLY`;
      dataParams = [...whereParams, offset, pageSize];
    } else if (t === "oracle") {
      const orderByOracle = effectiveOrderBy || "ORDER BY ROWNUM";
      dataSql = `SELECT * FROM ${qt} ${where} ${orderByOracle} OFFSET ? ROWS FETCH NEXT ? ROWS ONLY`;
      dataParams = [...whereParams, offset, pageSize];
    } else if (t === "pg") {
      const base = whereParams.length;
      dataSql = `SELECT * FROM ${qt} ${where} ${effectiveOrderBy} LIMIT $${base + 1} OFFSET $${base + 2}`;
      dataParams = [...whereParams, pageSize, offset];
    } else {
      dataSql = `SELECT * FROM ${qt} ${where} ${effectiveOrderBy} LIMIT ? OFFSET ?`;
      dataParams = [...whereParams, pageSize, offset];
    }

    const dataRes = await drv.query(dataSql, dataParams);
    const dataColumns = dataRes.columns;

    const colTypeMap = new Map(cols.map((c) => [c.name, c.type]));

    const formattedRows = dataRes.rows.map((row) => {
      const newRow: Record<string, unknown> = {};
      dataColumns.forEach((colName, i) => {
        const val = row[`__col_${i}`];

        if (Buffer.isBuffer(val)) {
          newRow[colName] =
            val.length === 0
              ? 0
              : val.length <= 6
                ? val.readUIntBE(0, val.length)
                : val.toString("hex");
          return;
        }

        if (val !== null && typeof val === "object" && !(val instanceof Date)) {
          if (
            t === "pg" &&
            "x" in (val as object) &&
            "y" in (val as object) &&
            Object.keys(val as object).length === 2
          ) {
            newRow[colName] = `(${(val as any).x}, ${(val as any).y})`;
          } else {
            newRow[colName] = JSON.stringify(val);
          }
        } else {
          const colType = colTypeMap.get(colName) ?? "";
          if (isDatetimeWithTime(colType, t)) {
            const formatted = formatDatetimeForDisplay(val);
            newRow[colName] = formatted !== null ? formatted : val;
          } else {
            newRow[colName] = val;
          }
        }
      });
      return newRow;
    });

    return {
      columns: cols,
      rows: formattedRows,
      totalCount,
    };
  }

  async updateRow(
    connectionId: string,
    database: string,
    schema: string,
    table: string,
    pkValues: Record<string, unknown>,
    changes: Record<string, unknown>,
  ): Promise<void> {
    const { cfg, drv } = this.conn(connectionId);

    const colsDef = await this.getColumns(
      connectionId,
      database,
      schema,
      table,
    );
    const op = buildUpdateRowSql(
      cfg.type,
      database,
      schema,
      table,
      pkValues,
      changes,
      colsDef,
    );
    if (!op) {
      return;
    }

    const result = await drv.query(op.sql, op.params);
    const affectedRows = result.affectedRows ?? result.rowCount;
    if (affectedRows === 0) {
      throw new Error(
        "Row not found — the row may have been modified or deleted by another user",
      );
    }
  }

  async insertRow(
    connectionId: string,
    database: string,
    schema: string,
    table: string,
    values: Record<string, unknown>,
  ): Promise<void> {
    const { cfg, drv } = this.conn(connectionId);
    const t = cfg.type;
    const qt = qualifiedTable(database, schema, table, t);
    const colsDef = await this.getColumns(
      connectionId,
      database,
      schema,
      table,
    );
    values = coerceRecord(values, t, colsDef);

    const cols = Object.keys(values).filter(
      (k) => values[k] !== undefined && values[k] !== "",
    );
    if (cols.length === 0) {
      throw new Error(
        "Insert failed: no values provided. " +
          "Fill in at least one field or explicitly set a field to NULL.",
      );
    }
    const params = cols.map((c) =>
      values[c] === NULL_SENTINEL ? null : values[c],
    );

    let result;
    if (t === "pg") {
      const colList = cols.map((c) => quoteId(c, t)).join(", ");
      const valList = cols.map((_, i) => `$${i + 1}`).join(", ");
      result = await drv.query(
        `INSERT INTO ${qt} (${colList}) VALUES (${valList})`,
        params,
      );
    } else {
      const colList = cols.map((c) => quoteId(c, t)).join(", ");
      const colTypeMap = new Map(colsDef.map((c) => [c.name, c.type]));
      const valList = cols
        .map((c, idx) => {
          if (t === "mysql" && isMysqlSpatialType(colTypeMap.get(c) ?? "")) {
            const raw = params[idx];
            const wkt =
              typeof raw === "string"
                ? (parseMysqlSpatialToWkt(raw) ?? raw)
                : String(raw ?? "");
            params[idx] = wkt;
            return "ST_GeomFromText(?)";
          }
          return "?";
        })
        .join(", ");
      result = await drv.query(
        `INSERT INTO ${qt} (${colList}) VALUES (${valList})`,
        params,
      );
    }

    const affected = result.affectedRows ?? result.rowCount;
    if (affected !== undefined && affected === 0) {
      throw new Error(
        "Insert failed: the database reported 0 rows affected. " +
          "The row may have been rejected by a trigger or constraint.",
      );
    }
  }

  async deleteRows(
    connectionId: string,
    database: string,
    schema: string,
    table: string,
    pkValuesList: Record<string, unknown>[],
  ): Promise<void> {
    if (pkValuesList.length === 0) {
      return;
    }
    const { cfg, drv } = this.conn(connectionId);
    const t = cfg.type;
    const qt = qualifiedTable(database, schema, table, t);
    const colsDef = await this.getColumns(
      connectionId,
      database,
      schema,
      table,
    );

    for (const rawPkValues of pkValuesList) {
      const pkValues = coerceRecord(rawPkValues, t, colsDef);
      const pkCols = Object.keys(pkValues);
      if (pkCols.length === 0) {
        continue;
      }
      const params: unknown[] = [];
      let whereParts: string[];

      if (t === "pg") {
        whereParts = pkCols.map((c, i) => {
          params.push(pkValues[c]);
          return `${quoteId(c, t)} = $${i + 1}`;
        });
      } else {
        whereParts = pkCols.map((c) => {
          params.push(pkValues[c]);
          return `${quoteId(c, t)} = ?`;
        });
      }

      const result = await drv.query(
        `DELETE FROM ${qt} WHERE ${whereParts.join(" AND ")}`,
        params,
      );
      if (result.affectedRows === 0) {
        throw new Error(
          "Row not found — the row may have been modified or deleted by another user",
        );
      }
    }
  }

  async *exportAll(
    connectionId: string,
    database: string,
    schema: string,
    table: string,
    chunkSize = 500,
    sort: SortConfig | null = null,
    filters: Filter[] = [],
  ): AsyncGenerator<{ columns: ColumnDef[]; rows: Record<string, unknown>[] }> {
    let page = 1;
    while (true) {
      const result = await this.getPage(
        connectionId,
        database,
        schema,
        table,
        page,
        chunkSize,
        filters,
        sort,
      );
      if (result.rows.length === 0) {
        break;
      }
      yield { columns: result.columns, rows: result.rows };
      if (result.rows.length < chunkSize) {
        break;
      }
      page++;
    }
  }
}

function buildUpdateRowSql(
  type: ConnectionConfig["type"],
  database: string,
  schema: string,
  table: string,
  pkValues: Record<string, unknown>,
  changes: Record<string, unknown>,
  cols?: ColumnDef[],
): { sql: string; params: unknown[] } | null {
  const t = type;
  const qt = qualifiedTable(database, schema, table, t);
  changes = coerceRecord(changes, t, cols);
  pkValues = coerceRecord(pkValues, t, cols);

  const setCols = Object.keys(changes);
  const pkCols = Object.keys(pkValues);
  if (setCols.length === 0 || pkCols.length === 0) {
    return null;
  }

  const params: unknown[] = [];

  if (t === "pg") {
    const setParts = setCols.map((c, i) => {
      params.push(changes[c]);
      return `${quoteId(c, t)} = $${i + 1}`;
    });
    const whereParts = pkCols.map((c, i) => {
      params.push(pkValues[c]);
      return `${quoteId(c, t)} = $${setCols.length + i + 1}`;
    });
    return {
      sql: `UPDATE ${qt} SET ${setParts.join(", ")} WHERE ${whereParts.join(" AND ")}`,
      params,
    };
  } else {
    const colMap =
      cols && cols.length > 0
        ? new Map(cols.map((c) => [c.name, c.type]))
        : null;
    const setParts = setCols.map((c) => {
      if (t === "mysql" && isMysqlSpatialType(colMap?.get(c) ?? "")) {
        const raw = changes[c];
        const wkt =
          typeof raw === "string"
            ? (parseMysqlSpatialToWkt(raw) ?? raw)
            : String(raw ?? "");
        params.push(wkt);
        return `${quoteId(c, t)} = ST_GeomFromText(?)`;
      }
      params.push(changes[c]);
      return `${quoteId(c, t)} = ?`;
    });
    const whereParts = pkCols.map((c) => {
      params.push(pkValues[c]);
      return `${quoteId(c, t)} = ?`;
    });
    return {
      sql: `UPDATE ${qt} SET ${setParts.join(", ")} WHERE ${whereParts.join(" AND ")}`,
      params,
    };
  }
}

export interface RowUpdate {
  primaryKeys: Record<string, unknown>;
  changes: Record<string, unknown>;
}

export interface ApplyResult {
  success: boolean;
  error?: string;
  failedRows?: number[];
}

export async function applyChangesTransactional(
  cm: ConnectionManager,
  connectionId: string,
  database: string,
  schema: string,
  table: string,
  updates: RowUpdate[],
): Promise<ApplyResult> {
  if (updates.length === 0) {
    return { success: true };
  }

  const config = cm.getConnection(connectionId);
  const driver = cm.getDriver(connectionId);
  if (!config || !driver) {
    return { success: false, error: "Not connected" };
  }

  const operations: import("./dbDrivers/types").TransactionOperation[] = [];
  const svc = new TableDataService(cm);
  const cols = await svc.getColumns(connectionId, database, schema, table);

  for (const { primaryKeys, changes } of updates) {
    const op = buildUpdateRowSql(
      config.type,
      database,
      schema,
      table,
      primaryKeys,
      changes,
      cols,
    );
    if (op) {
      operations.push({
        sql: op.sql,
        params: op.params,
        checkAffectedRows: true,
      });
    }
  }

  if (operations.length === 0) {
    return { success: true };
  }

  try {
    await driver.runTransaction(operations);
    return { success: true };
  } catch (err: any) {
    return { success: false, error: err?.message ?? String(err) };
  }
}
