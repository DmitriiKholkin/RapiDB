import { Pool, types as pgTypes } from "pg";
import type { DdlOnlyDbObjectKind } from "../../shared/dbObjectKinds";
import type { ConnectionConfig } from "../connectionManager";
import { getSshTcpForwardTransport } from "../driverRuntimeConfig";
import { resolveConnectionTlsSettings } from "../services/connectionTls";
import { logger } from "../utils/logger";
import {
  BaseDBDriver,
  formatDatetimeForDisplay,
  isoToLocalDateStr,
  normalizeSqlDatetimeOffsetSpacing,
} from "./BaseDBDriver";
import type { DriverTimeoutSettingsProvider } from "./timeout";
import type {
  ColumnMeta,
  ColumnTypeMeta,
  DatabaseInfo,
  DriverEntityManifest,
  FilterConditionResult,
  FilterOperator,
  ForeignKeyMeta,
  IndexMeta,
  PaginationResult,
  PersistedEditCheckOptions,
  PersistedEditCheckResult,
  QueryResult,
  SchemaInfo,
  TableInfo,
  TypeCategory,
  ValueSemantics,
} from "./types";
import { DATETIME_SQL_RE, ISO_DATETIME_RE, NULL_SENTINEL } from "./types";

const POSTGRES_ENTITY_MANIFEST: DriverEntityManifest = {
  dbObjectKinds: [
    "table",
    "view",
    "materializedView",
    "function",
    "procedure",
    "sequence",
    "type",
  ],
  tableSections: {
    columns: "supported",
    constraints: "supported",
    indexes: "supported",
    triggers: "supported",
  },
  tableSectionOverridesByObjectKind: {
    view: {
      constraints: "not_applicable",
      indexes: "not_applicable",
    },
    materializedView: {
      constraints: "not_applicable",
      triggers: "not_applicable",
    },
  },
};

const PG_OID_DATE = 1082;
const PG_OID_MONEY = 790;
const PG_OID_NUMERIC = 1700;
const PG_OID_TIMESTAMP = 1114;
const PG_OID_TIMESTAMPTZ = 1184;
pgTypes.setTypeParser(PG_OID_DATE, (val: string) => val);
pgTypes.setTypeParser(PG_OID_MONEY, (val: string) => val);
pgTypes.setTypeParser(PG_OID_NUMERIC, (val: string) => val);
pgTypes.setTypeParser(PG_OID_TIMESTAMP, (val: string) => val);
pgTypes.setTypeParser(PG_OID_TIMESTAMPTZ, (val: string) => val);
const PG_GEOMETRIC_TYPES = new Set([
  "point",
  "line",
  "lseg",
  "box",
  "path",
  "polygon",
  "circle",
]);

function pgIdentityGenerationKind(
  identityKind: string | null | undefined,
): ColumnMeta["identityGeneration"] {
  if (identityKind === "a") {
    return "always";
  }
  if (identityKind === "d") {
    return "by_default";
  }
  return undefined;
}

function normalizeJsonFilterValue(value: string): string | null {
  const trimmed = value.trim();
  if (trimmed === "") {
    return null;
  }

  try {
    return JSON.stringify(JSON.parse(trimmed));
  } catch {
    return null;
  }
}

function isPointValue(value: object): value is {
  x: unknown;
  y: unknown;
} {
  return "x" in value && "y" in value && Object.keys(value).length === 2;
}
function isCircleValue(value: object): value is {
  x: unknown;
  y: unknown;
  radius: unknown;
} {
  return (
    "x" in value &&
    "y" in value &&
    "radius" in value &&
    Object.keys(value).length === 3
  );
}
function hasFiniteNumber(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value);
}
function trimPostgresIntervalNumber(value: number): string {
  if (Number.isInteger(value)) {
    return String(value);
  }
  return value
    .toString()
    .replace(/(\.\d*?[1-9])0+$/, "$1")
    .replace(/\.0+$/, "");
}
function formatPostgresIntervalLikeValue(value: object): string | null {
  const record = value as Record<string, unknown>;
  const knownKeys = new Set([
    "years",
    "months",
    "days",
    "hours",
    "minutes",
    "seconds",
    "milliseconds",
    "microseconds",
  ]);
  const keys = Object.keys(record);
  if (
    keys.length === 0 ||
    keys.some((key) => !knownKeys.has(key)) ||
    keys.some((key) => !hasFiniteNumber(record[key]))
  ) {
    return null;
  }

  const years = hasFiniteNumber(record.years) ? record.years : 0;
  const months = hasFiniteNumber(record.months) ? record.months : 0;
  const days = hasFiniteNumber(record.days) ? record.days : 0;
  const hours = hasFiniteNumber(record.hours) ? record.hours : 0;
  const minutes = hasFiniteNumber(record.minutes) ? record.minutes : 0;
  const seconds = hasFiniteNumber(record.seconds) ? record.seconds : 0;
  const milliseconds = hasFiniteNumber(record.milliseconds)
    ? record.milliseconds
    : 0;
  const microseconds = hasFiniteNumber(record.microseconds)
    ? record.microseconds
    : 0;

  const normalizedSeconds =
    seconds + milliseconds / 1000 + microseconds / 1_000_000;
  let iso = "P";
  if (years !== 0) iso += `${trimPostgresIntervalNumber(years)}Y`;
  if (months !== 0) iso += `${trimPostgresIntervalNumber(months)}M`;
  if (days !== 0) iso += `${trimPostgresIntervalNumber(days)}D`;
  if (hours !== 0 || minutes !== 0 || normalizedSeconds !== 0) {
    iso += "T";
    if (hours !== 0) iso += `${trimPostgresIntervalNumber(hours)}H`;
    if (minutes !== 0) iso += `${trimPostgresIntervalNumber(minutes)}M`;
    if (normalizedSeconds !== 0) {
      iso += `${trimPostgresIntervalNumber(normalizedSeconds)}S`;
    }
  }

  return iso === "P" ? "P0D" : iso;
}
function isPgTrue(value: unknown): boolean {
  if (typeof value === "boolean") return value;
  if (typeof value === "number") return value === 1;
  return (
    typeof value === "string" &&
    ["true", "t", "1"].includes(value.toLowerCase())
  );
}
function toOptionalNumber(value: unknown): number | undefined {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value !== "string") {
    return undefined;
  }
  const normalized = value.trim();
  if (normalized === "") {
    return undefined;
  }
  const parsed = Number(normalized);
  return Number.isFinite(parsed) ? parsed : undefined;
}
function pgIdentityKind(value: unknown): "" | "a" | "d" {
  return value === "a" || value === "d" ? value : "";
}
function pgIdentityClause(value: unknown): string {
  const identityKind = pgIdentityKind(value);
  if (identityKind === "a") {
    return " GENERATED ALWAYS AS IDENTITY";
  }
  if (identityKind === "d") {
    return " GENERATED BY DEFAULT AS IDENTITY";
  }
  return "";
}
function escapePostgresPreviewString(value: string): string {
  return value.replace(/'/g, "''");
}
function normalizeTemporalSearchValue(value: string): string {
  const trimmed = value.trim();
  const normalizedSql = normalizeSqlDatetimeOffsetSpacing(
    trimmed.replace("T", " "),
  );
  if (ISO_DATETIME_RE.test(trimmed) || DATETIME_SQL_RE.test(normalizedSql)) {
    return normalizedSql
      .replace(/(\.\d*?[1-9])0+(?=[Zz+-]|$)/, "$1")
      .replace(/\.0+(?=[Zz+-]|$)/, "")
      .replace(" ", "%")
      .replace(/[zZ]$/, "")
      .replace(/[+-]\d{2}(?::?\d{2})?$/, "");
  }
  return trimmed;
}
function normalizePostgresTemporalValue(value: string): string {
  const trimmed = value.trim().replace(/^(["'])(.*)\1$/s, "$2");
  const normalizedSql = normalizeSqlDatetimeOffsetSpacing(
    trimmed.replace("T", " "),
  );
  if (ISO_DATETIME_RE.test(trimmed) || DATETIME_SQL_RE.test(normalizedSql)) {
    return normalizedSql
      .replace(/(\.\d*?[1-9])0+(?=[Zz+-]|$)/, "$1")
      .replace(/\.0+(?=[Zz+-]|$)/, "");
  }
  return trimmed;
}
function canonicalizePostgresTemporalPersistedValue(
  value: unknown,
): { canonical: string } | null {
  if (value === NULL_SENTINEL || value === null || value === undefined) {
    return { canonical: "__rapidb_null__" };
  }

  const raw =
    value instanceof Date
      ? (formatDatetimeForDisplay(value) ?? value.toISOString())
      : typeof value === "string"
        ? value
        : typeof value === "number" ||
            typeof value === "boolean" ||
            typeof value === "bigint"
          ? String(value)
          : null;
  if (raw === null) {
    return null;
  }

  const trimmed = raw.trim();
  if (trimmed === "") {
    return { canonical: "" };
  }

  const normalizedSql = normalizeSqlDatetimeOffsetSpacing(
    trimmed.replace("T", " "),
  );
  const normalizedOffset = normalizedSql
    .replace(/[zZ]$/, "+00:00")
    .replace(/([+-]\d{2})$/, "$1:00")
    .replace(/([+-]\d{2})(\d{2})$/, "$1:$2");
  const normalizedFraction = normalizedOffset
    .replace(/(\.\d*?[1-9])0+(?=[+-]\d{2}:\d{2}$|$)/, "$1")
    .replace(/\.0+(?=[+-]\d{2}:\d{2}$|$)/, "");

  return { canonical: normalizedFraction };
}
function isLikelyPostgresAutoUpdatedTemporalColumn(
  column: Pick<ColumnTypeMeta, "name" | "category">,
): boolean {
  if (column.category !== "datetime") {
    return false;
  }

  return /(^|_)(updated|modified)(_at|at|_on|on)?$/i.test(column.name);
}
function postgresTemporalCastType(
  column: Pick<ColumnTypeMeta, "category" | "nativeType">,
): "date" | "time" | "timetz" | "timestamp" | "timestamptz" {
  const nativeType = column.nativeType.toLowerCase();
  if (column.category === "date") {
    return "date";
  }
  if (column.category === "time") {
    return nativeType.includes("with time zone") || nativeType === "timetz"
      ? "timetz"
      : "time";
  }
  return nativeType.includes("with time zone") ? "timestamptz" : "timestamp";
}
function approximateNumericFilterTolerance(rawValue: string): number {
  const fraction = /\.(\d+)/.exec(rawValue)?.[1].length ?? 0;
  const precision = Math.min(Math.max(fraction + 2, 6), 12);
  return 10 ** -precision;
}

function parsePostgresRoutineIdentity(value: string | undefined): {
  oid: string;
} | null {
  if (!value) {
    return null;
  }

  const match = /^oid:(\d+)$/.exec(value.trim());
  if (!match) {
    return null;
  }

  return { oid: match[1] };
}

function safeJsonStringify(value: unknown): string {
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}

export class PostgresDriver extends BaseDBDriver {
  protected override getQueryEditorSqlDialect() {
    return "postgresql" as const;
  }

  private pool: Pool | null = null;
  private readonly config: ConnectionConfig;
  private _connected = false;
  private connectedDatabaseName = "";
  private timeoutRecoveryInFlight: Promise<void> | null = null;
  private requirePool(): Pool {
    if (!this.pool) {
      throw new Error("[RapiDB] PostgreSQL connection is not open");
    }
    return this.pool;
  }
  private createPool(database: string): Pool {
    const tlsSettings = resolveConnectionTlsSettings(this.config);
    const forwardedTransport = getSshTcpForwardTransport(this.config);
    const dbOperationTimeoutMs = this.getDbOperationTimeoutMs();
    return new Pool({
      host: forwardedTransport?.localHost ?? this.config.host,
      port: forwardedTransport?.localPort ?? this.config.port,
      database,
      user: this.config.username,
      password: this.config.password,
      max: 5,
      keepAlive: true,
      keepAliveInitialDelayMillis: 60000,
      connectionTimeoutMillis: this.getConnectionTimeoutMs(),
      query_timeout: dbOperationTimeoutMs,
      statement_timeout: dbOperationTimeoutMs,
      idleTimeoutMillis: 30000,
      ssl: tlsSettings
        ? {
            rejectUnauthorized: tlsSettings.rejectUnauthorized,
            servername: tlsSettings.servername,
            ca: tlsSettings.ca,
            cert: tlsSettings.cert,
            key: tlsSettings.key,
            passphrase: tlsSettings.passphrase,
            checkServerIdentity: tlsSettings.checkServerIdentity,
          }
        : undefined,
    });
  }
  private async withDatabasePool<T>(
    database: string,
    run: (pool: Pool) => Promise<T>,
  ): Promise<T> {
    if (
      !database ||
      database === this.connectedDatabaseName ||
      database === this.config.database
    ) {
      return run(this.requirePool());
    }

    const pool = this.createPool(database);
    try {
      const client = await pool.connect();
      client.release();
      return await run(pool);
    } finally {
      await pool.end().catch(() => undefined);
    }
  }
  constructor(
    config: ConnectionConfig,
    timeoutSettingsProvider?: DriverTimeoutSettingsProvider,
  ) {
    super(timeoutSettingsProvider);
    this.config = config;
  }
  async connect(): Promise<void> {
    if (this.pool !== null) {
      try {
        await this.pool.end();
      } catch {}
      this.pool = null;
    }
    this.pool = this.createPool(this.config.database ?? "");
    this.pool.on("error", (err) => {
      logger.error("PostgreSQL pool error", err);
      this._connected = false;
    });
    const client = await this.pool.connect();
    try {
      const databaseRes = await client.query<{ name: string }>(
        `SELECT current_database() AS name`,
      );
      this.connectedDatabaseName =
        databaseRes.rows[0]?.name ?? this.config.database ?? "";
    } finally {
      client.release();
    }
    this._connected = true;
  }
  async disconnect(): Promise<void> {
    this._connected = false;
    this.connectedDatabaseName = "";
    await this.pool?.end();
    this.pool = null;
  }

  async cancelCurrentOperation(): Promise<void> {
    await this.recycleConnectionAfterTimeout({
      timeoutKind: "dbOperation",
      operationName: "cancelCurrentOperation",
    });
  }

  async recycleConnectionAfterTimeout(_context?: {
    timeoutKind?: "connection" | "dbOperation";
    operationName?: string;
  }): Promise<void> {
    if (this.timeoutRecoveryInFlight) {
      await this.timeoutRecoveryInFlight;
      return;
    }

    const recover = async () => {
      const wasConnected = this.isConnected();
      await this.disconnect().catch(() => undefined);
      if (wasConnected) {
        await this.connect().catch(() => undefined);
      }
    };

    this.timeoutRecoveryInFlight = recover().finally(() => {
      this.timeoutRecoveryInFlight = null;
    });

    await this.timeoutRecoveryInFlight;
  }

  isConnected(): boolean {
    return this.pool !== null && this._connected;
  }

  getEntityManifest(): DriverEntityManifest {
    return POSTGRES_ENTITY_MANIFEST;
  }
  async listDatabases(): Promise<DatabaseInfo[]> {
    try {
      const res = await this.requirePool().query<{ name: string }>(
        `SELECT datname AS name
         FROM pg_database
         WHERE datistemplate = FALSE
           AND datallowconn = TRUE
         ORDER BY CASE WHEN datname = current_database() THEN 0 ELSE 1 END,
                  datname`,
      );
      if (res.rows.length > 0) {
        return res.rows.map((row) => ({ name: row.name, schemas: [] }));
      }
    } catch {}

    const fallbackName =
      this.connectedDatabaseName || this.config.database || "postgres";
    return [{ name: fallbackName, schemas: [] }];
  }
  async listSchemas(database: string): Promise<SchemaInfo[]> {
    const res = await this.withDatabasePool(database, (pool) =>
      pool.query<{
        schema_name: string;
      }>(`SELECT schema_name FROM information_schema.schemata
       WHERE schema_name NOT IN ('information_schema','pg_catalog','pg_toast','pg_temp_1','pg_toast_temp_1')
       ORDER BY schema_name`),
    );
    return res.rows.map((r) => ({ name: r.schema_name }));
  }
  async listObjects(database: string, schema: string): Promise<TableInfo[]> {
    return this.withDatabasePool(database, async (pool) => {
      const objects: TableInfo[] = [];
      const tableRes = await pool.query(
        `SELECT table_name AS name, table_type AS type
         FROM information_schema.tables WHERE table_schema = $1 ORDER BY table_name`,
        [schema],
      );
      for (const r of tableRes.rows) {
        objects.push({
          schema,
          name: r.name as string,
          type: (r.type === "VIEW" ? "view" : "table") as TableInfo["type"],
        });
      }
      try {
        const materializedViewRes = await pool.query(
          `SELECT matviewname AS name
           FROM pg_matviews
           WHERE schemaname = $1
           ORDER BY matviewname`,
          [schema],
        );
        for (const r of materializedViewRes.rows) {
          objects.push({
            schema,
            name: r.name as string,
            type: "materializedView",
          });
        }
      } catch {}
      try {
        const routineRes = await pool.query(
          `SELECT p.proname AS name,
                  p.oid::text AS routine_id,
                  CASE p.prokind WHEN 'f' THEN 'function' WHEN 'p' THEN 'procedure'
                                 WHEN 'a' THEN 'function'  ELSE 'function' END AS type
           FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
           WHERE n.nspname = $1 AND p.prokind IN ('f','p','a') ORDER BY p.proname`,
          [schema],
        );
        for (const r of routineRes.rows) {
          objects.push({
            schema,
            name: r.name as string,
            type: r.type as TableInfo["type"],
            routineIdentity:
              typeof r.routine_id === "string"
                ? `oid:${r.routine_id}`
                : undefined,
          });
        }
      } catch {
        try {
          const routineRes = await pool.query(
            `SELECT routine_name AS name, routine_type AS type
             FROM information_schema.routines WHERE routine_schema = $1 ORDER BY routine_name`,
            [schema],
          );
          for (const r of routineRes.rows) {
            objects.push({
              schema,
              name: r.name as string,
              type: (r.type === "PROCEDURE"
                ? "procedure"
                : "function") as TableInfo["type"],
            });
          }
        } catch {}
      }
      try {
        const sequenceRes = await pool.query(
          `SELECT sequence_name AS name
           FROM information_schema.sequences
           WHERE sequence_schema = $1
           ORDER BY sequence_name`,
          [schema],
        );
        for (const r of sequenceRes.rows) {
          objects.push({
            schema,
            name: r.name as string,
            type: "sequence",
          });
        }
      } catch {}
      try {
        const typeRes = await pool.query(
          `SELECT t.typname AS name
           FROM pg_type t
           JOIN pg_namespace n ON n.oid = t.typnamespace
           LEFT JOIN pg_class c ON c.oid = t.typrelid
           WHERE n.nspname = $1
             AND (
               t.typtype IN ('e', 'd')
               OR (t.typtype = 'c' AND c.relkind = 'c')
             )
           ORDER BY t.typname`,
          [schema],
        );
        for (const r of typeRes.rows) {
          objects.push({
            schema,
            name: r.name as string,
            type: "type",
          });
        }
      } catch {}
      return objects;
    });
  }
  async describeTable(
    _database: string,
    schema: string,
    table: string,
  ): Promise<ColumnMeta[]> {
    type DescribeTableRow = {
      column_name: string;
      data_type: string;
      is_nullable: boolean | number | string;
      column_default: string | null;
      generated_kind: string | null;
      identity_kind: string | null;
      is_pk: boolean | number | string;
      pk_ordinal: number | string | null;
      is_fk: boolean | number | string;
    };
    const res = await this.requirePool().query<DescribeTableRow>(
      `SELECT
         a.attname                                AS column_name,
         format_type(a.atttypid, a.atttypmod)    AS data_type,
         NOT a.attnotnull                         AS is_nullable,
         pg_get_expr(d.adbin, d.adrelid)         AS column_default,
         NULLIF(a.attgenerated, '')               AS generated_kind,
         NULLIF(a.attidentity, '')                AS identity_kind,
         pk.pk_ordinal IS NOT NULL                AS is_pk,
         pk.pk_ordinal                            AS pk_ordinal,
         EXISTS (
           SELECT 1 FROM pg_constraint con
           WHERE con.conrelid = a.attrelid
             AND con.contype = 'f'
             AND a.attnum = ANY(con.conkey)
         ) AS is_fk
       FROM pg_attribute a
       JOIN pg_class     c ON c.oid = a.attrelid
       JOIN pg_namespace n ON n.oid = c.relnamespace
       LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
       LEFT JOIN LATERAL (
         SELECT pk_key.ordinality::int AS pk_ordinal
         FROM pg_constraint con
           CROSS JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS pk_key(attnum, ordinality)
           WHERE con.conrelid = a.attrelid
             AND con.contype = 'p'
             AND pk_key.attnum = a.attnum
         LIMIT 1
       ) pk ON TRUE
       WHERE n.nspname = $1
         AND c.relname = $2
         AND a.attnum > 0
         AND NOT a.attisdropped
       ORDER BY a.attnum`,
      [schema, table],
    );
    return res.rows.map((r) => {
      const rawDefault = r.column_default as string | null | undefined;
      const generatedKind = r.generated_kind as string | null | undefined;
      const identityGeneration = pgIdentityGenerationKind(
        r.identity_kind as string | null | undefined,
      );
      const isComputed = generatedKind === "s";
      const computedExpression =
        isComputed && typeof rawDefault === "string" ? rawDefault : undefined;
      const defaultValue = !isComputed ? (rawDefault ?? undefined) : undefined;
      return {
        name: r.column_name as string,
        type: r.data_type as string,
        nullable: isPgTrue(r.is_nullable),
        defaultValue,
        identityGeneration,
        isComputed,
        computedExpression,
        generatedKind: isComputed ? "stored" : undefined,
        isPersisted: isComputed ? true : undefined,
        isPrimaryKey: isPgTrue(r.is_pk),
        primaryKeyOrdinal: toOptionalNumber(r.pk_ordinal),
        isForeignKey: isPgTrue(r.is_fk),
      };
    });
  }
  async query(sql: string, params?: unknown[]): Promise<QueryResult> {
    type PgArrayField = {
      name: string;
    };
    type PgArrayQueryResult = {
      fields?: PgArrayField[];
      rows?: unknown[][];
      rowCount?: number | null;
    };
    const start = Date.now();
    const res = await this.requirePool().query({
      text: sql,
      values: params ?? [],
      rowMode: "array",
    });
    const executionTimeMs = Date.now() - start;
    const result = (
      Array.isArray(res) ? res[res.length - 1] : res
    ) as PgArrayQueryResult;
    const columns = result.fields?.map((field) => field.name) ?? [];
    const rawRows: unknown[][] = result.rows ?? [];
    const rows = rawRows.map((row) =>
      Object.fromEntries(
        row.map((val, i) => {
          const normalized =
            val !== null &&
            typeof val === "object" &&
            !(val instanceof Date) &&
            isPointValue(val)
              ? `(${String(val.x)}, ${String(val.y)})`
              : val;
          return [`__col_${i}`, normalized];
        }),
      ),
    );
    return {
      columns,
      rows,
      rowCount: result.rowCount ?? rawRows.length,
      executionTimeMs,
    };
  }
  async getIndexes(
    _database: string,
    schema: string,
    table: string,
  ): Promise<IndexMeta[]> {
    type IndexRow = {
      name: string;
      unique: boolean | number | string;
      primary: boolean | number | string;
      column: string;
    };
    const res = await this.requirePool().query<IndexRow>(
      `SELECT i.relname AS name,
              ix.indisunique AS unique,
              ix.indisprimary AS primary,
              COALESCE(
                a.attname,
                pg_get_indexdef(i.oid, idx.key_ordinal::int, true)
              ) AS column
       FROM pg_class c
       JOIN pg_index ix ON ix.indrelid = c.oid
       JOIN pg_class i  ON i.oid = ix.indexrelid
       JOIN LATERAL unnest(string_to_array(ix.indkey::text, ' ')::int[]) WITH ORDINALITY AS idx(attnum, key_ordinal)
         ON TRUE
       LEFT JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = idx.attnum AND idx.attnum > 0
       JOIN pg_namespace n ON n.oid = c.relnamespace
       WHERE n.nspname = $1 AND c.relname = $2
       ORDER BY i.relname, idx.key_ordinal`,
      [schema, table],
    );
    const map = new Map<string, IndexMeta>();
    for (const r of res.rows) {
      if (!r.column) {
        continue;
      }
      if (!map.has(r.name)) {
        map.set(r.name, {
          name: r.name,
          columns: [],
          unique: isPgTrue(r.unique),
          primary: isPgTrue(r.primary),
        });
      }
      const index = map.get(r.name);
      if (index) {
        index.columns.push(r.column);
      }
    }
    return [...map.values()];
  }
  async getForeignKeys(
    _database: string,
    schema: string,
    table: string,
  ): Promise<ForeignKeyMeta[]> {
    type ForeignKeyRow = {
      constraint_name: string;
      column_name: string;
      ref_schema: string;
      ref_table: string;
      ref_column: string;
    };
    const res = await this.requirePool().query<ForeignKeyRow>(
      `SELECT con.conname        AS constraint_name,
              src.attname        AS column_name,
              ref_ns.nspname     AS ref_schema,
              ref_tbl.relname    AS ref_table,
              ref.attname        AS ref_column
       FROM pg_constraint con
       JOIN pg_class tbl ON tbl.oid = con.conrelid
       JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
       JOIN pg_class ref_tbl ON ref_tbl.oid = con.confrelid
       JOIN pg_namespace ref_ns ON ref_ns.oid = ref_tbl.relnamespace
       JOIN LATERAL unnest(con.conkey, con.confkey) WITH ORDINALITY AS fk_cols(local_attnum, ref_attnum, ordinality)
         ON TRUE
       JOIN pg_attribute src ON src.attrelid = con.conrelid AND src.attnum = fk_cols.local_attnum
       JOIN pg_attribute ref ON ref.attrelid = con.confrelid AND ref.attnum = fk_cols.ref_attnum
       WHERE con.contype = 'f'
         AND ns.nspname = $1
         AND tbl.relname = $2
       ORDER BY con.conname, fk_cols.ordinality`,
      [schema, table],
    );
    return res.rows.map((r) => ({
      constraintName: r.constraint_name,
      column: r.column_name,
      referencedSchema: r.ref_schema,
      referencedTable: r.ref_table,
      referencedColumn: r.ref_column,
    }));
  }
  async getConstraints(
    database: string,
    schema: string,
    table: string,
  ): Promise<import("./types").TableConstraintMeta[]> {
    const constraints = await super.getConstraints(database, schema, table);
    const res = await this.requirePool().query<{
      constraint_name: string;
      check_expression: string;
    }>(
      `SELECT con.conname AS constraint_name,
              pg_get_constraintdef(con.oid, true) AS check_expression
       FROM pg_constraint con
       JOIN pg_class tbl ON tbl.oid = con.conrelid
       JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
       WHERE con.contype = 'c'
         AND ns.nspname = $1
         AND tbl.relname = $2
       ORDER BY con.conname`,
      [schema, table],
    );
    constraints.push(
      ...res.rows.map((row) => ({
        name: row.constraint_name,
        kind: "check" as const,
        columns: [],
        checkExpression: row.check_expression,
        source: "catalog" as const,
      })),
    );
    return constraints;
  }
  async getTriggers(
    _database: string,
    schema: string,
    table: string,
  ): Promise<import("./types").TriggerMeta[] | null> {
    const res = await this.requirePool().query<{
      trigger_name: string;
      trigger_type: number | string;
      enabled_state: string;
      definition: string;
    }>(
      `SELECT t.tgname AS trigger_name,
              t.tgtype AS trigger_type,
              t.tgenabled AS enabled_state,
              pg_get_triggerdef(t.oid, true) AS definition
       FROM pg_trigger t
       JOIN pg_class c ON c.oid = t.tgrelid
       JOIN pg_namespace n ON n.oid = c.relnamespace
       WHERE n.nspname = $1
         AND c.relname = $2
         AND NOT t.tgisinternal
       ORDER BY t.tgname`,
      [schema, table],
    );
    return res.rows.map((row) => {
      const triggerType = Number(row.trigger_type);
      const events: import("./types").TriggerMeta["events"] = [];
      if ((triggerType & 4) === 4) {
        events.push("insert");
      }
      if ((triggerType & 8) === 8) {
        events.push("delete");
      }
      if ((triggerType & 16) === 16) {
        events.push("update");
      }
      if ((triggerType & 32) === 32) {
        events.push("truncate");
      }
      if (events.length === 0) {
        events.push("unknown");
      }

      return {
        name: row.trigger_name,
        timing:
          (triggerType & 64) === 64
            ? "instead_of"
            : (triggerType & 2) === 2
              ? "before"
              : "after",
        events,
        orientation: (triggerType & 1) === 1 ? "row" : "statement",
        enabled: row.enabled_state !== "D",
        definition: row.definition,
      };
    });
  }
  override async getConstraintDDL(
    _database: string,
    schema: string,
    table: string,
    constraintName: string,
  ): Promise<string> {
    const res = await this.requirePool().query<{ ddl: string }>(
      `SELECT 'ALTER TABLE ' || quote_ident(ns.nspname) || '.' || quote_ident(tbl.relname) ||
              ' ADD CONSTRAINT ' || quote_ident(con.conname) || ' ' ||
              pg_get_constraintdef(con.oid, true) || ';' AS ddl
       FROM pg_constraint con
       JOIN pg_class tbl ON tbl.oid = con.conrelid
       JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
       WHERE ns.nspname = $1
         AND tbl.relname = $2
         AND con.conname = $3
       LIMIT 1`,
      [schema, table, constraintName],
    );
    const ddl = res.rows[0]?.ddl;
    if (!ddl) {
      throw new Error(`Constraint "${constraintName}" not found`);
    }
    return ddl;
  }
  override async getIndexDDL(
    _database: string,
    schema: string,
    table: string,
    indexName: string,
  ): Promise<string> {
    const res = await this.requirePool().query<{ ddl: string }>(
      `SELECT pg_get_indexdef(idx.oid, 0, true) || ';' AS ddl
       FROM pg_class tbl
       JOIN pg_index pg_idx ON pg_idx.indrelid = tbl.oid
       JOIN pg_class idx ON idx.oid = pg_idx.indexrelid
       JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
       WHERE ns.nspname = $1
         AND tbl.relname = $2
         AND idx.relname = $3
       LIMIT 1`,
      [schema, table, indexName],
    );
    const ddl = res.rows[0]?.ddl;
    if (!ddl) {
      throw new Error(`Index "${indexName}" not found`);
    }
    return ddl;
  }
  override async getTriggerDDL(
    _database: string,
    schema: string,
    table: string,
    triggerName: string,
  ): Promise<string> {
    const res = await this.requirePool().query<{ ddl: string }>(
      `SELECT pg_get_triggerdef(trg.oid, true) || ';' AS ddl
       FROM pg_trigger trg
       JOIN pg_class tbl ON tbl.oid = trg.tgrelid
       JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
       WHERE ns.nspname = $1
         AND tbl.relname = $2
         AND trg.tgname = $3
         AND NOT trg.tgisinternal
       LIMIT 1`,
      [schema, table, triggerName],
    );
    const ddl = res.rows[0]?.ddl;
    if (!ddl) {
      throw new Error(`Trigger "${triggerName}" not found`);
    }
    return ddl;
  }
  async getCreateTableDDL(
    _database: string,
    schema: string,
    table: string,
  ): Promise<string> {
    type DdlColumnRow = {
      column_name: string;
      data_type: string;
      is_nullable: boolean | number | string;
      column_default: string | null;
      generated_kind: string | null;
      identity_kind: string | null;
    };
    const pool = this.requirePool();
    const kindRes = await pool.query<{
      relkind: string;
    }>(
      `SELECT c.relkind
       FROM pg_class c
       JOIN pg_namespace n ON n.oid = c.relnamespace
       WHERE n.nspname = $1
         AND c.relname = $2
         AND c.relkind IN ('r', 'p', 'v', 'm', 'f')
       LIMIT 1`,
      [schema, table],
    );
    const relkind = kindRes.rows[0]?.relkind;
    if (relkind === "v") {
      const res = await pool.query<{
        def: string;
      }>(
        `SELECT 'CREATE OR REPLACE VIEW "' || n.nspname || '"."' || c.relname || '" AS\n' ||
                pg_get_viewdef(c.oid, true) AS def
         FROM pg_class c
         JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = $1 AND c.relname = $2 AND c.relkind = 'v'
         LIMIT 1`,
        [schema, table],
      );
      return (
        res.rows[0]?.def ??
        `-- View definition not available for "${schema}"."${table}"`
      );
    }
    if (relkind === "m") {
      const res = await pool.query<{
        def: string;
      }>(
        `SELECT 'CREATE MATERIALIZED VIEW "' || n.nspname || '"."' || c.relname || '" AS\n' ||
                pg_get_viewdef(c.oid, true) || ';' AS def
         FROM pg_class c
         JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = $1 AND c.relname = $2 AND c.relkind = 'm'
         LIMIT 1`,
        [schema, table],
      );
      return (
        res.rows[0]?.def ??
        `-- Materialized view definition not available for "${schema}"."${table}"`
      );
    }
    const colRes = await pool.query<DdlColumnRow>(
      `SELECT
         a.attname                                AS column_name,
         format_type(a.atttypid, a.atttypmod)    AS data_type,
         NOT a.attnotnull                         AS is_nullable,
         pg_get_expr(d.adbin, d.adrelid)         AS column_default,
         NULLIF(a.attgenerated, '')               AS generated_kind,
         NULLIF(a.attidentity, '')                AS identity_kind
       FROM pg_attribute a
       JOIN pg_class     c ON c.oid = a.attrelid
       JOIN pg_namespace n ON n.oid = c.relnamespace
       LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
       WHERE n.nspname = $1
         AND c.relname = $2
         AND a.attnum > 0
         AND NOT a.attisdropped
       ORDER BY a.attnum`,
      [schema, table],
    );
    const pkRes = await pool.query<{
      column_name: string;
      key_ordinal: number;
    }>(
      `SELECT a.attname AS column_name,
              pk_key.ordinality::int AS key_ordinal
       FROM pg_constraint con
       JOIN pg_class     c ON c.oid = con.conrelid
       JOIN pg_namespace n ON n.oid = c.relnamespace
       JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS pk_key(attnum, ordinality)
         ON TRUE
       JOIN pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = pk_key.attnum
       WHERE n.nspname = $1 AND c.relname = $2 AND con.contype = 'p'
       ORDER BY pk_key.ordinality`,
      [schema, table],
    );
    const pkColumns = pkRes.rows.map((row) => row.column_name);
    const pkColumnSet = new Set(pkColumns);
    const hasCompositePrimaryKey = pkColumns.length > 1;
    const cols = colRes.rows.map((r) => {
      const columnName = r.column_name as string;
      const isPk = pkColumnSet.has(columnName);
      const isComputed = r.generated_kind === "s";
      const nullable = isPgTrue(r.is_nullable);
      const notNull = !nullable && !isPk ? " NOT NULL" : "";
      const identityClause = pgIdentityClause(r.identity_kind);
      if (isComputed && r.column_default) {
        return `  ${this.quoteIdentifier(columnName)} ${r.data_type} GENERATED ALWAYS AS (${r.column_default}) STORED${notNull}`;
      }
      const defClause =
        !identityClause && r.column_default
          ? ` DEFAULT ${r.column_default}`
          : "";
      const pk = !hasCompositePrimaryKey && isPk ? " PRIMARY KEY" : "";
      return `  ${this.quoteIdentifier(columnName)} ${r.data_type}${identityClause}${notNull}${defClause}${pk}`;
    });
    if (hasCompositePrimaryKey) {
      cols.push(
        `  PRIMARY KEY (${pkColumns.map((columnName) => this.quoteIdentifier(columnName)).join(", ")})`,
      );
    }
    return `CREATE TABLE ${this.qualifiedTableName("", schema, table)} (\n${cols.join(",\n")}\n);`;
  }
  async getObjectDefinition(
    _database: string,
    schema: string,
    name: string,
    kind: DdlOnlyDbObjectKind,
  ): Promise<string | null> {
    if (kind === "sequence") {
      return this.getSequenceDefinition(schema, name);
    }
    return this.getTypeDefinition(schema, name);
  }
  async getRoutineDefinition(
    _database: string,
    schema: string,
    name: string,
    _kind: "function" | "procedure",
    routineIdentity?: string,
  ): Promise<string> {
    const parsedIdentity = parsePostgresRoutineIdentity(routineIdentity);
    if (parsedIdentity) {
      const byOidRes = await this.requirePool().query<{
        def: string;
      }>(
        `SELECT pg_get_functiondef(p.oid) AS def
         FROM pg_proc p
         WHERE p.oid = $1::oid
         LIMIT 1`,
        [parsedIdentity.oid],
      );
      const byOidDefinition = byOidRes.rows[0]?.def;
      if (byOidDefinition) {
        return byOidDefinition;
      }
    }

    const res = await this.requirePool().query<{
      def: string;
    }>(
      `SELECT pg_get_functiondef(p.oid) AS def
       FROM pg_proc p
       JOIN pg_namespace n ON n.oid = p.pronamespace
       WHERE n.nspname = $1 AND p.proname = $2
       LIMIT 1`,
      [schema, name],
    );
    return res.rows[0]?.def ?? `-- Definition not available for ${name}`;
  }

  private async getSequenceDefinition(
    schema: string,
    name: string,
  ): Promise<string | null> {
    const res = await this.requirePool().query<{
      data_type: string;
      start_value: string | number;
      min_value: string | number;
      max_value: string | number;
      increment_by: string | number;
      cycle: boolean;
      cache_size: string | number;
    }>(
      `SELECT data_type,
              start_value,
              min_value,
              max_value,
              increment_by,
              cycle,
              cache_size
       FROM pg_sequences
       WHERE schemaname = $1 AND sequencename = $2
       LIMIT 1`,
      [schema, name],
    );
    const row = res.rows[0];
    if (!row) {
      return null;
    }

    const clauses = [
      `CREATE SEQUENCE ${this.qualifiedTableName("", schema, name)}`,
      row.data_type && row.data_type !== "bigint"
        ? `AS ${row.data_type}`
        : undefined,
      `INCREMENT BY ${row.increment_by}`,
      `MINVALUE ${row.min_value}`,
      `MAXVALUE ${row.max_value}`,
      `START WITH ${row.start_value}`,
      `CACHE ${row.cache_size}`,
      row.cycle ? "CYCLE" : "NO CYCLE",
    ].filter((value): value is string => Boolean(value));

    return `${clauses.join(" ")};`;
  }

  private async getTypeDefinition(
    schema: string,
    name: string,
  ): Promise<string | null> {
    const metaRes = await this.requirePool().query<{
      typtype: string;
      typnotnull: boolean;
      typdefault: string | null;
      base_type: string | null;
    }>(
      `SELECT t.typtype,
              t.typnotnull,
              t.typdefault,
              format_type(t.typbasetype, t.typtypmod) AS base_type
       FROM pg_type t
       JOIN pg_namespace n ON n.oid = t.typnamespace
       LEFT JOIN pg_class c ON c.oid = t.typrelid
       WHERE n.nspname = $1
         AND t.typname = $2
         AND (
           t.typtype IN ('e', 'd')
           OR (t.typtype = 'c' AND c.relkind = 'c')
         )
       LIMIT 1`,
      [schema, name],
    );
    const meta = metaRes.rows[0];
    if (!meta) {
      return null;
    }

    const qualifiedName = this.qualifiedTableName("", schema, name);
    if (meta.typtype === "e") {
      const labelsRes = await this.requirePool().query<{
        enumlabel: string;
      }>(
        `SELECT e.enumlabel
         FROM pg_enum e
         JOIN pg_type t ON t.oid = e.enumtypid
         JOIN pg_namespace n ON n.oid = t.typnamespace
         WHERE n.nspname = $1 AND t.typname = $2
         ORDER BY e.enumsortorder`,
        [schema, name],
      );
      const labels = labelsRes.rows.map(
        (row) => `'${row.enumlabel.replace(/'/g, "''")}'`,
      );
      return `CREATE TYPE ${qualifiedName} AS ENUM (${labels.join(", ")});`;
    }

    if (meta.typtype === "d") {
      const clauses = [
        `CREATE DOMAIN ${qualifiedName} AS ${meta.base_type ?? "text"}`,
        meta.typdefault ? `DEFAULT ${meta.typdefault}` : undefined,
        meta.typnotnull ? "NOT NULL" : undefined,
      ].filter((value): value is string => Boolean(value));
      return `${clauses.join(" ")};`;
    }

    const attributesRes = await this.requirePool().query<{
      column_name: string;
      data_type: string;
    }>(
      `SELECT a.attname AS column_name,
              format_type(a.atttypid, a.atttypmod) AS data_type
       FROM pg_type t
       JOIN pg_namespace n ON n.oid = t.typnamespace
       JOIN pg_class c ON c.oid = t.typrelid
       JOIN pg_attribute a ON a.attrelid = c.oid
       WHERE n.nspname = $1
         AND t.typname = $2
         AND c.relkind = 'c'
         AND a.attnum > 0
         AND NOT a.attisdropped
       ORDER BY a.attnum`,
      [schema, name],
    );
    const attributes = attributesRes.rows.map(
      (row) => `  ${this.quoteIdentifier(row.column_name)} ${row.data_type}`,
    );
    return `CREATE TYPE ${qualifiedName} AS (\n${attributes.join(",\n")}\n);`;
  }
  async runTransaction(
    operations: import("./types").TransactionOperation[],
  ): Promise<void> {
    const client = await this.requirePool().connect();
    try {
      await client.query("BEGIN");
      for (const op of operations) {
        const res = await client.query(op.sql, op.params ?? []);
        if (op.checkAffectedRows && res.rowCount === 0) {
          throw new Error(
            "Row not found — the row may have been modified or deleted by another user",
          );
        }
      }
      await client.query("COMMIT");
    } catch (e) {
      await client.query("ROLLBACK");
      throw e;
    } finally {
      client.release();
    }
  }
  mapTypeCategory(nativeType: string): TypeCategory {
    const ct = nativeType.toLowerCase().split("(")[0].trim();
    if (ct === "boolean" || ct === "bool") return "boolean";
    if (
      ct === "smallint" ||
      ct === "integer" ||
      ct === "bigint" ||
      ct === "serial" ||
      ct === "bigserial" ||
      ct === "smallserial" ||
      ct === "oid" ||
      ct === "xid" ||
      ct === "cid"
    )
      return "integer";
    if (
      ct === "real" ||
      ct === "double precision" ||
      ct === "float4" ||
      ct === "float8"
    )
      return "float";
    if (ct === "numeric" || ct === "decimal" || ct === "money")
      return "decimal";
    if (ct === "date") return "date";
    if (
      ct === "time" ||
      ct === "timetz" ||
      ct === "time with time zone" ||
      ct === "time without time zone"
    )
      return "time";
    if (ct.startsWith("timestamp")) return "datetime";
    if (ct === "bytea") return "binary";
    if (ct === "json" || ct === "jsonb") return "json";
    if (ct === "uuid") return "uuid";
    if (PG_GEOMETRIC_TYPES.has(ct)) return "spatial";
    if (ct === "interval" || nativeType.toLowerCase().startsWith("interval"))
      return "interval";
    if (
      nativeType.toLowerCase().endsWith("[]") ||
      nativeType.toLowerCase().startsWith("_") ||
      ct === "array"
    )
      return "array";
    if (ct === "bit" || ct === "varbit") return "other";
    if (ct === "inet" || ct === "cidr" || ct === "macaddr" || ct === "macaddr8")
      return "text";
    if (ct === "tsvector" || ct === "tsquery") return "text";
    if (
      ct === "text" ||
      ct === "char" ||
      ct === "bpchar" ||
      ct === "varchar" ||
      ct.startsWith("character") ||
      ct === "name" ||
      ct === "xml"
    )
      return "text";
    return "other";
  }
  protected getValueSemantics(
    nativeType: string,
    _category: TypeCategory,
  ): ValueSemantics {
    const ct = nativeType.toLowerCase().split("(")[0].trim();
    if (ct === "boolean" || ct === "bool") return "boolean";
    if (ct === "bit" || ct === "varbit" || ct === "bit varying") {
      return "bit";
    }
    return "plain";
  }
  protected override isFilterable(
    nativeType: string,
    category: TypeCategory,
  ): boolean {
    if (category === "interval" || category === "spatial") {
      return true;
    }
    return super.isFilterable(nativeType, category);
  }
  isDatetimeWithTime(nativeType: string): boolean {
    const ct = nativeType.toLowerCase();
    return (
      ct.startsWith("timestamp") ||
      ct === "timetz" ||
      ct === "time with time zone" ||
      ct === "time"
    );
  }
  override buildPagination(
    offset: number,
    limit: number,
    paramIndex: number,
  ): PaginationResult {
    return {
      sql: `LIMIT $${paramIndex} OFFSET $${paramIndex + 1}`,
      params: [limit, offset],
    };
  }
  override buildInsertValueExpr(
    _column: ColumnTypeMeta,
    paramIndex: number,
  ): string {
    return `$${paramIndex}`;
  }
  override buildSetExpr(column: ColumnTypeMeta, paramIndex: number): string {
    return `${this.quoteIdentifier(column.name)} = $${paramIndex}`;
  }
  materializePreviewColumnSql(
    sql: string,
    params: readonly unknown[] | undefined,
    columns: readonly (ColumnTypeMeta | undefined)[],
  ): string {
    if (!params || params.length === 0 || columns.length === 0) {
      return this.materializePreviewSql(sql, params);
    }
    return sql.replace(/\$(\d+)/g, (match, rawIndex: string) => {
      const index = Number.parseInt(rawIndex, 10) - 1;
      if (index < 0 || index >= params.length) {
        return match;
      }
      return this.formatColumnAwarePreviewLiteral(
        params[index],
        columns[index],
      );
    });
  }
  materializePreviewInsertSql(
    sql: string,
    params: readonly unknown[] | undefined,
    columns: readonly ColumnTypeMeta[],
  ): string {
    return this.materializePreviewColumnSql(sql, params, columns);
  }
  private formatColumnAwarePreviewLiteral(
    value: unknown,
    column: ColumnTypeMeta | undefined,
  ): string {
    if (column?.category === "array" && Array.isArray(value)) {
      return this.formatTypedPostgresArrayLiteral(value, column.nativeType);
    }
    return this.formatPreviewSqlLiteral(value);
  }
  private formatTypedPostgresArrayLiteral(
    value: readonly unknown[],
    nativeType: string,
  ): string {
    const arrayLiteral =
      value.length === 0
        ? "ARRAY[]"
        : this.formatPostgresArrayLiteralValue(value);
    return `CAST(${arrayLiteral} AS ${nativeType})`;
  }
  private formatPostgresArrayLiteralValue(value: unknown): string {
    if (value === null || value === undefined || value === NULL_SENTINEL) {
      return "NULL";
    }
    if (Array.isArray(value)) {
      return `ARRAY[${value.map((entry) => this.formatPostgresArrayLiteralValue(entry)).join(", ")}]`;
    }
    return this.formatPreviewSqlLiteral(value);
  }
  protected override formatPreviewSqlLiteral(value: unknown): string {
    if (value === null || value === undefined || value === NULL_SENTINEL) {
      return "NULL";
    }
    if (Array.isArray(value)) {
      if (value.length === 0) {
        return "'{}'";
      }
      return this.formatPostgresArrayLiteralValue(value);
    }
    if (Buffer.isBuffer(value)) {
      return `'\\x${value.toString("hex")}'::bytea`;
    }
    if (value instanceof ArrayBuffer) {
      return `'\\x${Buffer.from(new Uint8Array(value)).toString("hex")}'::bytea`;
    }
    if (ArrayBuffer.isView(value)) {
      return `'\\x${Buffer.from(value.buffer, value.byteOffset, value.byteLength).toString("hex")}'::bytea`;
    }
    if (typeof value === "string") {
      return `'${escapePostgresPreviewString(value)}'`;
    }
    return super.formatPreviewSqlLiteral(value);
  }
  override coerceInputValue(value: unknown, column: ColumnTypeMeta): unknown {
    if (value === null || value === undefined || value === "") return value;
    if (value === NULL_SENTINEL) return null;
    if (typeof value !== "string") return value;
    if (this.hasBooleanSemantics(column)) {
      const normalized = this.parseBooleanInput(value);
      if (normalized !== null) {
        return normalized;
      }
    }
    if (column.category === "array") {
      if (value.startsWith("[") && value.endsWith("]")) {
        try {
          const parsed = JSON.parse(value);
          if (Array.isArray(parsed)) return parsed;
        } catch {}
      }
    }
    if (column.category === "interval" && value.startsWith("{")) {
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
      return value;
    }
    if (column.category === "binary") {
      return super.coerceInputValue(value, column);
    }
    if (ISO_DATETIME_RE.test(value) && column.category === "date") {
      return isoToLocalDateStr(value) ?? value;
    }
    return value;
  }
  override formatOutputValue(value: unknown, column: ColumnTypeMeta): unknown {
    if (value === null || value === undefined) return value;
    if (Buffer.isBuffer(value)) return super.formatOutputValue(value, column);
    if (typeof value === "bigint") return value.toString();
    if (
      value !== null &&
      typeof value === "object" &&
      !(value instanceof Date)
    ) {
      const formattedInterval = formatPostgresIntervalLikeValue(value);
      if (formattedInterval !== null) {
        return formattedInterval;
      }
      if (isCircleValue(value)) {
        return `<(${String(value.x)},${String(value.y)}),${String(value.radius)}>`;
      }
      if (isPointValue(value)) {
        return `(${String(value.x)}, ${String(value.y)})`;
      }
      return safeJsonStringify(value);
    }
    if (this.isDatetimeWithTime(column.nativeType)) {
      const formatted = formatDatetimeForDisplay(value);
      if (formatted !== null) return formatted;
    }
    return value;
  }
  override checkPersistedEdit(
    column: ColumnTypeMeta,
    expectedValue: unknown,
    options?: PersistedEditCheckOptions,
  ): PersistedEditCheckResult | null {
    const baseType = column.nativeType.toLowerCase().split("(")[0].trim();
    if (column.category === "integer") {
      return this.checkExactNumericPersistedEdit(
        column,
        expectedValue,
        { precision: null, scale: 0 },
        options,
      );
    }
    if (column.category === "decimal") {
      if (baseType === "money") {
        return null;
      }
      if (!["numeric", "decimal"].includes(baseType)) {
        return null;
      }
      return this.checkExactNumericPersistedEdit(
        column,
        expectedValue,
        this.parseExactNumericConstraint(column.nativeType),
        options,
      );
    }
    if (column.category === "float") {
      const significantDigits =
        baseType === "real" || baseType === "float4" ? 7 : 15;
      return this.checkApproximateNumericPersistedEdit(
        column,
        expectedValue,
        significantDigits,
        options,
      );
    }
    if (column.category === "boolean") {
      return this.checkBooleanPersistedEdit(column, expectedValue, options);
    }
    if (column.category === "binary") {
      return this.checkBinaryPersistedEdit(column, expectedValue, options);
    }
    if (column.category === "json") {
      return this.checkJsonPersistedEdit(column, expectedValue, options);
    }
    if (column.category === "uuid") {
      return this.checkUuidPersistedEdit(column, expectedValue, options);
    }
    if (column.category === "array") {
      return this.checkJsonArrayPersistedEdit(column, expectedValue, options);
    }
    if (
      column.category === "text" ||
      column.category === "date" ||
      column.category === "time" ||
      column.category === "datetime"
    ) {
      if (isLikelyPostgresAutoUpdatedTemporalColumn(column)) {
        return {
          ok: true,
          shouldVerify: false,
        };
      }
      if (column.category === "date" || column.category === "time") {
        return this.checkNormalizedPersistedEdit(
          column,
          expectedValue,
          options,
          canonicalizePostgresTemporalPersistedValue,
          `Column "${column.name}" expects a temporal value.`,
        );
      }
      if (column.category === "datetime") {
        return this.checkNormalizedPersistedEdit(
          column,
          expectedValue,
          options,
          canonicalizePostgresTemporalPersistedValue,
          `Column "${column.name}" expects a temporal value.`,
        );
      }
      if (["char", "bpchar", "character"].includes(baseType)) {
        return this.checkFixedWidthCharPersistedEdit(
          column,
          expectedValue,
          options,
        );
      }
      return this.checkTextPersistedEdit(column, expectedValue, options);
    }
    return null;
  }
  override buildFilterCondition(
    column: ColumnTypeMeta,
    operator: FilterOperator,
    value: string | [string, string] | undefined,
    paramIndex: number,
  ): FilterConditionResult | null {
    const preamble = this.createFilterConditionPreamble(
      column,
      operator,
      value,
    );
    if (!preamble) return null;
    if (preamble.kind === "resolved") return preamble.condition;
    const col = preamble.columnSql;
    const val = preamble.value;
    if (column.category === "array") {
      if (operator !== "like" && operator !== "ilike") {
        return null;
      }
      const arrayValue = typeof val === "string" ? val : val[0];
      return {
        sql: `to_jsonb(${col})::text ILIKE $${paramIndex}`,
        params: [`%${arrayValue}%`],
      };
    }
    if (
      column.category === "binary" &&
      typeof val === "string" &&
      (operator === "eq" || operator === "neq")
    ) {
      const sqlOp = operator === "neq" ? "<>" : "=";
      return {
        sql: `${col} ${sqlOp} $${paramIndex}`,
        params: [this.coerceInputValue(val, column)],
      };
    }
    if (
      this.hasBooleanSemantics(column) &&
      (operator === "eq" || operator === "neq")
    ) {
      const strVal = (typeof val === "string" ? val : val[0]).toLowerCase();
      if (strVal === "true" || strVal === "false") {
        const boolVal = strVal === "true";
        const op = operator === "neq" ? "!=" : "=";
        return { sql: `${col} ${op} $${paramIndex}`, params: [boolVal] };
      }
    }
    if (column.category === "spatial" && typeof val === "string") {
      if (operator !== "eq" && operator !== "neq") {
        return null;
      }
      const spatialType = column.nativeType.toLowerCase().split("(")[0].trim();
      const searchValue = val.trim();
      if (!searchValue) {
        return null;
      }
      const spatialExpr =
        spatialType === "point"
          ? `REPLACE(CAST(${col} AS TEXT), ',', ', ')`
          : `CAST(${col} AS TEXT)`;
      return {
        sql: `${spatialExpr} ${operator === "neq" ? "<>" : "="} $${paramIndex}`,
        params: [searchValue],
      };
    }
    if (column.category === "json" && typeof val === "string") {
      const normalizedJson = normalizeJsonFilterValue(val);
      if (normalizedJson !== null) {
        if (operator === "eq") {
          return {
            sql: `(${col})::jsonb = $${paramIndex}::jsonb`,
            params: [normalizedJson],
          };
        }
        if (operator === "neq") {
          return {
            sql: `(${col})::jsonb <> $${paramIndex}::jsonb`,
            params: [normalizedJson],
          };
        }
      }
      if (operator === "like" || operator === "ilike") {
        const searchValue = val.trim();
        if (!searchValue) {
          return null;
        }
        return {
          sql: `CAST(${col} AS TEXT) ILIKE $${paramIndex}`,
          params: [`%${searchValue}%`],
        };
      }
    }
    if (column.category === "date" && typeof val === "string") {
      if (operator === "like" || operator === "ilike") {
        const normalized = this.normalizeFilterValue(column, "eq", val);
        const searchValue =
          typeof normalized === "string" && normalized.trim() !== ""
            ? normalized.trim()
            : val;
        return {
          sql: `CAST(${col} AS TEXT) ILIKE $${paramIndex}`,
          params: [`%${searchValue}%`],
        };
      }
      if (operator === "between" && Array.isArray(val)) {
        return {
          sql: `${col} BETWEEN $${paramIndex}::date AND $${paramIndex + 1}::date`,
          params: [val[0], val[1]],
        };
      }
      if (operator === "in") {
        const parts = val
          .split(",")
          .map((part) => part.trim())
          .filter(Boolean);
        const placeholders = parts
          .map((_, index) => `$${paramIndex + index}::date`)
          .join(", ");
        return { sql: `${col} IN (${placeholders})`, params: parts };
      }
      const sqlOp = this.sqlOperator(operator);
      return {
        sql: `${col} ${sqlOp} $${paramIndex}::date`,
        params: [val],
      };
    }
    if (column.category === "datetime" || column.category === "time") {
      if (operator === "like" || operator === "ilike") {
        const v = typeof val === "string" ? val : val[0];
        const searchValue = normalizeTemporalSearchValue(v);
        return {
          sql: `CAST(${col} AS TEXT) ILIKE $${paramIndex}`,
          params: [`%${searchValue}%`],
        };
      }
      const castType = postgresTemporalCastType(column);
      if (operator === "between" && Array.isArray(val)) {
        const startValue = normalizePostgresTemporalValue(val[0]);
        const endValue = normalizePostgresTemporalValue(val[1]);
        return {
          sql: `${col} BETWEEN $${paramIndex}::${castType} AND $${paramIndex + 1}::${castType}`,
          params: [startValue, endValue],
        };
      }
      if (operator === "in" && typeof val === "string") {
        const parts = val
          .split(",")
          .map((part) => normalizePostgresTemporalValue(part))
          .filter(Boolean);
        const placeholders = parts
          .map((_, index) => `$${paramIndex + index}::${castType}`)
          .join(", ");
        return { sql: `${col} IN (${placeholders})`, params: parts };
      }
      if (typeof val === "string") {
        const sqlOp = operator === "neq" ? "<>" : this.sqlOperator(operator);
        return {
          sql: `${col} ${sqlOp} $${paramIndex}::${castType}`,
          params: [normalizePostgresTemporalValue(val)],
        };
      }
    }
    if (column.category === "interval") {
      if (typeof val !== "string") {
        return null;
      }
      if (operator === "eq" || operator === "neq") {
        const intervalValue = val.trim();
        if (!intervalValue) {
          return null;
        }
        return {
          sql: `${col} ${operator === "neq" ? "<>" : "="} $${paramIndex}::interval`,
          params: [intervalValue],
        };
      }
      return null;
    }
    if (
      this.isNumericCategory(column.category) &&
      typeof val === "string" &&
      !Number.isNaN(Number(val)) &&
      val !== ""
    ) {
      const ct = column.nativeType.toLowerCase().split("(")[0].trim();
      const sqlOp = this.sqlOperator(operator);
      if (column.category === "decimal") {
        return { sql: `${col} ${sqlOp} $${paramIndex}`, params: [val] };
      }
      if (
        column.category === "float" &&
        (operator === "eq" || operator === "neq")
      ) {
        const numericValue = Number(val);
        const tolerance = approximateNumericFilterTolerance(val);
        const deltaExpr = `ABS((${col})::double precision - $${paramIndex}::double precision)`;
        const toleranceExpr =
          `GREATEST($${paramIndex + 1}::double precision, ` +
          `ABS($${paramIndex + 2}::double precision) * $${paramIndex + 3}::double precision)`;
        return {
          sql:
            operator === "neq"
              ? `${deltaExpr} >= ${toleranceExpr}`
              : `${deltaExpr} < ${toleranceExpr}`,
          params: [numericValue, tolerance, numericValue, tolerance],
        };
      }
      if (ct === "bigint" && /^-?\d+$/.test(val)) {
        return { sql: `${col} ${sqlOp} $${paramIndex}`, params: [BigInt(val)] };
      }
      return { sql: `${col} ${sqlOp} $${paramIndex}`, params: [Number(val)] };
    }
    if (operator === "between" && Array.isArray(val)) {
      return {
        sql: `${col} BETWEEN $${paramIndex} AND $${paramIndex + 1}`,
        params: [val[0], val[1]],
      };
    }
    if (operator === "in" && typeof val === "string") {
      const parts = val.split(",").map((s) => s.trim());
      const placeholders = parts.map((_, i) => `$${paramIndex + i}`).join(", ");
      return { sql: `${col} IN (${placeholders})`, params: parts };
    }
    const v = typeof val === "string" ? val : val[0];
    const finalVal = normalizeTemporalSearchValue(v);
    return {
      sql: `CAST(${col} AS TEXT) ILIKE $${paramIndex}`,
      params: [`%${finalVal}%`],
    };
  }
}
