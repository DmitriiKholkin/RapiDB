import { isIP } from "node:net";
import type { PoolClient } from "pg";
import { Pool, types as pgTypes } from "pg";
import type { DdlOnlyDbObjectKind } from "../../shared/dbObjectKinds";
import type { OperationCancellationContext } from "../../shared/safetyContracts";
import type { ConnectionConfig } from "../connectionManager";
import { getSshTcpForwardTransport } from "../driverRuntimeConfig";
import {
  type ResolvedConnectionTlsSettings,
  resolveConnectionTlsSettings,
} from "../services/connectionTls";
import { SqlTlsSocket } from "../services/sqlTlsSocket";
import { serializeArrayPreservingRawTokens } from "../utils/arraySerialization";
import {
  parseJsonPreservingRawNumbers,
  serializeCanonicalJson,
} from "../utils/jsonCanonical";
import { logger } from "../utils/logger";
import { jsonArrayLiteralToPgArrayLiteral } from "../utils/postgresArrayLiteral";
import {
  hasPostgresArrayBounds,
  parsePostgresArrayLiteral,
  parsePostgresBooleanArrayElement,
} from "../utils/postgresArrayParser";
import {
  BaseDBDriver,
  formatDatetimeForDisplay,
  isoToLocalDateStr,
  normalizeSqlDatetimeOffsetSpacing,
} from "./BaseDBDriver";
import { queryCollectionLimit } from "./boundedQueryRows";
import { literalContainsPattern } from "./literalContains";
import { BoundedPostgresQuery } from "./postgresBoundedQuery";
import {
  type IndexedPlaceholderOptions,
  indexedPlaceholderOffsets,
  replaceIndexedPlaceholders,
} from "./sqlPlaceholders";
import {
  type DriverTimeoutSettingsProvider,
  throwIfTransactionCancelled,
} from "./timeout";
import {
  TransactionIdentityStore,
  verifyTransaction,
} from "./transactionVerification";
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
  QueryExecutionOptions,
  QueryResult,
  SchemaInfo,
  TableInfo,
  TypeCategory,
  ValueSemantics,
} from "./types";
import {
  assertTransactionAffectedRows,
  DATETIME_SQL_RE,
  ISO_DATETIME_RE,
  NULL_SENTINEL,
} from "./types";

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
const POSTGRES_POOL_MAX = 5;
// `pg_attribute.attgenerated` was added in PostgreSQL 12. Reading the row as
// JSON keeps this projection parseable on PostgreSQL 10/11, where the key is
// simply absent.
const PG_GENERATED_KIND_EXPRESSION = "NULLIF(to_jsonb(a)->>'attgenerated', '')";

interface PostgresPoolWaitOperation {
  cancelled: boolean;
  wakePoolWait?: () => void;
}

interface PostgresQueryOperation extends PostgresPoolWaitOperation {
  requestToken?: number;
  client?: PoolClient;
}

const PG_OID_DATE = 1082;
const PG_OID_MONEY = 790;
const PG_OID_NUMERIC = 1700;
const PG_OID_TIMESTAMP = 1114;
const PG_OID_TIMESTAMPTZ = 1184;
const PG_OID_JSON = 114;
const PG_OID_JSONB = 3802;
const PG_ARRAY_OIDS = [
  1000, 1001, 1002, 1003, 1005, 1007, 1008, 1009, 1015, 1016, 1021, 1022, 1028,
  1231,
];
pgTypes.setTypeParser(PG_OID_DATE, (val: string) => val);
pgTypes.setTypeParser(PG_OID_MONEY, (val: string) => val);
pgTypes.setTypeParser(PG_OID_NUMERIC, (val: string) => val);
pgTypes.setTypeParser(PG_OID_TIMESTAMP, (val: string) => val);
pgTypes.setTypeParser(PG_OID_TIMESTAMPTZ, (val: string) => val);
pgTypes.setTypeParser(PG_OID_JSON, (val: string) => val);
pgTypes.setTypeParser(PG_OID_JSONB, (val: string) => val);
for (const oid of PG_ARRAY_OIDS) {
  pgTypes.setTypeParser(oid, (value: string) =>
    parsePostgresArrayLiteral(
      value,
      oid === 1000 ? parsePostgresBooleanArrayElement : undefined,
    ),
  );
}

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
  return trimmed;
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
    return literalContainsPattern(
      normalizedSql
        .replace(/(\.\d*?[1-9])0+(?=[Zz+-]|$)/, "$1")
        .replace(/\.0+(?=[Zz+-]|$)/, "")
        .replace(/[zZ]$/, "")
        .replace(/[+-]\d{2}(?::?\d{2})?$/, ""),
    ).replace(" ", "%");
  }
  return literalContainsPattern(trimmed);
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
function _canonicalizePostgresTemporalPersistedValue(
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
function _isLikelyPostgresAutoUpdatedTemporalColumn(
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

function postgresArrayElementKind(
  nativeType: string,
): "numeric" | "boolean" | "other" {
  // describeTable uses catalog namespaces to always qualify user-defined array
  // element types. Built-ins keep format_type's canonical names and typmods.
  // Also accept pg's internal _type aliases, but never strip an arbitrary schema
  // or quotes: an enum named audit.numeric must remain a string element type.
  const type = nativeType.trim().toLowerCase();
  let elementType: string;
  if (type.endsWith("[]")) {
    elementType = type.replace(/(?:\[\])+$/, "");
  } else if (type.startsWith("_")) {
    elementType = type.slice(1);
  } else {
    return "other";
  }
  elementType = elementType
    .replace(/^pg_catalog\./, "")
    .replace(/\(\s*\d+(?:\s*,\s*-?\d+)?\s*\)$/, "")
    .trim();
  if (["boolean", "bool"].includes(elementType)) return "boolean";
  if (
    [
      "smallint",
      "integer",
      "bigint",
      "int2",
      "int4",
      "int8",
      "numeric",
      "decimal",
      "real",
      "double precision",
      "float4",
      "float8",
      "oid",
      "xid",
      "cid",
    ].includes(elementType)
  )
    return "numeric";
  return "other";
}

function normalizePostgresArrayTextForDisplay(
  value: string,
  nativeType: string,
): string {
  const trimmed = value.trim();
  if (trimmed === "") {
    return value;
  }
  if (trimmed.startsWith("{") || hasPostgresArrayBounds(trimmed)) {
    const parsed = parsePostgresArrayLiteral(
      trimmed,
      postgresArrayElementKind(nativeType) === "boolean"
        ? parsePostgresBooleanArrayElement
        : undefined,
    );
    if (Array.isArray(parsed)) {
      return serializeArrayPreservingRawTokens(
        parsed,
        postgresArrayElementKind(nativeType) === "numeric",
      );
    }
    return value;
  }
  if (trimmed.startsWith("[")) {
    const parsed = parseJsonPreservingRawNumbers(trimmed);
    if (parsed !== undefined && Array.isArray(parsed)) {
      return serializeCanonicalJson(parsed);
    }
    return value;
  }
  return value;
}

function normalizePostgresBooleanInput(value: unknown): unknown {
  if (value === 1) return true;
  if (value === 0) return false;
  if (typeof value !== "string") return value;
  const token = value.trim().toLowerCase();
  if (token === "") return value;
  // boolin accepts case-insensitive unique prefixes. "o" is ambiguous.
  const truthy = ["true", "yes", "on", "1"].some((word) =>
    word.startsWith(token),
  );
  const falsy = ["false", "no", "off", "0"].some((word) =>
    word.startsWith(token),
  );
  return truthy !== falsy ? truthy : value;
}

function normalizePostgresBooleanArrayInput(value: unknown): unknown {
  if (Array.isArray(value))
    return value.map(normalizePostgresBooleanArrayInput);
  if (value === null) return null;
  const normalized = normalizePostgresBooleanInput(value);
  if (typeof normalized === "boolean") return normalized;
  throw new Error("Invalid PostgreSQL boolean array input element");
}

function normalizePostgresUuidInput(value: unknown): unknown {
  if (typeof value !== "string") return value;
  const token = value.trim().replace(/^\{(.*)\}$/, "$1");
  // uuid_in allows optional hyphens after each four hex digits and braces.
  if (!/^[0-9a-f]{4}(?:-?[0-9a-f]{4}){7}$/i.test(token)) return value;
  const hex = token.replace(/-/g, "").toLowerCase();
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

function canonicalizePostgresFloatSpecial(
  value: unknown,
): { canonical: string } | null {
  if (typeof value === "number" && !Number.isFinite(value)) {
    return { canonical: String(value) };
  }
  if (typeof value !== "string") return null;
  const token = value.trim().toLowerCase();
  // PostgreSQL's native float input also accepts signed NaN and strtod
  // payload forms. Payload/sign do not survive the server's NaN value.
  if (/^[+-]?nan(?:\([a-z0-9_]*\))?$/.test(token)) {
    return { canonical: "NaN" };
  }
  if (/^[+-]?inf(?:inity)?$/.test(token)) {
    return { canonical: token.startsWith("-") ? "-Infinity" : "Infinity" };
  }
  return null;
}

export class PostgresDriver extends BaseDBDriver {
  protected override getQueryEditorSqlDialect() {
    return "postgresql" as const;
  }

  private pool: Pool | null = null;
  private readonly databasePools = new Map<string, Pool>();
  private readonly pendingPools = new Set<Pool>();
  private readonly catalogSessions = new Map<
    Pool,
    { client?: PoolClient; cancelled: boolean }
  >();
  private readonly poolClosures = new WeakMap<Pool, Promise<void>>();
  private connectionEpoch = 0;
  private connectionAttempt: Promise<void> | null = null;
  private connectionAbortController?: AbortController;
  private publishedConnectionAttempt: Promise<void> | null = null;
  private readonly config: ConnectionConfig;
  private tlsSettings: ResolvedConnectionTlsSettings | undefined;
  private _connected = false;
  private connectedDatabaseName = "";
  private timeoutRecoveryInFlight: Promise<void> | null = null;
  private readonly activeTransactionClients = new Set<PoolClient>();
  private readonly activeQueryClients = new Set<PoolClient>();
  private readonly activeQueryOperations = new Set<PostgresQueryOperation>();
  private requirePool(database?: string): Pool {
    if (!this.pool) {
      throw new Error("[RapiDB] PostgreSQL connection is not open");
    }
    if (
      !database ||
      database === this.connectedDatabaseName ||
      database === this.config.database
    ) {
      return this.pool;
    }
    let pool = this.databasePools.get(database);
    if (!pool) {
      pool = this.createPool(database);
      pool.on("error", (error) => {
        logger.error(`PostgreSQL pool error (${database})`, error);
      });
      // Publish synchronously: concurrent callers share this database's pool,
      // never replace the connection/query editor's default pool.
      this.databasePools.set(database, pool);
    }
    return pool;
  }
  private createPool(
    database: string,
    max = 5,
    applicationName?: string,
  ): Pool {
    const tlsSettings = this.tlsSettings;
    const forwardedTransport = getSshTcpForwardTransport(this.config);
    const dbOperationTimeoutMs = this.getDbOperationTimeoutMs();
    const endpointHost = forwardedTransport?.localHost ?? this.config.host;
    const endpointPort = forwardedTransport?.localPort ?? this.config.port;
    const tlsIdentity = tlsSettings?.servername?.replace(/^\[|\]$/g, "");
    return new Pool({
      // pg overwrites ssl.servername with a DNS config.host. Route the socket
      // separately so both SNI and verification use the intended identity.
      host: tlsIdentity ?? endpointHost,
      port: endpointPort,
      stream: tlsIdentity
        ? () =>
            new SqlTlsSocket(
              endpointHost || "localhost",
              endpointPort ?? 5432,
              tlsIdentity,
            )
        : undefined,
      database,
      user: this.config.username,
      password: this.config.password,
      max,
      ...(applicationName ? { application_name: applicationName } : {}),
      keepAlive: true,
      keepAliveInitialDelayMillis: 60000,
      connectionTimeoutMillis: this.getConnectionTimeoutMs(),
      query_timeout: dbOperationTimeoutMs,
      statement_timeout: dbOperationTimeoutMs,
      idleTimeoutMillis: 30000,
      ssl: tlsSettings
        ? {
            rejectUnauthorized: tlsSettings.rejectUnauthorized,
            servername:
              tlsIdentity && isIP(tlsIdentity)
                ? undefined
                : tlsSettings.servername,
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
    return run(this.requirePool(database));
  }

  private async withCatalogClient<T>(
    database: string,
    run: (client: PoolClient) => Promise<T>,
  ): Promise<T> {
    // Only check the editor connection here; never borrow its backend. A pool
    // checkout may still contain an editor's explicit BEGIN and pending writes.
    this.requirePool();
    const epoch = this.connectionEpoch;
    const pool = this.createPool(
      database || this.connectedDatabaseName || this.config.database || "",
      1,
      "RapiDB catalog DDL",
    );
    const session: { client?: PoolClient; cancelled: boolean } = {
      cancelled: false,
    };
    this.catalogSessions.set(pool, session);
    pool.on("error", (error) =>
      logger.error("PostgreSQL catalog pool error", error),
    );
    let failed = false;
    try {
      const client = await pool.connect();
      if (session.cancelled || epoch !== this.connectionEpoch) {
        client.release(true);
        throw new Error("[RapiDB] PostgreSQL catalog session cancelled");
      }
      session.client = client;
      const result = await run(client);
      this.assertConnectionEpoch(epoch);
      return result;
    } catch (error) {
      failed = true;
      throw error;
    } finally {
      // Destroy even on success: this backend belongs to this DDL request only.
      // disconnect() may have already destroyed it while a query was pending.
      const client = session.client;
      session.client = undefined;
      try {
        client?.release(true);
        const closing = this.closePool(pool);
        if (failed)
          await closing.catch((error) =>
            logger.error("PostgreSQL catalog cleanup error", error),
          );
        else await closing;
      } finally {
        this.catalogSessions.delete(pool);
      }
    }
  }
  constructor(
    config: ConnectionConfig,
    timeoutSettingsProvider?: DriverTimeoutSettingsProvider,
  ) {
    super(timeoutSettingsProvider);
    this.config = config;
  }
  connect(): Promise<void> {
    if (this.connectionAttempt) return this.connectionAttempt;
    const epoch = ++this.connectionEpoch;
    const abortController = new AbortController();
    this.connectionAbortController = abortController;
    this._connected = false;
    this.connectedDatabaseName = "";
    this.publishedConnectionAttempt = null;
    const cleanup = this.closeOwnedPools().catch(() => undefined);
    const attempt: Promise<void> = cleanup
      .then(() => this.openConnection(epoch, attempt, abortController.signal))
      .finally(() => {
        if (this.connectionAttempt === attempt) this.connectionAttempt = null;
        if (this.connectionAbortController === abortController) {
          this.connectionAbortController = undefined;
        }
      });
    this.connectionAttempt = attempt;
    return attempt;
  }

  private assertConnectionEpoch(epoch: number): void {
    if (epoch !== this.connectionEpoch) {
      throw new DOMException(
        "[RapiDB] PostgreSQL connection attempt cancelled",
        "AbortError",
      );
    }
  }

  private async openConnection(
    epoch: number,
    attempt: Promise<void>,
    signal: AbortSignal,
  ): Promise<void> {
    this.assertConnectionEpoch(epoch);
    const tlsSettings = await resolveConnectionTlsSettings(this.config, signal);
    this.assertConnectionEpoch(epoch);
    this.tlsSettings = tlsSettings;
    const pool = this.createPool(this.config.database ?? "");
    this.pendingPools.add(pool);
    pool.on("error", (err) => {
      logger.error("PostgreSQL pool error", err);
      if (epoch === this.connectionEpoch && this.pool === pool) {
        this._connected = false;
      }
    });
    try {
      let databaseName: string;
      const client = await pool.connect();
      try {
        this.assertConnectionEpoch(epoch);
        const databaseRes = await client.query<{ name: string }>(
          `SELECT current_database() AS name`,
        );
        this.assertConnectionEpoch(epoch);
        databaseName = databaseRes.rows[0]?.name ?? this.config.database ?? "";
      } finally {
        if (epoch !== this.connectionEpoch) client.release(true);
        else client.release();
      }
      this.assertConnectionEpoch(epoch);
      this.pendingPools.delete(pool);
      this.pool = pool;
      this.connectedDatabaseName = databaseName;
      this.publishedConnectionAttempt = attempt;
      this._connected = true;
    } catch (error) {
      this.pendingPools.delete(pool);
      // A stale attempt owns only its pool; it cannot reset a newer connection.
      await this.closePool(pool).catch(() => undefined);
      throw error;
    }
  }

  /** Timeout cleanup must not disconnect a later, unrelated connect attempt. */
  async cancelConnectionAttempt(attempt: Promise<unknown>): Promise<void> {
    if (
      attempt === this.connectionAttempt ||
      attempt === this.publishedConnectionAttempt
    ) {
      await this.disconnect();
    }
  }

  async disconnect(): Promise<void> {
    ++this.connectionEpoch;
    this.connectionAbortController?.abort();
    this.connectionAbortController = undefined;
    this.connectionAttempt = null;
    this.publishedConnectionAttempt = null;
    this._connected = false;
    this.connectedDatabaseName = "";
    this.tlsSettings = undefined;
    await this.closeOwnedPools();
  }

  private closePool(pool: Pool): Promise<void> {
    let closing = this.poolClosures.get(pool);
    if (!closing) {
      closing = Promise.resolve().then(() => pool.end());
      this.poolClosures.set(pool, closing);
    }
    return closing;
  }

  private async closeOwnedPools(): Promise<void> {
    const pools = new Set([
      ...this.databasePools.values(),
      ...this.pendingPools,
      ...this.catalogSessions.keys(),
    ]);
    for (const session of this.catalogSessions.values()) {
      session.cancelled = true;
      const client = session.client;
      session.client = undefined;
      client?.release(true);
    }
    this.catalogSessions.clear();
    if (this.pool) pools.add(this.pool);
    this.pool = null;
    this.databasePools.clear();
    this.pendingPools.clear();
    // Start cleanup for every pool even if one end() fails.
    const results = await Promise.allSettled(
      [...pools].map((pool) => this.closePool(pool)),
    );
    const failure = results.find((result) => result.status === "rejected");
    if (failure?.status === "rejected") throw failure.reason;
  }

  async cancelCurrentOperation(
    context?: OperationCancellationContext,
  ): Promise<void> {
    if (context?.operationName === "query") {
      if (context.requestToken === undefined) {
        return;
      }
      for (const operation of this.activeQueryOperations) {
        if (operation.requestToken !== context.requestToken) {
          continue;
        }
        operation.cancelled = true;
        operation.wakePoolWait?.();
        if (operation.client) {
          const client = operation.client;
          operation.client = undefined;
          if (this.activeQueryClients.delete(client)) client.release(true);
        }
      }
      return;
    }

    if (context?.operationName === "runTransaction" || context === undefined) {
      const hadTransactionClient = this.activeTransactionClients.size > 0;
      for (const client of [...this.activeTransactionClients]) {
        this.activeTransactionClients.delete(client);
        client.release(true);
      }
      if (context === undefined && !hadTransactionClient) {
        await this.recycleConnectionAfterTimeout();
      }
    }
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
    database: string,
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
    const res = await this.requirePool(database).query<DescribeTableRow>(
      `SELECT
         a.attname                                AS column_name,
         CASE
           WHEN array_type.typcategory = 'A'
             AND element_ns.oid <> 'pg_catalog'::pg_catalog.regnamespace
             AND pg_catalog.pg_type_is_visible(element_type.oid)
           THEN pg_catalog.format('%I.%s[]', element_ns.nspname,
                  pg_catalog.format_type(element_type.oid, a.atttypmod))
           ELSE pg_catalog.format_type(a.atttypid, a.atttypmod)
         END                                      AS data_type,
         NOT a.attnotnull                         AS is_nullable,
         pg_get_expr(d.adbin, d.adrelid)         AS column_default,
          ${PG_GENERATED_KIND_EXPRESSION}           AS generated_kind,
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
       LEFT JOIN pg_catalog.pg_type array_type ON array_type.oid = a.atttypid
       LEFT JOIN pg_catalog.pg_type element_type ON element_type.oid = array_type.typelem
       LEFT JOIN pg_catalog.pg_namespace element_ns ON element_ns.oid = element_type.typnamespace
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
      const mappedGeneratedKind =
        generatedKind === "s"
          ? "stored"
          : generatedKind === "v"
            ? "virtual"
            : undefined;
      const isComputed = mappedGeneratedKind !== undefined;
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
        generatedKind: mappedGeneratedKind,
        isPersisted:
          mappedGeneratedKind === undefined
            ? undefined
            : mappedGeneratedKind === "stored",
        isPrimaryKey: isPgTrue(r.is_pk),
        primaryKeyOrdinal: toOptionalNumber(r.pk_ordinal),
        isForeignKey: isPgTrue(r.is_fk),
      };
    });
  }
  async query(
    sql: string,
    params?: unknown[],
    operationContext?: QueryExecutionOptions,
  ): Promise<QueryResult> {
    const hardCap = queryCollectionLimit(operationContext?.hardCap);
    type PgArrayField = {
      name: string;
    };
    type PgArrayQueryResult = {
      fields?: PgArrayField[];
      rows?: unknown[][];
      rowCount?: number | null;
      command?: string;
    };
    const start = Date.now();
    const operation: PostgresQueryOperation = {
      cancelled: false,
      requestToken: operationContext?.requestToken,
    };
    this.activeQueryOperations.add(operation);
    let client: PoolClient | undefined;
    let output: QueryResult | undefined;
    let queryError: unknown;
    let queryFailed = false;
    let rollbackError: unknown;
    let rollbackFailed = false;
    try {
      const pool = this.requirePool(operationContext?.database);
      client = await this.acquirePoolClient(operation, pool, () => {
        if (operation.cancelled) {
          throw new Error("PostgreSQL query cancelled before execution.");
        }
      });
      operation.client = client;
      if (operation.cancelled) {
        operation.client = undefined;
        client.release(true);
        throw new Error("PostgreSQL query cancelled before execution.");
      }
      this.activeQueryClients.add(client);
      if (operationContext?.readOnly) {
        await client.query("BEGIN READ ONLY");
      }
      const queryClient = client;
      let truncated = false;
      const res =
        hardCap === undefined
          ? await client.query({
              text: sql,
              values: params ?? [],
              rowMode: "array",
            })
          : await new Promise<PgArrayQueryResult>((resolve, reject) => {
              const query = new BoundedPostgresQuery(
                sql,
                params ?? [],
                hardCap,
                (error, result) => {
                  if (error) {
                    reject(error);
                    return;
                  }
                  truncated = query.retained.truncated;
                  resolve({
                    ...result,
                    rows: query.retained.rows,
                    // SHOW/EXPLAIN command tags omit a row count. The stream
                    // still observes the complete result, not just its sample.
                    rowCount: result?.rowCount ?? query.retained.rowCount,
                  });
                },
              );
              queryClient.query(query);
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
      const queryResult = {
        columns,
        rows,
        rowCount: result.rowCount ?? rawRows.length,
        executionTimeMs,
        ...(hardCap !== undefined ? { truncated } : {}),
        ...(hardCap !== undefined &&
        ["INSERT", "UPDATE", "DELETE", "MERGE"].includes(result.command ?? "")
          ? { affectedRows: result.rowCount ?? 0 }
          : {}),
      };
      output = queryResult;
    } catch (error) {
      queryFailed = true;
      queryError = error;
      // A read timeout can fire before ReadyForQuery. Never return that socket
      // to the pool while the server is still emitting the timed-out result.
      if (
        hardCap !== undefined &&
        client &&
        this.activeQueryClients.delete(client)
      )
        client.release(true);
    } finally {
      try {
        if (
          client &&
          this.activeQueryClients.has(client) &&
          operationContext?.readOnly
        ) {
          try {
            await client.query("ROLLBACK");
          } catch (error) {
            // Cancellation may already have discarded this client while rollback
            // was pending. Only the owner that removes it may release it.
            rollbackFailed = true;
            rollbackError = error;
            operation.client = undefined;
            if (this.activeQueryClients.delete(client)) client.release(true);
          }
        }
        if (client && this.activeQueryClients.delete(client)) {
          operation.client = undefined;
          client.release();
        }
      } finally {
        operation.client = undefined;
        this.activeQueryOperations.delete(operation);
      }
    }
    if (rollbackFailed) {
      throw new AggregateError(
        queryFailed ? [queryError, rollbackError] : [rollbackError],
        "PostgreSQL read-only query rollback failed; the connection was discarded.",
      );
    }
    if (queryFailed) throw queryError;
    return output as QueryResult;
  }
  override getCapabilities() {
    return { ...super.getCapabilities(), boundedQueryResults: true };
  }

  private async acquirePoolClient(
    operation: PostgresPoolWaitOperation,
    pool: Pool,
    assertActive: () => void,
  ): Promise<PoolClient> {
    while (true) {
      assertActive();
      if (
        pool !== this.pool &&
        ![...this.databasePools.values()].includes(pool)
      ) {
        throw new Error("[RapiDB] PostgreSQL connection is not open");
      }
      if (
        (pool.waitingCount ?? 0) === 0 &&
        ((pool.idleCount ?? 0) > 0 ||
          (pool.totalCount ?? 0) < POSTGRES_POOL_MAX)
      ) {
        // Begin checkout in the same turn as the capacity check. Awaiting a
        // separate readiness promise here leaves a race where another pool
        // user can take the slot and strand this request in pg-pool's
        // non-cancellable checkout queue.
        const client = await pool.connect();
        try {
          assertActive();
          if (
            pool !== this.pool &&
            ![...this.databasePools.values()].includes(pool)
          ) {
            throw new Error("[RapiDB] PostgreSQL connection is not open");
          }
        } catch (error) {
          client.release(true);
          throw error;
        }
        return client;
      }
      await new Promise<void>((resolve) => {
        let timer: ReturnType<typeof setTimeout>;
        const wake = () => {
          clearTimeout(timer);
          if (operation.wakePoolWait === wake) {
            operation.wakePoolWait = undefined;
          }
          resolve();
        };
        timer = setTimeout(wake, 25);
        operation.wakePoolWait = wake;
      });
    }
  }
  async getIndexes(
    database: string,
    schema: string,
    table: string,
  ): Promise<IndexMeta[]> {
    type IndexRow = {
      name: string;
      unique: boolean | number | string;
      primary: boolean | number | string;
      column: string;
    };
    const res = await this.requirePool(database).query<IndexRow>(
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
    database: string,
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
    const res = await this.requirePool(database).query<ForeignKeyRow>(
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
    const res = await this.requirePool(database).query<{
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
    database: string,
    schema: string,
    table: string,
  ): Promise<import("./types").TriggerMeta[] | null> {
    const res = await this.requirePool(database).query<{
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
    database: string,
    schema: string,
    table: string,
    constraintName: string,
  ): Promise<string> {
    const res = await this.requirePool(database).query<{ ddl: string }>(
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
    database: string,
    schema: string,
    table: string,
    indexName: string,
  ): Promise<string> {
    const res = await this.requirePool(database).query<{ ddl: string }>(
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
    database: string,
    schema: string,
    table: string,
    triggerName: string,
  ): Promise<string> {
    const res = await this.requirePool(database).query<{ ddl: string }>(
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
    database: string,
    schema: string,
    table: string,
  ): Promise<string> {
    return this.withCatalogClient(database, (client) =>
      this.readCreateTableDDL(client, schema, table),
    );
  }

  private async readCreateTableDDL(
    client: PoolClient,
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
    const kindRes = await client.query<{
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
      const res = await client.query<{
        def: string;
      }>(
        `SELECT 'CREATE OR REPLACE VIEW ' || quote_ident(n.nspname) || '.' || quote_ident(c.relname) || ' AS\n' ||
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
      const res = await client.query<{
        def: string;
      }>(
        `SELECT 'CREATE MATERIALIZED VIEW ' || quote_ident(n.nspname) || '.' || quote_ident(c.relname) || ' AS\n' ||
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
    // Deparse with a catalog-only search path on an owned session. This makes
    // user-schema FK, type, default and CHECK dependencies schema-qualified,
    // without changing the pool's search path for subsequent operations.
    try {
      await client.query("BEGIN READ ONLY");
      await client.query("SET LOCAL search_path TO pg_catalog");
      const colRes = await client.query<DdlColumnRow>(
        `SELECT
         a.attname                                AS column_name,
         format_type(a.atttypid, a.atttypmod)    AS data_type,
         NOT a.attnotnull                         AS is_nullable,
         pg_get_expr(d.adbin, d.adrelid)         AS column_default,
          ${PG_GENERATED_KIND_EXPRESSION}           AS generated_kind,
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
      const constraintRes = await client.query<{
        constraint_name: string;
        definition: string;
      }>(
        `SELECT con.conname AS constraint_name,
              pg_get_constraintdef(con.oid, false) AS definition
        FROM pg_constraint con
        JOIN pg_class     c ON c.oid = con.conrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = $1 AND c.relname = $2
          AND con.contype IN ('p', 'c', 'u', 'f', 'x')
        ORDER BY CASE con.contype WHEN 'p' THEN 0 WHEN 'c' THEN 1
                   WHEN 'u' THEN 2 WHEN 'f' THEN 3 ELSE 4 END,
                 con.conname COLLATE "C", con.oid`,
        [schema, table],
      );
      const cols = colRes.rows.map((r) => {
        const columnName = r.column_name as string;
        const isComputed = r.generated_kind === "s" || r.generated_kind === "v";
        const nullable = isPgTrue(r.is_nullable);
        const notNull = !nullable ? " NOT NULL" : "";
        const identityClause = pgIdentityClause(r.identity_kind);
        if (isComputed && r.column_default) {
          const generatedStorage =
            r.generated_kind === "v" ? "VIRTUAL" : "STORED";
          return `  ${this.quoteIdentifier(columnName)} ${r.data_type} GENERATED ALWAYS AS (${r.column_default}) ${generatedStorage}${notNull}`;
        }
        const defClause =
          !identityClause && r.column_default
            ? ` DEFAULT ${r.column_default}`
            : "";
        return `  ${this.quoteIdentifier(columnName)} ${r.data_type}${identityClause}${notNull}${defClause}`;
      });
      for (const constraint of constraintRes.rows) {
        cols.push(
          `  CONSTRAINT ${this.quoteIdentifier(constraint.constraint_name)} ${constraint.definition}`,
        );
      }
      return `-- Reconstructed PostgreSQL table DDL (columns and catalog table constraints).\n-- Not a pg_dump schema backup: dependencies must already exist.\n-- Sequences, standalone indexes, triggers, security, storage and partition/inheritance definitions are not included.\nCREATE TABLE ${this.qualifiedTableName("", schema, table)} (\n${cols.join(",\n")}\n);`;
    } finally {
      // This client is request-scoped and is destroyed by withCatalogClient.
      // A failed cleanup must not replace either a completed DDL result or the
      // catalog query error that caused cleanup.
      try {
        await client.query("ROLLBACK");
      } catch {
        // The client is discarded by withCatalogClient in all cases.
      }
    }
  }
  async getObjectDefinition(
    database: string,
    schema: string,
    name: string,
    kind: DdlOnlyDbObjectKind,
  ): Promise<string | null> {
    if (kind === "sequence") {
      return this.getSequenceDefinition(database, schema, name);
    }
    return this.getTypeDefinition(database, schema, name);
  }
  async getRoutineDefinition(
    database: string,
    schema: string,
    name: string,
    _kind: "function" | "procedure",
    routineIdentity?: string,
  ): Promise<string> {
    const parsedIdentity = parsePostgresRoutineIdentity(routineIdentity);
    if (parsedIdentity) {
      const byOidRes = await this.requirePool(database).query<{
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

    const res = await this.requirePool(database).query<{
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
    database: string,
    schema: string,
    name: string,
  ): Promise<string | null> {
    const res = await this.requirePool(database).query<{
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
    database: string,
    schema: string,
    name: string,
  ): Promise<string | null> {
    const metaRes = await this.requirePool(database).query<{
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
      const labelsRes = await this.requirePool(database).query<{
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

    const attributesRes = await this.requirePool(database).query<{
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
    context?: import("./types").TransactionContext,
    scope?: import("./types").TransactionOptions,
  ): Promise<void> {
    throwIfTransactionCancelled(context);
    const waitOperation: PostgresPoolWaitOperation = { cancelled: false };
    let client: PoolClient | undefined;
    let transactionStarted = false;
    let discardTransactionClient = false;
    const cancel = () => {
      waitOperation.cancelled = true;
      waitOperation.wakePoolWait?.();
      if (client && this.activeTransactionClients.delete(client)) {
        client.release(true);
      }
    };
    context?.signal.addEventListener("abort", cancel, { once: true });
    try {
      throwIfTransactionCancelled(context);
      const acquired = await this.acquirePoolClient(
        waitOperation,
        this.requirePool(scope?.database),
        () => {
          if (waitOperation.cancelled) {
            throw new Error(
              "PostgreSQL transaction cancelled before execution.",
            );
          }
          throwIfTransactionCancelled(context);
        },
      );
      if (waitOperation.cancelled) {
        acquired.release(true);
        throwIfTransactionCancelled(context);
        throw new Error("PostgreSQL transaction cancelled before execution.");
      }
      client = acquired;
      this.activeTransactionClients.add(client);
      const transactionClient = client;
      throwIfTransactionCancelled(context);
      transactionStarted = true;
      await transactionClient.query("BEGIN");
      const identities = new TransactionIdentityStore();
      for (const [index, op] of operations.entries()) {
        throwIfTransactionCancelled(context);
        const res = await transactionClient.query(op.sql, op.params ?? []);
        assertTransactionAffectedRows(op, res.rowCount ?? 0);
        await identities.capture(
          index,
          op,
          res.rows,
          async (sql, params) =>
            (await transactionClient.query(sql, params)).rows,
        );
      }
      await verifyTransaction(
        this,
        identities.resolve(scope?.verifications),
        async (verification) => {
          const result = await transactionClient.query(
            verification.sql,
            verification.params,
          );
          return result.rows.map((row) =>
            Object.fromEntries(
              verification.values.map((_, index) => {
                const value = row[`__col_${index}`];
                return [
                  `__col_${index}`,
                  value !== null &&
                  typeof value === "object" &&
                  !(value instanceof Date) &&
                  isPointValue(value)
                    ? `(${String(value.x)}, ${String(value.y)})`
                    : value,
                ];
              }),
            ),
          );
        },
        context,
      );
      throwIfTransactionCancelled(context);
      await transactionClient.query("COMMIT");
    } catch (error) {
      if (client && transactionStarted) {
        try {
          await client.query("ROLLBACK");
        } catch (rollbackError) {
          discardTransactionClient = true;
          throw new AggregateError(
            [error, rollbackError],
            "PostgreSQL transaction and rollback failed; the connection was discarded.",
          );
        }
      }
      throw error;
    } finally {
      context?.signal.removeEventListener("abort", cancel);
      if (client && this.activeTransactionClients.delete(client)) {
        if (discardTransactionClient) client.release(true);
        else client.release();
      }
    }
  }
  mapTypeCategory(nativeType: string): TypeCategory {
    const ct = nativeType.toLowerCase().split("(")[0].trim();
    if (
      nativeType.toLowerCase().endsWith("[]") ||
      nativeType.toLowerCase().startsWith("_") ||
      ct === "array"
    )
      return "array";
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
  override buildOriginalValueComparison(
    column: ColumnTypeMeta,
    paramIndex: number,
  ): string {
    const name = this.quoteIdentifier(column.name);
    const type = column.nativeType.toLowerCase();
    if (type === "json" || type === "xml") {
      return `${name}::text = $${paramIndex}::text`;
    }
    if (
      column.category === "spatial" ||
      type === "json[]" ||
      type === "xml[]"
    ) {
      return `${name}::text = ($${paramIndex}::${column.nativeType})::text`;
    }
    return super.buildOriginalValueComparison(column, paramIndex);
  }
  protected override getQuestionMarkPlaceholderOptions(): IndexedPlaceholderOptions {
    // BaseDBDriver uses this options hook for indexed preview scanning too.
    return { dialect: "pg" };
  }
  materializePreviewColumnSql(
    sql: string,
    params: readonly unknown[] | undefined,
    columns: readonly (ColumnTypeMeta | undefined)[],
  ): string {
    if (!params || params.length === 0 || columns.length === 0) {
      return this.materializePreviewSql(sql, params);
    }
    const offsets = indexedPlaceholderOffsets(sql, "$", { dialect: "pg" });
    return replaceIndexedPlaceholders(sql, offsets, ({ index, text }) => {
      if (index < 0 || index >= params.length) {
        return text;
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
      return jsonArrayLiteralToPgArrayLiteral(value);
    }
    if (column.category === "json") {
      return value;
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
    if (typeof value === "string") {
      if (column.category === "json") {
        return value;
      }
      if (column.category === "array") {
        return normalizePostgresArrayTextForDisplay(value, column.nativeType);
      }
    }
    if (
      value !== null &&
      typeof value === "object" &&
      !(value instanceof Date)
    ) {
      if (column.category === "array" && Array.isArray(value)) {
        return serializeArrayPreservingRawTokens(
          value,
          postgresArrayElementKind(column.nativeType) === "numeric",
        );
      }
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
      const constraint = this.parseExactNumericConstraint(column.nativeType);
      // numeric(p) has an implicit scale of zero; unconstrained numeric does not.
      if (constraint.precision !== null && constraint.scale === null) {
        constraint.scale = 0;
      }
      return this.checkExactNumericPersistedEdit(
        column,
        expectedValue,
        constraint,
        options,
      );
    }
    if (column.category === "float") {
      if (canonicalizePostgresFloatSpecial(expectedValue)) {
        return this.checkNormalizedPersistedEdit(
          column,
          expectedValue,
          options,
          canonicalizePostgresFloatSpecial,
        );
      }
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
      return this.checkBooleanPersistedEdit(
        column,
        normalizePostgresBooleanInput(expectedValue),
        options,
      );
    }
    if (column.category === "binary") {
      return this.checkBinaryPersistedEdit(column, expectedValue, options);
    }
    if (column.category === "json") {
      return this.checkJsonPersistedEdit(column, expectedValue, options);
    }
    if (column.category === "uuid") {
      return this.checkUuidPersistedEdit(
        column,
        normalizePostgresUuidInput(expectedValue),
        options,
      );
    }
    if (column.category === "array") {
      let normalizedExpected = expectedValue;
      if (postgresArrayElementKind(column.nativeType) === "boolean") {
        try {
          if (Array.isArray(expectedValue)) {
            normalizedExpected =
              normalizePostgresBooleanArrayInput(expectedValue);
          } else if (
            typeof expectedValue === "string" &&
            /^[[{]/.test(expectedValue.trim())
          ) {
            // Input uses boolin's lexicon; actual server output keeps the
            // strict registered B03 parser and formatOutputValue rules.
            normalizedExpected = parsePostgresArrayLiteral(
              jsonArrayLiteralToPgArrayLiteral(expectedValue),
              normalizePostgresBooleanArrayInput,
            );
          }
        } catch {
          return {
            ok: false,
            shouldVerify: false,
            message: `Column "${column.name}" expects a valid PostgreSQL boolean array.`,
          };
        }
      } else if (typeof expectedValue === "string") {
        normalizedExpected = this.formatOutputValue(expectedValue, column);
      }
      // Arrays with user-defined element OIDs arrive from pg as raw array text.
      // Use the same catalog-qualified display rules for verification as reads.
      const normalizedOptions =
        options && typeof options.persistedValue === "string"
          ? {
              ...options,
              persistedValue: this.formatOutputValue(
                options.persistedValue,
                column,
              ),
            }
          : options;
      return this.checkJsonArrayPersistedEdit(
        column,
        normalizedExpected,
        normalizedOptions,
      );
    }
    if (
      column.category === "date" ||
      column.category === "time" ||
      column.category === "datetime"
    ) {
      return {
        ok: true,
        shouldVerify: false,
      };
    }
    if (column.category === "text") {
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
        sql: `to_jsonb(${col})::text ILIKE $${paramIndex} ESCAPE '!'`,
        params: [literalContainsPattern(arrayValue)],
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
        const jsonCast =
          column.nativeType.toLowerCase() === "jsonb" ? "jsonb" : "json";
        if (operator === "eq") {
          return {
            sql: `(${col})::${jsonCast} = $${paramIndex}::${jsonCast}`,
            params: [normalizedJson],
          };
        }
        if (operator === "neq") {
          return {
            sql: `(${col})::${jsonCast} <> $${paramIndex}::${jsonCast}`,
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
          sql: `CAST(${col} AS TEXT) ILIKE $${paramIndex} ESCAPE '!'`,
          params: [literalContainsPattern(searchValue)],
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
          sql: `CAST(${col} AS TEXT) ILIKE $${paramIndex} ESCAPE '!'`,
          params: [literalContainsPattern(searchValue)],
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
          sql: `CAST(${col} AS TEXT) ILIKE $${paramIndex} ESCAPE '!'`,
          params: [searchValue],
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
      if (ct === "bigint" && /^[+-]?\d+$/.test(val)) {
        return { sql: `${col} ${sqlOp} $${paramIndex}`, params: [BigInt(val)] };
      }
      if (column.category === "integer" && !/^[+-]?\d+$/.test(val)) {
        // Fractional/scientific input must be interpreted by NUMERIC, not
        // rounded through JS Number (or coerced to the column's integer type).
        return {
          sql: `${col} ${sqlOp} $${paramIndex}::numeric`,
          params: [val],
        };
      }
      return { sql: `${col} ${sqlOp} $${paramIndex}`, params: [Number(val)] };
    }
    if (operator === "between" && Array.isArray(val)) {
      return {
        sql: `${col} BETWEEN $${paramIndex}${column.category === "integer" && !/^[+-]?\d+$/.test(val[0]) ? "::numeric" : ""} AND $${paramIndex + 1}${column.category === "integer" && !/^[+-]?\d+$/.test(val[1]) ? "::numeric" : ""}`,
        params: [val[0], val[1]],
      };
    }
    if (operator === "in" && typeof val === "string") {
      const parts = val.split(",").map((s) => s.trim());
      const placeholders = parts
        .map(
          (part, i) =>
            `$${paramIndex + i}${column.category === "integer" && !/^[+-]?\d+$/.test(part) ? "::numeric" : ""}`,
        )
        .join(", ");
      return { sql: `${col} IN (${placeholders})`, params: parts };
    }
    if (operator !== "like" && operator !== "ilike") {
      throw new Error(
        `[RapiDB Filter] Column ${column.name} does not support ${operator} filters for ${column.category} values.`,
      );
    }
    const v = typeof val === "string" ? val : val[0];
    return {
      sql: `CAST(${col} AS TEXT) ILIKE $${paramIndex} ESCAPE '!'`,
      params: [literalContainsPattern(v)],
    };
  }
}
