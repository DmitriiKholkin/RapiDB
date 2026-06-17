import * as mssql from "mssql";
import type { DdlOnlyDbObjectKind } from "../../shared/dbObjectKinds";
import type { ConnectionConfig } from "../connectionManager";
import {
  getMssqlServerName,
  getSshTcpForwardTransport,
} from "../driverRuntimeConfig";
import { resolveConnectionTlsSettings } from "../services/connectionTls";
import { logger } from "../utils/logger";
import {
  BaseDBDriver,
  formatDatetimeForDisplay,
  hasExplicitTimezone,
  isoToLocalDateStr,
  normalizeSqlDatetimeOffsetSpacing,
} from "./BaseDBDriver";
import {
  formatHexSqlPreviewLiteral,
  formatSqlPreviewStringLiteral,
} from "./sqlPreviewLiterals";
import type { DriverTimeoutSettingsProvider } from "./timeout";
import type {
  ColumnMeta,
  ColumnTypeMeta,
  DatabaseInfo,
  DriverEntityManifest,
  FilterConditionResult,
  FilterOperator,
  PaginationResult,
  PersistedEditCheckOptions,
  PersistedEditCheckResult,
  QueryResult,
  SchemaInfo,
  TableInfo,
  TypeCategory,
  ValueSemantics,
} from "./types";
import {
  DATE_ONLY_RE,
  DATETIME_SQL_RE,
  ISO_DATETIME_RE,
  NULL_SENTINEL,
} from "./types";

const MSSQL_TEDIOUS_EXACT_NUMERIC_PATCH_KEY = Symbol.for(
  "rapidb.mssqlTediousExactNumericPatch",
);

type TediousReadValueResult = {
  value: unknown;
  offset: number;
};

type TediousValueParserModule = {
  readValue: (
    buf: Buffer,
    offset: number,
    metadata: {
      type?: { name?: string };
      precision?: number;
      scale?: number;
    },
    options: unknown,
  ) => TediousReadValueResult;
};

type TediousHelpersModule = {
  Result: new <T>(value: T, offset: number) => TediousReadValueResult;
  NotEnoughDataError: new (byteCount: number) => Error;
};

type MssqlSqlType = Parameters<mssql.Request["input"]>[1];
interface NamedRow {
  name: string;
}
interface ObjectRow {
  name: string;
  type: string;
}
interface DescribeColumnRow {
  COLUMN_NAME: string;
  DATA_TYPE: string;
  max_length: number;
  precision: number;
  scale: number;
  IS_NULLABLE: boolean | number;
  is_identity: boolean | number;
  is_computed: boolean | number;
  COMPUTED_DEFINITION: string | null;
  is_persisted: boolean | number;
  COLUMN_DEFAULT: string | null;
  IS_PK: number;
  PK_ORDINAL: number | null;
  IS_FK: number;
}
interface IndexRow {
  idx_name: string;
  col_name: string;
  is_unique: boolean | number;
  is_pk: boolean | number;
}
interface ForeignKeyRow {
  constraint_name: string;
  column_name: string;
  ref_schema: string;
  ref_table: string;
  ref_column: string;
}
interface DdlColumnRow {
  COLUMN_NAME: string;
  DATA_TYPE: string;
  max_length: number;
  precision: number;
  scale: number;
  IS_NULLABLE: boolean | number;
  is_identity: boolean | number;
  is_computed: boolean | number;
  COMPUTED_DEFINITION: string | null;
  is_persisted: boolean | number;
  COLUMN_DEFAULT: string | null;
  IS_PK: number;
  PK_ORDINAL: number | null;
}
interface ObjectTypeRow {
  TABLE_TYPE: string;
}
function canonicalizeMssqlBitPersistedEditValue(value: unknown): {
  canonical: string;
} | null {
  if (value === NULL_SENTINEL || value === null) {
    return { canonical: "__rapidb_null__" };
  }
  if (value === true || value === 1) {
    return { canonical: "1" };
  }
  if (value === false || value === 0) {
    return { canonical: "0" };
  }
  if (typeof value !== "string") {
    return null;
  }
  const normalized = value.trim().toLowerCase();
  if (normalized === "true" || normalized === "1") {
    return { canonical: "1" };
  }
  if (normalized === "false" || normalized === "0") {
    return { canonical: "0" };
  }
  return null;
}
interface RoutineDefinitionRow {
  def: string | null;
}
interface MssqlArrayColumnMeta {
  index: number;
  name: string;
  type: MssqlSqlType;
  scale?: number;
  precision?: number;
  nullable: boolean;
  identity: boolean;
  readOnly: boolean;
}
interface MssqlArrayResult extends mssql.IResult<unknown[]> {
  columns?: MssqlArrayColumnMeta[][];
}

const MSSQL_ENTITY_MANIFEST: DriverEntityManifest = {
  dbObjectKinds: ["table", "view", "function", "procedure", "sequence", "type"],
  tableSections: {
    columns: "supported",
    constraints: "supported",
    indexes: "supported",
    triggers: "supported",
  },
  tableSectionOverridesByObjectKind: {
    view: {
      constraints: "not_applicable",
      triggers: "not_applicable",
    },
  },
};

function ensureTediousBufferLength(
  buf: Buffer,
  offset: number,
  byteLength: number,
  helpers: TediousHelpersModule,
): void {
  if (buf.length < offset + byteLength) {
    throw new helpers.NotEnoughDataError(offset + byteLength);
  }
}

function readTediousUInt8(
  buf: Buffer,
  offset: number,
  helpers: TediousHelpersModule,
): TediousReadValueResult {
  ensureTediousBufferLength(buf, offset, 1, helpers);
  return new helpers.Result(buf.readUInt8(offset), offset + 1);
}

function readTediousUnsignedBigIntLE(
  buf: Buffer,
  offset: number,
  byteLength: number,
  helpers: TediousHelpersModule,
): TediousReadValueResult {
  ensureTediousBufferLength(buf, offset, byteLength, helpers);
  let value = 0n;
  for (let index = 0; index < byteLength; index += 1) {
    value |= BigInt(buf[offset + index]) << BigInt(index * 8);
  }
  return new helpers.Result(value, offset + byteLength);
}

function formatTwoDigits(value: number): string {
  return String(value).padStart(2, "0");
}

function formatMssqlYear(value: number): string {
  const sign = value < 0 ? "-" : "";
  const absolute = Math.abs(value);
  return `${sign}${String(absolute).padStart(4, "0")}`;
}

function normalizeMssqlYearPrefix(value: string): string {
  const dateOnlyMatch = /^(\d{1,4})-(\d{2})-(\d{2})$/.exec(value);
  if (dateOnlyMatch) {
    const [, year, month, day] = dateOnlyMatch;
    return `${year.padStart(4, "0")}-${month}-${day}`;
  }
  const dateTimeMatch =
    /^(\d{1,4})-(\d{2})-(\d{2})([ T]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?: ?(?:Z|[+-]\d{2}(?::?\d{2})?))?)$/i.exec(
      value,
    );
  if (dateTimeMatch) {
    const [, year, month, day, tail] = dateTimeMatch;
    return `${year.padStart(4, "0")}-${month}-${day}${tail}`;
  }
  return value;
}

function formatMssqlOffsetMinutes(offsetMinutes: number): string {
  const sign = offsetMinutes < 0 ? "-" : "+";
  const absolute = Math.abs(offsetMinutes);
  const hours = Math.floor(absolute / 60);
  const minutes = absolute % 60;
  return `${sign}${formatTwoDigits(hours)}:${formatTwoDigits(minutes)}`;
}

function formatMssqlFractionFromMilliseconds(value: number): string {
  return value > 0
    ? `.${String(value).padStart(3, "0").replace(/0+$/, "")}`
    : "";
}

function formatMssqlDateParts(
  year: number,
  month: number,
  day: number,
): string {
  return `${formatMssqlYear(year)}-${formatTwoDigits(month)}-${formatTwoDigits(day)}`;
}

function formatMssqlTimeParts(
  hours: number,
  minutes: number,
  seconds: number,
  fraction = "",
): string {
  return `${formatTwoDigits(hours)}:${formatTwoDigits(minutes)}:${formatTwoDigits(seconds)}${fraction}`;
}

function formatMssqlDateTimeParts(
  date: { year: number; month: number; day: number },
  time: { hours: number; minutes: number; seconds: number; fraction?: string },
  offset?: string,
): string {
  return `${formatMssqlDateParts(date.year, date.month, date.day)} ${formatMssqlTimeParts(time.hours, time.minutes, time.seconds, time.fraction ?? "")}${offset ?? ""}`;
}

function canonicalizeMssqlTemporalPersistedEditValue(
  value: unknown,
  baseType: string,
): string | null {
  if (value === NULL_SENTINEL || value === null) {
    return String(NULL_SENTINEL);
  }
  if (typeof value === "string") {
    if (baseType === "date") {
      return normalizeDateLiteral(value);
    }
    if (baseType === "time") {
      return value.trim();
    }
    if (
      baseType === "datetime" ||
      baseType === "datetime2" ||
      baseType === "smalldatetime" ||
      baseType === "datetimeoffset"
    ) {
      return normalizeDatetimeLiteral(value).replace("T", " ");
    }
    return value;
  }
  if (!(value instanceof Date) || Number.isNaN(value.getTime())) {
    return null;
  }
  const fraction = formatMssqlFractionFromMilliseconds(value.getMilliseconds());
  if (baseType === "date") {
    return formatMssqlDateParts(
      value.getFullYear(),
      value.getMonth() + 1,
      value.getDate(),
    );
  }
  if (baseType === "time") {
    return formatMssqlTimeParts(
      value.getHours(),
      value.getMinutes(),
      value.getSeconds(),
      fraction,
    );
  }
  if (baseType === "datetimeoffset") {
    return formatMssqlDateTimeParts(
      {
        year: value.getFullYear(),
        month: value.getMonth() + 1,
        day: value.getDate(),
      },
      {
        hours: value.getHours(),
        minutes: value.getMinutes(),
        seconds: value.getSeconds(),
        fraction,
      },
      formatMssqlOffsetMinutes(-value.getTimezoneOffset()),
    );
  }
  if (
    baseType === "datetime" ||
    baseType === "datetime2" ||
    baseType === "smalldatetime"
  ) {
    return formatMssqlDateTimeParts(
      {
        year: value.getFullYear(),
        month: value.getMonth() + 1,
        day: value.getDate(),
      },
      {
        hours: value.getHours(),
        minutes: value.getMinutes(),
        seconds: value.getSeconds(),
        fraction,
      },
    );
  }
  return null;
}

function readTediousDateTimeOffsetValue(
  buf: Buffer,
  offset: number,
  metadata: { scale?: number },
  helpers: TediousHelpersModule,
): TediousReadValueResult {
  const dataLengthResult = readTediousUInt8(buf, offset, helpers);
  const dataLength = dataLengthResult.value as number;
  offset = dataLengthResult.offset;
  if (dataLength === 0) {
    return new helpers.Result(null, offset);
  }

  const scale = Math.max(0, Math.min(7, metadata.scale ?? 7));
  const timeByteLength = dataLength - 5;
  if (timeByteLength < 3 || timeByteLength > 5) {
    throw new Error(
      `Unsupported DateTimeOffset dataLength ${String(dataLength)} for scale ${String(scale)}`,
    );
  }

  const timeUnitsResult = readTediousUnsignedBigIntLE(
    buf,
    offset,
    timeByteLength,
    helpers,
  );
  offset = timeUnitsResult.offset;
  let ticks = timeUnitsResult.value as bigint;
  for (let digit = scale; digit < 7; digit += 1) {
    ticks *= 10n;
  }

  ensureTediousBufferLength(buf, offset, 5, helpers);
  const days =
    buf.readUInt8(offset) |
    (buf.readUInt8(offset + 1) << 8) |
    (buf.readUInt8(offset + 2) << 16);
  offset += 3;
  const timezoneOffsetMinutes = buf.readInt16LE(offset);
  offset += 2;

  const ticksPerSecond = 10_000_000n;
  const ticksPerMinute = ticksPerSecond * 60n;
  const ticksPerDay = ticksPerSecond * 86_400n;
  let localTicksOfDay = ticks + BigInt(timezoneOffsetMinutes) * ticksPerMinute;
  let localDays = BigInt(days);
  if (localTicksOfDay < 0n) {
    const dayBorrow = (-localTicksOfDay - 1n) / ticksPerDay + 1n;
    localDays -= dayBorrow;
    localTicksOfDay += dayBorrow * ticksPerDay;
  } else if (localTicksOfDay >= ticksPerDay) {
    const dayCarry = localTicksOfDay / ticksPerDay;
    localDays += dayCarry;
    localTicksOfDay -= dayCarry * ticksPerDay;
  }

  const secondsOfDay = localTicksOfDay / ticksPerSecond;
  const fractionTicks = localTicksOfDay % ticksPerSecond;
  const hours = Number(secondsOfDay / 3600n);
  const minutes = Number((secondsOfDay % 3600n) / 60n);
  const seconds = Number(secondsOfDay % 60n);

  const date = new Date(Date.UTC(2000, 0, Number(localDays) - 730118));
  const fractionDigits = fractionTicks.toString().padStart(7, "0");
  const fraction = scale > 0 ? `.${fractionDigits.slice(0, scale)}` : "";
  const formatted = formatMssqlDateTimeParts(
    {
      year: date.getUTCFullYear(),
      month: date.getUTCMonth() + 1,
      day: date.getUTCDate(),
    },
    {
      hours,
      minutes,
      seconds,
      fraction,
    },
    formatMssqlOffsetMinutes(timezoneOffsetMinutes),
  );
  return new helpers.Result(formatted, offset);
}

function readTediousDateValue(
  buf: Buffer,
  offset: number,
  helpers: TediousHelpersModule,
): TediousReadValueResult {
  const dataLengthResult = readTediousUInt8(buf, offset, helpers);
  const dataLength = dataLengthResult.value as number;
  offset = dataLengthResult.offset;
  if (dataLength === 0) {
    return new helpers.Result(null, offset);
  }
  if (dataLength !== 3) {
    throw new Error(`Unsupported Date dataLength ${String(dataLength)}`);
  }

  ensureTediousBufferLength(buf, offset, 3, helpers);
  const days =
    buf.readUInt8(offset) |
    (buf.readUInt8(offset + 1) << 8) |
    (buf.readUInt8(offset + 2) << 16);
  offset += 3;
  const date = new Date(Date.UTC(2000, 0, days - 730118));
  return new helpers.Result(
    formatMssqlDateParts(
      date.getUTCFullYear(),
      date.getUTCMonth() + 1,
      date.getUTCDate(),
    ),
    offset,
  );
}

function readTediousTimeCore(
  buf: Buffer,
  offset: number,
  dataLength: number,
  scale: number,
  helpers: TediousHelpersModule,
): { ticks: bigint; offset: number } {
  const unitsResult = readTediousUnsignedBigIntLE(
    buf,
    offset,
    dataLength,
    helpers,
  );
  offset = unitsResult.offset;
  let ticks = unitsResult.value as bigint;
  for (let digit = scale; digit < 7; digit += 1) {
    ticks *= 10n;
  }
  return { ticks, offset };
}

function formatTicksAsTimeString(ticks: bigint, scale: number): string {
  const ticksPerSecond = 10_000_000n;
  const secondsOfDay = ticks / ticksPerSecond;
  const fractionTicks = ticks % ticksPerSecond;
  const hours = Number(secondsOfDay / 3600n);
  const minutes = Number((secondsOfDay % 3600n) / 60n);
  const seconds = Number(secondsOfDay % 60n);
  const fractionDigits = fractionTicks.toString().padStart(7, "0");
  const fraction = scale > 0 ? `.${fractionDigits.slice(0, scale)}` : "";
  return formatMssqlTimeParts(hours, minutes, seconds, fraction);
}

function readTediousTimeValue(
  buf: Buffer,
  offset: number,
  metadata: { scale?: number },
  helpers: TediousHelpersModule,
): TediousReadValueResult {
  const dataLengthResult = readTediousUInt8(buf, offset, helpers);
  const dataLength = dataLengthResult.value as number;
  offset = dataLengthResult.offset;
  if (dataLength === 0) {
    return new helpers.Result(null, offset);
  }
  if (dataLength < 3 || dataLength > 5) {
    throw new Error(`Unsupported Time dataLength ${String(dataLength)}`);
  }
  const scale = Math.max(0, Math.min(7, metadata.scale ?? 7));
  const parsed = readTediousTimeCore(buf, offset, dataLength, scale, helpers);
  return new helpers.Result(
    formatTicksAsTimeString(parsed.ticks, scale),
    parsed.offset,
  );
}

function readTediousDateTime2Value(
  buf: Buffer,
  offset: number,
  metadata: { scale?: number },
  helpers: TediousHelpersModule,
): TediousReadValueResult {
  const dataLengthResult = readTediousUInt8(buf, offset, helpers);
  const dataLength = dataLengthResult.value as number;
  offset = dataLengthResult.offset;
  if (dataLength === 0) {
    return new helpers.Result(null, offset);
  }
  const scale = Math.max(0, Math.min(7, metadata.scale ?? 7));
  const timeByteLength = dataLength - 3;
  if (timeByteLength < 3 || timeByteLength > 5) {
    throw new Error(
      `Unsupported DateTime2 dataLength ${String(dataLength)} for scale ${String(scale)}`,
    );
  }

  const timeParsed = readTediousTimeCore(
    buf,
    offset,
    timeByteLength,
    scale,
    helpers,
  );
  offset = timeParsed.offset;
  ensureTediousBufferLength(buf, offset, 3, helpers);
  const days =
    buf.readUInt8(offset) |
    (buf.readUInt8(offset + 1) << 8) |
    (buf.readUInt8(offset + 2) << 16);
  offset += 3;

  const date = new Date(Date.UTC(2000, 0, days - 730118));
  return new helpers.Result(
    `${formatMssqlDateParts(
      date.getUTCFullYear(),
      date.getUTCMonth() + 1,
      date.getUTCDate(),
    )} ${formatTicksAsTimeString(timeParsed.ticks, scale)}`,
    offset,
  );
}

function formatScaledBigInt(value: bigint, scale: number): string {
  const negative = value < 0n;
  const absoluteValue = negative ? -value : value;
  const digits = absoluteValue.toString();
  if (scale <= 0) {
    return `${negative ? "-" : ""}${digits}`;
  }
  const paddedDigits = digits.padStart(scale + 1, "0");
  const integerPart = paddedDigits.slice(0, -scale);
  const fractionPart = paddedDigits.slice(-scale);
  return `${negative ? "-" : ""}${integerPart}.${fractionPart}`;
}

function readTediousExactNumericValue(
  buf: Buffer,
  offset: number,
  metadata: { precision?: number; scale?: number },
  helpers: TediousHelpersModule,
): TediousReadValueResult {
  const dataLengthResult = readTediousUInt8(buf, offset, helpers);
  const dataLength = dataLengthResult.value as number;
  offset = dataLengthResult.offset;
  if (dataLength === 0) {
    return new helpers.Result(null, offset);
  }
  const signResult = readTediousUInt8(buf, offset, helpers);
  const sign = (signResult.value as number) === 1 ? 1n : -1n;
  offset = signResult.offset;
  const magnitudeResult = readTediousUnsignedBigIntLE(
    buf,
    offset,
    dataLength - 1,
    helpers,
  );
  const magnitude = magnitudeResult.value as bigint;
  return new helpers.Result(
    formatScaledBigInt(magnitude * sign, metadata.scale ?? 0),
    magnitudeResult.offset,
  );
}

function readTediousSmallMoneyValue(
  buf: Buffer,
  offset: number,
  helpers: TediousHelpersModule,
): TediousReadValueResult {
  ensureTediousBufferLength(buf, offset, 4, helpers);
  return new helpers.Result(
    formatScaledBigInt(BigInt(buf.readInt32LE(offset)), 4),
    offset + 4,
  );
}

function readTediousMoneyValue(
  buf: Buffer,
  offset: number,
  helpers: TediousHelpersModule,
): TediousReadValueResult {
  ensureTediousBufferLength(buf, offset, 8, helpers);
  const value =
    (BigInt(buf.readInt32LE(offset)) << 32n) +
    BigInt(buf.readUInt32LE(offset + 4));
  return new helpers.Result(formatScaledBigInt(value, 4), offset + 8);
}

function readTediousMoneyNValue(
  buf: Buffer,
  offset: number,
  helpers: TediousHelpersModule,
): TediousReadValueResult {
  const dataLengthResult = readTediousUInt8(buf, offset, helpers);
  const dataLength = dataLengthResult.value as number;
  offset = dataLengthResult.offset;
  if (dataLength === 0) {
    return new helpers.Result(null, offset);
  }
  if (dataLength === 4) {
    return readTediousSmallMoneyValue(buf, offset, helpers);
  }
  if (dataLength === 8) {
    return readTediousMoneyValue(buf, offset, helpers);
  }
  throw new Error(`Unsupported MoneyN dataLength ${String(dataLength)}`);
}

function patchMssqlTediousExactNumericParsing(): void {
  const globalScope = globalThis as Record<PropertyKey, unknown>;
  if (globalScope[MSSQL_TEDIOUS_EXACT_NUMERIC_PATCH_KEY]) {
    return;
  }

  const tediousValueParser =
    require("tedious/lib/value-parser") as TediousValueParserModule;
  const tediousHelpers =
    require("tedious/lib/token/helpers") as TediousHelpersModule;
  const originalReadValue = tediousValueParser.readValue;

  tediousValueParser.readValue = (buf, offset, metadata, options) => {
    switch (metadata.type?.name) {
      case "NumericN":
      case "DecimalN":
        return readTediousExactNumericValue(
          buf,
          offset,
          metadata,
          tediousHelpers,
        );
      case "SmallMoney":
        return readTediousSmallMoneyValue(buf, offset, tediousHelpers);
      case "Money":
        return readTediousMoneyValue(buf, offset, tediousHelpers);
      case "MoneyN":
        return readTediousMoneyNValue(buf, offset, tediousHelpers);
      case "Date":
        return readTediousDateValue(buf, offset, tediousHelpers);
      case "Time":
        return readTediousTimeValue(buf, offset, metadata, tediousHelpers);
      case "DateTime2":
        return readTediousDateTime2Value(buf, offset, metadata, tediousHelpers);
      case "DateTimeOffset":
        return readTediousDateTimeOffsetValue(
          buf,
          offset,
          metadata,
          tediousHelpers,
        );
      default:
        return originalReadValue(buf, offset, metadata, options);
    }
  };

  globalScope[MSSQL_TEDIOUS_EXACT_NUMERIC_PATCH_KEY] = true;
}

patchMssqlTediousExactNumericParsing();

const MSSQL_TIME_RE = /^\d{2}:\d{2}:\d{2}(?:\.\d{1,7})?$/;
const MSSQL_UUID_RE =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const INTEGER_RE = /^-?\d+$/;
const DECIMAL_RE = /^-?\d+(?:\.\d+)?$/;
const MSSQL_LIKE_MAX_NVARCHAR_CHARS = 3900;
function approximateNumericFilterTolerance(rawValue: string): number {
  const fraction = /\.(\d+)/.exec(rawValue)?.[1].length ?? 0;
  const precision = Math.min(Math.max(fraction + 2, 6), 12);
  return 10 ** -precision;
}
function mssqlNumericFilterParam(
  column: ColumnTypeMeta,
  rawValue: string,
): string | number | bigint {
  if (column.category === "decimal") {
    return rawValue;
  }
  if (
    column.category === "integer" &&
    baseTypeName(column.nativeType) === "bigint" &&
    /^-?\d+$/.test(rawValue)
  ) {
    return BigInt(rawValue);
  }
  return Number(rawValue);
}
function mssqlNumericParamExpr(column: ColumnTypeMeta): string {
  return column.category === "decimal"
    ? `CAST(? AS ${column.nativeType})`
    : "?";
}
function mssqlDisplayedTemporalDiffUpperBound(rawValue: string): number | null {
  const fractionMatch = /\.(\d+)(?: ?(?:Z|[+-]\d{2}(?::?\d{2})?))?$/i.exec(
    rawValue.trim(),
  );
  if (!fractionMatch) {
    return 999;
  }
  const digits = fractionMatch[1].length;
  if (digits > 3) {
    return null;
  }

  return 10 ** (3 - digits) - 1;
}

function mssqlFloatSignificantDigits(nativeType: string): number {
  const normalized = nativeType.toLowerCase().trim();
  if (normalized.startsWith("real")) {
    return 7;
  }
  const match = /^float(?:\((\d+)\))?/.exec(normalized);
  if (!match?.[1]) {
    return 15;
  }
  return Number.parseInt(match[1], 10) <= 24 ? 7 : 15;
}
type MssqlTemporalDate = Date & {
  nanosecondDelta?: number;
};
function mssqlFullType(
  typeName: string,
  maxLength: number,
  precision: number,
  scale: number,
): string {
  const t = typeName.toLowerCase();
  if (["varchar", "char", "varbinary", "binary"].includes(t)) {
    return maxLength === -1 ? `${t}(max)` : `${t}(${maxLength})`;
  }
  if (["nvarchar", "nchar"].includes(t)) {
    return maxLength === -1 ? `${t}(max)` : `${t}(${maxLength / 2})`;
  }
  if (["decimal", "numeric"].includes(t)) {
    return `${t}(${precision},${scale})`;
  }
  if (t === "float") {
    return `float(${precision})`;
  }
  if (["datetime2", "datetimeoffset", "time"].includes(t)) {
    return `${t}(${scale})`;
  }
  return typeName;
}
function cleanMssqlDefault(raw: string): string {
  let s = raw.trim();
  while (s.startsWith("(") && s.endsWith(")")) {
    s = s.slice(1, -1).trim();
  }
  if (s.startsWith("N'") && s.endsWith("'")) {
    s = s.slice(1);
  }
  return s;
}
const escapeMssqlId = (s: string) => s.replace(/]/g, "]]");
function baseTypeName(typeName: string): string {
  return typeName.toLowerCase().split("(")[0].trim();
}
function isUnicodeMssqlLiteralType(nativeType: string): boolean {
  return ["nchar", "nvarchar", "ntext", "xml"].includes(
    baseTypeName(nativeType),
  );
}
function formatMssqlStringPreviewLiteral(
  value: string,
  nativeType?: string,
): string {
  return formatSqlPreviewStringLiteral(
    value,
    nativeType !== undefined && isUnicodeMssqlLiteralType(nativeType)
      ? "N"
      : "",
  );
}
function normalizeDatetimeLiteral(value: string): string {
  return normalizeSqlDatetimeOffsetSpacing(
    normalizeMssqlYearPrefix(value.trim()),
  );
}
function normalizeDateLiteral(value: string): string {
  const trimmed = normalizeMssqlYearPrefix(value.trim());
  if (DATE_ONLY_RE.test(trimmed)) {
    return trimmed;
  }
  if (ISO_DATETIME_RE.test(trimmed)) {
    if (!hasExplicitTimezone(trimmed)) {
      return trimmed.slice(0, 10);
    }
    return isoToLocalDateStr(trimmed) ?? trimmed.slice(0, 10);
  }
  const normalizedSql = normalizeSqlDatetimeOffsetSpacing(trimmed);
  if (DATETIME_SQL_RE.test(normalizedSql)) {
    if (hasExplicitTimezone(normalizedSql)) {
      return (
        isoToLocalDateStr(normalizedSql.replace(" ", "T")) ??
        normalizedSql.slice(0, 10)
      );
    }
    return normalizedSql.slice(0, 10);
  }
  return trimmed;
}
function detectScale(value: unknown): number | null {
  if (typeof value !== "string") {
    return null;
  }
  const match = /\.(\d+)/.exec(value);
  if (!match) return null;
  return Math.min(match[1].length, 7);
}
function parseMssqlTimeLiteral(value: string): MssqlTemporalDate {
  const match = /^(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,7}))?$/.exec(value);
  if (!match) {
    throw new TypeError("Invalid time.");
  }
  const hours = Number(match[1]);
  const minutes = Number(match[2]);
  const seconds = Number(match[3]);
  const fraction = match[4] ?? "";
  if (hours > 23 || minutes > 59 || seconds > 59) {
    throw new TypeError("Invalid time.");
  }
  const milliseconds = Number((fraction.slice(0, 3) || "0").padEnd(3, "0"));
  const result = new Date(0) as MssqlTemporalDate;
  result.setFullYear(1970, 0, 1);
  result.setHours(hours, minutes, seconds, milliseconds);
  const subMillisecond = fraction.slice(3);
  if (subMillisecond.length > 0) {
    result.nanosecondDelta = Number(subMillisecond) / 10 ** fraction.length;
  }
  return result;
}
function parseMssqlNaiveDatetimeLiteral(value: string): MssqlTemporalDate {
  const match =
    /^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,7}))?$/.exec(
      value,
    );
  if (!match) {
    throw new TypeError("Invalid datetime.");
  }
  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  const hours = Number(match[4]);
  const minutes = Number(match[5]);
  const seconds = Number(match[6]);
  const fraction = match[7] ?? "";
  if (
    month < 1 ||
    month > 12 ||
    day < 1 ||
    day > 31 ||
    hours > 23 ||
    minutes > 59 ||
    seconds > 59
  ) {
    throw new TypeError("Invalid datetime.");
  }
  const milliseconds = Number((fraction.slice(0, 3) || "0").padEnd(3, "0"));
  const result = new Date(0) as MssqlTemporalDate;
  result.setFullYear(year, month - 1, day);
  result.setHours(hours, minutes, seconds, milliseconds);
  if (
    result.getFullYear() !== year ||
    result.getMonth() !== month - 1 ||
    result.getDate() !== day ||
    result.getHours() !== hours ||
    result.getMinutes() !== minutes ||
    result.getSeconds() !== seconds
  ) {
    throw new TypeError("Invalid datetime.");
  }
  const subMillisecond = fraction.slice(3);
  if (subMillisecond.length > 0) {
    result.nanosecondDelta = Number(subMillisecond) / 10 ** fraction.length;
  }
  return result;
}
function isSetFlag(value: boolean | number | null | undefined): boolean {
  return value === true || value === 1;
}
function columnTypeName(meta: MssqlArrayColumnMeta | undefined): string {
  const rawType = meta?.type;
  if (!rawType) return "";
  if (typeof rawType === "function") {
    return rawType.name;
  }
  return typeof rawType.type === "function" ? rawType.type.name : "";
}
function mssqlSqlTypeName(sqlType: MssqlSqlType): string {
  if (typeof sqlType === "function") {
    return sqlType.name;
  }
  return typeof sqlType.type === "function" ? sqlType.type.name : "";
}
function temporalSearchLiteral(value: string): string {
  const trimmed = value.trim();
  const hasMatchingQuotes =
    (trimmed.startsWith('"') && trimmed.endsWith('"')) ||
    (trimmed.startsWith("'") && trimmed.endsWith("'"));
  const unquoted =
    hasMatchingQuotes && trimmed.length >= 2 ? trimmed.slice(1, -1) : trimmed;
  return normalizeDatetimeLiteral(unquoted).replace(" ", "T");
}
function datetimeOffsetSearchLiteral(value: string): string {
  const normalized = temporalSearchLiteral(value);
  const match =
    /^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(\.\d+)?([+-]\d{2}:\d{2}|Z)$/i.exec(
      normalized,
    );
  if (!match) {
    return normalized;
  }
  const prefix = match[1];
  const fraction = match[2] ?? "";
  const offset = match[3];
  if (fraction === "") {
    return normalized;
  }
  const compactFraction = fraction.replace(/0+$/, "");
  if (compactFraction === "") {
    return `${prefix}%${offset}`;
  }
  return `${prefix}${compactFraction}%${offset}`;
}
const MSSQL_FILTER_DENYLIST = new Set([
  "hierarchyid",
  "sql_variant",
  "timestamp",
  "rowversion",
]);
export class MSSQLDriver extends BaseDBDriver {
  protected override getQueryEditorSqlDialect() {
    return "transactsql" as const;
  }

  private pool: mssql.ConnectionPool | null = null;
  private readonly config: ConnectionConfig;
  private readonly activeRequests = new Set<mssql.Request>();
  private timeoutRecoveryInFlight: Promise<void> | null = null;
  constructor(
    config: ConnectionConfig,
    timeoutSettingsProvider?: DriverTimeoutSettingsProvider,
  ) {
    super(timeoutSettingsProvider);
    this.config = config;
  }
  private requirePool(): mssql.ConnectionPool {
    if (this.pool) {
      return this.pool;
    }
    throw new Error("[RapiDB] MSSQL connection is not open");
  }
  private poolConfig(): mssql.config {
    const forwardedTransport = getSshTcpForwardTransport(this.config);
    const serverHost = forwardedTransport?.localHost ?? this.config.host;
    const serverPort = forwardedTransport?.localPort ?? this.config.port;
    if (!serverHost) {
      throw new Error("[RapiDB] MSSQL host is required");
    }
    const tlsSettings = resolveConnectionTlsSettings(this.config);
    const trustCert = tlsSettings ? !tlsSettings.rejectUnauthorized : false;
    const runtimeServerName = getMssqlServerName(this.config);
    return {
      server: serverHost,
      port: serverPort,
      database: this.config.database,
      user: this.config.username,
      password: this.config.password,
      connectionTimeout: this.getConnectionTimeoutMs(),
      requestTimeout: this.getDbOperationTimeoutMs(),
      options: {
        encrypt: true,
        trustServerCertificate: trustCert,
        enableArithAbort: true,
        abortTransactionOnError: true,
        serverName:
          runtimeServerName ?? (!trustCert ? this.config.host : undefined),
        useUTC: false,
      },
    };
  }
  private createNVarCharType(value: unknown): MssqlSqlType {
    const length = typeof value === "string" ? value.length : 0;
    return mssql.NVarChar(length === 0 || length > 4000 ? mssql.MAX : length);
  }
  private normalizeInputValue(
    value: unknown,
    column?: ColumnTypeMeta,
  ): unknown {
    if (value === NULL_SENTINEL) return null;
    if (value === null || value === undefined || value === "") return value;
    if (typeof value !== "string") return value;
    const trimmed = value.trim();
    if (trimmed === "") return value;
    if (column && this.hasBooleanSemantics(column)) {
      const normalized = this.parseBooleanInput(trimmed);
      if (normalized !== null) {
        return normalized;
      }
    }
    if (column && this.hasBitSemantics(column)) {
      const normalized = this.parseBooleanInput(trimmed);
      if (normalized !== null) {
        return normalized ? 1 : 0;
      }
    }
    if (column?.category === "binary") {
      return super.coerceInputValue(trimmed, column);
    }
    if (column?.category === "integer") {
      if (INTEGER_RE.test(trimmed)) {
        return baseTypeName(column.nativeType) === "bigint"
          ? BigInt(trimmed)
          : Number(trimmed);
      }
      return value;
    }
    if (column?.category === "float") {
      return DECIMAL_RE.test(trimmed) ? Number(trimmed) : value;
    }
    if (column?.category === "decimal") {
      return DECIMAL_RE.test(trimmed) ? trimmed : value;
    }
    if (column?.category === "date") {
      return normalizeDateLiteral(trimmed);
    }
    if (column?.category === "time") {
      return MSSQL_TIME_RE.test(trimmed) ? trimmed : value;
    }
    if (column?.category === "datetime") {
      const normalized = normalizeDatetimeLiteral(trimmed);
      return ISO_DATETIME_RE.test(normalized) ||
        DATETIME_SQL_RE.test(normalized.replace("T", " "))
        ? normalized
        : value;
    }
    if (DATE_ONLY_RE.test(trimmed) || MSSQL_TIME_RE.test(trimmed)) {
      return trimmed;
    }
    const normalizedDateTime = normalizeDatetimeLiteral(trimmed);
    if (
      ISO_DATETIME_RE.test(normalizedDateTime) ||
      DATETIME_SQL_RE.test(normalizedDateTime.replace("T", " "))
    ) {
      return normalizedDateTime;
    }
    return value;
  }
  private typeForValue(value: unknown): MssqlSqlType {
    if (Buffer.isBuffer(value)) {
      return mssql.VarBinary(value.length === 0 ? mssql.MAX : value.length);
    }
    if (typeof value === "bigint") return mssql.BigInt;
    if (typeof value === "number") {
      if (!Number.isFinite(value) || !Number.isInteger(value)) {
        return mssql.Float;
      }
      if (!Number.isSafeInteger(value)) {
        return mssql.Float;
      }
      if (value >= 0 && value <= 255) return mssql.TinyInt;
      if (value >= -32768 && value <= 32767) return mssql.SmallInt;
      if (value >= -2147483648 && value <= 2147483647) return mssql.Int;
      return mssql.BigInt;
    }
    if (typeof value === "boolean") return mssql.Bit;
    if (value instanceof Date) return mssql.DateTime2(7);
    if (typeof value === "string") {
      if (MSSQL_UUID_RE.test(value)) return mssql.UniqueIdentifier;
      if (DATE_ONLY_RE.test(value)) return mssql.Date;
      if (MSSQL_TIME_RE.test(value)) {
        return mssql.Time(detectScale(value) ?? 7);
      }
      const normalized = normalizeDatetimeLiteral(value);
      if (ISO_DATETIME_RE.test(normalized)) {
        return hasExplicitTimezone(normalized)
          ? mssql.DateTimeOffset(detectScale(normalized) ?? 7)
          : mssql.DateTime2(detectScale(normalized) ?? 7);
      }
      if (DATETIME_SQL_RE.test(normalized.replace("T", " "))) {
        return hasExplicitTimezone(normalized)
          ? mssql.DateTimeOffset(detectScale(normalized) ?? 7)
          : mssql.DateTime2(detectScale(normalized) ?? 7);
      }
      return this.createNVarCharType(value);
    }
    return this.createNVarCharType(value);
  }
  private bindRequestInput(
    request: mssql.Request,
    name: string,
    rawValue: unknown,
  ): void {
    const normalizedValue = this.normalizeInputValue(rawValue);
    const baseValue = normalizedValue === undefined ? null : normalizedValue;
    const type = this.typeForValue(baseValue);
    const typeName = mssqlSqlTypeName(type);
    let value = baseValue;
    if (typeof baseValue === "string") {
      if (
        typeName === "Date" ||
        typeName === "Time" ||
        typeName === "DateTime2" ||
        typeName === "DateTimeOffset"
      ) {
        const normalizedTemporalValue =
          typeName === "Date"
            ? normalizeDateLiteral(baseValue)
            : typeName === "Time"
              ? baseValue.trim()
              : normalizeDatetimeLiteral(baseValue);
        request.input(
          name,
          this.createNVarCharType(normalizedTemporalValue),
          normalizedTemporalValue,
        );
        return;
      }
      if (typeName === "Time") {
        value = parseMssqlTimeLiteral(baseValue);
      } else if (typeName === "DateTime2" && !hasExplicitTimezone(baseValue)) {
        value = parseMssqlNaiveDatetimeLiteral(
          normalizeDatetimeLiteral(baseValue),
        );
      }
    }
    request.input(name, type, value);
  }
  private bindPositionalParameters(
    request: mssql.Request,
    sql: string,
    params?: readonly unknown[],
  ): string {
    if (!params || params.length === 0) {
      return sql;
    }
    const placeholderCount = (sql.match(/\?/g) ?? []).length;
    if (placeholderCount !== params.length) {
      throw new Error(
        `[RapiDB] MSSQL parameter mismatch: SQL has ${placeholderCount} placeholder(s) but ${params.length} value(s) were supplied.`,
      );
    }
    let index = 0;
    return sql.replace(/\?/g, () => {
      const name = `p${++index}`;
      this.bindRequestInput(request, name, params[index - 1]);
      return `@${name}`;
    });
  }
  private formatQueryValue(
    value: unknown,
    columnMeta: MssqlArrayColumnMeta | undefined,
  ): unknown {
    const typeName = columnTypeName(columnMeta);
    const pad = (n: number) => String(n).padStart(2, "0");
    if (typeName === "Bit") {
      if (value === true || value === 1 || value === "1") {
        return 1;
      }
      if (value === false || value === 0 || value === "0") {
        return 0;
      }
    }
    if (
      typeName === "Real" &&
      typeof value === "number" &&
      !Number.isInteger(value)
    ) {
      return Number.parseFloat(value.toPrecision(7));
    }
    if (value instanceof Date && !Number.isNaN(value.getTime())) {
      if (typeName === "Date") {
        return `${formatMssqlYear(value.getFullYear())}-${pad(value.getMonth() + 1)}-${pad(value.getDate())}`;
      }
      if (typeName === "Time") {
        const ms = value.getMilliseconds();
        const frac =
          ms > 0 ? `.${String(ms).padStart(3, "0").replace(/0+$/, "")}` : "";
        return `${pad(value.getHours())}:${pad(value.getMinutes())}:${pad(value.getSeconds())}${frac}`;
      }
      if (typeName === "DateTimeOffset") {
        const ms = value.getMilliseconds();
        const frac =
          ms > 0 ? `.${String(ms).padStart(3, "0").replace(/0+$/, "")}` : "";
        const offsetMinutes = -value.getTimezoneOffset();
        return (
          `${formatMssqlYear(value.getFullYear())}-${pad(value.getMonth() + 1)}-${pad(value.getDate())} ` +
          `${pad(value.getHours())}:${pad(value.getMinutes())}:${pad(value.getSeconds())}${frac}${formatMssqlOffsetMinutes(offsetMinutes)}`
        );
      }
    }
    return value;
  }
  async connect(): Promise<void> {
    if (this.pool !== null) {
      try {
        await this.pool.close();
      } catch {}
      this.pool = null;
    }
    const pool = new mssql.ConnectionPool(this.poolConfig());
    pool.on("error", (err: unknown) => {
      logger.error("MSSQL pool error", err);
    });
    this.pool = await pool.connect();
  }
  async disconnect(): Promise<void> {
    await this.pool?.close();
    this.pool = null;
  }

  async cancelCurrentOperation(): Promise<void> {
    for (const request of [...this.activeRequests]) {
      try {
        request.cancel();
      } catch {}
    }
  }

  async recycleConnectionAfterTimeout(): Promise<void> {
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

  private async executeTrackedRequest<T>(
    request: mssql.Request,
    operation: (request: mssql.Request) => Promise<T>,
  ): Promise<T> {
    this.activeRequests.add(request);
    try {
      return await operation(request);
    } finally {
      this.activeRequests.delete(request);
    }
  }

  isConnected(): boolean {
    return this.pool?.connected ?? false;
  }

  getEntityManifest(): DriverEntityManifest {
    return MSSQL_ENTITY_MANIFEST;
  }

  async listDatabases(): Promise<DatabaseInfo[]> {
    const res = await this.requirePool()
      .request()
      .query<NamedRow>(
        `SELECT name
         FROM sys.databases 
         WHERE database_id > 4 AND
          HAS_DBACCESS(name) = 1 AND
          state_desc = 'ONLINE' ORDER BY name`,
      );
    return res.recordset.map((row) => ({
      name: row.name,
      schemas: [],
    }));
  }
  async listSchemas(database: string): Promise<SchemaInfo[]> {
    const res = await this.requirePool()
      .request()
      .query<NamedRow>(`SELECT name
        FROM [${escapeMssqlId(database)}].sys.schemas
        WHERE principal_id < 16384
          AND name NOT IN ('sys', 'INFORMATION_SCHEMA', 'guest')
        ORDER BY name`);
    return res.recordset.map((row) => ({ name: row.name }));
  }
  async listObjects(database: string, schema: string): Promise<TableInfo[]> {
    const objects: TableInfo[] = [];
    const tableRes = await this.requirePool()
      .request()
      .input("schema", mssql.NVarChar, schema)
      .query<ObjectRow>(`SELECT TABLE_NAME AS name, TABLE_TYPE AS type
         FROM [${escapeMssqlId(database)}].INFORMATION_SCHEMA.TABLES
         WHERE TABLE_SCHEMA = @schema ORDER BY TABLE_NAME`);
    for (const row of tableRes.recordset) {
      objects.push({
        schema,
        name: row.name,
        type: (row.type.includes("VIEW")
          ? "view"
          : "table") as TableInfo["type"],
      });
    }
    try {
      const routineRes = await this.requirePool()
        .request()
        .input("schema", mssql.NVarChar, schema)
        .query<ObjectRow>(`SELECT o.name,
                  CASE o.type WHEN 'P' THEN 'procedure' WHEN 'PC' THEN 'procedure'
                              WHEN 'FN' THEN 'function'  WHEN 'IF' THEN 'function'
                              WHEN 'TF' THEN 'function'  WHEN 'AF' THEN 'function'
                              ELSE 'function' END AS type
           FROM [${escapeMssqlId(database)}].sys.objects o
           JOIN [${escapeMssqlId(database)}].sys.schemas s ON s.schema_id = o.schema_id
           WHERE s.name = @schema AND o.type IN ('P','PC','FN','IF','TF','AF')
           ORDER BY o.name`);
      for (const row of routineRes.recordset) {
        objects.push({
          schema,
          name: row.name,
          type: row.type as TableInfo["type"],
        });
      }
    } catch {}
    try {
      const sequenceRes = await this.requirePool()
        .request()
        .input("schema", mssql.NVarChar, schema)
        .query<NamedRow>(`SELECT seq.name
           FROM [${escapeMssqlId(database)}].sys.sequences seq
           JOIN [${escapeMssqlId(database)}].sys.schemas s ON s.schema_id = seq.schema_id
           WHERE s.name = @schema
           ORDER BY seq.name`);
      for (const row of sequenceRes.recordset) {
        objects.push({
          schema,
          name: row.name,
          type: "sequence",
        });
      }
    } catch {}
    try {
      const typeRes = await this.requirePool()
        .request()
        .input("schema", mssql.NVarChar, schema)
        .query<NamedRow>(`SELECT t.name
           FROM [${escapeMssqlId(database)}].sys.types t
           JOIN [${escapeMssqlId(database)}].sys.schemas s ON s.schema_id = t.schema_id
           WHERE s.name = @schema
             AND t.is_user_defined = 1
             AND t.is_table_type = 0
           ORDER BY t.name`);
      for (const row of typeRes.recordset) {
        objects.push({
          schema,
          name: row.name,
          type: "type",
        });
      }
    } catch {}
    return objects;
  }
  async describeTable(
    database: string,
    schema: string,
    table: string,
  ): Promise<ColumnMeta[]> {
    const res = await this.requirePool()
      .request()
      .input("schema", mssql.NVarChar, schema)
      .input("table", mssql.NVarChar, table)
      .query<DescribeColumnRow>(`SELECT
           c.name                                                AS COLUMN_NAME,
           TYPE_NAME(c.user_type_id)                            AS DATA_TYPE,
           c.max_length,
           c.precision,
           c.scale,
           c.is_nullable                                        AS IS_NULLABLE,
           c.is_identity,
           c.is_computed,
           cc.definition                                        AS COMPUTED_DEFINITION,
           cc.is_persisted,
           OBJECT_DEFINITION(c.default_object_id)               AS COLUMN_DEFAULT,
           CASE WHEN pk.column_id IS NOT NULL THEN 1 ELSE 0 END AS IS_PK,
           pk.key_ordinal                                       AS PK_ORDINAL,
           CASE WHEN fk.parent_column_id IS NOT NULL THEN 1 ELSE 0 END AS IS_FK
         FROM [${escapeMssqlId(database)}].sys.columns c
         JOIN [${escapeMssqlId(database)}].sys.objects  o ON o.object_id = c.object_id
         JOIN [${escapeMssqlId(database)}].sys.schemas  s ON s.schema_id  = o.schema_id
         LEFT JOIN [${escapeMssqlId(database)}].sys.computed_columns cc
           ON cc.object_id = c.object_id AND cc.column_id = c.column_id
         LEFT JOIN (
           SELECT ic.object_id, ic.column_id, ic.key_ordinal
           FROM [${escapeMssqlId(database)}].sys.index_columns ic
           JOIN [${escapeMssqlId(database)}].sys.indexes       i
             ON i.object_id = ic.object_id AND i.index_id = ic.index_id
           WHERE i.is_primary_key = 1
         ) pk ON pk.object_id = c.object_id AND pk.column_id = c.column_id
         LEFT JOIN (
           SELECT DISTINCT fkc.parent_object_id, fkc.parent_column_id
           FROM [${escapeMssqlId(database)}].sys.foreign_key_columns fkc
         ) fk ON fk.parent_object_id = c.object_id AND fk.parent_column_id = c.column_id
         WHERE s.name = @schema AND o.name = @table
         ORDER BY c.column_id`);
    return res.recordset.map((row) => {
      const isComputed = isSetFlag(row.is_computed);
      const isPersisted = isComputed ? isSetFlag(row.is_persisted) : undefined;
      const defaultValue =
        !isComputed && row.COLUMN_DEFAULT != null
          ? cleanMssqlDefault(row.COLUMN_DEFAULT)
          : undefined;
      return {
        name: row.COLUMN_NAME,
        type: mssqlFullType(
          row.DATA_TYPE,
          row.max_length,
          row.precision,
          row.scale,
        ),
        nullable: isSetFlag(row.IS_NULLABLE),
        defaultValue,
        identityGeneration: isSetFlag(row.is_identity) ? "always" : undefined,
        isComputed,
        computedExpression: row.COMPUTED_DEFINITION ?? undefined,
        generatedKind: isComputed
          ? isPersisted
            ? "stored"
            : "virtual"
          : undefined,
        isPersisted,
        isPrimaryKey: row.IS_PK === 1,
        primaryKeyOrdinal: row.PK_ORDINAL ?? undefined,
        isForeignKey: row.IS_FK === 1,
      };
    });
  }
  async query(sql: string, params?: unknown[]): Promise<QueryResult> {
    const start = Date.now();
    const batches = sql
      .split(/\r?\n/)
      .reduce<string[]>(
        (acc, line) => {
          const isGo = /^GO(?:\s+\d+)?$/i.test(line.trim());
          if (isGo) {
            acc.push("");
          } else {
            const lastIdx = acc.length - 1;
            acc[lastIdx] += (acc[lastIdx] ? "\n" : "") + line;
          }
          return acc;
        },
        [""],
      )
      .map((b) => b.trim())
      .filter((b) => b.length > 0);
    if (batches.length === 0) {
      return {
        columns: [],
        rows: [],
        rowCount: 0,
        executionTimeMs: Date.now() - start,
      };
    }
    if (batches.length > 1 && params && params.length > 0) {
      throw new Error(
        "[RapiDB] MSSQL:parameters are not supported in multi-batch scripts (GO). " +
          "Use parameters only in single queries without the GO separator.",
      );
    }
    let lastResult: QueryResult = {
      columns: [],
      rows: [],
      rowCount: 0,
      executionTimeMs: 0,
    };
    for (const batch of batches) {
      const currentParams = batches.length === 1 ? params : undefined;
      lastResult = await this._executeBatch(batch, currentParams, start);
    }
    lastResult.executionTimeMs = Date.now() - start;
    return lastResult;
  }
  private async _executeBatch(
    sql: string,
    params?: unknown[],
    start = Date.now(),
  ): Promise<QueryResult> {
    const req = this.requirePool().request();
    req.arrayRowMode = true;
    const res = (await this.executeTrackedRequest(req, async (trackedReq) => {
      const finalSql = this.bindPositionalParameters(trackedReq, sql, params);
      return await trackedReq.query(finalSql);
    })) as MssqlArrayResult;
    const executionTimeMs = Date.now() - start;
    const columnsMeta = res.columns?.[0] ?? [];
    const affectedRows = res.rowsAffected.at(-1) ?? 0;
    if (columnsMeta.length === 0) {
      return {
        columns: [],
        rows: [],
        rowCount: affectedRows,
        affectedRows,
        executionTimeMs,
      };
    }
    const columns = columnsMeta.map((column) =>
      column.name === "" ? " " : column.name,
    );
    const rows = ((res.recordset ?? []) as unknown[][]).map((row) =>
      Object.fromEntries(
        row.map((value, index) => [
          `__col_${index}`,
          this.formatQueryValue(value, columnsMeta[index]),
        ]),
      ),
    );
    return {
      columns,
      rows,
      rowCount: rows.length,
      affectedRows,
      executionTimeMs,
    };
  }
  async getIndexes(
    database: string,
    schema: string,
    table: string,
  ): Promise<import("./types").IndexMeta[]> {
    const res = await this.requirePool()
      .request()
      .input("schema", mssql.NVarChar, schema)
      .input("table", mssql.NVarChar, table)
      .query<IndexRow>(`SELECT i.name AS idx_name, c.name AS col_name,
                i.is_unique AS is_unique, i.is_primary_key AS is_pk
         FROM [${escapeMssqlId(database)}].sys.indexes i
         JOIN [${escapeMssqlId(database)}].sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
         JOIN [${escapeMssqlId(database)}].sys.columns c ON c.object_id = i.object_id AND c.column_id = ic.column_id
         JOIN [${escapeMssqlId(database)}].sys.objects o ON o.object_id = i.object_id
         JOIN [${escapeMssqlId(database)}].sys.schemas s ON s.schema_id = o.schema_id
         WHERE s.name = @schema AND o.name = @table
         ORDER BY i.name, ic.key_ordinal`);
    const map = new Map<string, import("./types").IndexMeta>();
    for (const row of res.recordset) {
      if (!map.has(row.idx_name)) {
        map.set(row.idx_name, {
          name: row.idx_name,
          columns: [],
          unique: isSetFlag(row.is_unique),
          primary: isSetFlag(row.is_pk),
        });
      }
      const entry = map.get(row.idx_name);
      if (entry) {
        entry.columns.push(row.col_name);
      }
    }
    return [...map.values()];
  }
  async getForeignKeys(
    database: string,
    schema: string,
    table: string,
  ): Promise<import("./types").ForeignKeyMeta[]> {
    const res = await this.requirePool()
      .request()
      .input("schema", mssql.NVarChar, schema)
      .input("table", mssql.NVarChar, table)
      .query<ForeignKeyRow>(`SELECT fk.name AS constraint_name,
                pc.name AS column_name,
                rs.name AS ref_schema,
                ro.name AS ref_table,
                rc.name AS ref_column
         FROM [${escapeMssqlId(database)}].sys.foreign_keys fk
         JOIN [${escapeMssqlId(database)}].sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id
         JOIN [${escapeMssqlId(database)}].sys.columns pc ON pc.object_id = fkc.parent_object_id AND pc.column_id = fkc.parent_column_id
         JOIN [${escapeMssqlId(database)}].sys.columns rc ON rc.object_id = fkc.referenced_object_id AND rc.column_id = fkc.referenced_column_id
         JOIN [${escapeMssqlId(database)}].sys.objects ro ON ro.object_id = fkc.referenced_object_id
         JOIN [${escapeMssqlId(database)}].sys.schemas rs ON rs.schema_id = ro.schema_id
         JOIN [${escapeMssqlId(database)}].sys.objects po ON po.object_id = fkc.parent_object_id
         JOIN [${escapeMssqlId(database)}].sys.schemas ps ON ps.schema_id = po.schema_id
         WHERE ps.name = @schema AND po.name = @table`);
    return res.recordset.map((row) => ({
      constraintName: row.constraint_name,
      column: row.column_name,
      referencedSchema: row.ref_schema,
      referencedTable: row.ref_table,
      referencedColumn: row.ref_column,
    }));
  }
  async getConstraints(
    database: string,
    schema: string,
    table: string,
  ): Promise<import("./types").TableConstraintMeta[]> {
    const constraints = await super.getConstraints(database, schema, table);
    const res = await this.requirePool()
      .request()
      .input("schema", mssql.NVarChar, schema)
      .input("table", mssql.NVarChar, table)
      .query<{
        constraint_name: string;
        definition: string | null;
      }>(`SELECT cc.name AS constraint_name,
               cc.definition AS definition
        FROM [${escapeMssqlId(database)}].sys.check_constraints cc
        JOIN [${escapeMssqlId(database)}].sys.objects o ON o.object_id = cc.parent_object_id
        JOIN [${escapeMssqlId(database)}].sys.schemas s ON s.schema_id = o.schema_id
        WHERE s.name = @schema AND o.name = @table
        ORDER BY cc.name`);
    constraints.push(
      ...res.recordset.map((row) => ({
        name: row.constraint_name,
        kind: "check" as const,
        columns: [],
        checkExpression: row.definition ?? undefined,
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
    const res = await this.requirePool()
      .request()
      .input("schema", mssql.NVarChar, schema)
      .input("table", mssql.NVarChar, table)
      .query<{
        trigger_name: string;
        is_disabled: boolean | number | null;
        is_instead_of_trigger: boolean | number | null;
        is_insert: boolean | number | null;
        is_update: boolean | number | null;
        is_delete: boolean | number | null;
        definition: string | null;
      }>(`SELECT tr.name AS trigger_name,
               tr.is_disabled,
               tr.is_instead_of_trigger,
               OBJECTPROPERTY(tr.object_id, 'ExecIsInsertTrigger') AS is_insert,
               OBJECTPROPERTY(tr.object_id, 'ExecIsUpdateTrigger') AS is_update,
               OBJECTPROPERTY(tr.object_id, 'ExecIsDeleteTrigger') AS is_delete,
               sm.definition AS definition
        FROM [${escapeMssqlId(database)}].sys.triggers tr
        JOIN [${escapeMssqlId(database)}].sys.tables tbl ON tbl.object_id = tr.parent_id
        JOIN [${escapeMssqlId(database)}].sys.schemas sch ON sch.schema_id = tbl.schema_id
        LEFT JOIN [${escapeMssqlId(database)}].sys.sql_modules sm ON sm.object_id = tr.object_id
        WHERE sch.name = @schema AND tbl.name = @table
        ORDER BY tr.name`);
    return res.recordset.map((row) => {
      const events: import("./types").TriggerMeta["events"] = [];
      if (isSetFlag(row.is_insert)) {
        events.push("insert");
      }
      if (isSetFlag(row.is_update)) {
        events.push("update");
      }
      if (isSetFlag(row.is_delete)) {
        events.push("delete");
      }
      if (events.length === 0) {
        events.push("unknown");
      }
      return {
        name: row.trigger_name,
        timing: isSetFlag(row.is_instead_of_trigger) ? "instead_of" : "after",
        events,
        orientation: "statement",
        enabled: !isSetFlag(row.is_disabled),
        definition: row.definition ?? undefined,
      };
    });
  }
  override async getTriggerDDL(
    database: string,
    schema: string,
    table: string,
    triggerName: string,
  ): Promise<string> {
    const res = await this.requirePool()
      .request()
      .input("schema", mssql.NVarChar, schema)
      .input("table", mssql.NVarChar, table)
      .input("triggerName", mssql.NVarChar, triggerName)
      .query<{ definition: string | null }>(
        `SELECT sm.definition AS definition
       FROM [${escapeMssqlId(database)}].sys.triggers tr
       JOIN [${escapeMssqlId(database)}].sys.tables tbl ON tbl.object_id = tr.parent_id
       JOIN [${escapeMssqlId(database)}].sys.schemas sch ON sch.schema_id = tbl.schema_id
       LEFT JOIN [${escapeMssqlId(database)}].sys.sql_modules sm ON sm.object_id = tr.object_id
       WHERE sch.name = @schema
         AND tbl.name = @table
         AND tr.name = @triggerName`,
      );
    const definition = res.recordset[0]?.definition?.trim();
    if (!definition) {
      throw new Error(`Trigger "${triggerName}" not found`);
    }
    return definition.endsWith(";") ? definition : `${definition};`;
  }
  async getCreateTableDDL(
    database: string,
    schema: string,
    table: string,
  ): Promise<string> {
    const objectTypeRes = await this.requirePool()
      .request()
      .input("schema", mssql.NVarChar, schema)
      .input("table", mssql.NVarChar, table)
      .query<ObjectTypeRow>(`SELECT TABLE_TYPE
         FROM [${escapeMssqlId(database)}].INFORMATION_SCHEMA.TABLES
         WHERE TABLE_SCHEMA = @schema AND TABLE_NAME = @table`);
    if (objectTypeRes.recordset[0]?.TABLE_TYPE === "VIEW") {
      const viewDef = await this.requirePool()
        .request()
        .query<RoutineDefinitionRow>(
          `SELECT OBJECT_DEFINITION(OBJECT_ID('[${escapeMssqlId(database)}].[${escapeMssqlId(schema)}].[${escapeMssqlId(table)}]')) AS def`,
        );
      return (
        viewDef.recordset[0]?.def ??
        `-- DDL not available for "${schema}"."${table}"`
      );
    }
    const cols = await this.requirePool()
      .request()
      .input("schema", mssql.NVarChar, schema)
      .input("table", mssql.NVarChar, table)
      .query<DdlColumnRow>(`SELECT
           c.name                              AS COLUMN_NAME,
           TYPE_NAME(c.user_type_id)           AS DATA_TYPE,
           c.max_length,
           c.precision,
           c.scale,
           c.is_nullable                       AS IS_NULLABLE,
           c.is_identity,
           c.is_computed,
           cc.definition                       AS COMPUTED_DEFINITION,
           cc.is_persisted,
           OBJECT_DEFINITION(c.default_object_id) AS COLUMN_DEFAULT,
           CASE WHEN pk.column_id IS NOT NULL THEN 1 ELSE 0 END AS IS_PK,
           pk.key_ordinal                      AS PK_ORDINAL
         FROM [${escapeMssqlId(database)}].sys.columns c
         JOIN [${escapeMssqlId(database)}].sys.objects  o ON o.object_id = c.object_id
         JOIN [${escapeMssqlId(database)}].sys.schemas  s ON s.schema_id  = o.schema_id
         LEFT JOIN [${escapeMssqlId(database)}].sys.computed_columns cc
           ON cc.object_id = c.object_id AND cc.column_id = c.column_id
         LEFT JOIN (
           SELECT ic.object_id, ic.column_id, ic.key_ordinal
           FROM [${escapeMssqlId(database)}].sys.index_columns ic
           JOIN [${escapeMssqlId(database)}].sys.indexes       i
             ON i.object_id = ic.object_id AND i.index_id = ic.index_id
           WHERE i.is_primary_key = 1
         ) pk ON pk.object_id = c.object_id AND pk.column_id = c.column_id
         WHERE s.name = @schema AND o.name = @table
         ORDER BY c.column_id`);
    const pkCols = cols.recordset
      .filter((row) => row.IS_PK === 1)
      .sort(
        (left, right) =>
          (left.PK_ORDINAL ?? Number.MAX_SAFE_INTEGER) -
          (right.PK_ORDINAL ?? Number.MAX_SAFE_INTEGER),
      )
      .map((row) => this.quoteIdentifier(row.COLUMN_NAME));
    const colDefs = cols.recordset.map((row) => {
      if (isSetFlag(row.is_computed) && row.COMPUTED_DEFINITION) {
        const persisted = isSetFlag(row.is_persisted) ? " PERSISTED" : "";
        return `  ${this.quoteIdentifier(row.COLUMN_NAME)} AS (${row.COMPUTED_DEFINITION})${persisted}`;
      }
      const typ = mssqlFullType(
        row.DATA_TYPE,
        row.max_length,
        row.precision,
        row.scale,
      );
      const nullable = isSetFlag(row.IS_NULLABLE) ? "" : " NOT NULL";
      const identity = isSetFlag(row.is_identity) ? " IDENTITY(1,1)" : "";
      const def = row.COLUMN_DEFAULT ? ` DEFAULT ${row.COLUMN_DEFAULT}` : "";
      const pk = pkCols.length === 1 && row.IS_PK === 1 ? " PRIMARY KEY" : "";
      return `  ${this.quoteIdentifier(row.COLUMN_NAME)} ${typ}${identity}${nullable}${def}${pk}`;
    });
    if (pkCols.length > 1) {
      colDefs.push(`  PRIMARY KEY (${pkCols.join(", ")})`);
    }
    return `CREATE TABLE ${this.qualifiedTableName("", schema, table)} (\n${colDefs.join(",\n")}\n);`;
  }
  async getObjectDefinition(
    database: string,
    schema: string,
    name: string,
    kind: DdlOnlyDbObjectKind,
  ): Promise<string | null> {
    return kind === "sequence"
      ? this.getSequenceDefinition(database, schema, name)
      : this.getTypeDefinition(database, schema, name);
  }
  async getRoutineDefinition(
    database: string,
    schema: string,
    name: string,
    _kind: "function" | "procedure",
    _routineIdentity?: string,
  ): Promise<string> {
    const res = await this.requirePool()
      .request()
      .query<RoutineDefinitionRow>(
        `SELECT OBJECT_DEFINITION(OBJECT_ID('[${escapeMssqlId(database)}].[${escapeMssqlId(schema)}].[${escapeMssqlId(name)}]')) AS def`,
      );
    const def = res.recordset[0]?.def ?? null;
    return def ?? `-- Definition not available for [${schema}].[${name}]`;
  }

  private async getSequenceDefinition(
    database: string,
    schema: string,
    name: string,
  ): Promise<string | null> {
    const res = await this.requirePool()
      .request()
      .input("schema", mssql.NVarChar, schema)
      .input("name", mssql.NVarChar, name)
      .query<{
        start_value: string | number;
        increment: string | number;
        minimum_value: string | number;
        maximum_value: string | number;
        is_cycling: boolean | number;
        cache_size: string | number;
      }>(`SELECT start_value,
                 increment,
                 minimum_value,
                 maximum_value,
                 is_cycling,
                 cache_size
          FROM [${escapeMssqlId(database)}].sys.sequences seq
          JOIN [${escapeMssqlId(database)}].sys.schemas s ON s.schema_id = seq.schema_id
          WHERE s.name = @schema
            AND seq.name = @name`);
    const row = res.recordset[0];
    if (!row) {
      return null;
    }

    const clauses = [
      `CREATE SEQUENCE ${this.qualifiedTableName("", schema, name)}`,
      `START WITH ${row.start_value}`,
      `INCREMENT BY ${row.increment}`,
      `MINVALUE ${row.minimum_value}`,
      `MAXVALUE ${row.maximum_value}`,
      isSetFlag(row.is_cycling) ? "CYCLE" : "NO CYCLE",
      `CACHE ${row.cache_size}`,
    ];
    return `${clauses.join(" ")};`;
  }

  private async getTypeDefinition(
    database: string,
    schema: string,
    name: string,
  ): Promise<string | null> {
    const res = await this.requirePool()
      .request()
      .input("schema", mssql.NVarChar, schema)
      .input("name", mssql.NVarChar, name)
      .query<{
        base_type: string;
        max_length: number;
        precision: number;
        scale: number;
        is_nullable: boolean | number;
      }>(`SELECT bt.name AS base_type,
                 t.max_length,
                 t.precision,
                 t.scale,
                 t.is_nullable
          FROM [${escapeMssqlId(database)}].sys.types t
          JOIN [${escapeMssqlId(database)}].sys.schemas s ON s.schema_id = t.schema_id
          JOIN [${escapeMssqlId(database)}].sys.types bt ON bt.user_type_id = t.system_type_id AND bt.user_type_id = bt.system_type_id
          WHERE s.name = @schema
            AND t.name = @name
            AND t.is_user_defined = 1
            AND t.is_table_type = 0`);
    const row = res.recordset[0];
    if (!row) {
      return null;
    }

    const baseType = mssqlFullType(
      row.base_type,
      row.max_length,
      row.precision,
      row.scale,
    );
    const nullability = isSetFlag(row.is_nullable) ? "NULL" : "NOT NULL";
    return `CREATE TYPE ${this.qualifiedTableName("", schema, name)} FROM ${baseType} ${nullability};`;
  }
  async runTransaction(
    operations: import("./types").TransactionOperation[],
  ): Promise<void> {
    const tx = new mssql.Transaction(this.requirePool());
    await tx.begin();
    try {
      for (const op of operations) {
        const req = tx.request();
        const res = await this.executeTrackedRequest(
          req,
          async (trackedReq) => {
            const finalSql = this.bindPositionalParameters(
              trackedReq,
              op.sql,
              op.params,
            );
            return await trackedReq.query(finalSql);
          },
        );
        if (op.checkAffectedRows && (res.rowsAffected?.[0] ?? 0) === 0) {
          throw new Error(
            "Row not found — the row may have been modified or deleted by another user",
          );
        }
      }
      await tx.commit();
    } catch (e) {
      try {
        await tx.rollback();
      } catch {}
      throw e;
    }
  }
  mapTypeCategory(nativeType: string): TypeCategory {
    const ct = baseTypeName(nativeType);
    if (ct === "bit") return "integer";
    if (["tinyint", "smallint", "int", "bigint"].includes(ct)) return "integer";
    if (["real", "float"].includes(ct)) return "float";
    if (["decimal", "numeric", "money", "smallmoney"].includes(ct))
      return "decimal";
    if (ct === "date") return "date";
    if (ct === "time") return "time";
    if (
      ["datetime", "datetime2", "datetimeoffset", "smalldatetime"].includes(ct)
    ) {
      return "datetime";
    }
    if (ct === "timestamp" || ct === "rowversion") return "binary";
    if (["binary", "varbinary", "image"].includes(ct)) return "binary";
    if (ct === "uniqueidentifier") return "uuid";
    if (["text", "ntext", "xml"].includes(ct)) return "text";
    if (["geography", "geometry"].includes(ct)) return "spatial";
    if (ct === "hierarchyid" || ct === "sql_variant") return "other";
    if (ct.includes("char") || ct.includes("varchar")) return "text";
    return "other";
  }
  protected getValueSemantics(
    nativeType: string,
    _category: TypeCategory,
  ): ValueSemantics {
    return baseTypeName(nativeType) === "bit" ? "bit" : "plain";
  }
  isDatetimeWithTime(nativeType: string): boolean {
    const ct = baseTypeName(nativeType);
    return [
      "datetime",
      "datetime2",
      "datetimeoffset",
      "smalldatetime",
    ].includes(ct);
  }
  protected override isFilterable(
    nativeType: string,
    category: TypeCategory,
  ): boolean {
    if (category === "spatial") {
      return !MSSQL_FILTER_DENYLIST.has(baseTypeName(nativeType));
    }
    return (
      super.isFilterable(nativeType, category) &&
      !MSSQL_FILTER_DENYLIST.has(baseTypeName(nativeType))
    );
  }
  override quoteIdentifier(name: string): string {
    return `[${name.replace(/]/g, "]]")}]`;
  }
  override qualifiedTableName(
    database: string,
    schema: string,
    table: string,
  ): string {
    const parts: string[] = [];
    if (database) {
      parts.push(this.quoteIdentifier(database));
    }
    if (schema) {
      parts.push(this.quoteIdentifier(schema));
    }
    parts.push(this.quoteIdentifier(table));
    return parts.join(".");
  }
  override buildPagination(
    offset: number,
    limit: number,
    _paramIndex: number,
  ): PaginationResult {
    return {
      sql: `OFFSET ? ROWS FETCH NEXT ? ROWS ONLY`,
      params: [offset, limit],
    };
  }
  override buildInsertValueExpr(
    column: ColumnTypeMeta,
    _paramIndex: number,
  ): string {
    if (column.category === "decimal") {
      return `CAST(? AS ${column.nativeType})`;
    }
    return "?";
  }
  override buildSetExpr(column: ColumnTypeMeta, _paramIndex: number): string {
    const expr = this.buildInsertValueExpr(column, _paramIndex);
    return `${this.quoteIdentifier(column.name)} = ${expr}`;
  }
  materializePreviewInsertSql(
    sql: string,
    params: readonly unknown[] | undefined,
    columns: readonly ColumnTypeMeta[],
  ): string {
    return this.materializePreviewColumnSql(sql, params, columns);
  }
  materializePreviewColumnSql(
    sql: string,
    params: readonly unknown[] | undefined,
    columns: readonly (ColumnTypeMeta | undefined)[],
  ): string {
    if (!params || params.length === 0) {
      return sql;
    }
    const placeholderCount = (sql.match(/\?/g) ?? []).length;
    if (placeholderCount !== params.length) {
      throw new Error(
        `[RapiDB] Preview parameter mismatch: SQL has ${placeholderCount} placeholder(s) but ${params.length} value(s) were supplied.`,
      );
    }
    let index = 0;
    return sql.replace(/\?/g, () => {
      const value = params[index];
      const column = columns[index];
      index += 1;
      if (typeof value === "string" && column) {
        return formatMssqlStringPreviewLiteral(value, column.nativeType);
      }
      return this.formatPreviewSqlLiteral(value);
    });
  }
  protected override formatPreviewSqlLiteral(value: unknown): string {
    if (typeof value === "string") {
      return formatMssqlStringPreviewLiteral(value);
    }
    if (typeof value === "boolean") {
      return value ? "1" : "0";
    }
    if (Buffer.isBuffer(value)) {
      return formatHexSqlPreviewLiteral(value, { prefix: "0x" });
    }
    if (value instanceof ArrayBuffer) {
      return formatHexSqlPreviewLiteral(Buffer.from(new Uint8Array(value)), {
        prefix: "0x",
      });
    }
    if (ArrayBuffer.isView(value)) {
      return formatHexSqlPreviewLiteral(
        Buffer.from(value.buffer, value.byteOffset, value.byteLength),
        { prefix: "0x" },
      );
    }
    return super.formatPreviewSqlLiteral(value);
  }
  override buildOrderByDefault(columns: ColumnTypeMeta[]): string {
    const pkCols = columns
      .filter((column) => column.isPrimaryKey)
      .sort(
        (left, right) =>
          (left.primaryKeyOrdinal ?? Number.MAX_SAFE_INTEGER) -
          (right.primaryKeyOrdinal ?? Number.MAX_SAFE_INTEGER),
      );
    if (pkCols.length > 0) {
      return `ORDER BY ${pkCols.map((column) => this.quoteIdentifier(column.name)).join(", ")}`;
    }
    if (columns.length > 0) {
      return `ORDER BY ${this.quoteIdentifier(columns[0].name)}`;
    }
    return "ORDER BY (SELECT NULL)";
  }
  override coerceInputValue(value: unknown, column: ColumnTypeMeta): unknown {
    return this.normalizeInputValue(value, column);
  }
  override formatOutputValue(value: unknown, column: ColumnTypeMeta): unknown {
    if (value === null || value === undefined) return value;
    if (this.hasBitSemantics(column)) {
      if (value === true || value === 1 || value === "1") return 1;
      if (value === false || value === 0 || value === "0") return 0;
    }
    if (Buffer.isBuffer(value)) return super.formatOutputValue(value, column);
    if (typeof value === "bigint") return value.toString();
    if (value instanceof Date && !Number.isNaN(value.getTime())) {
      const ct = baseTypeName(column.nativeType);
      const pad = (n: number) => String(n).padStart(2, "0");
      if (ct === "date") {
        return `${formatMssqlYear(value.getFullYear())}-${pad(value.getMonth() + 1)}-${pad(value.getDate())}`;
      }
      if (ct === "time") {
        const ms = value.getMilliseconds();
        const frac =
          ms > 0 ? `.${String(ms).padStart(3, "0").replace(/0+$/, "")}` : "";
        return `${pad(value.getHours())}:${pad(value.getMinutes())}:${pad(value.getSeconds())}${frac}`;
      }
      if (ct === "datetimeoffset") {
        const ms = value.getMilliseconds();
        const frac =
          ms > 0 ? `.${String(ms).padStart(3, "0").replace(/0+$/, "")}` : "";
        const offsetMinutes = -value.getTimezoneOffset();
        return `${formatMssqlYear(value.getFullYear())}-${pad(value.getMonth() + 1)}-${pad(value.getDate())} ${pad(value.getHours())}:${pad(value.getMinutes())}:${pad(value.getSeconds())}${frac}${formatMssqlOffsetMinutes(offsetMinutes)}`;
      }
      if (ct === "datetime" || ct === "datetime2" || ct === "smalldatetime") {
        const ms = value.getMilliseconds();
        const frac =
          ms > 0 ? `.${String(ms).padStart(3, "0").replace(/0+$/, "")}` : "";
        return `${formatMssqlYear(value.getFullYear())}-${pad(value.getMonth() + 1)}-${pad(value.getDate())} ${pad(value.getHours())}:${pad(value.getMinutes())}:${pad(value.getSeconds())}${frac}`;
      }
      const formatted = formatDatetimeForDisplay(value);
      if (formatted !== null) return formatted;
    }
    if (typeof value === "string") {
      const ct = baseTypeName(column.nativeType);
      if (ct === "datetimeoffset") {
        return normalizeDatetimeLiteral(value).replace("T", " ");
      }
    }
    return value;
  }
  override checkPersistedEdit(
    column: ColumnTypeMeta,
    expectedValue: unknown,
    options?: PersistedEditCheckOptions,
  ): PersistedEditCheckResult | null {
    if (this.hasBitSemantics(column)) {
      return this.checkNormalizedPersistedEdit(
        column,
        expectedValue,
        options,
        canonicalizeMssqlBitPersistedEditValue,
        `Column "${column.name}" expects 0 or 1.`,
      );
    }
    if (column.category === "integer") {
      return this.checkExactNumericPersistedEdit(
        column,
        expectedValue,
        { precision: null, scale: 0 },
        options,
      );
    }
    const baseType = baseTypeName(column.nativeType);
    if (column.category === "decimal") {
      if (baseType === "money") {
        return this.checkExactNumericPersistedEdit(
          column,
          expectedValue,
          { precision: 19, scale: 4 },
          options,
        );
      }
      if (baseType === "smallmoney") {
        return this.checkExactNumericPersistedEdit(
          column,
          expectedValue,
          { precision: 10, scale: 4 },
          options,
        );
      }
      if (!["decimal", "numeric"].includes(baseType)) {
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
      return this.checkApproximateNumericPersistedEdit(
        column,
        expectedValue,
        mssqlFloatSignificantDigits(column.nativeType),
        options,
      );
    }
    if (this.hasBooleanSemantics(column)) {
      return this.checkBooleanPersistedEdit(column, expectedValue, options);
    }
    if (column.category === "uuid") {
      return this.checkUuidPersistedEdit(column, expectedValue, options);
    }
    if (column.category === "binary") {
      return this.checkBinaryPersistedEdit(column, expectedValue, options);
    }
    if (
      column.category === "text" ||
      column.category === "date" ||
      column.category === "time" ||
      column.category === "datetime"
    ) {
      if (
        [
          "date",
          "time",
          "datetime",
          "datetime2",
          "datetimeoffset",
          "smalldatetime",
        ].includes(baseType)
      ) {
        return this.checkNormalizedPersistedEdit(
          column,
          expectedValue,
          options,
          (value) => {
            if (value === NULL_SENTINEL || value === null) {
              return { canonical: String(NULL_SENTINEL) };
            }
            const canonical = canonicalizeMssqlTemporalPersistedEditValue(
              value,
              baseType,
            );
            return canonical === null ? null : { canonical };
          },
          `Column "${column.name}" expects a temporal value.`,
        );
      }
      if (["char", "nchar"].includes(baseType)) {
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
    _paramIndex: number,
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
      if (arrayValue.length > MSSQL_LIKE_MAX_NVARCHAR_CHARS) {
        return {
          sql: `CAST(${col} AS NVARCHAR(MAX)) = CAST(? AS NVARCHAR(MAX))`,
          params: [arrayValue],
        };
      }
      return {
        sql: `CHARINDEX(CAST(? AS NVARCHAR(MAX)), CAST(${col} AS NVARCHAR(MAX))) > 0`,
        params: [arrayValue],
      };
    }
    if (
      column.category === "binary" &&
      typeof val === "string" &&
      (operator === "eq" || operator === "neq")
    ) {
      const sqlOp = operator === "neq" ? "<>" : "=";
      const leftExpr =
        baseTypeName(column.nativeType) === "image"
          ? `CONVERT(VARBINARY(MAX), ${col})`
          : col;
      const rightExpr =
        baseTypeName(column.nativeType) === "image"
          ? "CONVERT(VARBINARY(MAX), ?)"
          : "?";
      return {
        sql: `${leftExpr} ${sqlOp} ${rightExpr}`,
        params: [this.coerceInputValue(val, column)],
      };
    }
    if (
      (this.hasBooleanSemantics(column) || this.hasBitSemantics(column)) &&
      (operator === "eq" || operator === "neq")
    ) {
      const strVal = (typeof val === "string" ? val : val[0]).toLowerCase();
      const normalized = this.parseBooleanInput(strVal);
      if (normalized !== null) {
        const boolVal = normalized ? 1 : 0;
        const sqlOp = operator === "neq" ? "<>" : "=";
        return { sql: `${col} ${sqlOp} ?`, params: [boolVal] };
      }
      if (this.hasBitSemantics(column) && (strVal === "0" || strVal === "1")) {
        const sqlOp = operator === "neq" ? "<>" : "=";
        return { sql: `${col} ${sqlOp} ?`, params: [Number(strVal)] };
      }
    }
    if (column.category === "spatial" && typeof val === "string") {
      if (operator !== "eq" && operator !== "neq") {
        return null;
      }
      const searchValue = val.trim();
      if (!searchValue) {
        return null;
      }
      const sqlOp = operator === "neq" ? "<>" : "=";
      return {
        sql: `${col}.ToString() ${sqlOp} ?`,
        params: [searchValue],
      };
    }
    if (this.isNumericCategory(column.category) && Array.isArray(val)) {
      return {
        sql: `${col} BETWEEN ${mssqlNumericParamExpr(column)} AND ${mssqlNumericParamExpr(column)}`,
        params: [
          mssqlNumericFilterParam(column, val[0]),
          mssqlNumericFilterParam(column, val[1]),
        ],
      };
    }
    if (this.isNumericCategory(column.category) && typeof val === "string") {
      if (operator === "in") {
        const parts = val
          .split(",")
          .map((part) => mssqlNumericFilterParam(column, part.trim()));
        return {
          sql: `${col} IN (${parts.map(() => mssqlNumericParamExpr(column)).join(", ")})`,
          params: parts,
        };
      }
      if (!Number.isNaN(Number(val)) && val !== "") {
        if (
          column.category === "float" &&
          (operator === "eq" || operator === "neq")
        ) {
          const numericValue = Number(val);
          const tolerance = approximateNumericFilterTolerance(val);
          const toleranceExpr =
            "CASE WHEN ABS(?) * ? > ? THEN ABS(?) * ? ELSE ? END";
          const deltaExpr = `ABS(CAST(${col} AS float) - ?)`;
          return {
            sql:
              operator === "neq"
                ? `${deltaExpr} >= ${toleranceExpr}`
                : `${deltaExpr} < ${toleranceExpr}`,
            params: [
              numericValue,
              numericValue,
              tolerance,
              tolerance,
              numericValue,
              tolerance,
              tolerance,
            ],
          };
        }
        const sqlOp = this.sqlOperator(operator);
        return {
          sql: `${col} ${sqlOp} ${mssqlNumericParamExpr(column)}`,
          params: [mssqlNumericFilterParam(column, val)],
        };
      }
    }
    if (column.category === "date") {
      const v = typeof val === "string" ? val : val[0];
      if (Array.isArray(val)) {
        return {
          sql: `CONVERT(CHAR(10), ${col}, 23) BETWEEN ? AND ?`,
          params: [val[0], val[1]],
        };
      }
      if (operator === "in") {
        const parts = v.split(",").map((part) => part.trim());
        return {
          sql: `CONVERT(CHAR(10), ${col}, 23) IN (${parts.map(() => "?").join(", ")})`,
          params: parts,
        };
      }
      if (["eq", "neq", "gt", "gte", "lt", "lte"].includes(operator)) {
        const sqlOp = operator === "neq" ? "<>" : this.sqlOperator(operator);
        return { sql: `CONVERT(CHAR(10), ${col}, 23) ${sqlOp} ?`, params: [v] };
      }
      return {
        sql: `CONVERT(CHAR(10), ${col}, 23) LIKE ?`,
        params: [`%${v}%`],
      };
    }
    if (column.category === "time") {
      const v = typeof val === "string" ? val : val[0];
      if (Array.isArray(val)) {
        return {
          sql: `CAST(${col} AS time) BETWEEN CAST(? AS time) AND CAST(? AS time)`,
          params: [val[0], val[1]],
        };
      }
      if (operator === "in") {
        const parts = v.split(",").map((part) => part.trim());
        return {
          sql: `CAST(${col} AS time) IN (${parts.map(() => "CAST(? AS time)").join(", ")})`,
          params: parts,
        };
      }
      if (["eq", "neq", "gt", "gte", "lt", "lte"].includes(operator)) {
        if (operator === "eq" || operator === "neq") {
          const diffUpperBound = mssqlDisplayedTemporalDiffUpperBound(v);
          if (diffUpperBound !== null) {
            const diffExpr = `DATEDIFF(millisecond, CAST(? AS time), CAST(${col} AS time))`;
            const rangeExpr = `${diffExpr} BETWEEN 0 AND ?`;
            return {
              sql: operator === "neq" ? `NOT (${rangeExpr})` : rangeExpr,
              params: [v, diffUpperBound],
            };
          }
        }
        const sqlOp = operator === "neq" ? "<>" : this.sqlOperator(operator);
        return {
          sql: `CAST(${col} AS time) ${sqlOp} CAST(? AS time)`,
          params: [v],
        };
      }
      return {
        sql: `CONVERT(VARCHAR(16), ${col}, 114) LIKE ?`,
        params: [`%${v}%`],
      };
    }
    if (this.isDatetimeWithTime(column.nativeType)) {
      const v = typeof val === "string" ? val : val[0];
      const isDateTimeOffset =
        baseTypeName(column.nativeType) === "datetimeoffset";
      if (Array.isArray(val)) {
        return {
          sql: `${col} BETWEEN ? AND ?`,
          params: [
            normalizeDatetimeLiteral(val[0]),
            normalizeDatetimeLiteral(val[1]),
          ],
        };
      }
      if (operator === "in") {
        const parts = v
          .split(",")
          .map((part) => normalizeDatetimeLiteral(part.trim()));
        return {
          sql: `${col} IN (${parts.map(() => "?").join(", ")})`,
          params: parts,
        };
      }
      if (["eq", "neq", "gt", "gte", "lt", "lte"].includes(operator)) {
        if (operator === "eq" || operator === "neq") {
          const normalizedValue = normalizeDatetimeLiteral(v);
          const diffUpperBound = mssqlDisplayedTemporalDiffUpperBound(v);
          if (isDateTimeOffset && diffUpperBound === null) {
            const normalizedExpr = `REPLACE(REPLACE(REPLACE(CONVERT(VARCHAR(40), ${col}, 127), 'Z', '+00:00'), ' +', '+'), ' -', '-')`;
            return {
              sql:
                operator === "neq"
                  ? `NOT (${normalizedExpr} LIKE ?)`
                  : `${normalizedExpr} LIKE ?`,
              params: [`%${datetimeOffsetSearchLiteral(v)}%`],
            };
          }
          if (diffUpperBound !== null) {
            const castType =
              baseTypeName(column.nativeType) === "datetimeoffset"
                ? "datetimeoffset(7)"
                : "datetime2(7)";
            const diffExpr = `DATEDIFF(millisecond, CAST(? AS ${castType}), ${col})`;
            const rangeExpr = `${diffExpr} BETWEEN 0 AND ?`;
            return {
              sql: operator === "neq" ? `NOT (${rangeExpr})` : rangeExpr,
              params: [normalizedValue, diffUpperBound],
            };
          }
        }
        const sqlOp = operator === "neq" ? "<>" : this.sqlOperator(operator);
        return {
          sql: `${col} ${sqlOp} ?`,
          params: [normalizeDatetimeLiteral(v)],
        };
      }
      if (isDateTimeOffset) {
        const normalizedExpr = `REPLACE(REPLACE(REPLACE(CONVERT(VARCHAR(40), ${col}, 127), 'Z', '+00:00'), ' +', '+'), ' -', '-')`;
        return {
          sql: `${normalizedExpr} LIKE ?`,
          params: [`%${datetimeOffsetSearchLiteral(v)}%`],
        };
      }
      return {
        sql: `CONVERT(VARCHAR(33), ${col}, 126) LIKE ?`,
        params: [`%${temporalSearchLiteral(v)}%`],
      };
    }
    if (operator === "between" && Array.isArray(val)) {
      return { sql: `${col} BETWEEN ? AND ?`, params: [val[0], val[1]] };
    }
    if (operator === "in" && typeof val === "string") {
      const parts = val.split(",").map((part) => part.trim());
      return {
        sql: `${col} IN (${parts.map(() => "?").join(", ")})`,
        params: parts,
      };
    }
    const v = typeof val === "string" ? val : val[0];
    if (operator === "like" || operator === "ilike") {
      if (v.length > MSSQL_LIKE_MAX_NVARCHAR_CHARS) {
        return {
          sql: `CAST(${col} AS NVARCHAR(MAX)) = CAST(? AS NVARCHAR(MAX))`,
          params: [v],
        };
      }
      return {
        sql: `CHARINDEX(CAST(? AS NVARCHAR(MAX)), CAST(${col} AS NVARCHAR(MAX))) > 0`,
        params: [v],
      };
    }
    if (operator === "eq" || operator === "neq") {
      const sqlOp = operator === "neq" ? "<>" : "=";
      return { sql: `${col} ${sqlOp} ?`, params: [v] };
    }
    return {
      sql: `CAST(${col} AS NVARCHAR(MAX)) LIKE ?`,
      params: [`%${v}%`],
    };
  }
}
