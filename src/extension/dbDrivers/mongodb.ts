import vm from "node:vm";
import {
  Binary,
  BSONRegExp,
  BSONSymbol,
  Code,
  DBRef,
  Decimal128,
  Int32,
  Long,
  MaxKey,
  MinKey,
  MongoClient,
  ObjectId,
  Timestamp,
  UUID,
} from "mongodb";
import { QUERY_LIMIT_POLICY } from "../../shared/safetyContracts";
import type { ConnectionConfig } from "../connectionManager";
import { allowReadOnlyQuery, denyReadOnlyQuery } from "../utils/readOnlyGuards";
import { formatDatetimeForDisplay } from "./BaseDBDriver";
import {
  applyFilters,
  applySort,
  inferColumnsFromRows,
  pageRows,
  unsupported,
} from "./nosqlUtils";
import type {
  ColumnMeta,
  ColumnTypeMeta,
  DatabaseInfo,
  DriverDeleteRowsRequest,
  DriverEntityManifest,
  DriverInsertRowRequest,
  DriverMutationResult,
  DriverTablePageRequest,
  DriverTablePageResult,
  DriverUpdateRowsRequest,
  FilterExpression,
  FilterOperator,
  ForeignKeyMeta,
  IDBDriver,
  IndexMeta,
  PaginationResult,
  QueryResult,
  SchemaInfo,
  TableConstraintMeta,
  TableInfo,
  TransactionOperation,
  TriggerMeta,
  TypeCategory,
} from "./types";
import { resolveFilterOperators } from "./types";

const MONGODB_ENTITY_MANIFEST: DriverEntityManifest = {
  dbObjectKinds: ["table", "view"],
  tableSections: {
    columns: "supported",
    constraints: "not_applicable",
    indexes: "supported",
    triggers: "not_applicable",
  },
};

type MongoSchemaType = {
  category: TypeCategory;
  nativeType: string;
  bsonSubtype?: number;
};

type MongoshChainOperation = {
  op: string;
  args: unknown[];
};

type MongoshOperation = {
  dbName?: string;
  collName?: string;
  op: string;
  args: unknown[];
  chainOps: MongoshChainOperation[];
};

const MONGO_SCALAR_MISS = Symbol("mongo-scalar-miss");
const UUID_VALUE_RE =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const BASE64_VALUE_RE =
  /^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/;
const DISPLAY_DATETIME_RE =
  /^(\d{4}-\d{2}-\d{2})(?:[ T](\d{2}:\d{2}:\d{2})(\.\d{1,3})?)?(?: ?(Z|[+-]\d{2}(?::?\d{2})?))?$/;
const TIMESTAMP_LITERAL_RE = /^Timestamp\((\d+),\s*(\d+)\)$/i;

const MONGODB_READ_ONLY_QUERY_REASON =
  "[RapiDB] Read-only MongoDB connections allow only find, findOne, countDocuments, and aggregate queries without $out or $merge.";
const MONGODB_QUERY_HARD_CAP = QUERY_LIMIT_POLICY.hardCap;
const MONGOSH_VM_TIMEOUT_MS = 5000;
const MONGOSH_UNSAFE_TOKENS = [
  "process",
  "globalThis",
  "Function",
  "eval",
  "require",
  "import",
  "module",
  "constructor",
  "prototype",
] as const;
const MONGOSH_UNSAFE_TOKEN_RE = new RegExp(
  `(^|[^\\w$])(${MONGOSH_UNSAFE_TOKENS.join("|")})(?=$|[^\\w$])`,
);
const MONGOSH_UNSAFE_PATTERN_RULES: Array<{
  re: RegExp;
  reason: string;
}> = [
  { re: /(^|[^\w$])(__\w+__)(?=$|[^\w$])/, reason: "double-underscore key" },
  {
    re: /(^|[^\w$])(while|for|try|catch)(?=$|[^\w$])/,
    reason: "control-flow statement",
  },
  { re: /=>/, reason: "arrow function" },
];

function normalizeMongoshQueryText(queryText: string): string {
  return queryText
    .split("\n")
    .filter((line) => !line.trim().startsWith("//"))
    .join("\n")
    .trim()
    .replace(/;+\s*$/, "")
    .trim();
}

function ensureMongoshQueryIsSafe(queryText: string): void {
  const tokenMatch = MONGOSH_UNSAFE_TOKEN_RE.exec(queryText);
  if (tokenMatch) {
    throw new Error(
      `[RapiDB] Unsafe mongosh query blocked: disallowed token "${tokenMatch[2]}".`,
    );
  }

  for (const rule of MONGOSH_UNSAFE_PATTERN_RULES) {
    if (rule.re.test(queryText)) {
      throw new Error(
        `[RapiDB] Unsafe mongosh query blocked: disallowed ${rule.reason}.`,
      );
    }
  }
}

function executeMongoshParserInVm(
  queryText: string,
  sandbox: Record<string, unknown>,
): void {
  ensureMongoshQueryIsSafe(queryText);
  const context = vm.createContext(sandbox, {
    codeGeneration: {
      strings: false,
      wasm: false,
    },
  });
  vm.runInContext(queryText, context, {
    timeout: MONGOSH_VM_TIMEOUT_MS,
  });
}

function parseMongoshOperations(queryText: string): MongoshOperation[] {
  const operations: MongoshOperation[] = [];

  const createChainProxy = (operation: MongoshOperation): object => {
    return new Proxy({} as Record<string, unknown>, {
      get(_t, method: string | symbol) {
        if (typeof method !== "string") return undefined;
        return (...args: unknown[]) => {
          operation.chainOps.push({ op: method, args });
          return createChainProxy(operation);
        };
      },
    });
  };

  const createCollProxy = (
    dbName: string | undefined,
    coll: string,
  ): object => {
    return new Proxy({} as Record<string, unknown>, {
      get(_t, method: string | symbol) {
        if (typeof method !== "string") return undefined;
        if (method === "then") return undefined;
        return (...args: unknown[]) => {
          const operation: MongoshOperation = {
            dbName,
            collName: coll,
            op: method,
            args,
            chainOps: [],
          };
          operations.push(operation);
          return createChainProxy(operation);
        };
      },
    });
  };

  const createDbProxy = (dbName?: string): object => {
    return new Proxy({} as Record<string, unknown>, {
      get(_t, prop: string | symbol) {
        if (typeof prop !== "string") return undefined;
        if (prop === "getSiblingDB") {
          return (name: string) => createDbProxy(name);
        }
        if (prop === "getCollection") {
          return (name: string) => createCollProxy(dbName, String(name));
        }
        if (prop === "runCommand") {
          return (cmd: unknown) => {
            const operation: MongoshOperation = {
              dbName,
              op: "runCommand",
              args: [cmd],
              chainOps: [],
            };
            operations.push(operation);
            return createChainProxy(operation);
          };
        }
        if (prop === "createCollection") {
          return (name: string, options?: unknown) => {
            operations.push({
              dbName,
              op: "createCollection",
              args: [name, options],
              chainOps: [],
            });
          };
        }
        if (prop === "createView") {
          return (
            name: string,
            viewOn: string,
            pipeline?: unknown,
            options?: unknown,
          ) => {
            operations.push({
              dbName,
              op: "createView",
              args: [name, viewOn, pipeline, options],
              chainOps: [],
            });
          };
        }
        return createCollProxy(dbName, prop);
      },
    });
  };

  const sandbox = {
    db: createDbProxy(),
    process: undefined,
    globalThis: undefined,
    Function: undefined,
    eval: undefined,
    require: undefined,
    import: undefined,
    module: undefined,
    constructor: undefined,
    prototype: undefined,
    Date: function mongoDate(value: string) {
      return new globalThis.Date(value);
    },
    RegExp: function mongoRegExp(pattern: string, flags?: string) {
      return new globalThis.RegExp(pattern, flags);
    },
    ObjectId: function mongoObjectId(hex: string) {
      return new ObjectId(hex);
    },
    ISODate: function mongoIsoDate(value: string) {
      return new Date(value);
    },
    BinData: function mongoBinData(subtype: number | string, base64: string) {
      return new Binary(Buffer.from(String(base64), "base64"), Number(subtype));
    },
    DBRef: function mongoDBRef(collection: string, oid: unknown, db?: string) {
      return new DBRef(
        String(collection),
        oid as ObjectId,
        db ? String(db) : undefined,
      );
    },
    BSONSymbol: function mongoBSONSymbol(value: string) {
      return new BSONSymbol(String(value));
    },
    NumberLong: function mongoNumberLong(value: number | string) {
      return Long.fromString(String(value));
    },
    NumberInt: function mongoNumberInt(value: number | string) {
      return new Int32(Number.parseInt(String(value), 10));
    },
    NumberDecimal: function mongoNumberDecimal(value: string) {
      return Decimal128.fromString(value);
    },
    Timestamp: function mongoTimestamp(
      secondsOrSpec:
        | number
        | string
        | {
            t?: unknown;
            i?: unknown;
          },
      increment?: number | string,
    ) {
      if (secondsOrSpec && typeof secondsOrSpec === "object") {
        return new Timestamp({
          t: Number((secondsOrSpec as { t?: unknown }).t ?? 0),
          i: Number((secondsOrSpec as { i?: unknown }).i ?? 0),
        });
      }
      return new Timestamp({
        t: Number(secondsOrSpec),
        i: Number(increment ?? 0),
      });
    },
    Code: function mongoCode(source: string, scope?: unknown) {
      return scope !== undefined && scope !== null && typeof scope === "object"
        ? new Code(String(source), scope as Record<string, unknown>)
        : new Code(String(source));
    },
    MinKey: function mongoMinKey() {
      return new MinKey();
    },
    MaxKey: function mongoMaxKey() {
      return new MaxKey();
    },
  };

  try {
    executeMongoshParserInVm(queryText, sandbox);
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    if (message.includes(".then") || message.includes(".catch")) {
      throw new Error(
        `[RapiDB] Promise chaining is not allowed in mongosh queries. Use basic operations only.\n\nExamples:\n  db.users.find({})\n  db.users.find({ name: "Alice" }).limit(10)\n  db.users.insertOne({ name: "Alice" })\n  db.users.updateMany({ status: "active" }, { $set: { updated: true } })\n  db.users.deleteMany({ _id: ObjectId("507f1f77bcf86cd799439011") })\n  db.runCommand({ ping: 1 })\n  db.getSiblingDB("mydb").users.find({})`,
      );
    }
    throw new Error(
      `mongosh error: ${message}\n\nExamples:\n  db.users.find({})\n  db.users.find({ name: "Alice" }).limit(10)\n  db.users.insertOne({ name: "Alice" })\n  db.users.updateMany({ status: "active" }, { $set: { updated: true } })\n  db.users.deleteMany({ _id: ObjectId("507f1f77bcf86cd799439011") })\n  db.runCommand({ ping: 1 })\n  db.getSiblingDB("mydb").users.find({})`,
    );
  }

  if (operations.length === 0) {
    throw new Error(
      'No operation found in mongosh expression.\n\nExamples:\n  db.users.find({})\n  db.users.insertOne({ name: "Alice" })\n  db.runCommand({ ping: 1 })',
    );
  }

  return operations;
}

function isReadOnlyMongoOperation(operation: MongoshOperation): boolean {
  switch (operation.op) {
    case "find":
    case "findOne":
    case "countDocuments":
      return true;
    case "aggregate":
      return (
        Array.isArray(operation.args[0]) &&
        operation.args[0].every(
          (stage) =>
            stage !== null &&
            typeof stage === "object" &&
            !Array.isArray(stage) &&
            !Object.hasOwn(stage, "$merge") &&
            !Object.hasOwn(stage, "$out"),
        )
      );
    default:
      return false;
  }
}

function decideMongoReadOnlyQuery(queryText: string) {
  const normalizedQuery = normalizeMongoshQueryText(queryText);
  if (!normalizedQuery) {
    return denyReadOnlyQuery(MONGODB_READ_ONLY_QUERY_REASON);
  }

  try {
    const operations = parseMongoshOperations(normalizedQuery);
    return operations.every(isReadOnlyMongoOperation)
      ? allowReadOnlyQuery()
      : denyReadOnlyQuery(MONGODB_READ_ONLY_QUERY_REASON);
  } catch (error: unknown) {
    return denyReadOnlyQuery(
      error instanceof Error ? error.message : String(error),
    );
  }
}

function unwrapQuotedMongoDisplay(value: string): string {
  const trimmed = value.trim();
  if (trimmed.length >= 2) {
    const first = trimmed[0];
    const last = trimmed[trimmed.length - 1];
    if ((first === '"' && last === '"') || (first === "'" && last === "'")) {
      return trimmed.slice(1, -1);
    }
  }
  return trimmed;
}

function binarySubtypeFromNativeType(nativeType: string): number {
  const match = /^binData\((\d+)\)$/i.exec(nativeType.trim());
  return match ? Number.parseInt(match[1], 10) : 0;
}

function binarySubtypeFromColumn(
  column: Pick<ColumnTypeMeta, "nativeType"> & {
    bsonSubtype?: number;
  },
): number {
  return typeof column.bsonSubtype === "number"
    ? column.bsonSubtype
    : binarySubtypeFromNativeType(column.nativeType);
}

function parseMongoBase64(value: string): Buffer | null {
  const trimmed = unwrapQuotedMongoDisplay(value);
  if (!trimmed || trimmed.length % 4 !== 0 || !BASE64_VALUE_RE.test(trimmed)) {
    return null;
  }
  try {
    return Buffer.from(trimmed, "base64");
  } catch {
    return null;
  }
}

function parseMongoDisplayBinData(
  value: string,
): { subtype: number; bytes: Buffer } | null {
  const match = /^(?:new\s+)?BinData\(\s*(\d+)\s*,\s*"([^"]*)"\s*\)$/i.exec(
    value.trim(),
  );
  if (!match) return null;
  const subtype = Number.parseInt(match[1], 10);
  const bytes = parseMongoBase64(match[2]);
  return bytes !== null ? { subtype, bytes } : null;
}

function parseMongoDisplayJavascriptWithScope(value: string): Code | null {
  try {
    const parsed = JSON.parse(value) as {
      code?: unknown;
      scope?: unknown;
    };
    if (
      !parsed ||
      typeof parsed !== "object" ||
      typeof parsed.code !== "string"
    ) {
      return null;
    }
    const scope = parsed.scope;
    return scope !== null && typeof scope === "object" && !Array.isArray(scope)
      ? new Code(parsed.code, scope as Record<string, unknown>)
      : new Code(parsed.code);
  } catch {
    return null;
  }
}

function parseMongoDisplayDbPointer(value: string): DBRef | null {
  try {
    const parsed = JSON.parse(value) as {
      $ref?: unknown;
      $id?: unknown;
      $db?: unknown;
    };
    if (
      !parsed ||
      typeof parsed !== "object" ||
      typeof parsed.$ref !== "string"
    ) {
      return null;
    }
    const rawId = parsed.$id;
    const oid =
      typeof rawId === "string" &&
      ObjectId.isValid(rawId) &&
      rawId.length === 24
        ? new ObjectId(rawId)
        : rawId;
    return new DBRef(
      parsed.$ref,
      oid as ObjectId,
      typeof parsed.$db === "string" ? parsed.$db : undefined,
    );
  } catch {
    return null;
  }
}

function parseMongoDisplayDate(value: string): Date | null {
  const trimmed = unwrapQuotedMongoDisplay(value);
  const displayMatch = DISPLAY_DATETIME_RE.exec(trimmed);
  if (displayMatch) {
    const [, datePart, timePart = "00:00:00", fractionPart = "", timezone] =
      displayMatch;
    const fractionDigits = fractionPart
      ? fractionPart.slice(1).padEnd(3, "0").slice(0, 3)
      : "000";
    const isoString = `${datePart}T${timePart}.${fractionDigits}${timezone ?? "Z"}`;
    const parsed = new Date(isoString);
    return Number.isNaN(parsed.getTime()) ? null : parsed;
  }

  const parsed = new Date(trimmed);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
}

function formatMongoTimestampDisplay(seconds: number): string {
  return formatDatetimeForDisplay(new Date(seconds * 1000)) ?? String(seconds);
}

function parseMongoTimestampInput(value: string): Timestamp | null {
  const trimmed = unwrapQuotedMongoDisplay(value);
  const literalMatch = TIMESTAMP_LITERAL_RE.exec(trimmed);
  if (literalMatch) {
    return new Timestamp({
      t: Number.parseInt(literalMatch[1], 10),
      i: Number.parseInt(literalMatch[2], 10),
    });
  }

  const parsedDate = parseMongoDisplayDate(trimmed);
  if (!parsedDate) {
    return null;
  }

  return new Timestamp({
    t: Math.floor(parsedDate.getTime() / 1000),
    i: 1,
  });
}

function parseMongoRegexInput(value: string): RegExp | null {
  const trimmed = unwrapQuotedMongoDisplay(value);
  if (!trimmed.startsWith("/")) {
    return null;
  }

  const lastSlash = trimmed.lastIndexOf("/");
  if (lastSlash <= 0) {
    return null;
  }

  try {
    return new RegExp(
      trimmed.slice(1, lastSlash),
      trimmed.slice(lastSlash + 1),
    );
  } catch {
    return null;
  }
}

function bsonCodeScope(value: unknown): Record<string, unknown> | null {
  if (!value || typeof value !== "object") {
    return null;
  }
  const scope = (value as { scope?: unknown }).scope;
  return scope !== null && typeof scope === "object" && !Array.isArray(scope)
    ? (scope as Record<string, unknown>)
    : null;
}

function bsonDbRefValue(value: unknown): {
  collection: string;
  oid: unknown;
  database?: string;
} | null {
  if (!value || typeof value !== "object") {
    return null;
  }
  const collection = (value as { collection?: unknown }).collection;
  if (typeof collection !== "string") {
    return null;
  }
  const oid = (value as { oid?: unknown }).oid;
  const database = (value as { db?: unknown }).db;
  return {
    collection,
    oid,
    database: typeof database === "string" ? database : undefined,
  };
}

function bsonTypeTag(value: unknown): string | undefined {
  if (!value || typeof value !== "object") {
    return undefined;
  }
  const tag = (value as { _bsontype?: unknown })._bsontype;
  return typeof tag === "string" ? tag : undefined;
}

function bsonCtorName(value: unknown): string | undefined {
  if (!value || typeof value !== "object") {
    return undefined;
  }
  const ctorName = (value as { constructor?: { name?: unknown } }).constructor
    ?.name;
  return typeof ctorName === "string" ? ctorName : undefined;
}

function bsonBinarySubtype(value: unknown): number | undefined {
  if (!value || typeof value !== "object") {
    return undefined;
  }
  const subtype = (value as { sub_type?: unknown }).sub_type;
  return typeof subtype === "number" ? subtype : undefined;
}

function bsonBinaryBytes(value: unknown): Buffer | null {
  if (!value || typeof value !== "object") {
    return null;
  }
  const buffer = (value as { buffer?: unknown }).buffer;
  if (!Buffer.isBuffer(buffer)) {
    return null;
  }
  const position = (value as { position?: unknown }).position;
  const end =
    typeof position === "number" && Number.isFinite(position)
      ? Math.max(0, Math.min(buffer.length, position))
      : buffer.length;
  return buffer.subarray(0, end);
}

function formatMongoScalarValue(
  value: unknown,
): unknown | typeof MONGO_SCALAR_MISS {
  if (
    value === null ||
    value === undefined ||
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "boolean"
  ) {
    return value;
  }

  if (typeof value === "bigint") {
    return value.toString();
  }

  if (value instanceof Date) {
    return Number.isNaN(value.getTime())
      ? String(value)
      : (formatDatetimeForDisplay(value) ?? value.toISOString());
  }

  if (value instanceof RegExp) {
    return value.toString();
  }

  if (value instanceof ObjectId) {
    return value.toHexString();
  }

  const tag = bsonTypeTag(value);
  const ctorName = bsonCtorName(value);
  const binarySubtype = bsonBinarySubtype(value);

  if (ctorName === "UUID" || (tag === "Binary" && binarySubtype === 4)) {
    const bytes = bsonBinaryBytes(value);
    return bytes ? bytes.toString("base64") : String(value);
  }

  switch (tag) {
    case "Binary": {
      const bytes = bsonBinaryBytes(value);
      return bytes ? bytes.toString("base64") : String(value);
    }
    case "Code": {
      const code = (value as { code?: unknown }).code;
      const scope = bsonCodeScope(value);
      if (typeof code === "string" && scope) {
        return JSON.stringify({ code, scope });
      }
      return typeof code === "string" ? code : String(value);
    }
    case "DBRef": {
      const dbRef = bsonDbRefValue(value);
      return dbRef
        ? JSON.stringify({
            $ref: dbRef.collection,
            $id: toMongoJsonValue(dbRef.oid),
            ...(dbRef.database ? { $db: dbRef.database } : {}),
          })
        : String(value);
    }
    case "Decimal128":
    case "Long":
      return String(value);
    case "Double":
    case "Int32": {
      const numeric = Number(value);
      return Number.isFinite(numeric) ? numeric : String(value);
    }
    case "BSONSymbol":
      return String(value);
    case "Timestamp": {
      const increment = (value as { low?: unknown }).low;
      const seconds = (value as { high?: unknown }).high;
      return typeof seconds === "number" && typeof increment === "number"
        ? formatMongoTimestampDisplay(seconds)
        : String(value);
    }
    case "BSONRegExp": {
      const pattern = (value as { pattern?: unknown }).pattern;
      const options = (value as { options?: unknown }).options;
      if (typeof pattern === "string") {
        return `/${pattern}/${typeof options === "string" ? options : ""}`;
      }
      return String(value);
    }
    case "MinKey":
      return "MinKey()";
    case "MaxKey":
      return "MaxKey()";
    case "ObjectId":
      return String(value);
    default:
      return MONGO_SCALAR_MISS;
  }
}

function toMongoJsonValue(value: unknown): unknown {
  const scalar = formatMongoScalarValue(value);
  if (scalar !== MONGO_SCALAR_MISS) {
    return scalar;
  }

  if (Array.isArray(value)) {
    return value.map((entry) => toMongoJsonValue(entry));
  }

  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>).map(([key, entry]) => [
        key,
        toMongoJsonValue(entry),
      ]),
    );
  }

  return String(value);
}

function formatMongoDisplayValue(value: unknown): unknown {
  const scalar = formatMongoScalarValue(value);
  if (scalar !== MONGO_SCALAR_MISS) {
    return scalar;
  }

  if (Array.isArray(value) || (value !== null && typeof value === "object")) {
    try {
      return JSON.stringify(toMongoJsonValue(value));
    } catch {
      return String(value);
    }
  }

  return value;
}

function inferMongoSchemaType(value: unknown): MongoSchemaType {
  if (value === null) {
    return { category: "other", nativeType: "null" };
  }

  if (value === undefined) {
    return { category: "other", nativeType: "undefined" };
  }

  if (typeof value === "string") {
    return { category: "text", nativeType: "string" };
  }

  if (typeof value === "boolean") {
    return { category: "boolean", nativeType: "bool" };
  }

  if (typeof value === "number") {
    const isInt32 =
      Number.isInteger(value) && value >= -2147483648 && value <= 2147483647;
    return {
      category: Number.isInteger(value) ? "integer" : "float",
      nativeType: Number.isInteger(value) && isInt32 ? "int" : "double",
    };
  }

  if (typeof value === "bigint") {
    return { category: "integer", nativeType: "long" };
  }

  if (value instanceof Date) {
    return { category: "datetime", nativeType: "date" };
  }

  if (Array.isArray(value)) {
    return { category: "array", nativeType: "array" };
  }

  if (value instanceof RegExp) {
    return { category: "other", nativeType: "regex" };
  }

  if (value instanceof ObjectId) {
    return { category: "text", nativeType: "objectId" };
  }

  const tag = bsonTypeTag(value);
  const ctorName = bsonCtorName(value);
  if (ctorName === "UUID" || bsonBinarySubtype(value) === 4) {
    return { category: "binary", nativeType: "binData", bsonSubtype: 4 };
  }

  switch (tag) {
    case "Binary": {
      const subtype = bsonBinarySubtype(value);
      return {
        category: "binary",
        nativeType: "binData",
        bsonSubtype: typeof subtype === "number" ? subtype : 0,
      };
    }
    case "Code":
      return {
        category: "other",
        nativeType: bsonCodeScope(value) ? "javascriptWithScope" : "javascript",
      };
    case "DBRef":
      return { category: "other", nativeType: "dbPointer" };
    case "Decimal128":
      return { category: "decimal", nativeType: "decimal" };
    case "Double":
      return { category: "float", nativeType: "double" };
    case "Int32":
      return { category: "integer", nativeType: "int" };
    case "Long":
      return { category: "integer", nativeType: "long" };
    case "MaxKey":
      return { category: "other", nativeType: "maxKey" };
    case "MinKey":
      return { category: "other", nativeType: "minKey" };
    case "ObjectId":
      return { category: "text", nativeType: "objectId" };
    case "BSONRegExp":
      return { category: "other", nativeType: "regex" };
    case "BSONSymbol":
      return { category: "text", nativeType: "symbol" };
    case "Timestamp":
      return { category: "datetime", nativeType: "timestamp" };
    default:
      return { category: "json", nativeType: "object" };
  }
}

function selectMongoSchemaSample(
  documents: readonly Record<string, unknown>[],
  fieldName: string,
): unknown {
  let fallbackSample: unknown;

  for (const document of documents) {
    if (!Object.hasOwn(document, fieldName)) {
      continue;
    }

    const value = document[fieldName];
    if (fallbackSample === undefined) {
      fallbackSample = value;
    }

    if (value !== null && value !== undefined) {
      return value;
    }
  }

  return fallbackSample;
}

export class MongoDBDriver implements IDBDriver {
  private client: MongoClient | null = null;
  private connected = false;
  private timeoutRecoveryInFlight: Promise<void> | null = null;

  constructor(private readonly config: ConnectionConfig) {}

  async connect(): Promise<void> {
    if (this.connected) {
      return;
    }
    const uri = this.config.connectionUri ?? this.config.uri ?? this.buildUri();
    this.client = new MongoClient(uri, {
      tls: this.config.ssl,
      tlsAllowInvalidCertificates:
        this.config.ssl && this.config.rejectUnauthorized === false,
      authSource: this.config.authDatabase ?? this.config.authSource,
      replicaSet: this.config.replicaSet,
      directConnection: this.config.directConnection,
    });
    await this.client.connect();
    this.connected = true;
  }

  async disconnect(): Promise<void> {
    await this.client?.close();
    this.client = null;
    this.connected = false;
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
      try {
        await this.disconnect();
      } catch {}

      if (wasConnected) {
        try {
          await this.connect();
        } catch {}
      }
    };

    this.timeoutRecoveryInFlight = recover().finally(() => {
      this.timeoutRecoveryInFlight = null;
    });

    await this.timeoutRecoveryInFlight;
  }

  isConnected(): boolean {
    return this.connected;
  }

  getEntityManifest(): DriverEntityManifest {
    return MONGODB_ENTITY_MANIFEST;
  }

  getCapabilities() {
    return {
      tabularRead: "nosql" as const,
      queryMode: "text" as const,
      supportsMutations: true,
      readOnlyQueryGuard: decideMongoReadOnlyQuery,
      editorPresentation: {
        formatOnOpen: false,
        editorLanguage: "javascript" as const,
      },
    };
  }

  async listDatabases(): Promise<DatabaseInfo[]> {
    try {
      const admin = this.requireClient().db().admin();
      const dbs = await admin.listDatabases();
      return dbs.databases.map((database) => ({
        name: database.name,
        schemas: [],
      }));
    } catch {
      return [{ name: this.defaultDatabaseName(), schemas: [] }];
    }
  }

  async listSchemas(database: string): Promise<SchemaInfo[]> {
    return [{ name: database || this.defaultDatabaseName() }];
  }

  async listObjects(database: string): Promise<TableInfo[]> {
    try {
      const db = this.requireDb(database);
      const collections = await db
        .listCollections({}, { nameOnly: false })
        .toArray();
      return collections
        .filter((collection) => !this.isSystemNamespace(collection.name))
        .map((collection) => ({
          schema: database || this.defaultDatabaseName(),
          name: collection.name,
          type: collection.type === "view" ? "view" : "table",
        }));
    } catch {
      return [];
    }
  }

  async describeTable(
    database: string,
    _schema: string,
    table: string,
  ): Promise<ColumnMeta[]> {
    const columns = await this.describeSchemaColumns(database, table, 50);
    return columns.map((column) => ({
      name: column.name,
      type: column.type,
      nullable: column.nullable,
      defaultValue:
        column.isPrimaryKey && column.nativeType === "objectId"
          ? "ObjectId()"
          : undefined,
      isPrimaryKey: column.isPrimaryKey,
      primaryKeyOrdinal: column.primaryKeyOrdinal,
      isForeignKey: false,
    }));
  }

  async describeColumns(
    database: string,
    _schema: string,
    table: string,
  ): Promise<ColumnTypeMeta[]> {
    const columns = await this.describeSchemaColumns(database, table, 50);
    return columns.map((column) => ({
      ...column,
      defaultValue:
        column.isPrimaryKey && column.nativeType === "objectId"
          ? "ObjectId()"
          : undefined,
    }));
  }

  async getIndexes(
    database: string,
    _schema: string,
    table: string,
  ): Promise<IndexMeta[]> {
    try {
      const indexes = await this.requireDb(database)
        .collection(table)
        .indexes();
      return indexes.map((index) => ({
        name: index.name ?? "index",
        columns: Object.keys(index.key),
        unique: Boolean(index.unique),
        primary: index.name === "_id_",
      }));
    } catch {
      return [];
    }
  }

  async getForeignKeys(): Promise<ForeignKeyMeta[]> {
    return [];
  }

  async getConstraints(): Promise<TableConstraintMeta[]> {
    return [];
  }

  async getTriggers(): Promise<TriggerMeta[] | null> {
    return null;
  }

  async getConstraintDDL(): Promise<string> {
    unsupported("MongoDB constraints DDL");
  }

  async getIndexDDL(
    database: string,
    _schema: string,
    table: string,
    indexName: string,
  ): Promise<string> {
    const index = (
      await this.requireDb(database).collection(table).indexes()
    ).find((entry) => entry.name === indexName);
    if (!index) {
      throw new Error(`Index "${indexName}" not found`);
    }

    const key =
      index.key && typeof index.key === "object" && !Array.isArray(index.key)
        ? index.key
        : {};
    const options = Object.fromEntries(
      Object.entries(index as Record<string, unknown>).filter(
        ([keyName, value]) => {
          if (
            keyName === "key" ||
            keyName === "v" ||
            keyName === "ns" ||
            keyName === "background"
          ) {
            return false;
          }

          return value !== undefined && value !== false;
        },
      ),
    );
    const collectionRef = `${this.buildDbRef(database)}.getCollection(${JSON.stringify(table)})`;

    return `${collectionRef}.createIndex(\n  ${this.serializeMongosh(key)},\n  ${this.serializeMongosh(options)}\n);`;
  }

  async getTriggerDDL(): Promise<string> {
    unsupported("MongoDB trigger DDL");
  }

  async getCreateTableDDL(
    database: string,
    _schema: string,
    table: string,
  ): Promise<string> {
    const collection = await this.getCollectionDefinition(database, table);
    const dbRef = this.buildDbRef(database);

    if (collection.type === "view") {
      const viewOn =
        typeof collection.options.viewOn === "string"
          ? collection.options.viewOn
          : table;
      const pipeline = Array.isArray(collection.options.pipeline)
        ? collection.options.pipeline
        : [];
      const viewOptions = Object.fromEntries(
        Object.entries(collection.options).filter(
          ([key]) => key !== "viewOn" && key !== "pipeline",
        ),
      );
      const args = [
        JSON.stringify(table),
        JSON.stringify(viewOn),
        this.serializeMongosh(pipeline),
      ];
      if (Object.keys(viewOptions).length > 0) {
        args.push(this.serializeMongosh(viewOptions));
      }

      return `${dbRef}.createView(\n  ${args.join(",\n  ")}\n);`;
    }

    if (Object.keys(collection.options).length === 0) {
      return `${dbRef}.createCollection(${JSON.stringify(table)});`;
    }

    return `${dbRef}.createCollection(\n  ${JSON.stringify(table)},\n  ${this.serializeMongosh(collection.options)}\n);`;
  }

  async getObjectDefinition(): Promise<string | null> {
    return null;
  }

  async getRoutineDefinition(): Promise<string> {
    unsupported("MongoDB routine definition");
  }

  async query(sql: string, _params?: unknown[]): Promise<QueryResult> {
    const trimmed = normalizeMongoshQueryText(sql);

    if (trimmed.length === 0) {
      return { columns: [], rows: [], rowCount: 0, executionTimeMs: 0 };
    }

    const startedAt = Date.now();
    const operations = parseMongoshOperations(trimmed);

    const mapQueryRowsToObjects = (
      result: QueryResult,
    ): Record<string, unknown>[] => {
      return result.rows.map((row) =>
        Object.fromEntries(
          result.columns.map((column, index) => [
            column,
            row[`__col_${index}`],
          ]),
        ),
      );
    };

    const executeOperation = async (
      operation: MongoshOperation,
    ): Promise<QueryResult> => {
      const { dbName, collName, op, args: opArgs, chainOps } = operation;

      const limitOp = chainOps.find((c) => c.op === "limit");
      const normalizedRequestedLimit =
        typeof limitOp?.args[0] === "number" && Number.isFinite(limitOp.args[0])
          ? Math.floor(limitOp.args[0])
          : 100;
      const limit = Math.max(
        0,
        Math.min(MONGODB_QUERY_HARD_CAP, normalizedRequestedLimit),
      );
      const skipOp = chainOps.find((c) => c.op === "skip");
      const skip =
        typeof skipOp?.args[0] === "number" && Number.isFinite(skipOp.args[0])
          ? Math.max(0, Math.floor(skipOp.args[0]))
          : 0;

      if (op === "runCommand") {
        const cmd = this.normalizeFilterCriteria(
          opArgs[0] as Record<string, unknown>,
        );
        const result = await this.requireDb(dbName).command(cmd);
        const row = this.toRow(result as Record<string, unknown>);
        const columns = Object.keys(row);
        return {
          columns,
          rows: [this.mapRowToQueryRow(row, columns)],
          rowCount: 1,
          executionTimeMs: Date.now() - startedAt,
        };
      }

      if (op === "createCollection") {
        const name = String(opArgs[0]);
        const options =
          opArgs[1] !== null && typeof opArgs[1] === "object"
            ? (opArgs[1] as Record<string, unknown>)
            : undefined;
        if (options) {
          await this.requireDb(dbName).createCollection(name, options);
        } else {
          await this.requireDb(dbName).createCollection(name);
        }
        const row = { ok: 1, name, type: "collection" };
        const columns = Object.keys(row);
        return {
          columns,
          rows: [this.mapRowToQueryRow(row, columns)],
          rowCount: 1,
          executionTimeMs: Date.now() - startedAt,
        };
      }

      if (op === "createView") {
        const name = String(opArgs[0]);
        const viewOn = String(opArgs[1]);
        const pipeline = Array.isArray(opArgs[2]) ? opArgs[2] : [];
        const options =
          opArgs[3] !== null && typeof opArgs[3] === "object"
            ? (opArgs[3] as Record<string, unknown>)
            : undefined;
        await this.requireDb(dbName).createCollection(name, {
          viewOn,
          pipeline,
          ...(options ?? {}),
        });
        const row = { ok: 1, name, type: "view", viewOn };
        const columns = Object.keys(row);
        return {
          columns,
          rows: [this.mapRowToQueryRow(row, columns)],
          rowCount: 1,
          executionTimeMs: Date.now() - startedAt,
        };
      }

      if (!collName) {
        throw new Error(`Collection name is required for operation "${op}"`);
      }

      const mongoCollection = this.requireDb(dbName).collection(collName);

      if (op === "find" || op === "findOne") {
        const filter = this.normalizeFilterCriteria(
          (opArgs[0] as Record<string, unknown>) ?? {},
        );
        const actualLimit = op === "findOne" ? 1 : limit;
        const docs = await mongoCollection
          .find(filter, {
            promoteValues: false,
            bsonRegExp: false,
          })
          .skip(skip)
          .limit(actualLimit)
          .toArray();
        const rows = docs.map((doc) =>
          this.toRow(doc as Record<string, unknown>),
        );
        const columns = inferColumnsFromRows(rows, "_id").map((c) => c.name);
        return {
          columns,
          rows: rows.map((row) => this.mapRowToQueryRow(row, columns)),
          rowCount: rows.length,
          executionTimeMs: Date.now() - startedAt,
        };
      }

      if (op === "countDocuments") {
        const filter = this.normalizeFilterCriteria(
          (opArgs[0] as Record<string, unknown>) ?? {},
        );
        const count = await mongoCollection.countDocuments(filter);
        return {
          columns: ["count"],
          rows: [this.mapRowToQueryRow({ count }, ["count"])],
          rowCount: 1,
          executionTimeMs: Date.now() - startedAt,
        };
      }

      if (op === "insertOne") {
        const doc = opArgs[0] as Record<string, unknown>;
        const result = await mongoCollection.insertOne(doc);
        const row = {
          acknowledged: result.acknowledged,
          insertedId: String(result.insertedId),
        };
        return {
          columns: Object.keys(row),
          rows: [this.mapRowToQueryRow(row, Object.keys(row))],
          rowCount: 1,
          affectedRows: result.acknowledged ? 1 : 0,
          executionTimeMs: Date.now() - startedAt,
        };
      }

      if (op === "insertMany") {
        const docs = opArgs[0] as Record<string, unknown>[];
        const result = await mongoCollection.insertMany(docs);
        const row = {
          acknowledged: result.acknowledged,
          insertedCount: result.insertedCount,
        };
        return {
          columns: Object.keys(row),
          rows: [this.mapRowToQueryRow(row, Object.keys(row))],
          rowCount: 1,
          affectedRows: result.insertedCount,
          executionTimeMs: Date.now() - startedAt,
        };
      }

      if (op === "updateOne" || op === "updateMany") {
        const filter = this.normalizeFilterCriteria(
          opArgs[0] as Record<string, unknown>,
        );
        const update = opArgs[1] as Record<string, unknown>;
        const result =
          op === "updateOne"
            ? await mongoCollection.updateOne(filter, update)
            : await mongoCollection.updateMany(filter, update);
        const row = {
          matchedCount: result.matchedCount,
          modifiedCount: result.modifiedCount,
        };
        return {
          columns: Object.keys(row),
          rows: [this.mapRowToQueryRow(row, Object.keys(row))],
          rowCount: 1,
          affectedRows: result.modifiedCount,
          executionTimeMs: Date.now() - startedAt,
        };
      }

      if (op === "deleteOne" || op === "deleteMany") {
        const filter = this.normalizeFilterCriteria(
          opArgs[0] as Record<string, unknown>,
        );
        const result =
          op === "deleteOne"
            ? await mongoCollection.deleteOne(filter)
            : await mongoCollection.deleteMany(filter);
        const row = { deletedCount: result.deletedCount };
        return {
          columns: Object.keys(row),
          rows: [this.mapRowToQueryRow(row, Object.keys(row))],
          rowCount: 1,
          affectedRows: result.deletedCount,
          executionTimeMs: Date.now() - startedAt,
        };
      }

      if (op === "aggregate") {
        const pipeline = opArgs[0] as Record<string, unknown>[];
        const boundedPipeline = [...pipeline];
        if (skip > 0) {
          boundedPipeline.push({ $skip: skip });
        }
        boundedPipeline.push({ $limit: limit });
        const docs = await mongoCollection
          .aggregate(boundedPipeline, {
            promoteValues: false,
            bsonRegExp: false,
          })
          .toArray();
        const rows = docs.map((doc) =>
          this.toRow(doc as Record<string, unknown>),
        );
        const columns = inferColumnsFromRows(rows, "_id").map((c) => c.name);
        return {
          columns,
          rows: rows.map((row) => this.mapRowToQueryRow(row, columns)),
          rowCount: rows.length,
          executionTimeMs: Date.now() - startedAt,
        };
      }

      if (op === "createIndex") {
        const key =
          opArgs[0] &&
          typeof opArgs[0] === "object" &&
          !Array.isArray(opArgs[0])
            ? (opArgs[0] as Record<string, unknown>)
            : {};
        const options =
          opArgs[1] &&
          typeof opArgs[1] === "object" &&
          !Array.isArray(opArgs[1])
            ? (opArgs[1] as Record<string, unknown>)
            : undefined;
        const name = await mongoCollection.createIndex(
          key as Parameters<typeof mongoCollection.createIndex>[0],
          options as Parameters<typeof mongoCollection.createIndex>[1],
        );
        const row = { ok: 1, name };
        const columns = Object.keys(row);
        return {
          columns,
          rows: [this.mapRowToQueryRow(row, columns)],
          rowCount: 1,
          executionTimeMs: Date.now() - startedAt,
        };
      }

      throw new Error(
        `Unsupported mongosh operation: "${op}".\n\nSupported: find, findOne, countDocuments, insertOne, insertMany, updateOne, updateMany, deleteOne, deleteMany, aggregate, runCommand, createCollection, createView, createIndex`,
      );
    };

    if (operations.length === 1) {
      return executeOperation(operations[0]);
    }

    const rawRows: Record<string, unknown>[] = [];
    let affectedRows = 0;
    let sawAffectedRows = false;
    let totalRowCount = 0;

    for (const operation of operations) {
      const result = await executeOperation(operation);
      const mappedRows = mapQueryRowsToObjects(result);
      totalRowCount += mappedRows.length;
      const availableSlots = MONGODB_QUERY_HARD_CAP - rawRows.length;
      if (availableSlots > 0) {
        rawRows.push(...mappedRows.slice(0, availableSlots));
      }
      if (typeof result.affectedRows === "number") {
        affectedRows += result.affectedRows;
        sawAffectedRows = true;
      }
    }

    const columns = inferColumnsFromRows(rawRows, "_id").map((c) => c.name);
    return {
      columns,
      rows: rawRows.map((row) => this.mapRowToQueryRow(row, columns)),
      rowCount: totalRowCount,
      affectedRows: sawAffectedRows ? affectedRows : undefined,
      executionTimeMs: Date.now() - startedAt,
    };
  }

  async readTablePage(
    request: DriverTablePageRequest,
  ): Promise<DriverTablePageResult> {
    const schemaSampleLimit = Math.max(
      100,
      Math.min(500, request.pageSize * 2),
    );
    const schemaColumns = await this.describeSchemaColumns(
      request.database,
      request.table,
      schemaSampleLimit,
    );
    const normalizedFilters = this.normalizeInlineFilters(
      request.filters,
      schemaColumns,
    );
    const offset = Math.max(0, (request.page - 1) * request.pageSize);

    try {
      const criteria = this.buildMongoFilterCriteria(
        normalizedFilters,
        schemaColumns,
      );
      const sort = request.sort
        ? ([
            [request.sort.column, request.sort.direction === "desc" ? -1 : 1],
          ] as Array<[string, 1 | -1]>)
        : ([["_id", 1]] as Array<[string, 1 | -1]>);
      const docs = await this.requireDb(request.database)
        .collection(request.table)
        .find(criteria, {
          promoteValues: false,
          bsonRegExp: false,
        })
        .sort(sort)
        .skip(offset)
        .limit(request.pageSize)
        .toArray();
      const rows = docs.map((doc) =>
        this.toRow(doc as Record<string, unknown>),
      );
      const totalCount = request.skipCount
        ? 0
        : await this.requireDb(request.database)
            .collection(request.table)
            .countDocuments(criteria);

      return {
        columns:
          schemaColumns.length > 0
            ? schemaColumns
            : inferColumnsFromRows(rows, "_id", {
                nullableMode: "schemaLess",
              }),
        rows,
        totalCount,
      };
    } catch {
      const fallbackReadLimit = Math.max(
        request.page * request.pageSize * 2,
        request.pageSize * 10,
      );
      const boundedReadLimit = Math.min(
        MONGODB_QUERY_HARD_CAP,
        fallbackReadLimit,
      );
      const rows = await this.readRows(
        request.database,
        request.table,
        boundedReadLimit,
      );
      const filtered = applyFilters(rows, normalizedFilters);
      const sorted = applySort(filtered, request.sort);
      const paged = pageRows(sorted, request.page, request.pageSize);
      return {
        columns:
          schemaColumns.length > 0
            ? schemaColumns
            : inferColumnsFromRows(sorted, "_id", {
                nullableMode: "schemaLess",
              }),
        rows: paged,
        totalCount: request.skipCount ? 0 : sorted.length,
      };
    }
  }

  private buildMongoFilterCriteria(
    filters: readonly FilterExpression[],
    columns: readonly ColumnTypeMeta[],
  ): Record<string, unknown> {
    if (filters.length === 0) {
      return {};
    }

    const columnMap = new Map(columns.map((column) => [column.name, column]));
    const andClauses: Array<Record<string, unknown>> = [];

    for (const filter of filters) {
      const column = columnMap.get(filter.column);
      switch (filter.operator) {
        case "is_null":
          andClauses.push({ [filter.column]: null });
          break;
        case "is_not_null":
          andClauses.push({ [filter.column]: { $ne: null } });
          break;
        case "between": {
          const start = this.coerceFilterInput(filter.value[0], column);
          const end = this.coerceFilterInput(filter.value[1], column);
          andClauses.push({
            [filter.column]: {
              $gte: start,
              $lte: end,
            },
          });
          break;
        }
        case "eq": {
          andClauses.push({
            [filter.column]: this.coerceFilterInput(filter.value, column),
          });
          break;
        }
        case "neq": {
          andClauses.push({
            [filter.column]: {
              $ne: this.coerceFilterInput(filter.value, column),
            },
          });
          break;
        }
        case "gt":
        case "gte":
        case "lt":
        case "lte": {
          const operatorMap = {
            gt: "$gt",
            gte: "$gte",
            lt: "$lt",
            lte: "$lte",
          } as const;
          andClauses.push({
            [filter.column]: {
              [operatorMap[filter.operator]]: this.coerceFilterInput(
                filter.value,
                column,
              ),
            },
          });
          break;
        }
        case "in": {
          const entries = filter.value
            .split(",")
            .map((entry) => entry.trim())
            .filter((entry) => entry.length > 0)
            .map((entry) => this.coerceFilterInput(entry, column));
          if (entries.length > 0) {
            andClauses.push({ [filter.column]: { $in: entries } });
          }
          break;
        }
        case "like":
        case "ilike": {
          andClauses.push({
            [filter.column]: {
              $regex: this.buildContainsRegex(filter.value),
              ...(filter.operator === "ilike" ? { $options: "i" } : {}),
            },
          });
          break;
        }
      }
    }

    if (andClauses.length === 0) {
      return {};
    }

    const criteria =
      andClauses.length === 1 ? andClauses[0] : { $and: andClauses };
    return this.normalizeFilterCriteria(criteria);
  }

  private coerceFilterInput(
    value: string,
    column: ColumnTypeMeta | undefined,
  ): unknown {
    if (!column) {
      return value;
    }
    return this.coerceInputValue(value, column);
  }

  private buildContainsRegex(value: string): string {
    const escaped = value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    const normalized = escaped.replace(/%/g, ".*").replace(/_/g, ".").trim();
    return normalized.length > 0 ? normalized : ".*";
  }

  async updateRows(
    request: DriverUpdateRowsRequest,
  ): Promise<DriverMutationResult> {
    const collection = this.requireDb(request.database).collection(
      request.table,
    );
    let affectedRows = 0;
    for (const update of request.updates) {
      if (
        Object.hasOwn(update.changes, "_id") &&
        update.changes._id !== update.primaryKeys._id
      ) {
        throw new Error("MongoDB does not support updating the _id field.");
      }
      const criteria = this.normalizeCriteria(update.primaryKeys);
      const result = await collection.updateOne(criteria, {
        $set: update.changes,
      });
      affectedRows += result.matchedCount;
    }
    return { affectedRows };
  }

  async insertRow(
    request: DriverInsertRowRequest,
  ): Promise<DriverMutationResult> {
    const result = await this.requireDb(request.database)
      .collection(request.table)
      .insertOne(request.values);
    return { affectedRows: result.acknowledged ? 1 : 0 };
  }

  async deleteRows(
    request: DriverDeleteRowsRequest,
  ): Promise<DriverMutationResult> {
    const collection = this.requireDb(request.database).collection(
      request.table,
    );
    const criteria = request.primaryKeyValuesList.map((entry) =>
      this.normalizeCriteria(entry),
    );
    if (criteria.length === 0) {
      return { affectedRows: 0 };
    }
    const result = await collection.deleteMany({ $or: criteria });
    return { affectedRows: result.deletedCount };
  }

  buildMutationPreviewStatement(
    operation: "insert" | "update" | "delete",
    database: string,
    _schema: string,
    table: string,
    data: {
      primaryKeys?: Record<string, unknown>;
      changes?: Record<string, unknown>;
      values?: Record<string, unknown>;
      primaryKeyValuesList?: Array<Record<string, unknown>>;
    },
  ): string {
    const dbRef = database
      ? `db.getSiblingDB(${JSON.stringify(database)})`
      : "db";

    if (operation === "insert") {
      const doc = this.serializeMongosh(data.values ?? {});
      return `${dbRef}.${table}.insertOne(${doc})`;
    }
    if (operation === "update") {
      const filter = this.serializeMongosh(data.primaryKeys ?? {});
      const update = this.serializeMongosh({ $set: data.changes ?? {} });
      return `${dbRef}.${table}.updateMany(\n  ${filter},\n  ${update}\n)`;
    }
    // delete
    const filterValue = data.primaryKeyValuesList?.length
      ? data.primaryKeyValuesList.length === 1
        ? data.primaryKeyValuesList[0]
        : { $or: data.primaryKeyValuesList }
      : (data.primaryKeys ?? {});
    return `${dbRef}.${table}.deleteMany(${this.serializeMongosh(filterValue)})`;
  }

  async runTransaction(operations: TransactionOperation[]): Promise<void> {
    for (const operation of operations) {
      await this.query(operation.sql, operation.params);
    }
  }

  quoteIdentifier(name: string): string {
    return name;
  }

  qualifiedTableName(database: string, _schema: string, table: string): string {
    const db = database || this.defaultDatabaseName();
    return `${db}.${table}`;
  }

  buildPagination(
    offset: number,
    limit: number,
    _paramIndex: number,
  ): PaginationResult {
    return {
      sql: "LIMIT ? OFFSET ?",
      params: [limit, offset],
    };
  }

  buildOrderByDefault(_cols: ColumnTypeMeta[]): string {
    return "ORDER BY _id";
  }

  coerceInputValue(value: unknown, column: ColumnTypeMeta): unknown {
    if (value === null || value === undefined || value === "") {
      return value;
    }

    if (typeof value !== "string") {
      return value;
    }

    const normalized = unwrapQuotedMongoDisplay(value);

    if (column.nativeType === "objectId") {
      return ObjectId.isValid(normalized) && normalized.length === 24
        ? new ObjectId(normalized)
        : normalized;
    }

    if (column.nativeType === "null") {
      return /^null$/i.test(normalized) ? null : normalized;
    }

    if (column.nativeType === "undefined") {
      return /^undefined$/i.test(normalized) ? undefined : normalized;
    }

    if (column.nativeType === "uuid") {
      return UUID_VALUE_RE.test(normalized)
        ? UUID.createFromHexString(normalized.replace(/-/g, ""))
        : normalized;
    }

    if (column.nativeType === "date") {
      return parseMongoDisplayDate(normalized) ?? normalized;
    }

    if (column.nativeType === "timestamp") {
      return parseMongoTimestampInput(normalized) ?? normalized;
    }

    if (column.nativeType === "decimal" || column.nativeType === "decimal128") {
      try {
        return Decimal128.fromString(normalized);
      } catch {
        return normalized;
      }
    }

    if (column.nativeType === "int" || column.nativeType === "int32") {
      return /^[+-]?\d+$/.test(normalized)
        ? new Int32(Number.parseInt(normalized, 10))
        : normalized;
    }

    if (column.nativeType === "long" || column.nativeType === "int64") {
      return /^[+-]?\d+$/.test(normalized)
        ? Long.fromString(normalized)
        : normalized;
    }

    if (column.nativeType === "double" || column.nativeType === "number") {
      const numeric = Number(normalized);
      return Number.isFinite(numeric) ? numeric : normalized;
    }

    if (column.nativeType === "bool") {
      const lower = normalized.toLowerCase();
      if (lower === "true" || lower === "1") return true;
      if (lower === "false" || lower === "0") return false;
      return normalized;
    }

    if (column.nativeType === "javascript") {
      return new Code(normalized);
    }

    if (column.nativeType === "javascriptWithScope") {
      return parseMongoDisplayJavascriptWithScope(normalized) ?? normalized;
    }

    if (column.nativeType === "dbPointer" || column.nativeType === "dbRef") {
      return parseMongoDisplayDbPointer(normalized) ?? normalized;
    }

    if (column.nativeType === "symbol") {
      return new BSONSymbol(normalized);
    }

    if (column.nativeType === "regex") {
      return parseMongoRegexInput(normalized) ?? normalized;
    }

    if (column.nativeType === "minKey") {
      return /^MinKey\(\)$/i.test(normalized) ? new MinKey() : normalized;
    }

    if (column.nativeType === "maxKey") {
      return /^MaxKey\(\)$/i.test(normalized) ? new MaxKey() : normalized;
    }

    if (/^binData(?:\(\d+\))?$/i.test(column.nativeType)) {
      const binDataParsed = parseMongoDisplayBinData(normalized);
      if (binDataParsed) {
        return new Binary(binDataParsed.bytes, binDataParsed.subtype);
      }
      const bytes = parseMongoBase64(normalized);
      return bytes
        ? new Binary(bytes, binarySubtypeFromColumn(column))
        : normalized;
    }

    if (column.category === "array" || column.category === "json") {
      try {
        return JSON.parse(normalized) as unknown;
      } catch {
        return normalized;
      }
    }

    return normalized;
  }

  formatOutputValue(value: unknown, column: ColumnTypeMeta): unknown {
    if (typeof value === "string") {
      const coerced = this.coerceInputValue(value, column);
      if (coerced !== value) {
        return formatMongoDisplayValue(coerced);
      }
    }
    return formatMongoDisplayValue(value);
  }

  checkPersistedEdit(
    _column: ColumnTypeMeta,
    _expectedValue: unknown,
    _options?: { persistedValue: unknown },
  ) {
    return null;
  }

  normalizeFilterValue(
    column: ColumnTypeMeta,
    _operator: FilterOperator,
    value: string | [string, string] | undefined,
  ) {
    if (value === undefined) {
      return undefined;
    }
    if (Array.isArray(value)) {
      return value.map((entry) =>
        this.normalizeInlineFilterScalar(column, entry),
      ) as [string, string];
    }
    return this.normalizeInlineFilterScalar(column, value);
  }

  buildFilterCondition(
    column: ColumnTypeMeta,
    operator: never,
    value: string | [string, string] | undefined,
    _paramIndex: number,
  ) {
    return {
      sql: `${column.name}:${String(operator)}`,
      params: value === undefined ? [] : Array.isArray(value) ? value : [value],
    };
  }

  buildInsertDefaultValuesSql(qualifiedTableName: string): string {
    const dotIdx = qualifiedTableName.indexOf(".");
    const db = dotIdx !== -1 ? qualifiedTableName.slice(0, dotIdx) : "";
    const coll =
      dotIdx !== -1 ? qualifiedTableName.slice(dotIdx + 1) : qualifiedTableName;
    const dbRef = db ? `db.getSiblingDB(${JSON.stringify(db)})` : "db";
    return `${dbRef}.${coll}.insertOne({ })`;
  }

  buildInsertValueExpr(_column: ColumnTypeMeta, _paramIndex: number): string {
    return "?";
  }

  buildSetExpr(column: ColumnTypeMeta): string {
    return `${column.name} = ?`;
  }

  materializePreviewSql(sql: string): string {
    return sql;
  }

  private buildUri(): string {
    const host = this.config.host?.trim() || "localhost";
    const port = this.config.port ?? 27017;
    const auth = this.config.username
      ? `${encodeURIComponent(this.config.username)}:${encodeURIComponent(this.config.password ?? "")}@`
      : "";
    const database = this.defaultDatabaseName();
    const params = new URLSearchParams();
    const authDatabase = this.config.authDatabase ?? this.config.authSource;
    if (authDatabase) {
      params.set("authSource", authDatabase);
    }
    if (this.config.replicaSet) {
      params.set("replicaSet", this.config.replicaSet);
    }
    if (this.config.directConnection !== undefined) {
      params.set("directConnection", String(this.config.directConnection));
    }
    const suffix = params.size > 0 ? `?${params.toString()}` : "";
    return `mongodb://${auth}${host}:${port}/${database}${suffix}`;
  }

  private requireClient(): MongoClient {
    if (!this.client || !this.connected) {
      throw new Error("MongoDB is not connected.");
    }
    return this.client;
  }

  private defaultDatabaseName(): string {
    return this.config.database || "admin";
  }

  private requireDb(database?: string) {
    return this.requireClient().db(database || this.defaultDatabaseName());
  }

  private buildDbRef(database?: string): string {
    return `db.getSiblingDB(${JSON.stringify(database || this.defaultDatabaseName())})`;
  }

  private async getCollectionDefinition(
    database: string,
    table: string,
  ): Promise<{
    type: "collection" | "view";
    options: Record<string, unknown>;
  }> {
    const collection = (
      await this.requireDb(database)
        .listCollections({ name: table }, { nameOnly: false })
        .toArray()
    ).find((entry) => entry.name === table);

    if (!collection) {
      throw new Error(`Collection "${table}" not found`);
    }

    const options =
      collection.options &&
      typeof collection.options === "object" &&
      !Array.isArray(collection.options)
        ? { ...(collection.options as Record<string, unknown>) }
        : {};
    delete options.uuid;

    return {
      type: collection.type === "view" ? "view" : "collection",
      options,
    };
  }

  private toRow(document: Record<string, unknown>): Record<string, unknown> {
    return Object.fromEntries(
      Object.entries(document).map(([key, value]) => [
        key,
        formatMongoDisplayValue(value),
      ]),
    );
  }

  private async describeSchemaColumns(
    database: string,
    table: string,
    limit: number,
  ): Promise<ColumnTypeMeta[]> {
    const documents = await this.readSchemaDocuments(database, table, limit);
    const keys = new Set<string>();
    for (const document of documents) {
      for (const key of Object.keys(document)) {
        keys.add(key);
      }
    }

    return [...keys]
      .sort((left, right) => left.localeCompare(right))
      .map((name) => {
        const isPrimaryKey = name === "_id";
        const sample = selectMongoSchemaSample(documents, name);
        const { category, nativeType, bsonSubtype } =
          inferMongoSchemaType(sample);
        const filterable = category !== "binary" && category !== "spatial";

        return {
          name,
          type: nativeType,
          nativeType,
          bsonSubtype,
          category,
          nullable: !isPrimaryKey,
          defaultValue: undefined,
          isPrimaryKey,
          primaryKeyOrdinal: isPrimaryKey ? 1 : undefined,
          isForeignKey: false,
          filterable,
          filterOperators: resolveFilterOperators(category, {
            filterable,
            nullable: !isPrimaryKey,
          }),
          valueSemantics: "plain",
        } satisfies ColumnTypeMeta;
      });
  }

  private async readSchemaDocuments(
    database: string,
    table: string,
    limit: number,
  ): Promise<Record<string, unknown>[]> {
    try {
      const docs = await this.requireDb(database)
        .collection(table)
        .find(
          {},
          {
            promoteValues: false,
            bsonRegExp: false,
          },
        )
        .limit(limit)
        .toArray();
      return docs as Record<string, unknown>[];
    } catch {
      return [];
    }
  }

  private async readRows(
    database: string,
    table: string,
    limit: number,
  ): Promise<Record<string, unknown>[]> {
    try {
      const docs = await this.requireDb(database)
        .collection(table)
        .find(
          {},
          {
            promoteValues: false,
            bsonRegExp: false,
          },
        )
        .limit(limit)
        .toArray();
      return docs.map((doc) => this.toRow(doc as Record<string, unknown>));
    } catch {
      return [];
    }
  }

  private normalizeCriteria(
    criteria: Record<string, unknown>,
  ): Record<string, unknown> {
    const normalized = { ...criteria };
    if (
      typeof normalized._id === "string" &&
      ObjectId.isValid(normalized._id)
    ) {
      normalized._id = new ObjectId(normalized._id);
    }
    return normalized;
  }

  private normalizeFilterCriteria(
    filter: Record<string, unknown>,
  ): Record<string, unknown> {
    const normalized = this.normalizeCriteria(filter);
    if (Array.isArray(normalized.$or)) {
      normalized.$or = normalized.$or.map((item) =>
        item !== null && typeof item === "object"
          ? this.normalizeCriteria(item as Record<string, unknown>)
          : item,
      );
    }
    if (Array.isArray(normalized.$and)) {
      normalized.$and = normalized.$and.map((item) =>
        item !== null && typeof item === "object"
          ? this.normalizeCriteria(item as Record<string, unknown>)
          : item,
      );
    }
    return normalized;
  }

  private serializeMongosh(value: unknown): string {
    if (value === null) return "null";
    if (value === undefined) return "undefined";
    if (typeof value === "boolean") return String(value);
    if (typeof value === "number") return String(value);
    if (typeof value === "bigint") return value.toString();
    if (typeof value === "string") {
      if (ObjectId.isValid(value) && value.length === 24) {
        return `ObjectId(${JSON.stringify(value)})`;
      }
      return JSON.stringify(value);
    }
    if (value instanceof Date) {
      return `new Date(${JSON.stringify(value.toISOString())})`;
    }
    if (value instanceof ObjectId) {
      return `ObjectId(${JSON.stringify(value.toHexString())})`;
    }
    if (value instanceof RegExp) {
      return `new RegExp(${JSON.stringify(value.source)}, ${JSON.stringify(value.flags)})`;
    }
    if (value instanceof UUID) {
      const bytes = bsonBinaryBytes(value);
      return `new BinData(4, ${JSON.stringify(bytes?.toString("base64") ?? "")})`;
    }
    if (value instanceof Binary) {
      const bytes = bsonBinaryBytes(value);
      return `new BinData(${bsonBinarySubtype(value) ?? 0}, ${JSON.stringify(bytes?.toString("base64") ?? "")})`;
    }
    if (value instanceof DBRef) {
      return `new DBRef(${JSON.stringify(value.collection)}, ${this.serializeMongosh(value.oid)}${value.db ? `, ${JSON.stringify(value.db)}` : ""})`;
    }
    if (value instanceof BSONSymbol) {
      return `new BSONSymbol(${JSON.stringify(String(value))})`;
    }
    if (value instanceof Decimal128) {
      return `new NumberDecimal(${JSON.stringify(value.toString())})`;
    }
    if (value instanceof Int32) {
      return `new NumberInt(${JSON.stringify(value.toString())})`;
    }
    if (value instanceof Timestamp) {
      return `new Timestamp(${value.high}, ${value.low})`;
    }
    if (value instanceof Long) {
      return `new NumberLong(${JSON.stringify(value.toString())})`;
    }
    if (value instanceof Code) {
      const scope = bsonCodeScope(value);
      return scope
        ? `new Code(${JSON.stringify(value.code)}, ${this.serializeMongosh(scope)})`
        : `new Code(${JSON.stringify(value.code)})`;
    }
    if (value instanceof BSONRegExp) {
      return `new RegExp(${JSON.stringify(value.pattern)}, ${JSON.stringify(value.options)})`;
    }
    if (value instanceof MinKey) {
      return "MinKey()";
    }
    if (value instanceof MaxKey) {
      return "MaxKey()";
    }
    if (Array.isArray(value)) {
      return `[${value.map((v) => this.serializeMongosh(v)).join(", ")}]`;
    }
    if (typeof value === "object") {
      const entries = Object.entries(value as Record<string, unknown>).map(
        ([k, v]) => `${JSON.stringify(k)}: ${this.serializeMongosh(v)}`,
      );
      if (entries.length === 0) return "{}";
      return `{ ${entries.join(", ")} }`;
    }
    return JSON.stringify(value);
  }

  private normalizeInlineFilters(
    filters: readonly FilterExpression[],
    columns: readonly ColumnTypeMeta[],
  ): FilterExpression[] {
    if (filters.length === 0 || columns.length === 0) {
      return [...filters];
    }

    const columnMap = new Map(columns.map((column) => [column.name, column]));
    return filters.map((filter) => {
      const column = columnMap.get(filter.column);
      if (!column || !("value" in filter)) {
        return filter;
      }

      const normalized = this.normalizeFilterValue(
        column,
        filter.operator,
        filter.value,
      );
      if (normalized === undefined) {
        return filter;
      }

      return {
        ...filter,
        value: normalized,
      } as FilterExpression;
    });
  }

  private normalizeInlineFilterScalar(
    column: ColumnTypeMeta,
    value: string,
  ): string {
    const trimmed = unwrapQuotedMongoDisplay(value);
    if (
      column.category === "datetime" ||
      column.nativeType === "date" ||
      column.nativeType === "timestamp" ||
      column.nativeType === "objectId"
    ) {
      const coerced = this.coerceInputValue(trimmed, column);
      const formatted = this.formatOutputValue(coerced, column);
      return typeof formatted === "string" ? formatted : String(formatted);
    }

    return trimmed;
  }

  private mapRowToQueryRow(
    row: Record<string, unknown>,
    columns: string[],
  ): Record<string, unknown> {
    const mapped: Record<string, unknown> = {};
    columns.forEach((columnName, index) => {
      mapped[`__col_${index}`] = row[columnName];
    });
    return mapped;
  }

  private isSystemNamespace(name: string): boolean {
    return /^system\./i.test(name);
  }
}
