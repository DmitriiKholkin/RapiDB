import { isIPv6 } from "node:net";
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
import { resolveConnectionTlsSettings } from "../services/connectionTls";
import { allowReadOnlyQuery, denyReadOnlyQuery } from "../utils/readOnlyGuards";
import {
  formatDatetimeForDisplay,
  hexFromBuffer,
  isHexLike,
  parseHexToBuffer,
} from "./BaseDBDriver";
import { deleteRowsSequentially, prepareDeleteBatch } from "./deleteOutcomes";
import {
  applyFilters,
  applySort,
  createNoSqlUnsupportedMetadataHandlers,
  inferColumnsFromRows,
  pageRows,
} from "./nosqlUtils";
import type {
  ColumnMeta,
  ColumnTypeMeta,
  DatabaseInfo,
  DriverDeleteRowsRequest,
  DriverEntityManifest,
  DriverInsertRowRequest,
  DriverMutationResult,
  DriverOperationContext,
  DriverTablePageRequest,
  DriverTablePageResult,
  DriverUpdateRowsRequest,
  FilterExpression,
  FilterOperator,
  IDBDriver,
  IndexMeta,
  PaginationResult,
  QueryExecutionOptions,
  QueryResult,
  SchemaInfo,
  TableInfo,
  TransactionOperation,
  TypeCategory,
} from "./types";
import { NULL_SENTINEL, resolveFilterOperators } from "./types";

const MONGODB_ENTITY_MANIFEST: DriverEntityManifest = {
  dbObjectKinds: ["table", "view"],
  tableSections: {
    columns: "supported",
    constraints: "not_applicable",
    indexes: "supported",
    triggers: "not_applicable",
  },
  tableSectionOverridesByObjectKind: {
    view: {
      indexes: "not_applicable",
    },
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
const MONGO_CLIENT_SIDE_LIKE_NATIVE_TYPES = new Set([
  "objectId",
  "javascript",
  "javascriptWithScope",
]);
const UUID_VALUE_RE =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const BASE64_VALUE_RE =
  /^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/;
const DISPLAY_DATETIME_RE =
  /^(\d{4}-\d{2}-\d{2})(?:[ T](\d{2}:\d{2}:\d{2})(\.\d{1,3})?)?(?: ?(Z|[+-]\d{2}(?::?\d{2})?))?$/;
const TIMESTAMP_LITERAL_RE = /^Timestamp\((\d+),\s*(\d+)\)$/i;

const MONGODB_READ_ONLY_QUERY_REASON =
  "[RapiDB] Read-only MongoDB connections allow only find, findOne, countDocuments, and aggregate queries without $out or $merge (read-only cursor modifiers only).";
const MONGODB_QUERY_HARD_CAP = QUERY_LIMIT_POLICY.hardCap;
const MONGOSH_VM_TIMEOUT_MS = 5000;
const MONGODB_UNSUPPORTED_METADATA =
  createNoSqlUnsupportedMetadataHandlers("MongoDB");
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

// No host objects or functions may be reachable by query code: a host
// function's constructor provides an escape from vm's disabled code generation.
const MONGOSH_SANDBOX_SETUP = `
  const operations = [];
  const nativeRegExp = globalThis.RegExp;
  const safeApply = Reflect.apply;
  const nativeWeakSetAdd = WeakSet.prototype.add;
  const nativeWeakSetHas = WeakSet.prototype.has;
  const { marker, isMarker } = (() => {
    const markers = new WeakSet();
    return {
      marker: (type, args) => {
        const value = { type, args };
        safeApply(nativeWeakSetAdd, markers, [value]);
        return value;
      },
      isMarker: (value) => safeApply(nativeWeakSetHas, markers, [value]),
    };
  })();
  // Capture serialization primitives before query code can replace built-ins.
  // Encode every object as a typed node so user objects cannot impersonate BSON.
  const serializeOperations = (() => {
    const entries = Object.entries;
    const stringify = JSON.stringify;
    const create = Object.create;
    const setPrototype = Object.setPrototypeOf;
    const getPrototype = Object.getPrototypeOf;
    const regexPrototype = nativeRegExp.prototype;
    const regexSource = Object.getOwnPropertyDescriptor(regexPrototype, 'source').get;
    // The built-in flags getter reads mutable properties (e.g. ignoreCase).
    // Capture their original getters instead of trusting the query's prototype.
    const flagNames = ['hasIndices', 'global', 'ignoreCase', 'multiline', 'dotAll', 'unicode', 'unicodeSets', 'sticky'];
    const flagLetters = 'dgimsuvy';
    const flagGetters = flagNames.map(
      (name) => Object.getOwnPropertyDescriptor(regexPrototype, name)?.get,
    );
    const isArray = Array.isArray;
    const isFiniteNumber = Number.isFinite;
    const bigintToString = BigInt.prototype.toString;
    const nativeWeakSetDelete = WeakSet.prototype.delete;
    const safeArray = () => {
      const result = [];
      // A null prototype prevents query-added setters and toJSON methods from
      // changing the encoded array while it is built or serialized.
      safeApply(setPrototype, Object, [result, null]);
      return result;
    };
    const node = (kind, value, args) => {
      const result = safeApply(create, Object, [null]);
      result.kind = kind;
      if (value !== undefined) result.value = value;
      if (args !== undefined) result.args = args;
      return result;
    };
    return () => {
      const active = new WeakSet();
      const encodeList = (values) => {
        const result = safeArray();
        for (let i = 0; i < values.length; i++) result[i] = encode(values[i]);
        return result;
      };
      const encode = (value) => {
        if (value === undefined) return node('undefined');
        if (typeof value === 'bigint')
          return node('bigint', safeApply(bigintToString, value, []));
        if (typeof value === 'function' || typeof value === 'symbol')
          throw new Error('Functions and symbols are not supported in mongosh queries.');
        if (typeof value === 'number' && !isFiniteNumber(value))
          throw new Error('Non-finite numbers are not supported in mongosh queries.');
        if (value === null || typeof value !== 'object') return value;
        if (safeApply(nativeWeakSetHas, active, [value]))
          throw new Error('Cyclic mongosh values are not supported.');
        safeApply(nativeWeakSetAdd, active, [value]);
        // The native source getter checks the RegExp internal slot. Unlike
        // instanceof, this cannot be changed through Symbol.hasInstance.
        let regexpSource;
        try {
          regexpSource = safeApply(regexSource, value, []);
        } catch {
          if (safeApply(getPrototype, Object, [value]) === regexPrototype) {
            throw new Error('Proxy-wrapped RegExp values are not supported in mongosh queries.');
          }
          regexpSource = undefined;
        }
        let result;
        if (isMarker(value)) {
          result = node('marker', value.type, encodeList(value.args));
        } else if (regexpSource !== undefined) {
          const args = safeArray();
          args[0] = regexpSource;
          let flags = '';
          for (let i = 0; i < flagGetters.length; i++) {
            if (flagGetters[i] && safeApply(flagGetters[i], value, [])) {
              flags += flagLetters[i];
            }
          }
          args[1] = flags;
          result = node('marker', 'RegExp', args);
        } else if (isArray(value)) {
          result = node('array', encodeList(value));
        } else {
          const pairs = safeApply(entries, Object, [value]);
          const encoded = safeArray();
          for (let i = 0; i < pairs.length; i++) {
            const pair = safeArray();
            pair[0] = pairs[i][0];
            pair[1] = encode(pairs[i][1]);
            encoded[i] = pair;
          }
          result = node('object', encoded);
        }
        safeApply(nativeWeakSetDelete, active, [value]);
        return result;
      };
      return safeApply(stringify, JSON, [encode(operations)]);
    };
  })();
  const db = (() => {
  const InternalProxy = Proxy;
  const createChainProxy = (operation) => new InternalProxy({}, {
    get(_target, method) {
      if (typeof method !== 'string') return undefined;
      return (...args) => {
        operation.chainOps.push({ op: method, args });
        return createChainProxy(operation);
      };
    },
  });
  const createCollProxy = (dbName, collName) => new InternalProxy({}, {
    get(_target, method) {
      if (typeof method !== 'string' || method === 'then') return undefined;
      return (...args) => {
        const operation = { dbName, collName, op: method, args, chainOps: [] };
        operations.push(operation);
        return createChainProxy(operation);
      };
    },
  });
  const createDbProxy = (dbName) => new InternalProxy({}, {
    get(_target, prop) {
      if (typeof prop !== 'string') return undefined;
      if (prop === 'getSiblingDB') return (name) => createDbProxy(name);
      if (prop === 'getCollection') return (name) => createCollProxy(dbName, String(name));
      if (prop === 'runCommand') return (cmd) => {
        const operation = { dbName, op: 'runCommand', args: [cmd], chainOps: [] };
        operations.push(operation);
        return createChainProxy(operation);
      };
      if (prop === 'createCollection' || prop === 'createView') return (...args) => {
        operations.push({ dbName, op: prop, args, chainOps: [] });
      };
      return createCollProxy(dbName, prop);
    },
  });
  return createDbProxy();
  })();
  // Query code cannot create Proxy-wrapped values whose internal BSON type is
  // invisible to the VM serializer (including RegExp proxies with fake traps).
  globalThis.Proxy = undefined;
  const Date = function mongoDate(...args) { return marker('Date', args); };
  const ISODate = Date;
  const RegExp = function mongoRegExp(pattern, flags) { return marker('RegExp', [pattern, flags]); };
  const ObjectId = function mongoObjectId(hex) { return marker('ObjectId', [hex]); };
  const BinData = function mongoBinData(subtype, base64) { return marker('BinData', [subtype, base64]); };
  const DBRef = function mongoDBRef(collection, oid, database) { return marker('DBRef', [collection, oid, database]); };
  const BSONSymbol = function mongoBSONSymbol(value) { return marker('BSONSymbol', [value]); };
  const NumberLong = function mongoNumberLong(value) { return marker('NumberLong', [value]); };
  const NumberInt = function mongoNumberInt(value) { return marker('NumberInt', [value]); };
  const NumberDecimal = function mongoNumberDecimal(value) { return marker('NumberDecimal', [value]); };
  const Timestamp = function mongoTimestamp(seconds, increment) { return marker('Timestamp', [seconds, increment]); };
  const Code = function mongoCode(source, scope) { return marker('Code', [source, scope]); };
  const MinKey = function mongoMinKey() { return marker('MinKey', []); };
  const MaxKey = function mongoMaxKey() { return marker('MaxKey', []); };
`;

function reviveMongoshValue(value: unknown): unknown {
  if (value === null || typeof value !== "object") return value;
  const record = value as { kind: string; value?: unknown; args?: unknown[] };
  if (record.kind === "array") {
    return (record.value as unknown[]).map(reviveMongoshValue);
  }
  if (record.kind === "object") {
    return Object.fromEntries(
      (record.value as [string, unknown][]).map(([key, entry]) => [
        key,
        reviveMongoshValue(entry),
      ]),
    );
  }
  if (record.kind === "undefined") return undefined;
  if (record.kind === "bigint") return BigInt(record.value as string);
  if (record.kind === "marker") {
    const args = (record.args ?? []).map(reviveMongoshValue);
    switch (record.value) {
      case "Date":
        return args.length === 0
          ? new Date()
          : new Date(args[0] as string | number);
      case "RegExp":
        return new RegExp(
          String(args[0]),
          args[1] === undefined ? undefined : String(args[1]),
        );
      case "ObjectId":
        return args[0] === undefined
          ? new ObjectId()
          : new ObjectId(String(args[0]));
      case "BinData":
        return new Binary(
          Buffer.from(String(args[1]), "base64"),
          Number(args[0]),
        );
      case "DBRef":
        return new DBRef(
          String(args[0]),
          args[1] as ObjectId,
          args[2] ? String(args[2]) : undefined,
        );
      case "BSONSymbol":
        return new BSONSymbol(String(args[0]));
      case "NumberLong":
        return Long.fromString(String(args[0]));
      case "NumberInt":
        return new Int32(Number.parseInt(String(args[0]), 10));
      case "NumberDecimal":
        return Decimal128.fromString(String(args[0]));
      case "Timestamp": {
        const spec = args[0];
        return spec && typeof spec === "object"
          ? new Timestamp({
              t: Number((spec as { t?: unknown }).t ?? 0),
              i: Number((spec as { i?: unknown }).i ?? 0),
            })
          : new Timestamp({ t: Number(spec), i: Number(args[1] ?? 0) });
      }
      case "Code":
        return args[1] && typeof args[1] === "object"
          ? new Code(String(args[0]), args[1] as Record<string, unknown>)
          : new Code(String(args[0]));
      case "MinKey":
        return new MinKey();
      case "MaxKey":
        return new MaxKey();
      default:
        throw new Error("Unsupported BSON literal in mongosh query.");
    }
  }
  throw new Error("Invalid mongosh value.");
}

function parseMongoshOperations(queryText: string): MongoshOperation[] {
  let operations: MongoshOperation[];
  try {
    ensureMongoshQueryIsSafe(queryText);
    const context = vm.createContext(Object.create(null), {
      codeGeneration: { strings: false, wasm: false },
      microtaskMode: "afterEvaluate",
    });
    // Bootstrap and serialization both run under the VM deadline. Never read
    // query-created properties or call query-created getters in the host realm.
    vm.runInContext(MONGOSH_SANDBOX_SETUP, context, {
      timeout: MONGOSH_VM_TIMEOUT_MS,
    });
    vm.runInContext(queryText, context, { timeout: MONGOSH_VM_TIMEOUT_MS });
    const serialized = vm.runInContext("serializeOperations()", context, {
      timeout: MONGOSH_VM_TIMEOUT_MS,
    }) as string;
    operations = reviveMongoshValue(
      JSON.parse(serialized),
    ) as MongoshOperation[];
    if (
      !Array.isArray(operations) ||
      operations.some(
        (operation) =>
          !operation ||
          typeof operation !== "object" ||
          typeof operation.op !== "string" ||
          !Array.isArray(operation.args) ||
          !Array.isArray(operation.chainOps),
      )
    ) {
      throw new Error("Invalid mongosh operation.");
    }
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

function pipelineContainsWriteStage(pipeline: unknown[]): boolean {
  return pipeline.some((stage) => {
    if (stage === null || typeof stage !== "object" || Array.isArray(stage)) {
      return true;
    }
    const entries = Object.entries(stage as Record<string, unknown>);
    if (entries.some(([key]) => key === "$out" || key === "$merge")) {
      return true;
    }
    return entries.some(([key, options]) => {
      if (key === "$facet" && options && typeof options === "object") {
        return Object.values(options).some(
          (nested) =>
            !Array.isArray(nested) || pipelineContainsWriteStage(nested),
        );
      }
      if (
        (key === "$lookup" || key === "$unionWith") &&
        options &&
        typeof options === "object" &&
        Object.hasOwn(options, "pipeline")
      ) {
        const nested = (options as { pipeline?: unknown }).pipeline;
        return !Array.isArray(nested) || pipelineContainsWriteStage(nested);
      }
      return false;
    });
  });
}

const MONGODB_READ_ONLY_CHAIN_OPS = new Set([
  "limit",
  "skip",
  "sort",
  "toArray",
  "batchSize",
  "maxTimeMS",
  "hint",
  "collation",
  "explain",
  // Display / cursor helpers that have no write effect (execution ignores
  // unknown chain ops anyway; allowlist is fail-closed for the guard).
  "pretty",
  "comment",
  "count",
  "size",
  "max",
  "min",
  "readConcern",
  "readPref",
  "noCursorTimeout",
  "close",
]);

function isReadOnlyMongoOperation(operation: MongoshOperation): boolean {
  // Cursor modifiers must also be read-only: db.users.find({}).deleteMany()
  // parses as op=find + chainOp=deleteMany and must not pass the guard even
  // though execution currently ignores unknown chain ops.
  if (
    operation.chainOps.some(
      (chain) => !MONGODB_READ_ONLY_CHAIN_OPS.has(chain.op),
    )
  ) {
    return false;
  }
  switch (operation.op) {
    case "find":
    case "findOne":
    case "countDocuments":
      return true;
    case "aggregate":
      return (
        Array.isArray(operation.args[0]) &&
        !pipelineContainsWriteStage(operation.args[0])
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
    return bytes ? hexFromBuffer(bytes) : String(value);
  }

  switch (tag) {
    case "Binary": {
      const bytes = bsonBinaryBytes(value);
      return bytes ? hexFromBuffer(bytes) : String(value);
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
  private readonly tableRowCategories = new WeakMap<
    Record<string, unknown>,
    Map<string, TypeCategory>
  >();
  private readonly rowValueCategory = (
    row: Record<string, unknown>,
    column: string,
  ): TypeCategory =>
    this.tableRowCategories.get(row)?.get(column) ??
    inferMongoSchemaType(row[column]).category;
  private readonly tableRowIdTypes = new WeakMap<
    Record<string, unknown>,
    "objectId" | "string" | null
  >();
  private client: MongoClient | null = null;
  private connected = false;
  private timeoutRecoveryInFlight: Promise<void> | null = null;

  constructor(private readonly config: ConnectionConfig) {}

  async connect(): Promise<void> {
    if (this.connected) {
      return;
    }
    const uri = this.config.connectionUri ?? this.config.uri ?? this.buildUri();
    const tlsSettings = resolveConnectionTlsSettings(this.config);
    this.client = new MongoClient(uri, {
      tls: tlsSettings !== undefined,
      tlsAllowInvalidCertificates:
        tlsSettings !== undefined && !tlsSettings.rejectUnauthorized,
      tlsAllowInvalidHostnames: tlsSettings?.skipHostnameVerification === true,
      ca: tlsSettings?.ca,
      cert: tlsSettings?.cert,
      key: tlsSettings?.key,
      passphrase: tlsSettings?.passphrase,
      servername: tlsSettings?.servername,
      authSource: this.config.authSource,
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

  getForeignKeys = MONGODB_UNSUPPORTED_METADATA.getForeignKeys;

  getConstraints = MONGODB_UNSUPPORTED_METADATA.getConstraints;

  getTriggers = MONGODB_UNSUPPORTED_METADATA.getTriggers;

  getConstraintDDL = MONGODB_UNSUPPORTED_METADATA.getConstraintDDL;

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

  getTriggerDDL = MONGODB_UNSUPPORTED_METADATA.getTriggerDDL;

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

  getObjectDefinition = MONGODB_UNSUPPORTED_METADATA.getObjectDefinition;

  getRoutineDefinition = MONGODB_UNSUPPORTED_METADATA.getRoutineDefinition;

  async query(
    sql: string,
    _params?: unknown[],
    options?: QueryExecutionOptions,
  ): Promise<QueryResult> {
    const trimmed = normalizeMongoshQueryText(sql);

    if (trimmed.length === 0) {
      return { columns: [], rows: [], rowCount: 0, executionTimeMs: 0 };
    }

    const startedAt = Date.now();
    const operations = parseMongoshOperations(trimmed);
    if (
      (options?.readOnly === true || this.config.readOnly === true) &&
      !operations.every(isReadOnlyMongoOperation)
    ) {
      throw new Error(MONGODB_READ_ONLY_QUERY_REASON);
    }

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

      const dbHandlers: Record<string, () => Promise<QueryResult>> = {
        runCommand: async () => {
          const cmd = this.normalizeFilterCriteria(
            opArgs[0] as Record<string, unknown>,
          );
          const result = await this.requireDb(dbName).command(cmd);
          return this.buildMongoSingleRowQueryResult(
            this.toRow(result as Record<string, unknown>),
            startedAt,
          );
        },
        createCollection: async () => {
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
          return this.buildMongoSingleRowQueryResult(
            { ok: 1, name, type: "collection" },
            startedAt,
          );
        },
        createView: async () => {
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
          return this.buildMongoSingleRowQueryResult(
            { ok: 1, name, type: "view", viewOn },
            startedAt,
          );
        },
      };

      const dbHandler = dbHandlers[op];
      if (dbHandler) {
        return dbHandler();
      }

      if (!collName) {
        throw new Error(`Collection name is required for operation "${op}"`);
      }

      const mongoCollection = this.requireDb(dbName).collection(collName);
      const normalizeCriteria = (index = 0): Record<string, unknown> =>
        this.normalizeFilterCriteria(
          (opArgs[index] as Record<string, unknown>) ?? {},
        );
      const findDocuments = async (limitValue: number) =>
        mongoCollection
          .find(normalizeCriteria(), {
            promoteValues: false,
            bsonRegExp: false,
          })
          .skip(skip)
          .limit(limitValue)
          .toArray();
      const runUpdate = async (single: boolean) => {
        const update = opArgs[1] as Record<string, unknown>;
        return single
          ? mongoCollection.updateOne(normalizeCriteria(), update)
          : mongoCollection.updateMany(normalizeCriteria(), update);
      };
      const runDelete = async (single: boolean) =>
        single
          ? mongoCollection.deleteOne(normalizeCriteria())
          : mongoCollection.deleteMany(normalizeCriteria());

      const collectionHandlers: Record<string, () => Promise<QueryResult>> = {
        find: async () =>
          this.buildMongoDocumentRowsQueryResult(
            await findDocuments(limit),
            startedAt,
          ),
        findOne: async () =>
          this.buildMongoDocumentRowsQueryResult(
            await findDocuments(1),
            startedAt,
          ),
        countDocuments: async () => {
          const count = await mongoCollection.countDocuments(
            normalizeCriteria(),
          );
          return this.buildMongoSingleRowQueryResult({ count }, startedAt);
        },
        insertOne: async () => {
          const doc = opArgs[0] as Record<string, unknown>;
          const result = await mongoCollection.insertOne(doc);
          return this.buildMongoSingleRowQueryResult(
            {
              acknowledged: result.acknowledged,
              insertedId: String(result.insertedId),
            },
            startedAt,
            result.acknowledged ? 1 : 0,
          );
        },
        insertMany: async () => {
          const docs = opArgs[0] as Record<string, unknown>[];
          const result = await mongoCollection.insertMany(docs);
          return this.buildMongoSingleRowQueryResult(
            {
              acknowledged: result.acknowledged,
              insertedCount: result.insertedCount,
            },
            startedAt,
            result.insertedCount,
          );
        },
        updateOne: async () => {
          const result = await runUpdate(true);
          return this.buildMongoSingleRowQueryResult(
            {
              matchedCount: result.matchedCount,
              modifiedCount: result.modifiedCount,
            },
            startedAt,
            result.modifiedCount,
          );
        },
        updateMany: async () => {
          const result = await runUpdate(false);
          return this.buildMongoSingleRowQueryResult(
            {
              matchedCount: result.matchedCount,
              modifiedCount: result.modifiedCount,
            },
            startedAt,
            result.modifiedCount,
          );
        },
        deleteOne: async () => {
          const result = await runDelete(true);
          return this.buildMongoSingleRowQueryResult(
            { deletedCount: result.deletedCount },
            startedAt,
            result.deletedCount,
          );
        },
        deleteMany: async () => {
          const result = await runDelete(false);
          return this.buildMongoSingleRowQueryResult(
            { deletedCount: result.deletedCount },
            startedAt,
            result.deletedCount,
          );
        },
        aggregate: async () => {
          if (!Array.isArray(opArgs[0])) {
            throw new Error(
              "MongoDB aggregate expects a pipeline array as the first argument.",
            );
          }
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
          return this.buildMongoDocumentRowsQueryResult(docs, startedAt);
        },
        createIndex: async () => {
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
          return this.buildMongoSingleRowQueryResult(
            { ok: 1, name },
            startedAt,
          );
        },
      };

      const collectionHandler = collectionHandlers[op];
      if (collectionHandler) {
        return collectionHandler();
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
      const mappedRows = this.mapQueryResultRowsToObjects(result);
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
    const startTime = performance.now();
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
    const requiresClientSideFiltering = this.shouldUseClientSideFiltering(
      normalizedFilters,
      schemaColumns,
    );
    const offset = Math.max(0, (request.page - 1) * request.pageSize);

    if (!requiresClientSideFiltering) {
      try {
        const criteria = this.buildMongoFilterCriteria(
          normalizedFilters,
          schemaColumns,
        );
        const sort = request.sort
          ? ([
              [request.sort.column, request.sort.direction === "desc" ? -1 : 1],
              ...(request.sort.column === "_id" ? [] : [["_id", 1] as const]),
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
          mongoIdTypes: rows.map(
            (row) => this.tableRowIdTypes.get(row) ?? null,
          ),
          totalCount,
          executionTimeMs: Math.round(performance.now() - startTime),
        };
      } catch {
        // Fall through to display-value filtering when server-side BSON coercion is unavailable.
      }
    }

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
      boundedReadLimit + 1,
    );
    if (rows.length > boundedReadLimit) {
      throw new Error(
        `MongoDB filtering or sorting exceeded the ${boundedReadLimit}-row safety limit. Narrow the filter before continuing or exporting.`,
      );
    }
    const comparisonColumns =
      schemaColumns.length > 0
        ? schemaColumns
        : inferColumnsFromRows(rows, "_id", { nullableMode: "schemaLess" }).map(
            (column) => {
              const categories = new Set(
                rows
                  .filter((row) => row[column.name] != null)
                  .map((row) => this.rowValueCategory(row, column.name)),
              );
              const category: TypeCategory =
                categories.size === 1
                  ? [...categories][0]
                  : categories.size > 0 &&
                      [...categories].every(
                        (category) =>
                          category === "integer" ||
                          category === "decimal" ||
                          category === "float",
                      )
                    ? "decimal"
                    : "other";
              return {
                ...column,
                category,
                type: category,
                nativeType: category,
                filterOperators: resolveFilterOperators(category, {
                  filterable: column.filterable,
                  nullable: column.nullable,
                }),
              };
            },
          );
    const filtered = applyFilters(
      rows,
      normalizedFilters,
      comparisonColumns,
      this.rowValueCategory,
    );
    const sorted = applySort(
      filtered,
      request.sort,
      comparisonColumns,
      this.rowValueCategory,
    );
    const paged = pageRows(sorted, request.page, request.pageSize);
    return {
      columns: comparisonColumns,
      rows: paged,
      mongoIdTypes: paged.map((row) => this.tableRowIdTypes.get(row) ?? null),
      totalCount: request.skipCount ? 0 : sorted.length,
      executionTimeMs: Math.round(performance.now() - startTime),
    };
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
    context?: DriverOperationContext,
  ): Promise<DriverMutationResult> {
    if (
      request.updates.some(
        ({ primaryKeys }) =>
          Object.keys(primaryKeys).length !== 1 ||
          !Object.hasOwn(primaryKeys, "_id") ||
          primaryKeys._id === undefined ||
          primaryKeys._id === null,
      )
    ) {
      throw new Error("MongoDB update requires a complete _id for every row.");
    }
    const collection = this.requireDb(request.database).collection(
      request.table,
    );
    let affectedRows = 0;
    for (const update of request.updates) {
      context?.signal.throwIfAborted();
      if (
        Object.hasOwn(update.changes, "_id") &&
        update.changes._id !== update.primaryKeys._id
      ) {
        throw new Error("MongoDB does not support updating the _id field.");
      }
      const criteria = this.normalizeCriteria({
        ...update.primaryKeys,
        ...(update.originalValues ?? {}),
      });
      criteria._id = { $eq: update.primaryKeys._id };
      const options = this.getMutationTimeoutOptions(context);
      const result = options
        ? await collection.updateOne(
            criteria,
            { $set: update.changes },
            options,
          )
        : await collection.updateOne(criteria, { $set: update.changes });
      affectedRows += result.matchedCount;
    }
    return { affectedRows };
  }

  async insertRow(
    request: DriverInsertRowRequest,
    context?: DriverOperationContext,
  ): Promise<DriverMutationResult> {
    context?.signal.throwIfAborted();
    const collection = this.requireDb(request.database).collection(
      request.table,
    );
    const options = this.getMutationTimeoutOptions(context);
    const result = options
      ? await collection.insertOne(request.values, options)
      : await collection.insertOne(request.values);
    return { affectedRows: result.acknowledged ? 1 : 0 };
  }

  async deleteRows(
    request: DriverDeleteRowsRequest,
    context?: DriverOperationContext,
  ): Promise<DriverMutationResult> {
    const collection = await prepareDeleteBatch(
      request.primaryKeyValuesList,
      context,
      () => {
        if (
          request.primaryKeyValuesList.some(
            (entry) =>
              Object.keys(entry).length !== 1 ||
              !Object.hasOwn(entry, "_id") ||
              entry._id === undefined ||
              entry._id === null,
          )
        ) {
          throw new Error(
            "MongoDB delete requires a complete _id for every row.",
          );
        }
        return this.requireDb(request.database).collection(request.table);
      },
    );
    return deleteRowsSequentially(
      request.primaryKeyValuesList,
      context,
      (entry) => {
        // Keep the actual BSON identity: no ObjectId/string guessing.
        const filter = { _id: { $eq: entry._id } } as Parameters<
          typeof collection.deleteOne
        >[0];
        return async () => {
          const options = this.getMutationTimeoutOptions(context);
          const result = options
            ? await collection.deleteOne(filter, options)
            : await collection.deleteOne(filter);
          return result.acknowledged !== true
            ? "unknown"
            : result.deletedCount > 0
              ? "deleted"
              : "notfound";
        };
      },
    );
  }

  private getMutationTimeoutOptions(
    context?: DriverOperationContext,
  ): { timeoutMS: number } | undefined {
    if (!context || !Number.isFinite(context.deadline)) {
      return undefined;
    }
    return { timeoutMS: Math.max(1, Math.ceil(context.deadline - Date.now())) };
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
    const collectionRef = this.buildCollectionRef(dbRef, table);

    if (operation === "insert") {
      const doc = this.serializeMongosh(data.values ?? {});
      return `${collectionRef}.insertOne(${doc})`;
    }
    if (operation === "update") {
      const filter = this.serializeMongosh({
        _id: { $eq: data.primaryKeys?._id },
      });
      const update = this.serializeMongosh({ $set: data.changes ?? {} });
      // Execution applies updateOne per row (see updateRows); the preview must
      // match so copy-paste does not escalate to a multi-row write.
      return `${collectionRef}.updateOne(\n  ${filter},\n  ${update}\n)`;
    }
    return (data.primaryKeyValuesList ?? [data.primaryKeys ?? {}])
      .map(
        (entry) =>
          `${collectionRef}.deleteOne(${this.serializeMongosh({ _id: { $eq: entry._id } })})`,
      )
      .join(";\n");
  }

  async runTransaction(operations: TransactionOperation[]): Promise<void> {
    // MongoDB multi-document transactions require sessions/replica sets and
    // per-op retry semantics this text-protocol driver does not implement.
    // Fail closed instead of silently applying a non-atomic prefix on error.
    // Table mutations already avoid this path for multi-row driver applies
    // (see executeAtomicSqlApplyPlan); single-op callers are unaffected.
    if (operations.length > 1) {
      throw new Error(
        "[RapiDB] MongoDB driver does not support atomic multi-operation transactions. Apply one row at a time.",
      );
    }
    for (const operation of operations) {
      // One text item can contain several statements; query() executes all of
      // them in order. Reject before the first write rather than applying a
      // non-atomic prefix and reporting an error on the second statement.
      if (
        parseMongoshOperations(normalizeMongoshQueryText(operation.sql))
          .length !== 1
      ) {
        throw new Error(
          "[RapiDB] MongoDB driver does not support atomic multi-operation transactions. Apply one row at a time.",
        );
      }
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

  coerceOriginalValue(value: unknown, column: ColumnTypeMeta): unknown {
    return column.nativeType === "string"
      ? value
      : this.coerceInputValue(value, column);
  }

  coercePrimaryKeyValue(value: unknown, column: ColumnTypeMeta): unknown {
    // Table rows serialize _id for display. Never reinterpret an untyped string
    // using the sampled column type: it may identify a different document.
    if (
      column.name !== "_id" ||
      value === null ||
      typeof value !== "object" ||
      Array.isArray(value) ||
      Object.keys(value).length !== 1 ||
      !Object.hasOwn(value, "$rapidbMongoId")
    ) {
      throw new Error(
        "MongoDB row identity requires an explicit supported _id type hint.",
      );
    }
    return this.coerceInputValue(value, column);
  }

  coerceInputValue(value: unknown, column: ColumnTypeMeta): unknown {
    if (
      column.name === "_id" &&
      value !== null &&
      typeof value === "object" &&
      !Array.isArray(value) &&
      Object.keys(value).length === 1 &&
      Object.hasOwn(value, "$rapidbMongoId")
    ) {
      const hint = (value as { $rapidbMongoId?: unknown }).$rapidbMongoId;
      if (
        hint !== null &&
        typeof hint === "object" &&
        !Array.isArray(hint) &&
        Object.keys(hint).length === 2 &&
        Object.hasOwn(hint, "type") &&
        Object.hasOwn(hint, "value")
      ) {
        const { type, value: id } = hint as {
          type?: unknown;
          value?: unknown;
        };
        if (typeof id === "string" && type === "string") return id;
        if (
          typeof id === "string" &&
          type === "objectId" &&
          /^[0-9a-f]{24}$/i.test(id)
        ) {
          return new ObjectId(id);
        }
      }
      throw new Error("Invalid MongoDB _id type hint.");
    }
    if (value === null || value === undefined || value === "") {
      return value;
    }

    if (value === NULL_SENTINEL) {
      return null;
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
      if (isHexLike(normalized)) {
        return new Binary(
          parseHexToBuffer(normalized),
          binarySubtypeFromColumn(column),
        );
      }
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
    return `${this.buildCollectionRef(dbRef, coll)}.insertOne({ })`;
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
    const rawHost = this.config.host?.trim() || "localhost";
    const host = isIPv6(rawHost) ? `[${rawHost}]` : rawHost;
    const port = this.config.port ?? 27017;
    const auth = this.config.username
      ? `${encodeURIComponent(this.config.username)}:${encodeURIComponent(this.config.password ?? "")}@`
      : "";
    const database = encodeURIComponent(this.defaultDatabaseName());
    const params = new URLSearchParams();
    if (this.config.authSource) {
      params.set("authSource", this.config.authSource);
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

  private buildCollectionRef(dbRef: string, collectionName: string): string {
    return /^[A-Za-z_$][A-Za-z0-9_$]*$/.test(collectionName)
      ? `${dbRef}.${collectionName}`
      : `${dbRef}.getCollection(${JSON.stringify(collectionName)})`;
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
    const row = Object.fromEntries(
      Object.entries(document).map(([key, value]) => [
        key,
        formatMongoDisplayValue(value),
      ]),
    );
    this.tableRowCategories.set(
      row,
      new Map(
        Object.entries(document).map(([key, value]) => [
          key,
          inferMongoSchemaType(value).category,
        ]),
      ),
    );
    this.tableRowIdTypes.set(
      row,
      document._id instanceof ObjectId
        ? "objectId"
        : typeof document._id === "string"
          ? "string"
          : null,
    );
    return row;
  }

  private async describeSchemaColumns(
    database: string,
    table: string,
    limit: number,
  ): Promise<ColumnTypeMeta[]> {
    const documents = await this.readSchemaDocuments(database, table, limit);
    const isView = await this.isView(database, table);
    const inferredColumns = inferColumnsFromRows(documents, "_id", {
      primaryKeyNames: isView ? [] : ["_id"],
      nullableMode: "schemaLess",
    });

    return inferredColumns.map((inferredColumn) => {
      const { name, isPrimaryKey, primaryKeyOrdinal } = inferredColumn;
      const sample = selectMongoSchemaSample(documents, name);
      const { category, nativeType, bsonSubtype } =
        inferMongoSchemaType(sample);
      const filterable = category !== "spatial";
      const nullable = !isPrimaryKey;

      return {
        name,
        type: nativeType,
        nativeType,
        bsonSubtype,
        category,
        nullable,
        defaultValue: undefined,
        isPrimaryKey,
        primaryKeyOrdinal,
        isForeignKey: false,
        filterable,
        filterOperators: resolveFilterOperators(category, {
          filterable,
          nullable,
        }),
        valueSemantics: "plain",
      } satisfies ColumnTypeMeta;
    });
  }

  private async isView(database: string, table: string): Promise<boolean> {
    try {
      const definition = await this.getCollectionDefinition(database, table);
      return definition.type === "view";
    } catch {
      return false;
    }
  }

  private async readSchemaDocuments(
    database: string,
    table: string,
    limit: number,
  ): Promise<Record<string, unknown>[]> {
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
  }

  private async readRows(
    database: string,
    table: string,
    limit: number,
  ): Promise<Record<string, unknown>[]> {
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
  }

  private normalizeCriteria(
    criteria: Record<string, unknown>,
  ): Record<string, unknown> {
    return { ...criteria };
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

  private shouldUseClientSideFiltering(
    filters: readonly FilterExpression[],
    columns: readonly ColumnTypeMeta[],
  ): boolean {
    if (filters.length === 0 || columns.length === 0) {
      return false;
    }

    const columnMap = new Map(columns.map((column) => [column.name, column]));
    return filters.some((filter) => {
      if (filter.operator !== "like" && filter.operator !== "ilike") {
        return false;
      }

      const column = columnMap.get(filter.column);
      if (!column) {
        return false;
      }

      return (
        column.category === "json" ||
        column.category === "array" ||
        MONGO_CLIENT_SIDE_LIKE_NATIVE_TYPES.has(column.nativeType)
      );
    });
  }

  private serializeMongosh(value: unknown): string {
    if (value === null) return "null";
    if (value === undefined) return "undefined";
    if (typeof value === "boolean") return String(value);
    if (typeof value === "number") return String(value);
    if (typeof value === "bigint") return `${value}n`;
    if (typeof value === "string") {
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
      column.category === "binary" ||
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

  private buildMongoSingleRowQueryResult(
    row: Record<string, unknown>,
    startedAt: number,
    affectedRows?: number,
  ): QueryResult {
    const columns = Object.keys(row);
    return {
      columns,
      rows: [this.mapRowToQueryRow(row, columns)],
      rowCount: 1,
      affectedRows,
      executionTimeMs: Date.now() - startedAt,
    };
  }

  private buildMongoDocumentRowsQueryResult(
    docs: readonly Record<string, unknown>[],
    startedAt: number,
  ): QueryResult {
    const rows = docs.map((doc) => this.toRow(doc));
    const columns = inferColumnsFromRows(rows, "_id").map(
      (column) => column.name,
    );
    return {
      columns,
      rows: rows.map((row) => this.mapRowToQueryRow(row, columns)),
      rowCount: rows.length,
      executionTimeMs: Date.now() - startedAt,
    };
  }

  private mapQueryResultRowsToObjects(
    result: QueryResult,
  ): Record<string, unknown>[] {
    return result.rows.map((row) =>
      Object.fromEntries(
        result.columns.map((column, index) => [column, row[`__col_${index}`]]),
      ),
    );
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
