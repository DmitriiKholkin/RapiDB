import { isIP, isIPv6 } from "node:net";
import { checkServerIdentity } from "node:tls";
import vm from "node:vm";
import {
  Binary,
  BSON,
  BSONRegExp,
  BSONSymbol,
  Code,
  DBRef,
  Decimal128,
  Double,
  type FindOptions,
  Int32,
  Long,
  MaxKey,
  MinKey,
  MongoClient,
  MongoOperationTimeoutError,
  MongoServerError,
  ObjectId,
  Timestamp,
  UUID,
} from "mongodb";
import {
  buildMongoFieldsUri,
  getMongoConnectionUri,
  normalizeMongoConnectionConfig,
  resolveMongoConnectionMode,
  stripMongoUriTlsOptions,
} from "../../shared/mongoConnection";
import { resolveMongoDatabaseName } from "../../shared/mongoDatabase";
import { compareNumericTokens } from "../../shared/numericNormalization";
import { QUERY_LIMIT_POLICY } from "../../shared/safetyContracts";
import type { ConnectionConfig } from "../connectionManager";
import {
  getSshTcpForwardTransport,
  getTlsServername,
} from "../driverRuntimeConfig";
import { resolveConnectionTlsSettings } from "../services/connectionTls";
import {
  JsonNumericPrecisionError,
  parseJsonForWrite,
} from "../utils/jsonWriteSafety";
import { allowReadOnlyQuery, denyReadOnlyQuery } from "../utils/readOnlyGuards";
import {
  formatDatetimeForDisplay,
  hexFromBuffer,
  isHexLike,
  parseHexToBuffer,
} from "./BaseDBDriver";
import { deleteRowsSequentially, prepareDeleteBatch } from "./deleteOutcomes";
import { getMetadataSdkOptions } from "./metadataCancellation";
import { assertMongoshSupportedSyntax } from "./mongoshSyntax";
import {
  applyFilters,
  applySort,
  createNoSqlUnsupportedMetadataHandlers,
  inferColumnsFromRows,
  pageRows,
  splitInFilterValues,
} from "./nosqlUtils";
import {
  type DriverTimeoutSettingsProvider,
  getDefaultDriverTimeoutSettings,
  getQueryRemainingTimeoutMs,
  throwIfQueryCancelled,
} from "./timeout";
import type {
  ColumnMeta,
  ColumnTypeMeta,
  DatabaseInfo,
  DriverDeleteRowsRequest,
  DriverEntityManifest,
  DriverInsertRowRequest,
  DriverMutationResult,
  DriverOperationContext,
  DriverTableExportChunk,
  DriverTablePageRequest,
  DriverTablePageResult,
  DriverUpdateRowOutcome,
  DriverUpdateRowsRequest,
  FilterExpression,
  FilterOperator,
  IDBDriver,
  IndexMeta,
  MongoValueTypeHint,
  PaginationResult,
  QueryColumnMeta,
  QueryExecutionOptions,
  QueryResult,
  SchemaInfo,
  TableInfo,
  TransactionOperation,
  TypeCategory,
} from "./types";
import {
  filterOperatorsForCategory,
  NULL_SENTINEL,
  resolveFilterOperators,
} from "./types";
import {
  completeDriverUpdateRowsResult,
  failedDriverUpdateRows,
} from "./updateRowOutcomes";

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

type MongoSdkOptions = { signal?: AbortSignal; timeoutMS?: number };

const MONGO_DB_POINTER_TYPE: MongoSchemaType = {
  category: "other",
  nativeType: "dbPointer",
};

type MongoshOperation = {
  dbName?: string;
  collName?: string;
  op: string;
  args: unknown[];
  chainOps: MongoshChainOperation[];
};

const MONGO_SCALAR_MISS = Symbol("mongo-scalar-miss");
const MONGO_NUMERIC_LITERAL_RE =
  /^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:e[+-]?\d+)?$/i;
const MONGO_CLIENT_SIDE_LIKE_NATIVE_TYPES = new Set([
  "objectId",
  "javascript",
  "javascriptWithScope",
  "regex",
  "dbPointer",
  "dbRef",
  "minKey",
  "maxKey",
]);
const MONGO_TYPED_EDIT_NATIVE_TYPES = new Set([
  "objectId",
  "uuid",
  "date",
  "timestamp",
  "decimal",
  "decimal128",
  "double",
  "number",
  "int",
  "int32",
  "long",
  "int64",
  "bool",
  "javascriptWithScope",
  "dbPointer",
  "dbRef",
  "regex",
]);
const MONGO_NULL_LIKE_BSON_TYPES = ["null", "missing", "undefined"];
const MONGO_BSON_EMBEDDED_NATIVE_TYPES = new Set(["bsonDocument", "bsonArray"]);
const MONGO_SPECIAL_DOUBLE_LITERALS = new Set([
  "NaN",
  "Infinity",
  "-Infinity",
  "-0",
]);
const MONGO_READ_HELPER_OPTION_KEYS = new Map([
  ["find", new Set(["allowDiskUse", "comment", "maxTimeMS"])],
  ["findOne", new Set(["allowDiskUse", "comment", "maxTimeMS"])],
  ["aggregate", new Set(["allowDiskUse", "comment", "maxTimeMS"])],
  ["countDocuments", new Set(["comment", "maxTimeMS"])],
]);
const UUID_VALUE_RE =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const BASE64_VALUE_RE =
  /^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/;
const MONGO_BINDATA_ARGUMENTS_ERROR =
  "BinData requires an integer subtype from 0 to 255 and a canonical base64 string.";
const DISPLAY_DATETIME_RE =
  /^(\d{4}|[+-]\d{6})-(\d{2})-(\d{2})(?:[ T](\d{2}):(\d{2})(?::(\d{2})(?:\.(\d{1,9}))?)?)?(?: ?(Z|[+-]\d{2}(?::?\d{2})?))?$/i;
const MONGO_EXACT_TEXT_NATIVE_TYPES = new Set([
  "string",
  "symbol",
  "javascript",
]);
const TIMESTAMP_LITERAL_RE = /^Timestamp\((\d+),\s*(\d+)\)$/i;
const MONGOSH_ISODATE_RE =
  /^(\d{4})-?(\d{2})-?(\d{2})(?:[T ](\d{2})(?::?(\d{2})(?::?(\d{2})(?:\.(\d+))?)?)?(Z|[+-]\d{2}(?::?\d{2})?)?)?$/;
const MONGOSH_ISODATE_MIN_MS = -62167219200000;
const MONGOSH_ISODATE_MAX_MS = 253402300799999;

const MONGODB_READ_ONLY_QUERY_REASON =
  "[RapiDB] Read-only MongoDB connections allow only find, findOne, countDocuments, getCollectionNames, and aggregate queries without $out or $merge (read-only cursor modifiers only).";
const MONGODB_QUERY_HARD_CAP = QUERY_LIMIT_POLICY.hardCap;
const MONGODB_SCHEMA_SAMPLE_LIMIT = 50;
const MONGODB_MUTATION_ARGUMENT_COUNTS = new Map([
  ["insertOne", 1],
  ["insertMany", 1],
  ["updateOne", 2],
  ["updateMany", 2],
  ["deleteOne", 1],
  ["deleteMany", 1],
]);
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
const MONGOSH_UNSAFE_OBJECT_KEY_RE = /^(?:constructor|prototype|__\w+__)$/;
const MONGOSH_IDENTIFIER_START_RE = /[$_\p{ID_Start}]/u;
const MONGOSH_IDENTIFIER_RE = /^[$\p{ID_Continue}\u200C\u200D]+/u;

function isMongoshObjectKeyPosition(
  previous: string,
  following: string | undefined,
  computedBrackets: number,
): boolean {
  return (
    computedBrackets === 0 &&
    following === ":" &&
    (previous === "{" || previous === ",")
  );
}

function mongoshStringKeyValue(literal: string): string | undefined {
  if (!literal.includes("\\")) return literal.slice(1, -1);
  try {
    return decodeJavaScriptStringLiteral(literal);
  } catch {
    return undefined;
  }
}

function ensureMongoshObjectKeyIsSafe(key: string | undefined): void {
  if (key === undefined) {
    throw new Error(
      "[RapiDB] Unsafe mongosh query blocked: object key escape sequences cannot be verified.",
    );
  }
  if (MONGOSH_UNSAFE_OBJECT_KEY_RE.test(key)) {
    throw new Error(
      `[RapiDB] Unsafe mongosh query blocked: disallowed object key "${key}".`,
    );
  }
}

function normalizeMongoshQueryText(queryText: string): string {
  const trimmed = queryText.trim();
  if (
    trimmed
      .split(/[\r\n\u2028\u2029]/)
      .every((line) => !line.trim() || line.trim().startsWith("//"))
  ) {
    return "";
  }
  return trimmed.replace(/;+\s*$/, "").trim();
}

const MONGOSH_REGEX_PREFIX_CHARS = new Set([
  "",
  "(",
  ",",
  "=",
  ":",
  "[",
  "!",
  "&",
  "|",
  "?",
  "{",
  ";",
  "~",
  "^",
  "<",
  ">",
  "*",
  "%",
]);
const MONGOSH_LINE_TERMINATOR_RE = /[\n\r\u2028\u2029]/;

function nextMongoshCodeChar(
  queryText: string,
  start: number,
): string | undefined {
  let index = start;
  while (index < queryText.length) {
    if (/\s/.test(queryText[index])) {
      index++;
    } else if (queryText.startsWith("//", index)) {
      while (
        index < queryText.length &&
        !MONGOSH_LINE_TERMINATOR_RE.test(queryText[index])
      ) {
        index++;
      }
    } else if (queryText.startsWith("/*", index)) {
      const end = queryText.indexOf("*/", index + 2);
      if (end < 0) return undefined;
      index = end + 2;
    } else if (
      queryText.startsWith("<!--", index) ||
      queryText.startsWith("-->", index)
    ) {
      return undefined;
    } else {
      return queryText[index];
    }
  }
  return "";
}

function rawMongoshCode(code: string, afterAmbiguousSlash = false): string {
  if (code.includes("\\")) {
    throw new Error(
      afterAmbiguousSlash
        ? "[RapiDB] Unsafe mongosh query blocked: escape sequences cannot be verified after an ambiguous '/'; rewrite the division or remove escapes."
        : "[RapiDB] Unsafe mongosh query blocked: disallowed escape sequence outside a string literal.",
    );
  }
  return code;
}

function maskMongoshLiterals(queryText: string): string {
  if (queryText.startsWith("#!")) return rawMongoshCode(queryText);
  let output = "";
  let previous = "";
  let depth = 0;
  let template: { keep: boolean } | undefined;
  const templates: Array<{ depth: number; keep: boolean }> = [];
  const brackets: boolean[] = [];
  let computedBrackets = 0;
  let index = 0;
  while (index < queryText.length) {
    const char = queryText[index];
    if (template) {
      if (char === "\\") {
        if (template.keep) output += queryText.slice(index, index + 2);
        index += 2;
      } else if (char === "`") {
        output += char;
        previous = char;
        template = undefined;
        index++;
      } else if (char === "$" && queryText[index + 1] === "{") {
        output += "${";
        templates.push({ depth, keep: template.keep });
        depth++;
        previous = "{";
        template = undefined;
        index += 2;
      } else {
        if (template.keep) output += char;
        index++;
      }
      continue;
    }
    if (/\s/.test(char)) {
      output += char;
      index++;
      continue;
    }
    if (
      char === "\\" ||
      queryText.startsWith("<!--", index) ||
      queryText.startsWith("-->", index)
    ) {
      return output + rawMongoshCode(queryText.slice(index));
    }
    if (queryText.startsWith("//", index)) {
      let end = index + 2;
      while (
        end < queryText.length &&
        !MONGOSH_LINE_TERMINATOR_RE.test(queryText[end])
      ) {
        end++;
      }
      output += " ";
      index = end;
      continue;
    }
    if (queryText.startsWith("/*", index)) {
      const end = queryText.indexOf("*/", index + 2);
      if (end < 0) return output + rawMongoshCode(queryText.slice(index));
      output += " ";
      index = end + 2;
      continue;
    }
    if (char === '"' || char === "'") {
      let end = index + 1;
      while (
        end < queryText.length &&
        queryText[end] !== char &&
        !/[\n\r]/.test(queryText[end])
      ) {
        end += queryText[end] === "\\" ? 2 : 1;
      }
      if (end >= queryText.length || queryText[end] !== char) {
        return output + rawMongoshCode(queryText.slice(index));
      }
      end++;
      const following = nextMongoshCodeChar(queryText, end);
      const literal = queryText.slice(index, end);
      if (isMongoshObjectKeyPosition(previous, following, computedBrackets)) {
        ensureMongoshObjectKeyIsSafe(mongoshStringKeyValue(literal));
        output += char + char;
      } else {
        output +=
          computedBrackets > 0 || following === undefined || following === ":"
            ? literal
            : char + char;
      }
      previous = char;
      index = end;
      continue;
    }
    if (char === "`") {
      output += char;
      template = { keep: computedBrackets > 0 };
      index++;
      continue;
    }
    if (char === "/") {
      if (!MONGOSH_REGEX_PREFIX_CHARS.has(previous)) {
        return output + rawMongoshCode(queryText.slice(index), true);
      }
      let end = index + 1;
      let inClass = false;
      while (
        end < queryText.length &&
        !MONGOSH_LINE_TERMINATOR_RE.test(queryText[end]) &&
        (inClass || queryText[end] !== "/")
      ) {
        if (queryText[end] === "\\") end++;
        else if (queryText[end] === "[") inClass = true;
        else if (queryText[end] === "]") inClass = false;
        end++;
      }
      if (end >= queryText.length || queryText[end] !== "/") {
        return output + rawMongoshCode(queryText.slice(index));
      }
      output += computedBrackets > 0 ? queryText.slice(index, end + 1) : "//";
      previous = "/";
      index = end + 1;
      continue;
    }
    if (char === "[") {
      const computed = !MONGOSH_REGEX_PREFIX_CHARS.has(previous);
      brackets.push(computed);
      if (computed) computedBrackets++;
    }
    if (
      (previous === "{" || previous === ",") &&
      MONGOSH_IDENTIFIER_START_RE.test(char)
    ) {
      const identifier =
        MONGOSH_IDENTIFIER_RE.exec(queryText.slice(index))?.[0] ?? char;
      const following = nextMongoshCodeChar(
        queryText,
        index + identifier.length,
      );
      if (isMongoshObjectKeyPosition(previous, following, computedBrackets)) {
        ensureMongoshObjectKeyIsSafe(identifier);
        output += "k";
        previous = "k";
        index += identifier.length;
        continue;
      }
    }
    if (char === "]" && brackets.pop()) computedBrackets--;
    if (char === "{") depth++;
    if (char === "}") {
      depth--;
      const open = templates.at(-1);
      if (open && open.depth === depth) {
        templates.pop();
        output += char;
        template = { keep: open.keep };
        index++;
        continue;
      }
    }
    output += char;
    previous = char;
    index++;
  }
  return output;
}

function ensureMongoshQueryIsSafe(rawQueryText: string): void {
  const queryText = maskMongoshLiterals(rawQueryText);
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
  const { trackProxy, isProxy } = (() => {
    const proxies = new WeakSet();
    return {
      trackProxy: (value) => {
        safeApply(nativeWeakSetAdd, proxies, [value]);
        return value;
      },
      isProxy: (value) => safeApply(nativeWeakSetHas, proxies, [value]),
    };
  })();
  const serializeOperations = (() => {
    const entries = Object.entries;
    const stringify = JSON.stringify;
    const create = Object.create;
    const setPrototype = Object.setPrototypeOf;
    const getPrototype = Object.getPrototypeOf;
    const regexPrototype = nativeRegExp.prototype;
    const regexSource = Object.getOwnPropertyDescriptor(regexPrototype, 'source').get;
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
        if (isProxy(value))
          throw new Error('MongoDB operation results and db/collection handles cannot be used as values in mongosh queries; run each operation as a separate statement.');
        if (safeApply(nativeWeakSetHas, active, [value]))
          throw new Error('Cyclic mongosh values are not supported.');
        safeApply(nativeWeakSetAdd, active, [value]);
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
  const requireNamespaceName = (name) => {
    if (typeof name !== 'string' || name.length === 0) {
      throw new Error('MongoDB database/collection name must be a non-empty string.');
    }
    return name;
  };
  const rejectProxyAsValue = () => {
    throw new Error('MongoDB operation results and db/collection handles cannot be used as values in mongosh queries; run each operation as a separate statement.');
  };
  const createChainProxy = (operation) => trackProxy(new InternalProxy({}, {
    ownKeys: rejectProxyAsValue,
    get(_target, method) {
      if (typeof method !== 'string') return undefined;
      return (...args) => {
        operation.chainOps.push({ op: method, args });
        return createChainProxy(operation);
      };
    },
  }));
  // Every recorded operation must return an opaque result, including DB helpers.
  // Returning undefined here would let spreads/assign silently erase a nested
  // operation and turn a destructive filter into an explicit empty document.
  // The AST boundary forbids results in data expressions; proxies are defense
  // in depth, not an attempt to emulate or validate arbitrary JS dataflow.
  const recordOperation = (dbName, collName, op, args) => {
    const operation = { dbName, collName, op, args, chainOps: [] };
    operations.push(operation);
    return createChainProxy(operation);
  };
  const createCollProxy = (dbName, collName) => trackProxy(new InternalProxy({}, {
    ownKeys: rejectProxyAsValue,
    get(_target, method) {
      if (typeof method !== 'string' || method === 'then') return undefined;
      return (...args) => recordOperation(dbName, collName, method, args);
    },
  }));
  const createDbProxy = (dbName) => trackProxy(new InternalProxy({}, {
    ownKeys: rejectProxyAsValue,
    get(_target, prop) {
      if (typeof prop !== 'string') return undefined;
      if (prop === 'getSiblingDB') return (name) => createDbProxy(requireNamespaceName(name));
      if (prop === 'getCollection') return (name) => createCollProxy(dbName, requireNamespaceName(name));
      if (prop === 'runCommand' || prop === 'adminCommand') return (cmd) =>
        recordOperation(prop === 'adminCommand' ? 'admin' : dbName, undefined, 'runCommand', [cmd]);
      if (prop === 'getCollectionNames' || prop === 'createCollection' || prop === 'createView')
        return (...args) => recordOperation(dbName, undefined, prop, args);
      return createCollProxy(dbName, prop);
    },
  }));
  return createDbProxy();
  })();
  globalThis.Proxy = undefined;
  const Date = function mongoDate(...args) { return marker('Date', args); };
  const ISODate = function mongoISODate(...args) { return marker('ISODate', args); };
  const RegExp = function mongoRegExp(...args) { return marker('RegExp', args); };
  const BSONRegExp = function mongoBSONRegExp(...args) { return marker('BSONRegExp', args); };
  const ObjectId = function mongoObjectId(hex) { return marker('ObjectId', [hex]); };
  const BinData = function mongoBinData(...args) { return marker('BinData', args); };
  const DBRef = function mongoDBRef(collection, oid, database) { return marker('DBRef', [collection, oid, database]); };
  const BSONSymbol = function mongoBSONSymbol(value) { return marker('BSONSymbol', [value]); };
  const NumberLong = function mongoNumberLong(...args) { return marker('NumberLong', args); };
  const NumberInt = function mongoNumberInt(...args) { return marker('NumberInt', args); };
  const NumberDecimal = function mongoNumberDecimal(...args) { return marker('NumberDecimal', args); };
  const Timestamp = function mongoTimestamp(...args) { return marker('Timestamp', args); };
  const Code = function mongoCode(source, scope) { return marker('Code', [source, scope]); };
  const MinKey = function mongoMinKey() { return marker('MinKey', []); };
  const MaxKey = function mongoMaxKey() { return marker('MaxKey', []); };
  const Double = function mongoDouble(...args) { return marker('Double', args.length === 1 && args[0] === 0 && 1 / args[0] < 0 ? ['-0'] : args); };
`;

function reviveMongoshInteger(
  kind: "NumberInt" | "NumberLong",
  args: unknown[],
): Int32 | Long {
  const value = args[0];
  if (
    args.length !== 1 ||
    !["string", "number", "bigint"].includes(typeof value)
  ) {
    throw new Error(`${kind} requires one integer argument.`);
  }
  if (typeof value === "number" && !Number.isSafeInteger(value)) {
    throw new Error(
      `${kind} requires a safe integer number; use a string or bigint for exact large integers.`,
    );
  }
  const text = String(value).trim();
  if (!/^[+-]?\d+$/.test(text)) {
    throw new Error(`${kind} requires an integer without fractional digits.`);
  }
  const integer = BigInt(text);
  const int32 = kind === "NumberInt";
  const min = int32 ? -2147483648n : -9223372036854775808n;
  const max = int32 ? 2147483647n : 9223372036854775807n;
  if (integer < min || integer > max) {
    throw new Error(`${kind} integer is out of range (${min} to ${max}).`);
  }
  return int32
    ? new Int32(Number(integer))
    : Long.fromString(integer.toString());
}

function reviveMongoshDate(args: unknown[]): Date {
  if (
    args.length > 7 ||
    args.some(
      (value) =>
        !(
          args.length === 1 &&
          (typeof value === "string" || value instanceof Date)
        ) && !(typeof value === "number" && Number.isSafeInteger(value)),
    )
  ) {
    throw new Error(
      "Date requires no arguments, one date string or Date, or one to seven safe integer arguments.",
    );
  }
  const date = Reflect.construct(Date, args) as Date;
  if (!Number.isFinite(date.getTime())) {
    throw new Error("Date requires a valid date within the supported range.");
  }
  return date;
}

function reviveMongoshISODate(args: unknown[]): Date {
  if (args.length === 0) return new Date();
  const [input] = args;
  if (args.length === 1 && input instanceof Date) {
    return reviveMongoshDate(args);
  }
  if (args.length !== 1 || typeof input !== "string") {
    throw new Error(
      "ISODate requires no arguments or one ISO-8601 date string argument.",
    );
  }
  const match = MONGOSH_ISODATE_RE.exec(input);
  if (match) {
    const [, year, month, day, hour, minute, second, fraction, zone] = match;
    const fields = [month, day, hour, minute, second].map((part) =>
      Number(part ?? 0),
    );
    const yearNumber = Number(year);
    const leapYear =
      (yearNumber % 4 === 0 && yearNumber % 100 !== 0) ||
      yearNumber % 400 === 0;
    const daysInMonth =
      fields[0] === 2
        ? leapYear
          ? 29
          : 28
        : [4, 6, 9, 11].includes(fields[0])
          ? 30
          : 31;
    const offset =
      !zone || zone === "Z"
        ? "Z"
        : `${zone.slice(0, 3)}:${zone.slice(3).replace(":", "") || "00"}`;
    const date = new Date(
      `${year}-${month}-${day}T${hour ?? "00"}:${minute ?? "00"}:${second ?? "00"}.${`${fraction ?? ""}000`.slice(0, 3)}${offset}`,
    );
    if (
      fields[0] >= 1 &&
      fields[0] <= 12 &&
      fields[1] >= 1 &&
      fields[1] <= daysInMonth &&
      fields[2] <= 23 &&
      fields[3] <= 59 &&
      fields[4] <= 59 &&
      Number(zone?.slice(1, 3) ?? 0) <= 23 &&
      Number(zone?.slice(3).replace(":", "") || 0) <= 59 &&
      date.getTime() >= MONGOSH_ISODATE_MIN_MS &&
      date.getTime() <= MONGOSH_ISODATE_MAX_MS
    ) {
      return date;
    }
  }
  throw new Error(
    `ISODate requires a valid ISO-8601 date string from 0000-01-01 to 9999-12-31; ${JSON.stringify(input)} is not a valid ISODate.`,
  );
}

function reviveMongoshRegExp(args: unknown[]): BSONRegExp {
  if (args.length > 2) {
    throw new Error(
      "MongoDB RegExp accepts a pattern and optional flags only.",
    );
  }
  const original = args[0] instanceof BSONRegExp ? args[0] : undefined;
  const regex = new RegExp(
    original?.pattern ?? (args[0] === undefined ? "" : String(args[0])),
    args[1] === undefined ? original?.options : String(args[1]),
  );
  if (/[^imsu]/.test(regex.flags)) {
    throw new Error(
      "MongoDB regex flags must be limited to i, m, s, and u. JavaScript-only flags d, g, v, and y are not supported.",
    );
  }
  if (regex.source.includes("\u0000")) {
    throw new Error("MongoDB regex patterns cannot contain null bytes.");
  }
  return new BSONRegExp(regex.source, regex.flags);
}

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
  if (record.kind === "bigint") {
    const integer = BigInt(record.value as string);
    if (integer < -9223372036854775808n || integer > 9223372036854775807n) {
      throw new Error(
        'MongoDB bigint is outside the signed BSON int64 range. Use NumberDecimal("...") or a string for larger exact values.',
      );
    }
    return integer;
  }
  if (record.kind === "marker") {
    const args = (record.args ?? []).map(reviveMongoshValue);
    switch (record.value) {
      case "Date":
        return reviveMongoshDate(args);
      case "ISODate":
        return reviveMongoshISODate(args);
      case "RegExp":
        return reviveMongoshRegExp(args);
      case "BSONRegExp": {
        if (
          args.length < 1 ||
          args.length > 2 ||
          typeof args[0] !== "string" ||
          (args[1] !== undefined && typeof args[1] !== "string")
        ) {
          throw new Error(
            "BSONRegExp requires a string pattern and optional string options.",
          );
        }
        return new BSONRegExp(args[0], args[1]);
      }
      case "ObjectId":
        return args[0] === undefined
          ? new ObjectId()
          : new ObjectId(String(args[0]));
      case "BinData":
        if (args.length !== 2) throw new Error(MONGO_BINDATA_ARGUMENTS_ERROR);
        return createMongoBinData(args[0], args[1]);
      case "DBRef":
        return new DBRef(
          String(args[0]),
          args[1] as ObjectId,
          args[2] ? String(args[2]) : undefined,
        );
      case "BSONSymbol":
        return new BSONSymbol(String(args[0]));
      case "NumberLong":
      case "NumberInt":
        return reviveMongoshInteger(record.value, args);
      case "NumberDecimal":
        if (args.length !== 1 || typeof args[0] !== "string") {
          throw new Error(
            'NumberDecimal requires one string argument, for example NumberDecimal("0.123456789012345678901").',
          );
        }
        return Decimal128.fromString(args[0]);
      case "Timestamp": {
        const spec = args[0];
        const objectForm =
          spec !== null &&
          typeof spec === "object" &&
          Object.getPrototypeOf(spec) === Object.prototype;
        const t = objectForm ? (spec as { t?: unknown }).t : spec;
        const i = objectForm
          ? (spec as { i?: unknown }).i
          : args.length === 1
            ? 0
            : args[1];
        const uint32 = (part: unknown): part is number =>
          typeof part === "number" &&
          Number.isInteger(part) &&
          part >= 0 &&
          part <= 0xffffffff;
        if (
          (objectForm
            ? args.length !== 1
            : args.length < 1 || args.length > 2) ||
          !uint32(t) ||
          !uint32(i)
        ) {
          throw new Error(
            "Timestamp requires unsigned 32-bit integer arguments: Timestamp(t, i) or Timestamp({t, i}).",
          );
        }
        return new Timestamp({ t, i });
      }
      case "Code":
        return args[1] && typeof args[1] === "object"
          ? new Code(String(args[0]), args[1] as Record<string, unknown>)
          : new Code(String(args[0]));
      case "MinKey":
        return new MinKey();
      case "MaxKey":
        return new MaxKey();
      case "Double":
        if (
          args.length !== 1 ||
          !(
            (typeof args[0] === "number" && Number.isFinite(args[0])) ||
            MONGO_SPECIAL_DOUBLE_LITERALS.has(args[0] as string)
          )
        ) {
          throw new Error(
            'Double requires one finite number argument or one of "NaN", "Infinity", "-Infinity" or "-0".',
          );
        }
        return new Double(Number(args[0]));
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
    assertMongoshSupportedSyntax(queryText);
    const context = vm.createContext(Object.create(null), {
      codeGeneration: { strings: false, wasm: false },
      microtaskMode: "afterEvaluate",
    });
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

const MONGOSH_SUPPORTED_OPERATIONS = new Set([
  "find",
  "findOne",
  "countDocuments",
  "insertOne",
  "insertMany",
  "updateOne",
  "updateMany",
  "deleteOne",
  "deleteMany",
  "aggregate",
  "runCommand",
  "getCollectionNames",
  "createCollection",
  "createView",
  "createIndex",
  "drop",
]);

function validateMongoshOperation({
  op,
  args,
  chainOps,
}: MongoshOperation): void {
  if (!MONGOSH_SUPPORTED_OPERATIONS.has(op)) {
    throw new Error(
      `Unsupported mongosh operation: "${op}". No operations were submitted.`,
    );
  }
  const argumentCount = MONGODB_MUTATION_ARGUMENT_COUNTS.get(op);
  if (argumentCount !== undefined && args.length > argumentCount) {
    throw new Error(
      `[RapiDB] MongoDB ${op} options are not supported by this helper. Use db.runCommand with explicit command options instead. No operations were submitted.`,
    );
  }
  const optionKeys = MONGO_READ_HELPER_OPTION_KEYS.get(op);
  if (optionKeys) {
    const optionIndex = mongoReadHelperOptionIndex(op);
    const options = args[optionIndex] as Record<string, unknown> | undefined;
    if (
      args.length > optionIndex + 1 ||
      (options !== undefined &&
        (options === null ||
          typeof options !== "object" ||
          Object.getPrototypeOf(options) !== Object.prototype ||
          Object.keys(options).some((key) => !optionKeys.has(key)) ||
          (options.maxTimeMS !== undefined &&
            !(
              Number.isSafeInteger(options.maxTimeMS) &&
              (options.maxTimeMS as number) >= 0
            )) ||
          (options.allowDiskUse !== undefined &&
            typeof options.allowDiskUse !== "boolean")))
    ) {
      throw new Error(
        `[RapiDB] MongoDB ${op} supports only these options in this helper: ${[...optionKeys].join(", ")}. Use db.runCommand with explicit command options instead. No operations were submitted.`,
      );
    }
  }
  if (chainOps.length > 0 && !["find", "findOne", "aggregate"].includes(op)) {
    throw new Error(
      `[RapiDB] MongoDB ${op} does not support cursor modifiers. No operations were submitted.`,
    );
  }
  for (const chain of chainOps) {
    if (
      !["limit", "skip", "toArray", "pretty"].includes(chain.op) &&
      !(chain.op === "sort" && (op === "find" || op === "findOne"))
    ) {
      throw new Error(
        `MongoDB ${op} cursor modifier "${chain.op}" is not supported. No operations were submitted.`,
      );
    }
    if (
      (chain.op === "toArray" || chain.op === "pretty") &&
      chain.args.length
    ) {
      throw new Error(`MongoDB ${chain.op}() does not accept arguments.`);
    }
    if (
      chain.op === "skip" &&
      (chain.args.length !== 1 ||
        !Number.isSafeInteger(chain.args[0]) ||
        (chain.args[0] as number) < 0)
    ) {
      throw new Error("MongoDB skip() requires a non-negative safe integer.");
    }
    if (chain.op === "sort" && chain.args.length !== 1) {
      throw new Error("MongoDB find().sort() expects one object argument.");
    }
  }
  if (op === "createView") {
    if (!Array.isArray(args[2])) {
      throw new Error(
        "MongoDB createView requires an explicit pipeline array; use [] explicitly for an unfiltered view.",
      );
    }
    if (
      args.length < 3 ||
      args.length > 4 ||
      args
        .slice(0, 2)
        .some((name) => typeof name !== "string" || name.length === 0)
    ) {
      throw new Error(
        "MongoDB createView requires non-empty view and source collection names, a pipeline, and optional options.",
      );
    }
    const viewOptions = args[3];
    if (
      viewOptions !== undefined &&
      (viewOptions === null ||
        typeof viewOptions !== "object" ||
        Object.getPrototypeOf(viewOptions) !== Object.prototype ||
        Object.hasOwn(viewOptions, "viewOn") ||
        Object.hasOwn(viewOptions, "pipeline"))
    ) {
      throw new Error(
        "MongoDB createView options must be a document and cannot override viewOn or pipeline.",
      );
    }
  }
  if (["deleteOne", "deleteMany", "updateOne", "updateMany"].includes(op)) {
    const filter = args[0];
    if (
      filter === null ||
      typeof filter !== "object" ||
      Object.getPrototypeOf(filter) !== Object.prototype
    ) {
      throw new Error(
        `[RapiDB] MongoDB ${op} filter must be an explicit document. Use {} explicitly to target all documents.`,
      );
    }
  }

  const limitOp = chainOps.filter((chain) => chain.op === "limit").at(-1);
  const requestedLimit = limitOp?.args[0];
  if (
    limitOp &&
    (limitOp.args.length !== 1 ||
      typeof requestedLimit !== "number" ||
      !Number.isSafeInteger(requestedLimit) ||
      requestedLimit < 0)
  ) {
    throw new Error(
      "[RapiDB] MongoDB limit() requires a non-negative safe integer. Negative and fractional limits are not supported.",
    );
  }
  if (op === "aggregate") {
    if (
      buildAggregateChainStages(chainOps).some(
        (stage) =>
          "$limit" in stage &&
          !(Number.isSafeInteger(stage.$limit) && (stage.$limit as number) > 0),
      )
    ) {
      throw new Error(
        "[RapiDB] MongoDB aggregate().limit() requires a positive safe integer.",
      );
    }
    if (!Array.isArray(args[0])) {
      throw new Error(
        "MongoDB aggregate expects a pipeline array as the first argument.",
      );
    }
    if (
      pipelineEndsWithWriteStage(args[0]) &&
      chainOps.some((chain) => chain.op === "limit" || chain.op === "skip")
    ) {
      throw new Error(
        "[RapiDB] MongoDB aggregate() with a final $out or $merge stage cannot use limit() or skip(). Add $limit or $skip stages before the final stage instead.",
      );
    }
  }
  if (op === "find" || op === "findOne") {
    const projection = args[1];
    if (
      projection !== undefined &&
      projection !== null &&
      (typeof projection !== "object" || Array.isArray(projection))
    ) {
      throw new Error("MongoDB find projection must be an object.");
    }
    for (const chain of chainOps) {
      if (chain.op === "sort") {
        const sort = chain.args[0];
        if (sort === null || typeof sort !== "object" || Array.isArray(sort)) {
          throw new Error("MongoDB find().sort() expects an object.");
        }
      }
    }
  }
  if ((op === "drop" || op === "getCollectionNames") && args.length > 0) {
    throw new Error(
      op === "drop"
        ? "Collection drop() does not accept arguments."
        : "db.getCollectionNames() does not accept arguments.",
    );
  }
}

function mongoReadHelperOptionIndex(op: string): number {
  return op === "find" || op === "findOne" ? 2 : 1;
}

function buildAggregateChainStages(
  chainOps: readonly MongoshChainOperation[],
): Array<{ $limit: unknown } | { $skip: unknown }> {
  const stages: Array<{ $limit: unknown } | { $skip: unknown }> = [];
  for (const chain of chainOps) {
    if (chain.op === "skip" && chain.args[0] !== 0) {
      stages.push({ $skip: chain.args[0] });
    } else if (chain.op === "limit") {
      stages.push({ $limit: chain.args[0] });
    }
  }
  return stages;
}

function pipelineEndsWithWriteStage(pipeline: unknown[]): boolean {
  const stage = pipeline.at(-1);
  return (
    stage !== null &&
    typeof stage === "object" &&
    !Array.isArray(stage) &&
    (Object.hasOwn(stage, "$out") || Object.hasOwn(stage, "$merge"))
  );
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
    case "getCollectionNames":
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

function isMongoIntegerType(nativeType: string): boolean {
  return ["int", "int32", "long", "int64"].includes(nativeType);
}

function coerceMongoIntegerValue(
  value: string,
  column: ColumnTypeMeta,
  strict: boolean,
): unknown {
  if (!/^[+-]?\d+$/.test(value)) return value;
  const integer = BigInt(value);
  const int32 = column.nativeType === "int" || column.nativeType === "int32";
  const min = int32 ? -2147483648n : -9223372036854775808n;
  const max = int32 ? 2147483647n : 9223372036854775807n;
  if (integer < min || integer > max) {
    if (!strict) return value;
    throw new Error(
      `MongoDB ${column.nativeType} value is out of range (${min} to ${max}).`,
    );
  }
  return int32
    ? new Int32(Number(integer))
    : Long.fromString(integer.toString());
}

function coerceMongoNumericInput(
  value: string,
  column: ColumnTypeMeta,
): unknown {
  if (!MONGO_NUMERIC_LITERAL_RE.test(value)) {
    return value;
  }

  if (isMongoIntegerType(column.nativeType)) {
    if (!/^[+-]?\d+$/.test(value)) {
      return Decimal128.fromString(value);
    }
    try {
      return coerceMongoIntegerValue(value, column, true);
    } catch {
      return Decimal128.fromString(value);
    }
  }

  if (column.nativeType === "double" || column.nativeType === "number") {
    const numeric = Number(value);
    if (
      Number.isFinite(numeric) &&
      compareNumericTokens(value, String(numeric)) === 0
    ) {
      return column.nativeType === "double" ? new Double(numeric) : numeric;
    }
    return Decimal128.fromString(value);
  }

  return value;
}

function unwrapQuotedMongoDisplay(value: string): string {
  const trimmed = value.trim();
  if (isQuotedMongoString(trimmed)) {
    return trimmed.slice(1, -1);
  }
  return trimmed;
}

function isQuotedMongoString(value: string): boolean {
  const trimmed = value.trim();
  if (trimmed.length < 2) return false;
  const first = trimmed[0];
  const last = trimmed[trimmed.length - 1];
  return (first === '"' && last === '"') || (first === "'" && last === "'");
}

function parseExplicitMongoFilterString(value: string): string | undefined {
  const trimmed = value.trim();
  if (!isQuotedMongoString(trimmed)) return undefined;

  const quote = trimmed[0];
  if (quote === '"') {
    try {
      const parsed: unknown = JSON.parse(trimmed);
      if (typeof parsed === "string") return parsed;
    } catch {}
    throw new Error("Invalid quoted MongoDB filter string.");
  }
  return decodeJavaScriptStringLiteral(trimmed);
}

function decodeJavaScriptStringLiteral(trimmed: string): string {
  const quote = trimmed[0];
  let result = "";
  for (let index = 1; index < trimmed.length - 1; index++) {
    const char = trimmed[index];
    if (char === quote) {
      throw new Error("Invalid quoted MongoDB filter string.");
    }
    if (char !== "\\") {
      result += char;
      continue;
    }

    index++;
    if (index >= trimmed.length - 1) {
      throw new Error("Invalid quoted MongoDB filter string.");
    }
    const escaped = trimmed[index];
    if (escaped === "\n") continue;
    if (escaped === "\r") {
      if (trimmed[index + 1] === "\n") index++;
      continue;
    }
    if (escaped === "\u2028" || escaped === "\u2029") continue;
    const simpleEscapes: Record<string, string> = {
      "\\": "\\",
      "'": "'",
      '"': '"',
      "/": "/",
      b: "\b",
      f: "\f",
      n: "\n",
      r: "\r",
      t: "\t",
      v: "\v",
    };
    if (Object.hasOwn(simpleEscapes, escaped)) {
      result += simpleEscapes[escaped];
      continue;
    }
    if (escaped === "0") {
      if (/\d/.test(trimmed[index + 1] ?? "")) {
        throw new Error("Invalid quoted MongoDB filter string.");
      }
      result += "\0";
      continue;
    }
    if (escaped === "u" && trimmed[index + 1] === "{") {
      const end = trimmed.indexOf("}", index + 2);
      const hex = end < 0 ? "" : trimmed.slice(index + 2, end);
      const codePoint = Number.parseInt(hex, 16);
      if (
        end < 0 ||
        end >= trimmed.length - 1 ||
        !/^[0-9a-fA-F]+$/.test(hex) ||
        !Number.isSafeInteger(codePoint) ||
        codePoint > 0x10ffff
      ) {
        throw new Error("Invalid quoted MongoDB filter string.");
      }
      result += String.fromCodePoint(codePoint);
      index = end;
      continue;
    }
    const hexLength = escaped === "u" ? 4 : escaped === "x" ? 2 : 0;
    if (hexLength > 0) {
      const hex = trimmed.slice(index + 1, index + 1 + hexLength);
      if (!new RegExp(`^[0-9a-fA-F]{${hexLength}}$`).test(hex)) {
        throw new Error("Invalid quoted MongoDB filter string.");
      }
      result += String.fromCharCode(Number.parseInt(hex, 16));
      index += hexLength;
      continue;
    }
    throw new Error("Invalid quoted MongoDB filter string.");
  }
  return result;
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

function createMongoBinData(subtype: unknown, base64: unknown): Binary {
  if (
    typeof subtype !== "number" ||
    !Number.isInteger(subtype) ||
    subtype < 0 ||
    subtype > 255 ||
    typeof base64 !== "string" ||
    !BASE64_VALUE_RE.test(base64)
  ) {
    throw new Error(MONGO_BINDATA_ARGUMENTS_ERROR);
  }
  const bytes = Buffer.from(base64, "base64");
  if (bytes.toString("base64") !== base64) {
    throw new Error("BinData requires a canonical base64 string.");
  }
  return new Binary(bytes, subtype);
}

function parseMongoDisplayBinData(value: string): Binary | null {
  const trimmed = value.trim();
  if (!/^(?:new\s+)?BinData\s*\(/i.test(trimmed)) return null;
  const match =
    /^(?:new\s+)?BinData\s*\(\s*(\d+)\s*,\s*(["'])([^"']*)\2\s*\)$/i.exec(
      trimmed,
    );
  if (!match) throw new Error(MONGO_BINDATA_ARGUMENTS_ERROR);
  return createMongoBinData(Number.parseInt(match[1], 10), match[3]);
}

function coerceMongoBinaryValue(
  value: string,
  column: ColumnTypeMeta,
  strict: boolean,
): Binary | string {
  if (/^(?:0x|\\x)/i.test(value)) {
    if (!strict && !isHexLike(value)) return value;
    return new Binary(parseHexToBuffer(value), binarySubtypeFromColumn(column));
  }
  try {
    return parseMongoDisplayBinData(value) ?? value;
  } catch (error) {
    if (!strict) return value;
    throw error;
  }
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
  const match = DISPLAY_DATETIME_RE.exec(unwrapQuotedMongoDisplay(value));
  if (!match) return null;
  const [year, month, day, hour, minute, second] = match
    .slice(1, 7)
    .map((part) => Number(part ?? 0));
  const offset = /^([+-])(\d{2}):?(\d{2})?$/.exec(match[8] ?? "");
  const offsetHours = Number(offset?.[2] ?? 0);
  const offsetMinutes = Number(offset?.[3] ?? 0);
  if (
    hour > 23 ||
    minute > 59 ||
    second > 59 ||
    offsetHours > 23 ||
    offsetMinutes > 59
  ) {
    return null;
  }
  const parsed = new Date(0);
  parsed.setUTCFullYear(year, month - 1, day);
  if (
    parsed.getUTCFullYear() !== year ||
    parsed.getUTCMonth() !== month - 1 ||
    parsed.getUTCDate() !== day
  ) {
    return null;
  }
  parsed.setUTCHours(
    hour,
    minute,
    second,
    Number((match[7] ?? "").padEnd(3, "0").slice(0, 3)),
  );
  const offsetMs =
    (offset?.[1] === "-" ? -1 : 1) *
    (offsetHours * 60 + offsetMinutes) *
    60_000;
  return new Date(parsed.getTime() - offsetMs);
}

function formatMongoDisplayDate(value: Date): string {
  const formatted = formatDatetimeForDisplay(value) ?? value.toISOString();
  const year = value.getUTCFullYear();
  if (year >= 1000 && year <= 9999) return formatted;
  const yearText =
    year >= 0 && year <= 9999
      ? String(year).padStart(4, "0")
      : `${year < 0 ? "-" : "+"}${String(Math.abs(year)).padStart(6, "0")}`;
  return formatted.replace(/^-?\d+/, yearText);
}

function parseMongoTemporalInput(
  value: string,
  nativeType: string,
): Date | Timestamp | null {
  return nativeType === "timestamp"
    ? parseMongoTimestampInput(value)
    : parseMongoDisplayDate(value);
}

function formatMongoTimestampDisplay(
  seconds: number,
  increment: number,
): string {
  return `Timestamp(${seconds}, ${increment})`;
}

function parseMongoTimestampInput(value: string): Timestamp | null {
  const trimmed = unwrapQuotedMongoDisplay(value);
  const literalMatch = TIMESTAMP_LITERAL_RE.exec(trimmed);
  if (literalMatch) {
    const t = Number(literalMatch[1]);
    const i = Number(literalMatch[2]);
    if (
      ![t, i].every(
        (part) => Number.isInteger(part) && part >= 0 && part <= 0xffffffff,
      )
    )
      return null;
    return new Timestamp({
      t,
      i,
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

function parseMongoRegexInput(value: string): BSONRegExp | null {
  const trimmed = unwrapQuotedMongoDisplay(value);
  if (!trimmed.startsWith("/")) {
    return null;
  }

  const lastSlash = trimmed.lastIndexOf("/");
  if (lastSlash <= 0) {
    throw new Error("MongoDB regex literals must use /pattern/options syntax.");
  }

  try {
    return new BSONRegExp(
      trimmed.slice(1, lastSlash),
      trimmed.slice(lastSlash + 1),
    );
  } catch (error) {
    throw new Error(
      `Invalid MongoDB regex literal. Valid BSON flags are i, m, x, l, s and u. ${error instanceof Error ? error.message : "Invalid BSON regex value."}`,
      { cause: error },
    );
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
  return value instanceof BSON.BSONValue ? value._bsontype : undefined;
}

function bsonBinarySubtype(value: unknown): number | undefined {
  if (!value || typeof value !== "object") {
    return undefined;
  }
  const subtype = (value as { sub_type?: unknown }).sub_type;
  return typeof subtype === "number" ? subtype : undefined;
}

function setOwnMongoRecordField(
  record: Record<string, unknown>,
  field: string,
  value: unknown,
): void {
  Object.defineProperty(record, field, {
    configurable: true,
    enumerable: true,
    value,
    writable: true,
  });
}

const MONGO_EJSON_RESERVED_DOCUMENT_KEYS = new Set([
  "$oid",
  "$binary",
  "$uuid",
  "$symbol",
  "$numberInt",
  "$numberDecimal",
  "$numberDouble",
  "$numberLong",
  "$minKey",
  "$maxKey",
  "$regex",
  "$regularExpression",
  "$timestamp",
  "$date",
  "$code",
  "$undefined",
  "$dbPointer",
]);

function serializeMongoExportValue(
  value: unknown,
  ancestors = new Set<object>(),
): unknown {
  if (value === undefined) {
    return { $undefined: true };
  }
  if (
    value === null ||
    typeof value !== "object" ||
    value instanceof Date ||
    value instanceof RegExp
  ) {
    return BSON.EJSON.serialize(value, { relaxed: false });
  }
  if (ancestors.has(value)) {
    throw new TypeError(
      "MongoDB export cannot serialize circular BSON values.",
    );
  }
  ancestors.add(value);
  try {
    if (Array.isArray(value)) {
      return value.map((entry) => serializeMongoExportValue(entry, ancestors));
    }
    if (value instanceof Code) {
      return {
        $code: value.code,
        ...(value.scope
          ? { $scope: serializeMongoExportValue(value.scope, ancestors) }
          : {}),
      };
    }
    if (value instanceof DBRef) {
      const result: Record<string, unknown> = {
        $ref: value.collection,
        $id: serializeMongoExportValue(value.oid, ancestors),
        ...(value.db ? { $db: value.db } : {}),
      };
      for (const [name, entry] of Object.entries(value.fields)) {
        setOwnMongoRecordField(
          result,
          name,
          serializeMongoExportValue(entry, ancestors),
        );
      }
      return result;
    }
    if (value instanceof BSON.BSONValue) {
      return BSON.EJSON.serialize(value, { relaxed: false });
    }
    const reservedKey = Object.keys(value).find(
      (key) =>
        MONGO_EJSON_RESERVED_DOCUMENT_KEYS.has(key) ||
        (key === "$ref" && Object.hasOwn(value, "$id")),
    );
    if (reservedKey) {
      throw new Error(
        `MongoDB export cannot preserve the literal document key ${JSON.stringify(reservedKey)} in Extended JSON. Use a BSON backup to preserve this document's types.`,
      );
    }
    const result: Record<string, unknown> = {};
    for (const [name, entry] of Object.entries(value)) {
      setOwnMongoRecordField(
        result,
        name,
        serializeMongoExportValue(entry, ancestors),
      );
    }
    return result;
  } finally {
    ancestors.delete(value);
  }
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
      : formatMongoDisplayDate(value);
  }

  if (value instanceof RegExp) {
    return value.toString();
  }

  if (value instanceof ObjectId) {
    return value.toHexString();
  }

  const tag = bsonTypeTag(value);

  if (value instanceof Binary && value.sub_type === 4) {
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
        ? formatMongoTimestampDisplay(seconds >>> 0, increment >>> 0)
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
  if (value instanceof Binary && value.sub_type === 4) {
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
      return { category: "other", nativeType: "dbRef" };
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

function isJsonEditableMongoValue(value: unknown): boolean {
  if (
    value === null ||
    typeof value === "string" ||
    typeof value === "boolean" ||
    typeof value === "number" ||
    value instanceof Int32
  ) {
    return true;
  }
  if (value instanceof Double) {
    return Number.isFinite(value.value) && !Number.isInteger(value.value);
  }
  if (Array.isArray(value)) {
    return value.every(isJsonEditableMongoValue);
  }
  return (
    typeof value === "object" &&
    Object.getPrototypeOf(value) === Object.prototype &&
    Object.values(value).every(isJsonEditableMongoValue)
  );
}

function inferMongoRowValueType(value: unknown): MongoSchemaType {
  const schema = inferMongoSchemaType(value);
  if (
    (schema.category === "json" || schema.category === "array") &&
    !isJsonEditableMongoValue(value)
  ) {
    return {
      ...schema,
      nativeType: schema.category === "array" ? "bsonArray" : "bsonDocument",
    };
  }
  return schema;
}

function mongoQueryColumnMeta(values: readonly unknown[]): QueryColumnMeta {
  const categories = values
    .filter((value) => value !== null && value !== undefined)
    .map((value) => inferMongoSchemaType(value).category);
  return mongoQueryColumnMetaFromCategories(categories);
}

function mongoQueryColumnMetaFromCategories(
  categories: readonly (TypeCategory | null)[],
): QueryColumnMeta {
  const numericCategories = new Set<TypeCategory>([
    "integer",
    "decimal",
    "float",
  ]);

  return {
    category:
      categories.length > 0 &&
      categories.every(
        (category) => category !== null && numericCategories.has(category),
      )
        ? categories.every((category) => category === categories[0])
          ? (categories[0] ?? null)
          : "decimal"
        : null,
  };
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

function isUnsupportedMongoTableField(name: string): boolean {
  return name.length === 0 || name.includes(".") || name.startsWith("$");
}

function unsupportedMongoTableFieldMessage(name: string): string {
  return `MongoDB Table Viewer cannot safely edit literal field ${JSON.stringify(name)}: empty names, names containing a dot or starting with "$" are read-only. Use the Query Editor with explicit literal-field operations instead.`;
}

function assertSupportedMongoTableUpdate(update: {
  changes?: Record<string, unknown>;
  originalValues?: Record<string, unknown>;
  originalTypes?: Record<string, MongoValueTypeHint>;
}): void {
  for (const record of [
    update.changes,
    update.originalValues,
    update.originalTypes,
  ]) {
    for (const name of Object.keys(record ?? {})) {
      if (isUnsupportedMongoTableField(name)) {
        throw new Error(unsupportedMongoTableFieldMessage(name));
      }
    }
  }
  for (const [name, hint] of Object.entries(update.originalTypes ?? {})) {
    if (
      Object.hasOwn(update.changes ?? {}, name) &&
      MONGO_BSON_EMBEDDED_NATIVE_TYPES.has(hint.nativeType)
    ) {
      throw new Error(
        `MongoDB Table Viewer cannot safely edit field ${JSON.stringify(name)}: its embedded value contains BSON types (such as ObjectId, Date, Long, Decimal128, Binary or whole-number Double) that the JSON editor cannot preserve. Use the Query Editor with an explicit update instead.`,
      );
    }
    if (
      Object.hasOwn(update.changes ?? {}, name) &&
      hint.nativeType === "dbPointer"
    ) {
      throw new Error(dbPointerMongoTableFieldMessage(name));
    }
  }
}

function dbPointerMongoTableFieldMessage(name: string): string {
  return `MongoDB Table Viewer cannot safely edit field ${JSON.stringify(name)}: it contains deprecated DBPointer values, which the MongoDB driver reads as DBRef and cannot write back as DBPointer. Use the Query Editor with an explicit update instead.`;
}

function isMongoInterruption(error: unknown, signal?: AbortSignal): boolean {
  return (
    signal?.aborted === true ||
    error instanceof MongoOperationTimeoutError ||
    (error instanceof MongoServerError && error.code === 50) ||
    (error instanceof Error &&
      (error.name === "AbortError" || error.name === "DriverTimeoutError"))
  );
}

function mongoIdKey(id: unknown): string {
  return Buffer.from(BSON.serialize({ id })).toString("hex");
}

function buildMongoTableUpdateCriteria(update: {
  primaryKeys?: Record<string, unknown>;
  originalValues?: Record<string, unknown>;
  originalTypes?: Record<string, MongoValueTypeHint>;
}): Record<string, unknown> {
  const criteria: Record<string, unknown> = {};
  const exactChecks = new Map<string, unknown>();
  for (const [field, value] of Object.entries(update.originalValues ?? {})) {
    setOwnMongoRecordField(
      criteria,
      field,
      value !== null && typeof value === "object" ? { $eq: value } : value,
    );
    exactChecks.set(field, { $eq: [`$${field}`, { $literal: value }] });
  }
  for (const [field, hint] of Object.entries(update.originalTypes ?? {})) {
    if (hint.nativeType === "null") {
      setOwnMongoRecordField(criteria, field, { $eq: null, $type: 10 });
    } else if (hint.nativeType === "undefined") {
      setOwnMongoRecordField(criteria, field, { $type: 6 });
    } else if (hint.nativeType === "missing") {
      setOwnMongoRecordField(criteria, field, { $exists: false });
    }
    if (["null", "undefined", "missing"].includes(hint.nativeType)) {
      exactChecks.set(field, {
        $eq: [{ $type: `$${field}` }, { $literal: hint.nativeType }],
      });
    }
  }
  criteria._id = { $eq: update.primaryKeys?._id };
  if (exactChecks.size > 0) {
    criteria.$expr = { $and: [...exactChecks.values()] };
  }
  return criteria;
}

export class MongoDBDriver implements IDBDriver {
  private readonly tableRowRawDocuments = new WeakMap<
    Record<string, unknown>,
    Record<string, unknown>
  >();
  private readonly tableRowSchemas = new WeakMap<
    Record<string, unknown>,
    Map<string, ReturnType<typeof inferMongoSchemaType>>
  >();
  private readonly rowValueCategory = (
    row: Record<string, unknown>,
    column: string,
  ): TypeCategory =>
    this.tableRowSchemas.get(row)?.get(column)?.category ??
    inferMongoSchemaType(row[column]).category;
  private readonly tableRowIdTypes = new WeakMap<
    Record<string, unknown>,
    "objectId" | "string" | null
  >();
  private client: MongoClient | null = null;
  private connected = false;
  private timeoutRecoveryInFlight: Promise<void> | null = null;
  private connectionEpoch = 0;
  private connectionAbortController?: AbortController;

  private readonly config: ConnectionConfig;

  constructor(
    config: ConnectionConfig,
    private readonly timeoutSettingsProvider: DriverTimeoutSettingsProvider = getDefaultDriverTimeoutSettings,
  ) {
    const normalized = normalizeMongoConnectionConfig(config);
    this.config = getSshTcpForwardTransport(config)
      ? { ...normalized, directConnection: config.directConnection }
      : normalized;
  }

  private connectionTimeoutOptions(uri: string): {
    serverSelectionTimeoutMS?: number;
    connectTimeoutMS?: number;
  } {
    const timeoutMs = this.timeoutSettingsProvider().connectionTimeoutMs;
    if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) return {};
    const budget = Math.max(
      1,
      Math.floor(timeoutMs - Math.min(250, timeoutMs / 10)),
    );
    const queryIndex = uri.indexOf("?");
    const options = new URLSearchParams(
      queryIndex < 0 ? "" : uri.slice(queryIndex + 1),
    );
    const bounded = (name: string) => {
      const entry = [...options].find(
        ([key]) => key.toLowerCase() === name.toLowerCase(),
      );
      if (!entry) return budget;
      const value = Number(entry[1]);
      if (!Number.isFinite(value) || !Number.isInteger(value) || value < 0) {
        throw new Error(`MongoDB ${name} must be a non-negative integer.`);
      }
      return value === 0 ? budget : Math.min(value, budget);
    };
    return {
      serverSelectionTimeoutMS: bounded("serverSelectionTimeoutMS"),
      connectTimeoutMS: bounded("connectTimeoutMS"),
    };
  }

  async connect(): Promise<void> {
    if (this.connected) {
      return;
    }
    const epoch = ++this.connectionEpoch;
    this.connectionAbortController?.abort();
    const abortController = new AbortController();
    this.connectionAbortController = abortController;
    let client: MongoClient | undefined;
    try {
      const configuredUri =
        resolveMongoConnectionMode(this.config) === "uri"
          ? (getMongoConnectionUri(this.config) ?? "")
          : this.buildUri();
      const uri =
        this.config.tls !== undefined
          ? stripMongoUriTlsOptions(
              configuredUri,
              this.config.tls.mode !== "disabled",
            )
          : configuredUri;
      const tlsSettings = await resolveConnectionTlsSettings(
        this.config,
        abortController.signal,
      );
      this.assertConnectionEpoch(epoch);
      const tlsHostname = (
        tlsSettings?.servername ?? getTlsServername(this.config)
      )?.replace(/^\[|\]$/g, "");
      const tlsIpIdentity = tlsHostname && isIP(tlsHostname) !== 0;
      this.assertConnectionEpoch(epoch);
      client = new MongoClient(uri, {
        ...this.connectionTimeoutOptions(uri),
        ...(this.config.tls !== undefined
          ? { tls: tlsSettings !== undefined }
          : {}),
        ...(tlsSettings
          ? {
              tlsAllowInvalidCertificates: !tlsSettings.rejectUnauthorized,
              tlsAllowInvalidHostnames:
                tlsSettings.skipHostnameVerification === true,
            }
          : {}),
        ca: tlsSettings?.ca,
        cert: tlsSettings?.cert,
        key: tlsSettings?.key,
        passphrase: tlsSettings?.passphrase,
        servername: tlsIpIdentity ? undefined : tlsHostname,
        checkServerIdentity:
          tlsIpIdentity && !tlsSettings?.skipHostnameVerification
            ? (_hostname, certificate) =>
                checkServerIdentity(tlsHostname, certificate)
            : tlsSettings?.checkServerIdentity,
        authMechanism: this.config.authMechanism,
        authSource: this.config.authSource,
        replicaSet: this.config.replicaSet,
        directConnection: this.config.directConnection,
      });
      this.client = client;
      this.assertConnectionEpoch(epoch);
      await client.connect();
      this.assertConnectionEpoch(epoch);
      const catalog = client
        .db(this.defaultDatabaseName())
        .listCollections(
          {},
          { nameOnly: true, batchSize: 1, signal: abortController.signal },
        );
      try {
        await catalog.hasNext();
      } finally {
        await catalog.close();
      }
      this.assertConnectionEpoch(epoch);
      this.connected = true;
    } catch (error) {
      if (this.client === client) {
        this.client = null;
        this.connected = false;
        await client?.close().catch(() => undefined);
      }
      if (error && typeof error === "object" && "code" in error) {
        if (error.code === 18)
          throw new Error(
            "MongoDB authentication failed. Check username, password and Authentication database (authSource).",
            { cause: error },
          );
        if (error.code === 13)
          throw new Error(
            "MongoDB server reached, but access to the selected database was denied. Check authentication and database permissions.",
            { cause: error },
          );
      }
      throw error;
    } finally {
      if (this.connectionAbortController === abortController) {
        this.connectionAbortController = undefined;
      }
    }
  }

  private assertConnectionEpoch(epoch: number): void {
    if (epoch !== this.connectionEpoch) {
      throw new DOMException(
        "MongoDB connection attempt cancelled",
        "AbortError",
      );
    }
  }

  async disconnect(): Promise<void> {
    this.connectionEpoch += 1;
    this.connectionAbortController?.abort();
    this.connectionAbortController = undefined;
    const client = this.client;
    this.client = null;
    this.connected = false;
    await client?.close();
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
      rejectsUnknownMutationColumn: (name: string) =>
        !isUnsupportedMongoTableField(name),
      unsupportedMutationColumnMessage: (name: string) =>
        isUnsupportedMongoTableField(name)
          ? unsupportedMongoTableFieldMessage(name)
          : undefined,
      readOnlyQueryGuard: decideMongoReadOnlyQuery,
      schemaMutationGuard: (_queryText: string) => true,
      editorPresentation: {
        formatOnOpen: false,
        editorLanguage: "javascript" as const,
      },
    };
  }

  async listDatabases(): Promise<DatabaseInfo[]> {
    try {
      const admin = this.requireClient().db().admin();
      const dbs = await admin.listDatabases(getMetadataSdkOptions());
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
      .listCollections({}, { nameOnly: false, ...getMetadataSdkOptions() })
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
    const columns = await this.describeSchemaColumns(
      database,
      table,
      MONGODB_SCHEMA_SAMPLE_LIMIT,
    );
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
    const columns = await this.describeSchemaColumns(
      database,
      table,
      MONGODB_SCHEMA_SAMPLE_LIMIT,
    );
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
        .indexes(getMetadataSdkOptions());
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
      await this.requireDb(database)
        .collection(table)
        .indexes(getMetadataSdkOptions())
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
    throwIfQueryCancelled(options);
    const trimmed = normalizeMongoshQueryText(sql);

    if (trimmed.length === 0) {
      return { columns: [], rows: [], rowCount: 0, executionTimeMs: 0 };
    }

    const startedAt = Date.now();
    const operations = parseMongoshOperations(trimmed);
    for (const operation of operations) validateMongoshOperation(operation);
    if (
      (options?.readOnly === true || this.config.readOnly === true) &&
      !operations.every(isReadOnlyMongoOperation)
    ) {
      throw new Error(MONGODB_READ_ONLY_QUERY_REASON);
    }

    const executeOperation = async (
      operation: MongoshOperation,
      rowBudget = MONGODB_QUERY_HARD_CAP,
    ): Promise<QueryResult> => {
      throwIfQueryCancelled(options);
      const timeoutMS = getQueryRemainingTimeoutMs(options);
      const sdkOptions = {
        ...(options?.signal ? { signal: options.signal } : {}),
        ...(timeoutMS !== undefined ? { timeoutMS } : {}),
      };
      const commandSdkOptions = timeoutMS !== undefined ? { timeoutMS } : {};
      const sdkArgs =
        Object.keys(sdkOptions).length > 0
          ? ([sdkOptions] as const)
          : ([] as const);
      const { dbName, collName, op, args: opArgs, chainOps } = operation;

      const limitOp = chainOps.filter((chain) => chain.op === "limit").at(-1);
      const explicitLimit = limitOp?.args[0] as number | undefined;
      const resultLimit = Math.min(
        rowBudget,
        op === "findOne"
          ? 1
          : explicitLimit === 0
            ? MONGODB_QUERY_HARD_CAP
            : (explicitLimit ?? 100),
      );
      const fetchLimit = Math.min(
        resultLimit + 1,
        op === "findOne"
          ? 1
          : explicitLimit === undefined || explicitLimit === 0
            ? Infinity
            : explicitLimit,
      );
      const skipOp = chainOps.filter((c) => c.op === "skip").at(-1);
      const skip =
        typeof skipOp?.args[0] === "number" && Number.isFinite(skipOp.args[0])
          ? Math.max(0, Math.floor(skipOp.args[0]))
          : 0;

      const dbHandlers: Record<string, () => Promise<QueryResult>> = {
        getCollectionNames: async () => {
          const collections = await this.requireDb(dbName)
            .listCollections(
              {},
              { authorizedCollections: true, nameOnly: true, ...sdkOptions },
            )
            .toArray();
          return this.buildMongoDocumentRowsQueryResult(
            collections.map((collection) => ({ name: collection.name })),
            startedAt,
          );
        },
        runCommand: async () => {
          const cmd = this.normalizeFilterCriteria(
            opArgs[0] as Record<string, unknown>,
          );
          const result = await this.requireDb(dbName).command(cmd, ...sdkArgs);
          return this.buildMongoSingleRowQueryResult(
            this.toRow(result as Record<string, unknown>),
            startedAt,
          );
        },
        createCollection: async () => {
          const name = String(opArgs[0]);
          const collectionOptions =
            opArgs[1] !== null && typeof opArgs[1] === "object"
              ? (opArgs[1] as Record<string, unknown>)
              : undefined;
          const createOptions = {
            ...collectionOptions,
            ...commandSdkOptions,
          };
          if (Object.keys(createOptions).length > 0) {
            await this.requireDb(dbName).createCollection(name, createOptions);
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
          const pipeline = opArgs[2] as Record<string, unknown>[];
          const options =
            opArgs[3] !== null && typeof opArgs[3] === "object"
              ? (opArgs[3] as Record<string, unknown>)
              : undefined;
          await this.requireDb(dbName).createCollection(name, {
            ...(options ?? {}),
            viewOn,
            pipeline,
            ...commandSdkOptions,
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
      const requestedHelperOptions = MONGO_READ_HELPER_OPTION_KEYS.has(op)
        ? ((opArgs[mongoReadHelperOptionIndex(op)] as
            | Record<string, unknown>
            | undefined) ?? {})
        : {};
      const { maxTimeMS: helperMaxTimeMS, ...helperOptionsWithoutMaxTime } =
        requestedHelperOptions;
      const helperOptions =
        timeoutMS !== undefined && typeof helperMaxTimeMS === "number"
          ? {
              ...helperOptionsWithoutMaxTime,
              timeoutMS:
                helperMaxTimeMS > 0
                  ? Math.max(1, Math.min(timeoutMS, helperMaxTimeMS))
                  : timeoutMS,
            }
          : requestedHelperOptions;
      const findDocuments = async (limitValue: number) => {
        const projectionArg = opArgs[1];
        const projection = projectionArg
          ? this.normalizeCriteria(projectionArg as Record<string, unknown>)
          : undefined;
        const findOptions: FindOptions = {
          promoteValues: false,
          bsonRegExp: true,
          ...(projection !== undefined ? { projection } : {}),
          ...sdkOptions,
          ...helperOptions,
        };
        let cursor = mongoCollection.find(normalizeCriteria(), findOptions);
        for (const chain of chainOps) {
          if (chain.op === "sort") {
            const sort = chain.args[0];
            cursor = cursor.sort(sort as NonNullable<FindOptions["sort"]>);
          }
        }
        return cursor.skip(skip).limit(limitValue).toArray();
      };
      const runUpdate = async (single: boolean) => {
        const update = opArgs[1] as Record<string, unknown>;
        return single
          ? mongoCollection.updateOne(normalizeCriteria(), update, ...sdkArgs)
          : mongoCollection.updateMany(normalizeCriteria(), update, ...sdkArgs);
      };
      const runDelete = async (single: boolean) =>
        single
          ? mongoCollection.deleteOne(normalizeCriteria(), ...sdkArgs)
          : mongoCollection.deleteMany(normalizeCriteria(), ...sdkArgs);

      const collectionHandlers: Record<string, () => Promise<QueryResult>> = {
        drop: async () => {
          const result = await this.requireDb(dbName).command(
            { drop: collName },
            ...sdkArgs,
          );
          return this.buildMongoSingleRowQueryResult(
            this.toRow(result),
            startedAt,
          );
        },
        find: async () =>
          this.buildMongoDocumentRowsQueryResult(
            await findDocuments(fetchLimit),
            startedAt,
            resultLimit,
          ),
        findOne: async () =>
          this.buildMongoDocumentRowsQueryResult(
            await findDocuments(1),
            startedAt,
            resultLimit,
          ),
        countDocuments: async () => {
          const countOptions = { ...sdkOptions, ...helperOptions };
          const count =
            Object.keys(countOptions).length > 0
              ? await mongoCollection.countDocuments(
                  normalizeCriteria(),
                  countOptions,
                )
              : await mongoCollection.countDocuments(normalizeCriteria());
          return this.buildMongoSingleRowQueryResult({ count }, startedAt);
        },
        insertOne: async () => {
          const doc = opArgs[0] as Record<string, unknown>;
          const result = await mongoCollection.insertOne(doc, ...sdkArgs);
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
          const result = await mongoCollection.insertMany(docs, ...sdkArgs);
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
          const pipeline = opArgs[0] as Record<string, unknown>[];
          const boundedPipeline = [...pipeline];
          if (!pipelineEndsWithWriteStage(pipeline)) {
            boundedPipeline.push(...buildAggregateChainStages(chainOps), {
              $limit: fetchLimit,
            });
          }
          const docs = await mongoCollection
            .aggregate(boundedPipeline, {
              promoteValues: false,
              bsonRegExp: true,
              ...sdkOptions,
              ...helperOptions,
            })
            .toArray();
          return this.buildMongoDocumentRowsQueryResult(
            docs,
            startedAt,
            resultLimit,
          );
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
            (sdkArgs.length > 0
              ? { ...options, ...sdkOptions }
              : options) as Parameters<typeof mongoCollection.createIndex>[1],
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
        `Unsupported mongosh operation: "${op}".\n\nSupported: find, findOne, countDocuments, insertOne, insertMany, updateOne, updateMany, deleteOne, deleteMany, aggregate, runCommand, adminCommand, createCollection, createView, createIndex, drop`,
      );
    };

    if (operations.length === 1) {
      const result = await executeOperation(operations[0]);
      throwIfQueryCancelled(options);
      return result;
    }

    const rawRows: Record<string, unknown>[] = [];
    const categoriesByColumn = new Map<string, (TypeCategory | null)[]>();
    let affectedRows = 0;
    let sawAffectedRows = false;
    let totalRowCount = 0;
    let truncated = false;
    let partialError: string | undefined;
    let completedWrite = false;
    const total = operations.length;

    for (const [index, operation] of operations.entries()) {
      const availableSlots = MONGODB_QUERY_HARD_CAP - rawRows.length;
      const statement = `statement ${index + 1} of ${total}`;
      let result: QueryResult;
      try {
        result = await executeOperation(operation, availableSlots);
      } catch (error: unknown) {
        if (!completedWrite) throw error;
        const failure = options?.signal?.aborted
          ? "was cancelled"
          : `failed: ${error instanceof Error ? error.message : String(error)}`;
        partialError = `MongoDB ${statement} ${failure}. Earlier statements completed${isReadOnlyMongoOperation(operation) ? "" : `; the outcome of statement ${index + 1} is unknown`}; later statements were not run.`;
        break;
      }
      completedWrite ||= !isReadOnlyMongoOperation(operation);
      try {
        throwIfQueryCancelled(options);
      } catch (error: unknown) {
        if (!completedWrite) throw error;
        partialError = `MongoDB script was cancelled after ${statement}. Statements 1 to ${index + 1} completed${index + 1 < total ? "; later statements were not run" : ""}.`;
      }
      const mappedRows = this.mapQueryResultRowsToObjects(result);
      totalRowCount += mappedRows.length;
      truncated ||=
        result.truncated === true || mappedRows.length > availableSlots;
      if (availableSlots > 0) {
        const retainedRows = mappedRows.slice(0, availableSlots);
        rawRows.push(...retainedRows);
        result.columns.forEach((column, index) => {
          if (
            !retainedRows.some(
              (row) => Object.hasOwn(row, column) && row[column] != null,
            )
          )
            return;
          const categories = categoriesByColumn.get(column) ?? [];
          categories.push(result.columnMeta?.[index]?.category ?? null);
          categoriesByColumn.set(column, categories);
        });
      }
      if (typeof result.affectedRows === "number") {
        affectedRows += result.affectedRows;
        sawAffectedRows = true;
      }
      if (partialError !== undefined) break;
    }

    const columns = inferColumnsFromRows(rawRows, "_id").map((c) => c.name);
    const missingCells = this.queryMissingCells(rawRows, columns);
    return {
      columns,
      columnMeta: columns.map((column) =>
        mongoQueryColumnMetaFromCategories(
          categoriesByColumn.get(column) ?? [],
        ),
      ),
      rows: rawRows.map((row) => this.mapRowToQueryRow(row, columns)),
      ...(missingCells.length ? { missingCells } : {}),
      rowCount: totalRowCount,
      affectedRows: sawAffectedRows ? affectedRows : undefined,
      executionTimeMs: Date.now() - startedAt,
      ...(truncated ? { truncated: true } : {}),
      ...(partialError !== undefined ? { error: partialError } : {}),
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
      () => this.tablePageSdkOptions(request),
      MONGODB_SCHEMA_SAMPLE_LIMIT,
    );
    const normalizedFilters = this.normalizeInlineFilters(
      request.filters,
      schemaColumns,
    );
    const requiresClientSideFiltering = this.shouldUseClientSideFiltering(
      normalizedFilters,
      schemaColumns,
    );
    const requiresClientSideSort =
      request.sort !== null &&
      isUnsupportedMongoTableField(request.sort.column);
    const offset = Math.max(0, (request.page - 1) * request.pageSize);
    let criteria: Record<string, unknown> | undefined;
    if (!requiresClientSideFiltering) {
      try {
        criteria = this.buildMongoFilterCriteria(
          normalizedFilters,
          schemaColumns,
        );
      } catch {}
    }

    if (criteria && !requiresClientSideSort) {
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
          bsonRegExp: true,
          ...this.tablePageSdkOptions(request),
        })
        .sort(sort)
        .skip(offset)
        .limit(request.pageSize)
        .toArray();
      const rows = docs.map((doc) =>
        this.toRow(doc as Record<string, unknown>),
      );
      await this.markDbPointerCells(request, rows);
      const countOptions = this.tablePageSdkOptions(request);
      const collection = this.requireDb(request.database).collection(
        request.table,
      );
      const totalCount = request.skipCount
        ? 0
        : Object.keys(countOptions).length > 0
          ? await collection.countDocuments(criteria, countOptions)
          : await collection.countDocuments(criteria);

      return {
        columns:
          schemaColumns.length > 0
            ? this.withDocumentColumns(
                schemaColumns,
                docs as Record<string, unknown>[],
              )
            : inferColumnsFromRows(rows, "_id", {
                nullableMode: "schemaLess",
              }).map((column) => ({
                ...column,
                ...(isUnsupportedMongoTableField(column.name)
                  ? { isReadOnly: true }
                  : {}),
              })),
        rows,
        mongoIdTypes: rows.map((row) => this.tableRowIdTypes.get(row) ?? null),
        mongoValueTypes: rows.map((row) => this.getTableRowValueTypes(row)),
        totalCount,
        executionTimeMs: Math.round(performance.now() - startTime),
      };
    }

    const rows = await this.readRows(
      request.database,
      request.table,
      MONGODB_QUERY_HARD_CAP + 1,
      this.tablePageSdkOptions(request),
      criteria,
    );
    if (rows.length > MONGODB_QUERY_HARD_CAP) {
      throw new Error(
        `MongoDB evaluates this filter or sort in the extension, which is limited to collections of ${MONGODB_QUERY_HARD_CAP} documents. Use filters and sorts MongoDB can evaluate on the server, or run a query instead.`,
      );
    }
    const comparisonColumns =
      schemaColumns.length > 0
        ? this.withDocumentColumns(
            schemaColumns,
            rows.flatMap((row) => {
              const document = this.tableRowRawDocuments.get(row);
              return document ? [document] : [];
            }),
          )
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
                ...(isUnsupportedMongoTableField(column.name)
                  ? { isReadOnly: true }
                  : {}),
                filterOperators: resolveFilterOperators(category, {
                  filterable: column.filterable,
                  nullable: column.nullable,
                }),
              };
            },
          );
    const filtered = criteria
      ? rows
      : applyFilters(
          rows,
          normalizedFilters,
          comparisonColumns,
          this.rowValueCategory,
          (value, name) => {
            const column = schemaColumns.find(
              (candidate) => candidate.name === name,
            );
            return splitInFilterValues(value).map((entry) =>
              column ? this.normalizeInlineFilterScalar(column, entry) : entry,
            );
          },
          this.buildClientScalarEquality(normalizedFilters, schemaColumns),
        );
    const sorted = applySort(
      filtered,
      request.sort,
      comparisonColumns,
      this.rowValueCategory,
    );
    const paged = pageRows(sorted, request.page, request.pageSize);
    await this.markDbPointerCells(request, paged);
    return {
      columns: comparisonColumns,
      rows: paged,
      mongoIdTypes: paged.map((row) => this.tableRowIdTypes.get(row) ?? null),
      mongoValueTypes: paged.map((row) => this.getTableRowValueTypes(row)),
      totalCount: request.skipCount ? 0 : sorted.length,
      executionTimeMs: Math.round(performance.now() - startTime),
    };
  }

  formatTableExportChunk(
    chunk: DriverTableExportChunk,
    selectedColumns?: readonly string[],
  ): DriverTableExportChunk {
    const documents = chunk.rows.map((row) => {
      const document = this.tableRowRawDocuments.get(row);
      if (!document) {
        throw new Error(
          "MongoDB export could not access the original BSON values. Refresh the table and retry.",
        );
      }

      return selectedColumns
        ? Object.fromEntries(
            selectedColumns
              .filter((name) => Object.hasOwn(document, name))
              .map((name) => [name, document[name]]),
          )
        : document;
    });
    return {
      ...chunk,
      columns: this.withDocumentColumns(
        chunk.columns,
        documents,
        selectedColumns,
      ).map((column) => ({
        ...column,
        exportCategory: "other",
      })),
      rows: documents.map(
        (document) =>
          serializeMongoExportValue(document) as Record<string, unknown>,
      ),
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
        case "is_not_null": {
          const nullTypeCheck = {
            $in: [
              { $type: `$${filter.column}` },
              [...MONGO_NULL_LIKE_BSON_TYPES],
            ],
          };
          andClauses.push({
            $expr:
              filter.operator === "is_null"
                ? nullTypeCheck
                : { $not: [nullTypeCheck] },
          });
          break;
        }
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
          const candidates = this.getFilterInputCandidates(
            filter.value,
            column,
          );
          if (candidates.length === 1) {
            const [candidate] = candidates;
            const prototype =
              candidate !== null && typeof candidate === "object"
                ? Object.getPrototypeOf(candidate)
                : undefined;
            andClauses.push({
              [filter.column]:
                prototype === Object.prototype || prototype === null
                  ? { $eq: candidate }
                  : candidate,
            });
          } else {
            andClauses.push({
              $or: candidates.map((candidate) => ({
                [filter.column]: { $eq: candidate },
              })),
            });
          }
          break;
        }
        case "neq": {
          const candidates = this.getFilterInputCandidates(
            filter.value,
            column,
          );
          andClauses.push({
            $and: candidates.map((candidate) => ({
              [filter.column]: { $ne: candidate },
            })),
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
          const entries = splitInFilterValues(filter.value).flatMap((entry) =>
            this.getFilterInputCandidates(entry, column),
          );
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
    const normalized = unwrapQuotedMongoDisplay(value);
    let coerced: unknown;
    try {
      coerced = this.coerceInputValue(value, column);
    } catch (error) {
      if (column.category === "binary") return normalized;
      if (
        isMongoIntegerType(column.nativeType) &&
        /^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:e[+-]?\d+)?$/i.test(normalized)
      )
        return Decimal128.fromString(normalized);
      throw error;
    }
    if (
      (column.nativeType === "double" || column.nativeType === "number") &&
      /^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:e[+-]?\d+)?$/i.test(normalized) &&
      (typeof coerced === "string" ||
        (typeof coerced === "number" &&
          (!Number.isFinite(coerced) ||
            compareNumericTokens(normalized, String(coerced)) !== 0)))
    )
      return Decimal128.fromString(normalized);
    if (
      typeof coerced === "string" &&
      isMongoIntegerType(column.nativeType) &&
      !/^[+-]?\d+$/.test(normalized) &&
      /^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:e[+-]?\d+)?$/i.test(normalized)
    ) {
      return Decimal128.fromString(normalized);
    }
    return coerced;
  }

  private getFilterInputCandidates(
    value: string,
    column: ColumnTypeMeta | undefined,
  ): unknown[] {
    const normalized = unwrapQuotedMongoDisplay(value);
    const explicitString = parseExplicitMongoFilterString(value);
    if (explicitString !== undefined) {
      return [explicitString];
    }
    if (
      !MONGO_NUMERIC_LITERAL_RE.test(normalized) ||
      (column &&
        column.nativeType !== "string" &&
        !["integer", "decimal", "float"].includes(column.category))
    ) {
      return [this.coerceFilterInput(value, column)];
    }

    if (column && ["integer", "decimal", "float"].includes(column.category)) {
      if (
        column.nativeType === "decimal" ||
        column.nativeType === "decimal128"
      ) {
        return [Decimal128.fromString(normalized)];
      }
      return [this.coerceFilterInput(value, column)];
    }

    return [Decimal128.fromString(normalized)];
  }

  private buildClientScalarEquality(
    filters: readonly FilterExpression[],
    columns: readonly ColumnTypeMeta[],
  ) {
    const predicates = new Map<
      FilterExpression,
      (row: Record<string, unknown>) => boolean | undefined
    >();
    const numeric = (category: TypeCategory) =>
      category === "integer" || category === "decimal" || category === "float";
    for (const filter of filters) {
      if (
        (filter.operator === "like" || filter.operator === "ilike") &&
        !this.shouldUseClientSideFiltering([filter], columns)
      ) {
        predicates.set(filter, (row) =>
          this.matchesServerLikeFilter(row, filter),
        );
        continue;
      }
      if (
        !["eq", "neq", "in"].includes(filter.operator) ||
        !("value" in filter) ||
        typeof filter.value !== "string"
      )
        continue;
      const column = columns.find((column) => column.name === filter.column);
      if (!column || ["json", "array", "other"].includes(column.category))
        continue;
      const candidates = (
        filter.operator === "in"
          ? splitInFilterValues(filter.value)
          : [filter.value]
      ).flatMap((input) =>
        this.getFilterInputCandidates(input, column).map((value) => ({
          value,
          schema: inferMongoSchemaType(value),
          display: formatMongoDisplayValue(value),
        })),
      );
      predicates.set(filter, (row) => {
        const schema = this.tableRowSchemas.get(row);
        if (!schema) return undefined;
        const actual = schema.get(filter.column);
        const value = row[filter.column];
        const match = candidates.some((candidate) => {
          if (candidate.value == null) return value == null;
          if (!actual || value == null) return false;
          if (numeric(actual.category) && numeric(candidate.schema.category))
            return (
              compareNumericTokens(String(value), String(candidate.display)) ===
              0
            );
          if (
            actual.nativeType !== candidate.schema.nativeType ||
            actual.bsonSubtype !== candidate.schema.bsonSubtype
          )
            return false;
          return String(value) === String(candidate.display);
        });
        return filter.operator === "neq" ? !match : match;
      });
    }
    return (row: Record<string, unknown>, filter: FilterExpression) =>
      predicates.get(filter)?.(row);
  }

  private matchesServerLikeFilter(
    row: Record<string, unknown>,
    filter: FilterExpression,
  ): boolean | undefined {
    const document = this.tableRowRawDocuments.get(row);
    if (!document || !("value" in filter) || typeof filter.value !== "string")
      return undefined;
    const needle = filter.value.trim();
    const matches = (value: unknown): boolean => {
      const text =
        typeof value === "string"
          ? value
          : value instanceof BSONSymbol
            ? value.valueOf()
            : undefined;
      if (text === undefined) return false;
      return filter.operator === "ilike"
        ? text.toLowerCase().includes(needle.toLowerCase())
        : text.includes(needle);
    };
    const value = document[filter.column];
    return Array.isArray(value) ? value.some(matches) : matches(value);
  }

  private buildContainsRegex(value: string): string {
    const escaped = value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    const normalized = escaped.trim();
    return normalized.length > 0 ? normalized : ".*";
  }

  validateTableUpdates(
    updates: readonly Pick<
      DriverUpdateRowsRequest["updates"][number],
      "changes" | "originalValues" | "originalTypes"
    >[],
  ): void {
    for (const update of updates) {
      assertSupportedMongoTableUpdate(update);
    }
  }

  async updateRows(
    request: DriverUpdateRowsRequest,
    context?: DriverOperationContext,
  ): Promise<DriverMutationResult> {
    this.validateTableUpdates(request.updates);
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
    const updateRowOutcomes: DriverUpdateRowOutcome[] = [];
    for (const [rowIndex, update] of request.updates.entries()) {
      let writeMayHaveApplied = false;
      try {
        context?.signal.throwIfAborted();
        if (
          Object.hasOwn(update.changes, "_id") &&
          update.changes._id !== update.primaryKeys._id
        ) {
          throw new Error("MongoDB does not support updating the _id field.");
        }
        const criteria = buildMongoTableUpdateCriteria(update);
        const options = this.getMutationTimeoutOptions(context);
        writeMayHaveApplied = true;
        const result = options
          ? await collection.updateOne(
              criteria,
              { $set: update.changes },
              options,
            )
          : await collection.updateOne(criteria, { $set: update.changes });
        writeMayHaveApplied = false;
        const knownMatchCount =
          result.acknowledged !== false &&
          Number.isInteger(result.matchedCount) &&
          (result.matchedCount === 0 || result.matchedCount === 1);
        const status = !knownMatchCount
          ? "unknown"
          : result.matchedCount === 1
            ? "applied"
            : "not_applied";
        if (status === "applied") affectedRows++;
        updateRowOutcomes.push({ rowIndex, status });
      } catch (error: unknown) {
        throw failedDriverUpdateRows(
          error,
          request.updates.length,
          updateRowOutcomes,
          rowIndex,
          affectedRows,
          writeMayHaveApplied,
        );
      }
    }
    return completeDriverUpdateRowsResult(affectedRows, updateRowOutcomes);
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
      originalValues?: Record<string, unknown>;
      originalTypes?: Record<string, MongoValueTypeHint>;
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
      assertSupportedMongoTableUpdate(data);
      const filter = buildMongoTableUpdateCriteria(data);
      const serializedFilter = this.serializeMongosh(filter);
      const update = this.serializeMongosh({ $set: data.changes ?? {} });
      return `${collectionRef}.updateOne(\n  ${serializedFilter},\n  ${update}\n)`;
    }
    return (data.primaryKeyValuesList ?? [data.primaryKeys ?? {}])
      .map(
        (entry) =>
          `${collectionRef}.deleteOne(${this.serializeMongosh({ _id: { $eq: entry._id } })})`,
      )
      .join(";\n");
  }

  async runTransaction(operations: TransactionOperation[]): Promise<void> {
    if (operations.length > 1) {
      throw new Error(
        "[RapiDB] MongoDB driver does not support atomic multi-operation transactions. Apply one row at a time.",
      );
    }
    for (const operation of operations) {
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
    if (
      typeof value === "string" &&
      value !== NULL_SENTINEL &&
      /^binData(?:\(\d+\))?$/i.test(column.nativeType)
    ) {
      return coerceMongoBinaryValue(
        unwrapQuotedMongoDisplay(value),
        column,
        false,
      );
    }
    if (
      typeof value === "string" &&
      value !== NULL_SENTINEL &&
      isMongoIntegerType(column.nativeType)
    ) {
      return coerceMongoNumericInput(unwrapQuotedMongoDisplay(value), column);
    }
    if (column.nativeType === "string") return value;
    try {
      return this.coerceInputValue(value, column);
    } catch (error) {
      if (
        error instanceof JsonNumericPrecisionError &&
        typeof value === "string"
      ) {
        return JSON.parse(unwrapQuotedMongoDisplay(value)) as unknown;
      }
      throw error;
    }
  }

  coercePrimaryKeyValue(value: unknown, column: ColumnTypeMeta): unknown {
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

  coerceTableEditValue(
    value: unknown,
    column: ColumnTypeMeta,
    options?: { preserveNativeType?: boolean },
  ): unknown {
    const jsonColumn =
      column.category === "json" || column.category === "array";
    if (MONGO_EXACT_TEXT_NATIVE_TYPES.has(column.nativeType)) {
      return this.coerceInputValue(value, column);
    }
    if (typeof value === "string" && isQuotedMongoString(value)) {
      const quoted = value.trim();
      return jsonColumn && quoted.startsWith('"')
        ? (JSON.parse(quoted) as string)
        : unwrapQuotedMongoDisplay(quoted);
    }
    if (options?.preserveNativeType && isMongoIntegerType(column.nativeType)) {
      if (value === null || value === undefined || value === NULL_SENTINEL) {
        return this.coerceInputValue(value, column);
      }
      const normalized =
        typeof value === "string"
          ? unwrapQuotedMongoDisplay(value)
          : typeof value === "number" || typeof value === "bigint"
            ? String(value)
            : null;
      if (normalized === null) return this.coerceInputValue(value, column);
      if (!/^[+-]?\d+$/.test(normalized)) {
        throw new Error(
          `MongoDB ${column.nativeType} value must be an integer.`,
        );
      }
      return coerceMongoIntegerValue(normalized, column, true);
    }
    const coerced = this.coerceInputValue(value, column);
    if (
      typeof value === "string" &&
      value !== "" &&
      typeof coerced === "string" &&
      (MONGO_TYPED_EDIT_NATIVE_TYPES.has(column.nativeType) ||
        /^binData(?:\(\d+\))?$/i.test(column.nativeType) ||
        jsonColumn)
    ) {
      throw new Error(
        `Invalid MongoDB ${jsonColumn ? "JSON" : column.nativeType} value ${JSON.stringify(value)}. Quote the value to store it as a string.`,
      );
    }
    return coerced;
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

    if (column.nativeType === "string") return value;
    if (column.nativeType === "symbol") return new BSONSymbol(value);
    if (column.nativeType === "javascript") return new Code(value);

    const rawValue = value.trim();
    if (
      !isQuotedMongoString(rawValue) &&
      (column.nativeType === "minKey" || column.nativeType === "maxKey")
    ) {
      if (/^MinKey\(\)$/i.test(rawValue)) return new MinKey();
      if (/^MaxKey\(\)$/i.test(rawValue)) return new MaxKey();
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

    if (column.nativeType === "date" || column.nativeType === "timestamp") {
      if (!normalized || isQuotedMongoString(rawValue)) return normalized;
      const parsed = parseMongoTemporalInput(normalized, column.nativeType);
      if (parsed) return parsed;
      throw new Error(
        `Invalid MongoDB ${column.nativeType} value ${JSON.stringify(value)}. Quote the value to store it as a string.`,
      );
    }

    if (column.nativeType === "decimal" || column.nativeType === "decimal128") {
      try {
        return Decimal128.fromString(normalized);
      } catch {
        return normalized;
      }
    }

    if (isMongoIntegerType(column.nativeType)) {
      return coerceMongoNumericInput(normalized, column);
    }

    if (column.nativeType === "double" || column.nativeType === "number") {
      return coerceMongoNumericInput(normalized, column);
    }

    if (column.nativeType === "bool") {
      const lower = normalized.toLowerCase();
      if (lower === "true" || lower === "1") return true;
      if (lower === "false" || lower === "0") return false;
      return normalized;
    }

    if (column.nativeType === "javascriptWithScope") {
      return parseMongoDisplayJavascriptWithScope(normalized) ?? normalized;
    }

    if (column.nativeType === "dbPointer" || column.nativeType === "dbRef") {
      return parseMongoDisplayDbPointer(normalized) ?? normalized;
    }

    if (column.nativeType === "regex") {
      return parseMongoRegexInput(normalized) ?? normalized;
    }

    if (/^binData(?:\(\d+\))?$/i.test(column.nativeType)) {
      return coerceMongoBinaryValue(normalized, column, true);
    }

    if (column.category === "array" || column.category === "json") {
      try {
        return parseJsonForWrite(normalized);
      } catch (error) {
        if (error instanceof JsonNumericPrecisionError) throw error;
        return normalized;
      }
    }

    return normalized;
  }

  formatOutputValue(value: unknown, column: ColumnTypeMeta): unknown {
    if (
      typeof value === "string" &&
      (column.category === "json" || column.category === "array")
    ) {
      try {
        return formatMongoDisplayValue(this.coerceInputValue(value, column));
      } catch (error) {
        if (error instanceof JsonNumericPrecisionError) return value;
        throw error;
      }
    }
    if (
      typeof value === "string" &&
      (column.nativeType === "date" || column.nativeType === "timestamp")
    ) {
      return formatMongoDisplayValue(
        parseMongoTemporalInput(value, column.nativeType) ?? value,
      );
    }
    if (typeof value === "string") {
      const coerced =
        value !== NULL_SENTINEL &&
        /^binData(?:\(\d+\))?$/i.test(column.nativeType)
          ? coerceMongoBinaryValue(
              unwrapQuotedMongoDisplay(value),
              column,
              false,
            )
          : value !== NULL_SENTINEL && isMongoIntegerType(column.nativeType)
            ? coerceMongoIntegerValue(
                unwrapQuotedMongoDisplay(value),
                column,
                false,
              )
            : this.coerceInputValue(value, column);
      if (coerced !== value) {
        return formatMongoDisplayValue(coerced);
      }
    }
    return formatMongoDisplayValue(value);
  }

  checkPersistedEdit(
    column: ColumnTypeMeta,
    expectedValue: unknown,
    _options?: { persistedValue: unknown },
  ) {
    if (isUnsupportedMongoTableField(column.name)) {
      return {
        ok: false,
        shouldVerify: false,
        message: unsupportedMongoTableFieldMessage(column.name),
      };
    }
    if (column.nativeType === "dbPointer") {
      return {
        ok: false,
        shouldVerify: false,
        message: dbPointerMongoTableFieldMessage(column.name),
      };
    }
    if (
      typeof expectedValue === "string" &&
      (column.category === "json" || column.category === "array")
    ) {
      try {
        this.coerceInputValue(expectedValue, column);
      } catch (error) {
        return {
          ok: false,
          shouldVerify: false,
          message: error instanceof Error ? error.message : String(error),
        };
      }
    }
    if (
      typeof expectedValue === "string" &&
      ((/^binData(?:\(\d+\))?$/i.test(column.nativeType) &&
        !isQuotedMongoString(expectedValue)) ||
        isMongoIntegerType(column.nativeType))
    ) {
      try {
        const normalized = unwrapQuotedMongoDisplay(expectedValue);
        if (isMongoIntegerType(column.nativeType)) {
          coerceMongoNumericInput(normalized, column);
        } else if (
          typeof coerceMongoBinaryValue(normalized, column, true) ===
            "string" &&
          expectedValue !== "" &&
          expectedValue !== NULL_SENTINEL
        ) {
          throw new Error(
            `Invalid MongoDB ${column.nativeType} value ${JSON.stringify(expectedValue)}. Quote the value to store it as a string.`,
          );
        }
      } catch (error: unknown) {
        return {
          ok: false,
          shouldVerify: false,
          message: error instanceof Error ? error.message : String(error),
        };
      }
    }
    return null;
  }

  normalizeFilterValue(
    column: ColumnTypeMeta,
    operator: FilterOperator,
    value: string | [string, string] | undefined,
  ) {
    if (value === undefined) {
      return undefined;
    }
    if (operator === "in" && typeof value === "string") {
      return column.nativeType === "timestamp"
        ? splitInFilterValues(value)
            .map((entry) => this.normalizeInlineFilterScalar(column, entry))
            .join(", ")
        : value;
    }
    if (
      typeof value === "string" &&
      (operator === "eq" || operator === "neq") &&
      isQuotedMongoString(value)
    ) {
      return value.trim();
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
    if (this.config.mongoConnectionMode === "fields")
      return buildMongoFieldsUri(this.config);
    const rawHost = this.config.host?.trim() || "localhost";
    const host = isIPv6(rawHost) ? `[${rawHost}]` : rawHost;
    const port = this.config.port ?? 27017;
    const auth = this.config.username
      ? `${encodeURIComponent(this.config.username)}:${encodeURIComponent(this.config.password ?? "")}@`
      : "";
    const database = encodeURIComponent(this.defaultDatabaseName());
    const params = new URLSearchParams();
    if (this.config.authMechanism)
      params.set("authMechanism", this.config.authMechanism);
    if (this.config.authSource)
      params.set("authSource", this.config.authSource);
    if (this.config.replicaSet)
      params.set("replicaSet", this.config.replicaSet);
    if (this.config.directConnection !== undefined)
      params.set("directConnection", String(this.config.directConnection));
    return `mongodb://${auth}${host}:${port}/${database}${params.size ? `?${params}` : ""}`;
  }

  private requireClient(): MongoClient {
    if (!this.client || !this.connected) {
      throw new Error("MongoDB is not connected.");
    }
    return this.client;
  }

  private defaultDatabaseName(): string {
    return resolveMongoDatabaseName(this.config);
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
    sdkOptions: MongoSdkOptions = getMetadataSdkOptions(),
  ): Promise<{
    type: "collection" | "view";
    options: Record<string, unknown>;
  }> {
    const collection = (
      await this.requireDb(database)
        .listCollections({ name: table }, { nameOnly: false, ...sdkOptions })
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
    this.tableRowRawDocuments.set(row, document);
    this.tableRowSchemas.set(
      row,
      new Map(
        Object.entries(document).map(([key, value]) => [
          key,
          inferMongoRowValueType(value),
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

  private getTableRowValueTypes(
    row: Record<string, unknown>,
  ): Record<
    string,
    { category: TypeCategory; nativeType: string; bsonSubtype?: number }
  > {
    const schemas = this.tableRowSchemas.get(row);
    if (!schemas) return {};
    return Object.fromEntries(
      [...schemas].map(([name, schema]) => [name, { ...schema }]),
    );
  }

  private tablePageSdkOptions(
    request: DriverTablePageRequest,
  ): MongoSdkOptions {
    const timeoutMS = getQueryRemainingTimeoutMs({
      signal: request.signal,
      deadline: request.deadline,
    });
    return {
      ...(request.signal ? { signal: request.signal } : {}),
      ...(timeoutMS !== undefined ? { timeoutMS } : {}),
    };
  }

  private withDocumentColumns(
    columns: readonly ColumnTypeMeta[],
    documents: readonly Record<string, unknown>[],
    names: readonly string[] = [],
  ): ColumnTypeMeta[] {
    const extraNames = new Set([
      ...names,
      ...documents.flatMap((document) => Object.keys(document)),
    ]);
    for (const column of columns) extraNames.delete(column.name);
    return [
      ...columns,
      ...[...extraNames].map(
        (name): ColumnTypeMeta => ({
          name,
          type: "mixed",
          nativeType: "mixed",
          category: "other",
          nullable: true,
          defaultValue: undefined,
          isPrimaryKey: false,
          isForeignKey: false,
          filterable: false,
          filterOperators: [],
          valueSemantics: "plain",
          isReadOnly: true,
        }),
      ),
    ];
  }

  private async describeSchemaColumns(
    database: string,
    table: string,
    limit: number,
    sdkOptions: () => MongoSdkOptions = getMetadataSdkOptions,
    writableSampleLimit = limit,
  ): Promise<ColumnTypeMeta[]> {
    const documents = await this.readSchemaDocuments(
      database,
      table,
      limit,
      sdkOptions,
    );
    const isView = await this.isView(database, table, sdkOptions);
    const dbPointers = await this.findDbPointerFields(
      database,
      table,
      documents,
      sdkOptions,
    );
    const writableNames = new Set(
      documents.slice(0, writableSampleLimit).flatMap(Object.keys),
    );
    const inferredColumns = inferColumnsFromRows(documents, "_id", {
      primaryKeyNames: isView ? [] : ["_id"],
      nullableMode: "schemaLess",
    });

    return inferredColumns.map((inferredColumn) => {
      const { name, isPrimaryKey, primaryKeyOrdinal } = inferredColumn;
      const sample = selectMongoSchemaSample(documents, name);
      const { category, nativeType, bsonSubtype } = documents.some((document) =>
        dbPointers.get(document)?.has(name),
      )
        ? MONGO_DB_POINTER_TYPE
        : inferMongoSchemaType(sample);
      const filterable = category !== "spatial";
      const nullable = !isPrimaryKey;
      const observedTypes = documents
        .filter(
          (document) =>
            Object.hasOwn(document, name) &&
            document[name] !== null &&
            document[name] !== undefined,
        )
        .map((document) => inferMongoSchemaType(document[name]));
      const observedCategories = new Set(
        observedTypes.map((observedType) => observedType.category),
      );
      const observedTextNativeTypes = new Set(
        observedTypes
          .filter((observedType) => observedType.category === "text")
          .map((observedType) => observedType.nativeType),
      );
      const filterOperators = new Set<FilterOperator>();
      if (filterable) {
        const categories =
          observedCategories.size > 0 ? [...observedCategories] : [category];
        const isNumericCategory = (candidate: TypeCategory) =>
          candidate === "integer" ||
          candidate === "decimal" ||
          candidate === "float";
        const allNumeric = categories.every(isNumericCategory);
        const mixedTextAndNumeric =
          categories.includes("text") &&
          categories.some(isNumericCategory) &&
          observedTextNativeTypes.size === 1 &&
          observedTextNativeTypes.has("string") &&
          categories.every(
            (candidate) => candidate === "text" || isNumericCategory(candidate),
          );
        const operators =
          categories.length === 1
            ? filterOperatorsForCategory(categories[0])
            : allNumeric
              ? filterOperatorsForCategory("decimal")
              : mixedTextAndNumeric
                ? (["like", "eq", "neq", "in"] as FilterOperator[])
                : [];
        for (const operator of operators) {
          filterOperators.add(operator);
        }
      }
      if (nullable) {
        for (const operator of resolveFilterOperators(category, {
          filterable: false,
          nullable: true,
        })) {
          filterOperators.add(operator);
        }
      }

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
        filterOperators: [...filterOperators],
        valueSemantics: "plain",
        ...(isUnsupportedMongoTableField(name) ||
        nativeType === "dbPointer" ||
        !writableNames.has(name)
          ? { isReadOnly: true }
          : {}),
      } satisfies ColumnTypeMeta;
    });
  }

  private async isView(
    database: string,
    table: string,
    sdkOptions: () => MongoSdkOptions = getMetadataSdkOptions,
  ): Promise<boolean> {
    const options = sdkOptions();
    try {
      const definition = await this.getCollectionDefinition(
        database,
        table,
        options,
      );
      return definition.type === "view";
    } catch (error) {
      if (isMongoInterruption(error, options.signal)) throw error;
      return false;
    }
  }

  private async findDbPointerFields(
    database: string,
    table: string,
    documents: readonly Record<string, unknown>[],
    sdkOptions: () => MongoSdkOptions,
  ): Promise<Map<Record<string, unknown>, Set<string>>> {
    const pointers = new Map<Record<string, unknown>, Set<string>>();
    for (const document of documents) {
      const names = Object.keys(document).filter(
        (name) =>
          !isUnsupportedMongoTableField(name) &&
          document[name] instanceof DBRef,
      );
      if (names.length > 0) pointers.set(document, new Set(names));
    }
    const candidates = [...pointers.keys()].filter((document) =>
      Object.hasOwn(document, "_id"),
    );
    if (candidates.length === 0) return pointers;
    const names = [
      ...new Set([...pointers.values()].flatMap((set) => [...set])),
    ];
    const options = sdkOptions();
    let results: Record<string, unknown>[];
    try {
      results = await this.requireDb(database)
        .collection(table)
        .aggregate(
          [
            {
              $match: {
                _id: { $in: candidates.map((document) => document._id) },
              },
            },
            {
              $project: Object.fromEntries(
                names.map((name, index) => [
                  `t${index}`,
                  { $type: `$${name}` },
                ]),
              ),
            },
          ],
          { promoteValues: false, bsonRegExp: true, ...options },
        )
        .toArray();
    } catch (error) {
      if (isMongoInterruption(error, options.signal)) throw error;
      return pointers;
    }
    const typesById = new Map(
      results.map((result) => [mongoIdKey(result._id), result]),
    );
    for (const document of candidates) {
      const types = typesById.get(mongoIdKey(document._id));
      const fields = pointers.get(document);
      if (!types || !fields) continue;
      for (const name of fields) {
        if (types[`t${names.indexOf(name)}`] === "object") fields.delete(name);
      }
    }
    return pointers;
  }

  private async markDbPointerCells(
    request: DriverTablePageRequest,
    rows: readonly Record<string, unknown>[],
  ): Promise<void> {
    const documents = rows.flatMap((row) => {
      const document = this.tableRowRawDocuments.get(row);
      return document ? [{ row, document }] : [];
    });
    const pointers = await this.findDbPointerFields(
      request.database,
      request.table,
      documents.map(({ document }) => document),
      () => this.tablePageSdkOptions(request),
    );
    for (const { row, document } of documents) {
      const schema = this.tableRowSchemas.get(row);
      for (const name of pointers.get(document) ?? []) {
        schema?.set(name, MONGO_DB_POINTER_TYPE);
      }
    }
  }

  private async readSchemaDocuments(
    database: string,
    table: string,
    limit: number,
    sdkOptions: () => MongoSdkOptions = getMetadataSdkOptions,
  ): Promise<Record<string, unknown>[]> {
    const docs = await this.requireDb(database)
      .collection(table)
      .find(
        {},
        {
          promoteValues: false,
          bsonRegExp: true,
          ...sdkOptions(),
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
    sdkOptions: MongoSdkOptions = {},
    criteria: Record<string, unknown> = {},
  ): Promise<Record<string, unknown>[]> {
    const docs = await this.requireDb(database)
      .collection(table)
      .find(criteria, {
        promoteValues: false,
        bsonRegExp: true,
        ...sdkOptions,
      })
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
      if (isUnsupportedMongoTableField(filter.column)) {
        return true;
      }
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
    if (value instanceof Double) {
      return Number.isFinite(value.value) && !Object.is(value.value, -0)
        ? `Double(${String(value.value)})`
        : `Double(${JSON.stringify(Object.is(value.value, -0) ? "-0" : String(value.value))})`;
    }
    if (value instanceof Timestamp) {
      return `new Timestamp(${value.high >>> 0}, ${value.low >>> 0})`;
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
      return `new BSONRegExp(${JSON.stringify(value.pattern)}, ${JSON.stringify(value.options)})`;
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
        ([k, v]) => {
          const serializedKey =
            k === "__proto__" ? `[${JSON.stringify(k)}]` : JSON.stringify(k);
          return `${serializedKey}: ${this.serializeMongosh(v)}`;
        },
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
      trimmed &&
      (column.nativeType === "date" || column.nativeType === "timestamp")
    ) {
      const parsed = parseMongoTemporalInput(trimmed, column.nativeType);
      if (!parsed) {
        throw new Error(
          `Invalid MongoDB ${column.nativeType} filter value ${JSON.stringify(trimmed)}. Use YYYY-MM-DD or YYYY-MM-DD HH:MM[:SS[.fff]] with an optional Z or ±HH:MM offset${column.nativeType === "timestamp" ? ", or Timestamp(seconds, increment)" : ""}.`,
        );
      }
      return String(formatMongoDisplayValue(parsed));
    }
    if (
      column.category === "binary" ||
      column.category === "datetime" ||
      column.nativeType === "objectId"
    ) {
      let coerced: unknown;
      try {
        coerced = this.coerceInputValue(trimmed, column);
      } catch (error) {
        if (column.category !== "binary") throw error;
        return trimmed;
      }
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
      columnMeta: columns.map((column) => mongoQueryColumnMeta([row[column]])),
      rows: [this.mapRowToQueryRow(row, columns)],
      rowCount: 1,
      affectedRows,
      executionTimeMs: Date.now() - startedAt,
    };
  }

  private buildMongoDocumentRowsQueryResult(
    docs: readonly Record<string, unknown>[],
    startedAt: number,
    rowLimit = MONGODB_QUERY_HARD_CAP,
  ): QueryResult {
    const truncated = docs.length > rowLimit;
    const retainedDocs = truncated ? docs.slice(0, rowLimit) : docs;
    const rows = retainedDocs.map((doc) => this.toRow(doc));
    const columns = inferColumnsFromRows(rows, "_id").map(
      (column) => column.name,
    );
    const missingCells = this.queryMissingCells(rows, columns);
    return {
      columns,
      columnMeta: columns.map((column) =>
        mongoQueryColumnMeta(retainedDocs.map((doc) => doc[column])),
      ),
      rows: rows.map((row) => this.mapRowToQueryRow(row, columns)),
      ...(missingCells.length ? { missingCells } : {}),
      rowCount: rows.length,
      executionTimeMs: Date.now() - startedAt,
      ...(truncated ? { truncated: true } : {}),
    };
  }

  private mapQueryResultRowsToObjects(
    result: QueryResult,
  ): Record<string, unknown>[] {
    const missing = new Set(
      result.missingCells?.map(
        ({ rowIndex, columnIndex }) => `${rowIndex}:${columnIndex}`,
      ),
    );
    return result.rows.map((row, rowIndex) =>
      Object.fromEntries(
        result.columns.flatMap((column, index) =>
          missing.has(`${rowIndex}:${index}`)
            ? []
            : [[column, row[`__col_${index}`]]],
        ),
      ),
    );
  }

  private queryMissingCells(
    rows: readonly Record<string, unknown>[],
    columns: readonly string[],
  ): NonNullable<QueryResult["missingCells"]> {
    const missing: NonNullable<QueryResult["missingCells"]> = [];
    rows.forEach((row, rowIndex) => {
      columns.forEach((column, columnIndex) => {
        if (!Object.hasOwn(row, column))
          missing.push({ rowIndex, columnIndex });
      });
    });
    return missing;
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
