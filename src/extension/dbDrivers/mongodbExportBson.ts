import { types } from "node:util";
import {
  type AbstractCursor,
  BSON,
  Code,
  DBRef,
  type Document,
  ObjectId,
} from "mongodb";

// The SDK always decodes 0x0c as a DBRef and may split its namespace at a dot.
// Record provenance without changing the values used for display, CSV or edits.
const wireDbRefTypes = new WeakMap<DBRef, string | false>();

// The SDK can inherit useBigInt64 from the URI, but BSON rejects it with
// promoteValues: false. Keep int64 as BSON Long on type-preserving paths.
export const MONGO_NATIVE_BSON_OPTIONS = {
  useBigInt64: false,
  promoteValues: false,
  bsonRegExp: true,
} as const;

export const MONGO_EXPORT_BSON_OPTIONS = {
  ...MONGO_NATIVE_BSON_OPTIONS,
  raw: true,
} as const;

function couldBeDbPointer(ref: DBRef): boolean {
  return ref.oid instanceof ObjectId && Object.keys(ref.fields).length === 0;
}

export function mongoExportDbPointerNamespace(ref: DBRef): string | undefined {
  const wireType = wireDbRefTypes.get(ref);
  if (typeof wireType === "string") return wireType;
  if (wireType === undefined && couldBeDbPointer(ref)) {
    // Even a DBRef with a database name might have come from a dotted DBPointer.
    throw new Error(
      "MongoDB export cannot distinguish this DBRef from a deprecated BSON DBPointer because the SDK did not expose its original BSON bytes. Use a BSON backup to preserve this value's type.",
    );
  }
  return undefined;
}

function deserializeMongoExportDocument(
  bytes: Uint8Array,
  validateUtf8: boolean,
): Document {
  const document = BSON.deserialize(bytes, {
    ...MONGO_EXPORT_BSON_OPTIONS,
    validation: { utf8: validateUtf8 },
  });
  decodeMongoRawDocument(document, validateUtf8);
  return document;
}

function decodeMongoRawValue(value: unknown, validateUtf8: boolean): unknown {
  // In raw mode BSON still decodes the root, arrays and Code scopes, but
  // leaves embedded documents as byte views. Binary values are BSON Binary
  // (promoteValues is false), so a byte view always means an embedded document.
  if (types.isUint8Array(value)) {
    return deserializeMongoExportDocument(value, validateUtf8);
  }
  if (value instanceof DBRef && couldBeDbPointer(value)) {
    // Only wire DBPointer becomes an inline DBRef here. BSON 7.2 splits a
    // namespace only at exactly one dot; reverse that conversion losslessly.
    wireDbRefTypes.set(
      value,
      value.db === undefined
        ? value.collection
        : `${value.db}.${value.collection}`,
    );
  } else if (value instanceof Code && value.scope) {
    decodeMongoRawDocument(value.scope, validateUtf8);
  } else if (Array.isArray(value)) {
    for (let index = 0; index < value.length; index++) {
      value[index] = decodeMongoRawValue(value[index], validateUtf8);
    }
  }
  return value;
}

function decodeMongoRawDocument(
  document: Document,
  validateUtf8: boolean,
  isCommandReply = false,
): void {
  // The document itself may be promoted to DBRef, but it is not a DBPointer
  // element: it came from a known document boundary or a Code scope.
  const members = document instanceof DBRef ? document.fields : document;
  if (document instanceof DBRef) {
    wireDbRefTypes.set(document, false);
    document.oid = decodeMongoRawValue(document.oid, validateUtf8) as ObjectId;
  }
  for (const name of Object.keys(members)) {
    members[name] = decodeMongoRawValue(
      members[name],
      // Match the driver's UTF-8 exception for command write errors.
      validateUtf8 && !(isCommandReply && name === "writeErrors"),
    );
  }
}

export function readMongoExportCursor(
  cursor: AbstractCursor,
  validateUtf8 = true,
  signal?: AbortSignal,
): Promise<Document[]> {
  // Decode while consuming each batch; do not collect or retain raw row views.
  const decoded = cursor.map?.((value: Document) => {
    signal?.throwIfAborted();
    return types.isUint8Array(value)
      ? deserializeMongoExportDocument(value, validateUtf8)
      : value;
  });
  // A cursor adapter may already return decoded documents. If so, the JSON
  // serializer still refuses any ambiguous DBRef without wire provenance.
  return (decoded ?? cursor).toArray();
}

export function decodeMongoExportCommand(document: Document): Document {
  decodeMongoRawDocument(document, true, true);
  return document;
}
