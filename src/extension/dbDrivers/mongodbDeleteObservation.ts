import {
  Binary,
  BSON,
  BSONRegExp,
  Decimal128,
  type Document,
  Double,
  Int32,
  Long,
  ObjectId,
  Timestamp,
} from "mongodb";

// Server $eq is numeric for numbers: it merges +0/-0 and the representations
// of an equal Decimal128 (1.0 vs 1.00). Leaves that need more than $eq carry
// an extra exact check that is evaluated server-side in the same filter.
type ExactBson =
  | { type: "object"; fields: Array<[string, ExactBson]> }
  | { type: "array"; elements: ExactBson[] }
  | { type: "double"; value: Double; negativeZero?: boolean }
  | { type: "decimal"; value: Decimal128; text: string }
  | { type: string; value: unknown };

// Command size and expression depth are bounded independently of the
// server's BSON document limit. Every document/array level adds about eight
// levels of expression nesting ($cond/$let/$and/$map...), so the depth bound
// keeps the filter below MongoDB's nesting limit for command documents.
export const MONGO_DELETE_OBSERVATION_LIMITS = Object.freeze({
  wireBytes: 256 * 1024,
  nodes: 2048,
  depth: 12,
});

export interface MongoDeleteState {
  id: ObjectId | string;
  document: ExactBson;
  retainedBytes: number;
}

// Bound the command size and expression depth independently of the server's
// BSON document limit (an expression can be larger than the observed row).
export function observeMongoDeleteState(
  document: Document,
  wire: Uint8Array,
): MongoDeleteState | undefined {
  const limits = MONGO_DELETE_OBSERVATION_LIMITS;
  if (
    wire.byteLength > limits.wireBytes ||
    !(typeof document._id === "string" || document._id instanceof ObjectId)
  ) {
    return undefined;
  }

  const maxNodes = limits.nodes;
  let remaining = maxNodes;
  const read = (value: unknown, depth: number): ExactBson => {
    if (--remaining < 0 || depth > limits.depth)
      throw new Error("BSON shape limit");
    if (Array.isArray(value)) {
      return {
        type: "array",
        elements: value.map((entry) => read(entry, depth + 1)),
      };
    }
    if (
      value &&
      typeof value === "object" &&
      Object.getPrototypeOf(value) === Object.prototype
    ) {
      return {
        type: "object",
        fields: Object.entries(value).map(([key, entry]) => [
          key,
          read(entry, depth + 1),
        ]),
      };
    }
    if (value instanceof Double) {
      // NaN payloads are merged by every comparison; never use them as proof.
      if (Number.isNaN(value.value))
        throw new Error("Unsupported BSON equality");
      return value.value === 0
        ? { type: "double", value, negativeZero: Object.is(value.value, -0) }
        : { type: "double", value };
    }
    if (value instanceof Decimal128) {
      // Compare the exact decimal representation (sign, coefficient and
      // exponent) as text. Only canonical encodings round-trip; NaN payloads
      // have no distinguishing text.
      const text = value.toString();
      if (
        /nan/i.test(text) ||
        !Buffer.from(Decimal128.fromString(text).bytes).equals(
          Buffer.from(value.bytes),
        )
      ) {
        throw new Error("Unsupported BSON equality");
      }
      return { type: "decimal", value, text };
    }
    if (value instanceof RegExp || value instanceof BSONRegExp) {
      // BSON regex equality compares pattern and options byte-wise.
      return { type: "regex", value };
    }
    const type =
      value === null
        ? "null"
        : typeof value === "boolean"
          ? "bool"
          : typeof value === "string"
            ? "string"
            : value instanceof ObjectId
              ? "objectId"
              : value instanceof Timestamp
                ? "timestamp"
                : value instanceof Int32
                  ? "int"
                  : value instanceof Long
                    ? "long"
                    : value instanceof Date && Number.isFinite(value.getTime())
                      ? "date"
                      : value instanceof Binary
                        ? "binData"
                        : undefined;
    if (!type) throw new Error("Unsupported BSON equality");
    return { type, value };
  };

  try {
    const snapshot = read(document, 0);
    // Reject anything the SDK folded or reordered (e.g. duplicate keys,
    // undefined, nonroundtrippable dates, invalid UTF-8, DBPointer).
    if (!Buffer.from(wire).equals(BSON.serialize(document))) return undefined;
    return {
      id: document._id,
      document: snapshot,
      // Include the cost of the recursive predicate generated for a preview.
      retainedBytes: wire.byteLength + (maxNodes - remaining) * 256,
    };
  } catch {
    return undefined;
  }
}

export function buildMongoObservedDeleteFilter(
  observation: MongoDeleteState,
): Document {
  const literal = (value: unknown) => ({ $literal: value });
  const match = (value: string | Document, snapshot: ExactBson): Document => {
    const sameType = { $eq: [{ $type: value }, snapshot.type] };
    if ("fields" in snapshot) {
      return {
        $cond: [
          sameType,
          {
            $let: {
              vars: { entries: { $objectToArray: value } },
              in: {
                $and: [
                  {
                    $eq: [
                      {
                        $map: {
                          input: "$$entries",
                          as: "entry",
                          in: "$$entry.k",
                        },
                      },
                      literal(snapshot.fields.map(([key]) => key)),
                    ],
                  },
                  ...snapshot.fields.map(([, child], index) => ({
                    $let: {
                      vars: { member: { $arrayElemAt: ["$$entries", index] } },
                      in: match("$$member.v", child),
                    },
                  })),
                ],
              },
            },
          },
          false,
        ],
      };
    }
    if ("elements" in snapshot) {
      return {
        $cond: [
          sameType,
          {
            $and: [
              { $eq: [{ $size: value }, snapshot.elements.length] },
              ...snapshot.elements.map((child, index) =>
                match({ $arrayElemAt: [value, index] }, child),
              ),
            ],
          },
          false,
        ],
      };
    }
    if (snapshot.type === "decimal" && "text" in snapshot) {
      return {
        $and: [
          sameType,
          { $eq: [{ $toString: value }, literal(snapshot.text)] },
        ],
      };
    }
    const equal = { $eq: [value, literal(snapshot.value)] };
    if (snapshot.type === "double" && "negativeZero" in snapshot) {
      // atan2(+0, -1) = pi and atan2(-0, -1) = -pi: the sign of a zero.
      return {
        $and: [
          sameType,
          equal,
          {
            $eq: [
              { $lt: [{ $atan2: [value, -1] }, 0] },
              snapshot.negativeZero === true,
            ],
          },
        ],
      };
    }
    return { $and: [sameType, equal] };
  };
  return {
    _id: { $eq: observation.id },
    $expr: match("$$ROOT", observation.document),
  };
}
