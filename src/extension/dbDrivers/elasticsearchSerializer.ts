import { randomUUID } from "node:crypto";
import { errors, Serializer } from "@elastic/elasticsearch";
import { isRoundTripJsonNumberToken } from "../utils/jsonWriteSafety";

export function stringifyElasticsearchJson(
  value: unknown,
  space?: number,
): string {
  const marker = `rapidb-negative-zero-${randomUUID()}`;
  let hasNegativeZero = false;
  const json = JSON.stringify(
    value,
    (_key, entry: unknown) => {
      if (Object.is(entry, -0)) {
        hasNegativeZero = true;
        return marker;
      }
      return entry;
    },
    space,
  );
  return hasNegativeZero ? json.replaceAll(`"${marker}"`, "-0.0") : json;
}

export class ElasticsearchSerializer extends Serializer {
  override serialize(object: Record<string, unknown>): string {
    try {
      return stringifyElasticsearchJson(object);
    } catch (error) {
      throw new errors.SerializationError(
        error instanceof Error ? error.message : String(error),
        object,
      );
    }
  }

  override deserialize<T = unknown>(json: string): T {
    const parsed = super.deserialize<T>(json);
    const tokens =
      /"(?:\\.|[^"\\])*"|-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?|[{}[\]:,]|[^"{}[\]:,\d-]+/y;
    const frames: Frame[] = [];
    const lenientTokens: Array<[number, number]> = [];
    const reject = () =>
      new errors.DeserializationError(
        "Elasticsearch response rejected to avoid losing numeric precision. A JSON number cannot round-trip safely through this client's numeric representation.",
        json,
      );
    let match = tokens.exec(json);
    let scanned = 0;
    while (match !== null) {
      const token = match[0];
      scanned = tokens.lastIndex;
      const frame = frames[frames.length - 1];
      const mode =
        frame === undefined
          ? "strict"
          : frame.object
            ? childNumericMode(frames, frame.key)
            : frame.mode;
      if (token === "{" || token === "[") {
        frames.push({
          object: token === "{",
          mode,
          slot: frame?.object ? frame.key : undefined,
          key: undefined,
          expectsKey: token === "{",
        });
      } else if (token === "}" || token === "]") {
        frames.pop();
      } else if (token === ",") {
        if (frame?.object) frame.expectsKey = true;
      } else if (token.startsWith('"')) {
        if (frame?.object && frame.expectsKey) {
          frame.key = token.includes("\\")
            ? (JSON.parse(token) as string)
            : token.slice(1, -1);
          frame.expectsKey = false;
        }
      } else if (/^-?\d/.test(token) && !isRoundTripJsonNumberToken(token)) {
        if (mode !== "lenient") throw reject();
        lenientTokens.push([match.index, scanned]);
      }
      match = tokens.exec(json);
    }
    if (scanned !== json.length) throw reject();
    if (lenientTokens.length === 0) return parsed;
    let text = "";
    let cursor = 0;
    for (const [start, end] of lenientTokens) {
      text += `${json.slice(cursor, start)}"${json.slice(start, end)}"`;
      cursor = end;
    }
    return super.deserialize<T>(text + json.slice(cursor));
  }
}

type NumericMode = "strict" | "source" | "lenient";

type Frame = {
  object: boolean;
  mode: NumericMode;
  slot: string | undefined;
  key: string | undefined;
  expectsKey: boolean;
};

function childNumericMode(
  frames: Frame[],
  key: string | undefined,
): NumericMode {
  const depth = frames.length - 1;
  const parent = frames[depth];
  if (parent.mode === "source" || key === "_source") return "source";
  if (key === "aggregations" && depth === 0) return "lenient";
  if (
    key === "sort" &&
    parent.slot === undefined &&
    frames[depth - 1]?.object === false &&
    frames[depth - 1]?.slot === "hits" &&
    frames[depth - 2]?.slot === "hits"
  )
    return "lenient";
  return parent.mode;
}
