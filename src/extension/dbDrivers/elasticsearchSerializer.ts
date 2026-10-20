import { errors, Serializer } from "@elastic/elasticsearch";
import { compareNumericTokens } from "../../shared/numericNormalization";

export class ElasticsearchSerializer extends Serializer {
  override deserialize<T = unknown>(json: string): T {
    const parsed = super.deserialize<T>(json);
    const tokens = /"(?:\\.|[^"\\])*"|-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?/g;
    for (const match of json.matchAll(tokens)) {
      const token = match[0];
      if (token.startsWith('"')) continue;
      const number = Number(token);
      const actual = Number.isFinite(number)
        ? Number.isInteger(number)
          ? BigInt(number).toString()
          : String(number)
        : null;
      if (
        actual === null ||
        compareNumericTokens(token, actual) !== 0 ||
        compareNumericTokens(token, String(number)) !== 0
      ) {
        throw new errors.DeserializationError(
          "Elasticsearch response rejected to avoid losing numeric precision. A JSON number cannot round-trip safely through this client's numeric representation.",
          json,
        );
      }
    }
    return parsed;
  }
}
