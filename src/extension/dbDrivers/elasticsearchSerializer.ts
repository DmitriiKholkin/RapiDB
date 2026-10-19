import { errors, Serializer } from "@elastic/elasticsearch";
import { compareNumericTokens } from "../../shared/numericNormalization";

/** Keep SDK Number semantics, but never publish a silently rounded response. */
export class ElasticsearchSerializer extends Serializer {
  override deserialize<T = unknown>(json: string): T {
    // Retain the SDK's syntax/prototype-poisoning checks and error contract.
    const parsed = super.deserialize<T>(json);
    // As in jsonWriteSafety, syntax is already validated: matching strings
    // atomically skips quoted numbers/escaped keys and retains even shadowed
    // duplicate-key tokens. Do not inspect just JSON.stringify(parsed), which
    // has already lost the original numbers. Unlike a jsonCanonical tree this
    // also checks shadowed keys, without exposing token wrappers to consumers.
    const tokens = /"(?:\\.|[^"\\])*"|-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?/g;
    for (const match of json.matchAll(tokens)) {
      const token = match[0];
      if (token.startsWith('"')) continue;
      const number = Number(token);
      // Check both actual integer value (also safe for Number-based writes)
      // and JSON's decimal rendering. E.g. 1000000000000000100 rounds on read,
      // while exactly representable 1000000000000000128 rounds on stringify.
      // Ordinary decimals retain JSON round-trip semantics: 0.1 is allowed.
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
        // The SDK does not retry DeserializationError (including mutations).
        // No JSON.rawJSON or JSON.parse reviver context runtime dependency.
        throw new errors.DeserializationError(
          "Elasticsearch response rejected to avoid losing numeric precision. A JSON number cannot round-trip safely through this client's numeric representation.",
          json,
        );
      }
    }
    return parsed;
  }
}
