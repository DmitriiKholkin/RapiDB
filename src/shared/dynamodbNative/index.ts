/**
 * `dynamodbNative` — public entry point. Re-exports the public API.
 */

export {
  inferDynamoDbNativeOperationName,
  looksLikeLegacyDynamoPartiql,
  normalizeDynamoDbNativeOperationName,
} from "./operation";
export {
  parseDynamoDbNativeQueryInput,
  parseDynamoDbNativeQueryInputs,
  splitConcatenatedJsonObjects,
} from "./parser";
export * from "./types";
