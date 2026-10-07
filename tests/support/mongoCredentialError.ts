import { MongoClient } from "mongodb";

export const MONGO_ERROR_SECRET = "H10_MONGO_SENTINEL";
export const MONGO_ERROR_URI = `mongodb://h10-user:${MONGO_ERROR_SECRET}@/db`;
export const MONGO_NUMERIC_PASSWORD_ERROR_URIS = [
  'mongodb://user:123"H10_SECRET@/db',
  "mongodb://user:123'H10_SECRET@/db",
] as const;
export const MONGO_QUERY_ERROR_URIS = [
  'mongodb:///db?authMechanismProperties=AWS_SESSION_TOKEN:p"H10_QUERY_SECRET',
  "mongodb:///db?authMechanismProperties=AWS_SESSION_TOKEN:p H10_QUERY_SECRET&retryWrites=true",
] as const;

/** Actual installed upstream parser error; no connection/network is attempted. */
export function mongoCredentialError(uri = MONGO_ERROR_URI): Error {
  try {
    new MongoClient(uri);
  } catch (error) {
    if (error instanceof Error && error.message.includes(uri)) return error;
    throw error;
  }
  throw new Error("MongoDB fixture must fail with a URI-bearing parse error");
}
