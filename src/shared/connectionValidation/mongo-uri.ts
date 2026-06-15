/**
 * MongoDB-specific URI heuristics. Pure functions, no I/O.
 */

const MONGO_SRV_SCHEME = "mongodb+srv://";
const URI_SCHEME_SEPARATOR = "://";

/** Returns true if `value` looks like an `mongodb+srv://` URI. */
export function hasMongoSrvUri(value: string | undefined): boolean {
  return typeof value === "string" && value.trim().startsWith(MONGO_SRV_SCHEME);
}

/**
 * Returns true if `value` is a `mongodb://` URI that contains a
 * comma-separated host list. Used to reject multi-host URIs when
 * the driver is being tunneled over SSH.
 */
export function hasMongoMultiHostUri(value: string | undefined): boolean {
  if (typeof value !== "string") {
    return false;
  }

  const normalized = value.trim();
  const schemeIndex = normalized.indexOf(URI_SCHEME_SEPARATOR);
  if (schemeIndex < 0) {
    return false;
  }

  const authorityStart = schemeIndex + URI_SCHEME_SEPARATOR.length;
  const pathStart = normalized.indexOf("/", authorityStart);
  const authority =
    pathStart >= 0
      ? normalized.slice(authorityStart, pathStart)
      : normalized.slice(authorityStart);

  // Strip optional `user:pass@` prefix to inspect hosts only.
  const hosts = authority.includes("@")
    ? authority.slice(authority.lastIndexOf("@") + 1)
    : authority;

  return hosts.includes(",");
}
