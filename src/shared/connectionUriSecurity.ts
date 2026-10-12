export const AMBIGUOUS_URI_CREDENTIALS_MESSAGE =
  "Ambiguous URI credentials. Use a single URI scheme and percent-encode reserved characters and whitespace in URI usernames and passwords.";

/** Recognize nested schemes, network-path references and scheme-less authorities. */
export function splitUriAuthorityPrefix(value: string): {
  prefix: string;
  remainder: string;
} {
  const prefix = value.match(/^(?:[a-z][a-z\d+.-]*:)*\/\//i)?.[0] ?? "";
  return { prefix, remainder: value.slice(prefix.length) };
}

export function hasAmbiguousUriCredentials(value: string | undefined): boolean {
  if (!value) return false;

  const { prefix, remainder } = splitUriAuthorityPrefix(value.trim());
  // A nested scheme may actually contain a scheme-less password with ://.
  // Never preserve those prefix segments as safe metadata when userinfo is possible.
  if (
    prefix.indexOf(":") !== prefix.lastIndexOf(":") &&
    remainder.includes("@")
  ) {
    return true;
  }
  const boundary = remainder.search(/[/?#\s\\]/);
  const lastAt = remainder.lastIndexOf("@");
  if (boundary < 0 || lastAt < boundary) return false;

  // A colon before @ may introduce a password, not a host port or path text.
  // Do not guess which grammar was intended or persist a possible secret.
  return (
    remainder.slice(0, lastAt).includes(":") ||
    /[\s\\]/.test(remainder[boundary])
  );
}
