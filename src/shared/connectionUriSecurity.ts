export const AMBIGUOUS_URI_CREDENTIALS_MESSAGE =
  "Ambiguous URI credentials. Use a single URI scheme and percent-encode reserved characters and whitespace in URI usernames and passwords.";

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
  if (
    prefix.indexOf(":") !== prefix.lastIndexOf(":") &&
    remainder.includes("@")
  ) {
    return true;
  }
  const boundary = remainder.search(/[/?#\s\\]/);
  const lastAt = remainder.lastIndexOf("@");
  if (boundary < 0 || lastAt < boundary) return false;

  return (
    remainder.slice(0, lastAt).includes(":") ||
    /[\s\\]/.test(remainder[boundary])
  );
}
