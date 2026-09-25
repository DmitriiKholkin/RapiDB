export const AMBIGUOUS_URI_CREDENTIALS_MESSAGE =
  "Ambiguous URI credentials. Percent-encode reserved characters and whitespace in URI usernames and passwords.";

export function hasAmbiguousUriCredentials(value: string | undefined): boolean {
  const match = value?.trim().match(/^[a-z][a-z\d+.-]*:\/\/(.*)$/is);
  if (!match) return false;

  const remainder = match[1];
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
