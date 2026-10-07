/** Redact diagnostics only; never use these copies as connection/query inputs. */
const REDACTED = "[REDACTED]";
const SECRET_KEY =
  /^(?:password|passwd|pwd|passphrase|privateKey|keyPassphrase|tlsKeyPassphrase|sshPassword|sshPrivateKey|sshPassphrase|api[_-]?key|access[_-]?token|auth[_-]?token|authorization|token|secret|client[_-]?secret|(?:aws)?AccessKeyId|(?:aws)?SecretAccessKey|(?:aws)?SessionToken|proxyPassword)$/i;
const URI_SECRET_KEY =
  /^(?:auth|key|sig|proxyUsername|username|user|authMechanismProperties|aws[_-]?(?:access[_-]?key[_-]?id|secret[_-]?access[_-]?key|session[_-]?token))$/i;
const URI_KEY = /^(?:connectionUri|uri|endpoint|awsEndpoint)$/i;
const MAX_DEPTH = 40;
const MAX_OBJECTS = 1000;
const UNINSPECTABLE = "[Uninspectable diagnostic]";

interface Secrets {
  values: Set<string>;
  uris: Map<string, string>;
}

function isSecretKey(key: string, uri = false): boolean {
  try {
    key = decodeURIComponent(key.replace(/\+/g, " "));
  } catch {
    // Malformed escaping must not disable recognition of ordinary keys.
  }
  return SECRET_KEY.test(key) || (uri && URI_SECRET_KEY.test(key));
}

function collectSecrets(
  value: unknown,
  secrets: Secrets,
  seen = new Set<object>(),
  depth = 0,
): void {
  if (!value || typeof value !== "object" || seen.has(value)) return;
  if (depth > MAX_DEPTH || seen.size >= MAX_OBJECTS) return;
  seen.add(value);
  function collectField(key: unknown, item: unknown): void {
    if (typeof key !== "string") {
      collectSecrets(key, secrets, seen, depth + 1);
      collectSecrets(item, secrets, seen, depth + 1);
      return;
    }
    if (isSecretKey(key) && typeof item === "string" && item.length > 0) {
      secrets.values.add(item);
    } else if (URI_KEY.test(key) && typeof item === "string") {
      const safe = redactUri(item);
      if (safe !== item) secrets.uris.set(item, safe);
      // URI-only configurations have no separate password field. Collect
      // userinfo passwords too, including Mongo's comma-separated hosts.
      // Stop at authority delimiters so a query email is not userinfo.
      const authority = /^[a-z][a-z\d+.-]*:\/\/([^/?#]*)/i.exec(item)?.[1];
      if (authority !== undefined) {
        const at = authority.lastIndexOf("@");
        const userinfo = authority.slice(0, at);
        const colon = userinfo.indexOf(":");
        if (at >= 0 && colon >= 0) {
          const password = userinfo.slice(colon + 1);
          if (password) {
            secrets.values.add(password);
            try {
              secrets.values.add(decodeURIComponent(password));
            } catch {}
          }
        }
      }
      // Also hide a known URI's query secret when upstream reports it alone.
      for (const match of item.matchAll(/[?&#;]([^=&#;\s]+)=([^&#;]*)/g)) {
        if (!isSecretKey(match[1], true) || !match[2]) continue;
        secrets.values.add(match[2]);
        try {
          secrets.values.add(decodeURIComponent(match[2]));
        } catch {}
      }
    } else {
      collectSecrets(item, secrets, seen, depth + 1);
    }
  }
  try {
    if (value instanceof Map) {
      let entries = 0;
      for (const [key, item] of Map.prototype.entries.call(value)) {
        if (++entries > MAX_OBJECTS) break;
        collectField(key, item);
        if (seen.size >= MAX_OBJECTS) break;
      }
    } else if (value instanceof Set) {
      let entries = 0;
      for (const item of Set.prototype.values.call(value)) {
        if (++entries > MAX_OBJECTS) break;
        collectSecrets(item, secrets, seen, depth + 1);
        if (seen.size >= MAX_OBJECTS) break;
      }
    }
    for (const [key, descriptor] of Object.entries(
      Object.getOwnPropertyDescriptors(value),
    )) {
      if ("value" in descriptor) collectField(key, descriptor.value);
    }
  } catch {
    // Driver metadata can contain revoked proxies or hostile ownKeys traps.
    // Secret collection is best-effort; visit() replaces unreadable objects.
  }
}

function redactUri(uri: string): string {
  const scheme = /^[a-z][a-z\d+.-]*:\/\//i.exec(uri);
  if (!scheme) return uri;
  const rest = uri.slice(scheme[0].length);
  const boundary = rest.search(/[/?#\s"'<>`]/);
  const authorityEnd = boundary < 0 ? rest.length : boundary;
  let at = rest.lastIndexOf("@", authorityEnd);
  // Malformed userinfo can contain slashes, quotes or spaces. Only extend
  // beyond the authority for a credential-shaped prefix, never host:port
  // followed by unrelated diagnostic prose containing an email address.
  const colon = rest.indexOf(":");
  if (at < 0 && colon >= 0 && colon < authorityEnd && !rest.startsWith("[")) {
    const passwordPrefix = rest.slice(colon + 1, authorityEnd);
    // A numeric prefix followed by a quote and a contiguous userinfo suffix
    // ending in @ is a malformed password, not a host port. Do not look past
    // whitespace or a path/query delimiter into unrelated error prose/email.
    const quotedUserinfo = /^["'][^\s/?#<>`]*@/.test(rest.slice(authorityEnd));
    if (!/^\d+$/.test(passwordPrefix) || quotedUserinfo) {
      at = rest.indexOf("@");
    }
  }
  if (at >= 0) uri = `${scheme[0]}${REDACTED}@${rest.slice(at + 1)}`;
  // Whitespace and quotes are not reliable value terminators in a malformed
  // URI. The enclosing URI span, rather than a token regex, bounds the value.
  return uri.replace(
    /([?&#;])([^=&#;\s]+)=([^&#;]*)/g,
    (part, separator: string, key: string) =>
      isSecretKey(key, true) ? `${separator}${key}=${REDACTED}` : part,
  );
}

function redactUriSpans(text: string): string {
  const scheme = /\b[a-z][a-z\d+.-]*:\/\//gi;
  let result = "";
  let cursor = 0;
  for (let match = scheme.exec(text); match; match = scheme.exec(text)) {
    const start = match.index;
    const lineBoundary = text.slice(start).search(/[\r\n]/);
    const lineEnd = lineBoundary < 0 ? text.length : start + lineBoundary;
    const quote = text[start - 1];
    const closingQuote =
      quote === '"' || quote === "'"
        ? text.lastIndexOf(quote, lineEnd - 1)
        : -1;
    const tokenBoundary = text.slice(start, lineEnd).search(/[\s"'<>`]/);
    let end =
      closingQuote > start
        ? closingQuote
        : tokenBoundary < 0
          ? lineEnd
          : start + tokenBoundary;
    // In unquoted diagnostic text, a quote inside a clearly bounded userinfo
    // segment is not the URI's terminator. Include its @ and following host.
    const quotedPassword =
      /^[a-z][a-z\d+.-]*:\/\/[^\s/?#"'<>`]*:[^\s/?#"'<>`]*["'][^\s/?#<>`]*@/i.exec(
        text.slice(start, lineEnd),
      );
    if (closingQuote <= start && quotedPassword) {
      const hostStart = start + quotedPassword[0].length;
      const hostBoundary = text.slice(hostStart, lineEnd).search(/[\s"'<>`]/);
      end = hostBoundary < 0 ? lineEnd : hostStart + hostBoundary;
    }
    // With no enclosing quote, a credential query value containing malformed
    // whitespace/quotes has no unambiguous end. Hide its remaining line rather
    // than disclosing a suffix. Non-credential URIs retain their token boundary.
    if (
      closingQuote <= start &&
      /[?&#;]([^=&#;\s]+)=/.test(text.slice(start, end))
    ) {
      for (const parameter of text
        .slice(start, end)
        .matchAll(/[?&#;]([^=&#;\s]+)=/g)) {
        if (isSecretKey(parameter[1], true)) end = lineEnd;
      }
    }
    result += text.slice(cursor, start) + redactUri(text.slice(start, end));
    cursor = end;
    scheme.lastIndex = end;
  }
  return result + text.slice(cursor);
}

function redactText(text: string, secrets: Secrets): string {
  for (const [uri, safe] of secrets.uris) {
    text = text.split(uri).join(safe);
    text = text
      .split(JSON.stringify(uri).slice(1, -1))
      .join(JSON.stringify(safe).slice(1, -1));
  }
  text = redactUriSpans(text);
  // Serialized metadata is common in driver messages. Limit this to named
  // credential fields, not arbitrary SQL literals or user data.
  text = text.replace(
    /("([^"\\]+)"\s*:\s*)"(?:\\.|[^"\\])*"/g,
    (part, prefix: string, key: string) =>
      isSecretKey(key) ? `${prefix}"${REDACTED}"` : part,
  );
  for (const secret of [...secrets.values].sort(
    (a, b) => b.length - a.length,
  )) {
    let encoded = secret;
    try {
      encoded = encodeURIComponent(secret);
    } catch {
      // A lone surrogate in a known password must not break error reporting.
    }
    for (const spelling of new Set([
      secret,
      encoded,
      JSON.stringify(secret).slice(1, -1),
    ])) {
      text = text.split(spelling).join(REDACTED);
    }
  }
  return text;
}

/** Known raw secret values are scoped to this call, never retained by a logger. */
export function redactDiagnosticText(
  text: string,
  secretContext?: unknown,
): string {
  const secrets: Secrets = { values: new Set(), uris: new Map() };
  collectSecrets(secretContext, secrets);
  return redactText(text, secrets);
}

/** Copy only when redaction is needed, including non-enumerable Error fields. */
export function redactDiagnosticValue(
  value: unknown,
  secretContext?: unknown,
): unknown {
  const secrets: Secrets = { values: new Set(), uris: new Map() };
  collectSecrets(secretContext, secrets);
  collectSecrets(value, secrets);
  let changed = false;
  const seen = new Map<object, unknown>();
  function visit(item: unknown, depth = 0): unknown {
    if (typeof item === "string") {
      const safe = redactText(item, secrets);
      if (safe !== item) changed = true;
      return safe;
    }
    if (!item || typeof item !== "object") return item;
    if (seen.has(item)) return seen.get(item);
    if (depth > MAX_DEPTH || seen.size >= MAX_OBJECTS) {
      changed = true;
      return UNINSPECTABLE;
    }
    try {
      const copy =
        item instanceof Error
          ? new Error()
          : item instanceof Map
            ? new Map()
            : item instanceof Set
              ? new Set()
              : Array.isArray(item)
                ? []
                : {};
      seen.set(item, copy);
      function field(key: unknown, value: unknown): unknown {
        if (typeof key === "string" && isSecretKey(key) && value != null) {
          if (value !== REDACTED) changed = true;
          return REDACTED;
        }
        return visit(value, depth + 1);
      }
      if (copy instanceof Map) {
        let entries = 0;
        for (const [key, value] of Map.prototype.entries.call(item)) {
          if (++entries > MAX_OBJECTS) {
            copy.set(UNINSPECTABLE, UNINSPECTABLE);
            changed = true;
            break;
          }
          copy.set(visit(key, depth + 1), field(key, value));
        }
      } else if (copy instanceof Set) {
        let entries = 0;
        for (const value of Set.prototype.values.call(item)) {
          if (++entries > MAX_OBJECTS) {
            copy.add(UNINSPECTABLE);
            changed = true;
            break;
          }
          copy.add(visit(value, depth + 1));
        }
      } else if (copy instanceof Error) {
        try {
          copy.name = String(visit((item as Error).name, depth + 1));
          // DOMException exposes its message on the prototype, not as an own
          // data property. Preserve and redact it before copying descriptors.
          copy.message = String(visit((item as Error).message, depth + 1));
        } catch {
          copy.name = "Error";
          changed = true;
        }
      }
      for (const [key, descriptor] of Object.entries(
        Object.getOwnPropertyDescriptors(item),
      )) {
        if (!("value" in descriptor)) continue;
        Object.defineProperty(copy, key, {
          ...descriptor,
          value: field(key, descriptor.value),
        });
      }
      return copy;
    } catch {
      changed = true;
      seen.set(item, UNINSPECTABLE);
      return UNINSPECTABLE;
    }
  }
  const safe = visit(value);
  return changed ? safe : value;
}
