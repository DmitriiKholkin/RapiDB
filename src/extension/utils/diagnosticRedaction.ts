const REDACTED = "[REDACTED]";
const SECRET_KEY =
  /^(?:password|passwd|pwd|passphrase|tlsCertificateKeyFilePassword|privateKey|keyPassphrase|tlsKeyPassphrase|sshPassword|sshPrivateKey|sshPassphrase|api[_-]?key|access[_-]?token|auth[_-]?token|authorization|token|secret|client[_-]?secret|(?:aws)?AccessKeyId|(?:aws)?SecretAccessKey|(?:aws)?SessionToken|proxyPassword)$/i;
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
  } catch {}
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
  } catch {}
}

function redactUri(uri: string): string {
  const scheme = /^[a-z][a-z\d+.-]*:\/\//i.exec(uri);
  if (!scheme) return uri;
  const rest = uri.slice(scheme[0].length);
  const boundary = rest.search(/[/?#\s"'<>`]/);
  const authorityEnd = boundary < 0 ? rest.length : boundary;
  let at = rest.lastIndexOf("@", authorityEnd);
  const colon = rest.indexOf(":");
  if (at < 0 && colon >= 0 && colon < authorityEnd && !rest.startsWith("[")) {
    const passwordPrefix = rest.slice(colon + 1, authorityEnd);
    const quotedUserinfo = /^["'][^\s/?#<>`]*@/.test(rest.slice(authorityEnd));
    if (!/^\d+$/.test(passwordPrefix) || quotedUserinfo) {
      at = rest.indexOf("@");
    }
  }
  if (at >= 0) uri = `${scheme[0]}${REDACTED}@${rest.slice(at + 1)}`;
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
    const quotedPassword =
      /^[a-z][a-z\d+.-]*:\/\/[^\s/?#"'<>`]*:[^\s/?#"'<>`]*["'][^\s/?#<>`]*@/i.exec(
        text.slice(start, lineEnd),
      );
    if (closingQuote <= start && quotedPassword) {
      const hostStart = start + quotedPassword[0].length;
      const hostBoundary = text.slice(hostStart, lineEnd).search(/[\s"'<>`]/);
      end = hostBoundary < 0 ? lineEnd : hostStart + hostBoundary;
    }
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
    } catch {}
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

export function redactDiagnosticText(
  text: string,
  secretContext?: unknown,
): string {
  const secrets: Secrets = { values: new Set(), uris: new Map() };
  collectSecrets(secretContext, secrets);
  return redactText(text, secrets);
}

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
