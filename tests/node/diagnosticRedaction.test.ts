import { inspect } from "node:util";
import { describe, expect, it, vi } from "vitest";
import {
  redactDiagnosticText,
  redactDiagnosticValue,
} from "../../src/extension/utils/diagnosticRedaction";
import {
  logErrorWithContext,
  normalizeUnknownError,
} from "../../src/extension/utils/errorHandling";
import { installLoggerSink, logger } from "../../src/extension/utils/logger";
import {
  MONGO_ERROR_SECRET,
  MONGO_ERROR_URI,
  MONGO_NUMERIC_PASSWORD_ERROR_URIS,
  MONGO_QUERY_ERROR_URIS,
  mongoCredentialError,
} from "../support/mongoCredentialError";

describe("diagnostic credential redaction", () => {
  it("preserves and redacts inherited DOMException messages without mutating the original", () => {
    const upstream = new DOMException(
      "Credential inherited-secret rejected",
      "AbortError",
    );
    const safe = normalizeUnknownError(upstream, {
      password: "inherited-secret",
    });
    expect(safe.message).toBe("Credential [REDACTED] rejected");
    expect(upstream.message).toBe("Credential inherited-secret rejected");
  });

  it.each(
    MONGO_NUMERIC_PASSWORD_ERROR_URIS,
  )("redacts numeric-prefix quote passwords from actual Mongo errors: %s", (uri) => {
    const upstream = mongoCredentialError(uri);
    const originalStack = upstream.stack;
    const consoleError = vi
      .spyOn(console, "error")
      .mockImplementation(() => {});
    const safe = normalizeUnknownError(upstream);
    expect(safe.message).toBe(
      'Protocol and host list are required in "mongodb://[REDACTED]@/db"',
    );
    expect(safe.stack).not.toContain("H10_SECRET");
    expect(
      normalizeUnknownError(upstream, { connectionUri: uri }).message,
    ).toBe(safe.message);
    expect(redactDiagnosticText(uri)).toBe("mongodb://[REDACTED]@/db");
    expect(redactDiagnosticText(`Failure: ${uri}; code EINVAL`)).toBe(
      "Failure: mongodb://[REDACTED]@/db; code EINVAL",
    );
    expect(redactDiagnosticText(uri, { connectionUri: uri })).toBe(
      "mongodb://[REDACTED]@/db",
    );
    logErrorWithContext("Mongo numeric password", upstream);
    logger.error("Mongo numeric password", upstream);
    expect(inspect(consoleError.mock.calls)).not.toMatch(/H10_SECRET|user:123/);
    expect(upstream.message).toContain(uri);
    expect(upstream.stack).toBe(originalStack);
  });

  it.each(
    MONGO_QUERY_ERROR_URIS,
  )("redacts complete malformed query secrets from actual Mongo errors: %s", (uri) => {
    const upstream = mongoCredentialError(uri);
    const consoleError = vi
      .spyOn(console, "error")
      .mockImplementation(() => {});
    const safe = normalizeUnknownError(upstream);
    logErrorWithContext("Mongo connect", upstream);
    logger.error("Mongo connect", upstream);
    expect(safe.message).toContain("Protocol and host list are required");
    expect(safe.message).toContain("authMechanismProperties=[REDACTED]");
    expect(inspect(consoleError.mock.calls)).not.toContain("H10_QUERY_SECRET");
    expect(safe.stack).not.toContain("H10_QUERY_SECRET");
    if (uri.includes("retryWrites"))
      expect(safe.message).toContain("retryWrites=true");
    const secretValue = uri.split("authMechanismProperties=")[1].split("&")[0];
    expect(
      redactDiagnosticText(`Rejected credential ${secretValue}`, {
        connectionUri: uri,
      }),
    ).not.toContain("H10_QUERY_SECRET");
    expect(upstream.message).toContain(uri);
  });

  it("does not confuse a host port and a subsequent email with URI userinfo", () => {
    const message =
      "URL https://host:443/docs failed: ECONNREFUSED; contact admin@example.com";
    expect(redactDiagnosticText(message)).toBe(message);
    expect(normalizeUnknownError(new Error(message)).message).toBe(message);
    const endpoint = "https://host:443/docs?contact=admin@example.com";
    expect(redactDiagnosticText(endpoint, { endpoint })).toBe(endpoint);
    expect(
      redactDiagnosticText(
        'URL "https://host:443/docs" failed: ECONNREFUSED; contact admin@example.com',
      ),
    ).toBe(
      'URL "https://host:443/docs" failed: ECONNREFUSED; contact admin@example.com',
    );
  });

  it.each([
    'URL "https://host:443" failed: ECONNREFUSED; contact admin@example.com',
    "URL 'https://host:443' failed: ECONNREFUSED; contact admin@example.com",
    'URL "https://host:443/docs failed: ECONNREFUSED; contact admin@example.com"',
    "URL 'https://host:443/docs failed: ECONNREFUSED; contact admin@example.com'",
    'URL "https://host:443/docs?contact=admin@example.com" failed: ECONNREFUSED',
  ])("preserves port diagnostics rather than interpreting a later email as a password: %s", (message) => {
    expect(redactDiagnosticText(message)).toBe(message);
    const original = new Error(message);
    const safe = normalizeUnknownError(original);
    expect(safe).toBe(original);
    const consoleError = vi
      .spyOn(console, "error")
      .mockImplementation(() => {});
    logger.error("Port error", original);
    expect(consoleError.mock.calls[0]?.[1]).toBe(original);
  });

  it.each([
    "revoked",
    "ownKeys",
  ])("handles %s proxies in otherwise usable errors and secret contexts", (kind) => {
    const revocable = Proxy.revocable({}, {});
    revocable.revoke();
    const proxy =
      kind === "revoked"
        ? revocable.proxy
        : new Proxy(
            {},
            {
              ownKeys() {
                throw new Error("metadata trap");
              },
            },
          );
    const upstream = Object.assign(mongoCredentialError(), { context: proxy });
    const consoleError = vi
      .spyOn(console, "error")
      .mockImplementation(() => {});
    expect(() => redactDiagnosticText(MONGO_ERROR_URI, proxy)).not.toThrow();
    expect(() => redactDiagnosticValue(upstream, proxy)).not.toThrow();
    expect(() => logErrorWithContext("Mongo connect", upstream)).not.toThrow();
    expect(() => logger.error("Mongo connect", upstream)).not.toThrow();
    const safe = normalizeUnknownError(upstream, proxy) as Error & {
      context: unknown;
    };
    expect(safe.message).toContain("Protocol and host list are required");
    expect(safe.context).toBe("[Uninspectable diagnostic]");
    expect(inspect(consoleError.mock.calls)).not.toContain(MONGO_ERROR_SECRET);
    expect(upstream.context).toBe(proxy);
    expect(upstream.message).toContain(MONGO_ERROR_SECRET);
  });

  it("redacts Map/Set nested errors, keys, known metadata and cycles before real console output", () => {
    const nested = mongoCredentialError();
    const reason = new Map<unknown, unknown>([
      ["server", nested],
      [MONGO_ERROR_URI, "URI key"],
      ["password", "H10_MAP_SECRET"],
    ]);
    const contexts = new Set<unknown>([reason, nested]);
    reason.set("cycle", contexts);
    const upstream = Object.assign(new Error("Outer ECONNREFUSED"), {
      reason,
      contexts,
    });
    const consoleError = vi
      .spyOn(console, "error")
      .mockImplementation(() => {});
    const safe = logger.error(
      "Mongo nested failure",
      upstream,
    ) as typeof upstream;
    logErrorWithContext("Mongo nested failure", upstream);
    expect(inspect(consoleError.mock.calls, { depth: 20 })).not.toMatch(
      /H10_MONGO_SENTINEL|h10-user|H10_MAP_SECRET/,
    );
    expect(safe.message).toBe("Outer ECONNREFUSED");
    expect(safe.reason).toBeInstanceOf(Map);
    expect(safe.contexts).toBeInstanceOf(Set);
    expect((safe.reason.get("server") as Error).message).toContain(
      "Protocol and host list are required",
    );
    expect(safe.reason.get("password")).toBe("[REDACTED]");
    expect(safe.reason.get("cycle")).toBe(safe.contexts);
    expect(safe.contexts.has(safe.reason)).toBe(true);
    expect(reason.get("server")).toBe(nested);
    expect(reason.get("password")).toBe("H10_MAP_SECRET");
    expect(nested.message).toContain(MONGO_ERROR_SECRET);
  });

  it("bounds deeply nested metadata without throwing or returning raw deep secrets", () => {
    let context: unknown = { password: "H10_DEEP_SECRET" };
    for (let i = 0; i < 100; i++) context = { next: context };
    const upstream = Object.assign(new Error("Depth EINVAL"), { context });
    expect(() => normalizeUnknownError(upstream)).not.toThrow();
    const safe = normalizeUnknownError(upstream);
    expect(inspect(safe, { depth: null })).not.toContain("H10_DEEP_SECRET");
    expect(safe.message).toBe("Depth EINVAL");
  });

  it.each([
    "p'ass",
    'p"ass',
  ])("redacts the actual malformed quote-password parser error: %s", (password) => {
    const upstream = mongoCredentialError(`mongodb://user:${password}@/db`);
    const safe = normalizeUnknownError(upstream);
    expect(safe.message).toBe(
      'Protocol and host list are required in "mongodb://[REDACTED]@/db"',
    );
    expect(safe.stack).not.toContain(password);
    expect(upstream.message).toContain(password);
  });

  it("redacts an actual upstream Mongo URI error at both console boundaries without changing it", () => {
    const upstream = mongoCredentialError();
    const originalStack = upstream.stack;
    const consoleError = vi
      .spyOn(console, "error")
      .mockImplementation(() => {});
    for (const logged of [
      logErrorWithContext("Connect", upstream),
      logger.error("Connect", upstream),
    ]) {
      expect(logged.message).toContain("Protocol and host list are required");
      expect(logged.message).toContain("mongodb://[REDACTED]@/db");
      expect(inspect(logged)).not.toContain(MONGO_ERROR_SECRET);
      expect(logged).not.toBe(upstream);
    }
    expect(inspect(consoleError.mock.calls)).not.toContain(MONGO_ERROR_SECRET);
    expect(inspect(consoleError.mock.calls)).not.toContain("h10-user");
    expect(upstream.message).toContain(MONGO_ERROR_URI);
    expect(upstream.stack).toBe(originalStack);
  });

  it.each([
    "mongodb://user:p%40ss@[broken/db",
    "mongodb://user:raw/pass?word#tail@host/db",
    "mongodb://user:raw/pass word@host/db",
    "redis://user:raw password@host:6379/0",
    "mongodb://user:pass@",
    "postgres://user:pass@host/db?password=p%40ss&auth=AUTH_SENTINEL&authSource=admin&retryWrites=true",
    "https://host/db?api%4Bey=KEY_SENTINEL&proxyPassword=PROXY_SENTINEL&client_secret=CLIENT_SENTINEL#token=TOKEN_SENTINEL",
  ])("redacts malformed/encoded credential URIs: %s", (uri) => {
    const redacted = redactDiagnosticText(`Failure: "${uri}"; code EINVAL`);
    expect(redacted).not.toMatch(
      /user:|p%40ss|raw password|raw\/pass|AUTH_SENTINEL|KEY_SENTINEL|PROXY_SENTINEL|CLIENT_SENTINEL|TOKEN_SENTINEL/,
    );
    expect(redacted).toContain("code EINVAL");
    expect(redacted).toContain("[REDACTED]");
    if (uri.includes("authSource"))
      expect(redacted).toContain("authSource=admin&retryWrites=true");
    expect(redactDiagnosticText(redacted)).toBe(redacted);
  });

  it.each([
    ["connectionUri", "mongodb://user:s3cr3t@host/db", "s3cr3t"],
    ["uri", "redis://:p%40ss@host:6379/0", "p@ss"],
    ["endpoint", "https://user:p%40ss@host", "p@ss"],
    ["connectionUri", "mongodb://user:p%40ss@host1,host2/db", "p@ss"],
    ["connectionUri", "mongodb://user:bad%escape@host/db", "bad%escape"],
  ])("redacts separately reported userinfo passwords from %s: %s", (key, uri, password) => {
    const encoded = encodeURIComponent(password);
    const context = { [key]: uri };
    const original = Object.assign(
      new Error(`Authentication failed: ${password}; encoded: ${encoded}`),
      { cause: new Error(`Rejected credential ${password}`) },
    );
    const originalStack = original.stack;
    const safe = normalizeUnknownError(original, context) as typeof original;

    expect(safe.message).toBe(
      "Authentication failed: [REDACTED]; encoded: [REDACTED]",
    );
    expect(safe.cause.message).toBe("Rejected credential [REDACTED]");
    expect(safe.stack).not.toContain(password);
    expect(safe.stack).not.toContain(encoded);
    expect(redactDiagnosticText(password, context)).toBe("[REDACTED]");
    expect(original.message).toContain(password);
    expect(original.cause.message).toContain(password);
    expect(original.stack).toBe(originalStack);
    expect(context[key]).toBe(uri);
  });

  it.each([
    "mongodb://user@host/db",
    "redis://user:@host:6379/0",
    "https://host:443/docs?contact=admin@example.com",
  ])("does not collect usernames, empty passwords or host ports as passwords: %s", (uri) => {
    expect(
      redactDiagnosticText("user; 443; admin@example.com", {
        connectionUri: uri,
      }),
    ).toBe("user; 443; admin@example.com");
  });

  it("handles nested errors, cycles, stacks and known secret metadata without modifying inputs", () => {
    const nested = Object.assign(new Error("Denied RAW_SENTINEL"), {
      password: "RAW_SENTINEL",
      code: "EACCES",
    });
    const error = Object.assign(new Error("outer failure"), {
      cause: nested,
      context: { apiKey: "KEY_SENTINEL", uri: MONGO_ERROR_URI },
    });
    Object.assign(nested, { originalError: error });
    const safe = redactDiagnosticValue(error) as Error & {
      cause: Error & { code: string };
    };
    expect(inspect(safe, { depth: 10 })).not.toMatch(
      /RAW_SENTINEL|KEY_SENTINEL|H10_MONGO_SENTINEL|h10-user/,
    );
    expect(safe.cause.code).toBe("EACCES");
    expect(nested.message).toBe("Denied RAW_SENTINEL");
    expect(
      redactDiagnosticText(
        JSON.stringify({
          detail: {
            password: "JSON_SENTINEL",
            reason: "bad login",
            connectionUri: MONGO_ERROR_URI,
          },
        }),
      ),
    ).not.toMatch(/JSON_SENTINEL|H10_MONGO_SENTINEL/);
  });

  it("uses only supplied known config secrets for free text and preserves ordinary SQL diagnostics", () => {
    const original = new Error(
      "syntax error near 'password'; SELECT 'ordinary data'",
    );
    expect(normalizeUnknownError(original)).toBe(original);
    expect(
      redactDiagnosticText(
        "https://host/docs failed; contact admin@example.com",
      ),
    ).toBe("https://host/docs failed; contact admin@example.com");
    const error = new Error(
      "TLS failure RAW_SENTINEL and encoded p%40ss: ECONNRESET",
    );
    const safe = normalizeUnknownError(error, {
      ssh: { password: "RAW_SENTINEL" },
      tls: { keyPassphrase: "p@ss" },
    });
    expect(safe.message).toBe(
      "TLS failure [REDACTED] and encoded [REDACTED]: ECONNRESET",
    );
    expect(error.message).toContain("RAW_SENTINEL");
  });

  it("sanitizes messages before delivery to any logger sink", () => {
    const sink = vi.fn();
    const dispose = installLoggerSink(sink);
    try {
      logger.debug(MONGO_ERROR_URI);
      logger.info(MONGO_ERROR_URI);
      logger.warn(MONGO_ERROR_URI);
      logger.error(MONGO_ERROR_URI, mongoCredentialError());
      expect(inspect(sink.mock.calls)).not.toMatch(
        /H10_MONGO_SENTINEL|h10-user/,
      );
      expect(sink).toHaveBeenCalledTimes(4);
    } finally {
      dispose();
    }
  });
});
