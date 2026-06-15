/**
 * Centralized logger for the RapiDB extension.
 *
 * Replaces ad-hoc `console.*` calls so that:
 * - log levels are explicit and grep-able;
 * - the prefix is consistent (`[RapiDB]`);
 * - tests can inject a recorder (see {@link installLoggerSink}).
 *
 * The `error()` variant preserves the original call shape
 * (`console.error("[RapiDB] <context>:", <error>)`) so test mocks and
 * log scrapers that key on the 2-argument form keep working.
 *
 * Code that needs the timestamped, structured log format previously
 * produced by `logErrorWithContext` should call that helper directly —
 * this logger is the lightweight, dependency-free path.
 */

import { normalizeUnknownError } from "./errorHandling";

const PREFIX = "[RapiDB]";

/** Log levels supported by the extension logger. */
export type LogLevel = "debug" | "info" | "warn" | "error";

/** Structured log record delivered to a {@link LogSink}. */
export interface LogRecord {
  level: LogLevel;
  message: string;
  context?: string;
}

/** Consumer of log records (the default sink writes to `console.*`). */
export type LogSink = (record: LogRecord) => void;

/** Default sink writes to `console.*` using the level-appropriate stream. */
const consoleSink: LogSink = (record) => {
  // Format as a single first argument so tests / log scrapers can match
  // the full message text with one `toContain` / `substring` check.
  const formatted = record.context
    ? `${PREFIX} ${record.context}: ${record.message}`
    : `${PREFIX} ${record.message}`;
  switch (record.level) {
    case "debug":
      console.debug(formatted);
      return;
    case "info":
      console.info(formatted);
      return;
    case "warn":
      console.warn(formatted);
      return;
    case "error":
      console.error(formatted);
      return;
  }
};

let activeSink: LogSink = consoleSink;

/**
 * Replace the log sink (typically from tests). Returns a disposer that
 * restores the previous sink so the change is scoped to the caller.
 */
export function installLoggerSink(sink: LogSink): () => void {
  const previous = activeSink;
  activeSink = sink;
  return () => {
    activeSink = previous;
  };
}

function emit(
  level: LogLevel,
  context: string | undefined,
  message: string,
): void {
  activeSink({ level, message, context });
}

/**
 * Thin, opinionated logger. Use this instead of `console.*` everywhere in
 * `src/extension/` so we have a single place to add sinks, redact
 * sensitive data, or wire into the VSCode Output Channel later.
 */
export const logger = {
  debug(message: string): void {
    emit("debug", undefined, message);
  },

  info(message: string): void {
    emit("info", undefined, message);
  },

  warn(message: string): void {
    emit("warn", undefined, message);
  },

  /**
   * Log an error with a context tag and return the normalized Error.
   *
   * Preserves the original 2-argument `console.error` shape:
   *   `console.error("[RapiDB] <context>:", <error>)`
   * so test mocks and downstream log scrapers continue to work.
   */
  error(context: string, error: unknown): Error {
    const normalized = normalizeUnknownError(error);
    // Preserve the legacy 2-arg call shape: tests / log scrapers often
    // inspect the second argument (the Error instance) separately.
    console.error(`${PREFIX} ${context}:`, normalized);
    return normalized;
  },
} as const;
