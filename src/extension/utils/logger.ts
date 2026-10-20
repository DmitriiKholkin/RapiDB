import { redactDiagnosticText } from "./diagnosticRedaction";
import { normalizeUnknownError } from "./errorHandling";

const PREFIX = "[RapiDB]";

export type LogLevel = "debug" | "info" | "warn" | "error";

export interface LogRecord {
  level: LogLevel;
  message: string;
  context?: string;
}

export type LogSink = (record: LogRecord) => void;

const consoleSink: LogSink = (record) => {
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
  activeSink({
    level,
    message: redactDiagnosticText(message),
    context: context === undefined ? undefined : redactDiagnosticText(context),
  });
}

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

  error(context: string, error: unknown): Error {
    const normalized = normalizeUnknownError(error);
    const safeContext = redactDiagnosticText(context, error);
    if (activeSink === consoleSink) {
      console.error(`${PREFIX} ${safeContext}:`, normalized);
    } else {
      emit("error", safeContext, normalized.stack ?? normalized.message);
    }
    return normalized;
  },
} as const;
