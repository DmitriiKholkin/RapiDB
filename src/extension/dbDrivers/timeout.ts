import type { OperationCancellationContext } from "../../shared/safetyContracts";
import type { TransactionContext } from "./types";

export const CONNECTION_TIMEOUT_SECONDS_DEFAULT = 15;
export const DB_OPERATION_TIMEOUT_SECONDS_DEFAULT = 180;

const MIN_TIMEOUT_SECONDS = 1;
const MAX_TIMEOUT_SECONDS = 86400;

export interface DriverTimeoutSettingsSnapshot {
  connectionTimeoutSeconds: number;
  dbOperationTimeoutSeconds: number;
  connectionTimeoutMs: number;
  dbOperationTimeoutMs: number;
}

export type DriverTimeoutSettingsProvider = () => DriverTimeoutSettingsSnapshot;

export type DriverTimeoutKind = "connection" | "dbOperation";

export class DriverTimeoutError extends Error {
  readonly timeoutKind: DriverTimeoutKind;
  readonly operationName: string;
  readonly timeoutMs: number;

  constructor(
    timeoutKind: DriverTimeoutKind,
    operationName: string,
    timeoutMs: number,
  ) {
    const timeoutSeconds = Math.max(1, Math.round(timeoutMs / 1000));
    const operationLabel =
      timeoutKind === "connection"
        ? "Database connection"
        : "Database operation";
    super(
      `${operationLabel} timed out after ${timeoutSeconds} second(s) while running ${operationName}.${operationName === "runTransaction" ? " Transaction outcome may be unknown. Refresh and verify the data before retrying." : ""}`,
    );
    this.name = "DriverTimeoutError";
    this.timeoutKind = timeoutKind;
    this.operationName = operationName;
    this.timeoutMs = timeoutMs;
  }
}

interface TimeoutAwareDriverHooks {
  disconnect?(): void | Promise<void>;
  cancelCurrentOperation?(
    context: OperationCancellationContext,
  ): void | Promise<void>;
}

const TIMEOUT_CLEANUP_BUDGET_MS = 1_000;
let nextInternalQueryRequestToken = 0;

export function throwIfTransactionCancelled(
  context?: TransactionContext,
): void {
  context?.signal.throwIfAborted();
  if (context && Date.now() >= context.deadline) {
    throw new Error(
      "Transaction deadline exceeded. Refresh and verify the data before retrying.",
    );
  }
}

function runBoundedCleanup(cleanup: () => void | Promise<void>): void {
  void Promise.race([
    Promise.resolve().then(cleanup),
    new Promise<void>((resolve) => {
      const timer = setTimeout(resolve, TIMEOUT_CLEANUP_BUDGET_MS);
      timer.unref?.();
    }),
  ]).catch(() => undefined);
}

const CONNECT_METHODS = new Set(["connect"]);
const DB_OPERATION_METHODS = new Set([
  "listDatabases",
  "listSchemas",
  "listObjects",
  "describeTable",
  "describeColumns",
  "getIndexes",
  "getForeignKeys",
  "getConstraints",
  "getTriggers",
  "getConstraintDDL",
  "getIndexDDL",
  "getTriggerDDL",
  "getCreateTableDDL",
  "getObjectDefinition",
  "getRoutineDefinition",
  "query",
  "readTablePage",
  "updateRows",
  "insertRow",
  "deleteRows",
  "runTransaction",
  "getMutationAtomicityRisk",
]);

function normalizeTimeoutSeconds(
  value: number | undefined,
  fallback: number,
): number {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return fallback;
  }

  return Math.max(
    MIN_TIMEOUT_SECONDS,
    Math.min(MAX_TIMEOUT_SECONDS, Math.round(value)),
  );
}

export function createDriverTimeoutSettingsSnapshot(input?: {
  connectionTimeoutSeconds?: number;
  dbOperationTimeoutSeconds?: number;
}): DriverTimeoutSettingsSnapshot {
  const connectionTimeoutSeconds = normalizeTimeoutSeconds(
    input?.connectionTimeoutSeconds,
    CONNECTION_TIMEOUT_SECONDS_DEFAULT,
  );
  const dbOperationTimeoutSeconds = normalizeTimeoutSeconds(
    input?.dbOperationTimeoutSeconds,
    DB_OPERATION_TIMEOUT_SECONDS_DEFAULT,
  );

  return {
    connectionTimeoutSeconds,
    dbOperationTimeoutSeconds,
    connectionTimeoutMs: connectionTimeoutSeconds * 1000,
    dbOperationTimeoutMs: dbOperationTimeoutSeconds * 1000,
  };
}

export function getDefaultDriverTimeoutSettings(): DriverTimeoutSettingsSnapshot {
  return createDriverTimeoutSettingsSnapshot();
}

function resolveTimeoutKind(property: string): DriverTimeoutKind | null {
  if (CONNECT_METHODS.has(property)) {
    return "connection";
  }

  if (DB_OPERATION_METHODS.has(property)) {
    return "dbOperation";
  }

  return null;
}

function timeoutMsForKind(
  provider: DriverTimeoutSettingsProvider,
  timeoutKind: DriverTimeoutKind,
): number {
  const settings = provider();
  return timeoutKind === "connection"
    ? settings.connectionTimeoutMs
    : settings.dbOperationTimeoutMs;
}

async function withDriverTimeout<T>(
  promiseFactory: () => Promise<T>,
  options: {
    timeoutKind: DriverTimeoutKind;
    operationName: string;
    timeoutSettingsProvider: DriverTimeoutSettingsProvider;
    onDeadline?: () => void;
    onTimeout?: () => void | Promise<void>;
    onLateSettlementAfterTimeout?: () => void | Promise<void>;
  },
): Promise<T> {
  const timeoutMs = timeoutMsForKind(
    options.timeoutSettingsProvider,
    options.timeoutKind,
  );

  if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) {
    return promiseFactory();
  }

  return await new Promise<T>((resolve, reject) => {
    let settled = false;
    let timedOut = false;
    const timer = setTimeout(() => {
      if (settled) {
        return;
      }

      settled = true;
      timedOut = true;
      clearTimeout(timer);
      options.onDeadline?.();
      reject(
        new DriverTimeoutError(
          options.timeoutKind,
          options.operationName,
          timeoutMs,
        ),
      );
      if (options.onTimeout) {
        runBoundedCleanup(options.onTimeout);
      }
    }, timeoutMs);

    let pendingPromise: Promise<T>;
    try {
      pendingPromise = promiseFactory();
    } catch (error) {
      settled = true;
      clearTimeout(timer);
      reject(error);
      return;
    }

    void pendingPromise.then(
      (value) => {
        if (settled) {
          if (timedOut) {
            void Promise.resolve(
              options.onLateSettlementAfterTimeout?.(),
            ).catch(() => undefined);
          }
          return;
        }

        settled = true;
        clearTimeout(timer);
        resolve(value);
      },
      (error) => {
        if (settled) {
          if (timedOut) {
            void Promise.resolve(
              options.onLateSettlementAfterTimeout?.(),
            ).catch(() => undefined);
          }
          return;
        }

        settled = true;
        clearTimeout(timer);
        reject(error);
      },
    );
  });
}

export function createTimeoutAwareDriver<T extends object>(
  driver: T,
  timeoutSettingsProvider: DriverTimeoutSettingsProvider,
): T {
  const wrappedMethods = new Map<string, unknown>();

  return new Proxy(driver, {
    get(target, property, receiver) {
      const value = Reflect.get(target, property, receiver);
      if (typeof property !== "string" || typeof value !== "function") {
        return value;
      }

      const timeoutKind = resolveTimeoutKind(property);
      if (!timeoutKind) {
        return value.bind(target);
      }

      const cached = wrappedMethods.get(property);
      if (cached) {
        return cached;
      }

      const wrapped = (...args: unknown[]) => {
        const transactionAbort =
          property === "runTransaction" ? new AbortController() : undefined;
        if (transactionAbort) {
          const timeoutMs = timeoutSettingsProvider().dbOperationTimeoutMs;
          const supplied = args[1] as TransactionContext | undefined;
          args[1] = {
            signal: supplied
              ? AbortSignal.any([supplied.signal, transactionAbort.signal])
              : transactionAbort.signal,
            deadline: Math.min(
              supplied?.deadline ?? Infinity,
              timeoutMs > 0 ? Date.now() + timeoutMs : Infinity,
            ),
          } satisfies TransactionContext;
        }
        if (property === "query") {
          const operationContext =
            typeof args[2] === "object" && args[2] !== null
              ? (args[2] as { requestToken?: number })
              : {};
          if (operationContext.requestToken === undefined) {
            operationContext.requestToken = --nextInternalQueryRequestToken;
          }
          args[2] = operationContext;
        }
        return withDriverTimeout(
          () => Reflect.apply(value, target, args) as Promise<unknown>,
          {
            timeoutKind,
            operationName: property,
            timeoutSettingsProvider,
            onDeadline: () =>
              transactionAbort?.abort(
                new Error(
                  "Transaction cancelled after timeout; its outcome may be unknown. Refresh before retrying.",
                ),
              ),
            onTimeout: () => {
              const timeoutHooks = target as TimeoutAwareDriverHooks;
              const operationContext =
                property === "query" &&
                typeof args[2] === "object" &&
                args[2] !== null
                  ? (args[2] as { requestToken?: number })
                  : undefined;
              const timeoutContext: OperationCancellationContext = {
                reason: "timeout",
                timeoutKind,
                operationName: property,
                requestToken: operationContext?.requestToken,
              };

              if (
                property === "query" &&
                typeof timeoutHooks.cancelCurrentOperation === "function"
              ) {
                return timeoutHooks.cancelCurrentOperation(timeoutContext);
              }
              if (
                property === "connect" &&
                typeof timeoutHooks.disconnect === "function"
              ) {
                return timeoutHooks.disconnect();
              }
            },
            onLateSettlementAfterTimeout: () => {
              const timeoutHooks = target as TimeoutAwareDriverHooks;
              if (
                property === "connect" &&
                typeof timeoutHooks.disconnect === "function"
              ) {
                return timeoutHooks.disconnect();
              }
            },
          },
        );
      };

      wrappedMethods.set(property, wrapped);
      return wrapped;
    },
  });
}
