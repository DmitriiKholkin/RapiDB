import type { OperationCancellationContext } from "../../shared/safetyContracts";
import type {
  DriverMutationResult,
  DriverOperationContext,
  DriverTablePageRequest,
  QueryExecutionOptions,
  TransactionContext,
} from "./types";

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
  deleteResult?: DriverMutationResult;

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
      `${operationLabel} timed out after ${timeoutSeconds} second(s) while running ${operationName}.${["query", "updateRows", "insertRow", "deleteRows", "runTransaction"].includes(operationName) ? " Mutation outcome may be unknown. Refresh and verify the data before retrying." : ""}`,
    );
    this.name = "DriverTimeoutError";
    this.timeoutKind = timeoutKind;
    this.operationName = operationName;
    this.timeoutMs = timeoutMs;
  }
}

interface TimeoutAwareDriverHooks {
  disconnect?(): void | Promise<void>;
  /** Drivers with connect epochs can cancel just the timed-out attempt. */
  cancelConnectionAttempt?(attempt: Promise<unknown>): void | Promise<void>;
  cancelCurrentOperation?(
    context: OperationCancellationContext,
  ): void | Promise<void>;
}

const TIMEOUT_CLEANUP_BUDGET_MS = 1_000;
let nextInternalQueryRequestToken = 0;

export function throwIfQueryCancelled(context?: QueryExecutionOptions): void {
  context?.signal?.throwIfAborted();
  if (context?.deadline !== undefined && Date.now() >= context.deadline) {
    throw new Error(
      "Query deadline exceeded. Mutation outcome may be unknown. Refresh and verify the data before retrying.",
    );
  }
}

export function getQueryRemainingTimeoutMs(
  context?: QueryExecutionOptions,
): number | undefined {
  throwIfQueryCancelled(context);
  return context?.deadline !== undefined && Number.isFinite(context.deadline)
    ? Math.max(1, Math.ceil(context.deadline - Date.now()))
    : undefined;
}

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
const CANCELLABLE_MUTATION_METHODS = new Set([
  "updateRows",
  "insertRow",
  "deleteRows",
  "runTransaction",
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
    deadline?: number;
    onDeadline?: (error: DriverTimeoutError) => void;
    decorateTimeoutError?: (error: DriverTimeoutError) => void;
    onTimeout?: () => void | Promise<void>;
    onLateSettlementAfterTimeout?: () => void | Promise<void>;
  },
): Promise<T> {
  const timeoutMs =
    options.deadline !== undefined && Number.isFinite(options.deadline)
      ? Math.max(0, options.deadline - Date.now())
      : timeoutMsForKind(options.timeoutSettingsProvider, options.timeoutKind);

  const makeTimeoutError = () => {
    const error = new DriverTimeoutError(
      options.timeoutKind,
      options.operationName,
      timeoutMs,
    );
    options.decorateTimeoutError?.(error);
    return error;
  };
  if (options.deadline !== undefined && Date.now() >= options.deadline) {
    const error = makeTimeoutError();
    options.onDeadline?.(error);
    throw error;
  }

  if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) {
    return promiseFactory();
  }

  return await new Promise<T>((resolve, reject) => {
    let settled = false;
    let timedOut = false;
    const expire = () => {
      if (settled) {
        return;
      }

      settled = true;
      timedOut = true;
      clearTimeout(timer);
      const error = makeTimeoutError();
      options.onDeadline?.(error);
      reject(error);
      if (options.onTimeout) {
        runBoundedCleanup(options.onTimeout);
      }
    };
    const timer = setTimeout(expire, timeoutMs);

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
        // Promise microtasks can run before an overdue timer after expensive
        // synchronous result formatting. Never turn that race into success.
        if (options.deadline !== undefined && Date.now() >= options.deadline) {
          expire();
        }
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
        // SDK deadlines can reject just before our timer callback. Preserve
        // the same timeout/unknown-mutation outcome in either ordering.
        if (options.deadline !== undefined && Date.now() >= options.deadline) {
          expire();
        }
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
  // SQLite's process client owns deadlines and waits for native execution to
  // stop before settling, including a user-facing lost-session outcome.
  if (Reflect.get(driver, "driverTimeoutsManagedInternally") === true)
    return driver;
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
        let pendingOperation: Promise<unknown> | undefined;
        let deleteProgress: DriverMutationResult | undefined;
        const operationAbort =
          CANCELLABLE_MUTATION_METHODS.has(property) ||
          property === "readTablePage" ||
          property === "query"
            ? new AbortController()
            : undefined;
        if (operationAbort && property === "query") {
          const supplied = args[2] as QueryExecutionOptions | undefined;
          const timeoutMs = timeoutSettingsProvider().dbOperationTimeoutMs;
          args[2] = {
            ...supplied,
            requestToken:
              supplied?.requestToken ?? --nextInternalQueryRequestToken,
            signal: supplied?.signal
              ? AbortSignal.any([supplied.signal, operationAbort.signal])
              : operationAbort.signal,
            deadline: Math.min(
              supplied?.deadline ?? Infinity,
              Number.isFinite(timeoutMs) && timeoutMs > 0
                ? Date.now() + timeoutMs
                : Infinity,
            ),
          } satisfies QueryExecutionOptions;
        } else if (operationAbort && property === "readTablePage") {
          const request = args[0] as DriverTablePageRequest;
          args[0] = {
            ...request,
            signal: request.signal
              ? AbortSignal.any([request.signal, operationAbort.signal])
              : operationAbort.signal,
            deadline: Math.min(
              request.deadline ?? Infinity,
              Date.now() + timeoutSettingsProvider().dbOperationTimeoutMs,
            ),
          } satisfies DriverTablePageRequest;
        } else if (operationAbort) {
          const timeoutMs = timeoutSettingsProvider().dbOperationTimeoutMs;
          const supplied = args[1] as DriverOperationContext | undefined;
          args[1] = {
            ...supplied,
            signal: supplied
              ? AbortSignal.any([supplied.signal, operationAbort.signal])
              : operationAbort.signal,
            deadline: Math.min(
              supplied?.deadline ?? Infinity,
              timeoutMs > 0 ? Date.now() + timeoutMs : Infinity,
            ),
            ...(property === "deleteRows"
              ? {
                  onDeleteProgress: (result: DriverMutationResult) => {
                    deleteProgress = result;
                    supplied?.onDeleteProgress?.(result);
                  },
                }
              : {}),
          } satisfies DriverOperationContext;
        }
        return withDriverTimeout(
          () => {
            pendingOperation = Reflect.apply(
              value,
              target,
              args,
            ) as Promise<unknown>;
            return pendingOperation;
          },
          {
            timeoutKind,
            operationName: property,
            timeoutSettingsProvider,
            deadline:
              property === "query"
                ? (args[2] as QueryExecutionOptions).deadline
                : undefined,
            decorateTimeoutError: (error) => {
              if (property === "deleteRows")
                error.deleteResult = deleteProgress;
            },
            onDeadline: (error) => operationAbort?.abort(error),
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
              if (property === "connect") {
                if (
                  pendingOperation &&
                  typeof timeoutHooks.cancelConnectionAttempt === "function"
                ) {
                  return timeoutHooks.cancelConnectionAttempt(pendingOperation);
                }
                return timeoutHooks.disconnect?.();
              }
            },
            onLateSettlementAfterTimeout: () => {
              const timeoutHooks = target as TimeoutAwareDriverHooks;
              if (property === "connect") {
                if (
                  pendingOperation &&
                  typeof timeoutHooks.cancelConnectionAttempt === "function"
                ) {
                  return timeoutHooks.cancelConnectionAttempt(pendingOperation);
                }
                return timeoutHooks.disconnect?.();
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
