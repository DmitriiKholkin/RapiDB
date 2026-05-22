import type { OperationCancellationContext } from "../../shared/safetyContracts";

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
      `${operationLabel} timed out after ${timeoutSeconds} second(s) while running ${operationName}.`,
    );
    this.name = "DriverTimeoutError";
    this.timeoutKind = timeoutKind;
    this.operationName = operationName;
    this.timeoutMs = timeoutMs;
  }
}

interface TimeoutAwareDriverHooks {
  cancelCurrentOperation?(
    context: OperationCancellationContext,
  ): void | Promise<void>;
  recycleConnectionAfterTimeout?(
    context: OperationCancellationContext,
  ): void | Promise<void>;
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
      void Promise.resolve(options.onTimeout?.())
        .catch(() => undefined)
        .finally(() => {
          reject(
            new DriverTimeoutError(
              options.timeoutKind,
              options.operationName,
              timeoutMs,
            ),
          );
        });
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

      const wrapped = (...args: unknown[]) =>
        withDriverTimeout(
          () => Reflect.apply(value, target, args) as Promise<unknown>,
          {
            timeoutKind,
            operationName: property,
            timeoutSettingsProvider,
            onTimeout: async () => {
              const timeoutHooks = target as TimeoutAwareDriverHooks;
              const timeoutContext: OperationCancellationContext = {
                reason: "timeout",
                timeoutKind,
                operationName: property,
              };

              if (typeof timeoutHooks.cancelCurrentOperation === "function") {
                await timeoutHooks.cancelCurrentOperation(timeoutContext);
              }

              if (
                typeof timeoutHooks.recycleConnectionAfterTimeout === "function"
              ) {
                await timeoutHooks.recycleConnectionAfterTimeout(
                  timeoutContext,
                );
              }
            },
            onLateSettlementAfterTimeout: async () => {
              const timeoutHooks = target as TimeoutAwareDriverHooks;
              if (
                typeof timeoutHooks.recycleConnectionAfterTimeout !== "function"
              ) {
                return;
              }

              const timeoutContext: OperationCancellationContext = {
                reason: "late_settlement_after_timeout",
                timeoutKind,
                operationName: property,
              };
              await timeoutHooks.recycleConnectionAfterTimeout(timeoutContext);
            },
          },
        );

      wrappedMethods.set(property, wrapped);
      return wrapped;
    },
  });
}
