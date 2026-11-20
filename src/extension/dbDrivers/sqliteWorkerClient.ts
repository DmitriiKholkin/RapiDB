import { type ChildProcess, fork } from "node:child_process";
import { join } from "node:path";
import type { OperationCancellationContext } from "../../shared/safetyContracts";
import type { ConnectionConfig } from "../connectionManager";
import {
  ensureSQLiteRuntimeInstalled,
  repairSQLiteManagedRuntime,
  snapshotSQLiteManagedRuntime,
} from "../utils/sqliteInstaller";
import { resolveBetterSqlite3LoadTargets } from "./sqliteRuntime";
import {
  type SQLiteWorkerRequest,
  type SQLiteWorkerResponse,
  sqliteConnectionNow,
  sqliteWorkerRequestExpired,
} from "./sqliteWorkerProtocol";
import { DriverTimeoutError } from "./timeout";
import { TransactionVerificationError } from "./transactionVerification";

interface Pending {
  request: SQLiteWorkerRequest;
  token?: number;
  timeoutMs: number;
  deadlineLimited: boolean;
  resolve(value: unknown): void;
  reject(error: Error): void;
  cancelTimer?: () => void;
  removeAbort?: () => void;
  detachOnAbort: boolean;
  cleaned: boolean;
  settled: boolean;
}

const LOST_SESSION =
  " SQLite execution process was stopped and the connection is closed. Uncommitted transactions are rolled back. ATTACH, temporary tables and session state are lost; in-memory databases are lost. Use Connect to reconnect explicitly before continuing (:memory: starts a new empty database). Completed statements may have persisted; refresh file data before retrying.";
const DETACHED_READ_TIMEOUT =
  "A cancelled background SQLite read exceeded its timeout, so the SQLite worker was stopped and the connection is closed. Use Connect to reconnect explicitly before continuing.";
const DETACHED_READ_CANCELLED =
  "A cancelled background SQLite read was still running, so the SQLite worker was stopped to honor an explicit cancel and the connection is closed. Use Connect to reconnect explicitly before continuing.";
const UNKNOWN_OUTCOME =
  " The active SQLite request outcome is unknown (OUTCOME_UNKNOWN); if it contained a write, it may have partially or fully persisted. Refresh and verify before retrying.";

export type SQLiteWorkerNotExecutedReason =
  | "expired"
  | "worker-stopped"
  | "cancelled"
  | "disconnected";

export class SQLiteWorkerNotExecutedError extends Error {
  readonly code = "NOT_EXECUTED";
  readonly executionState = "not-executed";

  constructor(
    readonly method: SQLiteWorkerRequest["method"],
    readonly reason: Exclude<SQLiteWorkerNotExecutedReason, "expired">,
    detail?: string,
  ) {
    const explanation = {
      "worker-stopped": "the SQLite worker stopped before it was sent",
      cancelled: "it was cancelled while waiting in the queue",
      disconnected: "the connection was disconnected before it was sent",
    }[reason];
    super(
      `[RapiDB] SQLite operation ${method} was NOT_EXECUTED (not executed): ${explanation}.${detail ? ` ${detail}` : ""}`,
    );
    this.name = "SQLiteWorkerNotExecutedError";
  }
}

class SQLiteWorkerManagedNativeError extends Error {}

function connectionCancelled(): Error {
  return new Error("[RapiDB] SQLite connection attempt cancelled");
}

function connectionTimedOut(timeoutMs: number): DriverTimeoutError {
  return new DriverTimeoutError("connection", "connect", timeoutMs);
}

function watchConnectionDeadline(
  deadline: number,
  expire: () => void,
): (() => void) | undefined {
  if (!Number.isFinite(deadline)) return undefined;
  let timer: ReturnType<typeof setTimeout>;
  const schedule = () => {
    const delay = Math.ceil(deadline - sqliteConnectionNow());
    timer = setTimeout(
      () => {
        if (sqliteConnectionNow() >= deadline) expire();
        else schedule();
      },
      Math.min(2 ** 31 - 1, Math.max(0, delay)),
    );
  };
  schedule();
  return () => clearTimeout(timer);
}

async function waitForRuntime<T>(
  start: (signal: AbortSignal) => Promise<T>,
  signal: AbortSignal,
  timeout?: { deadline: number; timeoutMs: number },
): Promise<T> {
  if (signal.aborted) throw connectionCancelled();
  let cancelTimer: (() => void) | undefined;
  let abort: () => void = () => {};
  try {
    return await new Promise<T>((resolve, reject) => {
      abort = () => reject(connectionCancelled());
      signal.addEventListener("abort", abort, { once: true });
      let runtimeSignal = signal;
      if (timeout && Number.isFinite(timeout.deadline)) {
        if (sqliteConnectionNow() >= timeout.deadline) {
          reject(connectionTimedOut(timeout.timeoutMs));
          return;
        }
        const interest = new AbortController();
        runtimeSignal = AbortSignal.any([signal, interest.signal]);
        cancelTimer = watchConnectionDeadline(timeout.deadline, () => {
          reject(connectionTimedOut(timeout.timeoutMs));
          // Drop only this caller's interest. The installer can still finish
          // an authorized repair or serve its other waiters.
          interest.abort();
        });
      }
      start(runtimeSignal).then(resolve, reject);
    });
  } finally {
    cancelTimer?.();
    signal.removeEventListener("abort", abort);
  }
}

export class SQLiteWorkerClient {
  private child?: ChildProcess;
  private active?: Pending;
  private queue: Pending[] = [];
  private nextId = 0;
  private stopping: Promise<void> = Promise.resolve();
  connected = false;
  private memoryLost = false;
  private epoch = 0;
  private opening?: AbortController;

  get sessionLost(): boolean {
    return this.memoryLost;
  }

  async open(config: ConnectionConfig, timeoutMs: number): Promise<void> {
    const epoch = ++this.epoch;
    this.opening?.abort();
    const opening = new AbortController();
    this.opening = opening;
    const checkAttempt = () => {
      if (epoch !== this.epoch || opening.signal.aborted)
        throw connectionCancelled();
    };
    try {
      await this.close(false);
      checkAttempt();
      if (
        this.memoryLost &&
        (!config.filePath || config.filePath === ":memory:")
      ) {
        throw new Error(
          "[RapiDB] SQLite in-memory database was lost when execution was stopped. Create a new connection explicitly to start a new empty database.",
        );
      }
      const snapshot = snapshotSQLiteManagedRuntime(__dirname);
      const managedPackage = await waitForRuntime(
        () => ensureSQLiteRuntimeInstalled(__dirname),
        opening.signal,
      );
      checkAttempt();
      // Keep the existing cold-install authorization outside this budget.
      const deadline =
        timeoutMs > 0 ? sqliteConnectionNow() + timeoutMs : Infinity;
      try {
        await this.connectWorker(
          config,
          timeoutMs,
          deadline,
          resolveBetterSqlite3LoadTargets(managedPackage),
          managedPackage,
        );
      } catch (error) {
        checkAttempt();
        if (
          !(error instanceof SQLiteWorkerManagedNativeError) ||
          !snapshot ||
          snapshot.packagePath !== managedPackage
        )
          throw error;

        let repaired: string | null;
        try {
          repaired = await waitForRuntime(
            (signal) => repairSQLiteManagedRuntime(__dirname, snapshot, signal),
            opening.signal,
            { deadline, timeoutMs },
          );
        } catch (repairError) {
          checkAttempt();
          if (sqliteConnectionNow() >= deadline)
            throw connectionTimedOut(timeoutMs);
          throw new Error(
            `${error.message} SQLite runtime repair failed: ${repairError instanceof Error ? repairError.message : String(repairError)}`,
            { cause: repairError },
          );
        }
        checkAttempt();
        if (sqliteConnectionNow() >= deadline)
          throw connectionTimedOut(timeoutMs);
        if (!repaired) throw error;
        // The original worker tested all fallbacks before opening a file.
        // Retry only the replaced binary, in a fresh worker, without looping.
        await this.connectWorker(
          config,
          timeoutMs,
          deadline,
          [repaired],
          repaired,
        );
      }
      checkAttempt();
      if (!this.child) throw connectionCancelled();
      if (sqliteConnectionNow() >= deadline) {
        const error = connectionTimedOut(timeoutMs);
        await this.stop(error);
        throw error;
      }
      this.connected = true;
      this.memoryLost = false;
    } catch (error) {
      checkAttempt();
      throw error;
    } finally {
      if (this.opening === opening) this.opening = undefined;
    }
  }

  private connectWorker(
    config: ConnectionConfig,
    timeoutMs: number,
    deadline: number,
    targets: readonly string[],
    managedPackage: string | null,
  ): Promise<unknown> {
    const runtimeTargets = targets.map((target) => {
      try {
        return require.resolve(target);
      } catch {
        return target;
      }
    });
    return this.enqueue(
      "connect",
      [],
      timeoutMs,
      undefined,
      undefined,
      {
        config,
        runtimeTargets,
        managedRuntimePackage: managedPackage ?? undefined,
      },
      deadline,
    );
  }

  call<T>(
    method: SQLiteWorkerRequest["method"],
    args: unknown[],
    timeoutMs: number,
    token?: number,
    signal?: AbortSignal,
    deadline?: number,
    detachOnAbort = false,
  ): Promise<T> {
    if (!this.connected) {
      const error = new SQLiteWorkerNotExecutedError(method, "disconnected");
      error.message +=
        " The SQLite connection is closed. Use Connect to reconnect explicitly before continuing.";
      return Promise.reject(error);
    }
    return this.enqueue(
      method,
      args,
      timeoutMs,
      token,
      signal,
      undefined,
      deadline,
      detachOnAbort,
    ) as Promise<T>;
  }

  private enqueue(
    method: SQLiteWorkerRequest["method"],
    args: unknown[],
    timeoutMs: number,
    token?: number,
    signal?: AbortSignal,
    connection?: Pick<
      SQLiteWorkerRequest,
      "config" | "runtimeTargets" | "managedRuntimePackage"
    >,
    deadline?: number,
    detachOnAbort = false,
  ): Promise<unknown> {
    if (signal?.aborted)
      return Promise.reject(
        new SQLiteWorkerNotExecutedError(method, "cancelled"),
      );
    const connecting = method === "connect";
    const now = connecting ? sqliteConnectionNow : Date.now;
    const enqueuedAt = now();
    const timeoutDeadline = timeoutMs > 0 ? enqueuedAt + timeoutMs : Infinity;
    const expires = connecting
      ? (deadline ?? Infinity)
      : Math.min(deadline ?? Infinity, timeoutDeadline);
    return new Promise((resolve, reject) => {
      const pending: Pending = {
        request: {
          id: ++this.nextId,
          method,
          args,
          deadline: expires,
          ...connection,
        },
        token,
        timeoutMs:
          connecting || !Number.isFinite(expires)
            ? timeoutMs
            : Math.max(0, expires - enqueuedAt),
        deadlineLimited:
          !connecting && deadline !== undefined && deadline < timeoutDeadline,
        resolve,
        reject,
        detachOnAbort,
        cleaned: false,
        settled: false,
      };
      if (connecting) {
        pending.cancelTimer = watchConnectionDeadline(expires, () =>
          this.expire(pending),
        );
      } else {
        const timer = setTimeout(
          () => this.expire(pending),
          Number.isFinite(expires)
            ? Math.max(0, expires - Date.now())
            : 2 ** 31 - 1,
        );
        pending.cancelTimer = () => clearTimeout(timer);
      }
      if (signal) {
        const abort = () => {
          if (this.active === pending && pending.detachOnAbort) {
            this.detach(pending);
            return;
          }
          this.cancelPending(
            pending,
            new Error("[RapiDB] SQLite operation cancelled"),
          );
        };
        signal.addEventListener("abort", abort, { once: true });
        pending.removeAbort = () => signal.removeEventListener("abort", abort);
      }
      this.queue.push(pending);
      this.dispatch();
    });
  }

  private expire(pending: Pending): void {
    const method = pending.request.method;
    const error = new DriverTimeoutError(
      method === "connect" ? "connection" : "dbOperation",
      method,
      pending.timeoutMs,
    );
    const label =
      method === "connect" ? "Database connection" : "Database operation";
    if (this.active === pending) {
      error.message = pending.deadlineLimited
        ? `${label} timed out at its deadline while running ${method}.`
        : `${label} timed out after ${Math.max(1, Math.round(pending.timeoutMs / 1000))} second(s) while running ${method}.`;
      void this.stop(
        error,
        "worker-stopped",
        pending.settled ? DETACHED_READ_TIMEOUT : undefined,
      );
    } else {
      error.message = `${label} expired in the SQLite worker queue before running ${method}. The request was NOT_EXECUTED (not executed).`;
      Object.assign(error, {
        code: "NOT_EXECUTED",
        executionState: "not-executed",
        reason: "expired" satisfies SQLiteWorkerNotExecutedReason,
      });
      this.queue = this.queue.filter((entry) => entry !== pending);
      this.finish(pending, error);
    }
  }

  private dispatch(): void {
    if (this.active || !this.queue.length) return;
    const pending = this.queue.shift();
    if (!pending) return;
    if (sqliteWorkerRequestExpired(pending.request)) {
      this.expire(pending);
      this.dispatch();
      return;
    }
    this.active = pending;
    if (pending.request.method === "connect") {
      try {
        this.start(pending);
      } catch (error) {
        if (this.active === pending)
          void this.stop(
            error instanceof Error ? error : new Error(String(error)),
          );
      }
    } else {
      this.send(pending);
    }
  }

  private start(pending: Pending): void {
    const workerPath =
      process.env.NODE_ENV === "test" && process.env.RAPIDB_SQLITE_WORKER_PATH
        ? process.env.RAPIDB_SQLITE_WORKER_PATH
        : join(__dirname, "sqliteWorker.js");
    const child = fork(workerPath, [], {
      execPath: process.execPath,
      execArgv: [],
      env: { ...process.env, ELECTRON_RUN_AS_NODE: "1" },
      serialization: "advanced",
      stdio: ["ignore", "ignore", "ignore", "ipc"],
    });
    this.child = child;
    child.on("message", (response: SQLiteWorkerResponse) => {
      if (this.child !== child || this.active?.request.id !== response.id)
        return;
      const current = this.active;
      if (
        current.request.method === "connect" &&
        sqliteWorkerRequestExpired(current.request)
      ) {
        this.expire(current);
        return;
      }
      if (response.error?.name === "SQLiteRollbackFailure") {
        void this.stop(new Error(response.error.message));
        return;
      }
      if (response.error && current.request.method === "connect") {
        void this.stop(
          response.error.managedNativeFailure === true
            ? new SQLiteWorkerManagedNativeError(response.error.message)
            : new Error(response.error.message),
        );
        return;
      }
      this.active = undefined;
      this.finish(
        current,
        response.error
          ? response.error.verificationFailure
            ? Object.assign(
                new TransactionVerificationError(
                  response.error.verificationFailure,
                ),
                {
                  rollbackConfirmed: response.error.rollbackConfirmed === true,
                },
              )
            : Object.assign(new Error(response.error.message), {
                name: response.error.name,
                ...(response.error.executionState
                  ? { executionState: response.error.executionState }
                  : {}),
                ...(response.error.executionState === "not-executed"
                  ? { code: "NOT_EXECUTED" }
                  : {}),
              })
          : undefined,
        response.value,
      );
      this.dispatch();
    });
    child.on("error", (error) => {
      if (this.child === child) void this.stop(error);
    });
    child.on("exit", (code, signal) => {
      if (this.child === child)
        void this.stop(
          new Error(
            `[RapiDB] SQLite execution process exited (${signal ?? code}).`,
          ),
        );
    });
    this.send(pending);
  }

  private send(pending: Pending): void {
    if (
      pending.request.method === "connect" &&
      sqliteWorkerRequestExpired(pending.request)
    ) {
      this.expire(pending);
      return;
    }
    try {
      this.child?.send(pending.request, (error) => {
        if (error && this.active === pending) void this.stop(error);
      });
    } catch (error) {
      void this.stop(error instanceof Error ? error : new Error(String(error)));
    }
  }

  private finish(pending: Pending, error?: Error, value?: unknown): void {
    this.cleanup(pending);
    if (pending.settled) return;
    pending.settled = true;
    if (error) pending.reject(error);
    else pending.resolve(value);
  }

  private detach(pending: Pending): void {
    if (pending.settled) return;
    pending.settled = true;
    pending.removeAbort?.();
    pending.reject(
      new DOMException("[RapiDB] SQLite operation cancelled", "AbortError"),
    );
  }

  private cleanup(pending: Pending): void {
    if (pending.cleaned) return;
    pending.cleaned = true;
    pending.cancelTimer?.();
    pending.removeAbort?.();
  }

  private cancelPending(pending: Pending, error: Error): void {
    if (this.active === pending) {
      void this.stop(error, "cancelled");
    } else if (this.queue.includes(pending)) {
      this.queue = this.queue.filter((entry) => entry !== pending);
      this.finish(
        pending,
        new SQLiteWorkerNotExecutedError(pending.request.method, "cancelled"),
      );
    }
  }

  async cancel(context?: OperationCancellationContext): Promise<void> {
    const matches = (pending: Pending) =>
      context?.requestToken !== undefined
        ? pending.token === context.requestToken
        : !context?.operationName ||
          pending.request.method === context.operationName;
    const pending = [this.active, ...this.queue].find(
      (entry) => entry && !entry.settled && matches(entry),
    );
    const error = new Error(
      `[RapiDB] SQLite operation cancelled (${context?.reason ?? "manual"})`,
    );
    if (
      context?.requestToken === undefined &&
      (!context?.operationName || context.operationName === "connect")
    )
      this.opening?.abort();
    if (pending && this.active?.settled && this.active !== pending) {
      this.queue = this.queue.filter((entry) => entry !== pending);
      this.finish(
        pending,
        new SQLiteWorkerNotExecutedError(
          pending.request.method,
          "cancelled",
          DETACHED_READ_CANCELLED,
        ),
      );
      void this.stop(error, "worker-stopped", DETACHED_READ_CANCELLED);
    } else if (pending) {
      this.cancelPending(pending, error);
    }
    await this.stopping;
  }

  private stop(
    error: Error,
    queuedReason: Exclude<
      SQLiteWorkerNotExecutedReason,
      "expired"
    > = "worker-stopped",
    queuedDetail?: string,
  ): Promise<void> {
    const child = this.child;
    const active = this.active;
    const queued = this.queue;
    if (
      child &&
      this.connected &&
      (this.active || error.message.includes("process exited"))
    )
      this.memoryLost = true;
    if (active && active.request.method !== "connect") {
      if (!error.message.includes("OUTCOME_UNKNOWN"))
        error.message += UNKNOWN_OUTCOME;
      if (!error.message.includes("connection is closed"))
        error.message += LOST_SESSION;
      Object.assign(error, {
        code: "OUTCOME_UNKNOWN",
        executionState: "unknown",
      });
    } else if (
      child &&
      active?.request.method === "connect" &&
      error.message.includes("timed out") &&
      !error.message.includes("connection is closed")
    ) {
      error.message += LOST_SESSION;
    } else if (
      !active &&
      child &&
      this.connected &&
      error.message.includes("process exited") &&
      !error.message.includes("connection is closed")
    ) {
      error.message += LOST_SESSION;
    }
    this.child = undefined;
    this.connected = false;
    this.active = undefined;
    this.queue = [];
    for (const entry of [active, ...queued]) if (entry) this.cleanup(entry);
    const exit =
      child?.pid && child.exitCode === null && child.signalCode === null
        ? new Promise<void>((resolve) => {
            child.once("exit", () => resolve());
            child.kill("SIGKILL");
          })
        : Promise.resolve();
    this.stopping = Promise.all([this.stopping, exit]).then(() => {
      if (active) this.finish(active, error);
      for (const entry of queued) {
        this.finish(
          entry,
          new SQLiteWorkerNotExecutedError(
            entry.request.method,
            queuedReason,
            queuedDetail,
          ),
        );
      }
    });
    return this.stopping;
  }

  async close(invalidateAttempt = true): Promise<void> {
    if (invalidateAttempt) {
      this.epoch++;
      this.opening?.abort();
    }
    if (this.connected && !this.active && this.queue.length === 0) {
      try {
        await this.call("disconnect", [], 1000);
      } catch {}
    }
    await this.stop(new Error("[RapiDB] SQLite disconnected."), "disconnected");
  }
}
