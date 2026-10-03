import { type ChildProcess, fork } from "node:child_process";
import { join } from "node:path";
import type { OperationCancellationContext } from "../../shared/safetyContracts";
import type { ConnectionConfig } from "../connectionManager";
import { ensureSQLiteRuntimeInstalled } from "../utils/sqliteInstaller";
import { resolveBetterSqlite3LoadTargets } from "./sqliteRuntime";
import type {
  SQLiteWorkerRequest,
  SQLiteWorkerResponse,
} from "./sqliteWorkerProtocol";
import { DriverTimeoutError } from "./timeout";
import { TransactionVerificationError } from "./transactionVerification";

interface Pending {
  request: SQLiteWorkerRequest;
  token?: number;
  timeoutMs: number;
  resolve(value: unknown): void;
  reject(error: Error): void;
  timer: ReturnType<typeof setTimeout>;
  removeAbort?: () => void;
}

const LOST_SESSION =
  " SQLite execution process was stopped and the connection is closed. Uncommitted transactions are rolled back. ATTACH, temporary tables and session state are lost; in-memory databases are lost. Use Connect to reconnect explicitly before continuing (:memory: starts a new empty database). Completed statements may have persisted; refresh file data before retrying.";

/** One persistent process/handle, one active request, no automatic reopen/retry. */
export class SQLiteWorkerClient {
  private child?: ChildProcess;
  private active?: Pending;
  private queue: Pending[] = [];
  private nextId = 0;
  private stopping: Promise<void> = Promise.resolve();
  private closedMessage = "[RapiDB] SQLite connection is not open";
  connected = false;
  private memoryLost = false;
  private epoch = 0;

  get sessionLost(): boolean {
    return this.memoryLost;
  }

  async open(config: ConnectionConfig, timeoutMs: number): Promise<void> {
    const epoch = ++this.epoch;
    await this.close(false);
    if (epoch !== this.epoch)
      throw new Error("[RapiDB] SQLite connection attempt cancelled");
    if (
      this.memoryLost &&
      (!config.filePath || config.filePath === ":memory:")
    ) {
      throw new Error(
        "[RapiDB] SQLite in-memory database was lost when execution was stopped. Create a new connection explicitly to start a new empty database.",
      );
    }
    const request = this.enqueue(
      "connect",
      [],
      timeoutMs,
      undefined,
      undefined,
      config,
    );
    await request;
    if (epoch !== this.epoch || !this.child)
      throw new Error("[RapiDB] SQLite connection attempt cancelled");
    this.connected = true;
  }

  call<T>(
    method: SQLiteWorkerRequest["method"],
    args: unknown[],
    timeoutMs: number,
    token?: number,
    signal?: AbortSignal,
    deadline?: number,
  ): Promise<T> {
    if (!this.connected) return Promise.reject(new Error(this.closedMessage));
    return this.enqueue(
      method,
      args,
      timeoutMs,
      token,
      signal,
      undefined,
      deadline,
    ) as Promise<T>;
  }

  private enqueue(
    method: SQLiteWorkerRequest["method"],
    args: unknown[],
    timeoutMs: number,
    token?: number,
    signal?: AbortSignal,
    config?: ConnectionConfig,
    deadline?: number,
  ): Promise<unknown> {
    if (signal?.aborted) return Promise.reject(signal.reason);
    const expires = Math.min(
      deadline ?? Infinity,
      timeoutMs > 0 ? Date.now() + timeoutMs : Infinity,
    );
    return new Promise((resolve, reject) => {
      const pending: Pending = {
        request: { id: ++this.nextId, method, args, deadline: expires, config },
        token,
        timeoutMs,
        resolve,
        reject,
        timer: setTimeout(
          () => this.expire(pending),
          Number.isFinite(expires)
            ? Math.max(0, expires - Date.now())
            : 2 ** 31 - 1,
        ),
      };
      if (signal) {
        const abort = () =>
          this.cancelPending(
            pending,
            new Error("[RapiDB] SQLite operation cancelled"),
          );
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
    error.message = `${label} timed out after ${Math.max(1, Math.round(pending.timeoutMs / 1000))} second(s) while running ${method}.`;
    if (this.active === pending) {
      error.message += LOST_SESSION;
      void this.stop(error);
    } else {
      error.message +=
        " SQLite operation expired in the queue and was not executed.";
      this.queue = this.queue.filter((entry) => entry !== pending);
      this.finish(pending, error);
    }
  }

  private dispatch(): void {
    if (this.active || !this.queue.length) return;
    const pending = this.queue.shift();
    if (!pending) return;
    if (Date.now() >= pending.request.deadline) {
      this.expire(pending);
      this.dispatch();
      return;
    }
    this.active = pending;
    if (pending.request.method === "connect") {
      void this.start(pending).catch((error: unknown) => {
        if (this.active === pending)
          void this.stop(
            error instanceof Error ? error : new Error(String(error)),
          );
      });
    } else {
      this.send(pending);
    }
  }

  private async start(pending: Pending): Promise<void> {
    // Installation policy/configuration lives in the extension host. The child
    // receives resolved package paths and never installs or downloads anything.
    try {
      await ensureSQLiteRuntimeInstalled(__dirname);
    } catch {
      /* Runtime loader reports fallback/load errors. */
    }
    if (this.active !== pending) return; // connect timeout/disconnect during installation
    pending.request.runtimeTargets = resolveBetterSqlite3LoadTargets(
      __dirname,
    ).map((target) => {
      try {
        return require.resolve(target);
      } catch {
        return target;
      }
    });
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
      if (Date.now() >= current.request.deadline) {
        this.expire(current);
        return;
      }
      if (response.error && current.request.method === "connect") {
        void this.stop(new Error(response.error.message));
        return;
      }
      this.active = undefined;
      this.finish(
        current,
        response.error
          ? response.error.verificationFailure
            ? new TransactionVerificationError(
                response.error.verificationFailure,
              )
            : Object.assign(new Error(response.error.message), {
                name: response.error.name,
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
            `[RapiDB] SQLite execution process exited (${signal ?? code}).${LOST_SESSION}`,
          ),
        );
    });
    this.send(pending);
  }

  private send(pending: Pending): void {
    try {
      this.child?.send(pending.request, (error) => {
        if (error && this.active === pending) void this.stop(error);
      });
    } catch (error) {
      void this.stop(error instanceof Error ? error : new Error(String(error)));
    }
  }

  private finish(pending: Pending, error?: Error, value?: unknown): void {
    clearTimeout(pending.timer);
    pending.removeAbort?.();
    if (error) pending.reject(error);
    else pending.resolve(value);
  }

  private cancelPending(pending: Pending, error: Error): void {
    if (this.active === pending) {
      error.message += LOST_SESSION;
      void this.stop(error);
    } else if (this.queue.includes(pending)) {
      this.queue = this.queue.filter((entry) => entry !== pending);
      this.finish(pending, error);
    }
  }

  async cancel(context?: OperationCancellationContext): Promise<void> {
    const matches = (pending: Pending) =>
      context?.requestToken !== undefined
        ? pending.token === context.requestToken
        : !context?.operationName ||
          pending.request.method === context.operationName;
    const pending = [this.active, ...this.queue].find(
      (entry) => entry && matches(entry),
    );
    if (pending)
      this.cancelPending(
        pending,
        new Error(
          `[RapiDB] SQLite operation cancelled (${context?.reason ?? "manual"})`,
        ),
      );
    await this.stopping;
  }

  private stop(error: Error): Promise<void> {
    const child = this.child;
    if (
      child &&
      this.connected &&
      (this.active || error.message.includes("process exited"))
    )
      this.memoryLost = true;
    this.child = undefined;
    this.connected = false;
    this.closedMessage = error.message;
    const pending = [this.active, ...this.queue].filter(
      (entry): entry is Pending => !!entry,
    );
    this.active = undefined;
    this.queue = [];
    // Clear deadlines immediately, but settle only after process exit: native
    // SQLite cannot continue writing after cancellation has completed.
    for (const entry of pending) {
      clearTimeout(entry.timer);
      entry.removeAbort?.();
    }
    const exit =
      child?.pid && child.exitCode === null && child.signalCode === null
        ? new Promise<void>((resolve) => {
            child.once("exit", () => resolve());
            child.kill("SIGKILL");
          })
        : Promise.resolve();
    this.stopping = Promise.all([this.stopping, exit]).then(() => {
      for (const entry of pending) entry.reject(error);
    });
    return this.stopping;
  }

  async close(invalidateAttempt = true): Promise<void> {
    if (invalidateAttempt) this.epoch++;
    // Idle close is graceful so SQLite can checkpoint/release resources.
    if (this.connected && !this.active && this.queue.length === 0) {
      try {
        await this.call("disconnect", [], 1000);
      } catch {}
    }
    await this.stop(
      new Error(
        `[RapiDB] SQLite disconnected.${this.active ? LOST_SESSION : ""}`,
      ),
    );
  }
}
