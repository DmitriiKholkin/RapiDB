import type { EventEmitter } from "node:events";
import type * as mssql from "mssql";
import { logger } from "../utils/logger";
import {
  bridgeMssqlCompletionMetadata,
  type MssqlCompletionEvent,
  type MssqlStatementCompletionMetadata,
} from "./mssqlCompletionMetadata";
import { throwIfQueryCancelled } from "./timeout";
import type { QueryExecutionOptions } from "./types";

interface SessionConnection {
  inTransaction: boolean;
  closed: boolean;
  hasError?: boolean;
  reset(callback: (error?: unknown) => void): void;
  close(): void;
}
interface SessionPool {
  config: mssql.config;
  collation: unknown;
  connected: boolean;
  request(): mssql.Request;
  acquire(
    requester: unknown,
    callback: (error: unknown, connection: SessionConnection) => void,
  ): unknown;
  release(connection: SessionConnection): unknown;
}

const CLEANUP_TIMEOUT_MS = 1_000;

export class MssqlQuerySession {
  readonly requestToken: number | undefined;
  private readonly pool: SessionPool;
  private connection?: SessionConnection;
  private activeRequest?: mssql.Request;
  private stopped = false;
  private finished = false;
  private completed = false;
  private stopReason: unknown;
  private rejectStopped!: (error: unknown) => void;
  private readonly cancellation: Promise<never>;
  private deadlineTimer?: ReturnType<typeof setTimeout>;
  private readonly onAbort = () => this.cancel(this.context?.signal?.reason);

  constructor(
    pool: mssql.ConnectionPool,
    private readonly context?: QueryExecutionOptions,
    private readonly assertCurrent: () => void = () => {},
  ) {
    this.pool = pool as unknown as SessionPool;
    this.requestToken = context?.requestToken;
    this.cancellation = new Promise<never>((_, reject) => {
      this.rejectStopped = reject;
    });
    void this.cancellation.catch(() => {});
    context?.signal?.addEventListener("abort", this.onAbort, { once: true });
    if (context?.signal?.aborted) this.onAbort();
    if (context?.deadline !== undefined && Number.isFinite(context.deadline)) {
      this.deadlineTimer = setTimeout(
        () => this.cancel(new Error("MSSQL query deadline exceeded.")),
        Math.max(0, context.deadline - Date.now()),
      );
    }
  }

  cancel(
    reason: unknown = new DOMException("MSSQL query cancelled", "AbortError"),
  ): void {
    if (this.stopped || this.completed) return;
    this.stopped = true;
    this.stopReason = reason;
    this.rejectStopped(reason);
    try {
      this.activeRequest?.cancel();
    } catch {}
  }

  hasStopped(): boolean {
    return this.stopped;
  }

  private disarm(): void {
    this.completed = true;
    this.context?.signal?.removeEventListener("abort", this.onAbort);
    if (this.deadlineTimer) clearTimeout(this.deadlineTimer);
  }

  private assertActive(): void {
    if (this.stopped) throw this.stopReason;
    this.assertCurrent();
    throwIfQueryCancelled(this.context);
  }

  private discard(connection: SessionConnection): void {
    connection.hasError = true;
    connection.close();
  }

  private async acquire(): Promise<void> {
    this.assertActive();
    const pending = new Promise<void>((resolve, reject) => {
      this.pool.acquire(this, (error, connection) => {
        if (error) return reject(error);
        try {
          if (this.finished)
            throw new DOMException("MSSQL query ended", "AbortError");
          this.assertActive();
          if (connection.inTransaction) {
            throw new Error(
              "MSSQL acquired a session with an open transaction; the session was discarded.",
            );
          }
          this.connection = connection;
          resolve();
        } catch (failure) {
          let rejected = failure;
          try {
            this.discard(connection);
          } catch (cleanupError) {
            rejected = new AggregateError(
              [failure, cleanupError],
              "MSSQL late session close failed.",
            );
          } finally {
            try {
              this.pool.release(connection);
            } catch (releaseError) {
              rejected = new AggregateError(
                [rejected, releaseError],
                "MSSQL late session release failed.",
              );
            }
            reject(rejected);
          }
        }
      });
    });
    await Promise.race([pending, this.cancellation]);
  }

  request(): mssql.Request {
    this.assertActive();
    const parent = {
      config: this.pool.config,
      collation: this.pool.collation,
      connected: this.pool.connected,
      acquire: (
        _request: unknown,
        callback: (
          error: unknown,
          connection?: SessionConnection,
          config?: mssql.config,
        ) => void,
      ) => {
        setImmediate(() => {
          try {
            this.assertActive();
            if (this.finished || !this.connection)
              throw new Error("MSSQL query session ended.");
          } catch (error) {
            callback(error);
            return;
          }
          callback(null, this.connection, this.pool.config);
        });
      },
      release: () => {},
    };
    const remaining =
      this.context?.deadline === undefined ||
      !Number.isFinite(this.context.deadline)
        ? undefined
        : Math.max(1, this.context.deadline - Date.now());
    const configured = this.pool.config.requestTimeout;
    const requestTimeout =
      remaining === undefined
        ? configured
        : configured && configured > 0
          ? Math.min(configured, remaining)
          : remaining;
    const request = this.pool.request();
    const protocol = request as unknown as {
      parent: typeof parent;
      overrides: { requestTimeout?: number };
    };
    protocol.parent = parent;
    protocol.overrides.requestTimeout = requestTimeout;
    return request;
  }

  trackStatementCompletions(
    request: mssql.Request,
    onComplete: (
      event: MssqlCompletionEvent,
      metadata?: MssqlStatementCompletionMetadata,
    ) => void,
  ): () => void {
    type NativeRequest = Pick<EventEmitter, "emit">;
    type RequestProtocol = {
      _setCurrentRequest?: (nativeRequest: NativeRequest) => unknown;
    };
    const protocol = request as unknown as RequestProtocol;
    const originalSet = protocol._setCurrentRequest;
    if (typeof originalSet !== "function") return () => {};
    const metadataBridge = bridgeMssqlCompletionMetadata(this.connection);
    const hadOwnSet = Object.hasOwn(protocol, "_setCurrentRequest");
    const restorers: Array<() => void> = [];
    const seen = new WeakSet<object>();
    const wrappedSet = function (
      this: RequestProtocol,
      nativeRequest: NativeRequest,
    ): unknown {
      const result = originalSet.call(this, nativeRequest);
      if (
        !nativeRequest ||
        typeof nativeRequest.emit !== "function" ||
        seen.has(nativeRequest)
      )
        return result;
      seen.add(nativeRequest);
      metadataBridge.register(nativeRequest);
      const originalEmit = nativeRequest.emit;
      const hadOwnEmit = Object.hasOwn(nativeRequest, "emit");
      const wrappedEmit = function (
        this: NativeRequest,
        event: string | symbol,
        ...args: unknown[]
      ): boolean {
        const emitted = originalEmit.call(this, event, ...args);
        if (event === "done" || event === "doneInProc" || event === "doneProc")
          onComplete(event, metadataBridge.read(nativeRequest, event, args));
        return emitted;
      };
      nativeRequest.emit = wrappedEmit;
      restorers.push(() => {
        if (nativeRequest.emit !== wrappedEmit) return;
        if (hadOwnEmit) nativeRequest.emit = originalEmit;
        else Reflect.deleteProperty(nativeRequest, "emit");
      });
      return result;
    };
    protocol._setCurrentRequest = wrappedSet;
    return () => {
      if (protocol._setCurrentRequest === wrappedSet) {
        if (hadOwnSet) protocol._setCurrentRequest = originalSet;
        else Reflect.deleteProperty(protocol, "_setCurrentRequest");
      }
      for (const restore of restorers) restore();
      metadataBridge.dispose();
    };
  }

  async execute<T>(
    request: mssql.Request,
    operation: (request: mssql.Request) => Promise<T>,
  ): Promise<T> {
    this.assertActive();
    this.activeRequest = request;
    const onError = () => {};
    request.on("error", onError);
    const pending = Promise.resolve().then(() => {
      this.assertActive();
      return operation(request);
    });
    void pending.finally(() => request.off("error", onError)).catch(() => {});
    try {
      const result = await Promise.race([pending, this.cancellation]);
      this.assertActive();
      return result;
    } finally {
      this.activeRequest = undefined;
    }
  }

  private async reset(connection: SessionConnection): Promise<void> {
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      await Promise.race([
        new Promise<void>((resolve, reject) => {
          connection.reset((error) => (error ? reject(error) : resolve()));
        }),
        new Promise<never>((_, reject) => {
          timer = setTimeout(
            () => reject(new Error("MSSQL query session reset timed out.")),
            CLEANUP_TIMEOUT_MS,
          );
        }),
      ]);
      if (
        connection.inTransaction ||
        connection.closed ||
        connection.hasError
      ) {
        throw new Error(
          "MSSQL query session reset did not restore a healthy session.",
        );
      }
    } finally {
      if (timer) clearTimeout(timer);
    }
  }

  private async cleanup(connection: SessionConnection): Promise<void> {
    if (this.stopped || connection.closed || connection.hasError) {
      this.discard(connection);
      return;
    }
    try {
      await this.reset(connection);
    } catch (error) {
      try {
        this.discard(connection);
      } catch (closeError) {
        throw new AggregateError(
          [error, closeError],
          "MSSQL query session reset and close failed.",
        );
      }
      throw error;
    }
  }

  async run<T>(body: (session: MssqlQuerySession) => Promise<T>): Promise<T> {
    let failed = false;
    let failure: unknown;
    let result!: T;
    try {
      await this.acquire();
      this.assertActive();
      result = await body(this);
      this.assertActive();
      if (this.connection?.inTransaction) {
        throw new Error(
          "MSSQL script ended with an open transaction; it cannot be reported as successful.",
        );
      }
      this.disarm();
    } catch (error) {
      failed = true;
      failure = error;
    }
    this.finished = true;
    const connection = this.connection;
    this.connection = undefined;
    try {
      if (connection) await this.cleanup(connection);
    } catch (cleanupError) {
      if (failed) {
        failure = new AggregateError(
          [failure, cleanupError],
          "MSSQL query and session cleanup failed.",
        );
      } else {
        logger.error(
          "MSSQL session cleanup failed after a successful query",
          cleanupError,
        );
      }
    } finally {
      try {
        if (connection) this.pool.release(connection);
      } catch (releaseError) {
        if (failed) {
          failure = new AggregateError(
            [failure, releaseError],
            "MSSQL query session release failed.",
          );
        } else {
          logger.error(
            "MSSQL session release failed after a successful query",
            releaseError,
          );
        }
      }
      this.disarm();
    }
    if (this.stopped && !failed) throw this.stopReason;
    if (failed) throw failure;
    return result;
  }
}
