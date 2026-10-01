import { type Client, errors } from "@elastic/elasticsearch";
import { DriverTimeoutError } from "./timeout";

function cancellation(signal: AbortSignal): Error {
  return signal.reason instanceof DriverTimeoutError
    ? signal.reason
    : new DOMException("Elasticsearch read cancelled", "AbortError");
}

/** Observe physical settlement, but never require it for logical cancellation. */
function untilAborted<T>(promise: Promise<T>, signal: AbortSignal): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const onAbort = () => reject(cancellation(signal));
    signal.addEventListener("abort", onAbort, { once: true });
    promise.then(resolve, reject).finally(() => {
      signal.removeEventListener("abort", onAbort);
    });
    if (signal.aborted) onAbort();
  });
}

export async function drainElasticsearchCleanup(
  promise: Promise<unknown>,
  budgetMs: number,
): Promise<void> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    await Promise.race([
      promise.catch(() => undefined),
      new Promise<void>((resolve) => {
        timer = setTimeout(resolve, budgetMs);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

/** One PIT lifetime, with deadlines only while advancing (never at a yield). */
export class ElasticsearchPitSession {
  private readonly controller = new AbortController();
  readonly signal: AbortSignal = this.controller.signal;
  pitId: string | undefined;
  private pending: Promise<unknown> | undefined;
  private closing: Promise<void> | undefined;
  private readonly closes = new Map<string, Promise<void>>();
  private activeDeadline = Infinity;
  private readonly onAbort = () => {
    if (this.externalSignal)
      this.controller.abort(cancellation(this.externalSignal));
    void this.close().catch(() => undefined);
  };

  constructor(
    private readonly client: Client,
    private readonly timeoutMs: number,
    private readonly operationName: string,
    private readonly externalSignal: AbortSignal | undefined,
    private readonly pageDeadline: number,
    private readonly onClosed: () => void,
  ) {
    externalSignal?.addEventListener("abort", this.onAbort, { once: true });
    if (externalSignal?.aborted) this.onAbort();
  }

  check(): void {
    if (
      !this.signal.aborted &&
      Date.now() >= Math.min(this.activeDeadline, this.pageDeadline)
    )
      this.expire();
    if (this.signal.aborted) throw cancellation(this.signal);
  }

  private expire(): void {
    this.controller.abort(
      new DriverTimeoutError("dbOperation", this.operationName, this.timeoutMs),
    );
    void this.close().catch(() => undefined);
  }

  async advance<T>(work: () => Promise<T>): Promise<T> {
    this.check();
    this.activeDeadline = Math.min(
      Date.now() + this.timeoutMs,
      this.pageDeadline,
    );
    const remaining = this.activeDeadline - Date.now();
    if (remaining <= 0) {
      this.expire();
      this.check();
    }
    const timer = setTimeout(() => this.expire(), remaining);
    try {
      return await untilAborted(Promise.resolve().then(work), this.signal);
    } catch (error) {
      // SDK 9 rejects aborted HTTP requests with RequestAbortedError.
      // The operation signal distinguishes user cancellation from DB failures.
      if (error instanceof errors.TimeoutError) this.expire();
      this.check();
      throw error;
    } finally {
      clearTimeout(timer);
      this.activeDeadline = Infinity;
    }
  }

  async request<T>(
    send: (options: {
      signal: AbortSignal;
      requestTimeout: number;
    }) => Promise<T>,
    observe?: (value: T) => void,
  ): Promise<T> {
    this.check();
    const promise = send({
      signal: this.signal,
      requestTimeout: Math.max(1, this.activeDeadline - Date.now()),
    }).then((value) => {
      // This observer stays attached after a logical abort/deadline, even if
      // the SDK/transport ignores the signal and creates/rotates the PIT late.
      observe?.(value);
      return value;
    });
    this.pending = promise;
    const result = await untilAborted(promise, this.signal);
    this.check();
    return result;
  }

  recordPitId(id: string): void {
    this.pitId = id;
    if (this.signal.aborted) void this.closePit(id).catch(() => undefined);
  }

  private closePit(id: string): Promise<void> {
    const existing = this.closes.get(id);
    if (existing) return existing;
    const controller = new AbortController();
    const budget = Math.min(this.timeoutMs, 1000);
    const timer = setTimeout(
      () =>
        controller.abort(
          new DriverTimeoutError("dbOperation", "closePointInTime", budget),
        ),
      budget,
    );
    const close = untilAborted(
      Promise.resolve().then(() =>
        this.client.closePointInTime(
          { id },
          { signal: controller.signal, requestTimeout: budget },
        ),
      ),
      controller.signal,
    )
      .then((result) => {
        if (!result.succeeded)
          throw new Error("Elasticsearch PIT cleanup failed.");
      })
      .finally(() => clearTimeout(timer));
    this.closes.set(id, close);
    return close;
  }

  close(): Promise<void> {
    if (!this.closing) {
      this.controller.abort();
      this.externalSignal?.removeEventListener("abort", this.onAbort);
      this.closing = (async () => {
        // A short drain catches promptly rotating IDs without hanging on an
        // uncooperative physical request. Late observers handle later IDs.
        if (this.pending) await drainElasticsearchCleanup(this.pending, 50);
        if (this.pitId) await this.closePit(this.pitId);
      })().finally(this.onClosed);
    }
    return this.closing;
  }

  waitForClose(): Promise<void> {
    const cleanup = this.close();
    if (this.pageDeadline === Infinity) {
      return this.externalSignal
        ? untilAborted(cleanup, this.externalSignal)
        : cleanup;
    }
    const controller = new AbortController();
    const timer = setTimeout(
      () =>
        controller.abort(
          new DriverTimeoutError(
            "dbOperation",
            this.operationName,
            this.timeoutMs,
          ),
        ),
      Math.max(0, this.pageDeadline - Date.now()),
    );
    const signal = this.externalSignal
      ? AbortSignal.any([this.externalSignal, controller.signal])
      : controller.signal;
    return untilAborted(cleanup, signal).finally(() => clearTimeout(timer));
  }
}
