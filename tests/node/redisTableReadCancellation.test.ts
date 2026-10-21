import { afterEach, describe, expect, it, vi } from "vitest";
import { RedisDriver } from "../../src/extension/dbDrivers/redis";
import { createTimeoutAwareDriver } from "../../src/extension/dbDrivers/timeout";
import type { DriverTablePageRequest } from "../../src/extension/dbDrivers/types";
import { REDIS_READ_BUDGET } from "../../src/shared/safetyContracts";

const request: DriverTablePageRequest = {
  database: "db0",
  schema: "",
  table: "audit",
  page: 1,
  pageSize: 25,
  filters: [],
  sort: null,
  skipCount: false,
};

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

function fixture() {
  const scan = vi.fn(
    async (_cursor: string): Promise<{ cursor: string; keys: Buffer[] }> => ({
      cursor: "0",
      keys: [Buffer.from("audit:one")],
    }),
  );
  const client = {
    scan,
    type: vi.fn(async () => "string"),
    get: vi.fn(async () => Buffer.from("value")),
    ttl: vi.fn(async () => -1),
    withTypeMapping: vi.fn(),
    withAbortSignal: vi.fn(),
  };
  client.withTypeMapping.mockReturnValue(client);
  client.withAbortSignal.mockReturnValue(client);
  const getDatabaseClient = vi.fn(async () => client);
  const driver = new RedisDriver({
    id: "cancel",
    name: "Cancel",
    type: "redis",
  });
  Object.assign(driver, { getDatabaseClient });
  return { driver, client, getDatabaseClient };
}

afterEach(() => vi.useRealTimers());

describe("Redis table read cancellation", () => {
  it.each([
    "page",
    "discovery",
  ])("rejects a cycling SCAN without unbounded work: %s", async (mode) => {
    const h = fixture();
    h.client.scan.mockResolvedValue({
      cursor: "1",
      keys: [Buffer.from("audit:one")],
    });
    await expect(
      mode === "page"
        ? h.driver.readTablePage(request)
        : h.driver.listObjects("db0"),
    ).rejects.toThrow(/SCAN.*safety budget/);
    expect(h.client.scan).toHaveBeenCalledTimes(2);
    expect(h.client.type).not.toHaveBeenCalled();
  });

  it.each([
    "page",
    "discovery",
  ])("bounds empty SCAN batches even when every cursor is new: %s", async (mode) => {
    const h = fixture();
    let cursor = 0;
    h.client.scan.mockImplementation(async () => ({
      cursor: String(++cursor),
      keys: [],
    }));
    await expect(
      mode === "page"
        ? h.driver.readTablePage(request)
        : h.driver.listObjects("db0"),
    ).rejects.toThrow(/SCAN.*safety budget/);
    expect(h.client.scan).toHaveBeenCalledTimes(
      REDIS_READ_BUDGET.maxScanIterations,
    );
    expect(h.client.type).not.toHaveBeenCalled();
  });

  it("does not open a database client for a pre-aborted read", async () => {
    const h = fixture();
    const abort = new AbortController();
    abort.abort(new Error("Read cancelled"));
    await expect(
      h.driver.readTablePage({ ...request, signal: abort.signal }),
    ).rejects.toThrow("Read cancelled");
    expect(h.getDatabaseClient).not.toHaveBeenCalled();
  });

  it("does not dispatch SCAN with an expired caller deadline", async () => {
    const h = fixture();
    await expect(
      h.driver.readTablePage({ ...request, deadline: Date.now() - 1 }),
    ).rejects.toThrow(/deadline/);
    expect(h.client.scan).not.toHaveBeenCalled();
  });

  it.each([
    null,
    { column: "value", direction: "asc" as const },
  ])("stops after a late SCAN reply when aborted (sort=%j)", async (sort) => {
    const h = fixture();
    const scan = deferred<{ cursor: string; keys: Buffer[] }>();
    const entered = deferred<void>();
    h.client.scan.mockImplementationOnce(() => {
      entered.resolve();
      return scan.promise;
    });
    const abort = new AbortController();
    const pending = h.driver.readTablePage({
      ...request,
      sort,
      signal: abort.signal,
    });
    const rejected = expect(pending).rejects.toThrow("Read cancelled");
    await entered.promise;
    abort.abort(new Error("Read cancelled"));
    scan.resolve({ cursor: "1", keys: [Buffer.from("audit:one")] });
    await rejected;
    expect(h.getDatabaseClient).toHaveBeenCalledWith("db0", abort.signal);
    expect(h.client.withAbortSignal).toHaveBeenCalledWith(abort.signal);
    expect(h.client.scan).toHaveBeenCalledOnce();
    expect(h.client.type).not.toHaveBeenCalled();
  });

  it("does not fetch a value or queue more rows after cancellation during TYPE", async () => {
    const h = fixture();
    const abort = new AbortController();
    h.client.scan.mockResolvedValue({
      cursor: "0",
      keys: Array.from({ length: 50 }, (_, i) => Buffer.from(`audit:${i}`)),
    });
    h.client.type.mockImplementation(async () => {
      abort.abort(new Error("Read cancelled"));
      return "string";
    });
    await expect(
      h.driver.readTablePage({
        ...request,
        pageSize: 50,
        signal: abort.signal,
      }),
    ).rejects.toThrow(/Read cancelled|Operation aborted/);
    expect(h.client.get).not.toHaveBeenCalled();
    expect(h.client.ttl).not.toHaveBeenCalled();
    expect(h.client.type.mock.calls.length).toBeLessThanOrEqual(24);
  });

  it("does not continue a timed-out read after its SCAN settles, and leaves retries usable", async () => {
    vi.useFakeTimers();
    const h = fixture();
    const scan = deferred<{ cursor: string; keys: Buffer[] }>();
    const entered = deferred<void>();
    h.client.scan.mockImplementationOnce(() => {
      entered.resolve();
      return scan.promise;
    });
    const wrapped = createTimeoutAwareDriver(h.driver, () => ({
      connectionTimeoutSeconds: 1,
      dbOperationTimeoutSeconds: 1,
      connectionTimeoutMs: 1000,
      dbOperationTimeoutMs: 25,
    }));
    const rejected = expect(
      wrapped.readTablePage(request),
    ).rejects.toMatchObject({
      name: "DriverTimeoutError",
    });
    await entered.promise;
    await vi.advanceTimersByTimeAsync(25);
    await rejected;
    scan.resolve({ cursor: "1", keys: [Buffer.from("audit:late")] });
    await vi.advanceTimersByTimeAsync(0);
    expect(h.client.scan).toHaveBeenCalledOnce();
    expect(h.client.type).not.toHaveBeenCalled();
    const retry = await wrapped.readTablePage(request);
    expect(retry.rows).toEqual([
      { key: "audit:one", value: "value", ttl: null },
    ]);
  });
});
