import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ConnectionDriverFactory } from "../../src/extension/connectionDriverFactory";
import {
  createDriverTimeoutSettingsSnapshot,
  createTimeoutAwareDriver,
} from "../../src/extension/dbDrivers/timeout";
import type { DriverTableExportChunk } from "../../src/extension/dbDrivers/types";
import {
  createElasticsearchPitMock,
  deferred,
  elasticsearchResponseError,
  required,
} from "../support/elasticsearchPitMock";

const timeoutSettings = () =>
  createDriverTimeoutSettingsSnapshot({ dbOperationTimeoutSeconds: 1 });
const request = {
  database: "default",
  schema: "indices",
  table: "records",
  chunkSize: 500,
};
const pageRequest = {
  ...request,
  page: 3,
  pageSize: 3,
  filters: [],
  sort: null,
  skipCount: false,
};
async function collect(chunks: AsyncIterable<DriverTableExportChunk>) {
  const rows: Record<string, unknown>[] = [];
  for await (const chunk of chunks) rows.push(...chunk.rows);
  return rows;
}

describe("B11 logical and SDK deadlines", () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it.each(
    (["sorted", "filtered"] as const).flatMap((mode) =>
      (["abort", "deadline", "disconnect"] as const).map((reason) => ({
        mode,
        reason,
      })),
    ),
  )("cleans a $mode materialization on $reason even if the SDK resolves late", async ({
    mode,
    reason,
  }) => {
    const { driver, client } = createElasticsearchPitMock(
      17,
      10000,
      timeoutSettings,
    );
    const late = deferred<Awaited<ReturnType<typeof client.search>>>();
    client.search.mockReturnValueOnce(late.promise);
    const controller = new AbortController();
    const operation = driver.readTablePage({
      ...pageRequest,
      signal: controller.signal,
      sort: mode === "sorted" ? { column: "_id", direction: "asc" } : null,
      filters:
        mode === "filtered"
          ? [{ column: "_id", operator: "like", value: "%doc%" }]
          : [],
    });
    const rejected = expect(operation).rejects.toMatchObject({
      name: reason === "deadline" ? "DriverTimeoutError" : "AbortError",
    });
    await vi.advanceTimersByTimeAsync(0);
    if (reason === "abort") controller.abort();
    const disconnect =
      reason === "disconnect" ? driver.disconnect() : undefined;
    await vi.advanceTimersByTimeAsync(reason === "deadline" ? 1200 : 100);
    await rejected;
    await disconnect;
    expect(client.search.mock.calls[0][1]?.signal?.aborted).toBe(true);
    expect(
      (driver as unknown as { pitReaders: Set<unknown> }).pitReaders.size,
    ).toBe(0);
    late.resolve({
      timed_out: false,
      _shards: { total: 2, successful: 2, failed: 0 },
      hits: { total: { value: 0, relation: "eq" }, hits: [] },
      pit_id: undefined,
    });
    await vi.advanceTimersByTimeAsync(0);
    expect(client.search).toHaveBeenCalledTimes(1);
    expect(client.openPointInTime).not.toHaveBeenCalled();
    expect(client.closePointInTime).not.toHaveBeenCalled();
  });

  it.each([
    "settings",
    "open",
    "search",
    "close",
  ])("bounds a stuck %s request even when it ignores the SDK signal", async (stage) => {
    const { driver, client } = createElasticsearchPitMock(
      1,
      3,
      timeoutSettings,
    );
    const never = () => new Promise<never>(() => undefined);
    if (stage === "settings")
      client.indices.getSettings.mockImplementationOnce(never);
    if (stage === "open") client.openPointInTime.mockImplementationOnce(never);
    if (stage === "search") client.search.mockImplementationOnce(never);
    if (stage === "close")
      client.closePointInTime.mockImplementationOnce(never);
    const wrapped = createTimeoutAwareDriver(driver, timeoutSettings);
    let settled = false;
    const result = collect(wrapped.exportTableChunks(request));
    void result.then(
      () => {
        settled = true;
      },
      () => {
        settled = true;
      },
    );
    const rejected = expect(result).rejects.toMatchObject({
      name: "DriverTimeoutError",
    });
    await vi.advanceTimersByTimeAsync(999);
    expect(settled).toBe(false);
    await vi.advanceTimersByTimeAsync(201);
    await rejected;
    expect(settled).toBe(true);
    const options =
      stage === "settings"
        ? client.indices.getSettings.mock.calls[0][1]
        : stage === "open"
          ? client.openPointInTime.mock.calls[0][1]
          : stage === "search"
            ? client.search.mock.calls[0][1]
            : client.closePointInTime.mock.calls[0][1];
    expect(options?.requestTimeout).toBeLessThanOrEqual(1000);
    expect(options?.signal?.aborted).toBe(true);
  });

  it("passes the live timeout provider through the production driver factory", async () => {
    const { client } = createElasticsearchPitMock(3);
    client.indices.getSettings.mockImplementationOnce(
      () => new Promise(() => undefined),
    );
    const driver = new ConnectionDriverFactory(timeoutSettings).createDriver({
      id: "es",
      name: "ES",
      type: "elasticsearch",
    });
    Object.assign(driver, { client, connected: true });
    const result = required(driver.exportTableChunks)(request);
    const rejected = expect(collect(result)).rejects.toMatchObject({
      name: "DriverTimeoutError",
    });
    await vi.advanceTimersByTimeAsync(1200);
    await rejected;
    expect(client.indices.getSettings.mock.calls[0][1]?.requestTimeout).toBe(
      1000,
    );
  });

  it("stops the entire deep page on its proxy deadline and never searches after late settings", async () => {
    const { driver, client } = createElasticsearchPitMock(
      9,
      3,
      timeoutSettings,
    );
    const settings =
      deferred<Awaited<ReturnType<typeof client.indices.getSettings>>>();
    client.indices.getSettings.mockReturnValueOnce(settings.promise);
    const wrapped = createTimeoutAwareDriver(driver, timeoutSettings);
    const rejected = expect(
      wrapped.readTablePage(pageRequest),
    ).rejects.toMatchObject({ name: "DriverTimeoutError" });
    await vi.advanceTimersByTimeAsync(1200);
    await rejected;
    expect(client.indices.getSettings.mock.calls[0][1]?.signal?.aborted).toBe(
      true,
    );
    settings.resolve({
      records: { settings: { index: { max_result_window: "3" } } },
    });
    await vi.advanceTimersByTimeAsync(0);
    expect(client.openPointInTime).not.toHaveBeenCalled();
    expect(client.search).not.toHaveBeenCalled();
  });

  it("keeps one deadline across all cursor batches of a deep page", async () => {
    const { driver, client } = createElasticsearchPitMock(
      9,
      3,
      timeoutSettings,
    );
    const realSearch = required(client.search.getMockImplementation());
    client.search.mockImplementation(async (...args) => {
      await new Promise((resolve) => setTimeout(resolve, 600));
      return realSearch(...args);
    });
    const rejected = expect(
      driver.readTablePage(pageRequest),
    ).rejects.toMatchObject({ name: "DriverTimeoutError" });
    await vi.advanceTimersByTimeAsync(1200);
    await rejected;
    expect(client.search).toHaveBeenCalledTimes(2);
    await vi.advanceTimersByTimeAsync(1200);
    expect(client.search).toHaveBeenCalledTimes(2);
  });

  it("does not time out an export while the consumer is paused at yield", async () => {
    const { driver } = createElasticsearchPitMock(9, 3, timeoutSettings);
    const chunks = driver.exportTableChunks(request);
    await chunks.next();
    await vi.advanceTimersByTimeAsync(5000);
    expect((await chunks.next()).value?.rows).toHaveLength(3);
    await chunks.return(undefined);
  });

  it.each([
    "abort",
    "deadline",
    "disconnect",
  ])("bounds a stuck PIT open on %s and still closes an ID returned much later", async (reason) => {
    const { driver, client, snapshots } = createElasticsearchPitMock(
      9,
      3,
      timeoutSettings,
    );
    const open = deferred<{ id: string }>();
    const realOpen = required(client.openPointInTime.getMockImplementation());
    client.openPointInTime.mockReturnValueOnce(open.promise);
    const controller = new AbortController();
    const result = driver.exportTableChunks(request, controller.signal).next();
    const rejected = expect(result).rejects.toMatchObject({
      name: reason === "deadline" ? "DriverTimeoutError" : "AbortError",
    });
    await vi.advanceTimersByTimeAsync(0);
    let disconnect: Promise<void> | undefined;
    if (reason === "abort") controller.abort();
    if (reason === "disconnect") disconnect = driver.disconnect();
    await vi.advanceTimersByTimeAsync(reason === "deadline" ? 1200 : 100);
    await rejected;
    await disconnect;
    if (reason === "disconnect") expect(client.close).toHaveBeenCalledTimes(1);
    const late = await realOpen({});
    open.resolve(late);
    await vi.advanceTimersByTimeAsync(0);
    expect(client.closePointInTime).toHaveBeenCalledWith(
      { id: late.id },
      { signal: expect.any(AbortSignal), requestTimeout: 1000 },
    );
    expect(snapshots.size).toBe(0);
    expect(client.search).not.toHaveBeenCalled();
  });

  it("cancels a stuck close promptly while its bounded physical cleanup continues", async () => {
    const { driver, client } = createElasticsearchPitMock(
      1,
      3,
      timeoutSettings,
    );
    client.closePointInTime.mockImplementationOnce(
      () => new Promise(() => undefined),
    );
    const controller = new AbortController();
    const result = collect(
      driver.exportTableChunks(request, controller.signal),
    );
    const rejected = expect(result).rejects.toMatchObject({
      name: "AbortError",
    });
    await vi.advanceTimersByTimeAsync(0);
    expect(client.closePointInTime).toHaveBeenCalledTimes(1);
    controller.abort();
    await vi.advanceTimersByTimeAsync(0);
    await rejected;
    await vi.advanceTimersByTimeAsync(1000);
    expect(client.closePointInTime.mock.calls[0][1]?.signal?.aborted).toBe(
      true,
    );
  });

  it("disconnect closes the physical transport independently of a stuck PIT close and bounds a stuck transport close", async () => {
    const { driver, client } = createElasticsearchPitMock(
      9,
      3,
      timeoutSettings,
    );
    const chunks = driver.exportTableChunks(request);
    await chunks.next();
    client.closePointInTime.mockImplementationOnce(
      () => new Promise(() => undefined),
    );
    client.close.mockImplementationOnce(() => new Promise(() => undefined));
    let settled = false;
    const disconnect = driver.disconnect().then(() => {
      settled = true;
    });
    await vi.advanceTimersByTimeAsync(100);
    expect(client.close).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(1000);
    await disconnect;
    expect(settled).toBe(true);
    await expect(chunks.next()).rejects.toMatchObject({ name: "AbortError" });
  });

  it("forwards the page-only export cancellation signal through getPage", async () => {
    const { service, client } = createElasticsearchPitMock(
      9,
      3,
      timeoutSettings,
    );
    client.search.mockImplementationOnce(() => new Promise(() => undefined));
    const controller = new AbortController();
    const rejected = expect(
      service.getPage(
        "es-pit",
        "default",
        "indices",
        "records",
        3,
        3,
        [],
        null,
        true,
        controller.signal,
      ),
    ).rejects.toMatchObject({ name: "AbortError" });
    await vi.advanceTimersByTimeAsync(0);
    controller.abort();
    await vi.advanceTimersByTimeAsync(0);
    await rejected;
    expect(client.search.mock.calls[0][1]?.signal?.aborted).toBe(true);
    await vi.advanceTimersByTimeAsync(100);
  });
});

describe("B11 restricted metadata privileges", () => {
  it.each([
    1, 3, 9999,
  ])("exports a principal denied settings access with an unknown window of %i", async (window) => {
    const { driver, client, documents, snapshots } = createElasticsearchPitMock(
      17,
      window,
    );
    client.indices.getSettings.mockRejectedValueOnce(
      elasticsearchResponseError(403, "missing view_index_metadata privilege"),
    );
    const rows = await collect(driver.exportTableChunks(request));
    expect(rows.map((row) => row._id)).toEqual(documents.map((hit) => hit._id));
    expect(snapshots.size).toBe(0);
    const validRequests = client.search.mock.calls.filter(
      ([args]) => (args.size ?? 0) <= window,
    );
    expect(validRequests.length).toBeGreaterThan(0);
    expect(new Set(rows.map((row) => row._id)).size).toBe(17);
  });

  it("reads a deep page with only PIT/search privileges and an unknown small custom window", async () => {
    const { driver, client, documents } = createElasticsearchPitMock(21, 3);
    client.indices.getSettings.mockRejectedValueOnce(
      elasticsearchResponseError(403, "forbidden"),
    );
    const result = await driver.readTablePage({ ...pageRequest, pageSize: 7 });
    expect(result.rows.map((row) => row._id)).toEqual(
      documents.slice(14, 21).map((hit) => hit._id),
    );
    expect(result.totalCount).toBe(21);
  });

  it.each([
    401, 404, 500,
  ])("surfaces settings HTTP %i instead of falling back", async (status) => {
    const { driver, client } = createElasticsearchPitMock(3);
    client.indices.getSettings.mockRejectedValueOnce(
      elasticsearchResponseError(status, "settings failed"),
    );
    await expect(collect(driver.exportTableChunks(request))).rejects.toThrow(
      "settings failed",
    );
    expect(client.openPointInTime).not.toHaveBeenCalled();
  });

  it("does not retry unrelated search failures after metadata denial", async () => {
    const { driver, client } = createElasticsearchPitMock(3);
    client.indices.getSettings.mockRejectedValueOnce(
      elasticsearchResponseError(403, "forbidden"),
    );
    client.search.mockRejectedValueOnce(
      elasticsearchResponseError(400, "malformed query"),
    );
    await expect(collect(driver.exportTableChunks(request))).rejects.toThrow(
      "malformed query",
    );
    expect(client.search).toHaveBeenCalledTimes(1);
    expect(client.closePointInTime).toHaveBeenCalledTimes(1);
  });
});
