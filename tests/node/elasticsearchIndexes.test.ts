import { errors } from "@elastic/elasticsearch";
import { describe, expect, it, vi } from "vitest";
import { ElasticsearchDriver } from "../../src/extension/dbDrivers/elasticsearch";
import { runWithMetadataCancellation } from "../../src/extension/dbDrivers/metadataCancellation";
import { loadTableDetail } from "../../src/extension/schema/schemaLoaders";

function responseError(statusCode: number, type?: string) {
  return new errors.ResponseError({
    statusCode,
    body: { error: type ? { type, reason: type } : "Not Found" },
    headers: {},
    meta: {} as never,
    warnings: null,
  });
}

function fixture() {
  const driver = new ElasticsearchDriver({
    id: "es-indexes",
    name: "Search",
    type: "elasticsearch",
  });
  const get = vi.fn().mockResolvedValue({ records: {} });
  Object.assign(driver, { client: { indices: { get } }, connected: true });
  return { driver, get };
}

describe("Elasticsearch getIndexes error boundary", () => {
  it("keeps the synthetic identity index on success", async () => {
    const { driver, get } = fixture();
    await expect(
      driver.getIndexes("default", "indices", "records"),
    ).resolves.toEqual([
      {
        name: "records_id_idx",
        columns: ["_id", "_identity"],
        unique: true,
        primary: true,
      },
    ]);
    expect(get).toHaveBeenCalledWith({ index: "records" });
  });

  it("returns empty only for an explicitly missing Elasticsearch index", async () => {
    const { driver, get } = fixture();
    get.mockRejectedValue(responseError(404, "index_not_found_exception"));
    await expect(
      driver.getIndexes("default", "indices", "records"),
    ).resolves.toEqual([]);
  });

  it.each([
    ["authentication", responseError(401, "security_exception")],
    ["authorization", responseError(403, "security_exception")],
    ["network", new errors.ConnectionError("socket hang up")],
    ["timeout", new errors.TimeoutError("Request timed out")],
    ["cancellation", new errors.RequestAbortedError("Request aborted")],
    ["server", responseError(503, "unavailable_shards_exception")],
    ["unrelated 404", responseError(404, "resource_not_found_exception")],
    ["proxy 404", responseError(404)],
    ["mismatched status", responseError(403, "index_not_found_exception")],
    ["unstructured", new Error("index_not_found_exception")],
  ])("propagates %s errors unchanged and permits recovery", async (_kind, error) => {
    const { driver, get } = fixture();
    get.mockRejectedValueOnce(error);
    await expect(
      driver.getIndexes("default", "indices", "records"),
    ).rejects.toBe(error);
    await expect(
      driver.getIndexes("default", "indices", "records"),
    ).resolves.toHaveLength(1);
  });

  it("does not turn a disconnected driver into an empty index list", async () => {
    const { driver } = fixture();
    Object.assign(driver, { client: null, connected: false });
    await expect(
      driver.getIndexes("default", "indices", "records"),
    ).rejects.toThrow();
  });

  it("preserves metadata cancellation and passes the signal to transport", async () => {
    const { driver, get } = fixture();
    const controller = new AbortController();
    const run = () =>
      runWithMetadataCancellation(controller.signal, Infinity, () =>
        driver.getIndexes("default", "indices", "records"),
      );
    await run();
    expect(get).toHaveBeenCalledWith(
      { index: "records" },
      { signal: controller.signal },
    );
    const cancelled = new DOMException("Metadata cancelled", "AbortError");
    controller.abort(cancelled);
    await expect(run()).rejects.toBe(cancelled);
    expect(get).toHaveBeenCalledTimes(1);
  });

  it("keeps the production schema manifest's Indexes section not applicable", async () => {
    const { driver, get } = fixture();
    vi.spyOn(driver, "describeColumns").mockResolvedValue([]);
    get.mockRejectedValue(responseError(403, "security_exception"));
    const state = await loadTableDetail(driver, {
      connectionId: "es-indexes",
      database: "default",
      schema: "indices",
      table: "records",
      objectKind: "table",
    });
    expect(driver.getEntityManifest().tableSections.indexes).toBe(
      "not_applicable",
    );
    expect(get).not.toHaveBeenCalled();
    expect(state).toMatchObject({
      status: "loaded",
      isPartial: false,
      snapshot: { indexes: { status: "loaded", items: [] } },
    });
  });
});
