import { describe, expect, it, vi } from "vitest";
import { ElasticsearchDriver } from "../../src/extension/dbDrivers/elasticsearch";
import type { DriverTablePageRequest } from "../../src/extension/dbDrivers/types";
import {
  elasticsearchIndexSettings,
  elasticsearchSourceMetadata,
} from "../support/elasticsearchIdentity";
import { elasticsearchResponseError } from "../support/elasticsearchPitMock";

const fullIndex = "records-000001";
const otherIndex = "records-000002";
const otherUuid = "w2MS3cbPTM6-pYMgj5ds-A";
const request: DriverTablePageRequest = {
  database: "default",
  schema: "indices",
  table: fullIndex,
  page: 1,
  pageSize: 10,
  filters: [],
  sort: null,
  skipCount: false,
};
const hit = {
  _index: fullIndex,
  _id: "doc-1",
  _seq_no: 5,
  _primary_term: 1,
  _source: { visible: "before" },
  sort: [0],
};
const blockMessage = /require a complete stored _source/;
const full = elasticsearchSourceMetadata(fullIndex);
const filtered = elasticsearchSourceMetadata(fullIndex, undefined, {
  _source: { excludes: ["hidden"] },
});

function fixture() {
  const driver = new ElasticsearchDriver({
    id: "es",
    name: "ES",
    type: "elasticsearch",
  });
  const client = {
    indices: {
      getSettings: vi
        .fn()
        .mockResolvedValue(elasticsearchIndexSettings(fullIndex)),
      get: vi.fn().mockResolvedValue(full),
    },
    openPointInTime: vi.fn().mockResolvedValue({ id: "pit" }),
    closePointInTime: vi.fn().mockResolvedValue({ succeeded: true }),
    search: vi.fn().mockResolvedValue({ hits: { total: 1, hits: [hit] } }),
    index: vi.fn().mockResolvedValue({ result: "created" }),
    update: vi.fn().mockResolvedValue({ result: "updated" }),
    delete: vi.fn().mockResolvedValue({ result: "deleted" }),
  };
  Object.assign(driver, { client, connected: true });
  return { driver, client };
}

function updateFor(row: Record<string, unknown>) {
  return {
    primaryKeys: { _id: row._id, _identity: row._identity },
    changes: { _source: { visible: "after" } },
    originalValues: { _source: row._source },
  };
}

function previewUpdate(
  driver: ElasticsearchDriver,
  table: string,
  edit: ReturnType<typeof updateFor>,
) {
  return driver.buildMutationPreviewStatements(
    "update",
    request.database,
    request.schema,
    table,
    edit,
  );
}

describe("Elasticsearch Table Viewer source completeness", () => {
  it.each([
    full,
    elasticsearchSourceMetadata(fullIndex, undefined, {
      _source: { mode: "stored", includes: [], excludes: [] },
    }),
    {
      [fullIndex]: {
        ...full[fullIndex],
        defaults: {
          "index.mode": "standard",
          "index.mapping.source.mode": "STORED",
        },
      },
    },
  ])("allows editing a verified stored source", async (metadata) => {
    const { driver, client } = fixture();
    client.indices.get.mockResolvedValue(metadata);
    const page = await driver.readTablePage(request);
    expect(page.rowUpdateBlockReasons).toEqual([null]);
    const edit = updateFor(page.rows[0]);
    await expect(previewUpdate(driver, request.table, edit)).resolves.toEqual([
      expect.stringContaining(`POST /${fullIndex}/_update/doc-1`),
    ]);
    await expect(
      driver.updateRows({ ...request, updates: [edit] }),
    ).resolves.toMatchObject({ affectedRows: 1 });
    expect(client.indices.get).toHaveBeenCalledTimes(3);
    expect(client.update).toHaveBeenCalledWith(
      expect.objectContaining({
        index: fullIndex,
        script: expect.objectContaining({
          params: { original: hit._source, next: edit.changes._source },
        }),
      }),
      undefined,
    );
  });

  it.each([
    ["filtered excludes", filtered],
    [
      "filtered includes",
      elasticsearchSourceMetadata(fullIndex, undefined, {
        _source: { includes: ["visible"] },
      }),
    ],
    [
      "disabled",
      elasticsearchSourceMetadata(fullIndex, undefined, {
        _source: { enabled: false },
      }),
    ],
    [
      "synthetic mapping",
      elasticsearchSourceMetadata(fullIndex, undefined, {
        _source: { mode: "synthetic" },
      }),
    ],
    [
      "synthetic setting",
      {
        [fullIndex]: {
          ...full[fullIndex],
          settings: {
            ...full[fullIndex].settings,
            "index.mapping.source.mode": "synthetic",
          },
          defaults: { "index.mapping.source.mode": "stored" },
        },
      },
    ],
    [
      "implicit mode of a nonstandard index",
      {
        [fullIndex]: {
          ...full[fullIndex],
          defaults: { "index.mode": "time_series" },
        },
      },
    ],
    [
      "missing settings defaults",
      {
        [fullIndex]: {
          ...full[fullIndex],
          defaults: undefined,
        },
      },
    ],
    [
      "missing mappings",
      {
        [fullIndex]: {
          ...full[fullIndex],
          mappings: undefined,
        },
      },
    ],
    ["wrong concrete index", elasticsearchSourceMetadata(otherIndex)],
    ["metadata denied", elasticsearchResponseError(403, "forbidden")],
    ["metadata error", elasticsearchResponseError(500, "metadata unavailable")],
  ])("keeps %s readable/exportable without allowing updates", async (_name, metadata) => {
    const { driver, client } = fixture();
    if (metadata instanceof Error)
      client.indices.get.mockRejectedValueOnce(metadata);
    else client.indices.get.mockResolvedValueOnce(metadata);
    const page = await driver.readTablePage(request);
    expect(page.rowUpdateBlockReasons).toEqual([
      expect.stringMatching(blockMessage),
    ]);
    expect(page.rows[0]).toMatchObject({
      _identity: expect.any(String),
      _source: JSON.stringify(hit._source),
    });
    const edit = updateFor(page.rows[0]);
    // Subsequent metadata availability must not promote a read whose source was unverified.
    await expect(previewUpdate(driver, request.table, edit)).rejects.toThrow(
      blockMessage,
    );
    await expect(
      driver.updateRows({ ...request, updates: [edit] }),
    ).rejects.toMatchObject({
      message: expect.stringMatching(blockMessage),
      result: {
        affectedRows: 0,
        updateRowOutcomes: [{ status: "not_applied" }],
      },
    });
    expect(client.update).not.toHaveBeenCalled();
    expect(client.index).not.toHaveBeenCalled();
    const exported = [];
    for await (const chunk of driver.exportTableChunks({
      ...request,
      chunkSize: 10,
    }))
      exported.push(...chunk.rows);
    expect(exported[0]._source).toBe(JSON.stringify(hit._source));
    // Neither delete nor insert depends on the current document's source.
    await expect(
      driver.deleteRows({
        ...request,
        primaryKeyValuesList: [edit.primaryKeys],
      }),
    ).resolves.toMatchObject({ affectedRows: 1 });
    await expect(
      driver.insertRow({
        ...request,
        values: { _id: "new", _source: { visible: "new" } },
      }),
    ).resolves.toMatchObject({ affectedRows: 1 });
    expect(client.indices.get).toHaveBeenCalledTimes(1);
  });

  it("does not mark an absent hit source editable even when index metadata is full", async () => {
    const { driver, client } = fixture();
    client.search.mockResolvedValue({
      hits: { total: 1, hits: [{ ...hit, _source: undefined }] },
    });
    const page = await driver.readTablePage(request);
    expect(page.rowUpdateBlockReasons).toEqual([
      expect.stringMatching(blockMessage),
    ]);
    await expect(
      previewUpdate(driver, request.table, updateFor(page.rows[0])),
    ).rejects.toThrow(blockMessage);
  });

  it.each([
    "alias",
    "data stream",
  ])("binds edit capability to the hit's concrete index behind a %s", async (kind) => {
    const { driver, client } = fixture();
    const table = kind === "alias" ? "current" : "logs";
    client.indices.getSettings.mockResolvedValue({
      ...elasticsearchIndexSettings(fullIndex),
      ...elasticsearchIndexSettings(otherIndex, otherUuid),
    });
    client.indices.get.mockResolvedValueOnce({
      ...full,
      ...elasticsearchSourceMetadata(otherIndex, otherUuid, {
        _source: { excludes: ["hidden"] },
      }),
    });
    client.search.mockResolvedValue({
      hits: {
        total: 2,
        hits: [hit, { ...hit, _id: "doc-2", _index: otherIndex, sort: [1] }],
      },
    });
    const page = await driver.readTablePage({
      ...request,
      table,
      sort: { column: "_id", direction: "desc" },
    });
    expect(page.rows.map((row) => row._id)).toEqual(["doc-2", "doc-1"]);
    expect(page.rowUpdateBlockReasons).toEqual([
      expect.stringMatching(blockMessage),
      null,
    ]);
    await expect(
      previewUpdate(driver, table, updateFor(page.rows[0])),
    ).rejects.toThrow(blockMessage);
    // After browsing, the alias/data stream no longer needs to resolve to the old index.
    client.indices.get.mockImplementation(async ({ index }) =>
      index === fullIndex
        ? full
        : elasticsearchSourceMetadata(otherIndex, otherUuid),
    );
    await expect(
      driver.updateRows({
        ...request,
        table,
        updates: [updateFor(page.rows[1])],
      }),
    ).resolves.toMatchObject({ affectedRows: 1 });
    expect(
      client.indices.get.mock.calls.map(([params]) => params.index),
    ).toEqual([table, fullIndex]);
    expect(client.update).toHaveBeenCalledWith(
      expect.objectContaining({ index: fullIndex, id: hit._id }),
      undefined,
    );
  });

  it.each([
    "filtered",
    "forbidden",
    "missing mappings",
  ])("rechecks source for preview and freshly at dispatch: %s", async (mode) => {
    const { driver, client } = fixture();
    const page = await driver.readTablePage(request);
    const edit = updateFor(page.rows[0]);
    await expect(
      previewUpdate(driver, request.table, edit),
    ).resolves.toHaveLength(1);
    if (mode === "forbidden")
      client.indices.get.mockRejectedValue(
        elasticsearchResponseError(403, "metadata forbidden"),
      );
    else
      client.indices.get.mockResolvedValue(
        mode === "filtered"
          ? filtered
          : { [fullIndex]: { ...full[fullIndex], mappings: undefined } },
      );
    await expect(previewUpdate(driver, request.table, edit)).rejects.toThrow(
      blockMessage,
    );
    for (const update of [
      edit,
      { ...edit, originalValues: undefined },
      { ...edit, changes: { visible: "partial" }, originalValues: undefined },
    ]) {
      await expect(
        driver.updateRows({ ...request, updates: [update] }),
      ).rejects.toThrow(blockMessage);
    }
    expect(client.update).not.toHaveBeenCalled();
    expect(client.index).not.toHaveBeenCalled();
  });

  it("does not reuse a successful check for a later write to the same index", async () => {
    const { driver, client } = fixture();
    const page = await driver.readTablePage(request);
    const edit = updateFor(page.rows[0]);
    client.indices.get
      .mockResolvedValueOnce(full)
      .mockResolvedValueOnce(filtered);
    await expect(
      driver.updateRows({ ...request, updates: [edit, edit] }),
    ).rejects.toMatchObject({
      message: expect.stringMatching(blockMessage),
      result: {
        affectedRows: 1,
        updateRowOutcomes: [{ status: "applied" }, { status: "not_applied" }],
      },
    });
    expect(client.update).toHaveBeenCalledTimes(1);
  });
});
