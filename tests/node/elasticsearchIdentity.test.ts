import { describe, expect, it, vi } from "vitest";
import type { ConnectionManager } from "../../src/extension/connectionManager";
import { ElasticsearchDriver } from "../../src/extension/dbDrivers/elasticsearch";
import type { DriverTablePageRequest } from "../../src/extension/dbDrivers/types";
import {
  applyChangesTransactional,
  prepareApplyChangesPlan,
} from "../../src/extension/table/tableMutationExecution";
import { TableMutationService } from "../../src/extension/table/tableMutationService";
import { elasticsearchKeys } from "../support/elasticsearchIdentity";

const request: DriverTablePageRequest = {
  database: "default",
  schema: "indices",
  table: "current",
  page: 1,
  pageSize: 10,
  filters: [],
  sort: null,
  skipCount: false,
};
const hits = [
  {
    _index: "records-1",
    _id: "same-id",
    _seq_no: 0,
    _primary_term: 1,
    _source: { tenant: "default" },
    sort: [0],
  },
  {
    _index: "records-1",
    _id: "same-id",
    _routing: "tenant /+?&0",
    _seq_no: 3,
    _primary_term: 1,
    _source: { tenant: "selected" },
    sort: [1],
  },
];

function fixture() {
  const driver = new ElasticsearchDriver({
    id: "es-identity",
    name: "ES",
    type: "elasticsearch",
  });
  const client = {
    search: vi.fn().mockResolvedValue({ hits: { total: { value: 2 }, hits } }),
    openPointInTime: vi.fn().mockResolvedValue({ id: "pit" }),
    closePointInTime: vi.fn().mockResolvedValue({ succeeded: true }),
    indices: {
      getSettings: vi.fn().mockResolvedValue({
        "records-1": { settings: { index: { max_result_window: "100" } } },
      }),
      get: vi.fn().mockResolvedValue({}),
    },
    delete: vi.fn().mockResolvedValue({ result: "deleted" }),
    update: vi.fn().mockResolvedValue({ result: "updated" }),
    index: vi.fn().mockResolvedValue({ result: "updated" }),
  };
  Object.assign(driver, { client, connected: true });
  return { driver, client };
}

function primaryKeys(
  page: Awaited<ReturnType<ElasticsearchDriver["readTablePage"]>>,
  rowIndex = 1,
) {
  return Object.fromEntries(
    page.columns
      .filter((column) => column.isPrimaryKey)
      .map((column) => [column.name, page.rows[rowIndex][column.name]]),
  );
}

describe("Elasticsearch read-to-mutation identity", () => {
  it("preserves the read-only identity through Viewer apply and delete preflight", async () => {
    const { driver, client } = fixture();
    const page = await driver.readTablePage(request);
    const manager = {
      getConnection: () => ({ type: "elasticsearch", readOnly: false }),
      getDriver: () => driver,
    } as unknown as ConnectionManager;
    const updates = [
      {
        primaryKeys: primaryKeys(page),
        changes: { _source: '{"tenant":"edited"}' },
        originalValues: { _source: '{"tenant":"selected"}' },
      },
    ];
    const prepared = prepareApplyChangesPlan(
      manager,
      "es-identity",
      "default",
      "indices",
      "current",
      updates,
      page.columns,
    );
    expect(prepared.executable).toBe(true);
    const applied = await applyChangesTransactional(
      manager,
      "es-identity",
      "default",
      "indices",
      "current",
      updates,
      page.columns,
    );
    expect(applied.success).toBe(true);
    expect(client.update.mock.calls[0][0]).toMatchObject({
      index: "records-1",
      routing: "tenant /+?&0",
      if_seq_no: 3,
      if_primary_term: 1,
    });
    const service = new TableMutationService(manager, {
      getColumns: async () => page.columns,
    });
    const plan = await service.prepareDeleteRowsPlan(
      "es-identity",
      "default",
      "indices",
      "current",
      [primaryKeys(page)],
    );
    expect(plan?.primaryKeyValuesList).toEqual([primaryKeys(page)]);
    if (!plan) throw new Error("Expected a delete plan");
    await service.executePreparedDeletePlan(plan);
    expect(client.delete.mock.calls[0][0]).toMatchObject({
      index: "records-1",
      routing: "tenant /+?&0",
      if_seq_no: 3,
    });
  });

  it("does not require a read identity when inserting a new non-routing row", async () => {
    const { driver, client } = fixture();
    const columns = await driver.describeColumns(
      "default",
      "indices",
      "current",
    );
    const manager = {
      getConnection: () => ({ type: "elasticsearch", readOnly: false }),
      getDriver: () => driver,
    } as unknown as ConnectionManager;
    const service = new TableMutationService(manager, {
      getColumns: async () => columns,
    });
    await service.insertRow("es-identity", "default", "indices", "current", {
      _id: "new",
      _source: '{"tenant":"new"}',
    });
    expect(client.index).toHaveBeenCalledWith(
      {
        index: "current",
        id: "new",
        document: { tenant: "new" },
        op_type: "create",
        refresh: "wait_for",
      },
      undefined,
    );
  });

  it.each([
    "PUT",
    "POST",
  ])("preserves an application _identity in raw %s document bodies", async (method) => {
    const { driver, client } = fixture();
    const document = { _identity: "business-value", value: 1 };
    await driver.query(
      `${method} /records-1/_doc/example\n${JSON.stringify(document)}`,
    );
    expect(client.index.mock.calls[0][0].document).toEqual(document);
  });

  it.each([
    "insert",
    "replace",
  ] as const)("preserves an application _source._identity when executing %s previews", async (operation) => {
    const { driver, client } = fixture();
    const document = { _identity: "business-value", value: 1 };
    let preview: string;
    if (operation === "insert") {
      const values = { _id: "new", _source: JSON.stringify(document) };
      preview = driver.buildMutationPreviewStatement(
        "insert",
        "default",
        "indices",
        "records-1",
        { values },
      );
      await driver.insertRow({
        database: "default",
        schema: "indices",
        table: "records-1",
        values,
      });
    } else {
      const page = await driver.readTablePage(request);
      const update = {
        primaryKeys: primaryKeys(page),
        changes: { _source: JSON.stringify(document) },
      };
      preview = driver.buildMutationPreviewStatement(
        "update",
        "default",
        "indices",
        "current",
        update,
      );
      await driver.updateRows({
        database: "default",
        schema: "indices",
        table: "current",
        updates: [update],
      });
    }
    expect(client.index.mock.calls[0][0].document).toEqual(document);
    await driver.query(preview);
    expect(client.index).toHaveBeenCalledTimes(2);
    expect(client.index.mock.calls[1][0].document).toEqual(document);
  });

  it("allows a safe repair with imprecise display originals and a complete read identity", async () => {
    const { driver } = fixture();
    const columns = await driver.describeColumns(
      "default",
      "indices",
      "current",
    );
    const manager = {
      getConnection: () => ({ readOnly: false }),
      getDriver: () => driver,
    } as unknown as ConnectionManager;
    const prepared = prepareApplyChangesPlan(
      manager,
      "es-identity",
      "default",
      "indices",
      "current",
      [
        {
          primaryKeys: elasticsearchKeys(
            driver,
            "same-id",
            "records-1",
            "current",
          ),
          changes: { _source: '{"amount":42}' },
          originalValues: { _source: '{"amount":9007199254740993}' },
        },
      ],
      columns,
    );
    expect(prepared.executable).toBe(true);
  });

  it.each([
    null,
    { column: "_id", direction: "asc" as const },
  ])("keeps routed twins distinct through page/PK/delete/preview (sort=%j)", async (sort) => {
    const { driver, client } = fixture();
    const page = await driver.readTablePage({ ...request, sort });
    const keys = primaryKeys(page);
    expect(keys._identity).toEqual(expect.any(String));
    expect(keys).not.toEqual(primaryKeys(page, 0));
    expect(page.columns.filter((column) => column.isPrimaryKey)).toMatchObject([
      { name: "_id", nullable: false },
      { name: "_identity", nullable: false, isReadOnly: true },
    ]);
    expect(
      await driver.getIndexes("default", "indices", "current"),
    ).toMatchObject([
      { columns: ["_id", "_identity"], unique: true, primary: true },
    ]);
    expect(client.search.mock.calls[0][0]).toMatchObject({
      seq_no_primary_term: true,
    });
    await driver.deleteRows({ ...request, primaryKeyValuesList: [keys] });
    const expected = {
      index: "records-1",
      id: "same-id",
      routing: "tenant /+?&0",
      if_seq_no: 3,
      if_primary_term: 1,
      refresh: "wait_for",
    };
    expect(client.delete).toHaveBeenLastCalledWith(expected, undefined);
    const preview = driver.buildMutationPreviewStatement(
      "delete",
      "default",
      "indices",
      "current",
      { primaryKeys: keys },
    );
    await driver.query(preview);
    expect(client.delete).toHaveBeenLastCalledWith(expected);
  });

  it("preserves identity in exported rows, including no-routing rows", async () => {
    const { driver, client } = fixture();
    const chunks = driver.exportTableChunks({ ...request, chunkSize: 10 });
    const chunk = await chunks.next();
    if (chunk.done) throw new Error("Expected an export chunk");
    expect(chunk.value?.rows.map((row) => row._identity)).toEqual([
      expect.any(String),
      expect.any(String),
    ]);
    const keys = {
      _id: chunk.value.rows[0]._id,
      _identity: chunk.value.rows[0]._identity,
    };
    await driver.deleteRows({ ...request, primaryKeyValuesList: [keys] });
    expect(client.delete).toHaveBeenCalledWith(
      {
        index: "records-1",
        id: "same-id",
        if_seq_no: 0,
        if_primary_term: 1,
        refresh: "wait_for",
      },
      undefined,
    );
    await chunks.return(undefined);
  });

  it.each([
    "partial",
    "replace",
    "conditional",
  ])("keeps update preview and actual write identical: %s", async (mode) => {
    const { driver, client } = fixture();
    const page = await driver.readTablePage(request);
    const keys = primaryKeys(page);
    const changes =
      mode === "partial"
        ? { tenant: "edited" }
        : { _source: '{"tenant":"edited"}' };
    const originalValues =
      mode === "conditional" ? { _source: '{"tenant":"selected"}' } : undefined;
    await driver.updateRows({
      ...request,
      updates: [{ primaryKeys: keys, changes, originalValues }],
    });
    const method = mode === "replace" ? client.index : client.update;
    const actual = method.mock.calls[0][0];
    expect(actual).toMatchObject({
      index: "records-1",
      routing: "tenant /+?&0",
      if_seq_no: 3,
      if_primary_term: 1,
    });
    expect(JSON.stringify(actual)).not.toContain("_identity");
    const preview = driver.buildMutationPreviewStatement(
      "update",
      "default",
      "indices",
      "current",
      { primaryKeys: keys, changes, originalValues },
    );
    await driver.query(preview);
    expect(method.mock.calls[1][0]).toEqual(actual);
  });

  it.each([
    "001",
    "true",
    "false",
    "-12",
  ])("preserves literal routing %s when executing previews", async (routing) => {
    const { driver, client } = fixture();
    const keys = elasticsearchKeys(
      driver,
      "same-id",
      "records-1",
      "current",
      routing,
    );
    const preview = driver.buildMutationPreviewStatement(
      "delete",
      "default",
      "indices",
      "current",
      { primaryKeys: keys },
    );
    await driver.query(preview);
    expect(client.delete.mock.calls[0][0]).toMatchObject({ routing });
  });

  const invalidCases = [
    "missing",
    "null",
    "number",
    "malformed",
    "truncated",
    "routing removed",
    "id mismatch",
    "foreign connection",
    "old epoch",
    "wrong table",
  ];
  it.each(
    invalidCases,
  )("rejects %s identity before preview, update, or delete can write", async (kind) => {
    const { driver, client } = fixture();
    let keys: Record<string, unknown> = elasticsearchKeys(
      driver,
      "same-id",
      "records-1",
      "current",
      "tenant-0",
    );
    let table = "current";
    if (kind === "missing") keys = { _id: "same-id" };
    if (kind === "null") keys._identity = null;
    if (kind === "number") keys._identity = 42;
    if (kind === "malformed") keys._identity = "broken";
    if (kind === "truncated")
      keys._identity = String(keys._identity).split(".")[0];
    if (kind === "routing removed") {
      const [payload, signature] = String(keys._identity).split(".");
      const parsed = JSON.parse(Buffer.from(payload, "base64url").toString());
      parsed[5] = null;
      keys._identity = `${Buffer.from(JSON.stringify(parsed)).toString("base64url")}.${signature}`;
    }
    if (kind === "id mismatch") keys._id = "other-id";
    if (kind === "foreign connection")
      keys = elasticsearchKeys(
        fixture().driver,
        "same-id",
        "records-1",
        "current",
      );
    if (kind === "old epoch") Object.assign(driver, { connectionEpoch: 1 });
    if (kind === "wrong table") table = "other";
    for (const operation of ["delete", "update"] as const) {
      expect(() =>
        driver.buildMutationPreviewStatement(
          operation,
          "default",
          "indices",
          table,
          { primaryKeys: keys, changes: { _source: "{}" } },
        ),
      ).toThrow(/Refresh the table/);
    }
    await expect(
      driver.buildMutationPreviewStatements(
        "delete",
        "default",
        "indices",
        table,
        { primaryKeyValuesList: [keys] },
      ),
    ).rejects.toThrow(/Refresh the table/);
    await expect(
      driver.deleteRows({ ...request, table, primaryKeyValuesList: [keys] }),
    ).rejects.toMatchObject({
      deleteResult: { affectedRows: 0, rowOutcomes: [{ status: "failed" }] },
    });
    await expect(
      driver.updateRows({
        ...request,
        table,
        updates: [{ primaryKeys: keys, changes: { _source: "{}" } }],
      }),
    ).rejects.toMatchObject({
      result: {
        affectedRows: 0,
        updateRowOutcomes: [{ status: "not_applied" }],
      },
    });
    expect(client.delete).not.toHaveBeenCalled();
    expect(client.update).not.toHaveBeenCalled();
    expect(client.index).not.toHaveBeenCalled();
  });

  it.each([
    "_index",
    "_seq_no",
    "_primary_term",
  ])("makes incomplete %s metadata read-only", async (field) => {
    const { driver, client } = fixture();
    const incomplete: Record<string, unknown> = { ...hits[1] };
    delete incomplete[field];
    client.search.mockResolvedValue({
      hits: { total: { value: 1 }, hits: [incomplete] },
    });
    const page = await driver.readTablePage(request);
    expect(page.rows[0]._identity).toBeNull();
    await expect(
      driver.deleteRows({
        ...request,
        primaryKeyValuesList: [primaryKeys(page, 0)],
      }),
    ).rejects.toThrow(/Refresh/);
    expect(client.delete).not.toHaveBeenCalled();
  });

  it.each([
    "_id",
    "_identity",
  ])("does not permit changing %s", async (field) => {
    const { driver, client } = fixture();
    const keys = elasticsearchKeys(driver, "same-id", "records-1", "current");
    const changes = { [field]: "different" };
    expect(() =>
      driver.buildMutationPreviewStatement(
        "update",
        "default",
        "indices",
        "current",
        { primaryKeys: keys, changes },
      ),
    ).toThrow(/read-only/);
    await expect(
      driver.updateRows({
        ...request,
        updates: [{ primaryKeys: keys, changes }],
      }),
    ).rejects.toThrow(/does not support updating/);
    expect(client.update).not.toHaveBeenCalled();
    expect(client.index).not.toHaveBeenCalled();
  });
});
