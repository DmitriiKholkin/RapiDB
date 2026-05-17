import { describe, expect, it, vi } from "vitest";
import { ElasticsearchDriver } from "../../src/extension/dbDrivers/elasticsearch";

function createDriver() {
  const driver = new ElasticsearchDriver({
    id: "elasticsearch-data-test",
    name: "Elasticsearch Data Test",
    type: "elasticsearch",
    host: "localhost",
  });

  const search = vi.fn();
  const getMapping = vi.fn();
  const catIndices = vi.fn();
  const index = vi.fn().mockResolvedValue({ result: "created", _id: "doc-3" });

  (
    driver as unknown as {
      client: {
        search: typeof search;
        index: typeof index;
        cat: { indices: typeof catIndices };
        indices: {
          getMapping: typeof getMapping;
          get: ReturnType<typeof vi.fn>;
        };
      } | null;
      connected: boolean;
    }
  ).client = {
    search,
    index,
    cat: { indices: catIndices },
    indices: { getMapping, get: vi.fn().mockResolvedValue({}) },
  };
  (driver as unknown as { connected: boolean }).connected = true;

  return { driver, search, getMapping, catIndices, index };
}

describe("ElasticsearchDriver — metadata and pages", () => {
  it("lists sorted index names while ignoring malformed entries", async () => {
    const { driver, catIndices } = createDriver();
    catIndices.mockResolvedValue([{ index: "orders" }, {}, { index: "users" }]);

    await expect(driver.listObjects()).resolves.toEqual([
      { schema: "indices", name: "orders", type: "table" },
      { schema: "indices", name: "users", type: "table" },
    ]);
    expect(catIndices).toHaveBeenCalledWith({ format: "json" });
  });

  it("derives mapped column metadata and flattened rows for page reads", async () => {
    const { driver, search, getMapping } = createDriver();
    search.mockResolvedValue({
      hits: {
        hits: [
          {
            _id: "doc-2",
            _source: {
              created_at: "2026-04-02T10:30:00Z",
              email: "bravo@example.com",
              profile: { tier: "pro" },
              active: false,
            },
          },
          {
            _id: "doc-1",
            _source: {
              created_at: "2026-04-01T09:00:00Z",
              email: "alpha@example.com",
              profile: { tier: "free" },
              active: true,
              status: null,
            },
          },
        ],
      },
    });
    getMapping.mockResolvedValue({
      users: {
        mappings: {
          properties: {
            created_at: { type: "date" },
            email: { type: "keyword" },
            profile: { type: "object" },
            active: { type: "boolean" },
            status: { type: "keyword", null_value: "unknown" },
          },
        },
      },
    });

    const described = await driver.describeColumns(
      "default",
      "indices",
      "users",
    );
    const page = await driver.readTablePage({
      database: "default",
      schema: "indices",
      table: "users",
      page: 1,
      pageSize: 10,
      filters: [],
      sort: { column: "_id", direction: "asc" },
      skipCount: false,
    });

    expect(described).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          name: "_id",
          isPrimaryKey: true,
          primaryKeyOrdinal: 1,
        }),
        expect.objectContaining({
          name: "created_at",
          type: "datetime",
          nativeType: "datetime",
          category: "datetime",
        }),
        expect.objectContaining({
          name: "email",
          type: "text",
          nativeType: "text",
          category: "text",
        }),
        expect.objectContaining({
          name: "profile",
          type: "json",
          nativeType: "json",
          category: "json",
        }),
        expect.objectContaining({
          name: "status",
          defaultValue: "unknown",
        }),
      ]),
    );
    expect(page.totalCount).toBe(2);
    expect(page.rows).toEqual([
      {
        _id: "doc-1",
        active: true,
        created_at: "2026-04-01T09:00:00Z",
        email: "alpha@example.com",
        profile: '{"tier":"free"}',
        status: null,
      },
      {
        _id: "doc-2",
        active: false,
        created_at: "2026-04-02T10:30:00Z",
        email: "bravo@example.com",
        profile: '{"tier":"pro"}',
      },
    ]);
    expect(search).toHaveBeenCalledWith({
      index: "users",
      size: 1000,
      query: { match_all: {} },
      sort: ["_doc"],
    });
  });

  it("returns flattened rows from text-mode search queries", async () => {
    const { driver, search } = createDriver();
    search.mockResolvedValue({
      hits: {
        hits: [
          {
            _id: "doc-9",
            _source: {
              title: "Release note",
              tags: ["product", "beta"],
            },
          },
        ],
      },
    });

    const result = await driver.query(
      'search {"index":"notes","query":{"match":{"title":"Release"}},"size":1}',
    );

    expect(search).toHaveBeenCalledWith({
      index: "notes",
      size: 1,
      query: { match: { title: "Release" } },
    });
    expect(result.columns).toEqual(["_id", "tags", "title"]);
    expect(result.rowCount).toBe(1);
    expect(result.rows).toEqual([
      {
        __col_0: "doc-9",
        __col_1: '["product","beta"]',
        __col_2: "Release note",
      },
    ]);
  });

  it("uses _id as request metadata instead of indexing it inside the document body", async () => {
    const { driver, index } = createDriver();

    await expect(
      driver.insertRow({
        database: "default",
        schema: "indices",
        table: "users",
        values: {
          _id: "doc-3",
          email: "charlie@example.com",
          active: true,
        },
      }),
    ).resolves.toEqual({ affectedRows: 1 });

    expect(index).toHaveBeenCalledWith({
      index: "users",
      id: "doc-3",
      document: {
        email: "charlie@example.com",
        active: true,
      },
      op_type: "create",
      refresh: "wait_for",
    });
    expect(
      driver.buildMutationPreviewStatement(
        "insert",
        "default",
        "indices",
        "users",
        {
          values: {
            _id: "doc-3",
            email: "charlie@example.com",
            active: true,
          },
        },
      ),
    ).toBe(
      'index {"index":"users","id":"doc-3","document":{"email":"charlie@example.com","active":true}}',
    );
  });
});
