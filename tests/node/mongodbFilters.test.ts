import { describe, expect, it, vi } from "vitest";
import { MongoDBDriver } from "../../src/extension/dbDrivers/mongodb";
import type { FilterExpression } from "../../src/shared/tableTypes";

type Row = Record<string, unknown> & { _id: string };

type FilterSpec = {
  value?: string | [string, string];
  expectedIds: string[];
};

const rows: Row[] = [
  {
    _id: "507f1f77bcf86cd799439011",
    txt: "alpha",
    intNum: 2,
    floatNum: 2.5,
    boolVal: true,
    dateVal: "2026-01-02",
    timeVal: "10:20:30",
    dateTimeVal: "2026-01-02T10:20:30Z",
    uuidVal: "123e4567-e89b-12d3-a456-426614174000",
    jsonVal: '{"k":"one"}',
    arrayVal: "[1,2,3]",
  },
  {
    _id: "507f1f77bcf86cd799439012",
    txt: "beta",
    intNum: 10,
    floatNum: 10.75,
    boolVal: false,
    dateVal: "2026-02-03",
    timeVal: "16:45:00",
    dateTimeVal: "2026-02-03T16:45:00Z",
    uuidVal: "123e4567-e89b-12d3-a456-426614174001",
    jsonVal: '{"k":"two"}',
    arrayVal: "[4,5,6]",
  },
  {
    _id: "507f1f77bcf86cd799439013",
    txt: null,
    intNum: null,
    floatNum: null,
    boolVal: null,
    dateVal: null,
    timeVal: null,
    dateTimeVal: null,
    uuidVal: null,
    jsonVal: null,
    arrayVal: null,
  },
];

function createDriverWithRows(sampleRows: Row[]): MongoDBDriver {
  const driver = new MongoDBDriver({
    type: "mongodb",
    name: "mongo-filter-test",
    host: "localhost",
    port: 27017,
    database: "test",
  });

  const readRowsMock = vi.fn().mockResolvedValue(sampleRows);
  (driver as unknown as { readRows: typeof readRowsMock }).readRows =
    readRowsMock;
  return driver;
}

async function applyFilter(
  driver: MongoDBDriver,
  filter: FilterExpression,
): Promise<string[]> {
  const page = await driver.readTablePage({
    database: "test",
    schema: "test",
    table: "users",
    page: 1,
    pageSize: 100,
    filters: [filter],
    sort: null,
    skipCount: false,
  });
  return page.rows.map((row) => String(row._id));
}

describe("MongoDBDriver readTablePage filter coverage", () => {
  it("supports every advertised operator for each inferred MongoDB column type", async () => {
    const driver = createDriverWithRows(rows);
    const firstPage = await driver.readTablePage({
      database: "test",
      schema: "test",
      table: "users",
      page: 1,
      pageSize: 100,
      filters: [],
      sort: null,
      skipCount: false,
    });

    const columnsByName = new Map(
      firstPage.columns.map((column) => [column.name, column]),
    );

    const specsByColumn: Record<string, Record<string, FilterSpec>> = {
      txt: {
        like: { value: "alp", expectedIds: [rows[0]._id] },
        is_null: { expectedIds: [rows[2]._id] },
        is_not_null: { expectedIds: [rows[0]._id, rows[1]._id] },
      },
      intNum: {
        eq: { value: "10", expectedIds: [rows[1]._id] },
        neq: { value: "10", expectedIds: [rows[0]._id] },
        gt: { value: "2", expectedIds: [rows[1]._id] },
        gte: { value: "10", expectedIds: [rows[1]._id] },
        lt: { value: "10", expectedIds: [rows[0]._id] },
        lte: { value: "2", expectedIds: [rows[0]._id] },
        between: {
          value: ["2", "10"],
          expectedIds: [rows[0]._id, rows[1]._id],
        },
        in: { value: "2,10", expectedIds: [rows[0]._id, rows[1]._id] },
        is_null: { expectedIds: [rows[2]._id] },
        is_not_null: { expectedIds: [rows[0]._id, rows[1]._id] },
      },
      floatNum: {
        eq: { value: "10.75", expectedIds: [rows[1]._id] },
        neq: { value: "10.75", expectedIds: [rows[0]._id] },
        gt: { value: "2.5", expectedIds: [rows[1]._id] },
        gte: { value: "10.75", expectedIds: [rows[1]._id] },
        lt: { value: "10.75", expectedIds: [rows[0]._id] },
        lte: { value: "2.5", expectedIds: [rows[0]._id] },
        between: {
          value: ["2.5", "10.75"],
          expectedIds: [rows[0]._id, rows[1]._id],
        },
        in: { value: "2.5,10.75", expectedIds: [rows[0]._id, rows[1]._id] },
        is_null: { expectedIds: [rows[2]._id] },
        is_not_null: { expectedIds: [rows[0]._id, rows[1]._id] },
      },
      boolVal: {
        eq: { value: "true", expectedIds: [rows[0]._id] },
        neq: { value: "true", expectedIds: [rows[1]._id] },
        is_null: { expectedIds: [rows[2]._id] },
        is_not_null: { expectedIds: [rows[0]._id, rows[1]._id] },
      },
      dateVal: {
        eq: { value: "2026-01-02", expectedIds: [rows[0]._id] },
        neq: { value: "2026-01-02", expectedIds: [rows[1]._id] },
        gt: { value: "2026-01-15", expectedIds: [rows[1]._id] },
        gte: { value: "2026-02-03", expectedIds: [rows[1]._id] },
        lt: { value: "2026-02-03", expectedIds: [rows[0]._id] },
        lte: { value: "2026-01-02", expectedIds: [rows[0]._id] },
        between: {
          value: ["2026-01-01", "2026-01-31"],
          expectedIds: [rows[0]._id],
        },
        in: {
          value: "2026-01-02,2026-02-03",
          expectedIds: [rows[0]._id, rows[1]._id],
        },
        is_null: { expectedIds: [rows[2]._id] },
        is_not_null: { expectedIds: [rows[0]._id, rows[1]._id] },
      },
      timeVal: {
        eq: { value: "10:20:30", expectedIds: [rows[0]._id] },
        neq: { value: "10:20:30", expectedIds: [rows[1]._id] },
        gt: { value: "12:00:00", expectedIds: [rows[1]._id] },
        gte: { value: "16:45:00", expectedIds: [rows[1]._id] },
        lt: { value: "16:45:00", expectedIds: [rows[0]._id] },
        lte: { value: "10:20:30", expectedIds: [rows[0]._id] },
        between: {
          value: ["10:00:00", "12:00:00"],
          expectedIds: [rows[0]._id],
        },
        in: {
          value: "10:20:30,16:45:00",
          expectedIds: [rows[0]._id, rows[1]._id],
        },
        is_null: { expectedIds: [rows[2]._id] },
        is_not_null: { expectedIds: [rows[0]._id, rows[1]._id] },
      },
      dateTimeVal: {
        eq: { value: "2026-01-02T10:20:30Z", expectedIds: [rows[0]._id] },
        neq: { value: "2026-01-02T10:20:30Z", expectedIds: [rows[1]._id] },
        gt: { value: "2026-01-15T00:00:00Z", expectedIds: [rows[1]._id] },
        gte: { value: "2026-02-03T16:45:00Z", expectedIds: [rows[1]._id] },
        lt: { value: "2026-02-03T16:45:00Z", expectedIds: [rows[0]._id] },
        lte: { value: "2026-01-02T10:20:30Z", expectedIds: [rows[0]._id] },
        between: {
          value: ["2026-01-02T00:00:00Z", "2026-01-02T23:59:59Z"],
          expectedIds: [rows[0]._id],
        },
        in: {
          value: "2026-01-02T10:20:30Z,2026-02-03T16:45:00Z",
          expectedIds: [rows[0]._id, rows[1]._id],
        },
        is_null: { expectedIds: [rows[2]._id] },
        is_not_null: { expectedIds: [rows[0]._id, rows[1]._id] },
      },
      uuidVal: {
        like: { value: "4000", expectedIds: [rows[0]._id] },
        in: {
          value:
            "123e4567-e89b-12d3-a456-426614174000,123e4567-e89b-12d3-a456-426614174001",
          expectedIds: [rows[0]._id, rows[1]._id],
        },
        is_null: { expectedIds: [rows[2]._id] },
        is_not_null: { expectedIds: [rows[0]._id, rows[1]._id] },
      },
      jsonVal: {
        like: { value: '"two"', expectedIds: [rows[1]._id] },
        is_null: { expectedIds: [rows[2]._id] },
        is_not_null: { expectedIds: [rows[0]._id, rows[1]._id] },
      },
      arrayVal: {
        like: { value: "[4", expectedIds: [rows[1]._id] },
        is_null: { expectedIds: [rows[2]._id] },
        is_not_null: { expectedIds: [rows[0]._id, rows[1]._id] },
      },
    };

    for (const [columnName, specs] of Object.entries(specsByColumn)) {
      const column = columnsByName.get(columnName);
      expect(column, `column ${columnName} should exist`).toBeDefined();

      const operators = column?.filterOperators ?? [];
      for (const operator of operators) {
        const spec = specs[operator];
        expect(
          spec,
          `missing test spec for ${columnName}.${operator}`,
        ).toBeDefined();

        let filter: FilterExpression;
        if (operator === "between") {
          filter = {
            column: columnName,
            operator,
            value: spec?.value as [string, string],
          };
        } else if (operator === "is_null" || operator === "is_not_null") {
          filter = { column: columnName, operator };
        } else {
          filter = {
            column: columnName,
            operator,
            value: String(spec?.value ?? ""),
          };
        }

        const actualIds = await applyFilter(driver, filter);
        expect(actualIds, `${columnName}.${operator}`).toEqual(
          spec?.expectedIds ?? [],
        );
      }
    }
  });
});
