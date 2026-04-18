import { describe, expect, it, vi } from "vitest";
import type { IDBDriver } from "../../src/extension/dbDrivers/types";
import { buildWhere } from "../../src/extension/table/filterSql";
import { col } from "./helpers";

function makeDriver(): Pick<IDBDriver, "buildFilterCondition" | "normalizeFilterValue"> {
  return {
    normalizeFilterValue: vi.fn((_column, _operator, value) => value),
    buildFilterCondition: vi.fn((column, operator, value) => ({
      sql: `${column.name}:${operator}`,
      params: value === undefined ? [] : Array.isArray(value) ? value : [value],
    })),
  };
}

describe("buildWhere", () => {
  it("delegates value normalization to the driver hook before building SQL", () => {
    const driver = makeDriver();
    vi.mocked(driver.normalizeFilterValue).mockReturnValue("true");

    const result = buildWhere(
      driver as unknown as IDBDriver,
      [{ column: "active", operator: "eq", value: "1" }],
      [
        col({
          name: "active",
          type: "boolean",
          category: "boolean",
          filterOperators: ["eq"],
          isBoolean: true,
        }),
      ],
    );

    expect(driver.normalizeFilterValue).toHaveBeenCalledWith(
      expect.objectContaining({ name: "active" }),
      "eq",
      "1",
    );
    expect(driver.buildFilterCondition).toHaveBeenCalledWith(
      expect.objectContaining({ name: "active" }),
      "eq",
      "true",
      1,
    );
    expect(result).toEqual({ clause: "WHERE active:eq", params: ["true"] });
  });

  it("propagates normalization errors from the driver hook", () => {
    const driver = makeDriver();
    vi.mocked(driver.normalizeFilterValue).mockImplementation(() => {
      throw new Error("[RapiDB Filter] Column age expects a number.");
    });

    expect(() =>
      buildWhere(
        driver as unknown as IDBDriver,
        [{ column: "age", operator: "gt", value: "abc" }],
        [
          col({
            name: "age",
            type: "integer",
            category: "integer",
            filterOperators: ["gt"],
          }),
        ],
      ),
    ).toThrow("[RapiDB Filter] Column age expects a number.");

    expect(driver.normalizeFilterValue).toHaveBeenCalledWith(
      expect.objectContaining({ name: "age" }),
      "gt",
      "abc",
    );
    expect(driver.buildFilterCondition).not.toHaveBeenCalled();
  });

  it("rejects unsupported operators before invoking the driver hook", () => {
    const driver = makeDriver();

    expect(() =>
      buildWhere(
        driver as unknown as IDBDriver,
        [{ column: "name", operator: "eq", value: "alice" }],
        [
          col({
            name: "name",
            type: "text",
            category: "text",
            filterOperators: ["like"],
          }),
        ],
      ),
    ).toThrow("[RapiDB Filter] Column name does not support eq filters.");

    expect(driver.normalizeFilterValue).not.toHaveBeenCalled();
    expect(driver.buildFilterCondition).not.toHaveBeenCalled();
  });
});