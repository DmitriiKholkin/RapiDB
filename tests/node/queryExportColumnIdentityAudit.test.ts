import { describe, expect, it } from "vitest";
import {
  buildQueryExportColumns,
  serializeJsonExportRecord,
} from "../../src/extension/utils/exportValueFormat";

describe("query export column identity", () => {
  it("retains every value when duplicate names collide with literal suffixes", () => {
    const columns = buildQueryExportColumns(["id", "id", "id_2"]);
    const serialized = serializeJsonExportRecord(
      columns.map((column, index) => ({ ...column, value: index + 1 })),
    );

    expect(serialized).toBe('{"id":1,"id_3":2,"id_2":3}');
    expect(JSON.parse(serialized)).toEqual({ id: 1, id_3: 2, id_2: 3 });
  });

  it.each([
    { names: [], keys: [] },
    { names: ["id", "id", "id"], keys: ["id", "id_2", "id_3"] },
    { names: ["id_2", "id", "id"], keys: ["id_2", "id", "id_3"] },
    {
      names: ["id", "id", "id_2", "id_2", "id_2_2", "id_3"],
      keys: ["id", "id_4", "id_2", "id_2_3", "id_2_2", "id_3"],
    },
    { names: ["", "", "_2", "_3", ""], keys: ["", "_4", "_2", "_3", "_5"] },
    { names: [" ", " ", " _2"], keys: [" ", " _3", " _2"] },
    {
      names: ["ID", "id", "ID", "id"],
      keys: ["ID", "id", "ID_2", "id_2"],
    },
    {
      names: ["__proto__", "__proto__", "constructor", "constructor"],
      keys: ["__proto__", "__proto___2", "constructor", "constructor_2"],
    },
  ])("assigns deterministic unique labels to $names", ({ names, keys }) => {
    const columns = buildQueryExportColumns(names);
    expect(columns.map((column) => column.key)).toEqual(keys);
    expect(buildQueryExportColumns(names)).toEqual(columns);
    expect(new Set(keys).size).toBe(names.length);
  });

  it("keeps positional source keys and metadata attached to renamed columns", () => {
    expect(
      buildQueryExportColumns(
        ["id", "id", "id_2", "id"],
        [
          { category: "integer" },
          { category: "json" },
          { category: "boolean" },
        ],
      ),
    ).toEqual([
      { key: "id", sourceKey: "__col_0", category: "integer" },
      { key: "id_3", sourceKey: "__col_1", category: "json" },
      { key: "id_2", sourceKey: "__col_2", category: "boolean" },
      { key: "id_4", sourceKey: "__col_3", category: null },
    ]);
  });

  it("round-trips each positional value across combinations of colliding names", () => {
    const alphabet = ["", "_2", "id", "id_2", "id_2_2", "__proto__"];
    for (
      let combination = 0;
      combination < alphabet.length ** 4;
      combination++
    ) {
      let remaining = combination;
      const names = Array.from({ length: 4 }, () => {
        const name = alphabet[remaining % alphabet.length];
        remaining = Math.floor(remaining / alphabet.length);
        return name;
      });
      const columns = buildQueryExportColumns(names);
      const parsed = JSON.parse(
        serializeJsonExportRecord(
          columns.map((column, index) => ({ ...column, value: index + 1 })),
        ),
      );

      expect(Object.keys(parsed)).toHaveLength(names.length);
      for (const [index, column] of columns.entries()) {
        expect(parsed[column.key]).toBe(index + 1);
        // Generated labels must not steal the first occurrence of a literal name.
        if (names.indexOf(names[index]) === index) {
          expect(column.key).toBe(names[index]);
        }
      }
    }
  });
});
