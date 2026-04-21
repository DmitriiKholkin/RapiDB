import { describe, expect, it } from "vitest";

import { formatMutationPreviewSql } from "../../src/extension/utils/mutationPreview";

describe("formatMutationPreviewSql", () => {
  it("formats statements using the connection-specific dialect and skips empties", () => {
    const sql = formatMutationPreviewSql(
      [
        'update "users" set "name" = \'Alice\' where "id" = 1',
        "   ",
        'insert into "users" ("id", "name") values (2, \'Bob\')',
      ],
      "pg",
    );
    const normalizedSql = sql.replace(/\s+/g, " ").trim();

    expect(normalizedSql).toContain('UPDATE "users"');
    expect(normalizedSql).toContain("\"name\" = 'Alice'");
    expect(normalizedSql).toContain('WHERE "id" = 1;');
    expect(normalizedSql).toContain('INSERT INTO "users"');
    expect(normalizedSql).toContain("VALUES (2, 'Bob');");
    expect(sql).toContain("\n\nINSERT INTO");
  });

  it("adds a statement terminator when formatting generic SQL without a known connection type", () => {
    const sql = formatMutationPreviewSql(
      ["delete from users where id = 7"],
      undefined,
    );

    expect(sql).toContain("DELETE");
    expect(sql.trim().endsWith(";")).toBe(true);
  });
});
