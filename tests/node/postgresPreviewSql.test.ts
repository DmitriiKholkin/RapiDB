import { describe, expect, it } from "vitest";
import { PostgresDriver } from "../../src/extension/dbDrivers/postgres";

const driver = new PostgresDriver({
  id: "preview-test",
  name: "preview-test",
  type: "pg",
  host: "localhost",
  port: 5432,
  database: "postgres",
  username: "postgres",
  password: "postgres",
});

describe("postgres preview SQL materialization", () => {
  it("renders executable PostgreSQL array and bytea literals", () => {
    const sql =
      'UPDATE "public"."probe" SET "tags" = $1, "payload" = $2 WHERE "id" = $3';
    const preview = driver.materializePreviewSql(sql, [
      ["alpha", "b'et'a", null],
      Buffer.from([0x00, 0xff, 0x10]),
      7,
    ]);

    expect(preview).toContain("\"tags\" = ARRAY['alpha', 'b''et''a', NULL]");
    expect(preview).toContain("\"payload\" = '\\x00ff10'::bytea");
    expect(preview).toContain('WHERE "id" = 7');
  });

  it("renders empty arrays as PostgreSQL empty array literals", () => {
    const sql = "INSERT INTO probe(payload, note) VALUES (?, ?)";
    const preview = driver.materializePreviewSql(sql, [[], "ok"]);

    expect(preview).toBe(
      "INSERT INTO probe(payload, note) VALUES ('{}', 'ok')",
    );
  });
});
