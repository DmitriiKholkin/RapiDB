import { EventEmitter } from "node:events";
import mssql from "mssql";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MSSQLDriver } from "../../src/extension/dbDrivers/mssql";

function harness() {
  const driver = new MSSQLDriver({
    id: "mssql-placeholders",
    name: "MSSQL placeholders",
    type: "mssql",
    database: "db",
  });
  const request = Object.assign(new EventEmitter(), {
    input: vi.fn().mockReturnThis(),
    query: vi.fn().mockResolvedValue({ rowsAffected: [0], columns: [[]] }),
  });
  (driver as unknown as { pool: unknown }).pool = {
    request: () => request,
  };
  return { driver, request };
}

// T-SQL escapes quotes by doubling them; a backslash is an ordinary character
// in both '...' / N'...' strings and double-quoted identifiers.
const cases = [
  { sql: "SELECT 'C:\\', ?", expected: "SELECT 'C:\\', VALUE" },
  // Before the fix this silently binds the '?' literal instead of the value.
  {
    sql: "SELECT 'C:\\', ? AS [value], '?' AS [literal]",
    expected: "SELECT 'C:\\', VALUE AS [value], '?' AS [literal]",
  },
  {
    sql: "SELECT N'C:\\', '$1 :1 ?', ?",
    expected: "SELECT N'C:\\', '$1 :1 ?', VALUE",
  },
  {
    sql: "SELECT 'C:\\', ':1 ?', ?",
    expected: "SELECT 'C:\\', ':1 ?', VALUE",
  },
  {
    sql: "SELECT N'it\\''s ? $1 :1', ?",
    expected: "SELECT N'it\\''s ? $1 :1', VALUE",
  },
  {
    sql: 'SELECT "C:\\", "x""?$1:1", ?',
    expected: 'SELECT "C:\\", "x""?$1:1", VALUE',
  },
  {
    sql: "SELECT 'C:\\', [x]]?$1:1], ? -- ? $1 :1\n/* ? $1 :1 */",
    expected: "SELECT 'C:\\', [x]]?$1:1], VALUE -- ? $1 :1\n/* ? $1 :1 */",
  },
];

afterEach(() => vi.restoreAllMocks());

describe("MSSQL T-SQL placeholder boundaries", () => {
  describe.each([undefined, 10])("query hardCap=%s", (hardCap) => {
    it.each(cases)("binds only the active parameter in $sql", async ({
      sql,
      expected,
    }) => {
      const { driver, request } = harness();
      await driver.query(sql, [7], { hardCap });
      expect(request.query).toHaveBeenCalledWith(
        expected.replace("VALUE", "@p1"),
      );
      expect(request.input).toHaveBeenCalledExactlyOnceWith(
        "p1",
        mssql.TinyInt,
        7,
      );
    });
  });

  it.each(cases)("keeps generic and column-aware previews aligned for $sql", ({
    sql,
    expected,
  }) => {
    const { driver } = harness();
    const preview = expected.replace("VALUE", "7");
    expect(driver.materializePreviewSql(sql, [7])).toBe(preview);
    expect(driver.materializePreviewColumnSql(sql, [7], [undefined])).toBe(
      preview,
    );
    expect(driver.materializePreviewInsertSql(sql, [7], [])).toBe(preview);
  });

  it("binds multiple parameters in order, retaining actual mismatch checks", async () => {
    const { driver, request } = harness();
    const sql = "SELECT N'C:\\', ?, '?' AS [literal], ?";
    await driver.query(sql, [7, 8]);
    expect(request.query).toHaveBeenCalledWith(
      "SELECT N'C:\\', @p1, '?' AS [literal], @p2",
    );
    expect(
      request.input.mock.calls.map(([name, , value]) => [name, value]),
    ).toEqual([
      ["p1", 7],
      ["p2", 8],
    ]);
    expect(driver.materializePreviewSql(sql, [7, 8])).toBe(
      "SELECT N'C:\\', 7, '?' AS [literal], 8",
    );
    request.input.mockClear();
    request.query.mockClear();
    await expect(driver.query(sql, [7])).rejects.toThrow(
      "MSSQL parameter mismatch",
    );
    expect(() => driver.materializePreviewSql(sql, [7])).toThrow(
      "Preview parameter mismatch",
    );
    expect(request.input).not.toHaveBeenCalled();
    expect(request.query).not.toHaveBeenCalled();
  });

  it.each([
    { params: undefined },
    { params: [] },
  ])("leaves raw parameterless SQL unchanged ($params)", async ({ params }) => {
    const { driver, request } = harness();
    const sql = "SELECT N'C:\\', '?' AS [literal], '$1 :1' AS [markers]";
    await driver.query(sql, params);
    expect(request.query).toHaveBeenCalledWith(sql);
    expect(request.input).not.toHaveBeenCalled();
    expect(driver.materializePreviewSql(sql, params)).toBe(sql);
    expect(driver.materializePreviewColumnSql(sql, params, [])).toBe(sql);
  });

  it("uses the same boundaries for active transaction operations", async () => {
    const { driver, request } = harness();
    vi.spyOn(mssql.Transaction.prototype, "begin").mockResolvedValue(
      undefined as never,
    );
    const commit = vi
      .spyOn(mssql.Transaction.prototype, "commit")
      .mockResolvedValue(undefined as never);
    vi.spyOn(mssql.Transaction.prototype, "rollback").mockResolvedValue(
      undefined as never,
    );
    vi.spyOn(mssql.Transaction.prototype, "request").mockReturnValue(
      request as unknown as mssql.Request,
    );
    await driver.runTransaction([
      { sql: "UPDATE [t] SET [path] = N'C:\\', [value] = ?", params: [7] },
    ]);
    expect(request.query).toHaveBeenCalledWith(
      "UPDATE [t] SET [path] = N'C:\\', [value] = @p1",
    );
    expect(request.input).toHaveBeenCalledExactlyOnceWith(
      "p1",
      mssql.TinyInt,
      7,
    );
    expect(commit).toHaveBeenCalledOnce();
  });
});
