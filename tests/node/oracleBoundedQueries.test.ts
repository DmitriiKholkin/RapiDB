import oracledb from "oracledb";
import { expect, it, vi } from "vitest";
import { OracleDriver } from "../../src/extension/dbDrivers/oracle";

function cursor(size: number, fail = false, start = 0, columnCount = 2) {
  let fetched = 0;
  return {
    metaData: Array.from(
      { length: columnCount },
      (): { name: string; dbType?: unknown } => ({ name: "same" }),
    ),
    getRows: vi.fn(async (count: number) => {
      if (fail && fetched >= 100) throw new Error("late fetch failure");
      const rows = Array.from(
        { length: Math.min(count, size - fetched) },
        (_, i) =>
          Array.from(
            { length: columnCount },
            (_column, columnIndex) => start + fetched + i + columnIndex,
          ),
      );
      fetched += rows.length;
      return rows;
    }),
    close: vi.fn(async () => {}),
  };
}
function harness(results: object[]) {
  const connection = {
    execute: vi.fn(async (sql: string) =>
      sql.startsWith("ALTER SESSION") ? {} : (results.shift() ?? {}),
    ),
    commit: vi.fn(async () => {}),
    rollback: vi.fn(async () => {}),
    close: vi.fn(async () => {}),
    break: vi.fn(async () => {}),
  };
  const driver = new OracleDriver({
    id: "oracle-cap",
    name: "Oracle",
    type: "oracle",
  });
  (driver as unknown as { pool: unknown }).pool = {
    getConnection: async () => connection,
  };
  return { driver, connection };
}

it("retains every SELECT statement as a separate query result set", async () => {
  const first = cursor(1);
  const second = cursor(1, false, 2);
  const { driver, connection } = harness([
    { resultSet: first },
    { resultSet: second },
  ]);

  const result = await driver.query(
    "SELECT 1 AS first_result FROM DUAL; SELECT 2 AS second_result FROM DUAL",
  );

  expect(result).toMatchObject({
    columns: ["same", "same"],
    rows: [{ __col_0: 0, __col_1: 1 }],
    rowCount: 2,
    resultSets: [
      {
        id: "result-1",
        columns: ["same", "same"],
        rows: [{ __col_0: 0, __col_1: 1 }],
        rowCount: 1,
      },
      {
        id: "result-2",
        columns: ["same", "same"],
        rows: [{ __col_0: 2, __col_1: 3 }],
        rowCount: 1,
      },
    ],
  });
  expect(first.close).toHaveBeenCalledOnce();
  expect(second.close).toHaveBeenCalledOnce();
  expect(connection.commit).toHaveBeenCalledOnce();
});

it("maps Oracle NUMBER metadata on query results", async () => {
  const numeric = cursor(2, false, 2, 1);
  numeric.metaData = [{ name: "amount", dbType: oracledb.DB_TYPE_NUMBER }];
  const { driver } = harness([{ resultSet: numeric }]);

  const result = await driver.query("SELECT amount FROM DUAL");

  expect(result.columnMeta).toEqual([{ category: "decimal" }]);
  expect(result.resultSets?.[0]?.columnMeta).toEqual([{ category: "decimal" }]);
});

it("returns completed SELECT results with a later statement error and recovers", async () => {
  const first = cursor(1);
  const recovery = cursor(1, false, 3);
  const { driver, connection } = harness([]);
  connection.execute.mockImplementation(async (sql: string) => {
    if (sql.startsWith("ALTER SESSION")) return {};
    if (sql.includes("SELECT FROM")) {
      throw new Error("ORA-00936: missing expression");
    }
    return {
      resultSet: /RECOVERY_PROBE/i.test(sql) ? recovery : first,
    };
  });

  const partial = await driver.query(
    "SELECT 1 AS first_result FROM DUAL; SELECT FROM DUAL",
  );

  expect(partial).toMatchObject({
    columns: ["same", "same"],
    rows: [{ __col_0: 0, __col_1: 1 }],
    rowCount: 1,
    error: "ORA-00936: missing expression",
    resultSets: [
      {
        id: "result-1",
        columns: ["same", "same"],
        rows: [{ __col_0: 0, __col_1: 1 }],
        rowCount: 1,
      },
    ],
  });
  expect(connection.rollback).toHaveBeenCalledOnce();
  expect(connection.commit).not.toHaveBeenCalled();

  expect(
    await driver.query("SELECT 3 AS recovery_probe FROM DUAL"),
  ).toMatchObject({
    rows: [{ __col_0: 3, __col_1: 4 }],
    rowCount: 1,
  });
  expect(recovery.close).toHaveBeenCalledOnce();
});

it("surfaces both query and rollback errors when rollback fails", async () => {
  const first = cursor(1);
  const { driver, connection } = harness([]);
  connection.execute.mockImplementation(async (sql: string) => {
    if (sql.startsWith("ALTER SESSION")) return {};
    if (sql.includes("SELECT FROM")) {
      throw new Error("ORA-00936: missing expression");
    }
    return { resultSet: first };
  });
  connection.rollback.mockRejectedValueOnce(
    new Error("ORA-01013: rollback failed"),
  );

  await expect(
    driver.query("SELECT 1 AS first_result FROM DUAL; SELECT FROM DUAL"),
  ).rejects.toThrow(
    "Oracle query failed: ORA-00936: missing expression; rollback failed: ORA-01013: rollback failed",
  );
  expect(connection.rollback).toHaveBeenCalledOnce();
});

it("does not charge omitted wide result sets against the retained row budget", async () => {
  const tooWide = cursor(1, false, 0, 4097);
  const normal = cursor(1, false, 7);
  const { driver } = harness([{ resultSet: tooWide }, { resultSet: normal }]);

  const result = await driver.query(
    "SELECT wide_result FROM DUAL; SELECT 1 AS normal_result FROM DUAL",
    undefined,
    { hardCap: 1 },
  );

  expect(result).toMatchObject({
    columns: ["same", "same"],
    rows: [{ __col_0: 7, __col_1: 8 }],
    rowCount: 1,
    resultSetsTruncated: true,
    resultSets: [
      {
        id: "result-1",
        columns: ["same", "same"],
        rows: [{ __col_0: 7, __col_1: 8 }],
        rowCount: 1,
      },
    ],
  });
  expect(tooWide.close).toHaveBeenCalledOnce();
  expect(normal.close).toHaveBeenCalledOnce();
});

it.each([
  20, 30,
])("drains %i implicit cursors, preserves duplicate/empty columns and executes trailing mutations", async (cursorCount) => {
  const sets = Array.from({ length: cursorCount }, () => cursor(12050));
  const { driver, connection } = harness([
    { implicitResults: sets },
    { rowsAffected: 50000 },
  ]);
  const result = await driver.query(
    "BEGIN NULL; END;\n/\nUPDATE t SET n=1",
    undefined,
    { hardCap: 3 },
  );
  expect(result).toMatchObject({
    columns: ["same", "same"],
    rows: [
      { __col_0: 0, __col_1: 1 },
      { __col_0: 1, __col_1: 2 },
      { __col_0: 2, __col_1: 3 },
    ],
    rowCount: 12050 * cursorCount,
    truncated: true,
  });
  expect(result.resultSets).toHaveLength(cursorCount);
  expect(
    result.resultSets?.reduce((sum, set) => sum + set.rows.length, 0),
  ).toBe(3);
  for (const set of sets) {
    expect(set.getRows).toHaveBeenLastCalledWith(100);
    expect(set.close).toHaveBeenCalledOnce();
  }
  expect(connection.commit).toHaveBeenCalledOnce();
  expect(connection.execute).toHaveBeenLastCalledWith(
    "UPDATE t SET n=1",
    [],
    expect.objectContaining({
      resultSet: true,
      outFormat: oracledb.OUT_FORMAT_ARRAY,
      prefetchRows: 0,
    }),
  );
  const empty = cursor(0);
  const other = harness([{ outBinds: [empty] }]);
  expect(
    await other.driver.query(
      "BEGIN OPEN :1 FOR SELECT 1 FROM dual; END;",
      [{ dir: oracledb.BIND_OUT, type: oracledb.CURSOR }],
      { hardCap: 3 },
    ),
  ).toMatchObject({ columns: ["same", "same"], rowCount: 0, rows: [] });
  expect(empty.close).toHaveBeenCalledOnce();
});

it("retains implicit and OUT cursors in order with shared budgets across statements", async () => {
  const first = cursor(2, false, 1);
  const wide = cursor(1, false, 99, 4097);
  const empty = cursor(0);
  const out = cursor(2, false, 10);
  const later = cursor(1, false, 20);
  const { driver, connection } = harness([
    { implicitResults: [first, wide, empty], outBinds: { output: out } },
    { resultSet: later },
  ]);
  const result = await driver.query(
    "BEGIN NULL; END;\n/\nSELECT 20 FROM DUAL",
    undefined,
    { hardCap: 3 },
  );
  expect(result.resultSets?.map((set) => set.rows)).toEqual([
    [
      { __col_0: 1, __col_1: 2 },
      { __col_0: 2, __col_1: 3 },
    ],
    [],
    [{ __col_0: 10, __col_1: 11 }],
    [],
  ]);
  expect(result.rowCount).toBe(5);
  expect(result.resultSetsTruncated).toBe(true);
  expect(result.resultSets?.map((set) => set.truncated)).toEqual([
    false,
    false,
    true,
    true,
  ]);
  for (const set of [first, wide, empty, out, later])
    expect(set.close).toHaveBeenCalledOnce();
  expect(connection.commit).toHaveBeenCalledOnce();
});

it("bounds metadata across implicit cursors and later statements", async () => {
  const sets = Array.from({ length: 101 }, () => cursor(0));
  const { driver } = harness([
    { implicitResults: sets },
    { resultSet: cursor(1) },
  ]);
  const result = await driver.query(
    "BEGIN NULL; END;\n/\nSELECT 1 FROM DUAL",
    undefined,
    { hardCap: 2 },
  );
  expect(result.resultSets).toHaveLength(100);
  expect(result.resultSetsTruncated).toBe(true);
  for (const set of sets) expect(set.close).toHaveBeenCalledOnce();
});

it("closes unvisited cursors on late failure, rolls back and recovers", async () => {
  const sets = [cursor(1000), cursor(1000, true), cursor(1000)];
  const { driver, connection } = harness([
    { implicitResults: sets },
    { resultSet: cursor(1) },
  ]);
  await expect(
    driver.query("BEGIN NULL; END;", undefined, { hardCap: 2 }),
  ).rejects.toThrow("late fetch failure");
  for (const set of sets) expect(set.close).toHaveBeenCalledOnce();
  expect(connection.rollback).toHaveBeenCalledOnce();
  expect(connection.close).toHaveBeenCalledOnce();
  expect(connection.commit).not.toHaveBeenCalled();
  expect((await driver.query("SELECT 1 FROM dual")).rowCount).toBe(1);
});

it("cancels during fetch, closes all outputs and recovers", async () => {
  const set = cursor(1000);
  const unused = cursor(10);
  const { driver, connection } = harness([
    { resultSet: set, outBinds: [unused] },
    { rowsAffected: 90000 },
  ]);
  set.getRows.mockImplementationOnce(async () => {
    await driver.cancelCurrentOperation({
      operationName: "query",
      requestToken: 7,
      reason: "timeout",
    });
    return [[1, 2]];
  });
  await expect(
    driver.query("SELECT 1 FROM dual", undefined, {
      requestToken: 7,
      hardCap: 1,
    }),
  ).rejects.toThrow("cancelled");
  expect(connection.break).toHaveBeenCalledOnce();
  expect(set.close).toHaveBeenCalledOnce();
  expect(unused.close).toHaveBeenCalledOnce();
  expect(
    await driver.query("UPDATE t SET n=1", undefined, { hardCap: 1 }),
  ).toMatchObject({ rowCount: 90000, affectedRows: 90000 });
});

it("preserves a fetch timeout while attempting every close and releases the connection", async () => {
  const first = cursor(100);
  const next = cursor(100);
  first.getRows.mockRejectedValueOnce(new Error("DPI-1067: call timeout"));
  first.close.mockRejectedValueOnce(new Error("close failed"));
  const { driver, connection } = harness([{ implicitResults: [first, next] }]);
  connection.close.mockRejectedValueOnce(
    new Error("connection release failed"),
  );
  await expect(
    driver.query("BEGIN NULL; END;", undefined, { hardCap: 1 }),
  ).rejects.toThrow("call timeout");
  expect(next.close).toHaveBeenCalledOnce();
  expect(connection.close).toHaveBeenCalledOnce();
  expect(connection.rollback).toHaveBeenCalledOnce();
});

it("surfaces connection release errors after otherwise successful execution", async () => {
  const { driver, connection } = harness([{ resultSet: cursor(1) }]);
  connection.close.mockRejectedValueOnce(
    new Error("connection release failed"),
  );
  await expect(
    driver.query("SELECT 1 FROM dual", undefined, { hardCap: 1 }),
  ).rejects.toThrow("connection release failed");
  expect(connection.commit).toHaveBeenCalledOnce();
});

it("keeps native bind descriptors and ignores question marks inside Oracle literals and identifiers", async () => {
  const { driver, connection } = harness([{ rowsAffected: 12050 }]);
  const bind = { dir: oracledb.BIND_OUT, type: oracledb.NUMBER };
  await driver.query(
    `UPDATE t SET "?"=q'[it's ?;]', n=nq'{it's ?;}' RETURNING id INTO ?`,
    [bind],
    { hardCap: 1 },
  );
  expect(connection.execute).toHaveBeenLastCalledWith(
    `UPDATE t SET "?"=q'[it's ?;]', n=nq'{it's ?;}' RETURNING id INTO :1`,
    [bind],
    expect.objectContaining({ resultSet: true }),
  );
});
