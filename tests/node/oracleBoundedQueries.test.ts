import oracledb from "oracledb";
import { expect, it, vi } from "vitest";
import { OracleDriver } from "../../src/extension/dbDrivers/oracle";

function cursor(size: number, fail = false) {
  let fetched = 0;
  return {
    metaData: [{ name: "same" }, { name: "same" }],
    getRows: vi.fn(async (count: number) => {
      if (fail && fetched >= 100) throw new Error("late fetch failure");
      const rows = Array.from(
        { length: Math.min(count, size - fetched) },
        (_, i) => [fetched + i, fetched + i + 1],
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

it("drains large implicit and OUT cursors, preserves duplicate/empty columns and executes trailing mutations", async () => {
  const sets = Array.from({ length: 30 }, () => cursor(12050));
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
    rowCount: 12050,
    truncated: true,
  });
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
