import { EventEmitter } from "node:events";
import mssql from "mssql";
import { describe, expect, it, vi } from "vitest";
import { MSSQLDriver } from "../../src/extension/dbDrivers/mssql";
import { MssqlQuerySession } from "../../src/extension/dbDrivers/mssqlQuerySession";
import {
  MockMssqlRequest,
  mockMssqlSessionPool,
} from "../helpers/mssqlQuerySessionHarness";

function harness() {
  const request = new MockMssqlRequest();
  const originalSet = vi.fn((_native: EventEmitter) => request);
  Object.assign(request, { _setCurrentRequest: originalSet });
  const pool = mockMssqlSessionPool([request]);
  const driver = new MSSQLDriver({
    id: "nocount-qa",
    name: "NOCOUNT QA",
    type: "mssql",
  });
  Object.assign(driver, { pool });
  return { request, originalSet, pool, driver };
}

describe("MSSQL statement completion without DONE_COUNT", () => {
  it.each([
    "done",
    "doneInProc",
    "doneProc",
  ])("keeps completed SELECT complete after %s without rowsaffected and a later error", async (event) => {
    const { request, originalSet, driver } = harness();
    const native = new EventEmitter();
    const originalEmit = native.emit;
    const later = new Error("later statement error");
    request.execute.mockImplementation(async () => {
      Reflect.get(request, "_setCurrentRequest").call(request, native);
      request.emit("recordset", [{ name: "v", type: mssql.Int }]);
      request.emit("row", [1]);
      native.emit(event, undefined, true);
      request.emit("error", later);
      throw later;
    });
    const result = await driver.query("SET NOCOUNT ON; SELECT 1; THROW");
    expect(result.error).toBe("later statement error");
    expect(result.resultSets?.[0]).toMatchObject({
      rows: [{ __col_0: 1 }],
      rowCount: 1,
    });
    expect(result.resultSets?.[0].partial).not.toBe(true);
    expect(Reflect.get(request, "_setCurrentRequest")).toBe(originalSet);
    expect(native.emit).toBe(originalEmit);
    expect(Object.hasOwn(native, "emit")).toBe(false);
  });

  it("keeps a genuine partial set partial when its completion arrives after the error", async () => {
    const { request, driver } = harness();
    const native = new EventEmitter();
    const error = new Error("row execution failure");
    request.execute.mockImplementation(async () => {
      Reflect.get(request, "_setCurrentRequest").call(request, native);
      request.emit("recordset", [{ name: "v", type: mssql.Int }]);
      request.emit("row", [1]);
      request.emit("error", error);
      native.emit("done", undefined, false);
      throw error;
    });
    const result = await driver.query("SET NOCOUNT ON; SELECT failing");
    expect(result.resultSets?.[0]).toMatchObject({
      rows: [{ __col_0: 1 }],
      partial: true,
    });
  });

  it("observes completion only after SDK done handlers deliver chunked rows and decoding errors", async () => {
    const { request, driver } = harness();
    const native = new EventEmitter();
    const error = new Error("chunk decoder failure");
    request.execute.mockImplementation(async () => {
      Reflect.get(request, "_setCurrentRequest").call(request, native);
      native.on("done", () => {
        request.emit("row", [1]);
        request.emit("error", error);
      });
      request.emit("recordset", [{ name: "v", type: mssql.Int }]);
      native.emit("done", undefined, false);
      throw error;
    });
    const result = await driver.query("SET NOCOUNT ON; SELECT chunked");
    expect(result.resultSets?.[0]).toMatchObject({
      rows: [{ __col_0: 1 }],
      partial: true,
    });
  });

  it("restores only per-request hooks, preserves SDK order, and tolerates repeated native registration", () => {
    const { request, originalSet, pool } = harness();
    const native = new EventEmitter();
    const order: string[] = [];
    const session = new MssqlQuerySession(pool as never);
    const dispose = session.trackStatementCompletions(request as never, () =>
      order.push("complete"),
    );
    const set = Reflect.get(request, "_setCurrentRequest");
    set.call(request, native);
    set.call(request, native);
    native.on("done", () => order.push("sdk"));
    native.emit("done");
    expect(order).toEqual(["sdk", "complete"]);
    dispose();
    dispose();
    native.emit("done");
    expect(order).toEqual(["sdk", "complete", "sdk"]);
    expect(Reflect.get(request, "_setCurrentRequest")).toBe(originalSet);
    expect(Object.hasOwn(native, "emit")).toBe(false);
  });
});
