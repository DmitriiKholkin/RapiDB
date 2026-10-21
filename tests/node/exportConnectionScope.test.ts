import { describe, expect, it, vi } from "vitest";
import type { IDBDriver } from "../../src/extension/dbDrivers/types";
import { TableReadService } from "../../src/extension/table/tableReadService";

describe("table export connection scope", () => {
  it.each([
    "invalidated",
    "complete",
    "consumer-return",
    "read-error",
  ])("disposes the session listener after %s", async (ending) => {
    let invalidate!: (id: string) => void;
    const dispose = vi.fn();
    const driver = {
      async *exportTableChunks() {
        if (ending === "read-error") throw new Error("read failed");
        yield { columns: [], rows: [] };
      },
    } as unknown as IDBDriver;
    const service = new TableReadService({
      getConnection: () => ({ id: "c1" }),
      getDriver: () => driver,
      onDidInvalidateConnectionMetadata: (listener: (id: string) => void) => {
        invalidate = listener;
        return { dispose };
      },
    } as never);
    const iterator = service.exportAll("c1", "db", "public", "records");
    if (ending === "read-error") {
      await expect(iterator.next()).rejects.toThrow("read failed");
    } else {
      await iterator.next();
      invalidate("other-connection");
      if (ending === "invalidated") {
        invalidate("c1");
        await expect(iterator.next()).rejects.toThrow(/connection.*changed/i);
      } else if (ending === "consumer-return") {
        await iterator.return(undefined);
      } else {
        expect(await iterator.next()).toMatchObject({ done: true });
      }
    }
    expect(dispose).toHaveBeenCalledOnce();
  });

  it("rejects a replacement connection between pages without reading it", async () => {
    const columns = [{ name: "id", isPrimaryKey: false }];
    const readTablePage = vi.fn(async () => ({
      columns,
      rows: [{ id: "server-a" }],
      totalCount: 2,
    }));
    const original = { readTablePage } as unknown as IDBDriver;
    const replacementRead = vi.fn(async () => ({
      columns,
      rows: [{ id: "server-b" }],
      totalCount: 2,
    }));
    let driver = original;
    const service = new TableReadService({
      getConnection: () => ({ id: "c1" }),
      getDriver: () => driver,
    } as never);
    const iterator = service.exportAll("c1", "db", "public", "records", 1);
    expect((await iterator.next()).value?.rows).toEqual([{ id: "server-a" }]);
    driver = { readTablePage: replacementRead } as unknown as IDBDriver;
    await expect(iterator.next()).rejects.toThrow(/connection.*changed/i);
    expect(replacementRead).not.toHaveBeenCalled();
  });

  it("rejects a late native export chunk and closes its iterator", async () => {
    let release!: () => void;
    const pending = new Promise<void>((resolve) => {
      release = resolve;
    });
    const closed = vi.fn();
    const original = {
      async *exportTableChunks() {
        try {
          await pending;
          yield { columns: [], rows: [] };
        } finally {
          closed();
        }
      },
    } as unknown as IDBDriver;
    let driver = original;
    const service = new TableReadService({
      getConnection: () => ({ id: "c1" }),
      getDriver: () => driver,
    } as never);
    const iterator = service.exportAll("c1", "db", "public", "records");
    const next = iterator.next();
    driver = {} as IDBDriver;
    release();
    await expect(next).rejects.toThrow(/connection.*changed/i);
    expect(closed).toHaveBeenCalledOnce();
  });
});
