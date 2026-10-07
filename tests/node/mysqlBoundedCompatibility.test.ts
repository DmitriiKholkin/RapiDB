import { createRequire } from "node:module";
import { dirname, join } from "node:path";
import {
  Connection,
  type ConnectionOptions,
  type FieldPacket,
  type Query,
} from "mysql2";
import { afterEach, describe, expect, it, vi } from "vitest";
import { BoundedQueryRows } from "../../src/extension/dbDrivers/boundedQueryRows";
import { MySQLDriver } from "../../src/extension/dbDrivers/mysql";

const drift = vi.hoisted(() => ({ mysqlPackage: { version: "3.24.4" } }));
vi.mock("mysql2/package.json", () => ({ default: drift.mysqlPackage }));

// Use the installed packet reader, command state machine, column decoder and
// row parser. No database/socket or fake result/fields emitter is involved.
const require = createRequire(import.meta.url);
const mysqlRoot = dirname(require.resolve("mysql2"));
type Packet = { buffer: Buffer; offset: number; end: number };
const PacketReader = require(join(mysqlRoot, "lib/packets/packet.js")) as new (
  sequence: number,
  buffer: Buffer,
  start: number,
  end: number,
) => Packet;
const Config = require(join(mysqlRoot, "lib/connection_config.js")) as new (
  options: ConnectionOptions,
) => ConnectionOptions;
type WireQuery = Query & {
  _rows: unknown[];
  _fields: (FieldPacket[] | undefined)[];
  _currentRows: unknown[][] | null;
  _resultIndex: number;
  execute(packet: Packet | undefined, connection: Connection): boolean;
};

function packet(payload: number[] | Buffer): Packet {
  const buffer = Buffer.concat([Buffer.alloc(4), Buffer.from(payload)]);
  return new PacketReader(0, buffer, 0, buffer.length);
}

function column(name: string): Packet {
  const strings = ["def", "", "", "", name, name].map((value) => {
    const text = Buffer.from(value);
    return Buffer.concat([Buffer.from([text.length]), text]);
  });
  // fixed metadata: UTF8MB4 charset, INT length/type, no flags/decimals.
  return packet(
    Buffer.concat([
      ...strings,
      Buffer.from([12, 45, 0, 11, 0, 0, 0, 3, 0, 0, 0, 0, 0]),
    ]),
  );
}
const eof = (more: boolean) => packet([254, 0, 0, more ? 8 : 0, 0]);
const ok = (affected: number, more: boolean) =>
  packet([0, affected, 17, more ? 8 : 0, 0, 2, 0]);

function harness(
  run: (command: WireQuery, native: Connection) => void = () => {},
  disableEval = false,
) {
  const native = Object.create(Connection.prototype) as Connection;
  Object.assign(native, {
    config: new Config({ disableEval }),
    clientEncoding: "utf8",
    serverEncoding: "utf8",
    _handshakePacket: { capabilityFlags: 512 },
    _resetSequenceId: vi.fn(),
    writePacket: vi.fn(),
    addCommand: vi.fn((command: WireQuery) => {
      expect(Reflect.get(command, "onResult")).toBeUndefined();
      command.execute(undefined, native);
      queueMicrotask(() => run(command, native));
      return command;
    }),
  });
  const rawQuery = vi.spyOn(native, "query");
  const connection = {
    connection: native,
    query: vi.fn(async (_input: unknown) => [[], []]),
    release: vi.fn(),
    destroy: vi.fn(),
  };
  const driver = new MySQLDriver({
    id: "mysql-compatibility",
    name: "MySQL",
    type: "mysql",
    host: "localhost",
  });
  const tracked = driver as unknown as {
    pool: unknown;
    activeQueryOperations: Set<unknown>;
    activeQueryConnections: Set<unknown>;
    activeQueryConnectionSlots: number;
  };
  tracked.pool = { getConnection: vi.fn(async () => connection) };
  return { driver, native, rawQuery, connection, tracked };
}

afterEach(() => {
  drift.mysqlPackage.version = "3.24.4";
  vi.restoreAllMocks();
});

async function expectNoSql(
  detail: string,
  mutateNative?: (native: Connection) => void,
) {
  const { driver, native, rawQuery, connection, tracked } = harness();
  mutateNative?.(native);
  await expect(
    driver.query("CALL report(); UPDATE items SET active = 1", undefined, {
      hardCap: 2,
      readOnly: true,
    }),
  ).rejects.toThrow(
    `Cannot safely bound MySQL query results: incompatible mysql2 ${drift.mysqlPackage.version} (${detail}`,
  );
  expect(rawQuery).not.toHaveBeenCalled();
  expect(Reflect.get(native, "writePacket")).not.toHaveBeenCalled();
  expect(connection.query).not.toHaveBeenCalled();
  // Preflight failed without starting a command; the untouched socket is reusable.
  expect(connection.release).toHaveBeenCalledOnce();
  expect(connection.destroy).not.toHaveBeenCalled();
  expect(tracked.activeQueryOperations.size).toBe(0);
  expect(tracked.activeQueryConnections.size).toBe(0);
  expect(tracked.activeQueryConnectionSlots).toBe(0);
}

function mutateCommand(mutate: (command: Query) => void) {
  const create = Connection.createQuery;
  vi.spyOn(Connection, "createQuery").mockImplementation((...args) => {
    const command = Reflect.apply(create, Connection, args) as Query;
    mutate(command);
    return command;
  });
}

describe("MySQL bounded adapter compatibility", () => {
  it("discards a read-only query connection when rollback fails", async () => {
    const { driver, connection, tracked } = harness();
    connection.query.mockImplementation(async (input) => {
      if (input === "ROLLBACK") throw new Error("rollback refused");
      return [[], []];
    });

    await expect(
      driver.query("SELECT 1", [], { readOnly: true }),
    ).rejects.toMatchObject({
      name: "AggregateError",
      message: expect.stringContaining("rollback failed"),
    });

    expect(connection.destroy).toHaveBeenCalledOnce();
    expect(connection.release).not.toHaveBeenCalled();
    expect(tracked.activeQueryConnections.size).toBe(0);
  });

  it("rejects unvalidated versions before any SQL, without a buffered fallback", async () => {
    drift.mysqlPackage.version = "4.0.0";
    await expectNoSql("unvalidated version");
  });

  it.each([
    "query",
    "config",
  ])("rejects missing native %s before any SQL", async (field) => {
    await expectNoSql("missing native connection query/config", (native) =>
      Reflect.set(native, field, undefined),
    );
  });

  it("rejects a missing command factory before any SQL", async () => {
    vi.spyOn(Connection, "createQuery");
    Reflect.set(Connection, "createQuery", undefined);
    await expectNoSql("missing Connection.createQuery");
  });

  it("also validates the actual detached user command before submitting it", async () => {
    mutateCommand((command) => {
      if (Reflect.get(command, "sql"))
        Reflect.set(command, "onResult", () => {});
    });
    const { driver, rawQuery, native, connection } = harness();
    await expect(
      driver.query("UPDATE items SET active = 1", undefined, { hardCap: 2 }),
    ).rejects.toThrow("unexpected callback-free Query shape");
    expect(rawQuery).not.toHaveBeenCalled();
    expect(Reflect.get(native, "writePacket")).not.toHaveBeenCalled();
    expect(connection.release).toHaveBeenCalledOnce();
  });

  it.each([
    "execute",
    "start",
    "resultsetHeader",
    "readField",
    "fieldsEOF",
    "row",
    "doneInsert",
    "done",
    "on",
    "off",
  ])("rejects a missing Query.%s hook before any SQL", async (method) => {
    mutateCommand((command) => Reflect.set(command, method, undefined));
    await expectNoSql(`missing Query.${method}`);
  });

  it.each([
    "_rows",
    "_fields",
    "_resultIndex",
    "_currentRows",
    "_currentFields",
    "_rowParser",
    "_queryOptions",
    "next",
  ])("rejects missing %s state before any SQL", async (field) => {
    mutateCommand((command) => Reflect.deleteProperty(command, field));
    await expectNoSql("unexpected callback-free Query shape");
  });

  it.each([
    ["onResult", () => {}],
    ["_rows", [[], []]],
    ["_fields", {}],
    ["_resultIndex", 1],
    ["_queryOptions", { rowsAsArray: false }],
  ])("rejects incompatible %s state before any SQL", async (field, value) => {
    mutateCommand((command) => Reflect.set(command, field as string, value));
    await expectNoSql("unexpected callback-free Query shape");
  });

  it.each([
    false,
    true,
  ])("drains installed multi-result/OK protocol with bounded histories (disableEval=%s)", async (disableEval) => {
    let drained = 0;
    let peakRetained = 0;
    let peakHistory = 0;
    const add = BoundedQueryRows.prototype.add;
    vi.spyOn(BoundedQueryRows.prototype, "add").mockImplementation(function (
      this: BoundedQueryRows<unknown>,
      row,
    ) {
      add.call(this, row);
      peakRetained = Math.max(peakRetained, this.rows.length);
    });
    const { driver, native, connection, rawQuery } = harness(
      (command, wire) => {
        const advance = (next: Packet) => {
          const ended = command.execute(next, wire);
          peakHistory = Math.max(
            peakHistory,
            command._rows.length,
            command._fields.length,
          );
          expect(command._currentRows?.length ?? 0).toBe(0);
          return ended;
        };
        command.on("result", (row) => {
          if (Array.isArray(row)) drained++;
          else
            expect(row).toMatchObject({
              affectedRows: 7,
              insertId: 17,
              warningStatus: 2,
            });
        });
        // Interleave OK packets between SELECT sets, not just one trailing OK.
        for (let set = 0; set < 40; set++) {
          expect(advance(ok(7, true))).toBe(false);
          expect(advance(packet([2]))).toBe(false);
          expect(advance(column("same"))).toBe(false);
          expect(advance(column("same"))).toBe(false);
          expect(advance(eof(false))).toBe(false);
          for (let row = 0; row < 30; row++) {
            const values = [String(set * 1000 + row), String(row + 1)];
            advance(
              packet(
                Buffer.concat(
                  values.map((value) =>
                    Buffer.concat([
                      Buffer.from([value.length]),
                      Buffer.from(value),
                    ]),
                  ),
                ),
              ),
            );
          }
          expect(advance(eof(true))).toBe(false);
          expect(command._resultIndex).toBe(1);
        }
        expect(advance(ok(7, false))).toBe(true);
        expect(command._rows).toHaveLength(0);
        expect(command._fields).toEqual([undefined]);
      },
      disableEval,
    );
    const result = await driver.query(
      "CALL report(?)",
      ["O'Reilly; \\ quoted"],
      {
        hardCap: 3,
      },
    );
    expect(drained).toBe(1200);
    expect(peakRetained).toBe(3);
    expect(peakHistory).toBeLessThanOrEqual(2);
    expect(result).toMatchObject({
      columns: ["same", "same"],
      rows: [
        { __col_0: 39000, __col_1: 1 },
        { __col_0: 39001, __col_1: 2 },
        { __col_0: 39002, __col_1: 3 },
      ],
      rowCount: 30,
      truncated: true,
    });
    expect(rawQuery).toHaveBeenCalledOnce();
    const command = rawQuery.mock.calls[0][0] as unknown as Query;
    expect(Reflect.get(command, "sql")).toBe(
      "CALL report('O\\'Reilly; \\\\ quoted')",
    );
    expect(Reflect.get(native, "writePacket")).toHaveBeenCalledOnce();
    expect(connection.release).toHaveBeenCalledOnce();
    expect(connection.destroy).not.toHaveBeenCalled();
  });

  it("preserves installed OK affectedRows across rebased mutation-only results", async () => {
    const { driver } = harness((command, native) => {
      for (let index = 0; index < 50; index++)
        expect(command.execute(ok(7, index < 49), native)).toBe(index === 49);
    });
    expect(
      await driver.query("CALL mutate()", undefined, { hardCap: 1 }),
    ).toMatchObject({
      columns: [],
      rows: [],
      rowCount: 350,
      affectedRows: 350,
      truncated: false,
    });
  });
});
