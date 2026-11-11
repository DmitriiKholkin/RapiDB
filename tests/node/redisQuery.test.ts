import { type createClient, ErrorReply, RESP_TYPES } from "redis";
import { withRedisReplyMapping } from "../support/redisReplyMapping";

const queryReplyOptions = {
  typeMapping: {
    [RESP_TYPES.SIMPLE_STRING]: Buffer,
    [RESP_TYPES.BLOB_STRING]: Buffer,
    [RESP_TYPES.VERBATIM_STRING]: Buffer,
    [RESP_TYPES.DOUBLE]: String,
    [RESP_TYPES.NUMBER]: String,
    [RESP_TYPES.BIG_NUMBER]: String,
    [RESP_TYPES.MAP]: Map,
  },
};

import { beforeEach, describe, expect, it, vi } from "vitest";
import { RedisDriver } from "../../src/extension/dbDrivers/redis";
import {
  REDIS_ALL_KEYS_TABLE,
  REDIS_UNPREFIXED_KEYS_TABLE,
} from "../../src/extension/dbDrivers/redisKeyspace";
import { logger } from "../../src/extension/utils/logger";
import { REDIS_READ_BUDGET } from "../../src/shared/safetyContracts";

function createDriver() {
  const driver = new RedisDriver({
    id: "redis-test",
    name: "Redis Test",
    type: "redis",
    host: "localhost",
  });

  const sendCommand = vi.fn().mockResolvedValue("OK");

  (
    driver as unknown as { client: { sendCommand: typeof sendCommand } | null }
  ).client = {
    sendCommand,
  };
  (driver as unknown as { connected: boolean }).connected = true;

  return { driver, sendCommand };
}

describe("RedisDriver — query()", () => {
  it("preserves quoted arguments and escaped spaces when tokenizing commands", async () => {
    const { driver, sendCommand } = createDriver();

    const result = await driver.query('SET key "hello world"');

    expect(sendCommand).toHaveBeenCalledWith(
      ["SET", "key", "hello world"],
      queryReplyOptions,
    );
    expect(result.rowCount).toBe(1);
    expect(result.columns).toEqual(["result"]);
    expect(result.rows[0]?.__col_0).toBe("OK");
  });

  it("returns completed statements when a later statement hits a transport failure", async () => {
    const { driver, sendCommand } = createDriver();
    sendCommand
      .mockResolvedValueOnce("OK")
      .mockRejectedValueOnce(new Error("Socket closed unexpectedly"));

    const result = await driver.query("SET a 1; INCR b; SET c 3");

    expect(sendCommand).toHaveBeenCalledTimes(2);
    expect(JSON.parse(String(result.rows[0]?.__col_0))).toEqual(["OK"]);
    expect(result.error).toBe(
      "Redis statement 2 of 3 failed: Socket closed unexpectedly. Statement 1 completed and any writes it made were applied; the outcome of statement 2 of 3 is unknown and later statements were not run.",
    );
  });

  it("does not double the period when the Redis error text already ends with one", async () => {
    const { driver, sendCommand } = createDriver();
    sendCommand
      .mockResolvedValueOnce("OK")
      .mockRejectedValueOnce(new ErrorReply("ERR something went wrong."));

    const rejected = await driver.query("SET a 1; INCR b; SET c 3");

    expect(rejected.error).toBe(
      "Redis statement 2 of 3 failed: ERR something went wrong. Statement 1 completed and any writes it made were applied; later statements were not run.",
    );

    sendCommand
      .mockResolvedValueOnce("OK")
      .mockRejectedValueOnce(new Error("Connection reset by peer. "));

    const transport = await driver.query("SET a 1; INCR b; SET c 3");

    expect(transport.error).toBe(
      "Redis statement 2 of 3 failed: Connection reset by peer. Statement 1 completed and any writes it made were applied; the outcome of statement 2 of 3 is unknown and later statements were not run.",
    );
    expect(transport.error).not.toContain("..");
  });

  it("reports earlier writes as applied when a later statement is rejected", async () => {
    const { driver, sendCommand } = createDriver();
    sendCommand
      .mockResolvedValueOnce("OK")
      .mockResolvedValueOnce("OK")
      .mockRejectedValueOnce(new ErrorReply("WRONGTYPE wrong kind of value"));

    const result = await driver.query("SET a 1; SET b 2; INCR h; SET c 3");

    expect(sendCommand).toHaveBeenCalledTimes(3);
    expect(JSON.parse(String(result.rows[0]?.__col_0))).toEqual([
      "OK",
      "OK",
      "(error) WRONGTYPE wrong kind of value",
    ]);
    expect(result.error).toBe(
      "Redis statement 3 of 4 failed: WRONGTYPE wrong kind of value. Statements 1 to 2 completed and any writes they made were applied; later statements were not run.",
    );
  });

  it("returns completed statements when the query is aborted mid-statement", async () => {
    const { driver, sendCommand } = createDriver();
    const controller = new AbortController();
    const client = { sendCommand, withAbortSignal: () => client };
    const cancelClientEntry = vi.fn();
    Object.assign(driver, {
      openClient: () => ({
        client,
        promise: Promise.resolve(client),
        cancelled: false,
        openingSettled: true,
        abort: new AbortController(),
      }),
      cancelClientEntry,
    });
    sendCommand.mockResolvedValueOnce("OK").mockImplementationOnce(() => {
      controller.abort(new Error("Query cancelled"));
      return new Promise(() => undefined);
    });

    const result = await driver.query("SET a 1; SET b 2; SET c 3", undefined, {
      signal: controller.signal,
    });

    expect(sendCommand).toHaveBeenCalledTimes(2);
    expect(JSON.parse(String(result.rows[0]?.__col_0))).toEqual(["OK"]);
    expect(result.error).toBe(
      "Redis statement 2 of 3 was cancelled. Statement 1 completed and any writes it made were applied; the outcome of statement 2 of 3 is unknown and later statements were not run.",
    );
    expect(cancelClientEntry).toHaveBeenCalled();
  });

  it("returns completed statements when a disconnect interrupts a script", async () => {
    const { driver, sendCommand } = createDriver();
    sendCommand.mockImplementationOnce(async () => {
      (driver as unknown as { epoch: number }).epoch++;
      return "OK";
    });

    const result = await driver.query("SET a 1; SET b 2");

    expect(sendCommand).toHaveBeenCalledTimes(1);
    expect(JSON.parse(String(result.rows[0]?.__col_0))).toEqual(["OK"]);
    expect(result.error).toBe(
      "Redis script was cancelled before statement 2 of 2. Statement 1 completed and any writes it made were applied; later statements were not run.",
    );
  });

  it("reports queued commands as not applied when a statement inside MULTI is rejected", async () => {
    const { driver, sendCommand } = createDriver();
    sendCommand
      .mockResolvedValueOnce("OK")
      .mockResolvedValueOnce("QUEUED")
      .mockRejectedValueOnce(new ErrorReply("ERR unknown command 'BADCMD'"));

    const result = await driver.query("MULTI; SET a 1; BADCMD; EXEC");

    expect(sendCommand).toHaveBeenCalledTimes(4);
    expect(sendCommand).toHaveBeenLastCalledWith(["DISCARD"]);
    expect(result.error).toBe(
      "Redis statement 3 of 4 failed: ERR unknown command 'BADCMD'. The commands queued after MULTI at statement 1 were not applied because EXEC did not run; later statements were not run.",
    );
  });

  it("reports the transaction outcome as unknown when EXEC hits a transport failure", async () => {
    const { driver, sendCommand } = createDriver();
    sendCommand
      .mockResolvedValueOnce("OK")
      .mockResolvedValueOnce("QUEUED")
      .mockRejectedValueOnce(new Error("Socket closed unexpectedly"));

    const result = await driver.query("MULTI; SET a 1; EXEC");

    expect(JSON.parse(String(result.rows[0]?.__col_0))).toEqual([
      "OK",
      "QUEUED",
    ]);
    expect(result.error).toBe(
      "Redis statement 3 of 3 failed: Socket closed unexpectedly. The outcome of the transaction started by MULTI at statement 1 is unknown; later statements were not run.",
    );
  });

  it("reports the transaction as discarded when EXEC is rejected", async () => {
    const { driver, sendCommand } = createDriver();
    sendCommand
      .mockResolvedValueOnce("OK")
      .mockResolvedValueOnce("QUEUED")
      .mockRejectedValueOnce(
        new ErrorReply("EXECABORT Transaction discarded because of errors"),
      );

    const result = await driver.query("MULTI; SET a 1; EXEC; SET b 2");

    expect(sendCommand).toHaveBeenCalledTimes(3);
    expect(result.error).toBe(
      "Redis statement 3 of 4 failed: EXECABORT Transaction discarded because of errors. The transaction started by MULTI at statement 1 was discarded and its queued commands were not applied; later statements were not run.",
    );
  });

  it("separates writes applied before MULTI from queued commands", async () => {
    const { driver, sendCommand } = createDriver();
    sendCommand
      .mockResolvedValueOnce("OK")
      .mockResolvedValueOnce("OK")
      .mockRejectedValueOnce(new ErrorReply("ERR wrong number of arguments"));

    const result = await driver.query("SET a 1; multi; SET b; EXEC");

    expect(sendCommand).toHaveBeenLastCalledWith(["DISCARD"]);
    expect(result.error).toBe(
      "Redis statement 3 of 4 failed: ERR wrong number of arguments. Statement 1 completed and any writes it made were applied. The commands queued after MULTI at statement 2 were not applied because EXEC did not run; later statements were not run.",
    );
  });

  it("treats statements after a completed transaction as outside MULTI", async () => {
    const { driver, sendCommand } = createDriver();
    sendCommand
      .mockResolvedValueOnce("OK")
      .mockResolvedValueOnce("QUEUED")
      .mockResolvedValueOnce(["OK"])
      .mockRejectedValueOnce(new ErrorReply("WRONGTYPE wrong kind of value"));

    const result = await driver.query("MULTI; SET a 1; EXEC; INCR h; SET c 3");

    expect(sendCommand).toHaveBeenCalledTimes(4);
    expect(result.error).toBe(
      "Redis statement 4 of 5 failed: WRONGTYPE wrong kind of value. Statements 1 to 3 completed and any writes they made were applied; later statements were not run.",
    );
  });

  it("still discards the transaction when DISCARD is rejected", async () => {
    const { driver, sendCommand } = createDriver();
    sendCommand
      .mockResolvedValueOnce("OK")
      .mockResolvedValueOnce("QUEUED")
      .mockRejectedValueOnce(
        new ErrorReply("ERR wrong number of arguments for 'discard' command"),
      );

    const result = await driver.query("MULTI; SET a 1; DISCARD extra; SET b 2");

    expect(sendCommand).toHaveBeenCalledTimes(4);
    expect(sendCommand).toHaveBeenLastCalledWith(["DISCARD"]);
    expect(result.error).toBe(
      "Redis statement 3 of 4 failed: ERR wrong number of arguments for 'discard' command. The commands queued after MULTI at statement 1 were not applied because EXEC did not run; later statements were not run.",
    );
  });

  it("still discards the transaction when EXEC is rejected without EXECABORT", async () => {
    const { driver, sendCommand } = createDriver();
    sendCommand
      .mockResolvedValueOnce("OK")
      .mockResolvedValueOnce("QUEUED")
      .mockRejectedValueOnce(
        new ErrorReply("ERR wrong number of arguments for 'exec' command"),
      );

    const result = await driver.query("MULTI; SET a 1; EXEC x");

    expect(sendCommand).toHaveBeenCalledTimes(4);
    expect(sendCommand).toHaveBeenLastCalledWith(["DISCARD"]);
    expect(result.error).toBe(
      "Redis statement 3 of 3 failed: ERR wrong number of arguments for 'exec' command. The transaction started by MULTI at statement 1 was discarded and its queued commands were not applied; later statements were not run.",
    );
  });

  it("reports a WATCH-aborted transaction as not applied when a later statement fails", async () => {
    const { driver, sendCommand } = createDriver();
    sendCommand
      .mockResolvedValueOnce("OK")
      .mockResolvedValueOnce("OK")
      .mockResolvedValueOnce("QUEUED")
      .mockResolvedValueOnce(null)
      .mockRejectedValueOnce(new ErrorReply("ERR unknown command 'BADCMD'"));

    const result = await driver.query("WATCH a; MULTI; SET a 1; EXEC; BADCMD");

    expect(sendCommand).toHaveBeenCalledTimes(5);
    expect(result.error).toBe(
      "Redis statement 5 of 5 failed: ERR unknown command 'BADCMD'. Statements 1 to 4 completed. The commands queued after MULTI at statement 2 were not applied because EXEC at statement 4 returned nil (a WATCHed key changed). Any other writes they made were applied; later statements were not run.",
    );
  });

  it("reports a discarded transaction as not applied when a later statement fails", async () => {
    const { driver, sendCommand } = createDriver();
    sendCommand
      .mockResolvedValueOnce("OK")
      .mockResolvedValueOnce("QUEUED")
      .mockResolvedValueOnce("OK")
      .mockRejectedValueOnce(new ErrorReply("ERR unknown command 'BADCMD'"));

    const result = await driver.query("MULTI; SET a 1; DISCARD; BADCMD");

    expect(sendCommand).toHaveBeenCalledTimes(4);
    expect(result.error).toBe(
      "Redis statement 4 of 4 failed: ERR unknown command 'BADCMD'. Statements 1 to 3 completed. The commands queued after MULTI at statement 1 were not applied because DISCARD at statement 3 discarded them. Any other writes they made were applied; later statements were not run.",
    );

    sendCommand
      .mockResolvedValueOnce("OK")
      .mockResolvedValueOnce("QUEUED")
      .mockResolvedValueOnce("OK")
      .mockResolvedValueOnce("OK")
      .mockRejectedValueOnce(new ErrorReply("ERR wrong number of arguments"));

    const reopened = await driver.query(
      "MULTI; SET a 1; DISCARD; MULTI; SET b; EXEC",
    );

    expect(sendCommand).toHaveBeenLastCalledWith(["DISCARD"]);
    expect(reopened.error).toBe(
      "Redis statement 5 of 6 failed: ERR wrong number of arguments. Statements 1 to 3 completed. The commands queued after MULTI at statement 1 were not applied because DISCARD at statement 3 discarded them. Any other writes they made were applied. The commands queued after MULTI at statement 4 were not applied because EXEC did not run; later statements were not run.",
    );
  });

  it("treats RESET inside MULTI as discarding the transaction on a dedicated client", async () => {
    const { driver, sendCommand: sharedSendCommand } = createDriver();
    const sendCommand = vi
      .fn()
      .mockResolvedValueOnce("OK")
      .mockResolvedValueOnce("QUEUED")
      .mockResolvedValueOnce("RESET")
      .mockResolvedValueOnce("OK")
      .mockRejectedValueOnce(new ErrorReply("ERR unknown command 'BADCMD'"));
    const client = { sendCommand };
    const cancelClientEntry = vi.fn();
    Object.assign(driver, {
      openClient: () => ({
        client,
        promise: Promise.resolve(client),
        cancelled: false,
        openingSettled: true,
        abort: new AbortController(),
      }),
      cancelClientEntry,
    });

    const result = await driver.query("MULTI; SET a 1; RESET; SET b 2; BADCMD");

    expect(sharedSendCommand).not.toHaveBeenCalled();
    expect(sendCommand).toHaveBeenCalledTimes(5);
    expect(sendCommand).not.toHaveBeenCalledWith(["DISCARD"]);
    expect(cancelClientEntry).toHaveBeenCalled();
    expect(result.error).toBe(
      "Redis statement 5 of 5 failed: ERR unknown command 'BADCMD'. Statements 1 to 4 completed. The commands queued after MULTI at statement 1 were not applied because RESET at statement 3 discarded them. Any other writes they made were applied; later statements were not run.",
    );

    sendCommand
      .mockResolvedValueOnce("OK")
      .mockResolvedValueOnce("QUEUED")
      .mockResolvedValueOnce("OK")
      .mockRejectedValueOnce(new Error("Socket closed unexpectedly"));

    const quit = await driver.query("MULTI; SET a 1; QUIT; SET b 2");

    expect(quit.error).toBe(
      "Redis statement 4 of 4 failed: Socket closed unexpectedly. Statements 1 to 3 completed. The commands queued after MULTI at statement 1 were not applied because QUIT at statement 3 closed the connection. Any other writes they made were applied; the outcome of statement 4 of 4 is unknown and later statements were not run.",
    );
  });

  it("notes per-command errors inside a successful EXEC when a later statement fails", async () => {
    const { driver, sendCommand } = createDriver();
    sendCommand
      .mockResolvedValueOnce("OK")
      .mockResolvedValueOnce("QUEUED")
      .mockResolvedValueOnce("QUEUED")
      .mockResolvedValueOnce([
        new ErrorReply("ERR value is not an integer or out of range"),
        "OK",
      ])
      .mockRejectedValueOnce(new ErrorReply("ERR unknown command 'BADCMD'"));

    const result = await driver.query("MULTI; INCR s; SET b 2; EXEC; BADCMD");

    expect(sendCommand).toHaveBeenCalledTimes(5);
    expect(result.error).toBe(
      "Redis statement 5 of 5 failed: ERR unknown command 'BADCMD'. Statements 1 to 4 completed. Some of the commands queued after MULTI at statement 1 returned errors in the EXEC result at statement 4. Any other writes they made were applied; later statements were not run.",
    );
  });

  it("reports queued commands as not applied when cancelled before EXEC", async () => {
    const { driver, sendCommand } = createDriver();
    sendCommand.mockResolvedValueOnce("OK").mockImplementationOnce(async () => {
      (driver as unknown as { epoch: number }).epoch++;
      return "QUEUED";
    });

    const result = await driver.query("MULTI; SET a 1; EXEC");

    expect(sendCommand).toHaveBeenCalledTimes(2);
    expect(result.error).toBe(
      "Redis script was cancelled before statement 3 of 3. The commands queued after MULTI at statement 1 were not applied because EXEC did not run; later statements were not run.",
    );
  });

  it("still throws a transport failure on the first statement", async () => {
    const { driver, sendCommand } = createDriver();
    sendCommand.mockRejectedValueOnce(new Error("Socket closed unexpectedly"));

    await expect(driver.query("SET a 1; SET b 2")).rejects.toThrow(
      "Socket closed unexpectedly",
    );
    expect(sendCommand).toHaveBeenCalledTimes(1);
  });

  it("runs multiple preview-style Redis commands sequentially", async () => {
    const driver = new RedisDriver({
      id: "redis-query-preview-test",
      name: "Redis Query Preview Test",
      type: "redis",
      host: "localhost",
    });
    const sendCommand = vi
      .fn()
      .mockResolvedValueOnce(1)
      .mockResolvedValueOnce(2);

    (
      driver as unknown as {
        client: { sendCommand: typeof sendCommand } | null;
      }
    ).client = {
      sendCommand,
    };
    (driver as unknown as { connected: boolean }).connected = true;

    const result = await driver.query(
      'DEL "list:activity:recent"\n\nRPUSH "list:activity:recent" "{\\"id\\":\\"high-1\\",\\"type\\":\\"payment\\",\\"amount\\":999.99}" "{\\"id\\":\\"high-2\\",\\"type\\":\\"alert\\",\\"message\\":\\"System critical\\"}"',
    );

    expect(sendCommand).toHaveBeenNthCalledWith(
      1,
      ["DEL", "list:activity:recent"],
      queryReplyOptions,
    );
    expect(sendCommand).toHaveBeenNthCalledWith(
      2,
      [
        "RPUSH",
        "list:activity:recent",
        '{"id":"high-1","type":"payment","amount":999.99}',
        '{"id":"high-2","type":"alert","message":"System critical"}',
      ],
      queryReplyOptions,
    );
    expect(result.rowCount).toBe(1);
    expect(result.columns).toEqual(["results"]);
    expect(result.columnMeta).toEqual([{ category: "json" }]);
    expect(result.rows[0]?.__col_0).toBe("[1,2]");
  });

  it("marks structured replies for JSON-aware export but leaves JSON-looking strings as text", async () => {
    const { driver, sendCommand } = createDriver();
    sendCommand
      .mockResolvedValueOnce("string")
      .mockResolvedValueOnce("hash")
      .mockResolvedValueOnce("list");

    const arrayResult = await driver.query(
      "TYPE users:1\nTYPE users:2\nTYPE users:3",
    );

    expect(arrayResult.columnMeta).toEqual([{ category: "json" }]);
    expect(arrayResult.rows[0]?.__col_0).toBe('["string","hash","list"]');

    sendCommand.mockResolvedValueOnce([
      Buffer.from("draft"),
      Buffer.from("123"),
    ]);
    const singleArrayResult = await driver.query('LRANGE "users:3" 0 -1');

    expect(singleArrayResult.columns).toEqual(["result"]);
    expect(singleArrayResult.columnMeta).toEqual([{ category: "json" }]);
    expect(singleArrayResult.rows[0]?.__col_0).toBe('["draft","123"]');

    sendCommand.mockResolvedValueOnce(
      new Map([
        ["first", "value-1"],
        ["second", "value-2"],
      ]),
    );
    const objectResult = await driver.query('HGETALL "users:2"');

    expect(objectResult.columnMeta).toEqual([{ category: "json" }]);
    expect(objectResult.rows[0]?.__col_0).toBe(
      '{"first":"value-1","second":"value-2"}',
    );

    sendCommand.mockResolvedValueOnce('["not","an","array"]');
    const stringResult = await driver.query('GET "json-looking-value"');

    expect(stringResult.columnMeta).toEqual([{ category: null }]);
    expect(stringResult.rows[0]?.__col_0).toBe('["not","an","array"]');
  });

  it("keeps backslashes outside quotes literal like redis-cli", async () => {
    const { driver, sendCommand } = createDriver();

    await driver.query("SET key hello\\ world");

    expect(sendCommand).toHaveBeenCalledWith(
      ["SET", "key", "hello\\", "world"],
      queryReplyOptions,
    );
  });

  it("decodes JSON escapes only in double-quoted arguments", async () => {
    const { driver, sendCommand } = createDriver();
    await driver.query(
      String.raw`SET "key\n\r\t\u0000\u96EA" "\b\f\/\uD83D\uDE00\\n\\u0000\""`,
    );
    expect(sendCommand).toHaveBeenCalledExactlyOnceWith(
      ["SET", "key\n\r\t\0雪", '\b\f/😀\\n\\u0000"'],
      queryReplyOptions,
    );
  });

  it.each([
    [
      String.raw`SET key '\n\r\t\b\f\u0000\u96EA\\path\'quoted\'\ space'`,
      ["SET", "key", String.raw`\n\r\t\b\f\u0000\u96EA\\path'quoted'\ space`],
    ],
    [
      String.raw`SET key \n\r\t\b\f\u0000\u96EA\\path\ space`,
      ["SET", "key", `${String.raw`\n\r\t\b\f\u0000\u96EA\\path`}\\`, "space"],
    ],
  ])("keeps backslashes literal in single-quoted and unquoted input %s", async (query, expected) => {
    const { driver, sendCommand } = createDriver();
    await driver.query(query);
    expect(sendCommand).toHaveBeenCalledExactlyOnceWith(
      expected,
      queryReplyOptions,
    );
  });

  it.each([
    [
      String.raw`"\u0047ET" "key\u0022\u003B SET victim value\n\u0000"`,
      true,
      ["GET", 'key"; SET victim value\n\0'],
    ],
    [String.raw`"\u0053ET" key value`, false, ["SET", "key", "value"]],
    [
      String.raw`"G\u0045T" key; "S\u0045T" victim value`,
      false,
      ["GET", "key"],
    ],
    [String.raw`'G\u0045T' key`, false, [String.raw`G\U0045T`, "key"]],
    ["PING", true, ["PING"]],
  ] as const)("classifies the same decoded commands as execution for %s", async (query, allowed, firstCommand) => {
    const { driver, sendCommand } = createDriver();
    expect(driver.getCapabilities().readOnlyQueryGuard?.(query).allowed).toBe(
      allowed,
    );
    await driver.query(query);
    expect(sendCommand).toHaveBeenNthCalledWith(
      1,
      [...firstCommand],
      queryReplyOptions,
    );
    expect(sendCommand).toHaveBeenCalledTimes(query.includes('; "') ? 2 : 1);
  });

  it.each([
    String.raw`GET "key\u123"`,
    String.raw`GET "key\uZZZZ"`,
  ])("rejects malformed Unicode escapes consistently for %s", async (query) => {
    const { driver, sendCommand } = createDriver();
    expect(driver.getCapabilities().readOnlyQueryGuard?.(query).allowed).toBe(
      false,
    );
    await expect(driver.query(query)).rejects.toThrow("invalid Unicode escape");
    expect(sendCommand).not.toHaveBeenCalled();
  });

  it("rejects unterminated quoted arguments", async () => {
    const { driver } = createDriver();

    await expect(driver.query('SET key "hello world')).rejects.toThrow(
      /unterminated quoted argument/i,
    );
  });

  it.each([
    String.raw`DEL "key\uD800"`,
    String.raw`DEL "key\uDC00"`,
    `DEL "key${String.fromCharCode(0xd800)}"`,
    `DEL key${String.fromCharCode(0xdc00)}`,
  ])("rejects keys that UTF-8 would silently replace: %s", async (query) => {
    const { driver, sendCommand } = createDriver();
    await expect(driver.query(query)).rejects.toThrow(/Unicode|UTF-8/);
    expect(sendCommand).not.toHaveBeenCalled();
  });

  it.each([
    String.raw`SET target "value\uZZZZ"`,
    String.raw`SET target "value\uD800"`,
  ])("validates all commands before sending the first mutation: %s", async (tail) => {
    const { driver, sendCommand } = createDriver();
    await expect(driver.query(`DEL target; ${tail}`)).rejects.toThrow(
      /Unicode|UTF-8/,
    );
    expect(sendCommand).not.toHaveBeenCalled();
  });

  it("preserves valid surrogate pairs and literal replacement-character keys", async () => {
    const { driver, sendCommand } = createDriver();
    await driver.query(String.raw`DEL "key\uD83D\uDE00" "key�"`);
    expect(sendCommand).toHaveBeenCalledExactlyOnceWith(
      ["DEL", "key😀", "key�"],
      queryReplyOptions,
    );
  });
});

describe("RedisDriver — disconnect()", () => {
  it("destroys the active client and clears connection state", async () => {
    const driver = new RedisDriver({
      id: "redis-disconnect-test",
      name: "Redis Disconnect Test",
      type: "redis",
      host: "localhost",
    });
    const destroy = vi.fn();
    const driverState = driver as unknown as {
      client: { destroy: typeof destroy; isOpen: boolean } | null;
      connected: boolean;
    };

    driverState.client = { destroy, isOpen: true };
    driverState.connected = true;

    await driver.disconnect();

    expect(destroy).toHaveBeenCalledTimes(1);
    expect(driverState.client).toBeNull();
    expect(driver.isConnected()).toBe(false);
  });

  it("skips destroy when the client socket is already closed", async () => {
    const driver = new RedisDriver({
      id: "redis-disconnect-closed-client",
      name: "Redis Disconnect Closed Client",
      type: "redis",
      host: "localhost",
    });
    const destroy = vi.fn();
    const driverState = driver as unknown as {
      client: { destroy: typeof destroy; isOpen: boolean } | null;
      connected: boolean;
    };

    driverState.client = { destroy, isOpen: false };
    driverState.connected = true;

    await driver.disconnect();

    expect(destroy).not.toHaveBeenCalled();
    expect(driverState.client).toBeNull();
    expect(driver.isConnected()).toBe(false);
  });

  it("remains idempotent when no client is present", async () => {
    const driver = new RedisDriver({
      id: "redis-disconnect-empty",
      name: "Redis Disconnect Empty",
      type: "redis",
      host: "localhost",
    });

    await expect(driver.disconnect()).resolves.toBeUndefined();
    expect(driver.isConnected()).toBe(false);
  });
});

describe("RedisDriver — metadata and pages", () => {
  beforeEach(() => {
    vi.spyOn(
      RedisDriver.prototype as unknown as {
        getDatabaseClient(
          database?: string,
        ): Promise<ReturnType<typeof createClient>>;
      },
      "getDatabaseClient",
    ).mockImplementation(async function (this: unknown) {
      return (this as { client: ReturnType<typeof createClient> }).client;
    });
  });
  it("paginates over the same sorted key set on every page", async () => {
    const driver = new RedisDriver({
      id: "redis-stable-pages",
      name: "Redis Stable Pages",
      type: "redis",
      host: "localhost",
    });
    const client = {
      scan: vi.fn().mockResolvedValue({
        cursor: "0",
        keys: ["users:b", "users:d", "users:a", "users:c"],
      }),
      type: vi.fn().mockResolvedValue("string"),
      get: vi.fn(async (key: string) => key),
    };
    (
      driver as unknown as {
        client: typeof client | null;
        connected: boolean;
      }
    ).client = withRedisReplyMapping(client);
    (driver as unknown as { connected: boolean }).connected = true;

    const request = {
      database: "db0",
      schema: "db0",
      table: "users",
      pageSize: 2,
      filters: [],
      sort: { column: "key", direction: "asc" } as const,
      skipCount: true,
    };
    const firstPage = await driver.readTablePage({ ...request, page: 1 });
    const secondPage = await driver.readTablePage({ ...request, page: 2 });

    expect(firstPage.rows.map((row) => row.key)).toEqual([
      "users:a",
      "users:b",
    ]);
    expect(secondPage.rows.map((row) => row.key)).toEqual([
      "users:c",
      "users:d",
    ]);
  });

  it("builds string insert previews without scanning or inferring group types", async () => {
    const driver = new RedisDriver({
      id: "redis-insert-no-inference",
      name: "Redis Insert No Inference",
      type: "redis",
      host: "localhost",
    });
    const client = {
      scan: vi.fn().mockResolvedValue({
        cursor: "0",
        keys: ["users:1", "users:2"],
      }),
      type: vi.fn().mockResolvedValue("list"),
    };

    (
      driver as unknown as {
        client: typeof client | null;
        connected: boolean;
      }
    ).client = withRedisReplyMapping(client);
    (driver as unknown as { connected: boolean }).connected = true;

    const preview = await driver.buildMutationPreviewStatements(
      "insert",
      "db0",
      "db0",
      "users",
      {
        values: {
          key: "users:3",
          value: '["a","b"]',
        },
      },
    );

    expect(preview).toEqual(['SET "users:3" "[\\"a\\",\\"b\\"]" "NX"']);
    expect(client.scan).not.toHaveBeenCalled();
    expect(client.type).not.toHaveBeenCalled();
  });

  it("lists logical databases and groups keys into table-like prefixes", async () => {
    const driver = new RedisDriver({
      id: "redis-metadata-test",
      name: "Redis Metadata Test",
      type: "redis",
      host: "localhost",
    });
    const client = {
      info: vi
        .fn()
        .mockResolvedValue(
          "# Keyspace\ndb0:keys=2,expires=0,avg_ttl=0\ndb4:keys=7,expires=0,avg_ttl=0\n",
        ),
      scan: vi.fn().mockResolvedValue({
        cursor: "0",
        keys: ["users:1", "users:2", "orders:1", "orphan"],
      }),
    };

    (
      driver as unknown as {
        client: typeof client | null;
        connected: boolean;
      }
    ).client = withRedisReplyMapping(client);
    (driver as unknown as { connected: boolean }).connected = true;

    await expect(driver.listDatabases()).resolves.toEqual([
      { name: "db0", schemas: [] },
      { name: "db4", schemas: [] },
    ]);
    await expect(driver.listObjects()).resolves.toEqual([
      { schema: "", name: REDIS_ALL_KEYS_TABLE, type: "table" },
      { schema: "", name: REDIS_UNPREFIXED_KEYS_TABLE, type: "table" },
      { schema: "", name: "orders", type: "table" },
      { schema: "", name: "users", type: "table" },
    ]);
    expect(client.scan).toHaveBeenCalledWith("0", {
      MATCH: "*",
      COUNT: 1000,
    });
  });

  it("reads mixed Redis value types into stable flattened page rows and inferred columns", async () => {
    const driver = new RedisDriver({
      id: "redis-page-test",
      name: "Redis Page Test",
      type: "redis",
      host: "localhost",
    });
    const client = {
      scan: vi.fn().mockResolvedValue({
        cursor: "0",
        keys: ["users:5", "users:3", "users:4", "users:2", "users:1"],
      }),
      type: vi.fn(async (key: string) => {
        if (key === "users:1") {
          return "string";
        }
        if (key === "users:2") {
          return "hash";
        }
        if (key === "users:3") {
          return "list";
        }
        if (key === "users:4") {
          return "set";
        }
        if (key === "users:5") {
          return "zset";
        }
        return "none";
      }),
      get: vi.fn(async (key: string) => (key === "users:1" ? "Alice" : null)),
      hGetAll: vi.fn(async (key: string) =>
        key === "users:2" ? { first: "Bob", last: "Builder" } : {},
      ),
      lRange: vi.fn(async (key: string) =>
        key === "users:3" ? ["draft", "published"] : [],
      ),
      sMembers: vi.fn(async (key: string) =>
        key === "users:4" ? ["alpha", "beta"] : [],
      ),
      zRangeWithScores: vi.fn(async (key: string) =>
        key === "users:5"
          ? [
              { value: "gold", score: 10 },
              { value: "silver", score: 5 },
            ]
          : [],
      ),
    };

    (
      driver as unknown as {
        client: typeof client | null;
        connected: boolean;
      }
    ).client = withRedisReplyMapping(client);
    (driver as unknown as { connected: boolean }).connected = true;

    const described = await driver.describeColumns("db0", "db0", "users");
    const page = await driver.readTablePage({
      database: "db0",
      schema: "db0",
      table: "users",
      page: 1,
      pageSize: 10,
      filters: [],
      sort: { column: "key", direction: "asc" },
      skipCount: false,
    });

    expect(described).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          name: "key",
          isPrimaryKey: true,
          primaryKeyOrdinal: 1,
          category: "text",
          type: "string",
          nativeType: "string",
        }),
        expect.objectContaining({
          name: "value",
          category: "other",
          type: "mixed(string, hash, list, set, zset)",
          nativeType: "mixed(string, hash, list, set, zset)",
        }),
        expect.objectContaining({
          name: "type",
          category: "text",
          type: "string",
          nativeType: "redis_type",
          isReadOnly: true,
        }),
      ]),
    );
    expect(page.totalCount).toBe(5);
    expect(page.rows).toEqual([
      { key: "users:1", value: "Alice", ttl: null, type: "string" },
      {
        key: "users:2",
        value: '{"first":"Bob","last":"Builder"}',
        ttl: null,
        type: "hash",
      },
      {
        key: "users:3",
        value: '["draft","published"]',
        ttl: null,
        type: "list",
      },
      {
        key: "users:4",
        value: '["alpha","beta"]',
        ttl: null,
        type: "set",
      },
      {
        key: "users:5",
        value: '[{"value":"gold","score":10},{"value":"silver","score":5}]',
        ttl: null,
        type: "zset",
      },
    ]);
    const hashRows = await driver.readTablePage({
      database: "db0",
      schema: "db0",
      table: "users",
      page: 1,
      pageSize: 10,
      filters: [{ column: "type", operator: "eq", value: "hash" }],
      sort: { column: "key", direction: "asc" },
      skipCount: false,
    });
    expect(hashRows.rows).toEqual([
      {
        key: "users:2",
        value: '{"first":"Bob","last":"Builder"}',
        ttl: null,
        type: "hash",
      },
    ]);
  });

  it("reads only page keys when key-sort paging has no filters", async () => {
    const driver = new RedisDriver({
      id: "redis-key-page-test",
      name: "Redis Key Page Test",
      type: "redis",
      host: "localhost",
    });
    const client = {
      scan: vi.fn().mockResolvedValue({
        cursor: "0",
        keys: ["users:5", "users:3", "users:4", "users:2", "users:1"],
      }),
      type: vi.fn(async (key: string) =>
        key === "users:2" ? "hash" : "string",
      ),
      get: vi.fn(async (key: string) => `value:${key}`),
      hGetAll: vi.fn(async () => ({ first: "Bob", last: "Builder" })),
      lRange: vi.fn(async () => []),
      sMembers: vi.fn(async () => []),
      zRangeWithScores: vi.fn(async () => []),
    };

    (
      driver as unknown as {
        client: typeof client | null;
        connected: boolean;
      }
    ).client = withRedisReplyMapping(client);
    (driver as unknown as { connected: boolean }).connected = true;

    const page = await driver.readTablePage({
      database: "db0",
      schema: "db0",
      table: "users",
      page: 2,
      pageSize: 2,
      filters: [],
      sort: { column: "key", direction: "asc" },
      skipCount: false,
    });

    expect(page.rows.map((row) => row.key)).toEqual(["users:3", "users:4"]);
    expect(client.type).toHaveBeenCalledTimes(2);
    expect(client.type).toHaveBeenNthCalledWith(1, "users:3");
    expect(client.type).toHaveBeenNthCalledWith(2, "users:4");
  });

  it("uses the native Redis stream type for the value column", async () => {
    const driver = new RedisDriver({
      id: "redis-stream-test",
      name: "Redis Stream Test",
      type: "redis",
      host: "localhost",
    });
    const client = {
      scan: vi.fn().mockResolvedValue({
        cursor: "0",
        keys: ["events:1"],
      }),
      type: vi.fn().mockResolvedValue("stream"),
      xRange: vi.fn().mockResolvedValue([
        {
          id: "1716115200000-0",
          message: { event: "created", userId: "42" },
        },
      ]),
    };

    (
      driver as unknown as {
        client: typeof client | null;
        connected: boolean;
      }
    ).client = withRedisReplyMapping(client);
    (driver as unknown as { connected: boolean }).connected = true;

    const described = await driver.describeColumns("db0", "db0", "events");
    const page = await driver.readTablePage({
      database: "db0",
      schema: "db0",
      table: "events",
      page: 1,
      pageSize: 10,
      filters: [],
      sort: { column: "key", direction: "asc" },
      skipCount: false,
    });

    expect(described).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          name: "value",
          type: "stream",
          nativeType: "stream",
        }),
      ]),
    );
    expect(page.rows).toEqual([
      {
        key: "events:1",
        value:
          '[{"id":"1716115200000-0","message":{"event":"created","userId":"42"}}]',
        ttl: null,
        type: "stream",
      },
    ]);
    expect(client.xRange).toHaveBeenCalledWith("events:1", "-", "+", {
      COUNT: REDIS_READ_BUDGET.maxValueEntries + 1,
    });
  });

  it("filters rows by ttl column values", async () => {
    const driver = new RedisDriver({
      id: "redis-ttl-filter-test",
      name: "Redis TTL Filter Test",
      type: "redis",
      host: "localhost",
    });
    const client = {
      scan: vi.fn().mockResolvedValue({
        cursor: "0",
        keys: ["users:1", "users:2", "users:3"],
      }),
      type: vi.fn().mockResolvedValue("string"),
      get: vi.fn(async (key: string) => key),
      ttl: vi.fn(async (key: string) => {
        if (key === "users:1") {
          return -1;
        }
        if (key === "users:2") {
          return 5;
        }
        return 120;
      }),
    };

    (
      driver as unknown as {
        client: typeof client | null;
        connected: boolean;
      }
    ).client = withRedisReplyMapping(client);
    (driver as unknown as { connected: boolean }).connected = true;

    const page = await driver.readTablePage({
      database: "db0",
      schema: "db0",
      table: "users",
      page: 1,
      pageSize: 10,
      filters: [
        {
          column: "ttl",
          operator: "gt",
          value: "10",
        },
      ],
      sort: { column: "ttl", direction: "asc" },
      skipCount: false,
    });

    expect(page.rows).toEqual([
      { key: "users:3", value: "users:3", ttl: 120, type: "string" },
    ]);
    expect(page.totalCount).toBe(1);
  });

  it("preserves Redis list type when updating a list-backed key", async () => {
    const driver = new RedisDriver({
      id: "redis-list-update-test",
      name: "Redis List Update Test",
      type: "redis",
      host: "localhost",
    });
    const client = {
      exists: vi.fn().mockResolvedValue(1),
      type: vi.fn().mockResolvedValue("list"),
      del: vi.fn().mockResolvedValue(1),
      rPush: vi.fn().mockResolvedValue(2),
      sendCommand: vi.fn().mockResolvedValue(1),
    };

    (
      driver as unknown as {
        client: typeof client | null;
        connected: boolean;
      }
    ).client = withRedisReplyMapping(client);
    (driver as unknown as { connected: boolean }).connected = true;

    const listJson =
      '["{\\"id\\":\\"high-1\\",\\"type\\":\\"payment\\",\\"amount\\":999.99}","{\\"id\\":\\"high-2\\",\\"type\\":\\"alert\\",\\"message\\":\\"System critical\\"}"]';

    const [preview] = await driver.buildMutationPreviewStatements(
      "update",
      "db0",
      "db0",
      "list",
      {
        primaryKeys: { key: "list:activity:recent" },
        changes: { value: listJson },
      },
    );
    expect(JSON.parse(preview)).toMatchObject({
      source: "list:activity:recent",
      value: { input: listJson },
      ttl: "keep existing TTL",
    });

    await expect(
      driver.updateRows({
        database: "db0",
        schema: "db0",
        table: "list",
        updates: [
          {
            primaryKeys: { key: "list:activity:recent" },
            changes: { value: listJson },
          },
        ],
      }),
    ).resolves.toEqual({ affectedRows: 1 });

    expect(client.sendCommand).toHaveBeenCalledWith(
      expect.arrayContaining([
        "EVAL",
        expect.stringContaining("local ttl = redis.call('PTTL', source)"),
        "3",
        "list:activity:recent",
        "list:activity:recent",
        "list",
      ]),
    );
    expect(client.del).not.toHaveBeenCalled();
  });

  it("re-applies hash field TTLs to the staged replacement before renaming it", async () => {
    const driver = new RedisDriver({
      id: "redis-hash-field-ttl-test",
      name: "Redis Hash Field TTL Test",
      type: "redis",
      host: "localhost",
    });
    const client = {
      type: vi.fn().mockResolvedValue("hash"),
      hLen: vi.fn().mockResolvedValue(2),
      sendCommand: vi.fn().mockResolvedValue(1),
    };
    (
      driver as unknown as {
        client: typeof client | null;
        connected: boolean;
      }
    ).client = withRedisReplyMapping(client);
    (driver as unknown as { connected: boolean }).connected = true;

    await expect(
      driver.updateRows({
        database: "db0",
        schema: "db0",
        table: "session",
        updates: [
          {
            primaryKeys: { key: "session:1" },
            changes: { value: '{"token":"abc","user":"bob"}' },
          },
        ],
      }),
    ).resolves.toEqual({ affectedRows: 1 });

    const [args] = client.sendCommand.mock.calls[0] as [string[]];
    expect(args.slice(0, 1)).toEqual(["EVAL"]);
    const script = args[1];
    const probe = script.indexOf(
      "pcall(redis.acl_check_cmd, 'HPEXPIRETIME', source, 'FIELDS', '1', 'field')",
    );
    const read = script.indexOf(
      "redis.call('HPEXPIRETIME', source, 'FIELDS', 1, field)[1]",
    );
    const reapply = script.indexOf(
      "call('HPEXPIREAT', staging, entry[2], 'FIELDS', '1', entry[1])",
    );
    const permissionCheck = script.indexOf(
      "buildReplacement(requirePermission)",
    );
    const rename = script.indexOf("redis.call('RENAME', staging, target)");
    expect(probe).toBeGreaterThan(-1);
    expect(read).toBeGreaterThan(probe);
    expect(script).toContain(
      "RAPIDB_NOT_APPLIED Redis hash edits require HPEXPIRETIME permission to preserve field TTLs.",
    );
    const fieldTtlFallback = script.indexOf(
      "elseif pcall(redis.acl_check_cmd, 'HPEXPIRE', source, '1', 'FIELDS', '1', 'field') then",
    );
    expect(fieldTtlFallback).toBeGreaterThan(read);
    expect(script.slice(fieldTtlFallback)).toMatch(
      /^[^\n]*\n\s*return redis\.error_reply\('RAPIDB_NOT_APPLIED Redis hash edits cannot preserve field TTLs because HPEXPIRETIME is unavailable\.'\)/,
    );
    expect(reapply).toBeGreaterThan(read);
    expect(permissionCheck).toBeGreaterThan(reapply);
    expect(rename).toBeGreaterThan(permissionCheck);
    expect(JSON.parse(args[9])).toEqual({ token: "abc", user: "bob" });
  });

  it("applies ttl-only updates without rewriting values", async () => {
    const driver = new RedisDriver({
      id: "redis-ttl-update-test",
      name: "Redis TTL Update Test",
      type: "redis",
      host: "localhost",
    });
    const client = {
      exists: vi.fn().mockResolvedValue(1),
      expire: vi.fn().mockResolvedValue(1),
      persist: vi.fn().mockResolvedValue(1),
      type: vi.fn().mockResolvedValue("string"),
      set: vi.fn().mockResolvedValue("OK"),
      del: vi.fn().mockResolvedValue(1),
      rPush: vi.fn().mockResolvedValue(1),
      sendCommand: vi.fn().mockResolvedValue(1),
    };

    (
      driver as unknown as {
        client: typeof client | null;
        connected: boolean;
      }
    ).client = withRedisReplyMapping(client);
    (driver as unknown as { connected: boolean }).connected = true;

    await expect(
      driver.updateRows({
        database: "db0",
        schema: "db0",
        table: "users",
        updates: [
          {
            primaryKeys: { key: "users:1" },
            changes: { ttl: "90" },
          },
        ],
      }),
    ).resolves.toEqual({ affectedRows: 1 });

    expect(client.sendCommand).toHaveBeenCalledWith(
      expect.arrayContaining(["users:1", "users:1", "string", "", "0"]),
    );
    expect(client.type).toHaveBeenCalledWith("users:1");
    expect(client.expire).not.toHaveBeenCalled();
    expect(client.set).not.toHaveBeenCalled();
  });

  it("enables conflict detection for an empty original string", async () => {
    const driver = new RedisDriver({
      id: "redis-empty-string-conflict-test",
      name: "Redis Empty String Conflict Test",
      type: "redis",
      host: "localhost",
    });
    const client = {
      type: vi.fn().mockResolvedValue("string"),
      sendCommand: vi.fn().mockResolvedValue(1),
    };
    (
      driver as unknown as {
        client: typeof client | null;
        connected: boolean;
      }
    ).client = withRedisReplyMapping(client);
    (driver as unknown as { connected: boolean }).connected = true;

    await driver.updateRows({
      database: "db0",
      schema: "db0",
      table: "users",
      updates: [
        {
          primaryKeys: { key: "users:1" },
          changes: { value: "next" },
          originalValues: { value: "" },
        },
      ],
    });

    const command = client.sendCommand.mock.calls[0]?.[0];
    expect(command?.at(-1)).toBe("1");
  });

  it("passes the original list value to atomic conflict detection", async () => {
    const driver = new RedisDriver({
      id: "redis-list-conflict-test",
      name: "Redis List Conflict Test",
      type: "redis",
      host: "localhost",
    });
    const client = {
      type: vi.fn().mockResolvedValue("list"),
      sendCommand: vi.fn().mockResolvedValue(1),
    };
    (
      driver as unknown as {
        client: typeof client | null;
        connected: boolean;
      }
    ).client = withRedisReplyMapping(client);
    (driver as unknown as { connected: boolean }).connected = true;

    await driver.updateRows({
      database: "db0",
      schema: "db0",
      table: "list",
      updates: [
        {
          primaryKeys: { key: "list:recent" },
          changes: { value: '["A","C"]' },
          originalValues: { value: '["A","B"]' },
        },
      ],
    });

    const command = client.sendCommand.mock.calls[0]?.[0];
    expect(command).toEqual(
      expect.arrayContaining(["list", '["A","B"]', "1", '["A","C"]']),
    );
    expect(command?.at(-1)).toBe("1");
    expect(command?.[1]).toContain("redis.call('LLEN', source)");
    expect(command?.[1]).toContain("redis.call('ZSCORE', source");
  });

  it("renames keys when key field is edited", async () => {
    const driver = new RedisDriver({
      id: "redis-key-rename-test",
      name: "Redis Key Rename Test",
      type: "redis",
      host: "localhost",
    });
    const client = {
      exists: vi.fn(async (key: string) => (key === "users:1" ? 1 : 0)),
      rename: vi.fn().mockResolvedValue("OK"),
      type: vi.fn().mockResolvedValue("string"),
      set: vi.fn().mockResolvedValue("OK"),
      sendCommand: vi.fn().mockResolvedValue(1),
    };

    (
      driver as unknown as {
        client: typeof client | null;
        connected: boolean;
      }
    ).client = withRedisReplyMapping(client);
    (driver as unknown as { connected: boolean }).connected = true;

    await expect(
      driver.updateRows({
        database: "db0",
        schema: "db0",
        table: "users",
        updates: [
          {
            primaryKeys: { key: "users:1" },
            changes: { key: "users:1:renamed" },
          },
        ],
      }),
    ).resolves.toEqual({ affectedRows: 1 });

    expect(client.sendCommand).toHaveBeenCalledWith(
      expect.arrayContaining(["users:1", "users:1:renamed", "string", "", "0"]),
    );
    expect(client.rename).not.toHaveBeenCalled();
    expect(client.type).toHaveBeenCalledWith("users:1");
    expect(client.set).not.toHaveBeenCalled();
  });

  it("builds RENAME preview when key field is edited", () => {
    const driver = new RedisDriver({
      id: "redis-key-rename-preview-test",
      name: "Redis Key Rename Preview Test",
      type: "redis",
      host: "localhost",
    });

    const preview = driver.buildMutationPreviewStatement(
      "update",
      "db0",
      "db0",
      "users",
      {
        primaryKeys: { key: "users:1" },
        changes: { key: "users:1:renamed" },
      },
    );
    expect(JSON.parse(preview)).toMatchObject({
      source: "users:1",
      target: "users:1:renamed",
      preview: expect.stringContaining("not an executable command"),
      conditions: expect.stringContaining("target must not exist"),
    });
  });

  it("applies ttl during inserts", async () => {
    const driver = new RedisDriver({
      id: "redis-ttl-insert-test",
      name: "Redis TTL Insert Test",
      type: "redis",
      host: "localhost",
    });
    const client = {
      set: vi.fn().mockResolvedValue("OK"),
      persist: vi.fn().mockResolvedValue(1),
    };

    (
      driver as unknown as {
        client: typeof client | null;
        connected: boolean;
      }
    ).client = withRedisReplyMapping(client);
    (driver as unknown as { connected: boolean }).connected = true;

    await expect(
      driver.insertRow({
        database: "db0",
        schema: "db0",
        table: "users",
        values: {
          key: "users:1",
          value: "Alice",
          ttl: "60",
        },
      }),
    ).resolves.toEqual({ affectedRows: 1 });

    expect(client.set).toHaveBeenCalledWith("users:1", "Alice", {
      NX: true,
      EX: 60,
    });

    await expect(
      driver.insertRow({
        database: "db0",
        schema: "db0",
        table: "users",
        values: {
          key: "users:2",
          value: "Bob",
          ttl: "-1",
        },
      }),
    ).resolves.toEqual({ affectedRows: 1 });

    expect(client.set).toHaveBeenCalledWith("users:2", "Bob", {
      NX: true,
    });
    expect(client.persist).not.toHaveBeenCalled();
  });

  it("caps key scans when discovering Redis objects", async () => {
    const driver = new RedisDriver({
      id: "redis-object-cap-test",
      name: "Redis Object Cap Test",
      type: "redis",
      host: "localhost",
    });

    const batchSize = 250;
    let iteration = 0;
    const client = {
      scan: vi.fn().mockImplementation(async () => {
        const start = iteration++ * batchSize;
        return {
          cursor: String(iteration),
          keys: Array.from(
            { length: batchSize },
            (_, index) => `ns:${start + index}`,
          ),
        };
      }),
    };

    (
      driver as unknown as {
        client: typeof client | null;
        connected: boolean;
      }
    ).client = withRedisReplyMapping(client);
    (driver as unknown as { connected: boolean }).connected = true;

    const warn = vi.spyOn(logger, "warn").mockImplementation(() => undefined);
    try {
      const objects = await driver.listObjects();
      expect(objects.truncated).toBe(true);
      expect(objects.map((object) => object.name)).toEqual([
        REDIS_ALL_KEYS_TABLE,
        "ns",
      ]);
      expect(warn).toHaveBeenCalledWith(
        expect.stringContaining(
          `stopped after ${REDIS_READ_BUDGET.maxScanKeys} keys; some prefixes may be missing`,
        ),
      );
    } finally {
      warn.mockRestore();
    }

    // One extra SCAN step confirms more keys exist after reaching the cap exactly.
    const expectedCalls =
      Math.ceil(REDIS_READ_BUDGET.maxScanKeys / batchSize) + 1;
    expect(client.scan).toHaveBeenCalledTimes(expectedCalls);
  });

  it("does not warn when the keyspace holds exactly the discovery cap", async () => {
    const driver = new RedisDriver({
      id: "redis-object-exact-cap-test",
      name: "Redis Object Exact Cap Test",
      type: "redis",
      host: "localhost",
    });

    const batchSize = 250;
    const batches = REDIS_READ_BUDGET.maxScanKeys / batchSize;
    let iteration = 0;
    const client = {
      scan: vi.fn().mockImplementation(async () => {
        const start = iteration++ * batchSize;
        if (iteration > batches) return { cursor: "0", keys: [] };
        return {
          cursor: String(iteration),
          keys: Array.from(
            { length: batchSize },
            (_, index) => `ns:${start + index}`,
          ),
        };
      }),
    };

    (
      driver as unknown as {
        client: typeof client | null;
        connected: boolean;
      }
    ).client = withRedisReplyMapping(client);
    (driver as unknown as { connected: boolean }).connected = true;

    const warn = vi.spyOn(logger, "warn").mockImplementation(() => undefined);
    try {
      const objects = await driver.listObjects();
      expect(objects.truncated).toBeUndefined();
      expect(warn).not.toHaveBeenCalled();
    } finally {
      warn.mockRestore();
    }
    expect(client.scan).toHaveBeenCalledTimes(batches + 1);
  });

  it("does not warn when Redis object discovery scans the whole keyspace", async () => {
    const driver = new RedisDriver({
      id: "redis-object-complete-test",
      name: "Redis Object Complete Test",
      type: "redis",
      host: "localhost",
    });
    const client = {
      scan: vi.fn().mockResolvedValue({ cursor: "0", keys: ["ns:1", "other"] }),
    };
    (
      driver as unknown as {
        client: typeof client | null;
        connected: boolean;
      }
    ).client = withRedisReplyMapping(client);
    (driver as unknown as { connected: boolean }).connected = true;

    const warn = vi.spyOn(logger, "warn").mockImplementation(() => undefined);
    try {
      const objects = await driver.listObjects();
      expect(objects).toHaveLength(3);
      expect(objects.truncated).toBeUndefined();
      expect(warn).not.toHaveBeenCalled();
    } finally {
      warn.mockRestore();
    }
  });

  it("reports Redis update identities when a later row does not match", async () => {
    const driver = new RedisDriver({
      id: "redis-partial-update-test",
      name: "Redis Partial Update Test",
      type: "redis",
      host: "localhost",
    });
    const client = {
      type: vi.fn().mockResolvedValue("string"),
      sendCommand: vi.fn().mockResolvedValueOnce(1).mockResolvedValueOnce(0),
    };
    (driver as unknown as { client: typeof client | null }).client =
      withRedisReplyMapping(client);
    (driver as unknown as { connected: boolean }).connected = true;
    (
      driver as unknown as { getDatabaseClient: () => Promise<typeof client> }
    ).getDatabaseClient = async () => client;

    await expect(
      driver.updateRows({
        database: "db0",
        schema: "db0",
        table: "users",
        updates: [
          { primaryKeys: { key: "users:1" }, changes: { value: "One" } },
          { primaryKeys: { key: "users:2" }, changes: { value: "Two" } },
        ],
      }),
    ).resolves.toEqual({
      affectedRows: 1,
      updateRowOutcomes: [
        { rowIndex: 0, status: "applied" },
        { rowIndex: 1, status: "not_applied" },
      ],
    });
  });

  it("retains confirmed Redis updates when a later row errors", async () => {
    const driver = new RedisDriver({
      id: "redis-partial-error-test",
      name: "Redis Partial Error Test",
      type: "redis",
      host: "localhost",
    });
    const client = {
      type: vi.fn().mockResolvedValue("string"),
      sendCommand: vi
        .fn()
        .mockResolvedValueOnce(1)
        .mockRejectedValueOnce(new Error("Connection lost")),
    };
    (driver as unknown as { client: typeof client | null }).client =
      withRedisReplyMapping(client);
    (driver as unknown as { connected: boolean }).connected = true;
    (
      driver as unknown as { getDatabaseClient: () => Promise<typeof client> }
    ).getDatabaseClient = async () => client;

    await expect(
      driver.updateRows({
        database: "db0",
        schema: "db0",
        table: "users",
        updates: [
          { primaryKeys: { key: "users:1" }, changes: { value: "One" } },
          { primaryKeys: { key: "users:2" }, changes: { value: "Two" } },
          { primaryKeys: { key: "users:3" }, changes: { value: "Three" } },
        ],
      }),
    ).rejects.toMatchObject({
      result: {
        affectedRows: 1,
        updateRowOutcomes: [
          { rowIndex: 0, status: "applied" },
          { rowIndex: 1, status: "unknown" },
          { rowIndex: 2, status: "not_applied" },
        ],
      },
    });
  });
});
