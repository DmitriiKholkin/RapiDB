import { EventEmitter, once } from "node:events";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
const { Connection } = require("tedious") as {
  Connection: {
    prototype: {
      createTokenStreamParser(message: Buffer[], handler: object): EventEmitter;
    };
  };
};
const { RequestTokenHandler } = require("tedious/lib/token/handler") as {
  RequestTokenHandler: new (connection: object, native: object) => object;
};

/** Adds the actual tedious parser/handler boundary to public-request mocks.
 * Only node-mssql's rowsaffected forwarding is simulated; native done events
 * retain their real SDK signature, without invented CurCmd arguments. */
export function mssqlCompletionTokenHarness(
  connection: object,
  request: EventEmitter,
) {
  Object.assign(connection, {
    config: { options: { tdsVersion: "7_4" } },
    debug: { token: () => {} },
    createTokenStreamParser: Connection.prototype.createTokenStreamParser,
  });
  Object.assign(request, { _setCurrentRequest: () => request });
  const native = Object.assign(new EventEmitter(), { rowCount: 0, rst: [] });
  const forward = (count?: number) => {
    if (count !== undefined) request.emit("rowsaffected", count);
  };
  native.on("done", forward);
  native.on("doneInProc", forward);
  const handler = new RequestTokenHandler(connection, native);
  let started = false;
  return {
    native,
    async complete(curCmd: number, count?: number) {
      if (!started) {
        Reflect.get(request, "_setCurrentRequest").call(request, native);
        started = true;
      }
      const bytes = Buffer.alloc(13);
      bytes[0] = 0xfd;
      bytes.writeUInt16LE(1 | (count === undefined ? 0 : 16), 1);
      bytes.writeUInt16LE(curCmd, 3);
      bytes.writeBigUInt64LE(BigInt(count ?? 0), 5);
      const parser = Reflect.get(connection, "createTokenStreamParser").call(
        connection,
        [bytes],
        handler,
      ) as EventEmitter;
      await once(parser, "end");
    },
  };
}
