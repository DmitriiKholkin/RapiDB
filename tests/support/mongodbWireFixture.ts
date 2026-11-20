import { createServer, type Socket } from "node:net";
import { types } from "node:util";
import { BSON, type Document } from "mongodb";

// Only emulate the server; the actual SDK handles URI parsing, option
// inheritance, command/cursor execution, and BSON reply decoding.
export async function startMongoWireFixture(
  respond: (command: Document) => Document | Uint8Array | undefined,
) {
  const sockets = new Set<Socket>();
  const server = createServer((socket) => {
    sockets.add(socket);
    socket.on("error", () => undefined);
    socket.on("close", () => sockets.delete(socket));
    let pending = Buffer.alloc(0);
    socket.on("data", (chunk: Buffer) => {
      pending = Buffer.concat([pending, chunk]);
      while (pending.length >= 4 && pending.length >= pending.readInt32LE(0)) {
        const length = pending.readInt32LE(0);
        const message = pending.subarray(0, length);
        pending = pending.subarray(length);
        // Reply to the legacy OP_QUERY handshake with OP_REPLY, otherwise OP_MSG.
        const legacy = message.readInt32LE(12) === 2004;
        const command = legacy
          ? { ismaster: true }
          : BSON.deserialize(message.subarray(21));
        const reply =
          command.ismaster || command.hello
            ? {
                ok: 1,
                ismaster: true,
                isWritablePrimary: true,
                helloOk: true,
                minWireVersion: 0,
                maxWireVersion: 21,
              }
            : respond(command);
        if (!reply) continue; // Deliberately leave the request pending.
        const document = types.isUint8Array(reply)
          ? reply
          : BSON.serialize(reply);
        const prefix = Buffer.alloc(legacy ? 36 : 21);
        prefix.writeInt32LE(prefix.length + document.length, 0);
        prefix.writeInt32LE(message.readInt32LE(4), 8);
        prefix.writeInt32LE(legacy ? 1 : 2013, 12);
        if (legacy) prefix.writeInt32LE(1, 32);
        socket.write(Buffer.concat([prefix, document]));
      }
    });
  });
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  const address = server.address();
  if (!address || typeof address === "string")
    throw new Error("No MongoDB port");
  return {
    port: address.port,
    async close() {
      for (const socket of sockets) socket.destroy();
      await new Promise<void>((resolve, reject) =>
        server.close((error) => (error ? reject(error) : resolve())),
      );
    },
  };
}
