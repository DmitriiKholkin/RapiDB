import * as net from "node:net";
import { Duplex } from "node:stream";
import * as tls from "node:tls";

function mysqlPacket(payload: Buffer, sequence: number) {
  const header = Buffer.alloc(4);
  header.writeUIntLE(payload.length, 0, 3);
  header[3] = sequence;
  return Buffer.concat([header, payload]);
}

function mysqlHello() {
  const fields = Buffer.alloc(31);
  fields.writeUInt32LE(1, 0);
  fields.write("12345678", 4);
  const capabilities = 0x00088201 | 0x800;
  fields.writeUInt16LE(capabilities & 0xffff, 13);
  fields[15] = 45;
  fields.writeUInt16LE(2, 16);
  fields.writeUInt16LE(capabilities >>> 16, 18);
  fields[20] = 21;
  return mysqlPacket(
    Buffer.concat([
      Buffer.from("\x0a8.0.0-rapidb-tls\0"),
      fields,
      Buffer.from("123456789012\0mysql_native_password\0"),
    ]),
    0,
  );
}

// Minimal native wire negotiation, not a database server. All TLS operations
// and certificate decisions use the real client packages and Node TLS server.
export async function startSqlTlsFixture(
  engine: "mysql" | "pg",
  options: tls.TlsOptions,
) {
  const sockets = new Set<net.Socket>();
  const servernames: Array<string | false> = [];
  let authentications = 0;
  // tls.Server performs client-certificate authorization before this callback;
  // a raw server TLSSocket alone does not enforce rejectUnauthorized.
  const secureServer = tls.createServer(options, (secure) => {
    sockets.add(secure);
    secure.on("error", () => undefined);
    secure.on("close", () => sockets.delete(secure));
    servernames.push(
      (secure as tls.TLSSocket & { servername: string | false }).servername,
    );
    secure.once("data", () => {
      authentications += 1;
      if (engine === "mysql") {
        secure.write(mysqlPacket(Buffer.from([0, 0, 0, 2, 0, 0, 0]), 3));
      } else {
        // AuthenticationOk and ReadyForQuery suffice for pg Pool.connect().
        secure.write(Buffer.from("5200000008000000005a0000000549", "hex"));
        secure.on("data", (query: Buffer) => {
          if (query[0] === 0x51) {
            secure.write(
              Buffer.from("430000000d53454c4543542030005a0000000549", "hex"),
            );
          }
        });
      }
    });
  });
  secureServer.on("tlsClientError", () => undefined);
  const server = net.createServer((socket) => {
    sockets.add(socket);
    socket.on("error", () => undefined);
    socket.on("close", () => sockets.delete(socket));
    if (engine === "mysql") socket.write(mysqlHello());
    let pending = Buffer.alloc(0);
    const negotiate = (chunk: Buffer) => {
      pending = Buffer.concat([pending, chunk]);
      const length = engine === "mysql" ? 36 : 8;
      if (pending.length < length) return;
      socket.removeListener("data", negotiate);
      socket.pause();
      if (pending.length > length) socket.unshift(pending.subarray(length));
      if (engine === "pg") socket.write("S");
      // The SSLRequest and TLS ClientHello can share a TCP chunk. A generic
      // duplex makes TLS consume the socket's unshifted readable bytes too.
      const channel = new Duplex({
        read() {
          socket.resume();
        },
        write(chunk, _encoding, callback) {
          socket.write(chunk, callback);
        },
        final(callback) {
          socket.end(callback);
        },
        destroy(error, callback) {
          socket.destroy();
          callback(error);
        },
      });
      socket.on("data", (chunk) => {
        if (!channel.push(chunk)) socket.pause();
      });
      socket.on("end", () => channel.push(null));
      socket.on("close", () => channel.destroy());
      channel.on("error", () => undefined);
      // Feed the already-negotiated duplex through tls.Server's normal
      // connection listener, retaining its authorization gate and buffering.
      secureServer.emit("connection", channel);
    };
    socket.on("data", negotiate);
  });
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("No TLS port");
  return {
    port: address.port,
    servernames,
    get authentications() {
      return authentications;
    },
    async close() {
      for (const socket of sockets) socket.destroy();
      await new Promise<void>((resolve, reject) =>
        server.close((error) => (error ? reject(error) : resolve())),
      );
    },
  };
}
