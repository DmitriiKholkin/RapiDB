import * as tls from "node:tls";

// RESP2 command acknowledgments only; node-redis performs the real handshake.
export async function startRedisTlsFixture(options: tls.TlsOptions) {
  const sockets = new Set<tls.TLSSocket>();
  const servernames: Array<string | false> = [];
  let commands = 0;
  const server = tls.createServer(options, (socket) => {
    servernames.push(
      (socket as tls.TLSSocket & { servername: string | false }).servername,
    );
    let pending = Buffer.alloc(0);
    socket.on("data", (chunk: Buffer) => {
      pending = Buffer.concat([pending, chunk]);
      while (pending.length) {
        const firstLine = pending.indexOf("\r\n");
        if (firstLine < 0) return;
        const count = Number(pending.subarray(1, firstLine).toString());
        let offset = firstLine + 2;
        for (let arg = 0; arg < count; arg++) {
          const end = pending.indexOf("\r\n", offset);
          if (end < 0) return;
          const length = Number(pending.subarray(offset + 1, end).toString());
          offset = end + 2 + length + 2;
          if (offset > pending.length) return;
        }
        pending = pending.subarray(offset);
        commands += 1;
        socket.write("+OK\r\n");
      }
    });
  });
  server.on("connection", (socket) => {
    sockets.add(socket as tls.TLSSocket);
    socket.on("error", () => undefined);
    socket.on("close", () => sockets.delete(socket as tls.TLSSocket));
  });
  server.on("tlsClientError", () => undefined);
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("No TLS port");
  return {
    port: address.port,
    servernames,
    get commands() {
      return commands;
    },
    async close() {
      for (const socket of sockets) socket.destroy();
      await new Promise<void>((resolve, reject) =>
        server.close((error) => (error ? reject(error) : resolve())),
      );
    },
  };
}
