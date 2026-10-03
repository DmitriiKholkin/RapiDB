import { execFileSync } from "node:child_process";
import { readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import * as tls from "node:tls";
import { BSON } from "mongodb";
import { createProjectTempDir } from "../runtime/tempDirectories";

// Fresh, test-only certificates avoid expiration-sensitive checked-in keys.
export async function createMongoTlsCertificates() {
  const directory = await createProjectTempDir("unit-node", "mongo-tls-");
  const path = (name: string) => join(directory, name);
  const openssl = (...args: string[]) =>
    execFileSync("openssl", args, { cwd: directory, stdio: "pipe" });
  openssl(
    "req",
    "-x509",
    "-newkey",
    "rsa:2048",
    "-nodes",
    "-days",
    "2",
    "-subj",
    "/CN=RapiDB test CA",
    "-keyout",
    "ca.key",
    "-out",
    "ca.pem",
  );
  for (const [name, extensions] of [
    ["server", "extendedKeyUsage=serverAuth\nsubjectAltName=DNS:db.internal"],
    [
      "ip",
      "extendedKeyUsage=serverAuth\nsubjectAltName=IP:192.0.2.12,IP:2001:db8::12",
    ],
    ["client", "extendedKeyUsage=clientAuth"],
  ]) {
    writeFileSync(path(`${name}.ext`), extensions);
    openssl(
      "req",
      "-new",
      "-newkey",
      "rsa:2048",
      "-nodes",
      "-subj",
      `/CN=${name}`,
      "-keyout",
      `${name}.key`,
      "-out",
      `${name}.csr`,
    );
    openssl(
      "x509",
      "-req",
      "-in",
      `${name}.csr`,
      "-CA",
      "ca.pem",
      "-CAkey",
      "ca.key",
      "-CAcreateserial",
      "-days",
      "2",
      "-extfile",
      `${name}.ext`,
      "-out",
      `${name}.pem`,
    );
  }
  return {
    caFilePath: path("ca.pem"),
    certFilePath: path("client.pem"),
    keyFilePath: path("client.key"),
    ca: readFileSync(path("ca.pem")),
    serverCert: readFileSync(path("server.pem")),
    serverKey: readFileSync(path("server.key")),
    ipCert: readFileSync(path("ip.pem")),
    ipKey: readFileSync(path("ip.key")),
  };
}

// Only hello and command acknowledgments, not a database. Real MongoClient
// performs TLS, certificate checks, server selection and wire negotiation.
export async function startMongoTlsFixture(options: tls.TlsOptions) {
  const sockets = new Set<tls.TLSSocket>();
  const handshakes: Array<{ servername: string | false; authorized: boolean }> =
    [];
  let commands = 0;
  const server = tls.createServer(options, (socket) => {
    handshakes.push({
      servername: (socket as tls.TLSSocket & { servername: string | false })
        .servername,
      authorized: socket.authorized,
    });
    let pending = Buffer.alloc(0);
    socket.on("data", (chunk: Buffer) => {
      pending = Buffer.concat([pending, chunk]);
      while (pending.length >= 4 && pending.length >= pending.readInt32LE(0)) {
        const length = pending.readInt32LE(0);
        const message = pending.subarray(0, length);
        pending = pending.subarray(length);
        const requestId = message.readInt32LE(4);
        const opcode = message.readInt32LE(12);
        commands += 1;
        const document = BSON.serialize({
          ok: 1,
          ismaster: true,
          isWritablePrimary: true,
          helloOk: true,
          minWireVersion: 0,
          maxWireVersion: 25,
          maxBsonObjectSize: 16777216,
          maxMessageSizeBytes: 48000000,
          maxWriteBatchSize: 100000,
        });
        const prefix = Buffer.alloc(opcode === 2004 ? 36 : 21);
        prefix.writeInt32LE(prefix.length + document.length, 0);
        prefix.writeInt32LE(commands, 4);
        prefix.writeInt32LE(requestId, 8);
        prefix.writeInt32LE(opcode === 2004 ? 1 : 2013, 12);
        if (opcode === 2004) prefix.writeInt32LE(1, 32);
        socket.write(Buffer.concat([prefix, document]));
      }
    });
  });
  server.on("connection", (socket) => {
    const tlsSocket = socket as tls.TLSSocket;
    sockets.add(tlsSocket);
    socket.on("error", () => undefined);
    socket.on("close", () => sockets.delete(tlsSocket));
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
    handshakes,
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
