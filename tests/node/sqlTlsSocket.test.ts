import type { LookupFunction } from "node:net";
import * as net from "node:net";
import { describe, expect, it, vi } from "vitest";
import { SqlTlsSocket } from "../../src/extension/services/sqlTlsSocket";

async function fixture() {
  const peers = new Set<net.Socket>();
  const server = net.createServer((socket) => {
    peers.add(socket);
    socket.on("error", () => undefined);
    socket.on("close", () => peers.delete(socket));
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("No TCP port");
  return {
    port: address.port,
    peers,
    async close() {
      for (const socket of peers) socket.destroy();
      await new Promise<void>((resolve) => server.close(() => resolve()));
    },
  };
}

describe("SqlTlsSocket endpoint-bound connect options", () => {
  it("preserves lookup, family, local binding and callbacks while replacing the endpoint", async () => {
    const f = await fixture();
    const socket = new SqlTlsSocket("physical.internal", f.port, "db.internal");
    const lookup: LookupFunction = vi.fn((host, options, callback) => {
      expect(host).toBe("physical.internal");
      expect(options.family).toBe(4);
      callback(null, "127.0.0.1", 4);
    });
    try {
      await new Promise<void>((resolve, reject) => {
        socket.once("error", reject);
        socket.connect(
          {
            host: "not-the-endpoint.invalid",
            port: 1,
            lookup,
            family: 4,
            localAddress: "127.0.0.1",
            autoSelectFamily: false,
          },
          resolve,
        );
      });
      expect(lookup).toHaveBeenCalledOnce();
      expect(socket.remotePort).toBe(f.port);
      expect(socket.localAddress).toBe("127.0.0.1");
      expect(Reflect.get(socket, "_host")).toBe("db.internal");
    } finally {
      socket.destroy();
      await f.close();
    }
  });

  it.each([
    true,
    false,
  ])("honors abort before/during DNS lookup (already aborted=%s)", async (alreadyAborted) => {
    const controller = new AbortController();
    const socket = new SqlTlsSocket("physical.internal", 5432, "db.internal");
    const lookup = vi.fn<LookupFunction>(() => undefined);
    const error = new Promise<Error>((resolve) =>
      socket.once("error", resolve),
    );
    const closed = new Promise<void>((resolve) =>
      socket.once("close", () => resolve()),
    );
    if (alreadyAborted) controller.abort();
    socket.connect({
      port: 1,
      signal: controller.signal,
      lookup,
      autoSelectFamily: false,
    });
    if (!alreadyAborted) {
      expect(lookup).toHaveBeenCalledOnce();
      controller.abort();
    }
    expect(await error).toMatchObject({
      name: "AbortError",
      code: "ABORT_ERR",
    });
    await closed;
    expect(socket.destroyed).toBe(true);
    if (alreadyAborted) expect(lookup).not.toHaveBeenCalled();
  });

  it("keeps the connect signal active after connection establishment", async () => {
    const f = await fixture();
    const controller = new AbortController();
    const socket = new SqlTlsSocket("127.0.0.1", f.port, "db.internal");
    const error = new Promise<Error>((resolve) =>
      socket.once("error", resolve),
    );
    const closed = new Promise<void>((resolve) =>
      socket.once("close", () => resolve()),
    );
    try {
      await new Promise<void>((resolve) =>
        socket.connect({ port: 1, signal: controller.signal }, resolve),
      );
      controller.abort();
      expect(await error).toMatchObject({ code: "ABORT_ERR" });
      await closed;
      expect(socket.destroyed).toBe(true);
    } finally {
      socket.destroy();
      await f.close();
    }
  });
});
