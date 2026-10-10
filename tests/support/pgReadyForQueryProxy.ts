import net from "node:net";

/** A real PostgreSQL TCP relay that can withhold one transaction-state reply. */
export class PgReadyForQueryProxy {
  private readonly sockets = new Set<net.Socket>();
  private pauseStatus?: "T" | "E";
  private resume?: () => void;
  held = false;
  private readonly server: net.Server;

  constructor(host: string, port: number) {
    this.server = net.createServer((downstream) => {
      const upstream = net.createConnection({ host, port });
      this.sockets.add(downstream);
      this.sockets.add(upstream);
      downstream.on("close", () => {
        this.sockets.delete(downstream);
        upstream.destroy();
      });
      upstream.on("close", () => {
        this.sockets.delete(upstream);
        downstream.destroy();
      });
      downstream.on("error", () => upstream.destroy());
      upstream.on("error", () => downstream.destroy());
      downstream.pipe(upstream);
      let pending = Buffer.alloc(0);
      upstream.on("data", (chunk: Buffer) => {
        pending = Buffer.concat([pending, chunk]);
        while (pending.length >= 5) {
          const length = pending.readInt32BE(1) + 1;
          if (pending.length < length) break;
          const frame = pending.subarray(0, length);
          pending = pending.subarray(length);
          if (
            frame[0] === 90 && // ReadyForQuery ('Z')
            this.pauseStatus &&
            frame.toString("ascii", 5) === this.pauseStatus
          ) {
            this.pauseStatus = undefined;
            this.held = true;
            this.resume = () => {
              if (!downstream.destroyed) downstream.write(frame);
            };
          } else {
            downstream.write(frame);
          }
        }
      });
    });
  }

  async listen(): Promise<number> {
    await new Promise<void>((resolve) =>
      this.server.listen(0, "127.0.0.1", resolve),
    );
    return (this.server.address() as net.AddressInfo).port;
  }

  pauseNextTransaction(status: "T" | "E"): void {
    this.pauseStatus = status;
    this.held = false;
  }

  releaseHeld(): void {
    this.resume?.();
    this.resume = undefined;
  }

  async close(): Promise<void> {
    for (const socket of this.sockets) socket.destroy();
    await new Promise<void>((resolve) => this.server.close(() => resolve()));
  }
}
