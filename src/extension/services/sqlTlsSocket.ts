import { Socket, type SocketConnectOpts } from "node:net";
import { addAbortSignal } from "node:stream";

// pg and mysql2 derive TLS identity from their connection host. Keep that host
// separate from the physical endpoint (in particular, an SSH loopback forward).
export class SqlTlsSocket extends Socket {
  constructor(
    private readonly endpointHost: string,
    private readonly endpointPort: number,
    private readonly tlsIdentity: string,
  ) {
    super();
    this.once("connect", () => {
      // Node TLS uses socket._host when there is no DNS SNI (IP identities).
      // mysql2 does not expose a TLS host/checkServerIdentity override.
      Reflect.set(this, "_host", this.tlsIdentity);
    });
  }

  override connect(
    options: SocketConnectOpts & { signal?: AbortSignal },
    listener?: () => void,
  ): this;
  override connect(port: number, host: string, listener?: () => void): this;
  override connect(port: number, listener?: () => void): this;
  override connect(path: string, listener?: () => void): this;
  override connect(...args: unknown[]): this {
    const listener = args.find((arg) => typeof arg === "function") as
      | (() => void)
      | undefined;
    const options: Partial<SocketConnectOpts> & { signal?: AbortSignal } =
      typeof args[0] === "object" && args[0] !== null
        ? { ...(args[0] as SocketConnectOpts & { signal?: AbortSignal }) }
        : {};
    if (options.signal) {
      addAbortSignal(options.signal, this);
      if (options.signal.aborted) return this;
    }
    // This is an endpoint-bound TCP socket, including for positional callers.
    // Retain lookup/family/local binding/signal and all other connect options;
    // only the caller's destination is replaced by the physical endpoint.
    return super.connect(
      {
        ...options,
        path: undefined,
        host: this.endpointHost,
        port: this.endpointPort,
      },
      listener,
    );
  }
}
