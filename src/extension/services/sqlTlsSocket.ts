import { Socket, type SocketConnectOpts } from "node:net";
import { addAbortSignal } from "node:stream";

export class SqlTlsSocket extends Socket {
  constructor(
    private readonly endpointHost: string,
    private readonly endpointPort: number,
    private readonly tlsIdentity: string,
  ) {
    super();
    this.once("connect", () => {
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
