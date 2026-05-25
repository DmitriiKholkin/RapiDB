import { createHash } from "node:crypto";
import * as http from "node:http";
import * as https from "node:https";
import * as net from "node:net";
import { Duplex, PassThrough } from "node:stream";
import * as tls from "node:tls";
import { Agent as AgentBase, type AgentConnectOpts } from "agent-base";
import type { ConnectionSshHostVerificationMode } from "../../shared/connectionConfig";

export interface ConnectionSshSettings {
  host: string;
  port: number;
  username: string;
  hostVerificationMode: ConnectionSshHostVerificationMode;
  fingerprintSha256?: string;
  auth:
    | {
        kind: "password";
        password: string;
      }
    | {
        kind: "privateKey";
        privateKey: string;
        passphrase?: string;
      };
}

export type SshRuntimeRequest =
  | {
      kind: "tcpForward";
      remoteHost: string;
      remotePort: number;
    }
  | {
      kind: "httpAgent";
    };

export type SshRuntimeTransport =
  | {
      kind: "tcpForward";
      localHost: "127.0.0.1";
      localPort: number;
      remoteHost: string;
      remotePort: number;
    }
  | {
      kind: "httpAgent";
      httpAgent: http.Agent;
      httpsAgent: https.Agent;
    };

export interface SshRuntime {
  transport: SshRuntimeTransport;
  verifiedFingerprintSha256: string;
  dispose(): Promise<void>;
}

type SshHostVerifier = (hostKey: Buffer | string) => boolean;

type SshClientConnectOptions = {
  host: string;
  port: number;
  username: string;
  hostVerifier: SshHostVerifier;
  password?: string;
  privateKey?: string;
  passphrase?: string;
};

type ForwardOutCallback = (error: Error | undefined, stream?: Duplex) => void;

interface SshClientLike {
  connect(options: SshClientConnectOptions): void;
  end(): void;
  forwardOut(
    srcIP: string,
    srcPort: number,
    dstIP: string,
    dstPort: number,
    callback: ForwardOutCallback,
  ): void;
  on(event: "error", listener: (error: Error) => void): this;
  on(event: "close", listener: () => void): this;
  once(event: "ready", listener: () => void): this;
  once(event: "error", listener: (error: Error) => void): this;
  once(event: "close", listener: () => void): this;
  removeListener(event: "ready", listener: () => void): this;
  removeListener(event: "error", listener: (error: Error) => void): this;
  removeListener(event: "close", listener: () => void): this;
}

interface Ssh2ModuleLike {
  Client: new () => SshClientLike;
}

export interface SshRuntimeDependencies {
  loadSsh2?: () => Promise<Ssh2ModuleLike>;
}

const SSH2_MODULE_NAME = "ssh2";
type TlsClientRequestArgs = http.ClientRequestArgs &
  Partial<tls.ConnectionOptions> & {
    servername?: string;
  };

function defaultLoadSsh2(): Promise<Ssh2ModuleLike> {
  return import(SSH2_MODULE_NAME) as Promise<Ssh2ModuleLike>;
}

function normalizeFingerprint(value: string): string {
  return value.trim();
}

export function buildSshFingerprintSha256(hostKey: Buffer): string {
  return `SHA256:${createHash("sha256").update(hostKey).digest("base64")}`;
}

function resolvePresentedFingerprint(hostKey: Buffer | string): string {
  if (Buffer.isBuffer(hostKey)) {
    return buildSshFingerprintSha256(hostKey);
  }

  const normalized = normalizeFingerprint(hostKey);
  return normalized.startsWith("SHA256:") ? normalized : `SHA256:${normalized}`;
}

function createHostVerifier(ssh: ConnectionSshSettings): {
  hostVerifier: SshHostVerifier;
  getVerifiedFingerprintSha256(): string | undefined;
} {
  const expectedFingerprint = ssh.fingerprintSha256
    ? normalizeFingerprint(ssh.fingerprintSha256)
    : undefined;
  let verifiedFingerprintSha256 = expectedFingerprint;

  return {
    hostVerifier: (hostKey) => {
      const presentedFingerprint = resolvePresentedFingerprint(hostKey);

      if (expectedFingerprint) {
        const matches = presentedFingerprint === expectedFingerprint;
        if (matches) {
          verifiedFingerprintSha256 = expectedFingerprint;
        }
        return matches;
      }

      if (ssh.hostVerificationMode !== "trustOnFirstUse") {
        return false;
      }

      if (!verifiedFingerprintSha256) {
        verifiedFingerprintSha256 = presentedFingerprint;
        return true;
      }

      return verifiedFingerprintSha256 === presentedFingerprint;
    },
    getVerifiedFingerprintSha256: () => verifiedFingerprintSha256,
  };
}

function closeServer(server: net.Server): Promise<void> {
  if (!server.listening) {
    return Promise.resolve();
  }

  return new Promise((resolve) => {
    server.close(() => resolve());
  });
}

function waitForServerListening(server: net.Server): Promise<net.AddressInfo> {
  return new Promise((resolve, reject) => {
    const onError = (error: Error) => {
      server.removeListener("listening", onListening);
      reject(error);
    };
    const onListening = () => {
      server.removeListener("error", onError);
      const address = server.address();
      if (!address || typeof address === "string") {
        reject(new Error("[RapiDB] SSH local forward address is unavailable"));
        return;
      }

      resolve(address);
    };

    server.once("error", onError);
    server.once("listening", onListening);
    server.listen({ host: "127.0.0.1", port: 0, exclusive: true });
  });
}

function createForwardOutPromise(
  client: SshClientLike,
  remoteHost: string,
  remotePort: number,
): Promise<Duplex> {
  return new Promise((resolve, reject) => {
    client.forwardOut(
      "127.0.0.1",
      0,
      remoteHost,
      remotePort,
      (error, stream) => {
        if (error) {
          reject(error);
          return;
        }

        if (!stream) {
          reject(
            new Error("[RapiDB] SSH forward established without a stream"),
          );
          return;
        }

        resolve(stream);
      },
    );
  });
}

function resolveRequestHost(
  options: http.ClientRequestArgs,
): string | undefined {
  if (typeof options.hostname === "string" && options.hostname.length > 0) {
    return options.hostname;
  }

  if (typeof options.host !== "string" || options.host.length === 0) {
    return undefined;
  }

  try {
    return new URL(`http://${options.host}`).hostname;
  } catch {
    return options.host;
  }
}

function resolveRequestPort(
  options: http.ClientRequestArgs,
  defaultPort: number,
): number {
  if (typeof options.port === "number" && Number.isInteger(options.port)) {
    return options.port;
  }

  if (typeof options.port === "string") {
    const parsed = Number.parseInt(options.port, 10);
    if (Number.isInteger(parsed) && parsed > 0) {
      return parsed;
    }
  }

  return defaultPort;
}

function resolveSecureServername(options: TlsClientRequestArgs): string {
  if (typeof options.servername === "string" && options.servername.length > 0) {
    return options.servername;
  }

  const host = resolveRequestHost(options);
  if (host) {
    return host;
  }

  throw new Error("[RapiDB] SSH HTTPS agent could not resolve request host");
}

async function createAgentSocket(
  client: SshClientLike,
  options: http.ClientRequestArgs,
): Promise<Duplex> {
  const remoteHost = resolveRequestHost(options);
  if (!remoteHost) {
    throw new Error("[RapiDB] SSH agent could not resolve request host");
  }

  return createForwardOutPromise(
    client,
    remoteHost,
    resolveRequestPort(options, 80),
  );
}

async function createSecureAgentSocket(
  client: SshClientLike,
  options: TlsClientRequestArgs,
): Promise<tls.TLSSocket> {
  const remoteHost = resolveRequestHost(options);
  if (!remoteHost) {
    throw new Error("[RapiDB] SSH agent could not resolve request host");
  }

  const tunneledSocket = await createForwardOutPromise(
    client,
    remoteHost,
    resolveRequestPort(options, 443),
  );

  return new Promise((resolve, reject) => {
    const tlsSocket = tls.connect({
      ALPNProtocols: options.ALPNProtocols,
      ca: options.ca,
      cert: options.cert,
      checkServerIdentity: options.checkServerIdentity,
      ciphers: options.ciphers,
      clientCertEngine: options.clientCertEngine,
      crl: options.crl,
      dhparam: options.dhparam,
      ecdhCurve: options.ecdhCurve,
      honorCipherOrder: options.honorCipherOrder,
      key: options.key,
      maxVersion: options.maxVersion,
      minVersion: options.minVersion,
      passphrase: options.passphrase,
      pfx: options.pfx,
      rejectUnauthorized: options.rejectUnauthorized,
      secureContext: options.secureContext,
      secureOptions: options.secureOptions,
      servername: resolveSecureServername(options),
      session: options.session,
      sigalgs: options.sigalgs,
      socket: tunneledSocket,
    });

    const onError = (error: Error) => {
      tlsSocket.removeListener("secureConnect", onSecureConnect);
      reject(error);
    };
    const onSecureConnect = () => {
      tlsSocket.removeListener("error", onError);
      resolve(tlsSocket);
    };

    tlsSocket.once("error", onError);
    tlsSocket.once("secureConnect", onSecureConnect);
  });
}

class SshForwardAgent extends AgentBase {
  constructor(
    private readonly client: SshClientLike,
    private readonly secureEndpoint: boolean,
  ) {
    super({ keepAlive: true });
  }

  override connect(
    _req: http.ClientRequest,
    options: AgentConnectOpts,
  ): Promise<Duplex> {
    return this.secureEndpoint
      ? createSecureAgentSocket(this.client, options as TlsClientRequestArgs)
      : createAgentSocket(this.client, options);
  }
}

async function createVerifiedClient(
  ssh: ConnectionSshSettings,
  dependencies: SshRuntimeDependencies,
): Promise<{
  client: SshClientLike;
  verifiedFingerprintSha256: string;
}> {
  const { Client } = await (dependencies.loadSsh2 ?? defaultLoadSsh2)();
  const client = new Client();
  const hostVerifierState = createHostVerifier(ssh);

  try {
    await new Promise<void>((resolve, reject) => {
      const onReady = () => {
        client.removeListener("error", onError);
        client.removeListener("close", onClose);
        resolve();
      };
      const onError = (error: Error) => {
        client.removeListener("ready", onReady);
        client.removeListener("close", onClose);
        reject(error);
      };
      const onClose = () => {
        client.removeListener("ready", onReady);
        client.removeListener("error", onError);
        reject(
          new Error("[RapiDB] SSH connection closed before it became ready"),
        );
      };

      client.once("ready", onReady);
      client.once("error", onError);
      client.once("close", onClose);
      client.connect({
        host: ssh.host,
        port: ssh.port,
        username: ssh.username,
        hostVerifier: hostVerifierState.hostVerifier,
        password: ssh.auth.kind === "password" ? ssh.auth.password : undefined,
        privateKey:
          ssh.auth.kind === "privateKey" ? ssh.auth.privateKey : undefined,
        passphrase:
          ssh.auth.kind === "privateKey" ? ssh.auth.passphrase : undefined,
      });
    });
  } catch (error) {
    try {
      client.end();
    } catch {}
    throw error;
  }

  const verifiedFingerprintSha256 =
    hostVerifierState.getVerifiedFingerprintSha256();
  if (!verifiedFingerprintSha256) {
    try {
      client.end();
    } catch {}
    throw new Error(
      "[RapiDB] SSH host fingerprint could not be verified during connection setup.",
    );
  }

  return {
    client,
    verifiedFingerprintSha256,
  };
}

async function createTcpForwardRuntime(
  client: SshClientLike,
  verifiedFingerprintSha256: string,
  request: Extract<SshRuntimeRequest, { kind: "tcpForward" }>,
): Promise<SshRuntime> {
  const server = net.createServer((socket) => {
    void createForwardOutPromise(client, request.remoteHost, request.remotePort)
      .then((upstream) => {
        socket.pipe(upstream);
        upstream.pipe(socket);

        const destroySocket = () => {
          socket.destroy();
        };
        const destroyUpstream = () => {
          if (typeof (upstream as PassThrough).destroy === "function") {
            upstream.destroy();
          }
        };

        socket.once("error", destroyUpstream);
        upstream.once("error", destroySocket);
      })
      .catch(() => {
        socket.destroy();
      });
  });

  try {
    const address = await waitForServerListening(server);
    let disposed = false;
    return {
      transport: {
        kind: "tcpForward",
        localHost: "127.0.0.1",
        localPort: address.port,
        remoteHost: request.remoteHost,
        remotePort: request.remotePort,
      },
      verifiedFingerprintSha256,
      dispose: async () => {
        if (disposed) {
          return;
        }
        disposed = true;
        await closeServer(server);
        try {
          client.end();
        } catch {}
      },
    };
  } catch (error) {
    await closeServer(server).catch(() => undefined);
    try {
      client.end();
    } catch {}
    throw error;
  }
}

function createHttpAgentRuntime(
  client: SshClientLike,
  verifiedFingerprintSha256: string,
): SshRuntime {
  const httpAgent = new SshForwardAgent(client, false) as unknown as http.Agent;
  const httpsAgent = new SshForwardAgent(
    client,
    true,
  ) as unknown as https.Agent;
  let disposed = false;

  return {
    transport: {
      kind: "httpAgent",
      httpAgent,
      httpsAgent,
    },
    verifiedFingerprintSha256,
    dispose: async () => {
      if (disposed) {
        return;
      }
      disposed = true;
      httpAgent.destroy();
      httpsAgent.destroy();
      try {
        client.end();
      } catch {}
    },
  };
}

export async function createSshRuntime(
  ssh: ConnectionSshSettings,
  request: SshRuntimeRequest,
  dependencies: SshRuntimeDependencies = {},
): Promise<SshRuntime> {
  const { client, verifiedFingerprintSha256 } = await createVerifiedClient(
    ssh,
    dependencies,
  );

  if (request.kind === "tcpForward") {
    return createTcpForwardRuntime(client, verifiedFingerprintSha256, request);
  }

  return createHttpAgentRuntime(client, verifiedFingerprintSha256);
}
