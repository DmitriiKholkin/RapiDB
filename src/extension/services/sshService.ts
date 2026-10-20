import type { ConnectionConfig } from "../connectionManagerModels";
import { logErrorWithContext } from "../utils/errorHandling";
import type {
  ConnectionSshSettings,
  SshRuntime,
  SshRuntimeRequest,
} from "./sshRuntime";
import { createSshRuntime } from "./sshRuntime";

export interface SshServiceDependencies {
  createSshRuntime?: typeof createSshRuntime;
}

export class SshService {
  private readonly sshRuntimeMap = new Map<string, SshRuntime>();
  private readonly createSshRuntimeFn: typeof createSshRuntime;

  constructor(dependencies: SshServiceDependencies = {}) {
    this.createSshRuntimeFn = dependencies.createSshRuntime ?? createSshRuntime;
  }

  buildConnectionSshSettings(
    config: ConnectionConfig,
  ): ConnectionSshSettings | undefined {
    if (!config.ssh) {
      return undefined;
    }

    const ssh = config.ssh;
    const host = ssh.host?.trim();
    const port = ssh.port ?? 22;
    const username = ssh.username?.trim();
    const fingerprintSha256 = ssh.hostFingerprintSha256?.trim();
    const hostVerificationMode =
      ssh.hostVerificationMode === "trustOnFirstUse"
        ? "trustOnFirstUse"
        : "manual";

    if (
      !host ||
      !port ||
      !username ||
      (hostVerificationMode === "manual" && !fingerprintSha256)
    ) {
      throw new Error("[RapiDB] SSH settings are incomplete.");
    }

    if (ssh.authMethod === "password") {
      if (!ssh.password) {
        throw new Error("[RapiDB] SSH password is missing.");
      }

      return {
        host,
        port,
        username,
        hostVerificationMode,
        fingerprintSha256,
        auth: {
          kind: "password",
          password: ssh.password,
        },
      };
    }

    if (!ssh.privateKey) {
      throw new Error("[RapiDB] SSH private key is missing.");
    }

    return {
      host,
      port,
      username,
      hostVerificationMode,
      fingerprintSha256,
      auth: {
        kind: "privateKey",
        privateKey: ssh.privateKey,
        passphrase: ssh.passphrase,
      },
    };
  }

  resolveSshRuntimeRequest(config: ConnectionConfig): SshRuntimeRequest {
    switch (config.type) {
      case "pg":
      case "mysql":
      case "mssql":
      case "oracle":
      case "mongodb":
      case "redis": {
        const remoteTarget = this.resolveTcpRemoteTarget(config);
        return {
          kind: "tcpForward",
          remoteHost: remoteTarget.host,
          remotePort: remoteTarget.port,
        };
      }
      case "elasticsearch": {
        if (config.cloudId) {
          return { kind: "httpAgent" };
        }

        const remoteTarget = this.resolveTcpRemoteTarget(config);
        return {
          kind: "tcpForward",
          remoteHost: remoteTarget.host,
          remotePort: remoteTarget.port,
        };
      }
      case "dynamodb": {
        const remoteTarget = this.resolveTcpRemoteTarget(config);
        return {
          kind: "tcpForward",
          remoteHost: remoteTarget.host,
          remotePort: remoteTarget.port,
        };
      }
      default:
        throw new Error(
          `[RapiDB] SSH is not supported for ${config.type} connections.`,
        );
    }
  }

  resolveTcpRemoteTarget(config: ConnectionConfig): {
    host: string;
    port: number;
  } {
    switch (config.type) {
      case "pg":
        return {
          host: config.host?.trim() || "localhost",
          port: config.port ?? 5432,
        };
      case "mysql":
        return {
          host: config.host?.trim() || "localhost",
          port: config.port ?? 3306,
        };
      case "mssql":
        return {
          host: config.host?.trim() || "localhost",
          port: config.port ?? 1433,
        };
      case "oracle":
        return {
          host: config.host?.trim() || "localhost",
          port: config.port ?? 1521,
        };
      case "mongodb":
        return this.resolveMongoRemoteTarget(config);
      case "redis":
        return this.resolveRedisRemoteTarget(config);
      case "elasticsearch": {
        const remoteTarget = this.resolveElasticsearchRemoteTarget(config);
        if (remoteTarget) {
          return remoteTarget;
        }

        throw new Error(
          "[RapiDB] Elasticsearch over SSH requires a fixed host, endpoint, or connection URI when Cloud ID is not used.",
        );
      }
      case "dynamodb":
        return this.resolveDynamoRemoteTarget(config);
      default: {
        const unsupported = config.type;
        throw new Error(
          `[RapiDB] SSH TCP forwarding is not supported for ${unsupported}.`,
        );
      }
    }
  }

  async createSshRuntime(
    sshSettings: ConnectionSshSettings,
    request: SshRuntimeRequest,
  ): Promise<SshRuntime> {
    return this.createSshRuntimeFn(sshSettings, request);
  }

  getSshRuntime(connectionId: string): SshRuntime | undefined {
    return this.sshRuntimeMap.get(connectionId);
  }

  setSshRuntime(connectionId: string, runtime: SshRuntime): void {
    this.sshRuntimeMap.set(connectionId, runtime);
  }

  async disposeSshRuntime(connectionId: string): Promise<void> {
    const runtime = this.sshRuntimeMap.get(connectionId);
    this.sshRuntimeMap.delete(connectionId);
    if (!runtime) {
      return;
    }

    try {
      await runtime.dispose();
    } catch (e) {
      logErrorWithContext("Failed to dispose SSH runtime", e);
    }
  }

  async disposeAll(): Promise<void> {
    const runtimeIds = [...this.sshRuntimeMap.keys()];
    await Promise.allSettled(
      runtimeIds.map((id) => this.disposeSshRuntime(id)),
    );
  }

  rewriteUriHostPort(
    value: string | undefined,
    host: string,
    port: number,
  ): string | undefined {
    if (!value) {
      return undefined;
    }

    const url = new URL(value);
    url.hostname = host;
    url.port = String(port);
    return url.toString();
  }

  private resolveMongoRemoteTarget(config: ConnectionConfig): {
    host: string;
    port: number;
  } {
    const url = config.connectionUri || config.uri;
    if (url) {
      try {
        const parsed = new URL(url);
        return {
          host: parsed.hostname || "localhost",
          port: parsed.port ? Number.parseInt(parsed.port, 10) : 27017,
        };
      } catch {}
    }

    return {
      host: config.host?.trim() || "localhost",
      port: config.port ?? 27017,
    };
  }

  private resolveRedisRemoteTarget(config: ConnectionConfig): {
    host: string;
    port: number;
  } {
    const url = config.connectionUri || config.uri;
    if (url) {
      try {
        const parsed = new URL(url);
        return {
          host: parsed.hostname || "localhost",
          port: parsed.port ? Number.parseInt(parsed.port, 10) : 6379,
        };
      } catch {}
    }

    return {
      host: config.host?.trim() || "localhost",
      port: config.port ?? 6379,
    };
  }

  private resolveElasticsearchRemoteTarget(config: ConnectionConfig):
    | {
        host: string;
        port: number;
      }
    | undefined {
    const url = config.connectionUri || config.endpoint;
    if (url) {
      try {
        const parsed = new URL(url);
        return {
          host: parsed.hostname || "localhost",
          port: parsed.port ? Number.parseInt(parsed.port, 10) : 9200,
        };
      } catch {}
    }

    if (config.host) {
      return {
        host: config.host.trim(),
        port: config.port ?? 9200,
      };
    }

    return undefined;
  }

  private resolveDynamoRemoteTarget(config: ConnectionConfig): {
    host: string;
    port: number;
  } {
    const endpoint = config.endpoint || config.awsEndpoint;
    if (endpoint) {
      try {
        const parsed = new URL(endpoint);
        return {
          host: parsed.hostname || "localhost",
          port: parsed.port ? Number.parseInt(parsed.port, 10) : 443,
        };
      } catch {}
    }

    const region = config.awsRegion?.trim() || "us-east-1";
    return {
      host: `dynamodb.${region}.amazonaws.com`,
      port: 443,
    };
  }
}
