import type { ConnectionConfig } from "../connectionManagerModels";
import { logErrorWithContext } from "../utils/errorHandling";
import type {
  ConnectionSshSettings,
  SshRuntime,
  SshRuntimeRequest,
} from "./sshRuntime";
import { createSshRuntime } from "./sshRuntime";

/**
 * Dependencies for the SSH service.
 */
export interface SshServiceDependencies {
  createSshRuntime?: typeof createSshRuntime;
}

/**
 * Service for managing SSH connections and runtime.
 * Handles SSH settings validation, runtime creation, and cleanup.
 */
export class SshService {
  private readonly sshRuntimeMap = new Map<string, SshRuntime>();
  private readonly createSshRuntimeFn: typeof createSshRuntime;

  constructor(dependencies: SshServiceDependencies = {}) {
    this.createSshRuntimeFn = dependencies.createSshRuntime ?? createSshRuntime;
  }

  /**
   * Builds SSH settings from a connection config.
   * Throws if required settings are missing.
   */
  buildConnectionSshSettings(
    config: ConnectionConfig,
  ): ConnectionSshSettings | undefined {
    if (!config.ssh) {
      return undefined;
    }

    const ssh = config.ssh;
    const host = ssh.host?.trim();
    const port = ssh.port;
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

  /**
   * Resolves the SSH runtime request based on connection type.
   */
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

  /**
   * Resolves the TCP remote target for a connection.
   */
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

  /**
   * Creates an SSH runtime for a connection.
   */
  async createSshRuntime(
    sshSettings: ConnectionSshSettings,
    request: SshRuntimeRequest,
  ): Promise<SshRuntime> {
    return this.createSshRuntimeFn(sshSettings, request);
  }

  /**
   * Gets the SSH runtime for a connection.
   */
  getSshRuntime(connectionId: string): SshRuntime | undefined {
    return this.sshRuntimeMap.get(connectionId);
  }

  /**
   * Stores an SSH runtime for a connection.
   */
  setSshRuntime(connectionId: string, runtime: SshRuntime): void {
    this.sshRuntimeMap.set(connectionId, runtime);
  }

  /**
   * Disposes and removes the SSH runtime for a connection.
   */
  async disposeSshRuntime(connectionId: string): Promise<void> {
    const runtime = this.sshRuntimeMap.get(connectionId);
    this.sshRuntimeMap.delete(connectionId);
    if (!runtime) {
      return;
    }

    try {
      await runtime.dispose();
    } catch (e) {
      // Cleanup is best-effort — log the failure and continue.
      logErrorWithContext("Failed to dispose SSH runtime", e);
    }
  }

  /**
   * Disposes all SSH runtimes.
   */
  async disposeAll(): Promise<void> {
    const runtimeIds = [...this.sshRuntimeMap.keys()];
    await Promise.allSettled(
      runtimeIds.map((id) => this.disposeSshRuntime(id)),
    );
  }

  /**
   * Rewrites a URL's host and port.
   */
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

  /**
   * Resolves MongoDB remote target.
   */
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
      } catch {
        // Fall through to default
      }
    }

    return {
      host: config.host?.trim() || "localhost",
      port: config.port ?? 27017,
    };
  }

  /**
   * Resolves Redis remote target.
   */
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
      } catch {
        // Fall through to default
      }
    }

    return {
      host: config.host?.trim() || "localhost",
      port: config.port ?? 6379,
    };
  }

  /**
   * Resolves Elasticsearch remote target.
   */
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
      } catch {
        // Fall through to default
      }
    }

    if (config.host) {
      return {
        host: config.host.trim(),
        port: config.port ?? 9200,
      };
    }

    return undefined;
  }

  /**
   * Resolves DynamoDB remote target.
   */
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
      } catch {
        // Fall through to default
      }
    }

    const region = config.awsRegion?.trim() || "us-east-1";
    return {
      host: `dynamodb.${region}.amazonaws.com`,
      port: 443,
    };
  }
}
