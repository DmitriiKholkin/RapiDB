import { ConnectionDriverFactory } from "../connectionDriverFactory";
import type { ConnectionConfig } from "../connectionManagerModels";
import type { IDBDriver } from "../dbDrivers/types";
import {
  DEFAULT_DRIVER_ENTITY_MANIFEST,
  type DriverCapabilities,
  type DriverEntityManifest,
  type DriverStaticMetadata,
} from "../dbDrivers/types";
import type { DriverConnectionConfig } from "../driverRuntimeConfig";
import {
  resolveDriverCapabilities,
  resolveDriverEntityManifest,
} from "../schemaCacheManager";
import type { SshRuntime } from "../services/sshRuntime";
import { logErrorWithContext } from "../utils/errorHandling";

/**
 * Service for managing database drivers.
 * Handles driver creation, caching, and configuration.
 */
export class DriverService {
  private readonly driverMap = new Map<string, IDBDriver>();
  private readonly driverStaticMetadataCache = new Map<
    string,
    DriverStaticMetadata
  >();
  private readonly driverFactory: ConnectionDriverFactory;

  constructor(
    getTimeoutSettings: () => ReturnType<
      ConnectionDriverFactory["getTimeoutSettings"]
    >,
  ) {
    this.driverFactory = new ConnectionDriverFactory(getTimeoutSettings);
  }

  /**
   * Creates a driver for a connection config.
   */
  createDriver(config: ConnectionConfig): IDBDriver {
    return this.driverFactory.createDriver(config);
  }

  /**
   * Gets a driver by connection ID.
   */
  getDriver(connectionId: string): IDBDriver | undefined {
    return this.driverMap.get(connectionId);
  }

  /**
   * Stores a driver for a connection.
   */
  setDriver(connectionId: string, driver: IDBDriver): void {
    this.driverMap.set(connectionId, driver);
  }

  /**
   * Removes a driver for a connection.
   */
  removeDriver(connectionId: string): boolean {
    return this.driverMap.delete(connectionId);
  }

  /**
   * Checks if a driver exists for a connection.
   */
  hasDriver(connectionId: string): boolean {
    return this.driverMap.has(connectionId);
  }

  /**
   * Disconnects and removes a driver.
   */
  async disconnectDriver(connectionId: string): Promise<boolean> {
    const hadDriver = this.driverMap.has(connectionId);
    const driver = this.driverMap.get(connectionId);

    if (driver) {
      try {
        await driver.disconnect();
      } catch (e) {
        logErrorWithContext("Failed to disconnect driver", e);
      }
    }

    this.driverMap.delete(connectionId);
    return hadDriver;
  }

  /**
   * Disconnects all drivers.
   */
  async disconnectAllDrivers(): Promise<void> {
    const connectionIds = [...this.driverMap.keys()];
    await Promise.allSettled(
      connectionIds.map((id) => this.disconnectDriver(id)),
    );
  }

  /**
   * Checks if a connection is connected.
   */
  isConnected(connectionId: string): boolean {
    return this.driverMap.get(connectionId)?.isConnected() ?? false;
  }

  /**
   * Gets the count of connected drivers.
   */
  getConnectedCount(): number {
    return [...this.driverMap.values()].filter((driver) => driver.isConnected())
      .length;
  }

  /**
   * Resolves static metadata for a driver.
   * Caches the result for future calls.
   */
  resolveDriverStaticMetadata(
    connectionId: string,
    getConnection: (id: string) => ConnectionConfig | undefined,
  ): DriverStaticMetadata | undefined {
    const driver = this.getDriver(connectionId);
    if (driver) {
      const capabilities = resolveDriverCapabilities(driver);
      return {
        manifest: resolveDriverEntityManifest(driver),
        capabilities,
        editorPresentation: capabilities?.editorPresentation,
      };
    }

    const cachedMetadata = this.driverStaticMetadataCache.get(connectionId);
    if (cachedMetadata) {
      return cachedMetadata;
    }

    const config = getConnection(connectionId);
    if (!config) {
      return undefined;
    }

    const metadataDriver = this.createDriver(config);
    const capabilities = resolveDriverCapabilities(metadataDriver);
    const metadata = {
      manifest: resolveDriverEntityManifest(metadataDriver),
      capabilities,
      editorPresentation: capabilities?.editorPresentation,
    };

    this.driverStaticMetadataCache.set(connectionId, metadata);
    return metadata;
  }

  /**
   * Invalidates the static metadata cache for a connection.
   */
  invalidateDriverStaticMetadata(connectionId: string): void {
    this.driverStaticMetadataCache.delete(connectionId);
  }

  /**
   * Gets driver capabilities for a connection.
   */
  getDriverCapabilities(
    connectionId: string,
    getConnection: (id: string) => ConnectionConfig | undefined,
  ): DriverCapabilities | undefined {
    return this.resolveDriverStaticMetadata(connectionId, getConnection)
      ?.capabilities;
  }

  /**
   * Gets the query editor presentation for a connection.
   */
  getQueryEditorPresentation(
    connectionId: string,
    getConnection: (id: string) => ConnectionConfig | undefined,
  ) {
    return this.resolveDriverStaticMetadata(connectionId, getConnection)
      ?.editorPresentation;
  }

  /**
   * Gets the driver entity manifest for a connection.
   */
  getDriverEntityManifest(
    connectionId: string,
    getConnection: (id: string) => ConnectionConfig | undefined,
  ): DriverEntityManifest {
    return (
      this.resolveDriverStaticMetadata(connectionId, getConnection)?.manifest ??
      DEFAULT_DRIVER_ENTITY_MANIFEST
    );
  }

  /**
   * Applies SSH runtime to a connection config.
   */
  applySshRuntimeToConfig(
    config: ConnectionConfig,
    runtime: SshRuntime,
  ): DriverConnectionConfig {
    const runtimeConfig = config as DriverConnectionConfig;

    if (runtime.transport.kind === "httpAgent") {
      return {
        ...config,
        runtimeOverrides: {
          ...runtimeConfig.runtimeOverrides,
          transport: runtime.transport,
        },
      } as DriverConnectionConfig;
    }

    const remoteTarget = this.resolveTcpRemoteTarget(config);
    const baseConfig: DriverConnectionConfig = {
      ...config,
      host: runtime.transport.localHost,
      port: runtime.transport.localPort,
      runtimeOverrides: {
        ...runtimeConfig.runtimeOverrides,
        transport: runtime.transport,
      },
    };

    switch (config.type) {
      case "pg":
      case "mysql":
        baseConfig.runtimeOverrides = {
          ...baseConfig.runtimeOverrides,
          tlsServername: remoteTarget.host,
        };
        break;
      case "mssql":
        baseConfig.runtimeOverrides = {
          ...baseConfig.runtimeOverrides,
          mssqlServerName: remoteTarget.host,
        };
        break;
      case "mongodb": {
        const rewrittenConnectionUri = this.rewriteUriHostPort(
          config.connectionUri,
          runtime.transport.localHost,
          runtime.transport.localPort,
        );
        const rewrittenLegacyUri = this.rewriteUriHostPort(
          config.uri,
          runtime.transport.localHost,
          runtime.transport.localPort,
        );

        return {
          ...baseConfig,
          connectionUri: rewrittenConnectionUri,
          uri: rewrittenLegacyUri,
          directConnection: true,
        };
      }
      case "redis":
        return {
          ...baseConfig,
          runtimeOverrides: {
            ...baseConfig.runtimeOverrides,
            tlsServername: remoteTarget.host,
          },
          connectionUri: this.rewriteUriHostPort(
            config.connectionUri,
            runtime.transport.localHost,
            runtime.transport.localPort,
          ),
        };
      case "elasticsearch":
        return {
          ...baseConfig,
          runtimeOverrides: {
            ...baseConfig.runtimeOverrides,
            tlsServername: remoteTarget.host,
          },
          connectionUri: this.rewriteUriHostPort(
            config.connectionUri,
            runtime.transport.localHost,
            runtime.transport.localPort,
          ),
          endpoint: this.rewriteUriHostPort(
            config.endpoint,
            runtime.transport.localHost,
            runtime.transport.localPort,
          ),
        };
      case "dynamodb":
        return {
          ...baseConfig,
          runtimeOverrides: {
            ...baseConfig.runtimeOverrides,
            tlsServername: remoteTarget.host,
          },
          endpoint: this.rewriteUriHostPort(
            config.endpoint ?? this.resolveDynamoEndpoint(config),
            runtime.transport.localHost,
            runtime.transport.localPort,
          ),
          awsEndpoint: this.rewriteUriHostPort(
            config.awsEndpoint,
            runtime.transport.localHost,
            runtime.transport.localPort,
          ),
        };
      default:
        break;
    }

    return baseConfig;
  }

  /**
   * Resolves MongoDB remote target.
   */
  resolveMongoRemoteTarget(config: ConnectionConfig): {
    host: string;
    port: number;
  } {
    return this.driverFactory.resolveMongoRemoteTarget(config);
  }

  /**
   * Resolves Redis remote target.
   */
  resolveRedisRemoteTarget(config: ConnectionConfig): {
    host: string;
    port: number;
  } {
    return this.driverFactory.resolveRedisRemoteTarget(config);
  }

  /**
   * Resolves URL remote target.
   */
  resolveUrlRemoteTarget(
    value: string | undefined,
    defaultPort: number,
  ): { host: string; port: number } | undefined {
    return this.driverFactory.resolveUrlRemoteTarget(value, defaultPort);
  }

  /**
   * Resolves Elasticsearch remote target.
   */
  resolveElasticsearchRemoteTarget(
    config: ConnectionConfig,
  ): { host: string; port: number } | undefined {
    return this.driverFactory.resolveElasticsearchRemoteTarget(config);
  }

  /**
   * Resolves DynamoDB endpoint.
   */
  resolveDynamoEndpoint(config: ConnectionConfig): string {
    if (config.endpoint) {
      return config.endpoint;
    }

    if (config.awsEndpoint) {
      return config.awsEndpoint;
    }

    const region = config.awsRegion?.trim() || "us-east-1";
    return `https://dynamodb.${region}.amazonaws.com`;
  }

  /**
   * Resolves DynamoDB remote target.
   */
  resolveDynamoRemoteTarget(config: ConnectionConfig): {
    host: string;
    port: number;
  } {
    return (
      this.resolveUrlRemoteTarget(
        config.endpoint ?? config.awsEndpoint,
        443,
      ) ?? {
        host: `dynamodb.${config.awsRegion?.trim() || "us-east-1"}.amazonaws.com`,
        port: 443,
      }
    );
  }

  /**
   * Resolves TCP remote target for a connection.
   */
  private resolveTcpRemoteTarget(config: ConnectionConfig): {
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
   * Rewrites a URL's host and port.
   */
  private rewriteUriHostPort(
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
}
