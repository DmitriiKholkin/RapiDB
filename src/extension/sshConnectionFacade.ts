/**
 * SSH Connection Facade — инкапсулирует логику настройки SSH-туннелей.
 *
 * Извлечен из ConnectionManager для соблюдения SRP.
 * Содержит разрешение удалённых целей, построение SSH-настроек
 * и подготовку конфигурации драйвера с SSH-туннелем.
 */

import type { ConnectionDriverFactory } from "./connectionDriverFactory";
import type { ConnectionConfig } from "./connectionManagerModels";
import type { DriverConnectionConfig } from "./driverRuntimeConfig";
import type {
  ConnectionSshSettings,
  SshRuntime,
  SshRuntimeRequest,
} from "./services/sshRuntime";
import {
  applySshRuntimeToConfig as applySshConfig,
  buildConnectionSshSettings as buildSshSettings,
  type RemoteTarget,
  resolveSshRuntimeRequest as resolveSshRequest,
  resolveTcpRemoteTarget as resolveTcpTarget,
  rewriteUriHostPort as rewriteUriHost,
} from "./sshConnectionHelper";

export interface SshConnectionFacadeDependencies {
  createSshRuntime: (
    ssh: ConnectionSshSettings,
    request: SshRuntimeRequest,
  ) => Promise<SshRuntime>;
}

export class SshConnectionFacade {
  constructor(
    private readonly driverFactory: ConnectionDriverFactory,
    private readonly dependencies: SshConnectionFacadeDependencies,
  ) {}

  // ─── Remote Target Resolution ────────────────────────────────────────────

  resolveMongoRemoteTarget(config: ConnectionConfig): RemoteTarget {
    return this.driverFactory.resolveMongoRemoteTarget(config);
  }

  resolveRedisRemoteTarget(config: ConnectionConfig): RemoteTarget {
    return this.driverFactory.resolveRedisRemoteTarget(config);
  }

  resolveUrlRemoteTarget(
    value: string | undefined,
    defaultPort: number,
  ): RemoteTarget | undefined {
    return this.driverFactory.resolveUrlRemoteTarget(value, defaultPort);
  }

  resolveElasticsearchRemoteTarget(
    config: ConnectionConfig,
  ): RemoteTarget | undefined {
    return this.driverFactory.resolveElasticsearchRemoteTarget(config);
  }

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

  resolveDynamoRemoteTarget(config: ConnectionConfig): RemoteTarget {
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

  resolveTcpRemoteTarget(config: ConnectionConfig): RemoteTarget {
    return resolveTcpTarget(
      config,
      (c) => this.resolveMongoRemoteTarget(c),
      (c) => this.resolveRedisRemoteTarget(c),
      (v, p) => this.resolveUrlRemoteTarget(v, p),
      (c) => this.resolveElasticsearchRemoteTarget(c),
      (c) => this.resolveDynamoRemoteTarget(c),
    );
  }

  // ─── SSH Settings ────────────────────────────────────────────────────────

  buildSshSettings(
    config: ConnectionConfig,
  ): ConnectionSshSettings | undefined {
    return buildSshSettings(config);
  }

  resolveSshRuntimeRequest(config: ConnectionConfig): SshRuntimeRequest {
    return resolveSshRequest(
      config,
      (c) => this.resolveTcpRemoteTarget(c),
      (c) => this.resolveElasticsearchRemoteTarget(c),
    );
  }

  // ─── Config Transformation ──────────────────────────────────────────────

  applySshRuntimeToConfig(
    config: ConnectionConfig,
    runtime: SshRuntime,
  ): DriverConnectionConfig {
    return applySshConfig(config, runtime, (c) =>
      this.resolveTcpRemoteTarget(c),
    );
  }

  // ─── High-Level Orchestration ───────────────────────────────────────────

  async prepareDriverConfig(config: ConnectionConfig): Promise<{
    config: DriverConnectionConfig;
    runtime?: SshRuntime;
  }> {
    const sshSettings = this.buildSshSettings(config);
    if (!sshSettings) {
      return {
        config: config as DriverConnectionConfig,
      };
    }

    const runtime = await this.dependencies.createSshRuntime(
      sshSettings,
      this.resolveSshRuntimeRequest(config),
    );

    return {
      config: this.applySshRuntimeToConfig(config, runtime),
      runtime,
    };
  }
}
