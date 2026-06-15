/**
 * SSH Connection Helper — утилиты для настройки SSH-туннелей.
 *
 * Извлечены из ConnectionManager для соблюдения SRP.
 * Содержат логику построения SSH-настроек, разрешения удалённых целей
 * и применения SSH runtime к конфигурации драйвера.
 */

import type { ConnectionConfig } from "./connectionManagerModels";
import type { DriverConnectionConfig } from "./driverRuntimeConfig";
import type {
  ConnectionSshSettings,
  SshRuntime,
  SshRuntimeRequest,
} from "./services/sshRuntime";

// ─── SSH Settings Builder ──────────────────────────────────────────────────

/**
 * Строит SSH-настройки из конфигурации соединения.
 * @throws Если SSH-настройки неполные
 */
export function buildConnectionSshSettings(
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
      auth: { kind: "password", password: ssh.password },
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

// ─── Remote Target Resolution ──────────────────────────────────────────────

/** Результат разрешения удалённой цели (host:port). */
export interface RemoteTarget {
  host: string;
  port: number;
}

/**
 * Разрешает удалённую цель для TCP-соединения по типу драйвера.
 * @throws Если тип драйвера не поддерживает SSH
 */
export function resolveTcpRemoteTarget(
  config: ConnectionConfig,
  resolveMongoRemoteTarget: (config: ConnectionConfig) => RemoteTarget,
  resolveRedisRemoteTarget: (config: ConnectionConfig) => RemoteTarget,
  _resolveUrlRemoteTarget: (
    value: string | undefined,
    defaultPort: number,
  ) => RemoteTarget | undefined,
  resolveElasticsearchRemoteTarget: (
    config: ConnectionConfig,
  ) => RemoteTarget | undefined,
  resolveDynamoRemoteTarget: (config: ConnectionConfig) => RemoteTarget,
): RemoteTarget {
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
      return resolveMongoRemoteTarget(config);
    case "redis":
      return resolveRedisRemoteTarget(config);
    case "elasticsearch": {
      const remoteTarget = resolveElasticsearchRemoteTarget(config);
      if (remoteTarget) {
        return remoteTarget;
      }
      throw new Error(
        "[RapiDB] Elasticsearch over SSH requires a fixed host, endpoint, or connection URI when Cloud ID is not used.",
      );
    }
    case "dynamodb":
      return resolveDynamoRemoteTarget(config);
    default: {
      const unsupported = config.type;
      throw new Error(
        `[RapiDB] SSH TCP forwarding is not supported for ${unsupported}.`,
      );
    }
  }
}

/**
 * Разрешает SSH runtime request на основе типа драйвера.
 * @throws Если SSH не поддерживается для данного типа
 */
export function resolveSshRuntimeRequest(
  config: ConnectionConfig,
  resolveTcpRemoteTargetFn: (config: ConnectionConfig) => RemoteTarget,
  _resolveElasticsearchRemoteTarget: (
    config: ConnectionConfig,
  ) => RemoteTarget | undefined,
): SshRuntimeRequest {
  switch (config.type) {
    case "pg":
    case "mysql":
    case "mssql":
    case "oracle":
    case "mongodb":
    case "redis": {
      const remoteTarget = resolveTcpRemoteTargetFn(config);
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
      const remoteTarget = resolveTcpRemoteTargetFn(config);
      return {
        kind: "tcpForward",
        remoteHost: remoteTarget.host,
        remotePort: remoteTarget.port,
      };
    }
    case "dynamodb": {
      const remoteTarget = resolveTcpRemoteTargetFn(config);
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

// ─── URI Rewriting ─────────────────────────────────────────────────────────

/**
 * Перезаписывает host и port в URI.
 */
export function rewriteUriHostPort(
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

// ─── Config Transformation ─────────────────────────────────────────────────

/**
 * Применяет SSH runtime к конфигурации драйвера.
 * Перезаписывает host/port на локальные значения SSH-туннеля.
 */
export function applySshRuntimeToConfig(
  config: ConnectionConfig,
  runtime: SshRuntime,
  resolveTcpRemoteTargetFn: (config: ConnectionConfig) => RemoteTarget,
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

  const remoteTarget = resolveTcpRemoteTargetFn(config);
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
      const rewrittenConnectionUri = rewriteUriHostPort(
        config.connectionUri,
        runtime.transport.localHost,
        runtime.transport.localPort,
      );
      const rewrittenLegacyUri = rewriteUriHostPort(
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
        connectionUri: rewriteUriHostPort(
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
        connectionUri: rewriteUriHostPort(
          config.connectionUri,
          runtime.transport.localHost,
          runtime.transport.localPort,
        ),
        endpoint: rewriteUriHostPort(
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
        endpoint: rewriteUriHostPort(
          config.endpoint ?? config.awsEndpoint,
          runtime.transport.localHost,
          runtime.transport.localPort,
        ),
        awsEndpoint: rewriteUriHostPort(
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
