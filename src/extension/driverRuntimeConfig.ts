import type { ConnectionConfig } from "./connectionManagerModels";
import type { SshRuntimeTransport } from "./services/sshRuntime";

export interface ConnectionRuntimeOverrides {
  transport?: SshRuntimeTransport;
  tlsServername?: string;
  mssqlServerName?: string;
}

export type DriverConnectionConfig = ConnectionConfig & {
  runtimeOverrides?: ConnectionRuntimeOverrides;
};

export function asDriverConnectionConfig(
  config: ConnectionConfig,
): DriverConnectionConfig {
  return config as DriverConnectionConfig;
}

export function getRuntimeOverrides(
  config: ConnectionConfig,
): ConnectionRuntimeOverrides | undefined {
  return asDriverConnectionConfig(config).runtimeOverrides;
}

export function getSshTcpForwardTransport(
  config: ConnectionConfig,
): Extract<SshRuntimeTransport, { kind: "tcpForward" }> | undefined {
  const transport = getRuntimeOverrides(config)?.transport;
  return transport?.kind === "tcpForward" ? transport : undefined;
}

export function getSshHttpAgentTransport(
  config: ConnectionConfig,
): Extract<SshRuntimeTransport, { kind: "httpAgent" }> | undefined {
  const transport = getRuntimeOverrides(config)?.transport;
  return transport?.kind === "httpAgent" ? transport : undefined;
}

export function getTlsServername(config: ConnectionConfig): string | undefined {
  return getRuntimeOverrides(config)?.tlsServername;
}

export function getMssqlServerName(
  config: ConnectionConfig,
): string | undefined {
  return getRuntimeOverrides(config)?.mssqlServerName;
}
