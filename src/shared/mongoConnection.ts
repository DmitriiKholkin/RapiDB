import type { ConnectionConfig } from "./connectionConfig";

export function resolveMongoConnectionMode(
  config: Partial<ConnectionConfig>,
): "uri" | "fields" {
  return (
    config.mongoConnectionMode ??
    (config.connectionUri?.trim() || config.uri?.trim() ? "uri" : "fields")
  );
}

export function getMongoConnectionUri(
  config: Partial<ConnectionConfig>,
): string | undefined {
  if (resolveMongoConnectionMode(config) !== "uri") return undefined;
  if (config.connectionUri?.trim()) return config.connectionUri;
  if (config.uri?.trim()) return config.uri;
  return undefined;
}

function usesLegacyMongoFields(config: Partial<ConnectionConfig>): boolean {
  return (
    !config.mongoConnectionMode &&
    config.mongoScheme === undefined &&
    config.mongoAuthMethod === undefined &&
    resolveMongoConnectionMode(config) === "fields"
  );
}

export function getMongoFormHost(
  config: Partial<ConnectionConfig>,
): string | undefined {
  const host = config.host;
  if (!usesLegacyMongoFields(config) || !host?.includes(",")) return host;
  const hosts = host.trim().split(",");
  return hosts
    .map((value, index) => {
      const entry = value.trim();
      const hasPort =
        /^\[[^\]]+\]:\d+$/.test(entry) || /^[^:]+:\d+$/.test(entry);
      if (hasPort) return entry;
      return `${entry}:${index === hosts.length - 1 ? (config.port ?? 27017) : 27017}`;
    })
    .join(",");
}

export function getMongoFormUri(
  config: Partial<ConnectionConfig>,
  value = getMongoConnectionUri(config),
): string | undefined {
  if (!value || config.mongoConnectionMode) return value;
  const match = /^(mongodb(?:\+srv)?:\/\/[^/?#]+)([^?#]*)(?:\?([^#]*))?$/.exec(
    value,
  );
  if (!match) return value;
  const params = new URLSearchParams(match[3]);
  let changed = false;
  for (const key of [
    "authSource",
    "replicaSet",
    "directConnection",
    "authMechanism",
  ] as const) {
    if (config[key] === undefined) continue;
    for (const existing of [...params.keys()]) {
      if (existing.toLowerCase() === key.toLowerCase()) params.delete(existing);
    }
    params.set(key, String(config[key]));
    changed = true;
  }
  if (!changed) return value;
  return `${match[1]}${match[2]}${params.size ? `?${params}` : ""}`;
}

export function normalizeMongoConnectionConfig<
  T extends Partial<ConnectionConfig>,
>(config: T): T {
  if (config.type !== "mongodb") return config;
  const mode = resolveMongoConnectionMode(config);
  if (
    !config.mongoConnectionMode &&
    (mode === "uri" || usesLegacyMongoFields(config))
  )
    return config;
  if (mode === "uri") {
    return {
      ...config,
      connectionUri: getMongoConnectionUri(config),
      host: undefined,
      port: undefined,
      username: undefined,
      password: undefined,
      authSource: undefined,
      authMechanism: undefined,
      mongoAuthMethod: undefined,
      replicaSet: undefined,
      directConnection: undefined,
      uri: undefined,
    };
  }
  return {
    ...config,
    mongoConnectionMode: "fields",
    connectionUri: undefined,
    uri: undefined,
    ...(config.mongoAuthMethod === "none"
      ? {
          username: undefined,
          password: undefined,
          authSource: undefined,
          authMechanism: undefined,
        }
      : {}),
  };
}

export function buildMongoFieldsUri(config: Partial<ConnectionConfig>): string {
  const scheme = config.mongoScheme ?? "mongodb";
  const hosts = (config.host?.trim() || "localhost")
    .split(",")
    .map((entry) => {
      const host = entry.trim();
      if (scheme === "mongodb+srv") return host;
      const ipv6 = !host.startsWith("[") && (host.match(/:/g)?.length ?? 0) > 1;
      const formatted = ipv6 ? `[${host}]` : host;
      const hasPort =
        /^\[[^\]]+\]:\d+$/.test(formatted) || /^[^:]+:\d+$/.test(formatted);
      return hasPort ? formatted : `${formatted}:${config.port ?? 27017}`;
    })
    .join(",");
  const auth =
    config.mongoAuthMethod !== "none" && config.username
      ? `${encodeURIComponent(config.username)}:${encodeURIComponent(config.password ?? "")}@`
      : "";
  const params = new URLSearchParams();
  for (const key of [
    "authSource",
    "authMechanism",
    "replicaSet",
    "directConnection",
  ] as const) {
    if (config[key] !== undefined && config[key] !== "")
      params.set(key, String(config[key]));
  }
  return `${scheme}://${auth}${hosts}/${encodeURIComponent(config.database?.trim() || "admin")}${params.size ? `?${params}` : ""}`;
}

export function resolveMongoRemoteTarget(config: Partial<ConnectionConfig>): {
  host: string;
  port: number;
} {
  const uri =
    resolveMongoConnectionMode(config) === "uri"
      ? (getMongoConnectionUri(config) ?? "").trim()
      : buildMongoFieldsUri(config);
  const authority = /^mongodb(\+srv)?:\/\/([^/?#]*)/i.exec(uri);
  if (
    authority?.[1] ||
    authority?.[2].slice(authority[2].lastIndexOf("@") + 1).includes(",")
  ) {
    throw new Error(
      "SSH tunnel requires a single MongoDB host; multi-host and mongodb+srv URIs are not supported.",
    );
  }
  const parsed = new URL(uri);
  return {
    host: parsed.hostname.replace(/^\[|\]$/g, ""),
    port: parsed.port ? Number.parseInt(parsed.port, 10) : 27017,
  };
}

export function stripMongoUriTlsOptions(
  uri: string,
  keepCertificateOptions = false,
): string {
  const queryIndex = uri.indexOf("?");
  if (queryIndex < 0) return uri;
  const options = uri
    .slice(queryIndex + 1)
    .split("&")
    .filter((part) => {
      const key = decodeURIComponent(part.split("=", 1)[0]);
      if (keepCertificateOptions) {
        return !/^(?:tls|ssl|tlsInsecure|tlsAllowInvalidCertificates|tlsAllowInvalidHostnames)$/i.test(
          key,
        );
      }
      return !/^(?:tls|ssl)/i.test(key);
    })
    .join("&");
  return `${uri.slice(0, queryIndex)}${options ? `?${options}` : ""}`;
}
