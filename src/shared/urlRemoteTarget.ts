export function resolveUrlRemoteTarget(
  value: string | undefined,
  defaultPort: number,
): { host: string; port: number } | undefined {
  if (!value) {
    return undefined;
  }

  const url = new URL(value);
  return {
    host: url.hostname.replace(/^\[|\]$/g, ""),
    port:
      url.port.length > 0
        ? Number.parseInt(url.port, 10)
        : url.protocol === "https:"
          ? 443
          : url.protocol === "http:"
            ? 80
            : defaultPort,
  };
}
