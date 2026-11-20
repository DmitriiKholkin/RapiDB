export function resolvePostgresPort(configuredPort?: number): number {
  // Match pg's base-10 parsing and its fallback for an unset or empty PGPORT.
  // Never fall back to a different listener when a nonempty value is unusable.
  const port =
    configuredPort ?? Number.parseInt(process.env.PGPORT || "5432", 10);
  if (!Number.isInteger(port) || port < 1 || port > 65535) {
    throw new Error(
      configuredPort === undefined
        ? "[RapiDB] PGPORT must resolve to a PostgreSQL port between 1 and 65535. Set an explicit database port to override it."
        : "[RapiDB] Database port must be an integer between 1 and 65535.",
    );
  }
  return port;
}
