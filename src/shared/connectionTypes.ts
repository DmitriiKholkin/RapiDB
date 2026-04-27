export const CONNECTION_TYPES = [
  "pg",
  "mysql",
  "sqlite",
  "mssql",
  "oracle",
] as const;

export type ConnectionType = (typeof CONNECTION_TYPES)[number];

export const DEFAULT_PORT_BY_CONNECTION_TYPE: Record<ConnectionType, number> = {
  pg: 5432,
  mysql: 3306,
  sqlite: 0,
  mssql: 1433,
  oracle: 1521,
};

export const CONNECTION_TYPE_LABELS: Record<ConnectionType, string> = {
  pg: "PostgreSQL",
  mysql: "MySQL / MariaDB",
  sqlite: "SQLite",
  mssql: "SQL Server (MSSQL)",
  oracle: "Oracle",
};
