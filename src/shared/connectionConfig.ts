import type { ConnectionType } from "./connectionTypes";

export interface ConnectionConfig {
  id: string;
  name: string;
  type: ConnectionType;
  host?: string;
  port?: number;
  database?: string;
  username?: string;
  password?: string;
  filePath?: string;
  ssl?: boolean;
  rejectUnauthorized?: boolean;
  folder?: string;
  serviceName?: string;
  thickMode?: boolean;
  clientPath?: string;
  useSecretStorage?: boolean;
}
