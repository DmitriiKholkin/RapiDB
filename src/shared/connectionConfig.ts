import type { ConnectionType } from "./connectionTypes";

export interface ConnectionConfig {
  id: string;
  name: string;
  type: ConnectionType;
  readOnly?: boolean;
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
  connectionUri?: string;
  authDatabase?: string;
  replicaSet?: string;
  directConnection?: boolean;
  redisUsername?: string;
  keyPrefix?: string;
  awsProfile?: string;
  endpoint?: string;
  apiKey?: string;
  cloudId?: string;
  uri?: string;
  authSource?: string;
  redisDb?: number;
  awsRegion?: string;
  awsAccessKeyId?: string;
  awsSecretAccessKey?: string;
  awsSessionToken?: string;
  awsEndpoint?: string;
  useSecretStorage?: boolean;
}
