import { createHash } from "node:crypto";
import * as vscode from "vscode";
import {
  type ConnectionTlsConfig,
  type ConnectionTlsMode,
} from "../shared/connectionConfig";
import { QUERY_LIMIT_POLICY } from "../shared/safetyContracts";
import type {
  BookmarkEntry,
  ConnectionConfig,
  HistoryEntry,
  StoredConnectionConfig,
} from "./connectionManagerModels";
import {
  CONNECTION_TIMEOUT_SECONDS_DEFAULT,
  createDriverTimeoutSettingsSnapshot,
  DB_OPERATION_TIMEOUT_SECONDS_DEFAULT,
  type DriverTimeoutSettingsSnapshot,
} from "./dbDrivers/timeout";

const HISTORY_STATE_KEY = "rapidb.queryHistory";
const BOOKMARKS_STATE_KEY = "rapidb.bookmarks";
const HISTORY_LIMIT_DEFAULT = 100;
const HISTORY_LIMIT_MAX = 10000;
const VALID_PAGE_SIZES = [25, 100, 500, 1000] as const;

export interface ConnectionManagerStore {
  onDidChangeConfiguration(
    listener: (event: vscode.ConfigurationChangeEvent) => void,
    subscriptions: vscode.Disposable[],
  ): void;
  getConnections(): StoredConnectionConfig[];
  saveConnections(connections: ConnectionConfig[]): Promise<void>;
  getConnectionsRevision(): string;
  saveConnectionsIfRevision(
    expectedRevision: string,
    connections: ConnectionConfig[],
  ): Promise<boolean>;
  readHistory(): HistoryEntry[];
  writeHistory(entries: HistoryEntry[]): Promise<void>;
  readBookmarks(): BookmarkEntry[];
  writeBookmarks(entries: BookmarkEntry[]): Promise<void>;
  getSecret(id: string): Promise<string | undefined>;
  storeSecret(id: string, value: string): Promise<void>;
  deleteSecret(id: string): Promise<void>;
  getHistoryLimit(): number;
  getDefaultPageSize(): number;
  getQueryRowLimit(): number;
  getSkipTableMutationPreview(): boolean;
  getTimeoutSettings(): DriverTimeoutSettingsSnapshot;
}

function stableSerialize(value: unknown): string {
  if (Array.isArray(value)) {
    return `[${value.map((item) => stableSerialize(item)).join(",")}]`;
  }

  if (value && typeof value === "object") {
    const record = value as Record<string, unknown>;
    const keys = Object.keys(record).sort((a, b) => a.localeCompare(b));
    return `{${keys
      .map((key) => `${JSON.stringify(key)}:${stableSerialize(record[key])}`)
      .join(",")}}`;
  }

  return JSON.stringify(value);
}

function computeConnectionsRevision(
  connections: StoredConnectionConfig[],
): string {
  return createHash("sha256")
    .update(stableSerialize(connections))
    .digest("hex");
}

/**
 * Migrate legacy `ssl` / `rejectUnauthorized` boolean flags into the
 * structured `tls` config. Returns a new array; originals are not mutated.
 * Connections that already use `tls` are left untouched.
 */
function migrateLegacyTlsFlags(
  connections: StoredConnectionConfig[],
): StoredConnectionConfig[] {
  let changed = false;
  const migrated = connections.map((conn) => {
    const raw = conn as unknown as Record<string, unknown>;
    const hasLegacySsl = "ssl" in raw;
    const hasLegacyReject = "rejectUnauthorized" in raw;
    const hasTls = conn.tls != null;

    if ((!hasLegacySsl && !hasLegacyReject) || hasTls) {
      // No legacy flags, or already has tls — strip legacy keys if present
      if (hasLegacySsl || hasLegacyReject) {
        changed = true;
        const { ssl: _ssl, rejectUnauthorized: _ru, ...rest } = raw;
        return rest as unknown as StoredConnectionConfig;
      }
      return conn;
    }

    changed = true;
    const ssl = raw.ssl === true;
    const rejectUnauthorized = raw.rejectUnauthorized !== false;
    const mode: ConnectionTlsMode = ssl
      ? rejectUnauthorized
        ? "requireVerifyFull"
        : "requireTrustServerCertificate"
      : "disabled";
    const { ssl: _ssl2, rejectUnauthorized: _ru2, ...rest } = raw;
    return {
      ...rest,
      tls: { mode } as ConnectionTlsConfig,
    } as unknown as StoredConnectionConfig;
  });

  return changed ? migrated : connections;
}

export class VSCodeConnectionManagerStore implements ConnectionManagerStore {
  constructor(private readonly context: vscode.ExtensionContext) {}

  onDidChangeConfiguration(
    listener: (event: vscode.ConfigurationChangeEvent) => void,
    subscriptions: vscode.Disposable[],
  ): void {
    vscode.workspace.onDidChangeConfiguration(
      listener,
      undefined,
      subscriptions,
    );
  }

  getConnections(): StoredConnectionConfig[] {
    const raw =
      vscode.workspace
        .getConfiguration("rapidb")
        .get<StoredConnectionConfig[]>("connections") ?? [];
    const migrated = migrateLegacyTlsFlags(raw);
    // Persist the migration so legacy keys are removed from storage
    if (migrated !== raw) {
      vscode.workspace
        .getConfiguration("rapidb")
        .update("connections", migrated, vscode.ConfigurationTarget.Global)
        .then(undefined, () => {
          // Best-effort; stale keys will be migrated again on next read
        });
    }
    return migrated;
  }

  async saveConnections(connections: ConnectionConfig[]): Promise<void> {
    await vscode.workspace
      .getConfiguration("rapidb")
      .update("connections", connections, vscode.ConfigurationTarget.Global);
  }

  getConnectionsRevision(): string {
    return computeConnectionsRevision(this.getConnections());
  }

  async saveConnectionsIfRevision(
    expectedRevision: string,
    connections: ConnectionConfig[],
  ): Promise<boolean> {
    const configuration = vscode.workspace.getConfiguration("rapidb");
    const current =
      configuration.get<StoredConnectionConfig[]>("connections") ?? [];
    const currentRevision = computeConnectionsRevision(current);
    if (currentRevision !== expectedRevision) {
      return false;
    }

    await configuration.update(
      "connections",
      connections,
      vscode.ConfigurationTarget.Global,
    );
    return true;
  }

  readHistory(): HistoryEntry[] {
    return (
      this.context.globalState.get<HistoryEntry[]>(HISTORY_STATE_KEY) ?? []
    );
  }

  async writeHistory(entries: HistoryEntry[]): Promise<void> {
    await this.context.globalState.update(HISTORY_STATE_KEY, entries);
  }

  readBookmarks(): BookmarkEntry[] {
    return (
      this.context.globalState.get<BookmarkEntry[]>(BOOKMARKS_STATE_KEY) ?? []
    );
  }

  async writeBookmarks(entries: BookmarkEntry[]): Promise<void> {
    await this.context.globalState.update(BOOKMARKS_STATE_KEY, entries);
  }

  async getSecret(id: string): Promise<string | undefined> {
    return await this.context.secrets.get(id);
  }

  async storeSecret(id: string, value: string): Promise<void> {
    await this.context.secrets.store(id, value);
  }

  async deleteSecret(id: string): Promise<void> {
    await this.context.secrets.delete(id);
  }

  getHistoryLimit(): number {
    const raw = vscode.workspace
      .getConfiguration("rapidb")
      .get<number>("queryHistoryLimit", HISTORY_LIMIT_DEFAULT);
    return Math.max(0, Math.min(HISTORY_LIMIT_MAX, Math.round(raw)));
  }

  getDefaultPageSize(): number {
    const raw = vscode.workspace
      .getConfiguration("rapidb")
      .get<number>("defaultPageSize", 25);
    return (VALID_PAGE_SIZES as readonly number[]).includes(raw) ? raw : 25;
  }

  getQueryRowLimit(): number {
    const raw = vscode.workspace
      .getConfiguration("rapidb")
      .get<number>("queryRowLimit", 1000);
    return Math.max(10, Math.min(QUERY_LIMIT_POLICY.hardCap, Math.round(raw)));
  }

  getSkipTableMutationPreview(): boolean {
    return (
      vscode.workspace
        .getConfiguration("rapidb")
        .get<boolean>("skipTableMutationPreview", false) === true
    );
  }

  getTimeoutSettings(): DriverTimeoutSettingsSnapshot {
    const configuration = vscode.workspace.getConfiguration("rapidb");
    return createDriverTimeoutSettingsSnapshot({
      connectionTimeoutSeconds: configuration.get<number>(
        "connectionTimeoutSeconds",
        CONNECTION_TIMEOUT_SECONDS_DEFAULT,
      ),
      dbOperationTimeoutSeconds: configuration.get<number>(
        "dbOperationTimeoutSeconds",
        DB_OPERATION_TIMEOUT_SECONDS_DEFAULT,
      ),
    });
  }
}
