import { createHash, randomUUID } from "node:crypto";
import * as vscode from "vscode";
import type {
  ConnectionTlsConfig,
  ConnectionTlsMode,
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
  mutateConnections<TResult>(
    mutate: (
      current: StoredConnectionConfig[],
    ) =>
      | ConnectionStoreMutation<TResult>
      | Promise<ConnectionStoreMutation<TResult>>,
  ): Promise<TResult>;
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

export interface ConnectionStoreMutation<TResult> {
  connections?: ConnectionConfig[];
  result: TResult;
  rollback?: () => Promise<void>;
}

interface ConnectionsConfigurationSnapshot {
  connections: StoredConnectionConfig[];
  target: vscode.ConfigurationTarget;
  revision: string;
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
  // Compare the representation VS Code persists, not in-memory optional fields:
  // JSON omits object undefined, turns array holes/undefined into null, and
  // applies toJSON/non-finite number rules. Sort keys only after that roundtrip.
  const persisted = JSON.parse(JSON.stringify(connections));
  return createHash("sha256").update(stableSerialize(persisted)).digest("hex");
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
    // `ssl` / `rejectUnauthorized` are intentionally discarded here so
    // they don't leak into the sanitized record stored on disk.
    const { ssl: _ssl, rejectUnauthorized: _ru, ...rest } = raw;
    return {
      ...rest,
      tls: { mode } as ConnectionTlsConfig,
    } as unknown as StoredConnectionConfig;
  });

  return changed ? migrated : connections;
}

function normalizeStoredConnections(connections: StoredConnectionConfig[]): {
  connections: StoredConnectionConfig[];
  changed: boolean;
} {
  const tlsMigrated = migrateLegacyTlsFlags(connections);
  let changed = tlsMigrated !== connections;
  const usedIds = new Set<string>();
  const normalized = tlsMigrated.map((connection) => {
    const rawId = (connection as { id?: unknown }).id;
    const id = typeof rawId === "string" ? rawId.trim() : "";
    if (id && !usedIds.has(id)) {
      usedIds.add(id);
      return connection;
    }

    changed = true;
    let generatedId = randomUUID();
    while (usedIds.has(generatedId)) {
      generatedId = randomUUID();
    }
    usedIds.add(generatedId);
    return { ...connection, id: generatedId };
  });

  return { connections: changed ? normalized : connections, changed };
}

export class VSCodeConnectionManagerStore implements ConnectionManagerStore {
  private static mutationTail: Promise<void> = Promise.resolve();
  private normalizedCache:
    | (ConnectionsConfigurationSnapshot & {
        changed: boolean;
        scheduled: boolean;
      })
    | undefined;

  constructor(private readonly context: vscode.ExtensionContext) {}

  private readConnectionsSnapshot(): ConnectionsConfigurationSnapshot {
    const configuration = vscode.workspace.getConfiguration("rapidb");
    const inspected =
      typeof configuration.inspect === "function"
        ? configuration.inspect<StoredConnectionConfig[]>("connections")
        : undefined;
    let connections: StoredConnectionConfig[];
    let target: vscode.ConfigurationTarget;

    if (inspected?.workspaceFolderValue !== undefined) {
      connections = inspected.workspaceFolderValue;
      target = vscode.ConfigurationTarget.WorkspaceFolder;
    } else if (inspected?.workspaceValue !== undefined) {
      connections = inspected.workspaceValue;
      target = vscode.ConfigurationTarget.Workspace;
    } else if (inspected?.globalValue !== undefined) {
      connections = inspected.globalValue;
      target = vscode.ConfigurationTarget.Global;
    } else {
      connections =
        configuration.get<StoredConnectionConfig[]>("connections") ?? [];
      target = vscode.ConfigurationTarget.Global;
    }

    return {
      connections,
      target,
      revision: computeConnectionsRevision(connections),
    };
  }

  private enqueue<TResult>(
    operation: () => Promise<TResult>,
  ): Promise<TResult> {
    const run = VSCodeConnectionManagerStore.mutationTail.then(
      operation,
      operation,
    );
    VSCodeConnectionManagerStore.mutationTail = run.then(
      () => undefined,
      () => undefined,
    );
    return run;
  }

  private getNormalization(snapshot: ConnectionsConfigurationSnapshot) {
    if (
      this.normalizedCache?.revision !== snapshot.revision ||
      this.normalizedCache.target !== snapshot.target
    ) {
      this.normalizedCache = {
        ...snapshot,
        ...normalizeStoredConnections(snapshot.connections),
        scheduled: false,
      };
    }
    return this.normalizedCache;
  }

  private assertSnapshotCurrent(
    snapshot: ConnectionsConfigurationSnapshot,
  ): void {
    const current = this.readConnectionsSnapshot();
    if (
      current.revision !== snapshot.revision ||
      current.target !== snapshot.target
    ) {
      throw new Error(
        "Connection settings changed during the operation. Please retry.",
      );
    }
  }

  private async persistNormalization(
    snapshot: ConnectionsConfigurationSnapshot,
  ): Promise<void> {
    const normalized = this.getNormalization(snapshot);
    if (!normalized.changed) return;
    const backups: Array<{ id: string; raw: string | undefined }> = [];
    const retainedIds = new Set(
      normalized.connections.map((connection) => connection.id),
    );
    const orphanedSecrets = new Map<string, string>();
    let writeStarted = false;
    try {
      for (const [index, connection] of normalized.connections.entries()) {
        const originalId = snapshot.connections[index].id;
        if (typeof originalId !== "string" || originalId === connection.id)
          continue;
        const raw = await this.getSecret(originalId);
        if (raw === undefined) continue;
        backups.push({
          id: connection.id,
          raw: await this.getSecret(connection.id),
        });
        await this.storeSecret(connection.id, raw);
        // Duplicate IDs still belong to the first retained record. Only remove
        // old keys that no normalized record owns (e.g. empty/whitespace IDs).
        if (!retainedIds.has(originalId)) orphanedSecrets.set(originalId, raw);
      }
      this.assertSnapshotCurrent(snapshot);
      for (const [id, raw] of orphanedSecrets) {
        backups.push({ id, raw });
        await this.deleteSecret(id);
      }
      this.assertSnapshotCurrent(snapshot);
      writeStarted = true;
      await vscode.workspace
        .getConfiguration("rapidb")
        .update("connections", normalized.connections, snapshot.target);
    } catch (error) {
      const current = this.readConnectionsSnapshot();
      if (
        !(
          writeStarted &&
          current.target === snapshot.target &&
          current.revision ===
            computeConnectionsRevision(normalized.connections)
        )
      ) {
        const errors: unknown[] = [error];
        for (const backup of backups.reverse()) {
          try {
            if (backup.raw === undefined) await this.deleteSecret(backup.id);
            else await this.storeSecret(backup.id, backup.raw);
          } catch (rollbackError) {
            errors.push(rollbackError);
          }
        }
        if (errors.length > 1)
          throw new AggregateError(
            errors,
            "Connection normalization and rollback failed.",
          );
        throw error;
      }
    }
    if (this.normalizedCache === normalized) this.normalizedCache = undefined;
  }

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
    const snapshot = this.readConnectionsSnapshot();
    const normalized = this.getNormalization(snapshot);
    if (normalized.changed && !normalized.scheduled) {
      normalized.scheduled = true;
      void this.enqueue(async () => {
        this.assertSnapshotCurrent(snapshot);
        await this.persistNormalization(snapshot);
      })
        .catch(() => {
          // Keep the same repaired IDs and retry on the next read or mutation.
        })
        .finally(() => {
          normalized.scheduled = false;
        });
    }
    return normalized.connections.map((connection) => ({ ...connection }));
  }

  async saveConnections(connections: ConnectionConfig[]): Promise<void> {
    await this.mutateConnections(() => ({ connections, result: undefined }));
  }

  getConnectionsRevision(): string {
    return this.readConnectionsSnapshot().revision;
  }

  async saveConnectionsIfRevision(
    expectedRevision: string,
    connections: ConnectionConfig[],
  ): Promise<boolean> {
    return await this.enqueue(async () => {
      const snapshot = this.readConnectionsSnapshot();
      if (snapshot.revision !== expectedRevision) {
        return false;
      }

      const normalizedRevision = computeConnectionsRevision(
        this.getNormalization(snapshot).connections,
      );
      await this.persistNormalization(snapshot);
      const current = this.readConnectionsSnapshot();
      if (
        current.revision !== normalizedRevision ||
        current.target !== snapshot.target
      ) {
        return false;
      }
      await vscode.workspace
        .getConfiguration("rapidb")
        .update("connections", connections, snapshot.target);
      this.normalizedCache = undefined;
      return true;
    });
  }

  async mutateConnections<TResult>(
    mutate: (
      current: StoredConnectionConfig[],
    ) =>
      | ConnectionStoreMutation<TResult>
      | Promise<ConnectionStoreMutation<TResult>>,
  ): Promise<TResult> {
    return await this.enqueue(async () => {
      await this.persistNormalization(this.readConnectionsSnapshot());
      const snapshot = this.readConnectionsSnapshot();
      const normalized = this.getNormalization(snapshot);
      const mutation = await mutate(
        normalized.connections.map((connection) => ({ ...connection })),
      );
      let writeStarted = false;
      try {
        this.assertSnapshotCurrent(snapshot);
        if (!mutation.connections) {
          return mutation.result;
        }
        writeStarted = true;
        await vscode.workspace
          .getConfiguration("rapidb")
          .update("connections", mutation.connections, snapshot.target);
        this.normalizedCache = undefined;
      } catch (writeError) {
        const postFailureSnapshot = this.readConnectionsSnapshot();
        if (
          writeStarted &&
          mutation.connections &&
          postFailureSnapshot.target === snapshot.target &&
          computeConnectionsRevision(mutation.connections) ===
            postFailureSnapshot.revision
        ) {
          this.normalizedCache = undefined;
          return mutation.result;
        }
        if (!mutation.rollback) {
          throw writeError;
        }
        try {
          await mutation.rollback();
        } catch (rollbackError) {
          throw new AggregateError(
            [writeError, rollbackError],
            "Connection settings write and rollback both failed.",
          );
        }
        throw writeError;
      }
      return mutation.result;
    });
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
