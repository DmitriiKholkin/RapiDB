import { createHash, randomUUID } from "node:crypto";
import { realpath } from "node:fs/promises";
import { basename, dirname, isAbsolute, join } from "node:path";
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
  sanitizeConnectionConfigForResponse,
  sanitizePersistedConnectionConfig,
  shouldKeepHandWrittenSecretsInSettings,
} from "./connectionSecrets";
import {
  type ConnectionSettingsFence,
  readOrCreateStorageOwnerToken,
  withConnectionSettingsLock,
} from "./connectionSettingsCoordinator";
import {
  CONNECTION_TIMEOUT_SECONDS_DEFAULT,
  createDriverTimeoutSettingsSnapshot,
  DB_OPERATION_TIMEOUT_SECONDS_DEFAULT,
  type DriverTimeoutSettingsSnapshot,
} from "./dbDrivers/timeout";
import { logger } from "./utils/logger";

const HISTORY_STATE_KEY = "rapidb.queryHistory";
const BOOKMARKS_STATE_KEY = "rapidb.bookmarks";
const SESSION_NONCES_STATE_KEY = "rapidb.connectionSessionNonces";
const DYNAMODB_TARGET_STATE_PREFIX = "rapidb.dynamoDBTarget.";
const SETTINGS_OWNER_STATE_KEY = "rapidb.connectionSettingsOwner.v1";
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const HISTORY_LIMIT_DEFAULT = 100;
const HISTORY_LIMIT_MAX = 10000;
const VALID_PAGE_SIZES = [25, 100, 500, 1000] as const;

const pendingRepairIds = new WeakMap<vscode.SecretStorage, Set<string>>();

// The explorer can display repaired IDs before the asynchronous write. Until
// then a candidate may collide with a Secret Storage key not in settings.
export function readConnectionSecret(
  storage: vscode.SecretStorage,
  id: string,
): PromiseLike<string | undefined> {
  return pendingRepairIds.get(storage)?.has(id)
    ? Promise.resolve(undefined)
    : storage.get(id);
}

export interface ConnectionManagerStore {
  onDidChangeConfiguration(
    listener: (event: vscode.ConfigurationChangeEvent) => void,
    subscriptions: vscode.Disposable[],
  ): void;
  getConnections(): StoredConnectionConfig[];
  canMutateConnections?(): boolean;
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
  onDidChangeSecret?(
    listener: (id: string) => void,
  ): vscode.Disposable | undefined;
  readConnectionSessionNonce?(id: string): string | undefined;
  writeConnectionSessionNonce?(id: string, nonce: string): Promise<void>;
  readDynamoDBTarget?(id: string): DynamoDBTargetRecord | undefined;
  writeDynamoDBTarget?(id: string, target: DynamoDBTargetRecord): Promise<void>;
  getHistoryLimit(): number;
  getDefaultPageSize(): number;
  getQueryRowLimit(): number;
  getSkipTableMutationPreview(): boolean;
  getTimeoutSettings(): DriverTimeoutSettingsSnapshot;
}

export interface DynamoDBTargetRecord {
  settingsKey: string;
  fingerprint: string | null;
}

function collectConnectionIds(
  values: unknown[],
  includeBlank = false,
): string[] {
  const ids = new Set<string>();
  for (const value of values) {
    if (!Array.isArray(value)) continue;
    for (const connection of value) {
      const rawId = (connection as { id?: unknown } | null)?.id;
      if (typeof rawId === "string" && (includeBlank || rawId.trim()))
        ids.add(rawId);
    }
  }
  return [...ids];
}

function finiteOr(value: unknown, fallback: number): number {
  return typeof value === "number" && Number.isFinite(value) ? value : fallback;
}

export interface ConnectionStoreMutation<TResult> {
  connections?: ConnectionConfig[];
  result: TResult;
  rollback?: (settingsWriteUnconfirmed?: boolean) => Promise<void>;
  // A credential-only Save commits in SecretStorage, not in settings. Run
  // this only after checking the settings snapshot, under the mutation queue.
  commitSecretOnly?: () => Promise<void>;
  // Run under the same queue, but outside the settings rollback path.
  afterSettingsWrite?: () => Promise<void>;
}

interface ConnectionsConfigurationSnapshot {
  connections: StoredConnectionConfig[];
  target: vscode.ConfigurationTarget;
  revision: string;
  resource: vscode.Uri | undefined;
  singleFolder: boolean;
}

class UnconfirmedConnectionSettingsWrite extends Error {
  constructor(error: unknown) {
    super(
      `Could not confirm the connection settings write: ${error instanceof Error ? error.message : String(error)}`,
      { cause: error },
    );
  }
}

function stableSerialize(
  value: unknown,
  compareKeys = (a: string, b: string) => a.localeCompare(b),
): string {
  if (Array.isArray(value)) {
    return `[${value.map((item) => stableSerialize(item, compareKeys)).join(",")}]`;
  }

  if (value && typeof value === "object") {
    const record = value as Record<string, unknown>;
    const keys = Object.keys(record).sort(compareKeys);
    return `{${keys
      .map(
        (key) =>
          `${JSON.stringify(key)}:${stableSerialize(record[key], compareKeys)}`,
      )
      .join(",")}}`;
  }

  return JSON.stringify(value);
}

function computeConnectionsRevision(
  connections: StoredConnectionConfig[],
): string {
  const persisted = JSON.parse(JSON.stringify(connections));
  return createHash("sha256")
    .update(stableSerialize(persisted, (a, b) => (a < b ? -1 : Number(a > b))))
    .digest("hex");
}

async function canonicalSettingsPath(path: string): Promise<string> {
  try {
    return await realpath(path);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
    // The settings file or .vscode directory can be created by the first
    // writer. Preserve its identity before and after that creation.
    const parent = dirname(path);
    if (parent === path) throw error;
    return join(await canonicalSettingsPath(parent), basename(path));
  }
}

export function canonicalizeOracleServiceName(
  config: ConnectionConfig,
): ConnectionConfig {
  if (config.type !== "oracle") {
    return config;
  }

  const rawServiceName =
    (typeof config.serviceName === "string" && config.serviceName.trim()) ||
    (typeof config.database === "string" && config.database.trim()) ||
    undefined;

  return {
    ...config,
    serviceName: rawServiceName,
    database: undefined,
  };
}

export function connectionSessionSettings(config: StoredConnectionConfig) {
  const {
    name: _name,
    folder: _folder,
    color: _color,
    useSecretStorage: _storage,
    ...settings
  } = canonicalizeOracleServiceName({
    ...config,
    username: config.username ?? config.user,
  });
  delete (settings as StoredConnectionConfig).user;
  settings.readOnly = settings.readOnly === true;
  if (
    settings.type !== "pg" &&
    settings.type !== "mongodb" &&
    settings.tls?.mode === "disabled"
  ) {
    delete settings.tls;
  }
  for (const field of ["database", "username"] as const) {
    if (settings[field] === "") delete settings[field];
  }
  return JSON.parse(JSON.stringify(settings));
}

export function connectionSessionKey(
  config: ConnectionConfig | undefined,
  sessionNonce?: string,
): string | null | undefined {
  if (!config) return undefined;
  try {
    const settings = connectionSessionSettings(
      sanitizeConnectionSessionConfig(config),
    );
    delete settings.readOnly;
    if (settings.ssh) delete settings.ssh.hostFingerprintSha256;
    return createHash("sha256")
      .update(stableSerialize({ settings, sessionNonce: sessionNonce ?? null }))
      .digest("hex");
  } catch {
    return null;
  }
}

export function sanitizeConnectionSessionConfig(
  config: ConnectionConfig,
): ConnectionConfig {
  // A migration from inline static keys to Secret Storage adds only a source
  // marker; the old and newly sanitized settings describe the same session.
  return config.type === "dynamodb" && config.useSecretStorage === true
    ? sanitizePersistedConnectionConfig(config)
    : sanitizeConnectionConfigForResponse(config);
}

export function dynamoDBSessionKey(
  settingsKey: string,
  fingerprint: string,
): string {
  return createHash("sha256")
    .update(stableSerialize({ settingsKey, dynamoDBTarget: fingerprint }))
    .digest("hex");
}

export function bindDynamoDBSshTarget(
  fingerprint: string,
  sshHostKey: string,
): string {
  return createHash("sha256")
    .update(stableSerialize({ fingerprint, sshHostKey }))
    .digest("hex");
}

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
    const { ssl: _ssl, rejectUnauthorized: _ru, ...rest } = raw;
    return {
      ...rest,
      tls: { mode } as ConnectionTlsConfig,
    } as unknown as StoredConnectionConfig;
  });

  return changed ? migrated : connections;
}

function dropRemovedConnectionFields(
  connections: StoredConnectionConfig[],
): StoredConnectionConfig[] {
  let changed = false;
  const migrated = connections.map((conn) => {
    if (!("sqliteWalMode" in conn)) return conn;
    changed = true;
    const { sqliteWalMode: _walMode, ...rest } = conn as unknown as Record<
      string,
      unknown
    >;
    return rest as unknown as StoredConnectionConfig;
  });
  return changed ? migrated : connections;
}

function markHandWrittenPlaintextSecrets(
  connections: StoredConnectionConfig[],
): StoredConnectionConfig[] {
  let changed = false;
  const migrated = connections.map((conn) => {
    if (!shouldKeepHandWrittenSecretsInSettings(conn)) return conn;
    changed = true;
    return { ...conn, useSecretStorage: false };
  });
  return changed ? migrated : connections;
}

interface ConnectionIdRepair {
  id: string;
  moveSecretFrom?: string;
}

interface NormalizedConnections {
  connections: StoredConnectionConfig[];
  changed: boolean;
  repairs: ConnectionIdRepair[];
  credentialsNotCopied: boolean;
}

interface CachedNormalization
  extends ConnectionsConfigurationSnapshot,
    NormalizedConnections {
  scopeIdsKey: string;
  scheduled: boolean;
}

function normalizeStoredConnections(
  connections: StoredConnectionConfig[],
  otherIds: string[],
  pendingIds?: ReadonlySet<string>,
): NormalizedConnections {
  const tlsMigrated = markHandWrittenPlaintextSecrets(
    dropRemovedConnectionFields(migrateLegacyTlsFlags(connections)),
  );
  let changed = tlsMigrated !== connections;
  const idCounts = new Map<string, number>();
  for (const { id } of tlsMigrated) {
    if (typeof id !== "string") continue;
    const key = id.trim();
    idCounts.set(key, (idCounts.get(key) ?? 0) + 1);
  }
  const otherKeys = new Set(otherIds.map((id) => id.trim()));
  // Reserve IDs from the entire array, including profiles after the repair.
  const reservedIds = new Set([...idCounts.keys(), ...otherKeys]);
  const usedIds = new Set<string>();
  const repairs: ConnectionIdRepair[] = [];
  let credentialsNotCopied = false;
  const normalized = tlsMigrated.map((connection) => {
    const rawId = (connection as { id?: unknown }).id;
    const id = typeof rawId === "string" ? rawId.trim() : "";
    if (id && !usedIds.has(id)) {
      usedIds.add(id);
      return connection;
    }

    changed = true;
    let generatedId = randomUUID();
    while (reservedIds.has(generatedId) || pendingIds?.has(generatedId)) {
      generatedId = randomUUID();
    }
    reservedIds.add(generatedId);

    const repaired = { ...connection, id: generatedId };
    const mayUseStoredSecrets =
      connection.type !== "sqlite" && connection.useSecretStorage !== false;
    const ambiguous = (idCounts.get(id) ?? 0) > 1 || otherKeys.has(id);
    if (ambiguous || typeof rawId !== "string") {
      // A profile link is part of the old credential association, not a
      // setting to carry over to a profile that must supply its own secrets.
      delete repaired.secretStorageProfile;
      if (ambiguous && mayUseStoredSecrets) credentialsNotCopied = true;
      repairs.push({ id: generatedId });
    } else {
      repairs.push({
        id: generatedId,
        moveSecretFrom: mayUseStoredSecrets ? rawId : undefined,
      });
    }
    return repaired;
  });

  return {
    connections: changed ? normalized : connections,
    changed,
    repairs,
    credentialsNotCopied,
  };
}

export class VSCodeConnectionManagerStore implements ConnectionManagerStore {
  private static mutationTail: Promise<void> = Promise.resolve();
  private readonly writer = randomUUID();
  private normalizedCache: CachedNormalization | undefined;

  constructor(private readonly context: vscode.ExtensionContext) {}

  private readConnectionsSnapshot(): ConnectionsConfigurationSnapshot {
    const configuration = vscode.workspace.getConfiguration("rapidb");
    const inspected =
      typeof configuration.inspect === "function"
        ? configuration.inspect<StoredConnectionConfig[]>("connections")
        : undefined;
    const singleFolder =
      vscode.workspace.workspaceFolders?.length === 1
        ? vscode.workspace.workspaceFolders[0].uri
        : undefined;
    const folderValue =
      inspected?.workspaceValue === undefined &&
      singleFolder &&
      !vscode.workspace.workspaceFile
        ? vscode.workspace
            .getConfiguration("rapidb", singleFolder)
            .inspect?.<StoredConnectionConfig[]>("connections")
            ?.workspaceFolderValue
        : undefined;
    let connections: StoredConnectionConfig[];
    let target: vscode.ConfigurationTarget;
    let resource: vscode.Uri | undefined;

    const workspaceValue = inspected?.workspaceValue ?? folderValue;
    if (workspaceValue !== undefined) {
      connections = workspaceValue;
      target = vscode.ConfigurationTarget.Workspace;
      resource = vscode.workspace.workspaceFile ?? singleFolder;
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
      resource,
      singleFolder:
        target === vscode.ConfigurationTarget.Workspace &&
        vscode.workspace.workspaceFile === undefined,
    };
  }

  private getWriteDomain(snapshot: ConnectionsConfigurationSnapshot) {
    const storage = this.context.globalStorageUri;
    const remote = vscode.env.remoteName !== undefined;
    // Native VS Code exposes its file-backed User storage as vscode-userdata
    // in the desktop host. Do not treat arbitrary providers as local paths.
    if (
      !storage ||
      !isAbsolute(storage.fsPath) ||
      (storage.scheme !== "file" &&
        !(
          storage.scheme === "vscode-userdata" &&
          !remote &&
          vscode.env.uiKind === vscode.UIKind.Desktop
        ))
    )
      return;
    if (snapshot.target === vscode.ConfigurationTarget.Global) {
      // Global is a merged local/remote read in Remote windows. VS Code may
      // direct the write to either machine, without telling the extension.
      if (!remote) return { directory: storage.fsPath, resource: undefined };
      return;
    }
    const resource = snapshot.resource;
    if (!resource || !isAbsolute(resource.fsPath)) return;
    // File URIs in the remote workspace host refer to that host's filesystem.
    // Client-local and virtual URIs must never be keyed as paths on that host.
    if (
      resource.scheme !== "file" ||
      (remote &&
        this.context.extension.extensionKind !== vscode.ExtensionKind.Workspace)
    )
      return;
    return { directory: storage.fsPath, resource };
  }

  canMutateConnections(): boolean {
    return this.getWriteDomain(this.readConnectionsSnapshot()) !== undefined;
  }

  private async withWriteDomain<TResult>(
    operation: (
      snapshot: ConnectionsConfigurationSnapshot,
      fence: ConnectionSettingsFence,
    ) => Promise<TResult>,
  ): Promise<TResult> {
    return this.enqueue(async () => {
      const candidate = this.readConnectionsSnapshot();
      const domain = this.getWriteDomain(candidate);
      if (!domain) {
        throw new Error(
          "RapiDB cannot safely edit connections in this VS Code settings scope. Use the settings editor in the appropriate window instead.",
        );
      }
      const canonical =
        domain.resource &&
        (await canonicalSettingsPath(
          candidate.singleFolder
            ? join(domain.resource.fsPath, ".vscode", "settings.json")
            : domain.resource.fsPath,
        ));
      const identity =
        canonical &&
        (process.platform === "linux" ? canonical : canonical.toLowerCase());
      const key = identity
        ? createHash("sha256").update(identity).digest("hex")
        : "user";
      const directory = join(domain.directory, "connection-settings-v1", key);
      return withConnectionSettingsLock(
        directory,
        async () => this.settingsFileOwner(canonical, directory),
        key === "user" || process.platform !== "linux",
        async (fence) => {
          const snapshot = this.readConnectionsSnapshot();
          this.assertSameTarget(candidate, snapshot);
          fence.assertFresh(snapshot.revision);
          return operation(snapshot, fence);
        },
      );
    });
  }

  // Identifies the settings file behind a lock domain that may cover several
  // files, stably across reloads. realpath() returns the on-disk letter case,
  // so one workspace file has one owner while case variants share the lock.
  //
  // User settings: the token lives in the lock directory and is created under
  // the lock (see readOrCreateStorageOwnerToken), so windows of one profile
  // cannot diverge while globalState is still propagating between them. The
  // lock directory is inside globalStorageUri, which VS Code scopes exactly
  // like globalState (both live in the profile's global storage home). A
  // globalState token that differs from the directory's token belongs to a
  // profile that established its own identity, and is used instead: it never
  // inherits another profile's restored value.
  private async settingsFileOwner(
    canonical: string | undefined,
    directory: string,
  ): Promise<string> {
    if (canonical) {
      return `file:${createHash("sha256").update(canonical).digest("hex")}`;
    }
    const state = this.context.globalState;
    if (!state) return `window:${this.writer}`;
    const existing = state.get<unknown>(SETTINGS_OWNER_STATE_KEY);
    const own =
      typeof existing === "string" && UUID.test(existing)
        ? existing
        : undefined;
    const token = await readOrCreateStorageOwnerToken(directory, own);
    if (own !== undefined) return `profile:${own}`;
    try {
      await state.update(SETTINGS_OWNER_STATE_KEY, token);
    } catch (error) {
      // The directory token stays authoritative for this profile's windows.
      logger.warn(
        `Could not record the connection settings owner in profile state: ${String(error)}`,
      );
    }
    return `profile:${token}`;
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
    const cached = this.normalizedCache;
    const sameSnapshot =
      cached?.revision === snapshot.revision &&
      cached.target === snapshot.target &&
      cached.singleFolder === snapshot.singleFolder &&
      cached.resource?.toString() === snapshot.resource?.toString();
    if (sameSnapshot && cached.repairs.length === 0) return cached;
    const scopeIds = this.readConnectionIdsOutsideScope(snapshot.target, true);
    const scopeIdsKey = JSON.stringify(scopeIds.sort());
    if (sameSnapshot && cached.scopeIdsKey === scopeIdsKey) return cached;
    const storage = this.context.secrets;
    const pending = pendingRepairIds.get(storage);
    const normalized = normalizeStoredConnections(
      snapshot.connections,
      scopeIds,
      pending,
    );
    if (normalized.repairs.length > 0) {
      const reserved = pending ?? new Set<string>();
      for (const { id } of normalized.repairs) reserved.add(id);
      if (!pending) pendingRepairIds.set(storage, reserved);
    }
    this.normalizedCache = {
      ...snapshot,
      ...normalized,
      scopeIdsKey,
      scheduled: false,
    };
    return this.normalizedCache;
  }

  private assertNormalizationCurrent(
    snapshot: ConnectionsConfigurationSnapshot,
    normalized: CachedNormalization,
  ): void {
    this.assertSnapshotCurrent(snapshot);
    if (
      normalized.repairs.length > 0 &&
      JSON.stringify(
        this.readConnectionIdsOutsideScope(snapshot.target, true).sort(),
      ) !== normalized.scopeIdsKey
    ) {
      throw new Error(
        "Connection IDs in other settings scopes changed during the operation. Please retry.",
      );
    }
  }

  private assertSnapshotCurrent(
    snapshot: ConnectionsConfigurationSnapshot,
  ): void {
    const current = this.readConnectionsSnapshot();
    this.assertSameTarget(snapshot, current);
    if (current.revision !== snapshot.revision) {
      throw new Error(
        "Connection settings changed during the operation. Please retry.",
      );
    }
  }

  private assertSameTarget(
    before: ConnectionsConfigurationSnapshot,
    after: ConnectionsConfigurationSnapshot,
  ): void {
    if (
      before.target !== after.target ||
      before.singleFolder !== after.singleFolder ||
      before.resource?.toString() !== after.resource?.toString()
    ) {
      throw new Error(
        "Connection settings changed during the operation (settings scope changed). Please retry.",
      );
    }
  }

  private async writeConnections(
    snapshot: ConnectionsConfigurationSnapshot,
    connections: ConnectionConfig[],
    fence: ConnectionSettingsFence,
    check = () => this.assertSnapshotCurrent(snapshot),
  ): Promise<void> {
    const revision = computeConnectionsRevision(connections);
    if (revision === snapshot.revision) {
      check();
      return;
    }
    const finish = await fence.beginWrite(snapshot.revision, revision);
    try {
      check();
    } catch (error) {
      await finish(false); // No configuration RPC was issued.
      throw error;
    }
    try {
      await vscode.workspace
        .getConfiguration("rapidb")
        .update("connections", connections, snapshot.target);
    } catch (error) {
      let current: ConnectionsConfigurationSnapshot;
      try {
        current = this.readConnectionsSnapshot();
      } catch {
        throw new UnconfirmedConnectionSettingsWrite(error);
      }
      if (
        current.target !== snapshot.target ||
        current.singleFolder !== snapshot.singleFolder ||
        current.resource?.toString() !== snapshot.resource?.toString() ||
        current.revision !== revision
      )
        throw new UnconfirmedConnectionSettingsWrite(error);
    }
    try {
      await finish(true);
    } catch (error) {
      // Settings committed. Do not undo credentials because metadata failed;
      // the pending fence still prevents an overwrite by a stale participant.
      logger.error("Could not confirm the connection settings revision", error);
    }
  }

  private async persistNormalization(
    snapshot: ConnectionsConfigurationSnapshot,
    fence: ConnectionSettingsFence,
  ): Promise<void> {
    const normalized = this.getNormalization(snapshot);
    if (!normalized.changed) return;
    const copies: Array<{ id: string; raw: string }> = [];
    for (const { id, moveSecretFrom } of normalized.repairs) {
      if ((await this.context.secrets.get(id)) !== undefined) {
        // Even an unreferenced Secret Storage entry isn't ours to overwrite
        // or expose. Keep the rejected ID blocked for stale UI snapshots.
        if (this.normalizedCache === normalized)
          this.normalizedCache = undefined;
        throw new Error("The repaired connection ID is in use. Please retry.");
      }
      if (moveSecretFrom === undefined) continue;
      const raw = await this.context.secrets.get(moveSecretFrom);
      if (raw !== undefined) copies.push({ id, raw });
    }
    try {
      this.assertNormalizationCurrent(snapshot, normalized);
      for (const { id, raw } of copies) await this.storeSecret(id, raw);
      await this.writeConnections(snapshot, normalized.connections, fence, () =>
        this.assertNormalizationCurrent(snapshot, normalized),
      );
    } catch (error) {
      // Never delete the source (or a copy that may have been published).
      // A retry must allocate fresh IDs rather than treating a staged key
      // as evidence of ownership. Keep old candidates blocked in this host.
      if (copies.length > 0 && this.normalizedCache === normalized)
        this.normalizedCache = undefined;
      throw error;
    }
    const pending = pendingRepairIds.get(this.context.secrets);
    for (const { id } of normalized.repairs) pending?.delete(id);
    if (this.normalizedCache === normalized) this.normalizedCache = undefined;
    if (normalized.credentialsNotCopied) {
      void Promise.resolve()
        .then(() =>
          vscode.window.showWarningMessage(
            "[RapiDB] Duplicate connection IDs were repaired without copying saved credentials. If an affected profile needs credentials, re-enter them in Edit Connection.",
          ),
        )
        .catch(() => {});
    }
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
    if (
      normalized.changed &&
      !normalized.scheduled &&
      this.getWriteDomain(snapshot)
    ) {
      normalized.scheduled = true;
      void this.withWriteDomain(async (_current, fence) => {
        this.assertSnapshotCurrent(snapshot);
        await this.persistNormalization(snapshot, fence);
      })
        .catch(() => {})
        .finally(() => {
          normalized.scheduled = false;
        });
    }
    return normalized.connections.map((connection) => ({ ...connection }));
  }

  getConnectionsRevision(): string {
    return this.editRevision(this.readConnectionsSnapshot());
  }

  private editRevision(snapshot: ConnectionsConfigurationSnapshot): string {
    return JSON.stringify([
      snapshot.target,
      snapshot.singleFolder,
      snapshot.resource?.toString(),
      snapshot.revision,
    ]);
  }

  async saveConnectionsIfRevision(
    expectedRevision: string,
    connections: ConnectionConfig[],
  ): Promise<boolean> {
    return await this.withWriteDomain(async (snapshot, fence) => {
      if (this.editRevision(snapshot) !== expectedRevision) {
        return false;
      }

      const normalizedRevision = computeConnectionsRevision(
        this.getNormalization(snapshot).connections,
      );
      await this.persistNormalization(snapshot, fence);
      const current = this.readConnectionsSnapshot();
      if (
        current.revision !== normalizedRevision ||
        current.target !== snapshot.target ||
        current.singleFolder !== snapshot.singleFolder ||
        current.resource?.toString() !== snapshot.resource?.toString()
      ) {
        return false;
      }
      await this.writeConnections(current, connections, fence);
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
    return await this.withWriteDomain(async (original, fence) => {
      const expectedRevision = computeConnectionsRevision(
        this.getNormalization(original).connections,
      );
      await this.persistNormalization(original, fence);
      const snapshot = this.readConnectionsSnapshot();
      this.assertSameTarget(original, snapshot);
      if (snapshot.revision !== expectedRevision) {
        throw new Error(
          "Connection settings changed or are still refreshing. Please retry.",
        );
      }
      const normalized = this.getNormalization(snapshot);
      const mutation = await mutate(
        normalized.connections.map((connection) => ({ ...connection })),
      );
      try {
        this.assertSnapshotCurrent(snapshot);
        if (!mutation.connections) {
          await mutation.commitSecretOnly?.();
          return mutation.result;
        }
        await this.writeConnections(snapshot, mutation.connections, fence);
      } catch (writeError) {
        if (mutation.rollback) {
          try {
            await mutation.rollback(
              writeError instanceof UnconfirmedConnectionSettingsWrite,
            );
          } catch (rollbackError) {
            throw new AggregateError(
              [writeError, rollbackError],
              "Connection settings write and rollback both failed.",
            );
          }
        }
        throw writeError;
      }
      this.normalizedCache = undefined;
      await mutation.afterSettingsWrite?.();
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
    return await readConnectionSecret(this.context.secrets, id);
  }

  async storeSecret(id: string, value: string): Promise<void> {
    await this.context.secrets.store(id, value);
  }

  async deleteSecret(id: string): Promise<void> {
    await this.context.secrets.delete(id);
  }

  onDidChangeSecret(
    listener: (id: string) => void,
  ): vscode.Disposable | undefined {
    return this.context.secrets.onDidChange?.(({ key }) => listener(key));
  }

  private readConnectionIdsOutsideScope(
    target: vscode.ConfigurationTarget,
    includeBlank = false,
  ): string[] {
    const singleFolder = vscode.workspace.workspaceFile === undefined;
    const sharesWorkspaceFile =
      singleFolder && target !== vscode.ConfigurationTarget.Global;
    const otherScopes = this.readConnectionScopeEntries()
      .filter((entry) =>
        entry.scope === vscode.ConfigurationTarget.Global
          ? target !== entry.scope
          : !(sharesWorkspaceFile || (!entry.folder && target === entry.scope)),
      )
      .map((entry) => entry.value);
    return collectConnectionIds(otherScopes, includeBlank);
  }

  private readConnectionScopeEntries(): Array<{
    scope: vscode.ConfigurationTarget;
    value: unknown;
    folder?: boolean;
  }> {
    const entries: Array<{
      scope: vscode.ConfigurationTarget;
      value: unknown;
      folder?: boolean;
    }> = [];
    const configuration = vscode.workspace.getConfiguration("rapidb");
    if (typeof configuration.inspect === "function") {
      const inspected = configuration.inspect("connections");
      entries.push(
        {
          scope: vscode.ConfigurationTarget.Global,
          value: inspected?.globalValue,
        },
        {
          scope: vscode.ConfigurationTarget.Workspace,
          value: inspected?.workspaceValue,
        },
        {
          scope: vscode.ConfigurationTarget.WorkspaceFolder,
          value: inspected?.workspaceFolderValue,
        },
      );
    }
    for (const folder of vscode.workspace.workspaceFolders ?? []) {
      const folderConfiguration = vscode.workspace.getConfiguration(
        "rapidb",
        folder.uri,
      );
      if (typeof folderConfiguration.inspect === "function") {
        entries.push({
          scope: vscode.ConfigurationTarget.WorkspaceFolder,
          value:
            folderConfiguration.inspect("connections")?.workspaceFolderValue,
          folder: true,
        });
      }
    }
    return entries;
  }

  readConnectionSessionNonce(id: string): string | undefined {
    const nonce = this.readSessionNonces()[id];
    return typeof nonce === "string" ? nonce : undefined;
  }

  async writeConnectionSessionNonce(id: string, nonce: string): Promise<void> {
    await this.context.globalState.update(SESSION_NONCES_STATE_KEY, {
      ...this.readSessionNonces(),
      [id]: nonce,
    });
  }

  readDynamoDBTarget(id: string): DynamoDBTargetRecord | undefined {
    const value = this.context.globalState.get<Partial<DynamoDBTargetRecord>>(
      `${DYNAMODB_TARGET_STATE_PREFIX}${id}`,
    );
    if (
      value &&
      typeof value.settingsKey === "string" &&
      /^[a-f0-9]{64}$/.test(value.settingsKey) &&
      (value.fingerprint === null ||
        (typeof value.fingerprint === "string" &&
          /^[a-f0-9]{64}$/.test(value.fingerprint)))
    ) {
      return { settingsKey: value.settingsKey, fingerprint: value.fingerprint };
    }
  }

  async writeDynamoDBTarget(
    id: string,
    target: DynamoDBTargetRecord,
  ): Promise<void> {
    await this.context.globalState.update(
      `${DYNAMODB_TARGET_STATE_PREFIX}${id}`,
      target,
    );
  }

  private readSessionNonces(): Record<string, unknown> {
    const nonces = this.context.globalState.get<unknown>(
      SESSION_NONCES_STATE_KEY,
    );
    return nonces && typeof nonces === "object" && !Array.isArray(nonces)
      ? (nonces as Record<string, unknown>)
      : {};
  }

  getHistoryLimit(): number {
    const raw = vscode.workspace
      .getConfiguration("rapidb")
      .get<number>("queryHistoryLimit", HISTORY_LIMIT_DEFAULT);
    const value = finiteOr(raw, HISTORY_LIMIT_DEFAULT);
    return Math.max(0, Math.min(HISTORY_LIMIT_MAX, Math.round(value)));
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
    const value = finiteOr(raw, 1000);
    return Math.max(
      10,
      Math.min(QUERY_LIMIT_POLICY.hardCap, Math.round(value)),
    );
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
