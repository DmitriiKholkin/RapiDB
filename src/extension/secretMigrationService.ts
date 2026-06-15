/**
 * Secret Migration Service — инкапсулирует логику миграции и сохранения секретов.
 *
 * Извлечен из ConnectionManager для соблюдения SRP.
 * Управляет секретами подключений (пароли, SSH-ключи, API-ключи)
 * и их миграцией в VS Code SecretStorage.
 */

import type { ConnectionConfig } from "./connectionManagerModels";
import type { ConnectionManagerStore } from "./connectionManagerStore";
import {
  hasPersistedConnectionConfigChanges,
  sanitizePersistedConnectionConfig,
  serializeConnectionSecretsForConfig,
  shouldForceSecretStorage,
} from "./connectionSecrets";
import {
  parseStoredConnectionSecrets,
  type StoredConnectionSecrets,
} from "./connectionSecretsData";
import { pMapWithLimitAllSettled } from "./utils/concurrency";
import { logErrorWithContext } from "./utils/errorHandling";

/**
 * Concurrency limit for per-connection secret migrations.
 * SecretStorage backends (e.g. gnome-keyring on Linux) can be slow
 * per-call, so doing all migrations serially blocks extension
 * activation for tens of seconds when many connections exist.
 */
const SECRET_MIGRATION_CONCURRENCY = 4;

export interface SecretMigrationServiceDependencies {
  getConnections(): ConnectionConfig[];
  saveConnections(
    connections: ConnectionConfig[],
    options?: {
      expectedRevision?: string;
      skipIfRevisionMismatch?: boolean;
    },
  ): Promise<boolean>;
  onDidChangeConnections(): void;
}

export class SecretMigrationService {
  private pendingMigration: Promise<void> | null = null;

  constructor(
    private readonly store: ConnectionManagerStore,
    private readonly dependencies: SecretMigrationServiceDependencies,
  ) {}

  /**
   * Parse secrets stored in VS Code SecretStorage.
   */
  parseStoredSecrets(value: string | undefined): StoredConnectionSecrets {
    return parseStoredConnectionSecrets(value);
  }

  /**
   * Persists connection secrets to SecretStorage if needed.
   */
  async persistConnectionSecretsIfNeeded(
    config: ConnectionConfig,
  ): Promise<void> {
    if (!shouldForceSecretStorage(config)) {
      return;
    }

    const previousSecretSnapshot = await this.store.getSecret(config.id);
    const serializedSecrets = serializeConnectionSecretsForConfig(
      config,
      this.parseStoredSecrets(previousSecretSnapshot),
    );

    if (serializedSecrets === previousSecretSnapshot) {
      return;
    }

    if (!serializedSecrets) {
      await this.store.deleteSecret(config.id);
      return;
    }

    await this.store.storeSecret(config.id, serializedSecrets);
  }

  /**
   * Migrates secrets for a single connection.
   */
  async migrateSingleConnectionSecretsIfNeeded(
    config: ConnectionConfig,
  ): Promise<void> {
    if (!shouldForceSecretStorage(config)) {
      return;
    }

    const persisted = sanitizePersistedConnectionConfig(config);
    const needsPersistedConfigUpdate = hasPersistedConnectionConfigChanges(
      persisted,
      config,
    );

    await this.persistConnectionSecretsIfNeeded(config);

    if (!needsPersistedConfigUpdate) {
      return;
    }

    const expectedRevision = this.store.getConnectionsRevision();
    const storedConnections = this.dependencies.getConnections();
    const index = storedConnections.findIndex(
      (connection) => connection.id === config.id,
    );
    if (index < 0) {
      return;
    }

    storedConnections[index] = persisted;
    await this.dependencies.saveConnections(storedConnections, {
      expectedRevision,
      skipIfRevisionMismatch: true,
    });
  }

  /**
   * Migrates secrets for all stored connections.
   *
   * Per-connection SecretStorage I/O is parallelized with a bounded
   * concurrency to avoid blocking extension activation when many
   * connections exist. Errors are isolated per-connection so a single
   * failure does not abort the rest of the migration (preserving the
   * pre-refactor best-effort semantics).
   */
  async migrateAllStoredConnectionSecrets(): Promise<void> {
    const expectedRevision = this.store.getConnectionsRevision();
    const storedConnections = this.dependencies.getConnections();

    // Only candidates that actually need migration are worth scheduling.
    const candidates = storedConnections.filter((connection) =>
      shouldForceSecretStorage(connection),
    );

    if (candidates.length === 0) {
      return;
    }

    const results = await pMapWithLimitAllSettled(
      candidates,
      SECRET_MIGRATION_CONCURRENCY,
      async (connection) => {
        await this.persistConnectionSecretsIfNeeded(connection);
        const persisted = sanitizePersistedConnectionConfig(connection);
        return hasPersistedConnectionConfigChanges(persisted, connection)
          ? persisted
          : undefined;
      },
    );

    let hasChanges = false;
    results.forEach((result, index) => {
      if (result instanceof Error) {
        // Per-connection migration failures should not abort the batch.
        // Logged here so incident triage has a record, but the rest
        // of the migration continues.
        logErrorWithContext(
          `Secret migration failed for connection "${candidates[index]?.id ?? "unknown"}"`,
          result,
        );
        return;
      }
      if (result === undefined) {
        return;
      }
      const candidate = candidates[index];
      const persisted = result;
      const targetIndex = storedConnections.findIndex(
        (item) => item.id === candidate.id,
      );
      if (targetIndex >= 0) {
        storedConnections[targetIndex] = persisted;
        hasChanges = true;
      }
    });

    if (hasChanges) {
      const persisted = await this.dependencies.saveConnections(
        storedConnections,
        {
          expectedRevision,
          skipIfRevisionMismatch: true,
        },
      );
      if (persisted) {
        this.dependencies.onDidChangeConnections();
      }
    }
  }

  /**
   * Schedules async migration of all connection secrets.
   */
  scheduleSecretMigration(): void {
    if (this.pendingMigration) {
      return;
    }

    const pending = Promise.resolve()
      .then(() => this.migrateAllStoredConnectionSecrets())
      .catch(() => undefined)
      .finally(() => {
        this.pendingMigration = null;
      });

    this.pendingMigration = pending;
  }

  /**
   * Hydrates a connection config with secrets from SecretStorage.
   */
  async hydratePassword(config: ConnectionConfig): Promise<ConnectionConfig> {
    if (!config.useSecretStorage) {
      return config;
    }
    try {
      const stored = await this.store.getSecret(config.id);
      const secrets = this.parseStoredSecrets(stored);
      const ssh = config.ssh
        ? {
            ...config.ssh,
            password: secrets.sshPassword ?? config.ssh.password,
            privateKey: secrets.sshPrivateKey ?? config.ssh.privateKey,
            passphrase: secrets.sshPassphrase ?? config.ssh.passphrase,
          }
        : undefined;
      return {
        ...config,
        password: secrets.password ?? config.password ?? "",
        apiKey: secrets.apiKey ?? config.apiKey,
        awsAccessKeyId: secrets.awsAccessKeyId ?? config.awsAccessKeyId,
        awsSecretAccessKey:
          secrets.awsSecretAccessKey ?? config.awsSecretAccessKey,
        awsSessionToken: secrets.awsSessionToken ?? config.awsSessionToken,
        connectionUri: secrets.connectionUri ?? config.connectionUri,
        uri: secrets.uri ?? config.uri,
        endpoint: secrets.endpoint ?? config.endpoint,
        awsEndpoint: secrets.awsEndpoint ?? config.awsEndpoint,
        ssh,
        tls:
          config.tls !== undefined
            ? {
                ...config.tls,
                keyPassphrase:
                  secrets.tlsKeyPassphrase ?? config.tls.keyPassphrase,
              }
            : config.tls,
      };
    } catch {
      return { ...config, password: "" };
    }
  }
}
