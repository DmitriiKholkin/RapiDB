import assert from "node:assert/strict";
import * as net from "node:net";
import { setTimeout as delay } from "node:timers/promises";
import { ConnectionManager } from "../../src/extension/connectionManager";
import type { IDBDriver } from "../../src/extension/dbDrivers/types";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";
import {
  createExtensionContextStub,
  FakeConnectionManagerStore,
} from "./fakeConnectionManagerStore";

export const TEST_SSH_BASTION = {
  host: "127.0.0.1",
  port: 2222,
  username: "rapidb_ssh",
  password: "ssh_pass123",
} as const;

export interface ManagedLiveDriverSession {
  manager: ConnectionManager;
  store: FakeConnectionManagerStore;
  driver: IDBDriver;
}

export async function waitForSshBastionReady(
  timeoutMs = 60_000,
  retryDelayMs = 1_000,
): Promise<void> {
  const startedAt = Date.now();
  let lastError = "No connection attempts were made.";

  while (Date.now() - startedAt < timeoutMs) {
    try {
      await new Promise<void>((resolve, reject) => {
        const socket = net.createConnection({
          host: TEST_SSH_BASTION.host,
          port: TEST_SSH_BASTION.port,
        });

        socket.once("connect", () => {
          socket.end();
          resolve();
        });
        socket.once("error", reject);
      });
      return;
    } catch (error) {
      lastError = error instanceof Error ? error.message : String(error);
      await delay(retryDelayMs);
    }
  }

  throw new Error(
    `[RapiDB:ssh-live] SSH bastion did not become ready within ${timeoutMs}ms. Last error: ${lastError}`,
  );
}

export async function connectLiveDriverViaManager(
  connection: ConnectionConfig,
): Promise<ManagedLiveDriverSession> {
  await waitForSshBastionReady();

  const store = new FakeConnectionManagerStore();
  const manager = new ConnectionManager(
    createExtensionContextStub() as never,
    store,
  );

  await manager.saveConnection(connection);
  await manager.connectTo(connection.id);

  const driver = manager.getDriver(connection.id);
  assert(driver, `Expected an active driver for ${connection.id}.`);

  const storedConnection = store
    .getConnections()
    .find((candidate) => candidate.id === connection.id);
  assert.match(
    storedConnection?.sshHostFingerprintSha256 ?? "",
    /^SHA256:/,
    `Expected TOFU fingerprint pinning for ${connection.id}.`,
  );

  return {
    manager,
    store,
    driver,
  };
}

export async function disposeManagedLiveDriverSession(
  session: ManagedLiveDriverSession | undefined,
): Promise<void> {
  await session?.manager.dispose();
}

export function withTrustOnFirstUseSsh(
  connection: ConnectionConfig,
): ConnectionConfig {
  return {
    ...connection,
    sshEnabled: true,
    sshHost: TEST_SSH_BASTION.host,
    sshPort: TEST_SSH_BASTION.port,
    sshUsername: TEST_SSH_BASTION.username,
    sshAuthMethod: "password",
    sshPassword: TEST_SSH_BASTION.password,
    sshHostVerificationMode: "trustOnFirstUse",
    sshHostFingerprintSha256: undefined,
    useSecretStorage: false,
  };
}
