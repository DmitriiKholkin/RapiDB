import { describe, expect, it, vi } from "vitest";
import { ConnectionManager } from "../../../src/extension/connectionManager";
import { PostgresDriver } from "../../../src/extension/dbDrivers/postgres";
import type { IDBDriver } from "../../../src/extension/dbDrivers/types";
import type { ConnectionConfig } from "../../../src/shared/connectionConfig";
import {
  createExtensionContextStub,
  FakeConnectionManagerStore,
} from "../../support/fakeConnectionManagerStore";
import { rowsFromQuery } from "../../support/liveDbHarness";
import { TEST_SSH_BASTION } from "../../support/liveSshManagerHarness";

vi.mock(
  "vscode",
  async () =>
    (await import("../../support/mockVscode")).createMockVscodeModule().module,
);

// Opt-in fixture: PostgreSQL listening on 55432 with a TLS cert for db.internal,
// available at db.internal from the test bastion and published on loopback.
// Published ports and the isolated server's CA must be supplied explicitly.
const remotePort = 55432;
const directPort = Number(process.env.RAPIDB_PG_PORT_FIXTURE);
const sshPort = Number(process.env.RAPIDB_PG_PORT_FIXTURE_SSH);
const caFilePath = process.env.RAPIDB_PG_PORT_FIXTURE_CA;
const config = {
  id: "pg-live-port-inheritance",
  name: "PostgreSQL isolated port fixture",
  type: "pg",
  host: "127.0.0.1",
  database: "postgres",
  username: "port_fixture",
  password: "port_fixture",
  useSecretStorage: false,
} satisfies ConnectionConfig;

async function expectServer(
  driver: IDBDriver,
  tls: boolean,
  database?: string,
) {
  const result = await driver.query(
    `SELECT inet_server_port() AS port, ssl
     FROM pg_stat_ssl WHERE pid = pg_backend_pid()`,
    undefined,
    { database },
  );
  expect(rowsFromQuery(result)).toEqual([{ port: remotePort, ssl: tls }]);
}

describe.skipIf(!directPort || !sshPort || !caFilePath)(
  "PostgreSQL effective port (isolated live fixture)",
  () => {
    it("inherits PGPORT without TLS and keeps it for new pools in the session", async () => {
      vi.stubEnv("PGPORT", String(directPort));
      const driver = new PostgresDriver({
        ...config,
        tls: { mode: "disabled" },
      });
      try {
        await driver.connect();
        await expectServer(driver, false);
        vi.stubEnv("PGPORT", "invalid");
        await expectServer(driver, false, "template1");
      } finally {
        await driver.disconnect();
      }
    });

    it.each([
      false,
      true,
    ])("uses the selected port with full TLS verification and a name override (explicit=%s)", async (explicit) => {
      vi.stubEnv("PGPORT", explicit ? "invalid" : String(directPort));
      const driver = new PostgresDriver({
        ...config,
        port: explicit ? directPort : undefined,
        tls: {
          mode: "requireVerifyFull",
          serverNameOverride: "db.internal",
          caFilePath,
        },
      });
      try {
        await driver.connect();
        await expectServer(driver, true);
      } finally {
        await driver.disconnect();
      }
    });

    it("inherits the remote port through the real SSH bastion in Test and Connect", async () => {
      vi.stubEnv("PGPORT", String(remotePort));
      const sshConfig: ConnectionConfig = {
        ...config,
        host: "db.internal",
        ssh: {
          ...TEST_SSH_BASTION,
          port: sshPort,
          authMethod: "password",
          hostVerificationMode: "trustOnFirstUse",
        },
        tls: { mode: "requireVerifyFull", caFilePath },
      };
      const store = new FakeConnectionManagerStore();
      const manager = new ConnectionManager(
        createExtensionContextStub() as never,
        store,
      );
      try {
        await manager.saveConnection(sshConfig);
        expect(await manager.testConnection(sshConfig)).toEqual({
          success: true,
        });
        expect(
          manager.getConnection(config.id)?.ssh?.hostFingerprintSha256,
        ).toBeUndefined();
        await manager.connectTo(config.id);
        const driver = manager.getDriver(config.id);
        if (!driver) throw new Error("No connected PostgreSQL driver");
        await expectServer(driver, true);
        expect(manager.getConnection(config.id)?.port).toBeUndefined();
      } finally {
        await manager.dispose();
      }
    });
  },
);
