import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";
import type { ConnectionFormSubmission } from "../../src/shared/webviewContracts/connection";
import { ConnectionFormView } from "../../src/webview/components/ConnectionFormView";
import {
  clearPostedMessages,
  dispatchIncomingMessage,
  expectNoAxeViolations,
  getLastPostedMessage,
} from "./testUtils";

async function submitCreateConnection(
  user: ReturnType<typeof userEvent.setup>,
) {
  await user.click(screen.getByRole("button", { name: "Create Connection" }));
}

async function fillRequiredPostgresFields(
  user: ReturnType<typeof userEvent.setup>,
) {
  await user.type(screen.getByLabelText("Database"), "app");
  await user.type(screen.getByLabelText("Username"), "reader");
}

describe("ConnectionFormView", () => {
  it.each([
    "Port",
    "SSH port",
  ])("blocks invalid %s on both Test and Save, and allows correction", async (label) => {
    const user = userEvent.setup();
    render(<ConnectionFormView existing={null} />);
    await user.type(screen.getByLabelText("Connection name"), "Ports");
    await fillRequiredPostgresFields(user);
    if (label === "SSH port") {
      await user.click(
        screen.getByRole("switch", { name: /connect through ssh bastion/i }),
      );
      await user.type(screen.getByLabelText("SSH host"), "bastion.local");
      await user.type(screen.getByLabelText("SSH username"), "tunnel");
      await user.selectOptions(
        screen.getByLabelText("SSH auth method"),
        "password",
      );
      await user.type(screen.getByLabelText("SSH password"), "ssh-secret");
      await user.selectOptions(
        screen.getByLabelText("SSH host verification mode"),
        "trustOnFirstUse",
      );
    }
    const input = screen.getByLabelText(label);
    for (const value of [
      "abc",
      "0",
      "-1",
      "1.5",
      "65536",
      "70000",
      "Infinity",
      "1e999",
      "22abc",
    ]) {
      await user.clear(input);
      await user.type(input, value);
      for (const name of [/test connection/i, /create connection/i]) {
        clearPostedMessages();
        await user.click(screen.getByRole("button", { name }));
        expect(getLastPostedMessage()).toBeUndefined();
        expect(
          screen.getByText(
            `${label === "Port" ? "Database" : "SSH"} port must be an integer between 1 and 65535.`,
            { exact: false },
          ),
        ).toBeTruthy();
        expect((input as HTMLInputElement).value).toBe(value);
      }
    }
    await user.clear(input);
    await user.type(input, "65535");
    await user.click(screen.getByRole("button", { name: /test connection/i }));
    expect(getLastPostedMessage()).toMatchObject({
      type: "testConnection",
      payload: label === "Port" ? { port: 65535 } : { ssh: { port: 65535 } },
    });
  });

  it.each([
    "",
    "   ",
    "1",
    "65535",
  ])("uses defaults only for empty port fields: %j", async (value) => {
    const user = userEvent.setup();
    render(<ConnectionFormView existing={null} />);
    await user.type(screen.getByLabelText("Connection name"), "Ports");
    await fillRequiredPostgresFields(user);
    await user.click(
      screen.getByRole("switch", { name: /connect through ssh bastion/i }),
    );
    await user.type(screen.getByLabelText("SSH host"), "bastion.local");
    await user.type(screen.getByLabelText("SSH username"), "tunnel");
    await user.selectOptions(
      screen.getByLabelText("SSH auth method"),
      "password",
    );
    await user.type(screen.getByLabelText("SSH password"), "ssh-secret");
    await user.selectOptions(
      screen.getByLabelText("SSH host verification mode"),
      "trustOnFirstUse",
    );
    for (const label of ["Port", "SSH port"]) {
      await user.clear(screen.getByLabelText(label));
      if (value) await user.type(screen.getByLabelText(label), value);
    }
    await submitCreateConnection(user);
    expect(getLastPostedMessage()).toMatchObject({
      type: "saveConnection",
      payload: {
        port: value.trim() ? Number(value) : 5432,
        ssh: { port: value.trim() ? Number(value) : 22 },
      },
    });
  });

  it("omits an unused host port when MongoDB URI is supplied", async () => {
    const user = userEvent.setup();
    render(<ConnectionFormView existing={null} />);
    await user.type(screen.getByLabelText("Connection name"), "Mongo");
    await user.click(screen.getByRole("button", { name: /mongodb/i }));
    await user.clear(screen.getByLabelText("Port"));
    await user.type(screen.getByLabelText("Port"), "invalid");
    await user.selectOptions(
      screen.getByLabelText("MongoDB connection method"),
      "uri",
    );
    await user.type(
      screen.getByLabelText("MongoDB connection URI"),
      "mongodb://host",
    );
    await submitCreateConnection(user);
    expect(getLastPostedMessage()).toMatchObject({
      type: "saveConnection",
      payload: { connectionUri: "mongodb://host", port: undefined },
    });
  });

  it("shows TLS modes only for supported drivers and reveals mutual TLS fields", async () => {
    const user = userEvent.setup();

    render(<ConnectionFormView existing={null} />);

    expect(screen.getByLabelText("TLS mode")).toBeTruthy();
    expect(
      screen.getByRole("option", { name: /required, verify ca only/i }),
    ).toBeTruthy();
    expect(screen.getByRole("option", { name: /mutual tls/i })).toBeTruthy();

    await user.selectOptions(screen.getByLabelText("TLS mode"), ["mutualTls"]);

    expect(screen.getByLabelText("TLS client certificate file")).toBeTruthy();
    expect(screen.getByLabelText("TLS client key file")).toBeTruthy();
    expect(screen.getByLabelText("TLS client key passphrase")).toBeTruthy();
    expect(screen.getByLabelText("TLS server name override")).toBeTruthy();

    await user.selectOptions(screen.getByLabelText("TLS mode"), [
      "requireVerifyCa",
    ]);
    expect(screen.getByLabelText("TLS CA certificate file")).toBeTruthy();
    expect(screen.getByLabelText("TLS server name override")).toBeTruthy();

    await user.selectOptions(screen.getByLabelText("TLS mode"), [
      "requireTrustServerCertificate",
    ]);
    expect(screen.queryByLabelText("TLS CA certificate file")).toBeNull();
    expect(screen.queryByLabelText("TLS server name override")).toBeNull();

    await user.click(screen.getByRole("button", { name: /sql server/i }));

    expect(screen.getByLabelText("TLS mode")).toBeTruthy();
    expect(
      screen.queryByRole("option", { name: /required, verify ca only/i }),
    ).toBeNull();
    expect(screen.queryByRole("option", { name: /mutual tls/i })).toBeNull();

    await user.click(screen.getByRole("button", { name: /oracle/i }));

    expect(screen.queryByLabelText("TLS mode")).toBeNull();
  });

  it("shows connection-type-specific fields", async () => {
    const user = userEvent.setup();

    render(<ConnectionFormView existing={null} />);

    expect(
      screen.getByRole("button", { name: /mysql \/ mariadb/i }),
    ).toBeTruthy();
    expect(screen.getByLabelText("Host")).toBeTruthy();
    expect(screen.queryByLabelText("Database file path")).toBeNull();

    await user.click(screen.getByRole("button", { name: /sqlite/i }));

    expect(screen.getByLabelText("Database file path")).toBeTruthy();
    expect(screen.getByLabelText("SQLite WAL mode")).toBeTruthy();
    expect(
      screen.getByText(
        /wal is enabled automatically for writable sqlite connections unless you disable it here/i,
      ),
    ).toBeTruthy();
    expect(screen.queryByLabelText("Host")).toBeNull();

    await user.click(screen.getByRole("button", { name: /oracle/i }));

    expect(screen.getByLabelText("Oracle service name")).toBeTruthy();
    expect(screen.queryByLabelText("Oracle Instant Client path")).toBeNull();

    await user.click(screen.getByRole("button", { name: /mongodb/i }));

    expect(screen.getByLabelText("MongoDB connection method")).toBeTruthy();
    expect(screen.getByLabelText("MongoDB URI preview")).toBeTruthy();
    expect(screen.getByLabelText("Database")).toBeTruthy();

    await user.click(screen.getByRole("button", { name: /redis/i }));

    expect(screen.getByLabelText("Username")).toBeTruthy();
    expect(screen.getByLabelText("Database")).toBeTruthy();

    await user.click(screen.getByRole("button", { name: /elasticsearch/i }));

    expect(screen.getByLabelText("Elasticsearch endpoint")).toBeTruthy();
    expect(screen.getByLabelText("Elasticsearch API key")).toBeTruthy();
    expect(screen.getByLabelText("Elasticsearch cloud id")).toBeTruthy();
    expect(
      screen
        .getByRole("switch", {
          name: /store secrets in vs code secret storage/i,
        })
        .getAttribute("aria-disabled"),
    ).toBe("true");

    await user.click(screen.getByRole("button", { name: /dynamodb/i }));

    expect(screen.getByLabelText("AWS region")).toBeTruthy();
    expect(screen.getByLabelText("AWS profile")).toBeTruthy();
    expect(screen.getByLabelText("DynamoDB endpoint")).toBeTruthy();
  });

  it.each([
    ["pg", /postgresql/i, 5432],
    ["mysql", /mysql \/ mariadb/i, 3306],
    ["mssql", /sql server/i, 1433],
  ] as const)("submits the network fields and default port for %s", async (type, label, defaultPort) => {
    const user = userEvent.setup();
    render(<ConnectionFormView existing={null} />);
    await user.click(screen.getByRole("button", { name: label }));
    await user.type(screen.getByLabelText("Connection name"), `${type} QA`);
    await user.clear(screen.getByLabelText("Host"));
    await user.type(screen.getByLabelText("Host"), `${type}.db.local`);
    await user.type(screen.getByLabelText("Database"), "app_db");
    await user.type(screen.getByLabelText("Username"), "app_user");
    await user.type(screen.getByLabelText("Password"), "secret");

    await submitCreateConnection(user);

    expect(getLastPostedMessage()).toMatchObject({
      type: "saveConnection",
      payload: {
        type,
        host: `${type}.db.local`,
        port: defaultPort,
        database: "app_db",
        username: "app_user",
        password: "secret",
      },
    });
  });

  it("submits Oracle service name as its database target", async () => {
    const user = userEvent.setup();
    render(<ConnectionFormView existing={null} />);
    await user.click(screen.getByRole("button", { name: /oracle/i }));
    await user.type(screen.getByLabelText("Connection name"), "Oracle QA");
    await user.clear(screen.getByLabelText("Host"));
    await user.type(screen.getByLabelText("Host"), "oracle.db.local");
    await user.type(screen.getByLabelText("Oracle service name"), "FREEPDB1");

    await submitCreateConnection(user);

    expect(getLastPostedMessage()).toMatchObject({
      type: "saveConnection",
      payload: {
        type: "oracle",
        host: "oracle.db.local",
        port: 1521,
        serviceName: "FREEPDB1",
      },
    });
    expect(getLastPostedMessage()?.payload).toHaveProperty("database", "");
  });

  it("routes SQLite Browse results back into the path field and save payload", async () => {
    const user = userEvent.setup();
    render(<ConnectionFormView existing={null} />);
    await user.click(screen.getByRole("button", { name: /sqlite/i }));
    await user.type(screen.getByLabelText("Connection name"), "SQLite QA");
    await user.click(screen.getByRole("button", { name: /browse/i }));

    expect(getLastPostedMessage()).toEqual({
      type: "browseFile",
      payload: { target: "filePath" },
    });

    dispatchIncomingMessage("browseFileResult", {
      target: "filePath",
      filePath: "/tmp/rapidb-form-check.sqlite",
    });
    await submitCreateConnection(user);

    expect(getLastPostedMessage()).toMatchObject({
      type: "saveConnection",
      payload: {
        type: "sqlite",
        filePath: "/tmp/rapidb-form-check.sqlite",
        sqliteWalMode: "auto",
      },
    });
  });

  it("renders SSH controls conditionally, forces Secret Storage, and hides SSH for SQLite", async () => {
    const user = userEvent.setup();

    const { container } = render(<ConnectionFormView existing={null} />);

    const sshToggle = screen.getByRole("switch", {
      name: /connect through ssh bastion/i,
    });

    expect(sshToggle.getAttribute("aria-checked")).toBe("false");
    expect(screen.queryByLabelText("SSH host")).toBeNull();

    await user.click(sshToggle);

    expect(sshToggle.getAttribute("aria-checked")).toBe("true");
    expect(screen.getByLabelText("SSH host")).toBeTruthy();
    expect(screen.getByLabelText("SSH port")).toBeTruthy();
    expect(screen.getByLabelText("SSH username")).toBeTruthy();
    expect(screen.getByLabelText("SSH auth method")).toBeTruthy();
    expect(screen.getByLabelText("SSH host verification mode")).toBeTruthy();
    expect(
      (screen.getByLabelText("SSH host verification mode") as HTMLSelectElement)
        .value,
    ).toBe("manual");
    expect(screen.getByLabelText("SSH private key")).toBeTruthy();
    expect(screen.getByLabelText("SSH passphrase")).toBeTruthy();
    expect(screen.getByLabelText("SSH host fingerprint SHA256")).toBeTruthy();

    await user.selectOptions(
      screen.getByLabelText("SSH host verification mode"),
      ["trustOnFirstUse"],
    );

    expect(screen.queryByLabelText("SSH host fingerprint SHA256")).toBeNull();
    expect(
      screen.getByText(
        /first successful database connection through ssh will pin the discovered sha256 fingerprint automatically.*test connection does not save a pin/i,
      ),
    ).toBeTruthy();

    await user.selectOptions(
      screen.getByLabelText("SSH host verification mode"),
      ["manual"],
    );

    await user.selectOptions(screen.getByLabelText("SSH auth method"), [
      "password",
    ]);

    expect(screen.getByLabelText("SSH password")).toBeTruthy();
    expect(screen.queryByLabelText("SSH private key")).toBeNull();
    expect(screen.queryByLabelText("SSH passphrase")).toBeNull();

    const secretStorageToggle = screen.getByRole("switch", {
      name: /store secrets in vs code secret storage/i,
    });
    expect(secretStorageToggle.getAttribute("aria-disabled")).toBe("true");
    expect(secretStorageToggle.getAttribute("aria-checked")).toBe("true");

    await user.click(screen.getByRole("button", { name: /sqlite/i }));

    expect(
      screen.queryByRole("switch", {
        name: /connect through ssh bastion/i,
      }),
    ).toBeNull();
    expect(screen.queryByLabelText("SSH host")).toBeNull();

    await user.click(screen.getByRole("button", { name: /mongodb/i }));

    expect(
      screen.getByText(/single-host direct connections in v1/i),
    ).toBeTruthy();

    await expectNoAxeViolations(container);
  });

  it("associates the read-only toggle with its explanatory hint", () => {
    render(<ConnectionFormView existing={null} />);

    const toggle = screen.getByRole("switch", {
      name: /open connection as read-only/i,
    });
    const hint = screen.getByText(/Blocks table edits/i);

    expect(toggle.getAttribute("aria-describedby")).toBeTruthy();
    expect(toggle.getAttribute("aria-describedby")).toBe(
      hint.getAttribute("id"),
    );
    expect(hint.textContent).toContain(
      "limits query editor execution to read operations",
    );
    expect(hint.textContent).toContain("does not replace database permissions");
    expect(hint.textContent).not.toContain("Only read queries can be executed");
  });

  it.each([
    "SQL Server",
    "Oracle",
  ])("explains the read-only %s editor exception without disabling browsing", async (databaseType) => {
    const user = userEvent.setup();
    render(<ConnectionFormView existing={null} />);

    await user.click(
      screen.getByRole("button", { name: new RegExp(databaseType, "i") }),
    );

    const toggle = screen.getByRole("switch", {
      name: /open connection as read-only/i,
    });
    const hint = screen.getByText(
      /blocks table edits and query editor execution/i,
    );
    expect(toggle.getAttribute("aria-describedby")).toBe(
      hint.getAttribute("id"),
    );
    expect(hint.textContent).toContain(
      `query editor execution for ${databaseType}`,
    );
    expect(hint.textContent).toContain(
      "Table data and schema browsing remain available",
    );
    expect(hint.textContent).toContain(
      "database-enforced read-only permissions",
    );
    expect(hint.textContent).toContain("restrictions on executable routines");
    expect(hint.textContent).toContain("with a regular connection");
    expect(hint.textContent).toContain("does not replace database permissions");
    expect(hint.textContent).toContain(
      "may have side effects even during reads",
    );
    expect(hint.textContent).not.toContain("Only read queries can be executed");
    if (databaseType === "Oracle") {
      expect(hint.textContent).toContain(
        "Autonomous transactions can commit writes despite SET TRANSACTION READ ONLY and caller rollback",
      );
    }

    await user.click(toggle);
    expect(toggle.getAttribute("aria-checked")).toBe("true");
    await user.type(
      screen.getByLabelText("Connection name"),
      "Readonly browsing",
    );
    if (databaseType === "Oracle") {
      await user.type(screen.getByLabelText("Oracle service name"), "XEPDB1");
    } else {
      await user.type(screen.getByLabelText("Database"), "app");
    }
    await submitCreateConnection(user);
    expect(getLastPostedMessage()).toMatchObject({
      type: "saveConnection",
      payload: {
        type: databaseType === "Oracle" ? "oracle" : "mssql",
        readOnly: true,
      },
    });
  });

  it("updates the read-only hint when switching away from Oracle", async () => {
    const user = userEvent.setup();
    render(<ConnectionFormView existing={null} />);
    await user.click(screen.getByRole("button", { name: /oracle/i }));
    expect(
      screen.getByText(/Autonomous transactions can commit writes/i),
    ).toBeTruthy();

    await user.click(screen.getByRole("button", { name: /postgresql/i }));
    expect(
      screen.queryByText(/Autonomous transactions can commit writes/i),
    ).toBeNull();
    expect(
      screen.queryByText(/blocks table edits and query editor execution/i),
    ).toBeNull();
    expect(
      screen.getByText(/limits query editor execution to read operations/i),
    ).toBeTruthy();
  });

  it("posts modern NoSQL root fields in save payloads", async () => {
    const user = userEvent.setup();

    const mongoView = render(<ConnectionFormView existing={null} />);

    await user.clear(screen.getByLabelText("Connection name"));
    await user.type(screen.getByLabelText("Connection name"), "Mongo Local");
    await user.click(screen.getByRole("button", { name: /mongodb/i }));
    await user.selectOptions(
      screen.getByLabelText("MongoDB connection method"),
      "uri",
    );
    await user.type(
      screen.getByLabelText("MongoDB connection URI"),
      "mongodb://localhost:27017/app",
    );
    expect(screen.queryByLabelText("Database")).toBeNull();

    clearPostedMessages();
    await submitCreateConnection(user);

    const mongoMessage = getLastPostedMessage() as
      | { type: string; payload: ConnectionFormSubmission }
      | undefined;
    expect(mongoMessage).toEqual({
      type: "saveConnection",
      payload: expect.objectContaining({
        type: "mongodb",
        connectionUri: "mongodb://localhost:27017/app",
        database: undefined,
      }),
    });
    expect(mongoMessage?.payload?.uri).toBeUndefined();
    expect(mongoMessage?.payload?.authSource).toBeUndefined();

    mongoView.unmount();

    const redisView = render(<ConnectionFormView existing={null} />);
    await user.clear(screen.getByLabelText("Connection name"));
    await user.type(screen.getByLabelText("Connection name"), "Redis Cache");
    await user.click(screen.getByRole("button", { name: /redis/i }));
    await user.type(screen.getByLabelText("Username"), "cache-user");
    await user.clear(screen.getByLabelText("Database"));
    await user.type(screen.getByLabelText("Database"), "2");

    clearPostedMessages();
    await submitCreateConnection(user);

    expect(getLastPostedMessage()).toEqual({
      type: "saveConnection",
      payload: expect.objectContaining({
        type: "redis",
        username: "cache-user",
        database: "2",
      }),
    });

    redisView.unmount();

    const elasticView = render(<ConnectionFormView existing={null} />);
    await user.clear(screen.getByLabelText("Connection name"));
    await user.type(screen.getByLabelText("Connection name"), "Elastic Cloud");
    await user.click(screen.getByRole("button", { name: /elasticsearch/i }));
    await user.type(
      screen.getByLabelText("Elasticsearch endpoint"),
      "http://localhost:9200",
    );
    await user.type(screen.getByLabelText("Elasticsearch API key"), "api-key");
    await user.type(
      screen.getByLabelText("Elasticsearch cloud id"),
      "deployment:ZXM=",
    );

    clearPostedMessages();
    await submitCreateConnection(user);

    expect(getLastPostedMessage()).toEqual({
      type: "saveConnection",
      payload: expect.objectContaining({
        type: "elasticsearch",
        endpoint: "http://localhost:9200",
        apiKey: "api-key",
        cloudId: "deployment:ZXM=",
        useSecretStorage: true,
      }),
    });

    elasticView.unmount();

    render(<ConnectionFormView existing={null} />);
    await user.clear(screen.getByLabelText("Connection name"));
    await user.type(screen.getByLabelText("Connection name"), "Dynamo Local");
    await user.click(screen.getByRole("button", { name: /dynamodb/i }));
    await user.type(screen.getByLabelText("AWS region"), "us-east-1");
    await user.type(screen.getByLabelText("AWS profile"), "default");
    await user.type(screen.getByLabelText("AWS access key id"), "AKIAQA");
    await user.type(
      screen.getByLabelText("AWS secret access key"),
      "aws-secret",
    );
    await user.type(screen.getByLabelText("AWS session token"), "session");
    await user.type(
      screen.getByLabelText("DynamoDB endpoint"),
      "http://localhost:8000",
    );

    clearPostedMessages();
    await submitCreateConnection(user);

    const dynamoMessage = getLastPostedMessage();
    expect(dynamoMessage).toEqual({
      type: "saveConnection",
      payload: expect.objectContaining({
        type: "dynamodb",
        awsRegion: "us-east-1",
        awsProfile: "default",
        awsAccessKeyId: "AKIAQA",
        awsSecretAccessKey: "aws-secret",
        awsSessionToken: "session",
        endpoint: "http://localhost:8000",
      }),
    });
    expect(dynamoMessage?.payload).not.toHaveProperty("awsEndpoint");
  });

  it("posts TLS configuration and legacy compatibility flags in save payloads", async () => {
    const user = userEvent.setup();

    render(<ConnectionFormView existing={null} />);

    await user.clear(screen.getByLabelText("Connection name"));
    await user.type(screen.getByLabelText("Connection name"), "Postgres TLS");
    await fillRequiredPostgresFields(user);
    await user.selectOptions(screen.getByLabelText("TLS mode"), ["mutualTls"]);
    await user.type(
      screen.getByLabelText("TLS CA certificate file"),
      "/tmp/ca.pem",
    );
    await user.type(
      screen.getByLabelText("TLS client certificate file"),
      "/tmp/client.crt",
    );
    await user.type(
      screen.getByLabelText("TLS client key file"),
      "/tmp/client.key",
    );
    await user.type(
      screen.getByLabelText("TLS client key passphrase"),
      "key-passphrase",
    );
    await user.type(
      screen.getByLabelText("TLS server name override"),
      "db.example.com",
    );

    clearPostedMessages();
    await submitCreateConnection(user);

    expect(getLastPostedMessage()).toEqual({
      type: "saveConnection",
      payload: expect.objectContaining({
        type: "pg",
        tls: {
          mode: "mutualTls",
          caFilePath: "/tmp/ca.pem",
          certFilePath: "/tmp/client.crt",
          keyFilePath: "/tmp/client.key",
          keyPassphrase: "key-passphrase",
          serverNameOverride: "db.example.com",
        },
      }),
    });
  });

  it("posts SSH password payloads with forced Secret Storage", async () => {
    const user = userEvent.setup();

    render(<ConnectionFormView existing={null} />);

    await user.clear(screen.getByLabelText("Connection name"));
    await user.type(screen.getByLabelText("Connection name"), "PG SSH");
    await fillRequiredPostgresFields(user);
    await user.click(
      screen.getByRole("switch", { name: /connect through ssh bastion/i }),
    );
    await user.type(screen.getByLabelText("SSH host"), "bastion.example.com");
    await user.clear(screen.getByLabelText("SSH port"));
    await user.type(screen.getByLabelText("SSH port"), "22");
    await user.type(screen.getByLabelText("SSH username"), "tunnel");
    await user.selectOptions(screen.getByLabelText("SSH auth method"), [
      "password",
    ]);
    await user.selectOptions(
      screen.getByLabelText("SSH host verification mode"),
      ["manual"],
    );
    await user.type(screen.getByLabelText("SSH password"), "ssh-secret");
    await user.type(
      screen.getByLabelText("SSH host fingerprint SHA256"),
      "SHA256:AbCdEfGhIjKlMnOpQrStUvWxYz0123456789+/",
    );

    clearPostedMessages();
    await submitCreateConnection(user);

    expect(getLastPostedMessage()).toEqual({
      type: "saveConnection",
      payload: expect.objectContaining({
        type: "pg",
        useSecretStorage: true,
        ssh: {
          host: "bastion.example.com",
          port: 22,
          username: "tunnel",
          authMethod: "password",
          hostVerificationMode: "manual",
          password: "ssh-secret",
          privateKey: undefined,
          passphrase: undefined,
          hostFingerprintSha256:
            "SHA256:AbCdEfGhIjKlMnOpQrStUvWxYz0123456789+/",
        },
      }),
    });
  });

  it("posts trust-on-first-use SSH payloads without a manual fingerprint", async () => {
    const user = userEvent.setup();

    render(<ConnectionFormView existing={null} />);

    await user.clear(screen.getByLabelText("Connection name"));
    await user.type(screen.getByLabelText("Connection name"), "PG SSH TOFU");
    await fillRequiredPostgresFields(user);
    await user.click(
      screen.getByRole("switch", { name: /connect through ssh bastion/i }),
    );
    await user.type(screen.getByLabelText("SSH host"), "bastion.example.com");
    await user.clear(screen.getByLabelText("SSH port"));
    await user.type(screen.getByLabelText("SSH port"), "22");
    await user.type(screen.getByLabelText("SSH username"), "tunnel");
    await user.selectOptions(screen.getByLabelText("SSH auth method"), [
      "password",
    ]);
    await user.selectOptions(
      screen.getByLabelText("SSH host verification mode"),
      ["trustOnFirstUse"],
    );
    await user.type(screen.getByLabelText("SSH password"), "ssh-secret");

    clearPostedMessages();
    await submitCreateConnection(user);

    expect(getLastPostedMessage()).toEqual({
      type: "saveConnection",
      payload: expect.objectContaining({
        type: "pg",
        useSecretStorage: true,
        ssh: {
          host: "bastion.example.com",
          port: 22,
          username: "tunnel",
          authMethod: "password",
          hostVerificationMode: "trustOnFirstUse",
          password: "ssh-secret",
          privateKey: undefined,
          passphrase: undefined,
          hostFingerprintSha256: undefined,
        },
      }),
    });
  });

  it("keeps stored SSH private-key flags when edit fields stay blank", async () => {
    const user = userEvent.setup();
    const existing = {
      id: "conn-ssh",
      name: "PG SSH",
      type: "pg" as const,
      host: "db.internal",
      port: 5432,
      database: "app",
      username: "postgres",
      ssh: {
        host: "bastion.example.com",
        port: 22,
        username: "tunnel",
        authMethod: "privateKey" as const,
        hostVerificationMode: "manual" as const,
        hostFingerprintSha256: "SHA256:AbCdEfGhIjKlMnOpQrStUvWxYz0123456789+/",
      },
      useSecretStorage: true,
      hasStoredSshPrivateKey: true,
      hasStoredSshPassphrase: true,
    };

    render(<ConnectionFormView existing={existing} />);

    expect(
      screen.getByText(/keep the stored SSH private key unchanged/i),
    ).toBeTruthy();
    expect(
      screen.getByText(/keep the stored SSH passphrase unchanged/i),
    ).toBeTruthy();

    clearPostedMessages();
    await user.click(screen.getByRole("button", { name: "Save Changes" }));

    expect(getLastPostedMessage()).toEqual({
      type: "saveConnection",
      payload: expect.objectContaining({
        id: "conn-ssh",
        useSecretStorage: true,
        ssh: {
          host: "bastion.example.com",
          port: 22,
          username: "tunnel",
          authMethod: "privateKey",
          hostVerificationMode: "manual",
          password: undefined,
          privateKey: "",
          passphrase: "",
          hostFingerprintSha256:
            "SHA256:AbCdEfGhIjKlMnOpQrStUvWxYz0123456789+/",
        },
        hasStoredSshPrivateKey: true,
        hasStoredSshPassphrase: true,
      }),
    });
  });

  it("rejects invalid Redis database input instead of falling back to DB 0", async () => {
    const user = userEvent.setup();

    render(<ConnectionFormView existing={null} />);

    await user.clear(screen.getByLabelText("Connection name"));
    await user.type(screen.getByLabelText("Connection name"), "Redis Cache");
    await user.click(screen.getByRole("button", { name: /redis/i }));
    await user.clear(screen.getByLabelText("Database"));
    await user.type(screen.getByLabelText("Database"), "abc");

    clearPostedMessages();
    await submitCreateConnection(user);

    expect(getLastPostedMessage()).toBeUndefined();
    await waitFor(() => {
      expect(
        screen.getByText(
          /Redis database must be a non-negative safe integer or dbN\./i,
        ),
      ).toBeTruthy();
    });
  });

  it("posts test, save, and cancel messages and reacts to result messages", async () => {
    const user = userEvent.setup();
    const existing = {
      id: "conn-1",
      name: "Warehouse",
      type: "pg" as const,
      host: "db.local",
      port: 5432,
      database: "app_db",
      username: "admin",
      folder: "Reporting",
      useSecretStorage: true,
      hasStoredSecret: true,
    };

    const { container } = render(<ConnectionFormView existing={existing} />);

    await expectNoAxeViolations(container);

    await user.clear(screen.getByLabelText("Connection name"));
    await user.type(
      screen.getByLabelText("Connection name"),
      "  Warehouse Copy  ",
    );
    await user.clear(screen.getByLabelText("Connection folder"));
    await user.type(
      screen.getByLabelText("Connection folder"),
      "  Reporting  ",
    );

    clearPostedMessages();

    await user.click(screen.getByRole("button", { name: "Test Connection" }));

    expect(getLastPostedMessage()).toEqual({
      type: "testConnection",
      payload: expect.objectContaining({
        id: "conn-1",
        name: "Warehouse Copy",
        type: "pg",
        host: "db.local",
        port: 5432,
        database: "app_db",
        username: "admin",
        folder: "Reporting",
        useSecretStorage: true,
        hasStoredSecret: true,
      }),
    });

    dispatchIncomingMessage("testResult", { success: true });

    await waitFor(() => {
      expect(screen.getByText(/Connection successful/)).toBeTruthy();
    });

    clearPostedMessages();

    await user.click(screen.getByRole("button", { name: "Save Changes" }));

    expect(getLastPostedMessage()).toEqual({
      type: "saveConnection",
      payload: expect.objectContaining({
        id: "conn-1",
        name: "Warehouse Copy",
        folder: "Reporting",
      }),
    });

    dispatchIncomingMessage("saveResult", {
      success: false,
      error: "Duplicate connection",
    });

    await waitFor(() => {
      expect(screen.getByText(/Duplicate connection/)).toBeTruthy();
    });

    clearPostedMessages();

    await user.click(screen.getByRole("button", { name: "Cancel" }));

    expect(getLastPostedMessage()).toEqual({ type: "cancel" });
  });

  it.each([
    "pg",
    "oracle",
  ] as const)("tracks the %s read-only toggle through edit state, test, and save payloads", async (type) => {
    const user = userEvent.setup();
    const existing = {
      id: "conn-1",
      name: "Warehouse",
      type,
      host: "db.local",
      port: 5432,
      database: "app_db",
      username: "admin",
      readOnly: true,
      useSecretStorage: true,
      hasStoredSecret: true,
    };

    render(<ConnectionFormView existing={existing} />);

    const toggle = screen.getByRole("switch", {
      name: /open connection as read-only/i,
    });
    expect(toggle.getAttribute("aria-checked")).toBe("true");

    await user.click(toggle);

    expect(toggle.getAttribute("aria-checked")).toBe("false");

    clearPostedMessages();
    await user.click(screen.getByRole("button", { name: "Test Connection" }));

    expect(getLastPostedMessage()).toEqual({
      type: "testConnection",
      payload: expect.objectContaining({
        id: "conn-1",
        readOnly: false,
      }),
    });

    clearPostedMessages();

    await user.click(screen.getByRole("button", { name: "Cancel Testing" }));

    clearPostedMessages();
    await user.click(screen.getByRole("button", { name: "Save Changes" }));

    expect(getLastPostedMessage()).toEqual({
      type: "saveConnection",
      payload: expect.objectContaining({
        id: "conn-1",
        readOnly: false,
      }),
    });
  });

  it("tracks SQLite WAL mode through edit state and save payloads", async () => {
    const user = userEvent.setup();
    const existing = {
      id: "conn-sqlite",
      name: "Local SQLite",
      type: "sqlite" as const,
      filePath: "/tmp/app.db",
      sqliteWalMode: "off" as const,
    };

    render(<ConnectionFormView existing={existing} />);

    expect(
      (screen.getByLabelText("SQLite WAL mode") as HTMLSelectElement).value,
    ).toBe("off");
    expect(
      screen.getByText(
        /automatic wal handling is disabled for this sqlite connection/i,
      ),
    ).toBeTruthy();

    await user.selectOptions(screen.getByLabelText("SQLite WAL mode"), [
      "auto",
    ]);

    clearPostedMessages();
    await user.click(screen.getByRole("button", { name: "Save Changes" }));

    expect(getLastPostedMessage()).toEqual({
      type: "saveConnection",
      payload: expect.objectContaining({
        id: "conn-sqlite",
        type: "sqlite",
        filePath: "/tmp/app.db",
        sqliteWalMode: "auto",
      }),
    });
  });

  it("does not prefill secret edit fields from existing state", async () => {
    const user = userEvent.setup();

    const elasticView = render(
      <ConnectionFormView
        existing={{
          id: "conn-es",
          name: "Elastic",
          type: "elasticsearch",
          endpoint: "https://cluster.example.com",
          apiKey: "inline-api-key",
          useSecretStorage: true,
          hasStoredSecret: true,
        }}
      />,
    );

    expect(
      (screen.getByLabelText("Elasticsearch API key") as HTMLInputElement)
        .value,
    ).toBe("");

    elasticView.unmount();

    render(
      <ConnectionFormView
        existing={{
          id: "conn-ddb",
          name: "Dynamo",
          type: "dynamodb",
          awsRegion: "us-east-1",
          awsAccessKeyId: "AKIA123",
          awsSecretAccessKey: "secret-key",
          awsSessionToken: "session-token",
          endpoint: "http://localhost:8000",
          useSecretStorage: true,
        }}
      />,
    );

    await user.click(screen.getByRole("button", { name: /dynamodb/i }));

    expect(
      (screen.getByLabelText("AWS access key id") as HTMLInputElement).value,
    ).toBe("");
    expect(
      (screen.getByLabelText("AWS secret access key") as HTMLInputElement)
        .value,
    ).toBe("");
    expect(
      (screen.getByLabelText("AWS session token") as HTMLInputElement).value,
    ).toBe("");
  });
});

describe("MongoDB connection editor", () => {
  it("submits the same authSource and credentials for Test and Save, while keeping editor drafts", async () => {
    const user = userEvent.setup();
    render(<ConnectionFormView existing={null} />);
    await user.type(screen.getByLabelText("Connection name"), "Mongo fields");
    await user.click(screen.getByRole("button", { name: /mongodb/i }));
    await user.selectOptions(
      screen.getByLabelText("MongoDB authentication"),
      "password",
    );
    await user.type(screen.getByLabelText("Database"), "app");
    await user.type(screen.getByLabelText("Username"), "root");
    await user.type(screen.getByLabelText("Password"), "p@ss");
    await user.type(
      screen.getByLabelText("MongoDB authentication database"),
      "admin",
    );
    expect(
      (screen.getByLabelText("MongoDB URI preview") as HTMLInputElement).value,
    ).not.toContain("p%40ss");
    await user.selectOptions(
      screen.getByLabelText("MongoDB connection method"),
      "uri",
    );
    await user.type(
      screen.getByLabelText("MongoDB connection URI"),
      "mongodb://other/uri-db?authSource=admin&retryWrites=false",
    );
    expect(screen.queryByLabelText("Host")).toBeNull();
    expect(screen.queryByLabelText("Password")).toBeNull();
    await user.selectOptions(
      screen.getByLabelText("MongoDB connection method"),
      "fields",
    );
    expect(
      (
        screen.getByLabelText(
          "MongoDB authentication database",
        ) as HTMLInputElement
      ).value,
    ).toBe("admin");
    await user.click(screen.getByRole("button", { name: /test connection/i }));
    const tested = getLastPostedMessage()?.payload as ConnectionFormSubmission;
    expect(tested).toMatchObject({
      mongoConnectionMode: "fields",
      database: "app",
      authSource: "admin",
      username: "root",
      password: "p@ss",
    });
    expect(tested?.connectionUri).toBeUndefined();
    dispatchIncomingMessage("testResult", { success: true });
    await waitFor(() =>
      expect(
        screen
          .getByRole("button", { name: /create connection/i })
          .hasAttribute("disabled"),
      ).toBe(false),
    );
    await submitCreateConnection(user);
    expect(getLastPostedMessage()?.payload).toMatchObject({
      ...tested,
      id: expect.any(String),
    });
  });

  it("materializes legacy MongoDB seed ports before saving in fields mode", async () => {
    const user = userEvent.setup();
    render(
      <ConnectionFormView
        existing={{
          id: "legacy-seeds",
          name: "Legacy seeds",
          type: "mongodb",
          host: "mongo-a,mongo-b",
          port: 27018,
          database: "app",
        }}
      />,
    );
    expect((screen.getByLabelText("Host") as HTMLInputElement).value).toBe(
      "mongo-a:27017,mongo-b:27018",
    );
    await user.click(screen.getByRole("button", { name: /save changes/i }));
    expect(getLastPostedMessage()).toMatchObject({
      type: "saveConnection",
      payload: {
        mongoConnectionMode: "fields",
        host: "mongo-a:27017,mongo-b:27018",
        port: 27018,
      },
    });
  });

  it("preserves a legacy MongoDB working database separately from the URI", async () => {
    const user = userEvent.setup();
    render(
      <ConnectionFormView
        existing={{
          id: "mongo-db-override",
          name: "Mongo legacy",
          type: "mongodb",
          connectionUri: "mongodb+srv://cluster.example/users",
          database: "app",
          hasStoredMongoUriCredentials: true,
        }}
      />,
    );
    expect(
      (screen.getByLabelText("MongoDB connection URI") as HTMLInputElement)
        .value,
    ).toBe("mongodb+srv://cluster.example/users");
    expect(
      (
        screen.getByLabelText(
          "MongoDB working database override",
        ) as HTMLInputElement
      ).value,
    ).toBe("app");
    await user.click(screen.getByRole("button", { name: /test connection/i }));
    expect(getLastPostedMessage()).toMatchObject({
      type: "testConnection",
      payload: {
        database: "app",
        connectionUri: "mongodb+srv://cluster.example/users",
      },
    });
    dispatchIncomingMessage("testResult", { success: true });
    await user.click(screen.getByRole("button", { name: /save changes/i }));
    expect(getLastPostedMessage()).toMatchObject({
      type: "saveConnection",
      payload: {
        database: "app",
        connectionUri: "mongodb+srv://cluster.example/users",
      },
    });
  });

  it("opens a stored URI with TLS inheritance and explicit credential retention", async () => {
    const user = userEvent.setup();
    render(
      <ConnectionFormView
        existing={{
          id: "saved",
          type: "mongodb",
          name: "Saved",
          mongoConnectionMode: "uri",
          connectionUri: "mongodb+srv://cluster.example/app",
          hasStoredMongoUriCredentials: true,
        }}
      />,
    );
    expect(screen.queryByLabelText("Host")).toBeNull();
    expect(screen.queryByLabelText("TLS mode")).toBeNull();
    expect(
      screen
        .getByRole("switch", { name: /keep saved uri credentials/i })
        .getAttribute("aria-checked"),
    ).toBe("true");
    await user.click(screen.getByRole("button", { name: /save changes/i }));
    expect(getLastPostedMessage()?.payload).toMatchObject({
      mongoConnectionMode: "uri",
      keepMongoUriCredentials: true,
    });
    expect(
      (getLastPostedMessage()?.payload as ConnectionFormSubmission)?.tls,
    ).toBeUndefined();
  });
});

describe("Redis URI connection editor", () => {
  it("rejects a non-Redis URI before sending Test or Save", async () => {
    const user = userEvent.setup();
    render(<ConnectionFormView existing={null} />);
    await user.type(screen.getByLabelText("Connection name"), "Invalid Redis");
    await user.click(screen.getByRole("button", { name: /redis/i }));
    await user.selectOptions(
      screen.getByLabelText("Redis connection method"),
      "uri",
    );
    await user.type(
      screen.getByLabelText("Redis connection URI"),
      "http://redis.local:6379/0",
    );
    clearPostedMessages();

    await user.click(screen.getByRole("button", { name: /test connection/i }));

    expect(getLastPostedMessage()).toBeUndefined();
    expect(
      screen.getByText(/valid redis uri starting with redis:\/\//i),
    ).toBeTruthy();
  });

  it("edits and tests URI connections while keeping stored credentials opt-in", async () => {
    const user = userEvent.setup();
    render(
      <ConnectionFormView
        existing={{
          id: "redis-uri",
          name: "Redis URI",
          type: "redis",
          connectionUri: "redis://cache.local:6380/4",
          useSecretStorage: true,
          hasStoredConnectionUriCredentials: true,
        }}
      />,
    );

    expect(
      (screen.getByLabelText("Redis connection method") as HTMLSelectElement)
        .value,
    ).toBe("uri");
    expect(screen.queryByLabelText("Host")).toBeNull();
    expect(screen.queryByLabelText("Password")).toBeNull();
    expect(
      (
        screen.getByRole("switch", { name: "Keep saved URI credentials" }) as
          | HTMLInputElement
          | HTMLElement
      ).getAttribute("aria-checked"),
    ).toBe("true");

    await user.clear(screen.getByLabelText("Redis connection URI"));
    await user.type(
      screen.getByLabelText("Redis connection URI"),
      "redis://cache.local:6380/5",
    );
    clearPostedMessages();
    await user.click(screen.getByRole("button", { name: /test connection/i }));

    expect(getLastPostedMessage()).toMatchObject({
      type: "testConnection",
      payload: {
        type: "redis",
        connectionUri: "redis://cache.local:6380/5",
        keepConnectionUriCredentials: true,
        host: undefined,
        port: undefined,
        database: undefined,
      },
    });
  });

  it("switches from a saved URI to parameters without resending the URI", async () => {
    const user = userEvent.setup();
    render(
      <ConnectionFormView
        existing={{
          id: "redis-uri",
          name: "Redis URI",
          type: "redis",
          connectionUri: "redis://cache.local:6380/4",
        }}
      />,
    );
    await user.selectOptions(
      screen.getByLabelText("Redis connection method"),
      "fields",
    );
    await user.clear(screen.getByLabelText("Host"));
    await user.type(screen.getByLabelText("Host"), "redis-new.local");
    await user.clear(screen.getByLabelText("Database"));
    await user.type(screen.getByLabelText("Database"), "2");
    await user.type(screen.getByLabelText("Connection name"), " edited");

    await user.click(screen.getByRole("button", { name: /save changes/i }));

    expect(getLastPostedMessage()).toMatchObject({
      type: "saveConnection",
      payload: {
        type: "redis",
        host: "redis-new.local",
        port: 6379,
        database: "2",
        connectionUri: undefined,
      },
    });
  });
});

describe("Elasticsearch endpoint editor", () => {
  it("offers explicit retention for credentials saved in a URI alias", async () => {
    const user = userEvent.setup();
    render(
      <ConnectionFormView
        existing={{
          id: "elastic-uri",
          name: "Elastic URI",
          type: "elasticsearch",
          connectionUri: "https://elastic.local/base",
          useSecretStorage: true,
          hasStoredConnectionUriCredentials: true,
        }}
      />,
    );
    expect(
      (screen.getByLabelText("Elasticsearch endpoint") as HTMLInputElement)
        .value,
    ).toBe("https://elastic.local/base");
    expect(
      screen
        .getByRole("switch", {
          name: "Keep saved endpoint credentials",
        })
        .getAttribute("aria-checked"),
    ).toBe("true");

    await user.clear(screen.getByLabelText("Elasticsearch endpoint"));
    await user.type(
      screen.getByLabelText("Elasticsearch endpoint"),
      "https://elastic.local/edited",
    );
    await user.type(screen.getByLabelText("Connection name"), " edited");
    await user.click(screen.getByRole("button", { name: /save changes/i }));

    expect(getLastPostedMessage()).toMatchObject({
      type: "saveConnection",
      payload: {
        type: "elasticsearch",
        endpoint: "https://elastic.local/edited",
        keepConnectionUriCredentials: true,
      },
    });
    expect(getLastPostedMessage()?.payload).not.toHaveProperty("connectionUri");
  });
});

describe("saved credential removal controls", () => {
  it.each([
    {
      label: "Remove saved password",
      existing: {
        id: "pg-password",
        name: "Postgres",
        type: "pg" as const,
        host: "localhost",
        database: "app",
        username: "user",
        hasStoredSecret: true,
        useSecretStorage: true,
      },
      payloadFlag: "clearPassword",
    },
    {
      label: "Remove saved API key",
      existing: {
        id: "elastic-api-key",
        name: "Elasticsearch",
        type: "elasticsearch" as const,
        endpoint: "https://elastic.local",
        hasStoredApiKey: true,
        useSecretStorage: true,
      },
      payloadFlag: "clearApiKey",
    },
    {
      label: "Remove saved AWS credentials",
      existing: {
        id: "dynamo-keys",
        name: "DynamoDB",
        type: "dynamodb" as const,
        awsRegion: "us-east-1",
        hasStoredAwsCredentials: true,
        useSecretStorage: true,
      },
      payloadFlag: "clearAwsCredentials",
    },
  ])("sends $payloadFlag when the user removes a saved secret", async ({
    label,
    existing,
    payloadFlag,
  }) => {
    const user = userEvent.setup();
    render(<ConnectionFormView existing={existing} />);
    await user.click(screen.getByRole("switch", { name: label }));
    await user.click(screen.getByRole("button", { name: /save changes/i }));
    expect(getLastPostedMessage()?.payload).toMatchObject({
      [payloadFlag]: true,
    });
  });

  it("uses a DynamoDB profile instead of saved static credentials", async () => {
    const user = userEvent.setup();
    render(
      <ConnectionFormView
        existing={{
          id: "dynamo-profile",
          name: "DynamoDB",
          type: "dynamodb",
          awsRegion: "us-east-1",
          awsProfile: "team",
          hasStoredAwsCredentials: true,
          useSecretStorage: true,
        }}
      />,
    );
    await user.click(screen.getByRole("button", { name: /save changes/i }));
    const payload = getLastPostedMessage()?.payload as
      | Record<string, unknown>
      | undefined;
    expect(payload).toMatchObject({
      awsProfile: "team",
      clearAwsCredentials: true,
    });
    expect(payload?.awsAccessKeyId).toBeUndefined();
    expect(payload?.awsSecretAccessKey).toBeUndefined();
    expect(payload?.awsSessionToken).toBeUndefined();
  });
});
