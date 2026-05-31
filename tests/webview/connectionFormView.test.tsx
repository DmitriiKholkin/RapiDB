import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";
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

describe("ConnectionFormView", () => {
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

    await user.click(
      screen.getByRole("switch", {
        name: /Use thick mode \(requires Oracle Instant Client\)/i,
      }),
    );

    expect(screen.getByLabelText("Oracle Instant Client path")).toBeTruthy();

    await user.click(screen.getByRole("button", { name: /mongodb/i }));

    expect(screen.getByLabelText("MongoDB connection URI")).toBeTruthy();
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
        /first successful ssh handshake will pin the discovered sha256 fingerprint automatically/i,
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
    const hint = screen.getByText(/Blocks data mutations/i);

    expect(toggle.getAttribute("aria-describedby")).toBeTruthy();
    expect(toggle.getAttribute("aria-describedby")).toBe(
      hint.getAttribute("id"),
    );
  });

  it("posts modern NoSQL root fields in save payloads", async () => {
    const user = userEvent.setup();

    const mongoView = render(<ConnectionFormView existing={null} />);

    await user.clear(screen.getByLabelText("Connection name"));
    await user.type(screen.getByLabelText("Connection name"), "Mongo Local");
    await user.click(screen.getByRole("button", { name: /mongodb/i }));
    await user.type(
      screen.getByLabelText("MongoDB connection URI"),
      "mongodb://localhost:27017/app",
    );
    await user.clear(screen.getByLabelText("Database"));
    await user.type(screen.getByLabelText("Database"), "admin");

    clearPostedMessages();
    await submitCreateConnection(user);

    const mongoMessage = getLastPostedMessage();
    expect(mongoMessage).toEqual({
      type: "saveConnection",
      payload: expect.objectContaining({
        type: "mongodb",
        connectionUri: "mongodb://localhost:27017/app",
        database: "admin",
      }),
    });
    expect(mongoMessage?.payload).not.toHaveProperty("uri");
    expect(mongoMessage?.payload).not.toHaveProperty("authSource");

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
        endpoint: "http://localhost:8000",
      }),
    });
    expect(dynamoMessage?.payload).not.toHaveProperty("awsEndpoint");
  });

  it("posts SSH password payloads with forced Secret Storage", async () => {
    const user = userEvent.setup();

    render(<ConnectionFormView existing={null} />);

    await user.clear(screen.getByLabelText("Connection name"));
    await user.type(screen.getByLabelText("Connection name"), "PG SSH");
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
        sshEnabled: true,
        sshHost: "bastion.example.com",
        sshPort: 22,
        sshUsername: "tunnel",
        sshAuthMethod: "password",
        sshHostVerificationMode: "manual",
        sshPassword: "ssh-secret",
        sshHostFingerprintSha256:
          "SHA256:AbCdEfGhIjKlMnOpQrStUvWxYz0123456789+/",
      }),
    });
  });

  it("posts trust-on-first-use SSH payloads without a manual fingerprint", async () => {
    const user = userEvent.setup();

    render(<ConnectionFormView existing={null} />);

    await user.clear(screen.getByLabelText("Connection name"));
    await user.type(screen.getByLabelText("Connection name"), "PG SSH TOFU");
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
        sshEnabled: true,
        sshHost: "bastion.example.com",
        sshPort: 22,
        sshUsername: "tunnel",
        sshAuthMethod: "password",
        sshHostVerificationMode: "trustOnFirstUse",
        sshPassword: "ssh-secret",
        sshHostFingerprintSha256: undefined,
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
      sshEnabled: true,
      sshHost: "bastion.example.com",
      sshPort: 22,
      sshUsername: "tunnel",
      sshAuthMethod: "privateKey" as const,
      sshHostVerificationMode: "manual" as const,
      sshHostFingerprintSha256: "SHA256:AbCdEfGhIjKlMnOpQrStUvWxYz0123456789+/",
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
        sshEnabled: true,
        sshAuthMethod: "privateKey",
        sshHostVerificationMode: "manual",
        sshHost: "bastion.example.com",
        sshPort: 22,
        sshUsername: "tunnel",
        sshHostFingerprintSha256:
          "SHA256:AbCdEfGhIjKlMnOpQrStUvWxYz0123456789+/",
        hasStoredSshPrivateKey: true,
        hasStoredSshPassphrase: true,
        sshPrivateKey: "",
        sshPassphrase: "",
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
        screen.getByText(/Redis database must be a non-negative integer\./i),
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

  it("tracks the read-only toggle through edit state, test, and save payloads", async () => {
    const user = userEvent.setup();
    const existing = {
      id: "conn-1",
      name: "Warehouse",
      type: "pg" as const,
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
