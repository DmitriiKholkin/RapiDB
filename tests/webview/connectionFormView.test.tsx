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

    expect(screen.getByLabelText("Host")).toBeTruthy();
    expect(screen.queryByLabelText("Database file path")).toBeNull();

    await user.click(screen.getByRole("button", { name: /sqlite/i }));

    expect(screen.getByLabelText("Database file path")).toBeTruthy();
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
    expect(screen.getByLabelText("MongoDB auth database")).toBeTruthy();

    await user.click(screen.getByRole("button", { name: /redis/i }));

    expect(screen.getByLabelText("Redis username")).toBeTruthy();
    expect(screen.getByLabelText("Redis key prefix")).toBeTruthy();
    expect(screen.getByLabelText("Redis DB")).toBeTruthy();

    await user.click(screen.getByRole("button", { name: /elasticsearch/i }));

    expect(screen.getByLabelText("Elasticsearch endpoint")).toBeTruthy();
    expect(screen.getByLabelText("Elasticsearch API key")).toBeTruthy();
    expect(screen.getByLabelText("Elasticsearch cloud id")).toBeTruthy();

    await user.click(screen.getByRole("button", { name: /dynamodb/i }));

    expect(screen.getByLabelText("AWS region")).toBeTruthy();
    expect(screen.getByLabelText("AWS profile")).toBeTruthy();
    expect(screen.getByLabelText("DynamoDB endpoint")).toBeTruthy();
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
    await user.type(screen.getByLabelText("MongoDB auth database"), "admin");

    clearPostedMessages();
    await submitCreateConnection(user);

    const mongoMessage = getLastPostedMessage();
    expect(mongoMessage).toEqual({
      type: "saveConnection",
      payload: expect.objectContaining({
        type: "mongodb",
        connectionUri: "mongodb://localhost:27017/app",
        authDatabase: "admin",
      }),
    });
    expect(mongoMessage?.payload).not.toHaveProperty("uri");
    expect(mongoMessage?.payload).not.toHaveProperty("authSource");

    mongoView.unmount();

    const redisView = render(<ConnectionFormView existing={null} />);
    await user.clear(screen.getByLabelText("Connection name"));
    await user.type(screen.getByLabelText("Connection name"), "Redis Cache");
    await user.click(screen.getByRole("button", { name: /redis/i }));
    await user.type(screen.getByLabelText("Redis username"), "cache-user");
    await user.clear(screen.getByLabelText("Redis DB"));
    await user.type(screen.getByLabelText("Redis DB"), "2");
    await user.type(screen.getByLabelText("Redis key prefix"), "app:");

    clearPostedMessages();
    await submitCreateConnection(user);

    expect(getLastPostedMessage()).toEqual({
      type: "saveConnection",
      payload: expect.objectContaining({
        type: "redis",
        redisUsername: "cache-user",
        redisDb: 2,
        keyPrefix: "app:",
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
});
