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

describe("ConnectionFormView", () => {
  it("shows connection-type-specific fields", async () => {
    const user = userEvent.setup();

    render(<ConnectionFormView existing={null} />);

    expect(screen.getByLabelText("Host")).toBeTruthy();
    expect(screen.queryByLabelText("Database file path")).toBeNull();

    await user.selectOptions(screen.getByLabelText("Database type"), "sqlite");

    expect(screen.getByLabelText("Database file path")).toBeTruthy();
    expect(screen.queryByLabelText("Host")).toBeNull();

    await user.selectOptions(screen.getByLabelText("Database type"), "oracle");

    expect(screen.getByLabelText("Oracle service name")).toBeTruthy();

    await user.click(
      screen.getByRole("checkbox", {
        name: /Use thick mode \(requires Oracle Instant Client\)/i,
      }),
    );

    expect(screen.getByLabelText("Oracle Instant Client path")).toBeTruthy();
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

    await user.click(screen.getByRole("button", { name: "Save" }));

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
