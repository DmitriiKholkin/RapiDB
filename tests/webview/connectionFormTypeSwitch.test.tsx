import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";
import type { ConnectionFormExistingState } from "../../src/shared/webviewContracts";
import { ConnectionFormView } from "../../src/webview/components/ConnectionFormView";
import { clearPostedMessages, getLastPostedMessage } from "./testUtils";

const existing: ConnectionFormExistingState = {
  id: "pg-mtls",
  name: "Secure PG",
  type: "pg",
  host: "db.local",
  port: 5544,
  database: "app",
  username: "reader",
  hasStoredSecret: true,
  tls: {
    mode: "mutualTls",
    caFilePath: "/ca.pem",
    certFilePath: "/client.pem",
    keyFilePath: "/client.key",
    serverNameOverride: "db.internal",
  },
};

describe("ConnectionFormView type switching", () => {
  it("restores the edited connection's port and TLS when switching back to its type", async () => {
    const user = userEvent.setup();
    render(<ConnectionFormView existing={existing} />);
    await user.click(screen.getByRole("button", { name: "MySQL / MariaDB" }));
    await user.click(screen.getByRole("button", { name: "PostgreSQL" }));
    clearPostedMessages();
    await user.click(screen.getByRole("button", { name: "Save Changes" }));
    expect(getLastPostedMessage()).toMatchObject({
      type: "saveConnection",
      payload: {
        id: "pg-mtls",
        type: "pg",
        port: 5544,
        tls: existing.tls,
      },
    });
  });

  it("still resets port and TLS when switching to a different type", async () => {
    const user = userEvent.setup();
    render(<ConnectionFormView existing={existing} />);
    await user.click(screen.getByRole("button", { name: "MySQL / MariaDB" }));
    clearPostedMessages();
    await user.click(screen.getByRole("button", { name: "Save Changes" }));
    const payload = (
      getLastPostedMessage() as { payload: Record<string, unknown> }
    ).payload;
    expect(payload).toMatchObject({ type: "mysql", port: 3306 });
    expect(payload.tls).toMatchObject({ mode: "disabled" });
  });

  it("restores unsaved port and TLS edits when switching back to a type", async () => {
    const user = userEvent.setup();
    render(<ConnectionFormView existing={existing} />);
    await user.clear(screen.getByLabelText("Port"));
    await user.type(screen.getByLabelText("Port"), "6000");
    await user.clear(screen.getByLabelText("TLS server name override"));
    await user.type(
      screen.getByLabelText("TLS server name override"),
      "edited.internal",
    );
    await user.click(screen.getByRole("button", { name: "MySQL / MariaDB" }));
    await user.click(screen.getByRole("button", { name: "PostgreSQL" }));
    clearPostedMessages();
    await user.click(screen.getByRole("button", { name: "Save Changes" }));
    expect(getLastPostedMessage()).toMatchObject({
      type: "saveConnection",
      payload: {
        type: "pg",
        port: 6000,
        tls: { ...existing.tls, serverNameOverride: "edited.internal" },
      },
    });
  });

  it("keeps per-type TLS drafts for a new connection without leaking them across types", async () => {
    const user = userEvent.setup();
    render(<ConnectionFormView existing={null} />);
    await user.type(screen.getByLabelText("Connection name"), "New");
    await user.type(screen.getByLabelText("Database"), "app");
    await user.type(screen.getByLabelText("Username"), "reader");
    await user.selectOptions(
      screen.getByLabelText("TLS mode"),
      "requireVerifyFull",
    );
    await user.click(screen.getByRole("button", { name: "MySQL / MariaDB" }));
    expect((screen.getByLabelText("Port") as HTMLInputElement).value).toBe(
      "3306",
    );
    expect((screen.getByLabelText("TLS mode") as HTMLSelectElement).value).toBe(
      "disabled",
    );
    await user.click(screen.getByRole("button", { name: "PostgreSQL" }));
    expect((screen.getByLabelText("Port") as HTMLInputElement).value).toBe(
      "5432",
    );
    clearPostedMessages();
    await user.click(screen.getByRole("button", { name: "Create Connection" }));
    expect(getLastPostedMessage()).toMatchObject({
      type: "saveConnection",
      payload: { type: "pg", port: 5432, tls: { mode: "requireVerifyFull" } },
    });
  });

  it("keeps one connection id for Test and Save of a new connection", async () => {
    const user = userEvent.setup();
    render(<ConnectionFormView existing={null} />);
    await user.type(screen.getByLabelText("Connection name"), "New");
    await user.type(screen.getByLabelText("Database"), "app");
    await user.type(screen.getByLabelText("Username"), "reader");
    await user.click(screen.getByRole("button", { name: "Test Connection" }));
    const tested = getLastPostedMessage() as { payload: { id: string } };
    await user.click(screen.getByRole("button", { name: "Cancel Testing" }));
    await user.click(screen.getByRole("button", { name: "Create Connection" }));
    const saved = getLastPostedMessage() as {
      type: string;
      payload: { id: string };
    };
    expect(saved.type).toBe("saveConnection");
    expect(saved.payload.id).toBe(tested.payload.id);
  });
});
