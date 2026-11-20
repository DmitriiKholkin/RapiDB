import { act, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";
import type {
  ConnectionFormExistingState,
  ConnectionFormSubmission,
} from "../../src/shared/webviewContracts";
import { ConnectionFormView } from "../../src/webview/components/ConnectionFormView";
import {
  clearPostedMessages,
  dispatchIncomingMessage,
  getLastPostedMessage,
} from "./testUtils";

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

async function testDraft(user: ReturnType<typeof userEvent.setup>) {
  await user.click(screen.getByRole("button", { name: "Test Connection" }));
  const message = getLastPostedMessage();
  expect(message?.type).toBe("testConnection");
  const payload = message?.payload as ConnectionFormSubmission & {
    requestId: string;
  };
  act(() =>
    dispatchIncomingMessage("testResult", {
      requestId: payload.requestId,
      success: true,
    }),
  );
  return payload;
}

describe("ConnectionFormView type switching", () => {
  it("restores the edited connection's port and TLS when switching back to its type", async () => {
    const user = userEvent.setup();
    render(<ConnectionFormView existing={existing} />);
    await user.click(screen.getByRole("button", { name: "MySQL / MariaDB" }));
    await user.click(screen.getByRole("button", { name: "PostgreSQL" }));
    expect(await testDraft(user)).toMatchObject({
      type: "pg",
      password: "",
      hasStoredSecret: true,
      clearPassword: false,
    });
    clearPostedMessages();
    await user.click(screen.getByRole("button", { name: "Save Changes" }));
    expect(getLastPostedMessage()).toMatchObject({
      type: "saveConnection",
      payload: {
        id: "pg-mtls",
        type: "pg",
        port: 5544,
        tls: existing.tls,
        password: "",
        hasStoredSecret: true,
        clearPassword: false,
      },
    });
  });

  it("still resets port and TLS when switching to a different type", async () => {
    const user = userEvent.setup();
    render(<ConnectionFormView existing={existing} />);
    await user.type(screen.getByLabelText("Password"), "synthetic-pg");
    await user.click(screen.getByRole("button", { name: "MySQL / MariaDB" }));
    clearPostedMessages();
    await user.click(screen.getByRole("button", { name: "Save Changes" }));
    const payload = (
      getLastPostedMessage() as { payload: Record<string, unknown> }
    ).payload;
    expect(payload).toMatchObject({
      type: "mysql",
      port: 3306,
      password: "",
      hasStoredSecret: undefined,
    });
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

  it.each([
    { profile: null, saveLabel: "Create Connection" },
    { profile: existing, saveLabel: "Save Changes" },
  ])("$saveLabel tests and saves only the selected engine's password draft", async ({
    profile,
    saveLabel,
  }) => {
    const user = userEvent.setup();
    render(<ConnectionFormView existing={profile} />);
    if (!profile) {
      await user.type(screen.getByLabelText("Connection name"), "New");
      await user.type(screen.getByLabelText("Database"), "app");
      await user.type(screen.getByLabelText("Username"), "reader");
    }
    const password = () => screen.getByLabelText<HTMLInputElement>("Password");
    expect(password().value).toBe("");
    await user.type(password(), "synthetic-pg");

    await user.click(screen.getByRole("button", { name: "MySQL / MariaDB" }));
    expect(password().value).toBe("");
    expect(await testDraft(user)).toMatchObject({
      type: "mysql",
      password: "",
      hasStoredSecret: undefined,
    });
    await user.type(password(), "synthetic-mysql");

    await user.click(screen.getByRole("button", { name: "PostgreSQL" }));
    expect(password().value).toBe("synthetic-pg");
    expect(await testDraft(user)).toMatchObject({
      type: "pg",
      password: "synthetic-pg",
    });

    await user.click(screen.getByRole("button", { name: "MySQL / MariaDB" }));
    expect(password().value).toBe("synthetic-mysql");
    expect(await testDraft(user)).toMatchObject({
      type: "mysql",
      password: "synthetic-mysql",
    });
    if (!profile) await user.clear(password());
    await user.click(screen.getByRole("button", { name: saveLabel }));
    expect(getLastPostedMessage()).toMatchObject({
      type: "saveConnection",
      payload: { type: "mysql", password: profile ? "synthetic-mysql" : "" },
    });
  });

  it.each([
    { replace: false, expectedPassword: "", expectedClear: true },
    {
      replace: true,
      expectedPassword: "synthetic-replacement",
      expectedClear: false,
    },
  ])("restores removal intent on return to the original engine (replace=$replace)", async ({
    replace,
    expectedPassword,
    expectedClear,
  }) => {
    const user = userEvent.setup();
    render(<ConnectionFormView existing={existing} />);
    const removePassword = () =>
      screen.getByRole("switch", { name: "Remove saved password" });
    await user.click(removePassword());

    await user.click(screen.getByRole("button", { name: "MySQL / MariaDB" }));
    expect(
      screen.queryByRole("switch", { name: "Remove saved password" }),
    ).toBeNull();
    expect(await testDraft(user)).toMatchObject({
      type: "mysql",
      password: "",
      clearPassword: undefined,
    });
    await user.type(screen.getByLabelText("Password"), "synthetic-mysql");

    await user.click(screen.getByRole("button", { name: "PostgreSQL" }));
    expect(removePassword().getAttribute("aria-checked")).toBe("true");
    expect(screen.getByLabelText<HTMLInputElement>("Password").value).toBe("");
    if (replace) {
      await user.type(screen.getByLabelText("Password"), expectedPassword);
      expect(removePassword().getAttribute("aria-checked")).toBe("false");
    }
    const expected = {
      type: "pg",
      password: expectedPassword,
      hasStoredSecret: true,
      clearPassword: expectedClear,
    };
    expect(await testDraft(user)).toMatchObject(expected);
    await user.click(screen.getByRole("button", { name: "Save Changes" }));
    expect(getLastPostedMessage()).toMatchObject({
      type: "saveConnection",
      payload: expected,
    });
  });
});
