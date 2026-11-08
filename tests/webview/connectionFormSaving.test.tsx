import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";
import type { ConnectionFormExistingState } from "../../src/shared/webviewContracts";
import { ConnectionFormView } from "../../src/webview/components/ConnectionFormView";
import {
  clearPostedMessages,
  dispatchIncomingMessage,
  expectNoAxeViolations,
  getLastPostedMessage,
} from "./testUtils";

const existing: ConnectionFormExistingState = {
  id: "saving",
  name: "Saving",
  type: "pg",
  host: "localhost",
  database: "app",
  username: "reader",
  hasStoredSecret: true,
  hasStoredSshPrivateKey: true,
  hasStoredSshPassphrase: true,
  hasStoredTlsKeyPassphrase: true,
  ssh: {
    host: "bastion.local",
    username: "tunnel",
    authMethod: "privateKey",
    hostVerificationMode: "trustOnFirstUse",
  },
  tls: {
    mode: "mutualTls",
    caFilePath: "/ca.pem",
    certFilePath: "/client.pem",
    keyFilePath: "/client.key",
  },
};

describe("ConnectionFormView saving lock", () => {
  it.each([
    false,
    true,
  ])("locks native and custom controls until Save fails, retaining the draft (edit=%s)", async (edit) => {
    const user = userEvent.setup();
    const { container } = render(
      <ConnectionFormView existing={edit ? existing : null} />,
    );
    if (!edit) {
      await user.type(screen.getByLabelText("Connection name"), "Saving");
      await user.type(screen.getByLabelText("Database"), "app");
      await user.type(screen.getByLabelText("Username"), "reader");
    }
    await user.type(screen.getByLabelText("Connection folder"), "Draft");
    await user.type(screen.getByLabelText("Password"), "draft-secret");
    await user.click(
      screen.getByRole("button", {
        name: edit ? "Save Changes" : "Create Connection",
      }),
    );
    expect(getLastPostedMessage()).toMatchObject({
      type: "saveConnection",
      payload: { name: "Saving", folder: "Draft", password: "draft-secret" },
    });
    clearPostedMessages();

    for (const control of container.querySelectorAll(
      "input, select, textarea, button",
    )) {
      expect(control.matches(":disabled"), control.outerHTML).toBe(true);
    }
    for (const toggle of screen.getAllByRole("switch")) {
      expect(toggle.getAttribute("aria-disabled")).toBe("true");
      expect(toggle.tabIndex).toBe(-1);
      const checked = toggle.getAttribute("aria-checked");
      await user.click(toggle);
      fireEvent.keyDown(toggle, { key: " " });
      fireEvent.keyDown(toggle, { key: "Enter" });
      expect(toggle.getAttribute("aria-checked")).toBe(checked);
    }
    await user.click(screen.getByText("Open connection as read-only"));
    await user.tab();
    await user.keyboard("lost edit");
    await user.selectOptions(screen.getByLabelText("TLS mode"), "disabled");
    await user.click(screen.getByRole("button", { name: "SQLite" }));
    await user.click(screen.getByRole("button", { name: "Test Connection" }));
    await user.click(screen.getByRole("button", { name: "Saving…" }));
    await user.click(screen.getByRole("button", { name: "Cancel" }));
    expect(getLastPostedMessage()).toBeUndefined();
    expect(
      (screen.getByLabelText("Connection name") as HTMLInputElement).value,
    ).toBe("Saving");
    await expectNoAxeViolations(container);

    dispatchIncomingMessage("saveResult", {
      success: false,
      error: "Save failed; retry",
    });
    const name = screen.getByLabelText("Connection name");
    await waitFor(() => expect(name.matches(":disabled")).toBe(false));
    expect(screen.getByText(/Save failed; retry/)).toBeTruthy();
    expect((screen.getByLabelText("Password") as HTMLInputElement).value).toBe(
      "draft-secret",
    );
    const readOnly = screen.getByRole("switch", {
      name: "Open connection as read-only",
    });
    expect(readOnly.getAttribute("aria-disabled")).toBe("false");
    expect(readOnly.tabIndex).toBe(0);
    if (edit) {
      expect(
        screen
          .getByRole("switch", { name: /store secrets in vs code/i })
          .getAttribute("aria-disabled"),
      ).toBe("false");
    }
    await user.click(readOnly);
    await user.type(name, " retry");
    await user.click(
      screen.getByRole("button", {
        name: edit ? "Save Changes" : "Create Connection",
      }),
    );
    expect(getLastPostedMessage()).toMatchObject({
      type: "saveConnection",
      payload: {
        name: "Saving retry",
        folder: "Draft",
        password: "draft-secret",
        readOnly: true,
      },
    });
  });

  it.each([
    {
      id: "mongo",
      name: "MongoDB",
      type: "mongodb",
      mongoConnectionMode: "uri",
      connectionUri: "mongodb://localhost/app",
      hasStoredMongoUriCredentials: true,
    },
    {
      id: "redis",
      name: "Redis",
      type: "redis",
      connectionUri: "redis://localhost/0",
      hasStoredConnectionUriCredentials: true,
    },
    {
      id: "elastic",
      name: "Elasticsearch",
      type: "elasticsearch",
      endpoint: "https://localhost:9200",
      hasStoredApiKey: true,
      hasStoredConnectionUriCredentials: true,
    },
    {
      id: "dynamo",
      name: "DynamoDB",
      type: "dynamodb",
      awsRegion: "us-east-1",
      hasStoredAwsCredentials: true,
      hasStoredAwsKeyPair: true,
    },
    { id: "sqlite", name: "SQLite", type: "sqlite", filePath: "/app.db" },
  ] satisfies ConnectionFormExistingState[])("disables driver-specific custom switches while saving $name", async (connection) => {
    const user = userEvent.setup();
    render(<ConnectionFormView existing={connection} />);
    await user.click(screen.getByRole("button", { name: "Save Changes" }));
    expect(getLastPostedMessage()?.type).toBe("saveConnection");
    for (const toggle of screen.getAllByRole("switch")) {
      expect(toggle.getAttribute("aria-disabled")).toBe("true");
      expect(toggle.tabIndex).toBe(-1);
      const checked = toggle.getAttribute("aria-checked");
      await user.click(toggle);
      fireEvent.keyDown(toggle, { key: " " });
      expect(toggle.getAttribute("aria-checked")).toBe(checked);
    }
  });

  it.each([
    ["filePath", "Database file path"],
    ["tlsCaFile", "TLS CA certificate file"],
    ["tlsCertFile", "TLS client certificate file"],
    ["tlsKeyFile", "TLS client key file"],
  ])("ignores a delayed %s Browse result while saving", async (target, label) => {
    const user = userEvent.setup();
    render(
      <ConnectionFormView
        existing={
          target === "filePath"
            ? {
                id: "sqlite",
                name: "SQLite",
                type: "sqlite",
                filePath: "/app.db",
              }
            : existing
        }
      />,
    );
    const input = screen.getByLabelText(label) as HTMLInputElement;
    const originalPath = input.value;
    const browse = input.parentElement?.querySelector("button");
    expect(browse).toBeTruthy();
    await user.click(browse as HTMLButtonElement);
    expect(getLastPostedMessage()).toMatchObject({
      type: "browseFile",
      payload: { target },
    });
    await user.click(screen.getByRole("button", { name: "Save Changes" }));
    clearPostedMessages();
    dispatchIncomingMessage("browseFileResult", { target, filePath: "/late" });
    await user.click(browse as HTMLButtonElement);
    expect(input.value).toBe(originalPath);
    expect(getLastPostedMessage()).toBeUndefined();

    dispatchIncomingMessage("saveResult", { success: false, error: "Retry" });
    await waitFor(() => expect(input.matches(":disabled")).toBe(false));
    await user.click(browse as HTMLButtonElement);
    dispatchIncomingMessage("browseFileResult", { target, filePath: "/retry" });
    await waitFor(() => expect(input.value).toBe("/retry"));
  });
});
