import { act, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";
import type { ConnectionFormSubmission } from "../../src/shared/webviewContracts/connection";
import { ConnectionFormView } from "../../src/webview/components/ConnectionFormView";
import { dispatchIncomingMessage, getLastPostedMessage } from "./testUtils";

const PIN = "SHA256:AbCdEfGhIjKlMnOpQrStUvWxYz0123456789+/";

const pinnedProfile = {
  id: "tunnelled",
  name: "Tunnelled",
  type: "pg" as const,
  host: "db.internal",
  port: 5432,
  database: "app",
  username: "qa",
  password: undefined,
  ssh: {
    host: "bastion.example.com",
    port: 22,
    username: "tunnel",
    authMethod: "privateKey" as const,
    hostVerificationMode: "trustOnFirstUse" as const,
    hostFingerprintSha256: PIN,
  },
  hasStoredSshPrivateKey: true,
};

async function save(user: ReturnType<typeof userEvent.setup>) {
  await user.click(screen.getByRole("button", { name: "Save Changes" }));
  act(() => dispatchIncomingMessage("saveResult", { success: true }));
}

const payload = () =>
  getLastPostedMessage()?.payload as ConnectionFormSubmission;

describe("trust-on-first-use SSH fingerprint pin", () => {
  it("keeps the pin while the SSH target is unchanged", async () => {
    const user = userEvent.setup();
    render(<ConnectionFormView existing={pinnedProfile} />);

    await save(user);

    expect(payload().ssh?.hostFingerprintSha256).toBe(PIN);
  });

  it.each([
    ["SSH host", "new-bastion.example.com"],
    ["SSH port", "2222"],
    ["SSH username", "other"],
  ])("drops the pin when the %s changes", async (label, value) => {
    const user = userEvent.setup();
    render(<ConnectionFormView existing={pinnedProfile} />);

    await user.clear(screen.getByLabelText(label));
    await user.type(screen.getByLabelText(label), value);
    await save(user);

    expect(payload().ssh?.hostFingerprintSha256).toBeUndefined();
    expect(
      screen.queryByRole("button", { name: "Reset pinned fingerprint" }),
    ).toBeNull();
  });

  it("resets the pin from the form", async () => {
    const user = userEvent.setup();
    render(<ConnectionFormView existing={pinnedProfile} />);

    expect(screen.getByText(new RegExp(PIN.slice(7, 20)))).toBeTruthy();
    await user.click(
      screen.getByRole("button", { name: "Reset pinned fingerprint" }),
    );
    await save(user);

    expect(payload().ssh?.hostFingerprintSha256).toBeUndefined();
    expect(
      screen.queryByRole("button", { name: "Reset pinned fingerprint" }),
    ).toBeNull();
  });
});

describe("stored password across a type change", () => {
  it("does not offer to keep the old stored password after switching type", async () => {
    const user = userEvent.setup();
    render(
      <ConnectionFormView
        existing={{
          id: "pg",
          name: "Warehouse",
          type: "pg",
          host: "db.local",
          port: 5432,
          database: "app",
          username: "qa",
          hasStoredSecret: true,
        }}
      />,
    );
    expect(
      screen.getByText("Leave blank to keep the stored password unchanged."),
    ).toBeTruthy();

    await user.click(screen.getByRole("button", { name: "MySQL / MariaDB" }));

    expect(
      screen.queryByText("Leave blank to keep the stored password unchanged."),
    ).toBeNull();
    expect(
      screen.queryByRole("switch", { name: "Remove saved password" }),
    ).toBeNull();
    await save(user);
    expect(payload().hasStoredSecret).toBeUndefined();
  });
});
