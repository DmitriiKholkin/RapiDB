import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";
import { ConnectionFormView } from "../../src/webview/components/ConnectionFormView";

describe("ConnectionFormView SSH trust-on-first-use hint", () => {
  it("says the pin comes from the first SSH handshake, not a database login", async () => {
    const user = userEvent.setup();
    render(<ConnectionFormView existing={null} />);

    await user.click(
      screen.getByRole("switch", { name: /connect through ssh bastion/i }),
    );
    await user.selectOptions(
      screen.getByLabelText("SSH host verification mode"),
      ["trustOnFirstUse"],
    );

    const hint = screen.getByText(/first successful ssh handshake/i);
    expect(hint.textContent).toMatch(
      /pin the discovered SHA256 fingerprint automatically, even if the database login then fails/,
    );
    expect(hint.textContent).toMatch(/Test Connection does not save a pin/);
    expect(hint.textContent).not.toMatch(/database connection through SSH/);
  });
});
