import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { Icon } from "../../src/webview/components/Icon";

describe("Icon", () => {
  it("renders the requested codicon classes and label", () => {
    render(<Icon name="plug" title="Connect" size={16} />);

    const icon = screen.getByRole("img", { name: "Connect" });
    expect(icon.tagName).toBe("SPAN");
    expect(icon.getAttribute("title")).toBe("Connect");
    expect(icon.className).toContain("codicon");
    expect(icon.className).toContain("codicon-plug");
  });
});
