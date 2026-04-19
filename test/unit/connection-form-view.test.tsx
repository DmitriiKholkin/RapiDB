/**
 * @vitest-environment jsdom
 */

import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ConnectionFormView } from "../../src/webview/components/ConnectionFormView";

afterEach(cleanup);

const postMessage = vi.fn();
const getState = vi.fn();
const setState = vi.fn();

describe("ConnectionFormView", () => {
  beforeEach(() => {
    postMessage.mockReset();
    getState.mockReset();
    setState.mockReset();
    (
      window as Window & {
        __vscode?: {
          postMessage: typeof postMessage;
          getState: typeof getState;
          setState: typeof setState;
        };
      }
    ).__vscode = {
      postMessage,
      getState,
      setState,
    };
  });

  it("resets the port when the selected connection type changes", () => {
    render(<ConnectionFormView />);

    const typeSelect = screen.getByRole("combobox");

    fireEvent.change(typeSelect, { target: { value: "mysql" } });
    expect(screen.getByDisplayValue("3306")).toBeDefined();

    fireEvent.change(typeSelect, { target: { value: "oracle" } });
    expect(screen.getByDisplayValue("1521")).toBeDefined();
  });

  it("posts the selected shared connection type and default port", () => {
    render(<ConnectionFormView />);

    fireEvent.change(screen.getByPlaceholderText("My Database"), {
      target: { value: "Analytics" },
    });
    fireEvent.change(screen.getByRole("combobox"), {
      target: { value: "mssql" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Test Connection" }));

    expect(postMessage).toHaveBeenCalledWith({
      type: "testConnection",
      payload: expect.objectContaining({
        name: "Analytics",
        type: "mssql",
        host: "localhost",
        port: 1433,
        username: "",
      }),
    });
  });
});
