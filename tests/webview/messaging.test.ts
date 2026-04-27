import { describe, expect, it, vi } from "vitest";
import { onMessage, postMessage } from "../../src/webview/utils/messaging";
import {
  clearPostedMessages,
  dispatchIncomingMessage,
  dispatchWindowMessage,
  getPostedMessages,
} from "./testUtils";

describe("messaging", () => {
  it("posts envelopes to the VS Code bridge", () => {
    clearPostedMessages();

    postMessage("getConnections");
    postMessage("executeQuery", { sql: "select 1", connectionId: "conn-1" });

    expect(getPostedMessages()).toEqual([
      { type: "getConnections" },
      {
        type: "executeQuery",
        payload: { sql: "select 1", connectionId: "conn-1" },
      },
    ]);
  });

  it("routes inbound messages to matching handlers and unsubscribes cleanly", () => {
    const schemaHandler = vi.fn();
    const queryHandler = vi.fn();

    const unsubscribeSchema = onMessage("schema", schemaHandler);
    const unsubscribeQuery = onMessage("queryResult", queryHandler);

    dispatchIncomingMessage("schema", { connectionId: "conn-1" });
    dispatchIncomingMessage("queryResult", { rowCount: 1 });

    expect(schemaHandler).toHaveBeenCalledWith({ connectionId: "conn-1" });
    expect(queryHandler).toHaveBeenCalledWith({ rowCount: 1 });

    unsubscribeSchema();
    unsubscribeQuery();

    dispatchIncomingMessage("schema", { connectionId: "conn-2" });
    dispatchIncomingMessage("queryResult", { rowCount: 2 });

    expect(schemaHandler).toHaveBeenCalledTimes(1);
    expect(queryHandler).toHaveBeenCalledTimes(1);
  });

  it("ignores malformed window messages", () => {
    const handler = vi.fn();
    const unsubscribe = onMessage("schema", handler);

    expect(() => dispatchWindowMessage(null)).not.toThrow();
    expect(() => dispatchWindowMessage("schema")).not.toThrow();
    expect(() =>
      dispatchWindowMessage({ payload: { connectionId: "conn-1" } }),
    ).not.toThrow();

    expect(handler).not.toHaveBeenCalled();

    unsubscribe();
  });
});
