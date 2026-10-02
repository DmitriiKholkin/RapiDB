import { describe, expect, it } from "vitest";
import {
  parseBookmarkSavedPayload,
  parseQueryPanelMessage,
} from "../../src/shared/webviewContracts";

describe("query bookmark request contract", () => {
  it("preserves request identity and accepts legacy requests without it", () => {
    expect(
      parseQueryPanelMessage({
        type: "addBookmark",
        payload: {
          queryText: "select 1",
          connectionId: "conn:1",
          requestId: "panel:bookmark:1",
        },
      }),
    ).toEqual({
      type: "addBookmark",
      payload: {
        queryText: "select 1",
        sql: "select 1",
        connectionId: "conn:1",
        requestId: "panel:bookmark:1",
      },
    });
    expect(
      parseQueryPanelMessage({
        type: "addBookmark",
        payload: { sql: "select 1" },
      })?.payload,
    ).toMatchObject({ queryText: "select 1", sql: "select 1" });
  });

  it.each(
    [null, 1, true, {}, [], "", "   "].map((requestId) => ({ requestId })),
  )("rejects an invalid supplied request ID: $requestId", ({ requestId }) => {
    expect(
      parseQueryPanelMessage({
        type: "addBookmark",
        payload: { queryText: "select 1", requestId },
      }),
    ).toBeNull();
    expect(parseBookmarkSavedPayload({ ok: true, requestId })).toBeNull();
  });

  it("parses typed success, failure and legacy replies", () => {
    expect(
      parseBookmarkSavedPayload({ ok: true, requestId: "bookmark:1" }),
    ).toEqual({ ok: true, requestId: "bookmark:1" });
    expect(
      parseBookmarkSavedPayload({
        ok: false,
        requestId: "bookmark:2",
        error: "Storage failed",
      }),
    ).toEqual({ ok: false, requestId: "bookmark:2", error: "Storage failed" });
    expect(parseBookmarkSavedPayload({ ok: true })).toEqual({ ok: true });
    expect(
      parseBookmarkSavedPayload({ ok: "yes", requestId: "bookmark:1" }),
    ).toBeNull();
    expect(parseBookmarkSavedPayload({ ok: false, error: 123 })).toBeNull();
    expect(parseBookmarkSavedPayload(null)).toBeNull();
  });
});
