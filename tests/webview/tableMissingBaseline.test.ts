import { describe, expect, it } from "vitest";
import {
  buildPendingRestoreState,
  parsePersistedTableDraft,
  restorePendingEditsSafely,
  serializeTableDraft,
} from "../../src/webview/components/table/tableViewHelpers";

describe("missing-field recovery baseline", () => {
  it.each([
    "__proto__",
    "constructor",
    "ordinary",
  ])("distinguishes missing and own undefined for %s", (name) => {
    const edits = new Map([[0, new Map([[name, "pending"]])]]);
    for (const exists of [false, true]) {
      const row = exists
        ? Object.fromEntries([
            ["id", 1],
            [name, undefined],
          ])
        : { id: 1 };
      const state = buildPendingRestoreState(edits, [row], ["id"]);
      const persisted = JSON.parse(
        JSON.stringify(serializeTableDraft("table", state, [])),
      );
      const parsed = parsePersistedTableDraft(persisted, "table");
      expect(parsed).not.toBeNull();
      const matching = restorePendingEditsSafely(
        parsed?.restoreState ?? null,
        [row],
        ["id"],
      );
      expect(matching.pendingEdits.get(0)?.get(name)).toBe("pending");
      const changed = exists
        ? { id: 1 }
        : Object.fromEntries([
            ["id", 1],
            [name, undefined],
          ]);
      const conflicted = restorePendingEditsSafely(
        parsed?.restoreState ?? null,
        [changed],
        ["id"],
      );
      expect(conflicted.unresolved).toHaveLength(1);
    }
  });

  it("keeps old undefined baselines conservative and rejects invalid missing markers", () => {
    const edits = new Map([[0, new Map([["constructor", "pending"]])]]);
    const state = buildPendingRestoreState(edits, [{ id: 1 }], ["id"]);
    const serialized = serializeTableDraft("table", state, []);
    const entry = serialized.restoreState.entries[0];
    delete entry.originalMissingFields;
    delete entry.baselinePresenceVersion;
    const legacy = parsePersistedTableDraft(serialized, "table");
    expect(
      restorePendingEditsSafely(
        legacy?.restoreState ?? null,
        [{ id: 1 }],
        ["id"],
      ).unresolved,
    ).toHaveLength(1);
    entry.originalMissingFields = ["other"];
    expect(parsePersistedTableDraft(serialized, "table")).toBeNull();
  });
});
