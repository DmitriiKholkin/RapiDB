import { act, renderHook } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { buildUndoRedoSnapshot } from "../../src/webview/components/table/tableViewHelpers";
import { useUndoRedoHistory } from "../../src/webview/components/table/useUndoRedoHistory";
import type { MutationSnapshot, PendingEdits } from "../../src/webview/types";

function assertPresent<T>(value: T | undefined, label?: string): T {
  if (value === undefined) {
    throw new Error(
      label ? `${label} is undefined` : "Expected value to be defined",
    );
  }
  return value;
}

function makeSnapshot(
  overrides: Partial<MutationSnapshot> = {},
): MutationSnapshot {
  return {
    pendingEdits: new Map(),
    newRows: [],
    editCell: null,
    ...overrides,
  };
}

function pendingEditsFrom(
  entries: Array<[number, Record<string, unknown>]>,
): PendingEdits {
  return new Map(
    entries.map(([row, cols]) => [row, new Map(Object.entries(cols))]),
  );
}

describe("useUndoRedoHistory", () => {
  it("tracks unresolved identities in both stacks until remapped or cleared", () => {
    const { result } = renderHook(() => useUndoRedoHistory());
    const snapshot = makeSnapshot({
      unresolvedPendingEdits: [
        {
          originalSignature: "original B",
          changes: new Map([["name", "first"]]),
        },
      ],
    });
    expect(result.current.hasUnresolvedEdits()).toBe(false);
    act(() => result.current.push(snapshot));
    expect(result.current.hasUnresolvedEdits()).toBe(true);
    act(() => result.current.undo(snapshot));
    expect(result.current.canUndo).toBe(false);
    expect(result.current.canRedo).toBe(true);
    expect(result.current.hasUnresolvedEdits()).toBe(true);
    act(() =>
      result.current.remap((entry) => ({
        ...entry,
        unresolvedPendingEdits: [],
      })),
    );
    expect(result.current.hasUnresolvedEdits()).toBe(false);
    act(() => result.current.push(snapshot));
    act(() => result.current.clear());
    expect(result.current.hasUnresolvedEdits()).toBe(false);
  });

  it("starts with canUndo=false and canRedo=false", () => {
    const { result } = renderHook(() => useUndoRedoHistory());

    expect(result.current.canUndo).toBe(false);
    expect(result.current.canRedo).toBe(false);
  });

  it("push() sets canUndo to true and canRedo to false", () => {
    const { result } = renderHook(() => useUndoRedoHistory());

    act(() => {
      result.current.push(makeSnapshot());
    });

    expect(result.current.canUndo).toBe(true);
    expect(result.current.canRedo).toBe(false);
  });

  it("push() after undo clears the future stack", () => {
    const { result } = renderHook(() => useUndoRedoHistory());

    const s1 = makeSnapshot();
    const s2 = makeSnapshot();

    act(() => result.current.push(s1));
    act(() => result.current.undo(makeSnapshot()));
    act(() => result.current.push(s2));

    expect(result.current.canRedo).toBe(false);

    act(() => {
      const restored = result.current.undo(makeSnapshot());
      expect(restored).toEqual(s2);
    });

    act(() => {
      const restored = result.current.undo(makeSnapshot());
      expect(restored).toBeNull();
    });
  });

  it("undo() returns the most recent snapshot from the past stack", () => {
    const { result } = renderHook(() => useUndoRedoHistory());

    const snapshot1 = makeSnapshot({
      pendingEdits: pendingEditsFrom([[0, { a: 1 }]]),
    });
    const snapshot2 = makeSnapshot({
      pendingEdits: pendingEditsFrom([[0, { a: 2 }]]),
    });
    const currentState = makeSnapshot();

    act(() => result.current.push(snapshot1));
    act(() => result.current.push(snapshot2));

    let restored: MutationSnapshot | null = null;
    act(() => {
      restored = result.current.undo(currentState);
    });

    expect(restored).toEqual(snapshot2);
  });

  it("undo() pushes the current state onto the future stack", () => {
    const { result } = renderHook(() => useUndoRedoHistory());

    const snapshot1 = makeSnapshot();
    const currentState = makeSnapshot({ newRows: [{ col1: { value: "x" } }] });

    act(() => result.current.push(snapshot1));
    act(() => result.current.undo(currentState));

    let redone: MutationSnapshot | null = null;
    act(() => {
      redone = result.current.redo(makeSnapshot());
    });
    expect(redone).toEqual(currentState);
  });

  it("undo() returns null when past is empty", () => {
    const { result } = renderHook(() => useUndoRedoHistory());

    let restored: MutationSnapshot | null = null;
    act(() => {
      restored = result.current.undo(makeSnapshot());
    });

    expect(restored).toBeNull();
  });

  it("undo() updates canUndo and canRedo correctly", () => {
    const { result } = renderHook(() => useUndoRedoHistory());

    act(() => result.current.push(makeSnapshot()));
    act(() => result.current.push(makeSnapshot()));

    expect(result.current.canUndo).toBe(true);
    expect(result.current.canRedo).toBe(false);

    act(() => result.current.undo(makeSnapshot()));
    expect(result.current.canUndo).toBe(true);
    expect(result.current.canRedo).toBe(true);

    act(() => result.current.undo(makeSnapshot()));
    expect(result.current.canUndo).toBe(false);
    expect(result.current.canRedo).toBe(true);
  });

  it("redo() returns the most recent snapshot from the future stack", () => {
    const { result } = renderHook(() => useUndoRedoHistory());

    const snapshot1 = makeSnapshot();
    const currentState = makeSnapshot();

    act(() => result.current.push(snapshot1));
    act(() => result.current.undo(currentState));

    let redone: MutationSnapshot | null = null;
    act(() => {
      redone = result.current.redo(makeSnapshot());
    });

    expect(redone).toEqual(currentState);
  });

  it("redo() pushes the current state onto the past stack", () => {
    const { result } = renderHook(() => useUndoRedoHistory());

    const snapshot1 = makeSnapshot();
    const currentState = makeSnapshot();
    const currentState2 = makeSnapshot({ newRows: [{ c: { value: "y" } }] });

    act(() => result.current.push(snapshot1));
    act(() => result.current.undo(currentState));
    act(() => result.current.redo(currentState2));

    let restored: MutationSnapshot | null = null;
    act(() => {
      restored = result.current.undo(makeSnapshot());
    });
    expect(restored).toEqual(currentState2);
  });

  it("redo() returns null when future is empty", () => {
    const { result } = renderHook(() => useUndoRedoHistory());

    let redone: MutationSnapshot | null = null;
    act(() => {
      redone = result.current.redo(makeSnapshot());
    });

    expect(redone).toBeNull();
  });

  it("redo() updates canUndo and canRedo correctly", () => {
    const { result } = renderHook(() => useUndoRedoHistory());

    act(() => result.current.push(makeSnapshot()));
    act(() => result.current.undo(makeSnapshot()));

    expect(result.current.canUndo).toBe(false);
    expect(result.current.canRedo).toBe(true);

    act(() => result.current.redo(makeSnapshot()));
    expect(result.current.canUndo).toBe(true);
    expect(result.current.canRedo).toBe(false);
  });

  it("clear() resets both stacks and flags", () => {
    const { result } = renderHook(() => useUndoRedoHistory());

    act(() => result.current.push(makeSnapshot()));
    act(() => result.current.push(makeSnapshot()));
    act(() => result.current.undo(makeSnapshot()));

    expect(result.current.canUndo).toBe(true);
    expect(result.current.canRedo).toBe(true);

    act(() => result.current.clear());

    expect(result.current.canUndo).toBe(false);
    expect(result.current.canRedo).toBe(false);

    let restored: MutationSnapshot | null = null;
    act(() => {
      restored = result.current.undo(makeSnapshot());
    });
    expect(restored).toBeNull();

    let redone: MutationSnapshot | null = null;
    act(() => {
      redone = result.current.redo(makeSnapshot());
    });
    expect(redone).toBeNull();
  });

  it("supports a sequence of push -> undo -> undo -> redo -> redo", () => {
    const { result } = renderHook(() => useUndoRedoHistory());

    const s1 = makeSnapshot({
      pendingEdits: pendingEditsFrom([[0, { x: 1 }]]),
    });
    const s2 = makeSnapshot({
      pendingEdits: pendingEditsFrom([[0, { x: 2 }]]),
    });
    const s3 = makeSnapshot({
      pendingEdits: pendingEditsFrom([[0, { x: 3 }]]),
    });

    const current1 = makeSnapshot({
      pendingEdits: pendingEditsFrom([[0, { x: 10 }]]),
    });
    const current2 = makeSnapshot({
      pendingEdits: pendingEditsFrom([[0, { x: 20 }]]),
    });

    act(() => result.current.push(s1));
    act(() => result.current.push(s2));
    act(() => result.current.push(s3));

    let r: MutationSnapshot | null = null;
    act(() => {
      r = result.current.undo(current1);
    });
    expect(r).toEqual(s3);

    act(() => {
      r = result.current.undo(current2);
    });
    expect(r).toEqual(s2);

    act(() => {
      r = result.current.redo(makeSnapshot());
    });
    expect(r).toEqual(current2);

    act(() => {
      r = result.current.redo(makeSnapshot());
    });
    expect(r).toEqual(current1);

    act(() => {
      r = result.current.redo(makeSnapshot());
    });
    expect(r).toBeNull();
  });

  it("push after multiple undos discards the redo stack", () => {
    const { result } = renderHook(() => useUndoRedoHistory());

    const s1 = makeSnapshot();
    const s2 = makeSnapshot();
    const s3 = makeSnapshot();

    act(() => result.current.push(s1));
    act(() => result.current.push(s2));
    act(() => result.current.undo(makeSnapshot()));
    act(() => result.current.undo(makeSnapshot()));

    act(() => result.current.push(s3));

    expect(result.current.canRedo).toBe(false);

    let r: MutationSnapshot | null = null;
    act(() => {
      r = result.current.undo(makeSnapshot());
    });
    expect(r).toEqual(s3);

    act(() => {
      r = result.current.undo(makeSnapshot());
    });
    expect(r).toBeNull();
  });

  it("keeps only the 100 newest history steps and redoes them back to the latest state", () => {
    const { result } = renderHook(() => useUndoRedoHistory());
    const snapshots = Array.from({ length: 103 }, (_, value) =>
      makeSnapshot({ pendingEdits: pendingEditsFrom([[0, { value }]]) }),
    );
    const latest = makeSnapshot({
      newRows: [{ a: { value: "latest draft" } }],
    });
    let current = latest;

    act(() => {
      for (const snapshot of snapshots) result.current.push(snapshot);
    });
    expect(result.current.canUndo).toBe(true);
    expect(result.current.canRedo).toBe(false);

    act(() => {
      for (const expected of snapshots.slice(3).reverse()) {
        expect(result.current.undo(current)).toBe(expected);
        current = expected;
      }
      expect(result.current.undo(current)).toBeNull();
    });
    expect(result.current.canUndo).toBe(false);
    expect(result.current.canRedo).toBe(true);

    act(() => {
      for (const expected of [...snapshots.slice(4), latest]) {
        expect(result.current.redo(current)).toBe(expected);
        current = expected;
      }
      expect(result.current.redo(current)).toBeNull();
    });
    expect(result.current.canUndo).toBe(true);
    expect(result.current.canRedo).toBe(false);
  });

  it("drops the redo branch after an over-cap history while keeping earlier retained edits", () => {
    const { result } = renderHook(() => useUndoRedoHistory());
    const evicted = makeSnapshot({
      unresolvedPendingEdits: [
        {
          originalSignature: "evicted",
          changes: new Map([["name", "old edit"]]),
        },
      ],
    });
    const earlier = makeSnapshot({
      pendingEdits: pendingEditsFrom([[0, { a: 1 }]]),
    });
    const branchPoint = makeSnapshot({
      pendingEdits: pendingEditsFrom([[0, { a: 2 }]]),
    });
    const abandoned = makeSnapshot({
      pendingEdits: pendingEditsFrom([[0, { a: 3 }]]),
    });
    const newBranch = makeSnapshot({
      pendingEdits: pendingEditsFrom([[0, { a: 4 }]]),
    });

    act(() => {
      result.current.push(evicted);
      for (let i = 0; i < 98; i++) result.current.push(makeSnapshot());
      result.current.push(earlier);
      result.current.push(branchPoint);
    });
    expect(result.current.hasUnresolvedEdits()).toBe(false);

    act(() => {
      expect(result.current.undo(abandoned)).toBe(branchPoint);
    });
    expect(result.current.canRedo).toBe(true);

    act(() => result.current.push(branchPoint));
    expect(result.current.canRedo).toBe(false);

    act(() => {
      expect(result.current.redo(newBranch)).toBeNull();
      expect(result.current.undo(newBranch)).toBe(branchPoint);
      expect(result.current.undo(branchPoint)).toBe(earlier);
    });
  });

  it("stores snapshots by reference: caller must clone before pushing", () => {
    const { result } = renderHook(() => useUndoRedoHistory());

    const pending1 = pendingEditsFrom([[0, { a: "original" }]]);
    const snapshot1 = makeSnapshot({ pendingEdits: pending1 });

    act(() => result.current.push(snapshot1));

    assertPresent(pending1.get(0), "row 0").set("a", "mutated");

    const restored = result.current.undo(makeSnapshot());
    expect(restored).not.toBeNull();
    expect(restored?.pendingEdits.get(0)?.get("a")).toBe("mutated");

    const pending2 = pendingEditsFrom([[0, { a: "safe" }]]);
    const snapshot2 = buildUndoRedoSnapshot(pending2, [], null);
    act(() => result.current.push(snapshot2));

    assertPresent(pending2.get(0), "row 0").set("a", "mutated again");

    const restored2 = result.current.undo(makeSnapshot());
    expect(restored2?.pendingEdits.get(0)?.get("a")).toBe("safe");
  });
});
