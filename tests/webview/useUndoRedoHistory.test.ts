import { act, renderHook } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { buildUndoRedoSnapshot } from "../../src/webview/components/table/tableViewHelpers";
import { useUndoRedoHistory } from "../../src/webview/components/table/useUndoRedoHistory";
import type { MutationSnapshot, PendingEdits } from "../../src/webview/types";

/* ------------------------------------------------------------------ */
/*  Helpers                                                           */
/* ------------------------------------------------------------------ */

/**
 * Type-narrowing helper that throws if the value is undefined.
 * Avoids non-null assertions (``!``) in test assertions.
 */
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
    newRow: null,
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

/* ------------------------------------------------------------------ */
/*  Tests                                                             */
/* ------------------------------------------------------------------ */

describe("useUndoRedoHistory", () => {
  /* ---------- initial state ---------- */

  it("starts with canUndo=false and canRedo=false", () => {
    const { result } = renderHook(() => useUndoRedoHistory());

    expect(result.current.canUndo).toBe(false);
    expect(result.current.canRedo).toBe(false);
  });

  /* ---------- push ---------- */

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
    act(() => result.current.undo(makeSnapshot())); // pop s1 -> future = [currentState]
    act(() => result.current.push(s2)); // should clear future

    // Redo should not be available because push cleared the future
    expect(result.current.canRedo).toBe(false);

    // Undoing once should return s2, then nothing left
    act(() => {
      const restored = result.current.undo(makeSnapshot());
      expect(restored).toEqual(s2);
    });

    act(() => {
      const restored = result.current.undo(makeSnapshot());
      expect(restored).toBeNull();
    });
  });

  /* ---------- undo ---------- */

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
    const currentState = makeSnapshot({ newRow: { col1: { value: "x" } } });

    act(() => result.current.push(snapshot1));
    act(() => result.current.undo(currentState));

    // Now redo should return currentState
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

    // canUndo = true, canRedo = false
    expect(result.current.canUndo).toBe(true);
    expect(result.current.canRedo).toBe(false);

    // Undo first
    act(() => result.current.undo(makeSnapshot()));
    expect(result.current.canUndo).toBe(true); // still one left
    expect(result.current.canRedo).toBe(true);

    // Undo second: past is now empty
    act(() => result.current.undo(makeSnapshot()));
    expect(result.current.canUndo).toBe(false);
    expect(result.current.canRedo).toBe(true);
  });

  /* ---------- redo ---------- */

  it("redo() returns the most recent snapshot from the future stack", () => {
    const { result } = renderHook(() => useUndoRedoHistory());

    const snapshot1 = makeSnapshot();
    const currentState = makeSnapshot();

    act(() => result.current.push(snapshot1));
    act(() => result.current.undo(currentState)); // future = [currentState]

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
    const currentState2 = makeSnapshot({ newRow: { c: { value: "y" } } });

    act(() => result.current.push(snapshot1));
    act(() => result.current.undo(currentState)); // future = [currentState]
    act(() => result.current.redo(currentState2)); // past = [snapshot1, currentState2]

    // Undo should return currentState2
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
    act(() => result.current.undo(makeSnapshot())); // future = [currentState]

    expect(result.current.canUndo).toBe(false);
    expect(result.current.canRedo).toBe(true);

    // Redo: future is now empty
    act(() => result.current.redo(makeSnapshot()));
    expect(result.current.canUndo).toBe(true);
    expect(result.current.canRedo).toBe(false);
  });

  /* ---------- clear ---------- */

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

    // Undo and redo should both return null after clear
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

  /* ---------- multiple undo/redo ---------- */

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

    // Each undo call receives the REAL current state at that moment
    const current1 = makeSnapshot({
      pendingEdits: pendingEditsFrom([[0, { x: 10 }]]),
    });
    const current2 = makeSnapshot({
      pendingEdits: pendingEditsFrom([[0, { x: 20 }]]),
    });

    // push s1, s2, s3
    act(() => result.current.push(s1));
    act(() => result.current.push(s2));
    act(() => result.current.push(s3));

    // undo with current1 -> returns s3, future = [current1]
    let r: MutationSnapshot | null = null;
    act(() => {
      r = result.current.undo(current1);
    });
    expect(r).toEqual(s3);

    // undo with current2 -> returns s2, future = [current1, current2]
    act(() => {
      r = result.current.undo(current2);
    });
    expect(r).toEqual(s2);

    // redo -> returns current2 (last pushed to future), past = [s1, current2]
    act(() => {
      r = result.current.redo(makeSnapshot());
    });
    expect(r).toEqual(current2);

    // redo -> returns current1, past = [s1, current2, current1]
    act(() => {
      r = result.current.redo(makeSnapshot());
    });
    expect(r).toEqual(current1);

    // redo -> future is empty, returns null
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

    // future = [currentState, currentState] - now push should discard
    act(() => result.current.push(s3));

    expect(result.current.canRedo).toBe(false);

    // Only s3 should be undoable
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

  /* ---------- deep cloning isolation ---------- */

  it("stores snapshots by reference: caller must clone before pushing", () => {
    const { result } = renderHook(() => useUndoRedoHistory());

    const pending1 = pendingEditsFrom([[0, { a: "original" }]]);
    const snapshot1 = makeSnapshot({ pendingEdits: pending1 });

    act(() => result.current.push(snapshot1));

    // The hook stores the reference, so mutating the pendingEdits IS visible
    assertPresent(pending1.get(0), "row 0").set("a", "mutated");

    const restored = result.current.undo(makeSnapshot());
    expect(restored).not.toBeNull();
    // Because the hook stores by reference, the mutation IS visible
    expect(restored?.pendingEdits.get(0)?.get("a")).toBe("mutated");

    // This is why buildUndoRedoSnapshot clones - the caller should use it
    // to create an independent snapshot before pushing
    const pending2 = pendingEditsFrom([[0, { a: "safe" }]]);
    const snapshot2 = buildUndoRedoSnapshot(pending2, null, null);
    act(() => result.current.push(snapshot2));

    assertPresent(pending2.get(0), "row 0").set("a", "mutated again");

    const restored2 = result.current.undo(makeSnapshot());
    expect(restored2?.pendingEdits.get(0)?.get("a")).toBe("safe");
  });
});
