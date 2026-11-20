import { useCallback, useMemo, useRef, useState } from "react";
import type { MutationSnapshot } from "../../types";

// Bound full snapshots in memory, not the current edits or persisted draft.
// Undo/redo move one step between stacks, so only push needs to enforce the cap.
const MAX_HISTORY_STEPS = 100;

export interface UndoRedoHistory {
  push: (snapshot: MutationSnapshot) => void;
  undo: (currentState: MutationSnapshot) => MutationSnapshot | null;
  redo: (currentState: MutationSnapshot) => MutationSnapshot | null;
  canUndo: boolean;
  canRedo: boolean;
  clear: () => void;
  hasUnresolvedEdits: () => boolean;
  remap: (transform: (snapshot: MutationSnapshot) => MutationSnapshot) => void;
}

export function useUndoRedoHistory(): UndoRedoHistory {
  const pastRef = useRef<MutationSnapshot[]>([]);
  const futureRef = useRef<MutationSnapshot[]>([]);
  const [canUndo, setCanUndo] = useState(false);
  const [canRedo, setCanRedo] = useState(false);

  const push = useCallback((snapshot: MutationSnapshot) => {
    const past = pastRef.current;
    past.push(snapshot);
    if (past.length > MAX_HISTORY_STEPS) past.shift();
    futureRef.current.length = 0;
    setCanUndo(true);
    setCanRedo(false);
  }, []);

  const undo = useCallback(
    (currentState: MutationSnapshot): MutationSnapshot | null => {
      const past = pastRef.current;
      const snapshot = past.pop();
      if (!snapshot) return null;
      futureRef.current.push(currentState);

      setCanUndo(past.length > 0);
      setCanRedo(true);

      return snapshot;
    },
    [],
  );

  const redo = useCallback(
    (currentState: MutationSnapshot): MutationSnapshot | null => {
      const future = futureRef.current;
      const snapshot = future.pop();
      if (!snapshot) return null;
      pastRef.current.push(currentState);

      setCanRedo(future.length > 0);
      setCanUndo(true);

      return snapshot;
    },
    [],
  );

  const clear = useCallback(() => {
    pastRef.current.length = 0;
    futureRef.current.length = 0;
    setCanUndo(false);
    setCanRedo(false);
  }, []);

  const remap = useCallback(
    (transform: (snapshot: MutationSnapshot) => MutationSnapshot) => {
      pastRef.current = pastRef.current.map(transform);
      futureRef.current = futureRef.current.map(transform);
    },
    [],
  );

  const hasUnresolvedEdits = useCallback(() => {
    const isUnresolved = (snapshot: MutationSnapshot) =>
      (snapshot.unresolvedPendingEdits?.length ?? 0) > 0;
    return (
      pastRef.current.some(isUnresolved) || futureRef.current.some(isUnresolved)
    );
  }, []);

  return useMemo(
    () => ({
      push,
      undo,
      redo,
      canUndo,
      canRedo,
      clear,
      remap,
      hasUnresolvedEdits,
    }),
    [push, undo, redo, canUndo, canRedo, clear, remap, hasUnresolvedEdits],
  );
}
