import { useCallback, useRef, useState } from "react";
import type { MutationSnapshot } from "../../types";

export interface UndoRedoHistory {
  /** Push the current state BEFORE an action, so undo restores to this state */
  push: (snapshot: MutationSnapshot) => void;
  /** Undo: returns the snapshot to restore, or null if nothing to undo */
  undo: (currentState: MutationSnapshot) => MutationSnapshot | null;
  /** Redo: returns the snapshot to restore, or null if nothing to redo */
  redo: (currentState: MutationSnapshot) => MutationSnapshot | null;
  canUndo: boolean;
  canRedo: boolean;
  clear: () => void;
}

export function useUndoRedoHistory(): UndoRedoHistory {
  const pastRef = useRef<MutationSnapshot[]>([]);
  const futureRef = useRef<MutationSnapshot[]>([]);
  const [canUndo, setCanUndo] = useState(false);
  const [canRedo, setCanRedo] = useState(false);

  const push = useCallback((snapshot: MutationSnapshot) => {
    pastRef.current = [...pastRef.current, snapshot];
    futureRef.current = [];
    setCanUndo(true);
    setCanRedo(false);
  }, []);

  const undo = useCallback(
    (currentState: MutationSnapshot): MutationSnapshot | null => {
      const past = pastRef.current;
      if (past.length === 0) return null;

      const snapshot = past[past.length - 1];
      pastRef.current = past.slice(0, -1);
      futureRef.current = [...futureRef.current, currentState];

      setCanUndo(pastRef.current.length > 0);
      setCanRedo(true);

      return snapshot;
    },
    [],
  );

  const redo = useCallback(
    (currentState: MutationSnapshot): MutationSnapshot | null => {
      const future = futureRef.current;
      if (future.length === 0) return null;

      const snapshot = future[future.length - 1];
      futureRef.current = future.slice(0, -1);
      pastRef.current = [...pastRef.current, currentState];

      setCanRedo(futureRef.current.length > 0);
      setCanUndo(true);

      return snapshot;
    },
    [],
  );

  const clear = useCallback(() => {
    pastRef.current = [];
    futureRef.current = [];
    setCanUndo(false);
    setCanRedo(false);
  }, []);

  return { push, undo, redo, canUndo, canRedo, clear };
}
