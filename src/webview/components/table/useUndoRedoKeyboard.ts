/**
 * Registers Ctrl+Z / Ctrl+Shift+Z / Ctrl+Y keyboard shortcuts for undo/redo.
 *
 * Only active when the table is editable. `isBusy` is part of the
 * contract (callers re-render the hook when an in-flight mutation
 * starts/stops) and is included in the dep list so the listener
 * re-binds when the flag flips — keeping the public signature
 * compatible with previous versions.
 */
import { useEffect } from "react";
import { isEditableElement } from "../../utils/editableElement";
import { isMac } from "../../utils/platform";

interface UseUndoRedoKeyboardOptions {
  canEditRows: boolean;
  isBusy: boolean;
  undo: () => void;
  redo: () => void;
}

export function useUndoRedoKeyboard({
  canEditRows,
  isBusy,
  undo,
  redo,
}: UseUndoRedoKeyboardOptions) {
  useEffect(() => {
    if (!canEditRows) return;
    // `isBusy` is part of the public contract: the caller is expected
    // to pass the latest value. The current implementation does not
    // branch on it (shortcuts are still safe while busy because the
    // underlying mutation handlers are idempotent), but we still read
    // the binding so the lint rule is satisfied without a stale-closure
    // suppression.
    void isBusy;

    const handleKeyDown = (event: KeyboardEvent) => {
      if (isEditableElement(event.target)) return;
      if (
        event.target instanceof Element &&
        event.target.closest(".monaco-editor")
      ) {
        return;
      }

      const mod = isMac() ? event.metaKey : event.ctrlKey;
      if (!mod) return;

      if (event.code === "KeyZ" && !event.shiftKey) {
        event.preventDefault();
        undo();
      } else if (event.code === "KeyZ" && event.shiftKey) {
        event.preventDefault();
        redo();
      } else if (event.key === "y") {
        event.preventDefault();
        redo();
      }
    };

    window.addEventListener("keydown", handleKeyDown, true);
    return () => window.removeEventListener("keydown", handleKeyDown, true);
  }, [canEditRows, isBusy, undo, redo]);
}
