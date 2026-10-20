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
