/** Selection editing helpers for Monaco. */
import type * as monaco from "monaco-editor";

/**
 * Compute the range of the current selection, or the caret position
 * when nothing is selected. Used as the insertion target for paste.
 */
function selectionRangeOrCaret(
  editor: monaco.editor.IStandaloneCodeEditor,
): monaco.IRange {
  const model = editor.getModel();
  const selection = editor.getSelection();
  if (model && selection) {
    return {
      startLineNumber: selection.startLineNumber,
      startColumn: selection.startColumn,
      endLineNumber: selection.endLineNumber,
      endColumn: selection.endColumn,
    };
  }
  const pos = editor.getPosition() ?? { lineNumber: 1, column: 1 };
  return {
    startLineNumber: pos.lineNumber,
    startColumn: pos.column,
    endLineNumber: pos.lineNumber,
    endColumn: pos.column,
  };
}

/**
 * Insert text at the current selection, replacing any active selection.
 * No-op when the editor is read-only.
 */
export function insertTextAtSelection(
  editor: monaco.editor.IStandaloneCodeEditor,
  text: string,
  isReadOnly: () => boolean,
): void {
  if (isReadOnly()) {
    return;
  }
  const model = editor.getModel();
  if (!model) {
    editor.trigger("keyboard", "type", { text });
    return;
  }
  const range = selectionRangeOrCaret(editor);
  editor.executeEdits("paste", [{ range, text, forceMoveMarkers: true }]);
  editor.pushUndoStop();

  const newPos = model.getPositionAt(
    model.getOffsetAt({
      lineNumber: range.startLineNumber,
      column: range.startColumn,
    }) + text.length,
  );
  editor.setPosition(newPos);
  editor.revealPosition(newPos);
}

/**
 * Delete the currently selected text (no clipboard interaction). Used
 * by the cut handler. No-op for empty selections or read-only editors.
 */
export function deleteSelectedText(
  editor: monaco.editor.IStandaloneCodeEditor,
  isReadOnly: () => boolean,
): void {
  if (isReadOnly()) {
    return;
  }
  const selection = editor.getSelection();
  if (!selection || selection.isEmpty()) {
    return;
  }
  editor.executeEdits("native-cut", [
    { range: selection, text: "", forceMoveMarkers: true },
  ]);
  editor.pushUndoStop();
}
