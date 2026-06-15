/**
 * Clipboard bridge for the Monaco editor.
 *
 * Monaco runs inside the webview but the OS clipboard is owned by the
 * extension host. We therefore go through a message-bus roundtrip for
 * every copy/cut/paste:
 *
 *   - Copy/Cut: read the selection in the webview, post a
 *     `writeClipboard` request to the host, which writes to the real
 *     clipboard and replies via the host.
 *   - Paste: post a `readClipboard` request; the host replies via a
 *     `clipboardText` message, which the editor then inserts.
 *
 * This module centralises both the native DOM listener wiring and the
 * `clipboardText` subscription so the `MonacoEditor` component itself
 * stays focused on its lifecycle.
 */
import * as monaco from "monaco-editor";
import { onMessage, postMessage } from "../utils/messaging";

/** Refs/closures the component needs in order to re-evaluate the
 *  "has selection" predicate for the context menu. */
export interface ClipboardHost {
  getSelectedText(): string;
  isReadOnly(): boolean;
  insertText(text: string): void;
  focus(): void;
}

export interface ClipboardHandles {
  /** Wires native copy/cut/paste listeners and the clipboardText
   *  subscription. Returns a teardown that undoes all of them. */
  attach(): () => void;
}

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

/** Build the bridge for a single editor instance. */
export function createClipboardBridge(
  editor: monaco.editor.IStandaloneCodeEditor,
  host: ClipboardHost,
): ClipboardHandles {
  return {
    attach(): () => void {
      // 1. Listen for the host's reply when paste is requested.
      const unsubClipboard = onMessage<string>("clipboardText", (text) => {
        host.insertText(text);
        host.focus();
      });

      // 2. Native DOM event handlers — used when Monaco does not
      //    intercept the keystroke (e.g. focus moved out of editor).
      const domNode = editor.getDomNode();

      const nativeCopy = (e: ClipboardEvent) => {
        const text = host.getSelectedText();
        if (!text) {
          return;
        }
        e.preventDefault();
        e.stopImmediatePropagation();
        postMessage("writeClipboard", { text });
      };

      const nativeCut = (e: ClipboardEvent) => {
        const text = host.getSelectedText();
        if (!text || host.isReadOnly()) {
          return;
        }
        e.preventDefault();
        e.stopImmediatePropagation();
        postMessage("writeClipboard", { text });
        deleteSelectedText(editor, host.isReadOnly);
      };

      const nativePaste = (e: ClipboardEvent) => {
        if (host.isReadOnly()) {
          return;
        }
        e.preventDefault();
        e.stopImmediatePropagation();
        postMessage("readClipboard");
      };

      domNode?.addEventListener("copy", nativeCopy, true);
      domNode?.addEventListener("cut", nativeCut, true);
      domNode?.addEventListener("paste", nativePaste, true);

      // 3. Monaco command bindings — fire when the editor has focus.
      const copyCmd = editor.addCommand(
        monaco.KeyMod.CtrlCmd | monaco.KeyCode.KeyC,
        () => {
          const text = host.getSelectedText();
          if (text) {
            postMessage("writeClipboard", { text });
          }
        },
      );
      const cutCmd = editor.addCommand(
        monaco.KeyMod.CtrlCmd | monaco.KeyCode.KeyX,
        () => {
          const text = host.getSelectedText();
          if (text && !host.isReadOnly()) {
            postMessage("writeClipboard", { text });
            deleteSelectedText(editor, host.isReadOnly);
          }
        },
      );
      const pasteCmd = editor.addCommand(
        monaco.KeyMod.CtrlCmd | monaco.KeyCode.KeyV,
        () => {
          if (!host.isReadOnly()) {
            postMessage("readClipboard");
          }
        },
      );
      const pasteShiftCmd = editor.addCommand(
        monaco.KeyMod.CtrlCmd | monaco.KeyMod.Shift | monaco.KeyCode.KeyV,
        () => {
          if (!host.isReadOnly()) {
            postMessage("readClipboard");
          }
        },
      );

      return () => {
        unsubClipboard();
        domNode?.removeEventListener("copy", nativeCopy, true);
        domNode?.removeEventListener("cut", nativeCut, true);
        domNode?.removeEventListener("paste", nativePaste, true);
        // The command handles returned by Monaco are not disposable in
        // all versions, but `editor` itself disposes them on dispose.
        void copyCmd;
        void cutCmd;
        void pasteCmd;
        void pasteShiftCmd;
      };
    },
  };
}
