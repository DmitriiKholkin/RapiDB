import type React from "react";
import {
  useCallback,
  useEffect,
  useLayoutEffect,
  useRef,
  useState,
} from "react";
import {
  NULL_SENTINEL,
  normalizeBinaryHexDisplayPrefix,
  type TypeCategory,
} from "../../../shared/tableTypes";
import type { ClipboardTextPayload } from "../../../shared/webviewContracts";
import { placeholderForCategory } from "../../types";
import { buildSmallGhostButtonStyle } from "../../utils/buttonStyles";
import { buildTextInputStyle } from "../../utils/controlStyles";
import { cssVar } from "../../utils/cssVar";
import { isImeComposing } from "../../utils/ime";
import { onMessage, postMessage } from "../../utils/messaging";
import { formatScalarValueForDisplay } from "../../utils/valueFormatting";
import { ROW_H } from "./tableViewHelpers";

function normalizeEditInitialValue(
  value: string,
  category?: TypeCategory,
): string {
  if (category === "binary") {
    return normalizeBinaryHexDisplayPrefix(value);
  }
  return value;
}

export function EditInput({
  initial,
  initialMissing = false,
  nullable,
  category,
  suppressPlaceholder = false,
  showDefaultButton = false,
  onSetDefault,
  readOnly = false,
  onCommit,
  onCancel,
  onDirtyChange,
}: {
  initial: string;
  initialMissing?: boolean;
  nullable: boolean;
  category?: TypeCategory;
  suppressPlaceholder?: boolean;
  showDefaultButton?: boolean;
  onSetDefault?: () => void;
  readOnly?: boolean;
  onCommit: (v: string) => void;
  onCancel: () => void;
  onDirtyChange?: (dirty: boolean) => void;
}) {
  const isInitiallyNull = initial === NULL_SENTINEL;
  const [isNull, setIsNull] = useState(isInitiallyNull);
  const [val, setVal] = useState(
    isInitiallyNull ? "" : normalizeEditInitialValue(initial, category),
  );
  const [contextMenu, setContextMenu] = useState<{
    x: number;
    y: number;
    hasSelection: boolean;
  } | null>(null);
  const multiline =
    category === "text" || category === "lob" || category === "other";
  const ref = useRef<HTMLInputElement | HTMLTextAreaElement>(null);
  const editedRef = useRef(false);
  const menuRef = useRef<HTMLDivElement>(null);
  const editorClosedRef = useRef(false);
  const pasteUnsubscribeRef = useRef<(() => void) | null>(null);
  const pasteCursorRef = useRef<number | null>(null);
  const clipboardRecipientRef = useRef(`edit-input:${crypto.randomUUID()}`);

  const invalidatePaste = useCallback(() => {
    pasteUnsubscribeRef.current?.();
    pasteUnsubscribeRef.current = null;
    pasteCursorRef.current = null;
  }, []);

  const finishEditing = () => {
    editorClosedRef.current = true;
    invalidatePaste();
  };

  useLayoutEffect(() => {
    editorClosedRef.current = false;
    const input = ref.current;
    if (input) {
      input.focus({ preventScroll: true });
      input.setSelectionRange(0, input.value.length);
      input.scrollLeft = 0;
    }
    return () => {
      editorClosedRef.current = true;
      invalidatePaste();
    };
  }, [invalidatePaste]);

  useLayoutEffect(() => {
    if (readOnly) {
      invalidatePaste();
      return;
    }
    const cursor = pasteCursorRef.current;
    pasteCursorRef.current = null;
    const input = ref.current;
    if (
      cursor !== null &&
      input &&
      !editorClosedRef.current &&
      document.activeElement === input
    ) {
      input.setSelectionRange(cursor, cursor);
    }
  });

  const handleContextMenu = useCallback((event: MouseEvent) => {
    event.stopImmediatePropagation();
    event.preventDefault();
    const input = ref.current;
    const hasSelection = input
      ? input.selectionStart !== input.selectionEnd
      : false;
    setContextMenu({
      x: event.clientX,
      y: event.clientY,
      hasSelection,
    });
  }, []);

  useEffect(() => {
    const input: HTMLElement | null = ref.current;
    if (!input) return;
    input.addEventListener("contextmenu", handleContextMenu);
    return () => {
      input.removeEventListener("contextmenu", handleContextMenu);
    };
  }, [handleContextMenu]);

  const closeContextMenu = useCallback(() => setContextMenu(null), []);

  const handleCopy = useCallback(() => {
    const input = ref.current;
    if (!input) return;
    const start = input.selectionStart ?? 0;
    const end = input.selectionEnd ?? 0;
    const selectedText = input.value.substring(start, end);
    if (selectedText) {
      postMessage("writeClipboard", { text: selectedText });
    }
    closeContextMenu();
  }, [closeContextMenu]);

  const handlePaste = useCallback(() => {
    const input = ref.current;
    closeContextMenu();
    if (!input || input.readOnly || editorClosedRef.current) return;

    invalidatePaste();

    const requestId = crypto.randomUUID();
    pasteUnsubscribeRef.current = onMessage<ClipboardTextPayload>(
      "clipboardText",
      (payload) => {
        if (
          payload.requestId !== requestId ||
          payload.recipient !== clipboardRecipientRef.current
        )
          return;
        invalidatePaste();

        const text = multiline
          ? payload.text.replace(/\r\n?/g, "\n")
          : payload.text;
        const input = ref.current;
        if (!text || !input || input.readOnly || editorClosedRef.current)
          return;
        const start = input.selectionStart ?? 0;
        const end = input.selectionEnd ?? start;
        const currentVal = input.value;
        const newVal = normalizeEditInitialValue(
          currentVal.slice(0, start) + text + currentVal.slice(end),
          category,
        );
        const cursor = start + text.length;
        if (newVal === currentVal) {
          if (document.activeElement === input) {
            input.setSelectionRange(cursor, cursor);
          }
        } else {
          pasteCursorRef.current = cursor;
        }
        editedRef.current = true;
        setIsNull(false);
        setVal(newVal);
      },
    );

    postMessage("readClipboard", {
      requestId,
      recipient: clipboardRecipientRef.current,
    });
  }, [category, closeContextMenu, invalidatePaste, multiline]);

  useEffect(() => {
    if (!contextMenu) return;

    const handlePointerDown = (event: PointerEvent) => {
      const menu = menuRef.current;
      if (menu?.contains(event.target as Node)) return;
      closeContextMenu();
    };

    const handleEscape = (event: KeyboardEvent) => {
      if (event.key === "Escape") closeContextMenu();
    };

    window.addEventListener("pointerdown", handlePointerDown);
    window.addEventListener("keydown", handleEscape);

    return () => {
      window.removeEventListener("pointerdown", handlePointerDown);
      window.removeEventListener("keydown", handleEscape);
    };
  }, [contextMenu, closeContextMenu]);
  const normalizedValue = normalizeEditInitialValue(val, category);
  const dirty =
    !readOnly &&
    ((initialMissing && editedRef.current) ||
      isNull !== isInitiallyNull ||
      (!isNull &&
        normalizedValue !== normalizeEditInitialValue(initial, category)));
  const onDirtyChangeRef = useRef(onDirtyChange);
  onDirtyChangeRef.current = onDirtyChange;
  useEffect(() => {
    onDirtyChangeRef.current?.(dirty);
  }, [dirty]);
  useEffect(() => () => onDirtyChangeRef.current?.(false), []);
  const commit = () => {
    finishEditing();
    if (initialMissing && !editedRef.current) {
      onCancel();
      return;
    }
    onCommit(isNull ? NULL_SENTINEL : normalizeEditInitialValue(val, category));
  };
  const inputStyle: React.CSSProperties = {
    ...buildTextInputStyle("sm"),
    flex: 1,
    minWidth: 0,
    height: "100%",
    fontSize: 12,
    color: isNull
      ? "var(--vscode-disabledForeground)"
      : "var(--vscode-input-foreground)",
    border: "0px",
    boxSizing: "border-box" as const,
    fontStyle: isNull ? "italic" : "normal",
  };
  const nullBtnStyle: React.CSSProperties = {
    ...buildSmallGhostButtonStyle(),
    flexShrink: 0,
    height: "100%",
    padding: "0 5px",
    fontSize: 9,
    fontStyle: "italic",
    background: "var(--vscode-input-foreground)",
    color: "var(--vscode-input-background)",
    border: "0px",
    letterSpacing: "0em",
  };
  const fieldProps = {
    "aria-label": "Cell value",
    value: normalizedValue,
    readOnly,
    onChange: (
      e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>,
    ) => {
      editedRef.current = true;
      if (isNull) {
        setIsNull(false);
      }
      setVal(e.target.value);
    },
    onKeyDown: (e: React.KeyboardEvent) => {
      if (isImeComposing(e)) return;
      if (e.key === "Enter") {
        if (multiline && e.shiftKey) return;
        e.preventDefault();
        commit();
      }
      if (e.key === "Escape") {
        e.preventDefault();
        finishEditing();
        onCancel();
      }
    },
    onBlur: commit,
    onClick: (e: React.MouseEvent) => e.stopPropagation(),
    onDoubleClick: (e: React.MouseEvent) => e.stopPropagation(),
    placeholder: suppressPlaceholder
      ? undefined
      : isNull
        ? "NULL"
        : category
          ? placeholderForCategory(category)
          : undefined,
  };
  return (
    <div
      style={{
        display: "flex",
        gap: 2,
        alignItems: "center",
        width: "100%",
        height: "100%",
      }}
    >
      {multiline ? (
        <textarea
          ref={ref as React.RefObject<HTMLTextAreaElement>}
          rows={1}
          {...fieldProps}
          style={{
            ...inputStyle,
            lineHeight: `${ROW_H - 2}px`,
            resize: "none",
            overflow: "hidden",
            whiteSpace: "pre",
          }}
        />
      ) : (
        <input
          ref={ref as React.RefObject<HTMLInputElement>}
          {...fieldProps}
          style={inputStyle}
        />
      )}
      {showDefaultButton && !readOnly && onSetDefault && (
        <button
          type="button"
          onMouseDown={(e) => e.preventDefault()}
          onClick={() => {
            finishEditing();
            onSetDefault();
          }}
          title="Set field to DEFAULT (omit from insert)"
          style={nullBtnStyle}
        >
          DEF
        </button>
      )}
      {nullable && !readOnly && (
        <button
          type="button"
          data-null-btn="1"
          onMouseDown={(e) => e.preventDefault()}
          onClick={() => {
            finishEditing();
            onCommit(NULL_SENTINEL);
          }}
          title="Set field to NULL"
          style={nullBtnStyle}
        >
          NULL
        </button>
      )}
      {contextMenu && (
        <div
          ref={menuRef}
          role="menu"
          aria-label="Cell context menu"
          style={{
            position: "fixed",
            top: contextMenu.y,
            left: contextMenu.x,
            minWidth: 80,
            padding: 3,
            display: "flex",
            flexDirection: "column",
            background:
              cssVar("--vscode-menu-background") ||
              cssVar("--vscode-editorWidget-background") ||
              "#252526",
            border: `1px solid ${
              cssVar("--vscode-menu-border") || "rgba(255, 255, 255, 0.12)"
            }`,
            borderRadius: 6,
            boxShadow:
              "0 10px 30px rgba(0, 0, 0, 0.24), 0 2px 8px rgba(0, 0, 0, 0.18)",
            zIndex: 100,
          }}
          onMouseDown={(e) => {
            e.preventDefault();
            e.stopPropagation();
          }}
        >
          <button
            type="button"
            role="menuitem"
            disabled={!contextMenu.hasSelection}
            onClick={handleCopy}
            style={{
              appearance: "none",
              border: "none",
              background: "transparent",
              color: contextMenu.hasSelection
                ? cssVar("--vscode-menu-foreground") ||
                  cssVar("--vscode-foreground") ||
                  "#cccccc"
                : cssVar("--vscode-disabledForeground") ||
                  "rgba(255, 255, 255, 0.4)",
              padding: "4px 10px",
              fontSize: 12,
              textAlign: "left",
              cursor: contextMenu.hasSelection ? "pointer" : "default",
              borderRadius: 4,
              width: "100%",
            }}
          >
            Copy
          </button>
          <button
            type="button"
            role="menuitem"
            disabled={readOnly}
            onClick={handlePaste}
            style={{
              appearance: "none",
              border: "none",
              background: "transparent",
              color: readOnly
                ? cssVar("--vscode-disabledForeground") ||
                  "rgba(255, 255, 255, 0.4)"
                : cssVar("--vscode-menu-foreground") ||
                  cssVar("--vscode-foreground") ||
                  "#cccccc",
              padding: "4px 10px",
              fontSize: 12,
              textAlign: "left",
              cursor: readOnly ? "default" : "pointer",
              borderRadius: 4,
              width: "100%",
            }}
          >
            Paste
          </button>
        </div>
      )}
    </div>
  );
}
export function valueToEditString(value: unknown): string {
  if (value == null) return NULL_SENTINEL;
  return formatScalarValueForDisplay(value);
}
