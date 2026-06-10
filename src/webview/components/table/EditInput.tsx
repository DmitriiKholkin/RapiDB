import React, {
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
import { placeholderForCategory } from "../../types";
import { buildSmallGhostButtonStyle } from "../../utils/buttonStyles";
import { buildTextInputStyle } from "../../utils/controlStyles";
import { cssVar } from "../../utils/cssVar";
import { onMessage, postMessage } from "../../utils/messaging";
import { formatScalarValueForDisplay } from "../../utils/valueFormatting";

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
  nullable,
  category,
  suppressPlaceholder = false,
  showDefaultButton = false,
  onSetDefault,
  readOnly = false,
  onCommit,
  onCancel,
}: {
  initial: string;
  nullable: boolean;
  category?: TypeCategory;
  suppressPlaceholder?: boolean;
  showDefaultButton?: boolean;
  onSetDefault?: () => void;
  readOnly?: boolean;
  onCommit: (v: string) => void;
  onCancel: () => void;
}) {
  const isInitiallyNull = initial === NULL_SENTINEL;
  const [isNull, setIsNull] = useState(false);
  const [val, setVal] = useState(
    isInitiallyNull ? "" : normalizeEditInitialValue(initial, category),
  );
  const [contextMenu, setContextMenu] = useState<{
    x: number;
    y: number;
    hasSelection: boolean;
  } | null>(null);
  const ref = useRef<HTMLInputElement>(null);
  const menuRef = useRef<HTMLDivElement>(null);
  useLayoutEffect(() => {
    if (ref.current instanceof HTMLInputElement) {
      ref.current.focus({ preventScroll: true });
      ref.current.setSelectionRange(0, ref.current.value.length);
      ref.current.scrollLeft = 0;
    }
    return () => {
      pasteUnsubscribeRef.current?.();
    };
  }, []);

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
    const input = ref.current;
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

  const pasteUnsubscribeRef = useRef<(() => void) | null>(null);

  const handlePaste = useCallback(() => {
    const input = ref.current;
    if (!input) return;
    closeContextMenu();

    pasteUnsubscribeRef.current?.();

    pasteUnsubscribeRef.current = onMessage<string>("clipboardText", (text) => {
      pasteUnsubscribeRef.current?.();
      pasteUnsubscribeRef.current = null;

      if (!text || !ref.current) return;
      const start = ref.current.selectionStart ?? 0;
      const end = ref.current.selectionEnd ?? 0;
      const currentVal = val;
      const newVal = currentVal.slice(0, start) + text + currentVal.slice(end);
      setVal(newVal);

      const cursorPos = start + text.length;
      requestAnimationFrame(() => {
        if (ref.current instanceof HTMLInputElement) {
          ref.current.setSelectionRange(cursorPos, cursorPos);
        }
      });
    });

    postMessage("readClipboard");
  }, [closeContextMenu, val]);

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
  const commit = () =>
    onCommit(isNull ? NULL_SENTINEL : normalizeEditInitialValue(val, category));
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
      <input
        ref={ref}
        aria-label="Cell value"
        value={normalizedValue}
        readOnly={readOnly}
        onChange={(e) => {
          if (isNull) {
            setIsNull(false);
          }
          setVal(e.target.value);
        }}
        onKeyDown={(e) => {
          if (e.key === "Enter") {
            e.preventDefault();
            commit();
          }
          if (e.key === "Escape") {
            e.preventDefault();
            onCancel();
          }
        }}
        onBlur={commit}
        onClick={(e) => e.stopPropagation()}
        onDoubleClick={(e) => e.stopPropagation()}
        placeholder={
          suppressPlaceholder
            ? undefined
            : isNull
              ? "NULL"
              : category
                ? placeholderForCategory(category)
                : undefined
        }
        style={inputStyle}
      />
      {showDefaultButton && !readOnly && onSetDefault && (
        <button
          type="button"
          onMouseDown={(e) => e.preventDefault()}
          onClick={onSetDefault}
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
          onClick={() => onCommit(NULL_SENTINEL)}
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
            onClick={handlePaste}
            style={{
              appearance: "none",
              border: "none",
              background: "transparent",
              color:
                cssVar("--vscode-menu-foreground") ||
                cssVar("--vscode-foreground") ||
                "#cccccc",
              padding: "4px 10px",
              fontSize: 12,
              textAlign: "left",
              cursor: "pointer",
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
