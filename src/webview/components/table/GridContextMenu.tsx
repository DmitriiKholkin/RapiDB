import React, { useCallback, useEffect, useRef, useState } from "react";
import { cssVar } from "../../utils/cssVar";

interface GridContextMenuState {
  x: number;
  y: number;
}

interface GridContextMenuProps {
  containerRef: React.RefObject<HTMLDivElement | null>;
  onCopy: () => void;
  onPaste?: () => void;
  canPaste?: boolean;
}

const MENU_WIDTH = 80;
const MENU_HEIGHT = 28;

export function GridContextMenu({
  containerRef,
  onCopy,
  onPaste,
  canPaste = false,
}: GridContextMenuProps) {
  const [menuState, setMenuState] = useState<GridContextMenuState | null>(null);
  const [hoveredItem, setHoveredItem] = useState<string | null>(null);
  const menuRef = useRef<HTMLDivElement>(null);

  const closeMenu = useCallback(() => {
    setMenuState(null);
    setHoveredItem(null);
  }, []);

  useEffect(() => {
    const container = containerRef.current;
    if (!container) {
      return;
    }

    const handleContextMenu = (event: MouseEvent) => {
      event.preventDefault();

      const bounds = container.getBoundingClientRect();
      const x = Math.min(
        Math.max(event.clientX - bounds.left, 4),
        Math.max(bounds.width - MENU_WIDTH, 4),
      );
      const y = Math.min(
        Math.max(event.clientY - bounds.top, 4),
        Math.max(bounds.height - MENU_HEIGHT, 4),
      );

      setMenuState({ x, y });
    };

    container.addEventListener("contextmenu", handleContextMenu);

    return () => {
      container.removeEventListener("contextmenu", handleContextMenu);
    };
  }, [containerRef]);

  useEffect(() => {
    if (!menuState) {
      return;
    }

    const handlePointerDown = (event: PointerEvent) => {
      const menu = menuRef.current;
      if (menu?.contains(event.target as Node)) {
        return;
      }
      closeMenu();
    };

    const handleEscape = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        closeMenu();
      }
    };

    window.addEventListener("pointerdown", handlePointerDown);
    window.addEventListener("keydown", handleEscape);
    window.addEventListener("blur", closeMenu);

    return () => {
      window.removeEventListener("pointerdown", handlePointerDown);
      window.removeEventListener("keydown", handleEscape);
      window.removeEventListener("blur", closeMenu);
    };
  }, [menuState, closeMenu]);

  const runAction = useCallback(
    (action: () => void) => {
      action();
      closeMenu();
    },
    [closeMenu],
  );

  return (
    <>
      {menuState ? (
        <div
          ref={menuRef}
          role="menu"
          aria-label="Grid context menu"
          style={{
            position: "absolute",
            top: menuState.y,
            left: menuState.x,
            minWidth: MENU_WIDTH,
            padding: 3,
            display: "flex",
            flexDirection: "column",
            gap: 0,
            background:
              cssVar("--vscode-menu-background") ||
              cssVar("--vscode-editorWidget-background") ||
              cssVar("--vscode-editor-background") ||
              "#252526",
            border: `1px solid ${
              cssVar("--vscode-menu-border") ||
              cssVar("--vscode-contrastBorder") ||
              "rgba(255, 255, 255, 0.12)"
            }`,
            borderRadius: 6,
            boxShadow:
              "0 10px 30px rgba(0, 0, 0, 0.24), 0 2px 8px rgba(0, 0, 0, 0.18)",
            zIndex: 20,
          }}
        >
          <button
            type="button"
            role="menuitem"
            onClick={() => runAction(onCopy)}
            onMouseEnter={() => setHoveredItem("copy")}
            onMouseLeave={() => setHoveredItem(null)}
            style={menuButtonStyle(false, hoveredItem === "copy")}
          >
            Copy
          </button>
          {onPaste && (
            <button
              type="button"
              role="menuitem"
              disabled={!canPaste}
              onClick={() => runAction(onPaste)}
              onMouseEnter={() => setHoveredItem("paste")}
              onMouseLeave={() => setHoveredItem(null)}
              style={menuButtonStyle(!canPaste, hoveredItem === "paste")}
            >
              Paste
            </button>
          )}
        </div>
      ) : null}
    </>
  );
}

function menuButtonStyle(
  disabled: boolean,
  hovered = false,
): React.CSSProperties {
  const hoverBg =
    cssVar("--vscode-menu-selectionBackground") || "rgba(255, 255, 255, 0.10)";

  return {
    appearance: "none",
    border: "none",
    background: hovered && !disabled ? hoverBg : "transparent",
    color: disabled
      ? cssVar("--vscode-disabledForeground") || "rgba(255, 255, 255, 0.4)"
      : hovered
        ? cssVar("--vscode-menu-selectionForeground") ||
          cssVar("--vscode-menu-foreground") ||
          cssVar("--vscode-foreground") ||
          "#cccccc"
        : cssVar("--vscode-menu-foreground") ||
          cssVar("--vscode-foreground") ||
          "#cccccc",
    padding: "4px 10px",
    fontSize: 12,
    lineHeight: "18px",
    textAlign: "left",
    cursor: disabled ? "default" : "pointer",
    borderRadius: 4,
    width: "100%",
  };
}
