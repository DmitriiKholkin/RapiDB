import React, { useEffect, useRef } from "react";
import { Icon } from "./Icon";

interface GridLoadingOverlayProps {
  mode?: "overlay" | "fullscreen";
  message?: string;
  trapFocus?: boolean;
}

export function GridLoadingOverlay({
  mode = "overlay",
  message = "Loading data...",
  trapFocus = false,
}: GridLoadingOverlayProps): React.ReactElement {
  const isFullscreen = mode === "fullscreen";
  const overlayRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!trapFocus) {
      return;
    }

    const previousActiveElement =
      document.activeElement instanceof HTMLElement
        ? document.activeElement
        : null;

    overlayRef.current?.focus();

    return () => {
      previousActiveElement?.focus();
    };
  }, [trapFocus]);

  return (
    <div
      ref={overlayRef}
      role="status"
      aria-live="polite"
      aria-atomic="true"
      aria-label={message}
      tabIndex={trapFocus ? 0 : undefined}
      onKeyDown={(event) => {
        if (trapFocus && event.key === "Tab") {
          event.preventDefault();
        }
      }}
      style={{
        position: "absolute",
        inset: 0,
        zIndex: isFullscreen ? 30 : 20,
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        background: isFullscreen
          ? "var(--vscode-editor-background)"
          : "color-mix(in srgb, var(--vscode-editor-background) 68%, transparent)",
        pointerEvents: "all",
        userSelect: "none",
        WebkitUserSelect: "none",
        outline: "none",
      }}
    >
      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: 8,
          padding: "7px 14px",
          borderRadius: 6,
          background:
            "var(--vscode-editorWidget-background, var(--vscode-editor-background))",
          border: "1px solid var(--vscode-panel-border)",
          fontSize: 12,
          color: "var(--vscode-foreground)",
          boxShadow: "0 2px 8px rgba(0,0,0,0.18)",
        }}
      >
        <Icon name="sync" size={13} spin />
        {message}
      </div>
    </div>
  );
}
