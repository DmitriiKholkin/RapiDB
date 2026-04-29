import React from "react";
import { Icon } from "./Icon";

/**
 * Semi-transparent overlay displayed over the grid during pagination
 * and filter fetches. Uses `position: absolute` so it must be placed
 * inside a container with `position: relative`.
 */
export function GridLoadingOverlay(): React.ReactElement {
  return (
    <div
      role="status"
      aria-live="polite"
      aria-label="Loading data"
      style={{
        position: "absolute",
        inset: 0,
        zIndex: 10,
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        background:
          "color-mix(in srgb, var(--vscode-editor-background) 68%, transparent)",
        pointerEvents: "all",
        userSelect: "none",
        WebkitUserSelect: "none",
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
        Loading…
      </div>
    </div>
  );
}
