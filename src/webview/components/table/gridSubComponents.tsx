/**
 * Shared sub-components for TableGrid and QueryResultsGrid.
 *
 * These small, presentational components were extracted from TableGrid.tsx
 * to reduce its size and improve reusability.
 */
import React from "react";

// ─── Constants ──────────────────────────────────────────────────────────────

export const RESIZE_HANDLE_WIDTH = 12;
export const RESIZE_HANDLE_OVERHANG = 6;

// ─── Helper Functions ───────────────────────────────────────────────────────

export function isCollapsedWidth(width: number): boolean {
  return width <= 1;
}

export function getDisplaySize(width: number): number {
  return isCollapsedWidth(width) ? 0 : width;
}

export function getBaseTableStyle(width: number): React.CSSProperties {
  return {
    width,
    borderCollapse: "collapse",
    tableLayout: "fixed",
    fontSize: 12,
    fontFamily: "var(--vscode-editor-font-family, monospace)",
  };
}

// ─── Components ─────────────────────────────────────────────────────────────

export function HeaderContent({
  isCollapsed,
  justifyContent,
  children,
}: {
  isCollapsed: boolean;
  justifyContent: "flex-start" | "center";
  children: React.ReactNode;
}) {
  return (
    <div
      style={{
        display: "flex",
        alignItems: "center",
        justifyContent,
        gap: 4,
        width: "100%",
        overflow: "hidden",
        opacity: isCollapsed ? 0 : 1,
        pointerEvents: isCollapsed ? "none" : "auto",
      }}
    >
      {children}
    </div>
  );
}

export function ColumnResizeHandle({
  ariaLabel,
  onMouseDown,
  onTouchStart,
  onClick,
  isResizing,
  tabIndex,
}: {
  ariaLabel: string;
  onMouseDown: React.MouseEventHandler<HTMLButtonElement>;
  onTouchStart?: React.TouchEventHandler<HTMLButtonElement>;
  onClick?: React.MouseEventHandler<HTMLButtonElement>;
  isResizing: boolean;
  tabIndex?: number;
}) {
  return (
    <button
      type="button"
      aria-label={ariaLabel}
      tabIndex={tabIndex}
      onMouseDown={onMouseDown}
      onTouchStart={onTouchStart}
      onClick={onClick}
      style={{
        position: "absolute",
        right: -RESIZE_HANDLE_OVERHANG,
        top: 0,
        height: "100%",
        width: RESIZE_HANDLE_WIDTH,
        padding: 0,
        border: "none",
        cursor: "col-resize",
        userSelect: "none",
        touchAction: "none",
        zIndex: 3,
        background: isResizing ? "var(--vscode-focusBorder)" : "transparent",
      }}
    />
  );
}

export function TopSpacerRow({
  height,
  colSpan,
}: {
  height: number;
  colSpan: number;
}) {
  if (height <= 0) {
    return null;
  }

  return (
    <tr>
      <td
        colSpan={colSpan}
        style={{
          height,
          padding: 0,
          border: "none",
        }}
      />
    </tr>
  );
}

export function BottomSpacerRow({
  virtualItems,
  totalVirtualHeight,
  colSpan,
}: {
  virtualItems: readonly { end: number }[];
  totalVirtualHeight: number;
  colSpan: number;
}) {
  if (virtualItems.length === 0) {
    return null;
  }

  const lastVirtualRow = virtualItems[virtualItems.length - 1];
  const remainingHeight = totalVirtualHeight - lastVirtualRow.end;
  if (remainingHeight <= 0) {
    return null;
  }

  return (
    <tr>
      <td
        colSpan={colSpan}
        style={{
          height: remainingHeight,
          padding: 0,
          border: "none",
        }}
      />
    </tr>
  );
}
