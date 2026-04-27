import type React from "react";

export type ControlSize = "sm" | "md";

function controlSizeTokens(size: ControlSize): {
  height: number;
  paddingX: number;
  fontSize: number;
} {
  if (size === "md") {
    return {
      height: 28,
      paddingX: 8,
      fontSize: 13,
    };
  }

  return {
    height: 24,
    paddingX: 6,
    fontSize: 12,
  };
}

export function buildTextInputStyle(
  size: ControlSize = "md",
  focused = false,
): React.CSSProperties {
  const tokens = controlSizeTokens(size);

  return {
    width: "100%",
    height: tokens.height,
    padding: `0 ${tokens.paddingX}px`,
    fontSize: tokens.fontSize,
    background: "var(--vscode-input-background)",
    color: "var(--vscode-input-foreground)",
    borderWidth: 1,
    borderStyle: "solid",
    borderColor: focused
      ? "var(--vscode-focusBorder)"
      : "var(--vscode-input-border, var(--vscode-widget-border))",
    borderRadius: 2,
    outline: "none",
    fontFamily: "inherit",
    boxSizing: "border-box",
  };
}

export function buildSelectControlStyle(
  size: ControlSize = "sm",
  focused = false,
): React.CSSProperties {
  return {
    ...buildTextInputStyle(size, focused),
    background:
      "var(--vscode-dropdown-background, var(--vscode-input-background))",
    color: "var(--vscode-dropdown-foreground, var(--vscode-foreground))",
    borderColor: focused
      ? "var(--vscode-focusBorder)"
      : "var(--vscode-dropdown-border, var(--vscode-input-border))",
    cursor: "pointer",
  };
}
