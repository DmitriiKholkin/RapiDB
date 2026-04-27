import type React from "react";

export type ButtonVariant =
  | "primary"
  | "ghost"
  | "danger"
  | "warning"
  | "secondary";

export type ButtonSize = "sm" | "md";

interface ButtonStyleOptions {
  disabled?: boolean;
  gap?: number;
  size?: ButtonSize;
}

function sizeTokens(size: ButtonSize): {
  height: number;
  paddingX: number;
  fontSize: number;
} {
  if (size === "md") {
    return {
      height: 28,
      paddingX: 14,
      fontSize: 13,
    };
  }

  return {
    height: 24,
    paddingX: 10,
    fontSize: 12,
  };
}

export function buildButtonStyle(
  variant: ButtonVariant = "ghost",
  options: ButtonStyleOptions = {},
): React.CSSProperties {
  const { disabled = false, gap = 0, size = "sm" } = options;
  const tokens = sizeTokens(size);

  return {
    height: tokens.height,
    padding: `0 ${tokens.paddingX}px`,
    fontSize: tokens.fontSize,
    borderRadius: 2,
    cursor: disabled ? "default" : "pointer",
    fontFamily: "inherit",
    opacity: disabled ? 0.45 : 1,
    whiteSpace: "nowrap",
    display: "inline-flex",
    alignItems: "center",
    justifyContent: "center",
    gap,
    ...(variant === "primary"
      ? {
          background: "var(--vscode-button-background)",
          color: "var(--vscode-button-foreground)",
          border: "none",
        }
      : variant === "secondary"
        ? {
            background: "var(--vscode-button-secondaryBackground, transparent)",
            color:
              "var(--vscode-button-secondaryForeground, var(--vscode-foreground))",
            border:
              "1px solid var(--vscode-button-border, var(--vscode-widget-border))",
          }
        : variant === "danger"
          ? {
              background:
                "var(--vscode-inputValidation-errorBackground, rgba(200,50,50,0.2))",
              color: "var(--vscode-errorForeground)",
              border:
                "1px solid var(--vscode-inputValidation-errorBorder, rgba(200,50,50,0.4))",
            }
          : variant === "warning"
            ? {
                background: "rgba(200,150,0,0.15)",
                color: "var(--vscode-editorWarning-foreground, #cca700)",
                border: "1px solid rgba(200,150,0,0.4)",
              }
            : {
                background: "transparent",
                color: "var(--vscode-foreground)",
                border:
                  "1px solid var(--vscode-button-border, var(--vscode-panel-border))",
              }),
  };
}
