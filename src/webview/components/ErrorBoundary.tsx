import React from "react";
import { buildButtonStyle } from "../utils/buttonStyles";

interface Props {
  context?: string;
  hideOnError?: { label: string; message: string };
  children: React.ReactNode;
}

interface State {
  hasError: boolean;
  error: Error | null;
  retried: boolean;
  hidden: boolean;
}

export class ErrorBoundary extends React.Component<Props, State> {
  constructor(props: Props) {
    super(props);
    this.state = {
      hasError: false,
      error: null,
      retried: false,
      hidden: false,
    };
  }

  static getDerivedStateFromError(
    error: Error,
  ): Pick<State, "hasError" | "error"> {
    return { hasError: true, error };
  }

  componentDidCatch(error: Error, info: React.ErrorInfo): void {
    const ctx = this.props.context ?? "unknown";
    console.error(
      `[RapiDB] Unhandled render error in <${ctx}>:`,
      error,
      info.componentStack,
    );
  }

  private handleRetry = (): void => {
    this.setState({ hasError: false, error: null, retried: true });
  };

  private handleHide = (): void => {
    this.setState({ hidden: true, error: null });
  };

  render(): React.ReactNode {
    const { hasError, error, retried, hidden } = this.state;
    const { hideOnError } = this.props;
    if (hidden && hideOnError) {
      return (
        <div role="status" style={{ margin: 16, fontSize: 13 }}>
          {hideOnError.message}
        </div>
      );
    }
    if (!hasError) {
      return this.props.children;
    }

    const ctx = this.props.context ?? "View";
    const msg = error?.message ?? "Unknown error";

    return (
      <div
        style={{
          margin: 16,
          padding: "14px 16px",
          borderRadius: 4,
          background: "var(--vscode-inputValidation-errorBackground)",
          border: "1px solid var(--vscode-inputValidation-errorBorder)",
          color: "var(--vscode-errorForeground)",
          fontFamily: "var(--vscode-font-family, system-ui, sans-serif)",
          fontSize: 13,
          display: "flex",
          flexDirection: "column",
          gap: 10,
        }}
      >
        <strong role="alert" style={{ fontSize: 14 }}>
          ⚠ {ctx} crashed
        </strong>
        <pre
          style={{
            margin: 0,
            padding: "8px 10px",
            borderRadius: 3,
            fontSize: 12,
            background: "rgba(0,0,0,0.15)",
            overflowX: "auto",
            whiteSpace: "pre-wrap",
            wordBreak: "break-word",
          }}
        >
          {msg}
        </pre>
        <div style={{ display: "flex", flexWrap: "wrap", gap: 8 }}>
          <button
            type="button"
            onClick={this.handleRetry}
            style={buildButtonStyle(retried ? "ghost" : "primary", {
              size: "sm",
            })}
          >
            Retry
          </button>
          {hideOnError && (
            <button
              type="button"
              onClick={this.handleHide}
              style={buildButtonStyle(retried ? "primary" : "ghost", {
                size: "sm",
              })}
            >
              {hideOnError.label}
            </button>
          )}
        </div>
        {retried && !hideOnError && (
          <div>
            Use <strong>Developer: Reload Webviews</strong> from the VS Code
            Command Palette. This reloads all webviews, so save work in other
            panels and let active operations finish first. Verify any uncertain
            write before repeating it. If reloading does not help, report the
            error and stack trace.
          </div>
        )}
        <div style={{ fontSize: 11, opacity: 0.6 }}>
          Check the Developer Console (Help → Toggle Developer Tools) for the
          full stack trace.
        </div>
      </div>
    );
  }
}
