import type * as vscode from "vscode";
import type { PanelRetentionMode } from "../../shared/webviewContracts";

export function shouldRetainContextWhenHidden(
  mode: PanelRetentionMode,
): boolean {
  return mode === "retain";
}

export function createPanelWebviewOptions(
  mode: PanelRetentionMode,
): vscode.WebviewOptions & vscode.WebviewPanelOptions {
  return {
    enableScripts: true,
    retainContextWhenHidden: shouldRetainContextWhenHidden(mode),
  };
}
