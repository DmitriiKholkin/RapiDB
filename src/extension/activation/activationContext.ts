import * as vscode from "vscode";
import type { ConnectionManager } from "../connectionManager";
import type { ConnectionProvider } from "../providers/connectionProvider";

export interface ActivationServices {
  context: vscode.ExtensionContext;
  connectionManager: ConnectionManager;
  connectionProvider: ConnectionProvider;
  refresh: () => void;
}

export interface ActivationState {
  services: ActivationServices;
  disposables: vscode.Disposable[];
  connectionManager: ConnectionManager;
}

let state: ActivationState | null = null;

export function hasActiveState(): boolean {
  return state !== null;
}

export function setActiveState(next: ActivationState): void {
  state = next;
}

export function takeActiveState(): ActivationState | null {
  const current = state;
  state = null;
  return current;
}
