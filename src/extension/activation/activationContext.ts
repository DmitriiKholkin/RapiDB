/**
 * Activation lifecycle state for the RapiDB extension.
 *
 * VSCode invokes `activate` exactly once per extension host, but the
 * Extension Test Runner re-imports the entry module between test cases.
 * We keep activation state in a closure-scoped variable inside this
 * module (instead of a module-level singleton in `extension.ts`) so a
 * stale reference cannot leak across hot-reloads or test isolation
 * boundaries.
 */

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

/** True if `activate()` has already wired up the extension. */
export function hasActiveState(): boolean {
  return state !== null;
}

/** Store the activation state for later disposal. */
export function setActiveState(next: ActivationState): void {
  state = next;
}

/** Take the activation state so `deactivate()` can dispose it. */
export function takeActiveState(): ActivationState | null {
  const current = state;
  state = null;
  return current;
}
