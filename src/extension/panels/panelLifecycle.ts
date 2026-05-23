import type * as vscode from "vscode";

export function attachPanelDisposables(
  panel: vscode.WebviewPanel,
  ...disposables: readonly vscode.Disposable[]
): void {
  panel.onDidDispose(() => {
    for (const disposable of disposables) {
      try {
        disposable.dispose();
      } catch {
        // Ignore dispose errors during panel shutdown.
      }
    }
  });
}

export function disposePanelInstances<T>(
  instances: Iterable<T>,
  dispose: (instance: T) => void,
): void {
  for (const instance of instances) {
    try {
      dispose(instance);
    } catch {
      // Ignore dispose errors when force-closing all panels.
    }
  }
}
