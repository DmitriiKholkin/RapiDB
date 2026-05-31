import * as vscode from "vscode";

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

type ConnectionLifecycleManager = {
  onDidDisconnect(listener: (connectionId: string) => void): vscode.Disposable;
};

export function attachConnectionScopedPanelLifecycle(
  panel: vscode.WebviewPanel,
  connectionManager: ConnectionLifecycleManager,
  connectionId: string,
  onConnectionsConfigurationChange: () => void,
): void {
  const configSubscription = vscode.workspace.onDidChangeConfiguration(
    (event) => {
      if (event.affectsConfiguration("rapidb.connections")) {
        onConnectionsConfigurationChange();
      }
    },
  );

  const disconnectSubscription = connectionManager.onDidDisconnect((id) => {
    if (id === connectionId) {
      panel.dispose();
    }
  });

  attachPanelDisposables(panel, configSubscription, disconnectSubscription);
}
