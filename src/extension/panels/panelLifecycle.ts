import * as vscode from "vscode";

function safelyDispose(action: () => void): void {
  try {
    action();
  } catch {
    // Ignore dispose errors during panel shutdown and forced cleanup.
  }
}

export function attachPanelDisposables(
  panel: vscode.WebviewPanel,
  ...disposables: readonly vscode.Disposable[]
): void {
  panel.onDidDispose(() => {
    for (const disposable of disposables) {
      safelyDispose(() => disposable.dispose());
    }
  });
}

export function disposePanelInstances<T>(
  instances: Iterable<T>,
  dispose: (instance: T) => void,
): void {
  for (const instance of instances) {
    safelyDispose(() => dispose(instance));
  }
}

export function attachPanelMessageHandler(
  panel: vscode.WebviewPanel,
  handler: (message: unknown) => Promise<void>,
  onError: (error: unknown, message: unknown) => void,
): void {
  panel.webview.onDidReceiveMessage(async (message) => {
    try {
      await handler(message);
    } catch (error: unknown) {
      onError(error, message);
    }
  });
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
