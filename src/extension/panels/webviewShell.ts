import { randomUUID } from "crypto";
import * as vscode from "vscode";

interface WebviewShellOptions {
  context: vscode.ExtensionContext;
  webview: vscode.Webview;
  title: string;
  initialState: unknown;
  includeMediaRoot?: boolean;
  extraLocalResourceRoots?: vscode.Uri[];
  extraCspDirectives?: string[];
  htmlStyles?: string;
  bodyStyles?: string;
  rootStyles?: string;
  extraStyles?: string;
}

interface WebviewResourceOptions {
  context: vscode.ExtensionContext;
  webview: vscode.Webview;
  includeMediaRoot?: boolean;
  extraLocalResourceRoots?: vscode.Uri[];
}

function escapeHtml(value: string): string {
  return value
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

export function configureWebviewResources({
  context,
  webview,
  includeMediaRoot = false,
  extraLocalResourceRoots = [],
}: WebviewResourceOptions): void {
  const localResourceRoots = [
    vscode.Uri.joinPath(context.extensionUri, "dist"),
    ...(includeMediaRoot
      ? [vscode.Uri.joinPath(context.extensionUri, "media")]
      : []),
    ...extraLocalResourceRoots,
  ];

  webview.options = {
    enableScripts: true,
    localResourceRoots,
  };
}

export function createWebviewShell({
  context,
  webview,
  title,
  initialState,
  includeMediaRoot = false,
  extraLocalResourceRoots = [],
  extraCspDirectives = [],
  htmlStyles = "",
  bodyStyles = "",
  rootStyles = "height: 100vh; overflow: auto;",
  extraStyles = "",
}: WebviewShellOptions): string {
  configureWebviewResources({
    context,
    webview,
    includeMediaRoot,
    extraLocalResourceRoots,
  });

  const webviewJs = webview.asWebviewUri(
    vscode.Uri.joinPath(context.extensionUri, "dist", "webview.js"),
  );
  const webviewCss = webview.asWebviewUri(
    vscode.Uri.joinPath(context.extensionUri, "dist", "webview.css"),
  );
  const nonce = randomUUID();
  const csp = [
    "default-src 'none'",
    ...extraCspDirectives,
    `script-src 'nonce-${nonce}' ${webview.cspSource}`,
    `style-src ${webview.cspSource} 'unsafe-inline'`,
    `font-src ${webview.cspSource} data:`,
    `img-src ${webview.cspSource} https: data:`,
  ].join("; ");

  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <meta http-equiv="Content-Security-Policy" content="${csp}" />
  <title>${escapeHtml(title)}</title>
  <link rel="stylesheet" href="${webviewCss}" />
  <style nonce="${nonce}">
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
    html { ${htmlStyles} }
    body {
      background: var(--vscode-editor-background);
      color: var(--vscode-foreground);
      font-family: var(--vscode-font-family, system-ui, sans-serif);
      font-size: var(--vscode-font-size, 13px);
      ${bodyStyles}
    }
    #root { ${rootStyles} }
    ${extraStyles}
  </style>
</head>
<body>
  <div id="root"></div>
  <script nonce="${nonce}">
    window.__RAPIDB_INITIAL_STATE__ = ${JSON.stringify(initialState)};
  </script>
  <script nonce="${nonce}" src="${webviewJs}"></script>
</body>
</html>`;
}
