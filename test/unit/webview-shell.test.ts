import { beforeEach, describe, expect, it, vi } from "vitest";
import type * as vscode from "vscode";

const { randomUUID, joinPath } = vi.hoisted(() => ({
  randomUUID: vi.fn(() => "nonce-123"),
  joinPath: vi.fn((base: { path: string }, ...parts: string[]) => ({
    path: [base.path, ...parts].join("/"),
  })),
}));

vi.mock("crypto", () => ({
  randomUUID,
}));

vi.mock("vscode", () => ({
  Uri: {
    joinPath,
  },
}));

import {
  configureWebviewResources,
  createWebviewShell,
} from "../../src/extension/panels/webviewShell";

describe("webviewShell", () => {
  const context = {
    extensionUri: { path: "/extension" },
  } as unknown as vscode.ExtensionContext;

  let webview: {
    options?: vscode.WebviewOptions;
    cspSource: string;
    asWebviewUri: ReturnType<typeof vi.fn>;
  } & vscode.Webview;

  beforeEach(() => {
    joinPath.mockClear();
    randomUUID.mockClear();
    webview = {
      html: "",
      options: {
        enableScripts: false,
        localResourceRoots: [],
      },
      cspSource: "vscode-webview-resource:",
      asWebviewUri: vi.fn(
        (uri: { path: string }) =>
          ({
            path: `webview:${uri.path}`,
            toString(this: { path: string }) {
              return this.path;
            },
          }) as unknown as vscode.Uri,
      ),
      postMessage: vi.fn(async () => true),
      onDidReceiveMessage: vi.fn(),
    };
  });

  it("configures local resource roots including media when requested", () => {
    const extraRoot = { path: "/tmp/extra" } as unknown as vscode.Uri;

    configureWebviewResources({
      context,
      webview,
      includeMediaRoot: true,
      extraLocalResourceRoots: [extraRoot],
    });

    expect(webview.options).toEqual({
      enableScripts: true,
      localResourceRoots: [
        { path: "/extension/dist" },
        { path: "/extension/media" },
        extraRoot,
      ],
    });
  });

  it("escapes the title and injects the initial state into the shell html", () => {
    const html = createWebviewShell({
      context,
      webview,
      title: 'Admin <Panel> & "Test"',
      initialState: {
        view: "query",
        connectionId: "conn-1",
        connectionType: "pg",
        initialSql: 'SELECT "</script><script>bad()</script>"',
      },
    });

    expect(html).toContain(
      "<title>Admin &lt;Panel&gt; &amp; &quot;Test&quot;</title>",
    );
    expect(html).toContain(
      'window.__RAPIDB_INITIAL_STATE__ = {"view":"query","connectionId":"conn-1","connectionType":"pg"',
    );
    expect(html).toContain(
      "\\u003c/script\\u003e\\u003cscript\\u003ebad()\\u003c/script\\u003e",
    );
    expect(html).not.toContain("</script><script>bad()");
    expect(html).toContain(
      "script-src 'nonce-nonce-123' vscode-webview-resource:",
    );
    expect(webview.asWebviewUri).toHaveBeenCalledTimes(2);
  });
});
