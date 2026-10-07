import { describe, expect, it, vi } from "vitest";
import { createWebviewShell } from "../../src/extension/panels/webviewShell";

const vscodeMock = vi.hoisted(() => ({
  module: {
    Uri: {
      joinPath: vi.fn((_base: unknown, ...segments: string[]) => ({
        segments,
      })),
    },
  },
}));

vi.mock("vscode", () => vscodeMock.module);

function buildShell(extraCspDirectives: string[] = []): string {
  const webview = {
    cspSource: "vscode-webview-resource:",
    asWebviewUri: vi.fn(() => "vscode-webview-resource:/asset"),
  };

  return createWebviewShell({
    context: { extensionUri: {} } as never,
    webview: webview as never,
    title: "CSP test",
    initialState: { view: "table" } as never,
    extraCspDirectives,
  });
}

function readCsp(html: string): string {
  const match = html.match(
    /<meta http-equiv="Content-Security-Policy" content="([^"]+)" \/>/,
  );
  if (!match) {
    throw new Error("Webview shell did not include a CSP meta tag");
  }
  return match[1];
}

describe("webview CSP", () => {
  it("allows blob workers only when requested and keeps scripts nonce-only", () => {
    const defaultCsp = readCsp(buildShell());
    expect(defaultCsp).toContain("default-src 'none'");
    expect(defaultCsp).not.toContain("worker-src");

    const tableHtml = buildShell(["worker-src blob:"]);
    const tableCsp = readCsp(tableHtml);
    expect(tableCsp.split("; ")).toContain("worker-src blob:");
    const scriptPolicy = tableCsp
      .split("; ")
      .find((directive) => directive.startsWith("script-src "));
    expect(scriptPolicy).toMatch(
      /^script-src 'nonce-([^']+)' vscode-webview-resource:$/,
    );
    const nonce = scriptPolicy?.match(/^script-src 'nonce-([^']+)'/)?.[1];
    expect(tableCsp).not.toContain("'unsafe-eval'");
    expect(tableHtml).toContain(`<script nonce="${nonce}">`);
    expect(tableHtml).toContain(
      `<script nonce="${nonce}" src="vscode-webview-resource:/asset">`,
    );
  });
});
