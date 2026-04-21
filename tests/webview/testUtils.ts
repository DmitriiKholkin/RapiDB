import axeCore from "axe-core";
import { expect, vi } from "vitest";

type PostMessageMock = ReturnType<typeof vi.fn>;

type VsCodeApiMock = NonNullable<Window["__vscode"]> & {
  postMessage: PostMessageMock;
  getState: PostMessageMock;
  setState: PostMessageMock;
};

export interface PostedMessage {
  type: string;
  payload?: unknown;
}

function getVsCodeApiMock(): VsCodeApiMock {
  const api = window.__vscode as VsCodeApiMock | undefined;
  if (!api) {
    throw new Error("VS Code API stub is not available");
  }

  return api;
}

export function getPostedMessages(): PostedMessage[] {
  return getVsCodeApiMock().postMessage.mock.calls.map(([message]) => {
    return message as PostedMessage;
  });
}

export function getLastPostedMessage(): PostedMessage | undefined {
  return getPostedMessages().at(-1);
}

export function clearPostedMessages(): void {
  getVsCodeApiMock().postMessage.mockClear();
}

export function dispatchIncomingMessage<TPayload>(
  type: string,
  payload?: TPayload,
): void {
  dispatchWindowMessage(payload === undefined ? { type } : { type, payload });
}

export function dispatchWindowMessage(data: unknown): void {
  window.dispatchEvent(new MessageEvent("message", { data }));
}

export async function expectNoAxeViolations(
  container: HTMLElement,
): Promise<void> {
  const results = await axeCore.run(container, {
    rules: {
      "color-contrast": { enabled: false },
    },
  });

  const violationSummary = results.violations
    .map((violation) => {
      const nodes = violation.nodes.map((node) => node.html).join("\n");
      return `${violation.id}: ${violation.help}\n${nodes}`;
    })
    .join("\n");

  expect(results.violations, violationSummary).toHaveLength(0);
}
