import type { WebviewMessageEnvelope } from "../../shared/webviewContracts";

const vscodeApi = (): Window["__vscode"] => window.__vscode;

function isMessageEnvelope(value: unknown): value is WebviewMessageEnvelope {
  if (value === null || typeof value !== "object") {
    return false;
  }

  return typeof (value as { type?: unknown }).type === "string";
}

export function postMessage<TType extends string, TPayload>(
  type: TType,
  payload?: TPayload,
): void {
  const message: WebviewMessageEnvelope<TType, TPayload> =
    payload === undefined ? { type } : { type, payload };
  vscodeApi()?.postMessage(message);
}

type MessageHandler<T> = (payload: T) => void;
const handlers = new Map<string, Array<MessageHandler<unknown>>>();

window.addEventListener("message", (event) => {
  if (!isMessageEnvelope(event.data)) {
    return;
  }

  const msg = event.data;
  const fns = handlers.get(msg.type) ?? [];
  fns.forEach((fn) => {
    fn(msg.payload);
  });
});

export function onMessage<T>(
  type: string,
  handler: MessageHandler<T>,
): () => void {
  const existing = handlers.get(type) ?? [];
  handlers.set(type, [...existing, handler as MessageHandler<unknown>]);
  return () => {
    const updated = (handlers.get(type) ?? []).filter((h) => h !== handler);
    if (updated.length === 0) {
      handlers.delete(type);
      return;
    }

    handlers.set(type, updated);
  };
}
