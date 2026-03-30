const vscodeApi = (): { postMessage: (msg: unknown) => void } => (window as any).__vscode;

export function postMessage(type: string, payload?: unknown): void {
  vscodeApi()?.postMessage({ type, payload });
}

type MessageHandler<T> = (payload: T) => void;
const handlers = new Map<string, MessageHandler<any>[]>();

window.addEventListener("message", (event) => {
  const msg = event.data as { type: string; payload?: unknown };
  const fns = handlers.get(msg.type) ?? [];
  fns.forEach((fn) => fn(msg.payload));
});

export function onMessage<T>(type: string, handler: MessageHandler<T>): () => void {
  const existing = handlers.get(type) ?? [];
  handlers.set(type, [...existing, handler]);
  return () => {
    const updated = (handlers.get(type) ?? []).filter((h) => h !== handler);
    handlers.set(type, updated);
  };
}
