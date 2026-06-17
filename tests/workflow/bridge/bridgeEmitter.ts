export class BridgeEmitter<T> {
  private listeners = new Set<(value: T) => void>();
  readonly event = (listener: (value: T) => void): { dispose(): void } => {
    this.listeners.add(listener);
    return {
      dispose: () => {
        this.listeners.delete(listener);
      },
    };
  };
  fire(value: T): void {
    for (const listener of Array.from(this.listeners)) {
      listener(value);
    }
  }
  dispose(): void {
    this.listeners.clear();
  }
}
