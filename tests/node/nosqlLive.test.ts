import { describe, it, vi } from "vitest";
import { runNoSqlLiveCheck } from "../scripts/nosql-live-check";

class MockEventEmitter<T> {
  private listeners: Array<(value: T) => unknown> = [];

  readonly event = (listener: (value: T) => unknown) => {
    this.listeners.push(listener);
    return {
      dispose: () => {
        this.listeners = this.listeners.filter(
          (candidate) => candidate !== listener,
        );
      },
    };
  };

  fire(value: T): void {
    for (const listener of this.listeners) {
      listener(value);
    }
  }

  dispose(): void {
    this.listeners = [];
  }
}

vi.mock("vscode", () => ({
  EventEmitter: MockEventEmitter,
}));

const liveIt = process.env.RAPIDB_LIVE_NOSQL === "1" ? it : it.skip;

describe("NoSQL live driver verification", () => {
  liveIt(
    "verifies Redis, MongoDB, Elasticsearch, and DynamoDB against live services",
    async () => {
      await runNoSqlLiveCheck();
    },
    600_000,
  );
});
