import { afterEach, describe, expect, it, vi } from "vitest";
import { DynamoDBDriver } from "../../src/extension/dbDrivers/dynamodb";
import {
  createDriverTimeoutSettingsSnapshot,
  createTimeoutAwareDriver,
} from "../../src/extension/dbDrivers/timeout";

const harness = vi.hoisted(() => ({
  clients: [] as Array<{
    send: ReturnType<typeof vi.fn>;
    destroy: ReturnType<typeof vi.fn>;
  }>,
}));

vi.mock("@aws-sdk/client-dynamodb", async (importOriginal) => {
  const actual =
    await importOriginal<typeof import("@aws-sdk/client-dynamodb")>();
  class MockClient {
    constructor() {
      const client = harness.clients.shift();
      if (!client) throw new Error("Unexpected DynamoDB client creation");
      // biome-ignore lint/correctness/noConstructorReturn: SDK client test double
      return client as never;
    }
  }
  return { ...actual, DynamoDBClient: MockClient };
});

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (error: Error) => void;
  const promise = new Promise<T>((yes, no) => {
    resolve = yes;
    reject = no;
  });
  return { promise, resolve, reject };
}

function createClient() {
  const client = {
    send: vi.fn(async (): Promise<object> => ({})),
    destroy: vi.fn(),
  };
  harness.clients.push(client);
  return client;
}

function createDriver() {
  return new DynamoDBDriver({ id: "dynamo", name: "Dynamo", type: "dynamodb" });
}

async function flushMicrotasks() {
  for (let i = 0; i < 30; i++) await Promise.resolve();
}

afterEach(() => {
  harness.clients.length = 0;
  vi.useRealTimers();
});

describe("DynamoDB pending connection lifecycle", () => {
  it("disconnect before client creation prevents a late connect", async () => {
    const driver = createDriver();
    const pending = driver.connect();
    const rejected = expect(pending).rejects.toThrow(/cancelled/i);
    await driver.disconnect();
    await rejected;
    expect(driver.isConnected()).toBe(false);
  });

  it("can cancel a published attempt once without cancelling its replacement", async () => {
    const old = createClient();
    const driver = createDriver();
    const attempt = driver.connect();
    await attempt;
    await driver.cancelConnectionAttempt(attempt);
    expect(driver.isConnected()).toBe(false);
    const current = createClient();
    await driver.connect();
    await driver.cancelConnectionAttempt(attempt);
    expect(driver.isConnected()).toBe(true);
    expect(old.destroy).toHaveBeenCalledOnce();
    expect(current.destroy).not.toHaveBeenCalled();
    await driver.disconnect();
  });

  it("deduplicates concurrent probes", async () => {
    const client = createClient();
    const probe = deferred<object>();
    client.send.mockReturnValue(probe.promise);
    const driver = createDriver();
    const first = driver.connect();
    const second = driver.connect();
    await flushMicrotasks();
    expect(client.send).toHaveBeenCalledOnce();
    expect(second).toBe(first);
    probe.resolve({});
    await first;
    await driver.disconnect();
    expect(client.destroy).toHaveBeenCalledOnce();
  });

  it.each([
    "success",
    "failure",
  ] as const)("disconnect closes a pending client and fences late %s after reconnect", async (settlement) => {
    const old = createClient();
    const probe = deferred<object>();
    old.send.mockReturnValue(probe.promise);
    const driver = createDriver();
    const pending = driver.connect();
    const rejected = expect(pending).rejects.toThrow();
    await flushMicrotasks();
    await driver.disconnect();
    expect(old.destroy).toHaveBeenCalledOnce();
    const current = createClient();
    await driver.connect();
    if (settlement === "success") probe.resolve({});
    else probe.reject(new Error("old probe failed"));
    await rejected;
    await driver.cancelConnectionAttempt(pending);
    expect(driver.isConnected()).toBe(true);
    expect(current.destroy).not.toHaveBeenCalled();
    await driver.disconnect();
    expect(old.destroy).toHaveBeenCalledOnce();
    expect(current.destroy).toHaveBeenCalledOnce();
  });

  it("timeout destroys a hung probe before settlement; late cleanup preserves a new connection", async () => {
    vi.useFakeTimers();
    const old = createClient();
    const probe = deferred<object>();
    old.send.mockReturnValue(probe.promise);
    const driver = createDriver();
    const wrapped = createTimeoutAwareDriver(driver, () =>
      createDriverTimeoutSettingsSnapshot({ connectionTimeoutSeconds: 1 }),
    );
    const rejected = expect(wrapped.connect()).rejects.toThrow("timed out");
    await vi.advanceTimersByTimeAsync(1000);
    await rejected;
    expect(old.destroy).toHaveBeenCalledOnce();
    const current = createClient();
    await wrapped.connect();
    probe.resolve({});
    await flushMicrotasks();
    expect(driver.isConnected()).toBe(true);
    expect(current.destroy).not.toHaveBeenCalled();
    expect(old.destroy).toHaveBeenCalledOnce();
    await driver.disconnect();
  });

  it("wrapper eventually cleans a successful late connect even without reconnect", async () => {
    vi.useFakeTimers();
    const client = createClient();
    const probe = deferred<object>();
    client.send.mockReturnValue(probe.promise);
    const driver = createDriver();
    const wrapped = createTimeoutAwareDriver(driver, () =>
      createDriverTimeoutSettingsSnapshot({ connectionTimeoutSeconds: 1 }),
    );
    const rejected = expect(wrapped.connect()).rejects.toThrow("timed out");
    await vi.advanceTimersByTimeAsync(1000);
    await rejected;
    probe.resolve({});
    await flushMicrotasks();
    expect(driver.isConnected()).toBe(false);
    expect(client.destroy).toHaveBeenCalledOnce();
  });
});
