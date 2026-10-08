import { afterEach, describe, expect, it, vi } from "vitest";
import { DynamoDBDriver } from "../../src/extension/dbDrivers/dynamodb";
import { ElasticsearchDriver } from "../../src/extension/dbDrivers/elasticsearch";
import { MongoDBDriver } from "../../src/extension/dbDrivers/mongodb";
import {
  createTimeoutAwareDriver,
  DriverTimeoutError,
} from "../../src/extension/dbDrivers/timeout";
import type { IDBDriver } from "../../src/extension/dbDrivers/types";

const settings = () => ({
  connectionTimeoutSeconds: 15,
  dbOperationTimeoutSeconds: 1,
  connectionTimeoutMs: 15000,
  dbOperationTimeoutMs: 25,
});
const reply = {
  acknowledged: true,
  insertedId: "id",
  result: "created",
  _id: "id",
  Attributes: { id: { S: "id" } },
};

function createHarness(kind: "mongodb" | "elasticsearch" | "dynamodb") {
  const send = vi.fn(async (_input: unknown, _options?: unknown) => reply);
  const config = { id: kind, name: kind, type: kind, database: "test" };
  let driver: Pick<IDBDriver, "query">;
  let single: string;
  let batch: string;
  if (kind === "mongodb") {
    const mongo = new MongoDBDriver(config);
    Object.assign(mongo, {
      connected: true,
      client: { db: () => ({ collection: () => ({ insertOne: send }) }) },
    });
    driver = mongo;
    single = 'db.t.insertOne({ id: "retry" })';
    batch = 'db.t.insertOne({ id: "first" }); db.t.insertOne({ id: "second" })';
  } else if (kind === "elasticsearch") {
    const elastic = new ElasticsearchDriver(config);
    Object.assign(elastic, { connected: true, client: { index: send } });
    driver = elastic;
    single = 'PUT /t/_doc/retry\n{"v":1}';
    batch = 'PUT /t/_doc/first\n{"v":1}\nPUT /t/_doc/second\n{"v":2}';
  } else {
    const dynamo = new DynamoDBDriver(config);
    Object.assign(dynamo, { connected: true, client: { send } });
    driver = dynamo;
    single = '{"TableName":"t","Item":{"id":{"S":"retry"}}}';
    batch =
      '{"TableName":"t","Item":{"id":{"S":"first"}}}\n' +
      '{"TableName":"t","Item":{"id":{"S":"second"}}}';
  }
  return {
    driver,
    wrapped: createTimeoutAwareDriver(driver, settings),
    send,
    single,
    batch,
  };
}

afterEach(() => vi.useRealTimers());

describe.each([
  "mongodb",
  "elasticsearch",
  "dynamodb",
] as const)("%s query deadlines", (kind) => {
  it("stops a timed-out batch even if the SDK ignores abort, without cancelling a retry", async () => {
    vi.useFakeTimers();
    const { wrapped, send, single, batch } = createHarness(kind);
    let finish!: (value: typeof reply) => void;
    send.mockImplementationOnce(
      () =>
        new Promise((resolve) => {
          finish = resolve;
        }),
    );

    const pending = wrapped.query(batch);
    const rejection = expect(pending).rejects.toMatchObject({
      name: "DriverTimeoutError",
      operationName: "query",
      message: expect.stringContaining("verify the data before retrying"),
    });
    const firstOptions = send.mock.calls[0][1] as {
      signal?: AbortSignal;
      abortSignal?: AbortSignal;
    };
    const firstSignal = firstOptions.signal ?? firstOptions.abortSignal;
    expect(firstSignal?.aborted).toBe(false);

    await vi.advanceTimersByTimeAsync(25);
    await rejection;
    expect(firstSignal?.aborted).toBe(true);
    expect(firstSignal?.reason).toBeInstanceOf(DriverTimeoutError);

    // The old HTTP request is still pending. A retry owns a separate signal.
    await wrapped.query(single);
    const retryOptions = send.mock.calls[1][1] as typeof firstOptions;
    expect((retryOptions.signal ?? retryOptions.abortSignal)?.aborted).toBe(
      false,
    );
    expect(retryOptions.signal ?? retryOptions.abortSignal).not.toBe(
      firstSignal,
    );
    finish(reply);
    await vi.advanceTimersByTimeAsync(0);
    expect(send).toHaveBeenCalledTimes(2); // first old command + retry, no old second command
  });

  it("executes a successful batch with one shared deadline and a decreasing SDK budget", async () => {
    vi.useFakeTimers();
    const { wrapped, send, batch } = createHarness(kind);
    send.mockImplementationOnce(async () => {
      vi.setSystemTime(Date.now() + 10);
      return reply;
    });
    await wrapped.query(batch);
    expect(send).toHaveBeenCalledTimes(2);
    const first = send.mock.calls[0][1] as Record<string, unknown>;
    const second = send.mock.calls[1][1] as Record<string, unknown>;
    const signalKey = kind === "dynamodb" ? "abortSignal" : "signal";
    expect(second[signalKey]).toBe(first[signalKey]);
    expect((second[signalKey] as AbortSignal).aborted).toBe(false);
    if (kind !== "dynamodb") {
      const timeoutKey = kind === "mongodb" ? "timeoutMS" : "requestTimeout";
      expect(first[timeoutKey]).toBe(25);
      expect(second[timeoutKey]).toBe(15);
    }
  });

  it("aborts an in-flight SDK request without dispatching another batch command", async () => {
    vi.useFakeTimers();
    const { wrapped, send, batch } = createHarness(kind);
    const cancelled = vi.fn();
    send.mockImplementationOnce((_input, options) => {
      const context = options as {
        signal?: AbortSignal;
        abortSignal?: AbortSignal;
      };
      const signal = context.signal ?? context.abortSignal;
      if (!signal) throw new Error("SDK cancellation signal was not provided");
      return new Promise((_resolve, reject) => {
        signal.addEventListener(
          "abort",
          () => {
            cancelled();
            reject(signal.reason);
          },
          { once: true },
        );
      });
    });
    const rejection = expect(wrapped.query(batch)).rejects.toBeInstanceOf(
      DriverTimeoutError,
    );
    await vi.advanceTimersByTimeAsync(25);
    await rejection;
    expect(cancelled).toHaveBeenCalledOnce();
    expect(send).toHaveBeenCalledOnce();
  });

  it("does not dispatch a command with an expired caller deadline", async () => {
    const { wrapped, send, single } = createHarness(kind);
    await expect(
      wrapped.query(single, undefined, { deadline: Date.now() - 1 }),
    ).rejects.toThrow(/timed out/);
    expect(send).not.toHaveBeenCalled();
  });

  it("uses an earlier caller deadline for the in-flight cancellation timer", async () => {
    vi.useFakeTimers();
    const { wrapped, send, batch } = createHarness(kind);
    send.mockImplementationOnce(() => new Promise(() => undefined));
    const pending = wrapped.query(batch, undefined, {
      deadline: Date.now() + 10,
    });
    const rejection = expect(pending).rejects.toMatchObject({
      name: "DriverTimeoutError",
      timeoutMs: 10,
      message: expect.stringContaining("verify the data before retrying"),
    });
    const options = send.mock.calls[0][1] as {
      signal?: AbortSignal;
      abortSignal?: AbortSignal;
    };
    const signal = options.signal ?? options.abortSignal;
    await vi.advanceTimersByTimeAsync(9);
    expect(signal?.aborted).toBe(false);
    await vi.advanceTimersByTimeAsync(1);
    await rejection;
    expect(signal?.aborted).toBe(true);
    expect(signal?.reason).toMatchObject({ timeoutMs: 10 });
    expect(send).toHaveBeenCalledOnce();
  });

  it("does not report success when final result formatting crosses the deadline", async () => {
    vi.useFakeTimers();
    const { driver, wrapped, batch } = createHarness(kind);
    const mapping = driver as unknown as {
      mapRowToQueryRow(
        row: Record<string, unknown>,
        columns: string[],
      ): Record<string, unknown>;
    };
    const original = mapping.mapRowToQueryRow.bind(driver);
    let calls = 0;
    vi.spyOn(mapping, "mapRowToQueryRow").mockImplementation((row, columns) => {
      calls++;
      // Mongo/Elastic format two individual replies before assembling the
      // batch result. Dynamo formats its rows only after the final guard.
      if (calls === (kind === "dynamodb" ? 1 : 3)) {
        vi.setSystemTime(Date.now() + 30);
      }
      return original(row, columns);
    });
    await expect(wrapped.query(batch)).rejects.toMatchObject({
      name: "DriverTimeoutError",
      message: expect.stringContaining("verify the data before retrying"),
    });
  });

  it("normalizes an SDK rejection at the deadline to the mutation-unknown timeout", async () => {
    vi.useFakeTimers();
    const { wrapped, send, single } = createHarness(kind);
    send.mockImplementationOnce(async () => {
      vi.setSystemTime(Date.now() + 25);
      throw new Error("SDK request timeout");
    });
    await expect(wrapped.query(single)).rejects.toMatchObject({
      name: "DriverTimeoutError",
      message: expect.stringContaining("verify the data before retrying"),
    });
  });

  it("stops before the next command when the caller aborts", async () => {
    const { wrapped, send, batch } = createHarness(kind);
    const controller = new AbortController();
    const reason = new Error("caller cancelled");
    send.mockImplementationOnce(async () => {
      controller.abort(reason);
      return reply;
    });
    await expect(
      wrapped.query(batch, undefined, { signal: controller.signal }),
    ).rejects.toBe(reason);
    expect(send).toHaveBeenCalledOnce();
  });

  it("rejects a pre-aborted query before sending anything", async () => {
    const { wrapped, send, single } = createHarness(kind);
    const reason = new Error("already cancelled");
    await expect(
      wrapped.query(single, undefined, { signal: AbortSignal.abort(reason) }),
    ).rejects.toBe(reason);
    expect(send).not.toHaveBeenCalled();
  });

  it("checks the deadline even before the wrapper timer gets a turn", async () => {
    vi.useFakeTimers();
    const { wrapped, send, batch } = createHarness(kind);
    send.mockImplementationOnce(async () => {
      vi.setSystemTime(Date.now() + 30);
      return reply;
    });
    await expect(wrapped.query(batch)).rejects.toThrow(/timed out/);
    expect(send).toHaveBeenCalledOnce();
  });
});
