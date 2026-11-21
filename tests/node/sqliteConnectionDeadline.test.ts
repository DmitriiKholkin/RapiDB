import { EventEmitter } from "node:events";
import { join } from "node:path";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { SQLiteWorkerClient } from "../../src/extension/dbDrivers/sqliteWorkerClient";
import type {
  SQLiteWorkerRequest,
  SQLiteWorkerResponse,
} from "../../src/extension/dbDrivers/sqliteWorkerProtocol";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";

const installer = vi.hoisted(() => ({
  ensureSQLiteRuntimeInstalled: vi.fn(),
}));
const fork = vi.hoisted(() => vi.fn());
vi.mock("node:child_process", async (importOriginal) => ({
  ...(await importOriginal<typeof import("node:child_process")>()),
  fork,
}));
vi.mock(
  "../../src/extension/utils/sqliteInstaller",
  async (importOriginal) => ({
    ...(await importOriginal<
      typeof import("../../src/extension/utils/sqliteInstaller")
    >()),
    ...installer,
  }),
);

class StubWorker extends EventEmitter {
  readonly pid = workers.length + 1;
  readonly exitCode = null;
  readonly signalCode = null;
  readonly requests: SQLiteWorkerRequest[] = [];
  killed = false;

  send(request: SQLiteWorkerRequest) {
    this.requests.push(request);
    if (request.method === "disconnect") {
      void Promise.resolve().then(() =>
        this.emit("message", { id: request.id }),
      );
    }
  }

  reply(error?: SQLiteWorkerResponse["error"]) {
    this.emit("message", { id: this.requests[0]?.id, error });
  }

  kill() {
    this.killed = true;
    this.emit("exit", null, "SIGKILL");
  }
}

const workers: StubWorker[] = [];
const clients: SQLiteWorkerClient[] = [];
const managed = join(__dirname, "managed-sqlite-stub");
const config = {
  id: "sqlite-deadline",
  name: "SQLite deadline test",
  type: "sqlite",
  filePath: ":memory:",
} satisfies ConnectionConfig;

beforeEach(() => {
  vi.clearAllMocks();
  vi.useFakeTimers({
    toFake: ["Date", "hrtime", "setTimeout", "clearTimeout"],
  });
  fork.mockImplementation(() => {
    const worker = new StubWorker();
    workers.push(worker);
    return worker;
  });
  installer.ensureSQLiteRuntimeInstalled.mockResolvedValue(managed);
});

afterEach(async () => {
  await Promise.all(clients.splice(0).map((client) => client.close()));
  workers.length = 0;
  vi.useRealTimers();
});

async function openClient(timeoutMs = 100) {
  const client = new SQLiteWorkerClient();
  clients.push(client);
  const index = workers.length;
  const opening = client.open(config, timeoutMs);
  // Tests sometimes advance the clock past rejection before inspecting it.
  void opening.catch(() => undefined);
  await vi.advanceTimersByTimeAsync(0);
  const worker = workers[index];
  expect(worker?.requests[0]?.method).toBe("connect");
  return { client, opening, worker };
}

it("fails a connect whose verified managed binary cannot be loaded, without a repair download or retry", async () => {
  const { client, opening, worker } = await openClient();
  expect(worker.requests[0].runtimeTargets?.[0]).toBe(managed);
  worker.reply({
    name: "SQLiteRuntimeLoadError",
    message: "failed managed native probe",
  });
  await expect(opening).rejects.toThrow("failed managed native probe");
  expect(client.connected).toBe(false);
  expect(installer.ensureSQLiteRuntimeInstalled).toHaveBeenCalledOnce();
  expect(fork).toHaveBeenCalledOnce();
  expect(vi.getTimerCount()).toBe(0);
});

it("times out a worker that does not finish connecting within the budget", async () => {
  const { client, opening, worker } = await openClient();
  await vi.advanceTimersByTimeAsync(99);
  expect(worker.killed).toBe(false);
  await vi.advanceTimersByTimeAsync(1);
  await expect(opening).rejects.toMatchObject({
    name: "DriverTimeoutError",
    timeoutKind: "connection",
    operationName: "connect",
    timeoutMs: 100,
  });
  expect(worker.killed).toBe(true);
  expect(client.connected).toBe(false);
  expect(vi.getTimerCount()).toBe(0);
});

it("rejects a late worker reply even if the timeout callback has not run yet", async () => {
  const { client, opening, worker } = await openClient();
  const late = BigInt(Math.ceil(worker.requests[0].deadline + 1)) * 1_000_000n;
  const monotonic = vi.spyOn(process.hrtime, "bigint").mockReturnValue(late);
  try {
    worker.reply();
    await expect(opening).rejects.toMatchObject({ name: "DriverTimeoutError" });
    expect(client.connected).toBe(false);
    expect(worker.killed).toBe(true);
    expect(vi.getTimerCount()).toBe(0);
  } finally {
    monotonic.mockRestore();
  }
});

it("keeps a no-timeout attempt unbounded", async () => {
  const { client, opening, worker } = await openClient(0);
  expect(worker.requests[0].deadline).toBe(Infinity);
  await vi.advanceTimersByTimeAsync(50_000);
  worker.reply();
  await opening;
  expect(client.connected).toBe(true);
  expect(vi.getTimerCount()).toBe(0);
});

it.each([
  "disconnect",
  "cancel",
] as const)("clears the connect timer on explicit %s", async (action) => {
  const { client, opening } = await openClient();
  const stop =
    action === "disconnect"
      ? client.close()
      : client.cancel({ reason: "manual", operationName: "connect" });
  vi.advanceTimersByTime(100);
  await stop;
  await expect(opening).rejects.toThrow(
    /connection attempt cancelled|cancel|closed|disconnect/i,
  );
  expect(client.connected).toBe(false);
  expect(fork).toHaveBeenCalledOnce();
  expect(vi.getTimerCount()).toBe(0);
});
