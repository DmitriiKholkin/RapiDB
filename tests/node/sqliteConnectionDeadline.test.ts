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
  repairSQLiteManagedRuntime: vi.fn(),
  snapshotSQLiteManagedRuntime: vi.fn(),
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
  installer.snapshotSQLiteManagedRuntime.mockReturnValue({
    packagePath: managed,
    revision: "broken",
  });
  installer.ensureSQLiteRuntimeInstalled.mockResolvedValue(managed);
});

afterEach(async () => {
  await Promise.all(clients.splice(0).map((client) => client.close()));
  workers.length = 0;
  vi.useRealTimers();
});

function repairGate() {
  let finish!: (value: string | null) => void;
  const job = new Promise<string | null>((resolve) => {
    finish = resolve;
  });
  installer.repairSQLiteManagedRuntime.mockReturnValue(job);
  return () => finish(managed);
}

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

async function failNativeProbe(worker: StubWorker) {
  worker.reply({
    name: "SQLiteRuntimeLoadError",
    message: "failed managed native probe",
    managedNativeFailure: true,
  });
  await vi.advanceTimersByTimeAsync(0);
}

it("times out during repair without starting a retry or cancelling another waiter", async () => {
  const finishRepair = repairGate();
  const short = await openClient();
  const long = await openClient(200);
  await vi.advanceTimersByTimeAsync(20);
  await failNativeProbe(short.worker);
  await failNativeProbe(long.worker);
  expect(installer.repairSQLiteManagedRuntime).toHaveBeenCalledTimes(2);
  const [shortInterest, longInterest] =
    installer.repairSQLiteManagedRuntime.mock.calls.map(
      (args) => args[2] as AbortSignal,
    );

  await vi.advanceTimersByTimeAsync(80);
  await expect(short.opening).rejects.toMatchObject({
    name: "DriverTimeoutError",
    timeoutKind: "connection",
    operationName: "connect",
    timeoutMs: 100,
  });
  expect(shortInterest.aborted).toBe(true);
  expect(longInterest.aborted).toBe(false);
  expect(short.client.connected).toBe(false);
  expect(fork).toHaveBeenCalledTimes(2);

  finishRepair();
  await vi.advanceTimersByTimeAsync(0);
  expect(fork).toHaveBeenCalledTimes(3);
  workers[2].reply();
  await long.opening;
  expect(long.client.connected).toBe(true);
  expect(short.client.connected).toBe(false);
  expect(vi.getTimerCount()).toBe(0);
});

it.each([
  true,
  false,
])("uses the remaining budget (reply in time: %s)", async (replyInTime) => {
  const finishRepair = repairGate();
  const { client, opening, worker } = await openClient();
  await vi.advanceTimersByTimeAsync(20);
  await failNativeProbe(worker);
  await vi.advanceTimersByTimeAsync(60);
  // Wall-clock changes must not move the connection deadline.
  vi.setSystemTime(Date.now() + 50_000);
  finishRepair();
  await vi.advanceTimersByTimeAsync(0);
  const retry = workers[1];
  expect(retry.requests[0]).toMatchObject({
    method: "connect",
    deadline: worker.requests[0].deadline,
    runtimeTargets: [managed],
  });
  await vi.advanceTimersByTimeAsync(19);
  expect(retry.killed).toBe(false);
  if (replyInTime) {
    retry.reply();
    await opening;
    expect(client.connected).toBe(true);
  } else {
    await vi.advanceTimersByTimeAsync(1);
    await expect(opening).rejects.toMatchObject({
      name: "DriverTimeoutError",
      timeoutMs: 100,
    });
    expect(retry.killed).toBe(true);
    expect(client.connected).toBe(false);
  }
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

it("does not retry if repair finishes after the deadline before its timer fires", async () => {
  const finishRepair = repairGate();
  const { client, opening, worker } = await openClient();
  await failNativeProbe(worker);
  const late = BigInt(Math.ceil(worker.requests[0].deadline + 1)) * 1_000_000n;
  const monotonic = vi.spyOn(process.hrtime, "bigint").mockReturnValue(late);
  try {
    finishRepair();
    await expect(opening).rejects.toMatchObject({ name: "DriverTimeoutError" });
    expect(client.connected).toBe(false);
    expect(fork).toHaveBeenCalledOnce();
    expect(vi.getTimerCount()).toBe(0);
  } finally {
    monotonic.mockRestore();
  }
});

it("keeps a no-timeout attempt unbounded across repair", async () => {
  const finishRepair = repairGate();
  const { client, opening, worker } = await openClient(0);
  await failNativeProbe(worker);
  await vi.advanceTimersByTimeAsync(50_000);
  finishRepair();
  await vi.advanceTimersByTimeAsync(0);
  expect(workers[1].requests[0].deadline).toBe(Infinity);
  workers[1].reply();
  await opening;
  expect(client.connected).toBe(true);
  expect(vi.getTimerCount()).toBe(0);
});

it.each([
  "disconnect",
  "cancel",
] as const)("clears the repair timer on explicit %s", async (action) => {
  const finishRepair = repairGate();
  const { client, opening, worker } = await openClient();
  await failNativeProbe(worker);
  const signal = installer.repairSQLiteManagedRuntime.mock
    .calls[0][2] as AbortSignal;
  const stop =
    action === "disconnect"
      ? client.close()
      : client.cancel({ reason: "manual", operationName: "connect" });
  vi.advanceTimersByTime(100);
  await stop;
  await expect(opening).rejects.toThrow(/connection attempt cancelled/);
  expect(signal.aborted).toBe(true);
  finishRepair();
  await vi.advanceTimersByTimeAsync(0);
  expect(client.connected).toBe(false);
  expect(fork).toHaveBeenCalledOnce();
  expect(vi.getTimerCount()).toBe(0);
});
