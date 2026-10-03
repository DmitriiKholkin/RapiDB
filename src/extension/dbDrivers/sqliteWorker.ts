import { SQLiteCoreDriver } from "./sqliteCore";
import {
  SQLITE_WORKER_METHODS,
  type SQLiteWorkerRequest,
  type SQLiteWorkerResponse,
} from "./sqliteWorkerProtocol";
import { TransactionVerificationError } from "./transactionVerification";

let driver: SQLiteCoreDriver | undefined;
// Defense in depth: even a caller bypassing the host queue cannot interleave
// transactions or async metadata methods on the same database handle.
let queue = Promise.resolve();
process.on("message", (request: SQLiteWorkerRequest) => {
  queue = queue
    .then(async () => {
      const response: SQLiteWorkerResponse = { id: request.id };
      try {
        if (Date.now() >= request.deadline)
          throw new Error("SQLite operation deadline exceeded");
        if (request.method === "connect") {
          if (!request.config || !request.runtimeTargets)
            throw new Error("Invalid SQLite worker startup");
          driver = new SQLiteCoreDriver(
            request.config,
            undefined,
            request.runtimeTargets,
          );
          await driver.connect();
        } else if (request.method === "disconnect") {
          await driver?.disconnect();
        } else {
          if (!driver || !SQLITE_WORKER_METHODS.includes(request.method))
            throw new Error("Invalid SQLite worker operation");
          if (request.method === "runTransaction") {
            request.args[1] = {
              signal: new AbortController().signal,
              deadline: request.deadline,
            };
          } else if (request.method === "query") {
            request.args[2] = {
              ...(request.args[2] as object),
              deadline: request.deadline,
            };
          }
          response.value = await Reflect.apply(
            driver[request.method],
            driver,
            request.args,
          );
        }
      } catch (error) {
        response.error = {
          message: error instanceof Error ? error.message : String(error),
          name: error instanceof Error ? error.name : "Error",
          ...(error instanceof TransactionVerificationError
            ? { verificationFailure: error.verificationFailure }
            : {}),
        };
      }
      if (process.connected) process.send?.(response);
    })
    .catch(() => process.exit(1));
});
// Release the idle DB handle after the host's IPC channel closes.
process.on("disconnect", () => process.exit(0));
