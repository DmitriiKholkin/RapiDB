import { constants } from "node:fs";
import { open, stat } from "node:fs/promises";
import { basename } from "node:path";
import * as tls from "node:tls";
import {
  type ConnectionConfig,
  type ConnectionTlsConfig,
  normalizeConnectionTlsConfig,
} from "../../shared/connectionConfig";
import { getTlsServername } from "../driverRuntimeConfig";

export interface ResolvedConnectionTlsSettings {
  config: ConnectionTlsConfig;
  rejectUnauthorized: boolean;
  skipHostnameVerification: boolean;
  servername?: string;
  ca?: Buffer;
  cert?: Buffer;
  key?: Buffer;
  passphrase?: string;
  checkServerIdentity?: typeof tls.checkServerIdentity;
}

export const MAX_TLS_FILE_SIZE_BYTES = 1024 * 1024;

const TLS_FILE_READ_CHUNK_SIZE = 64 * 1024;

type TlsFileRole = "CA certificate" | "client certificate" | "client key";

class TlsFileReadError extends Error {}

function failTlsFileRead(
  role: TlsFileRole,
  filePath: string,
  reason: string,
): never {
  throw new TlsFileReadError(
    `[RapiDB] TLS ${role} file "${basename(filePath)}" ${reason}`,
  );
}

function getFilesystemErrorCode(error: unknown): string | undefined {
  if (typeof error !== "object" || error === null || !("code" in error)) {
    return undefined;
  }
  return typeof error.code === "string" ? error.code : undefined;
}

function createAbortError(): DOMException {
  return new DOMException("The operation was aborted", "AbortError");
}

function throwIfAborted(signal?: AbortSignal): void {
  if (signal?.aborted) {
    throw createAbortError();
  }
}

function awaitWithAbort<T>(
  operation: Promise<T>,
  signal: AbortSignal | undefined,
  onLateValue?: (value: T) => void | Promise<void>,
): Promise<T> {
  if (!signal) {
    return operation;
  }

  const discardLateValue = (value: T) => {
    if (onLateValue) {
      void Promise.resolve(onLateValue(value)).catch(() => undefined);
    }
  };
  if (signal.aborted) {
    void operation.then(discardLateValue, () => undefined);
    return Promise.reject(createAbortError());
  }

  return new Promise<T>((resolve, reject) => {
    let settled = false;
    const cleanup = () => signal.removeEventListener("abort", onAbort);
    const onAbort = () => {
      if (settled) return;
      settled = true;
      cleanup();
      reject(createAbortError());
    };

    signal.addEventListener("abort", onAbort, { once: true });
    void operation.then(
      (value) => {
        if (settled) {
          discardLateValue(value);
          return;
        }
        settled = true;
        cleanup();
        if (signal.aborted) {
          discardLateValue(value);
          reject(createAbortError());
          return;
        }
        resolve(value);
      },
      (error: unknown) => {
        if (settled) return;
        settled = true;
        cleanup();
        reject(signal.aborted ? createAbortError() : error);
      },
    );
  });
}

async function readOptionalTlsFile(
  filePath: string | undefined,
  role: TlsFileRole,
  signal?: AbortSignal,
): Promise<Buffer | undefined> {
  throwIfAborted(signal);
  if (typeof filePath !== "string") {
    return undefined;
  }

  const normalized = filePath.trim();
  if (!normalized) {
    return undefined;
  }

  let fileHandle: Awaited<ReturnType<typeof open>> | undefined;
  let closePromise: Promise<void> | undefined;
  const closeHandle = (
    handle: Awaited<ReturnType<typeof open>> | undefined = fileHandle,
  ): Promise<void> => {
    if (!handle) return Promise.resolve();
    if (handle === fileHandle && closePromise) return closePromise;
    const closing = handle.close().catch(() => undefined);
    if (handle === fileHandle) closePromise = closing;
    return closing;
  };
  const onAbort = () => {
    void closeHandle();
  };
  signal?.addEventListener("abort", onAbort, { once: true });
  try {
    // stat() follows regular symlinks, which are commonly used by certificate
    // managers. Reject known special files before opening, and use nonblocking
    // open flags so a path replaced with a FIFO cannot stall the extension host.
    const pathStats = await awaitWithAbort(stat(normalized), signal);
    if (!pathStats.isFile()) {
      failTlsFileRead(
        role,
        normalized,
        "must refer to a regular file (directories, devices, and FIFOs are not supported).",
      );
    }
    if (pathStats.size > MAX_TLS_FILE_SIZE_BYTES) {
      failTlsFileRead(
        role,
        normalized,
        `exceeds the ${MAX_TLS_FILE_SIZE_BYTES / 1024 / 1024} MiB size limit.`,
      );
    }

    fileHandle = await awaitWithAbort(
      open(normalized, constants.O_RDONLY | (constants.O_NONBLOCK ?? 0)),
      signal,
      (lateHandle) => closeHandle(lateHandle),
    );
    throwIfAborted(signal);
    const openedStats = await awaitWithAbort(fileHandle.stat(), signal);
    if (!openedStats.isFile()) {
      failTlsFileRead(
        role,
        normalized,
        "must refer to a regular file (directories, devices, and FIFOs are not supported).",
      );
    }
    if (
      openedStats.dev !== pathStats.dev ||
      openedStats.ino !== pathStats.ino
    ) {
      failTlsFileRead(
        role,
        normalized,
        "changed while being opened; select a stable file and retry.",
      );
    }
    if (openedStats.size > MAX_TLS_FILE_SIZE_BYTES) {
      failTlsFileRead(
        role,
        normalized,
        `exceeds the ${MAX_TLS_FILE_SIZE_BYTES / 1024 / 1024} MiB size limit.`,
      );
    }

    const chunks: Buffer[] = [];
    let totalBytes = 0;
    while (true) {
      // Read one byte past the cap to detect files that grew after stat().
      const chunk = Buffer.allocUnsafe(
        Math.min(
          TLS_FILE_READ_CHUNK_SIZE,
          MAX_TLS_FILE_SIZE_BYTES + 1 - totalBytes,
        ),
      );
      const { bytesRead } = await awaitWithAbort(
        fileHandle.read(chunk, 0, chunk.length, null),
        signal,
      );
      throwIfAborted(signal);
      if (bytesRead === 0) {
        break;
      }
      totalBytes += bytesRead;
      if (totalBytes > MAX_TLS_FILE_SIZE_BYTES) {
        failTlsFileRead(
          role,
          normalized,
          `exceeds the ${MAX_TLS_FILE_SIZE_BYTES / 1024 / 1024} MiB size limit.`,
        );
      }
      chunks.push(chunk.subarray(0, bytesRead));
    }
    throwIfAborted(signal);
    return Buffer.concat(chunks, totalBytes);
  } catch (error) {
    if (signal?.aborted) {
      throw createAbortError();
    }
    if (
      typeof error === "object" &&
      error !== null &&
      "name" in error &&
      error.name === "AbortError"
    ) {
      throw error;
    }
    if (error instanceof TlsFileReadError) {
      throw error;
    }
    const code = getFilesystemErrorCode(error);
    const reason =
      code === "ENOENT"
        ? "was not found"
        : code === "EACCES" || code === "EPERM"
          ? "is not readable (permission denied)"
          : code === "ELOOP"
            ? "could not be resolved"
            : "could not be read";
    failTlsFileRead(role, normalized, `${reason}${code ? ` (${code})` : ""}.`);
  } finally {
    signal?.removeEventListener("abort", onAbort);
    if (signal?.aborted) {
      // FileHandle.close() waits for in-flight I/O. Start it promptly but do
      // not make cancellation wait behind the operation being cancelled.
      void closeHandle();
    } else {
      // Abort may arrive after close has started. Keep its handled cleanup in
      // the background instead of making cancellation wait for file I/O.
      await awaitWithAbort(closeHandle(), signal);
    }
  }
}

export async function resolveConnectionTlsSettings(
  config: ConnectionConfig,
  signal?: AbortSignal,
): Promise<ResolvedConnectionTlsSettings | undefined> {
  throwIfAborted(signal);
  const tlsConfig = normalizeConnectionTlsConfig(config);
  if (!tlsConfig || tlsConfig.mode === "disabled") {
    return undefined;
  }

  const skipHostnameVerification =
    tlsConfig.mode === "requireVerifyCa" ||
    tlsConfig.mode === "requireTrustServerCertificate";
  const rejectUnauthorized = tlsConfig.mode !== "requireTrustServerCertificate";

  const ca = await readOptionalTlsFile(
    tlsConfig.caFilePath,
    "CA certificate",
    signal,
  );
  const cert = await readOptionalTlsFile(
    tlsConfig.certFilePath,
    "client certificate",
    signal,
  );
  const key = await readOptionalTlsFile(
    tlsConfig.keyFilePath,
    "client key",
    signal,
  );
  throwIfAborted(signal);

  return {
    config: tlsConfig,
    rejectUnauthorized,
    skipHostnameVerification,
    servername: getTlsServername(config),
    ca,
    cert,
    key,
    passphrase:
      typeof tlsConfig.keyPassphrase === "string" &&
      tlsConfig.keyPassphrase.length > 0
        ? tlsConfig.keyPassphrase
        : undefined,
    checkServerIdentity: skipHostnameVerification
      ? () => undefined
      : tls.checkServerIdentity,
  };
}
