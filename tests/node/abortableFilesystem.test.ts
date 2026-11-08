import { describe, expect, it, vi } from "vitest";
import { runAbortableFilesystemOperation } from "../../src/extension/utils/abortableFilesystem";

describe("abortable filesystem operations", () => {
  it("does not start work for an already cancelled request", async () => {
    const controller = new AbortController();
    controller.abort();
    const operation = vi.fn(async () => "unexpected");
    await expect(
      runAbortableFilesystemOperation(controller.signal, operation),
    ).rejects.toMatchObject({ name: "AbortError" });
    expect(operation).not.toHaveBeenCalled();
  });

  it("returns promptly on cancellation and disposes a resource acquired later", async () => {
    const controller = new AbortController();
    let finish!: (value: string) => void;
    const operation = vi.fn(
      () =>
        new Promise<string>((resolve) => {
          finish = resolve;
        }),
    );
    const dispose = vi.fn(async (_value: string) => undefined);
    const pending = runAbortableFilesystemOperation(
      controller.signal,
      operation,
      dispose,
    );
    await Promise.resolve();
    controller.abort();
    await expect(pending).rejects.toMatchObject({ name: "AbortError" });
    finish("resource");
    await vi.waitFor(() =>
      expect(dispose).toHaveBeenCalledExactlyOnceWith("resource"),
    );
  });
});
