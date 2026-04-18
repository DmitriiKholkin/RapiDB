import { afterEach, describe, expect, it, vi } from "vitest";
import {
  logErrorWithContext,
  normalizeUnknownError,
} from "../../src/extension/utils/errorHandling";

afterEach(() => {
  vi.restoreAllMocks();
});

describe("normalizeUnknownError", () => {
  it("returns existing Error instances unchanged", () => {
    const error = new Error("boom");

    expect(normalizeUnknownError(error)).toBe(error);
  });

  it("wraps string and primitive values in Error instances", () => {
    expect(normalizeUnknownError("boom").message).toBe("boom");
    expect(normalizeUnknownError(42).message).toBe("42");
  });
});

describe("logErrorWithContext", () => {
  it("logs the normalized error with the RapiDB prefix and returns it", () => {
    const consoleError = vi
      .spyOn(console, "error")
      .mockImplementation(() => {});
    const error = new Error("connect failed");

    Object.defineProperty(error, "stack", {
      value: undefined,
      configurable: true,
    });

    const result = logErrorWithContext("Connect command failed", error);

    expect(result).toBe(error);
    expect(consoleError).toHaveBeenCalledWith(
      "[RapiDB] Connect command failed:",
      "connect failed",
    );
  });
});
