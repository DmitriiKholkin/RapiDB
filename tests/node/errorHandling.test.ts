import { inspect } from "node:util";
import { describe, expect, it, vi } from "vitest";
import { logErrorWithContext } from "../../src/extension/utils/errorHandling";
import { installLoggerSink, logger } from "../../src/extension/utils/logger";

describe("connection-aware diagnostic logging", () => {
  it("redacts bare configured secrets from both console paths and the installed sink", () => {
    const password = "synthetic-password-11";
    const token = "synthetic-token-11";
    const context = { password };
    const upstream = Object.assign(new Error(`Rejected ${password}`), {
      cause: new Error(`Remote said ${password}`),
      metadata: { token },
    });
    const label = `Lookup ${password} and ${token}`;
    const consoleError = vi
      .spyOn(console, "error")
      .mockImplementation(() => {});
    const sink = vi.fn();
    const expected = "Rejected [REDACTED]";
    try {
      expect(logErrorWithContext(label, upstream, context).message).toBe(
        expected,
      );
      expect(logger.error(label, upstream, context).message).toBe(expected);
      const output = inspect(consoleError.mock.calls, { depth: null });
      expect(output).toContain("Lookup [REDACTED] and [REDACTED]");
      expect(output).toContain(expected);
      expect(output).not.toContain(password);
      expect(output).not.toContain(token);

      const restore = installLoggerSink(sink);
      try {
        logger.error(label, upstream, context);
        expect(sink).toHaveBeenCalledWith({
          level: "error",
          context: "Lookup [REDACTED] and [REDACTED]",
          message: expect.stringContaining(expected),
        });
        expect(inspect(sink.mock.calls)).not.toContain(password);
      } finally {
        restore();
      }
      expect(upstream.message).toBe(`Rejected ${password}`);
      expect(upstream.cause.message).toBe(`Remote said ${password}`);
    } finally {
      consoleError.mockRestore();
    }
  });
});
