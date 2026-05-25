import { render, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { MonacoEditor } from "../../src/webview/components/MonacoEditor";

describe("MonacoEditor", () => {
  it("does not emit onChange when syncing a new initialValue", async () => {
    const handleChange = vi.fn();

    const { rerender } = render(
      <MonacoEditor initialValue='{"key":1}' onChange={handleChange} />,
    );

    rerender(<MonacoEditor initialValue="" onChange={handleChange} />);

    await waitFor(() => {
      expect(handleChange).not.toHaveBeenCalled();
    });
  });
});
