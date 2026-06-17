import { afterAll, afterEach, beforeAll, beforeEach, vi } from "vitest";
import { ensureRunTempRoot } from "../../runtime/tempDirectories";
import { resetSharedWorkflowState } from "../sharedState";

function createStubCanvasContext(): CanvasRenderingContext2D {
  return {
    canvas: document.createElement("canvas"),
    font: "12px monospace",
    measureText(text: string): TextMetrics {
      return {
        width: text.length * 8,
      } as TextMetrics;
    },
  } as CanvasRenderingContext2D;
}

let workflowState = resetSharedWorkflowState();

export function getWorkflowState() {
  return workflowState;
}

beforeAll(async () => {
  process.env.TZ = "UTC";
  process.env.RAPIDB_WORKFLOW_TEST_MODE = "true";
  await ensureRunTempRoot();
});

beforeEach(() => {
  vi.spyOn(HTMLCanvasElement.prototype, "getContext").mockImplementation(
    (contextId: string) => {
      return contextId === "2d" ? createStubCanvasContext() : null;
    },
  );
});

afterEach(() => {
  // Replace the state so each test starts fresh. The mock factory closes
  // over the same state object via getSharedWorkflowState().
  workflowState = resetSharedWorkflowState();
});

afterAll(async () => {
  vi.restoreAllMocks();
});
