/**
 * Singleton workflow state shared between the workflow harness helpers
 * (getSharedWorkflowState) and the vscode mock factory.
 *
 * harness.tsx uses vi.hoisted() to install vi.mock("vscode", ...) which
 * creates its own state object — it does NOT import from this module.
 * This module is used by:
 *   - workflow.setup.ts (resetSharedWorkflowState in afterEach)
 *   - workflowContext.ts  (getSharedWorkflowState when no state is passed)
 *
 * IMPORTANT: harness.tsx MUST be imported first in every workflow test
 * file (as `import "./harness"`) to ensure vi.mock() is installed
 * before any production code tries to import "vscode".
 */
import {
  createWorkflowVscodeState,
  type WorkflowVscodeState,
} from "./bridge/workflowVscode";

let sharedState: WorkflowVscodeState | null = null;

export function getSharedWorkflowState(): WorkflowVscodeState {
  if (!sharedState) {
    sharedState = createWorkflowVscodeState();
  }
  return sharedState;
}

export function resetSharedWorkflowState(): WorkflowVscodeState {
  sharedState = createWorkflowVscodeState();
  return sharedState;
}
