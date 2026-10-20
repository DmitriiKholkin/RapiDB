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
