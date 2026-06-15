/**
 * Connection form action handlers — validation, test, save.
 */
import { useCallback } from "react";
import type { ConnectionFormExistingState } from "../../../shared/webviewContracts";
import { postMessage } from "../../utils/messaging";
import type {
  ConnectionFormDerived,
  ConnectionFormState,
} from "./useConnectionFormState";
import { buildConnectionPayload } from "./useConnectionFormState";

// ─── Validation ─────────────────────────────────────────────────────────────

function validateRedisDatabase(database: string): string | null {
  if (database.trim().length > 0 && !/^\d+$/.test(database.trim())) {
    return "Redis database must be a non-negative integer.";
  }
  return null;
}

// ─── Actions Hook ───────────────────────────────────────────────────────────

export interface ConnectionFormActions {
  handleTest: () => void;
  handleSave: () => void;
  validateSubmission: () => boolean;
}

export function useConnectionFormActions(
  state: ConnectionFormState,
  derived: ConnectionFormDerived,
  existing: ConnectionFormExistingState | null,
): ConnectionFormActions {
  const validateSubmission = useCallback((): boolean => {
    if (derived.isRedis) {
      const redisError = validateRedisDatabase(state.database);
      if (redisError) {
        state.setTestState("fail");
        state.setTestError(redisError);
        return false;
      }
    }
    return true;
  }, [state, derived.isRedis]);

  const handleTest = useCallback(() => {
    if (!state.name.trim()) {
      state.setNameError("Name is required");
      return;
    }
    if (!validateSubmission()) {
      return;
    }
    state.setTestState("testing");
    state.setTestError("");
    postMessage(
      "testConnection",
      buildConnectionPayload(state, derived, existing),
    );
  }, [state, derived, existing, validateSubmission]);

  const handleSave = useCallback(() => {
    if (!state.name.trim()) {
      state.setNameError("Name is required");
      return;
    }
    if (!validateSubmission()) {
      return;
    }
    state.setSaving(true);
    postMessage(
      "saveConnection",
      buildConnectionPayload(state, derived, existing),
    );
  }, [state, derived, existing, validateSubmission]);

  return { handleTest, handleSave, validateSubmission };
}
