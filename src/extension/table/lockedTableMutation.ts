import type { ConnectionManager } from "../connectionManager";
import { findUnverifiedTriggerVisibilityError } from "../dbDrivers/mysqlTriggerVisibility";
import type {
  IDBDriver,
  TableMutationTarget,
  TransactionOptions,
} from "../dbDrivers/types";
import { assertConnectionWritable } from "../utils/readOnlyGuards";

type UnverifiedTriggerVisibilityConfirmation = (
  tables: readonly string[],
) => Promise<boolean>;

let confirmUnverifiedTriggerVisibility:
  | UnverifiedTriggerVisibilityConfirmation
  | undefined;

/** Registered by the extension host; absent means fail closed. */
export function setUnverifiedTriggerVisibilityConfirmation(
  confirm: UnverifiedTriggerVisibilityConfirmation | undefined,
): void {
  confirmUnverifiedTriggerVisibility = confirm;
}

/**
 * Runs a locked table mutation. If the driver refused it before any DML
 * because trigger visibility cannot be proven, ask the user once and, only
 * after an explicit confirmation, retry with exactly the reported tables
 * accepted. Without a confirmation the original (not executed) error stands.
 */
export async function runLockedTableTransaction<TResult>(
  options: Pick<TransactionOptions, "lockedTableMutation">,
  run: (
    options: Pick<TransactionOptions, "lockedTableMutation">,
  ) => Promise<TResult>,
): Promise<TResult> {
  try {
    return await run(options);
  } catch (error) {
    const locked = options.lockedTableMutation;
    const confirm = confirmUnverifiedTriggerVisibility;
    const unverified = findUnverifiedTriggerVisibilityError(error);
    if (!locked || !confirm || !unverified) throw error;
    if (!(await confirm(unverified.tables))) throw error;
    return run({
      lockedTableMutation: {
        ...locked,
        acceptUnverifiedTriggerVisibility: [...unverified.tables],
      },
    });
  }
}

export function lockedTableMutationOptions(
  manager: ConnectionManager,
  driver: IDBDriver,
  connectionId: string,
  action: string,
  targets: readonly TableMutationTarget[],
): Pick<TransactionOptions, "lockedTableMutation"> {
  if (!driver.usesLockedTableMutationChecks) return {};
  return {
    lockedTableMutation: {
      targets,
      assertWritable: () => {
        assertConnectionWritable(manager, connectionId, action);
        if (manager.getDriver(connectionId) !== driver) {
          throw new Error(
            "Connection changed during mutation preflight. Prepare the changes again before retrying.",
          );
        }
      },
    },
  };
}
