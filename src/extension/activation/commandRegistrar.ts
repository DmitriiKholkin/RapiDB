/**
 * Resilient `vscode.commands.registerCommand` wrapper.
 *
 * Why a wrapper:
 *  - registers the resulting `Disposable` on the extension's
 *    `context.subscriptions` so cleanup is automatic
 *  - captures mis-registrations (e.g. a command name already used) and
 *    keeps activation going — the caller can log a summary at the end
 *  - keeps `extension.ts` free of try/catch noise
 */

import * as vscode from "vscode";
import { logger } from "../utils/logger";

export type RegisterCommand = <TArgs extends unknown[]>(
  command: string,
  callback: (...args: TArgs) => unknown,
) => vscode.Disposable;

export interface CommandRegistrar {
  register: RegisterCommand;
  /** Names of commands that failed to register. */
  failures: readonly string[];
}

/**
 * Build a registrar bound to the given extension context.
 *
 * @param context  Extension context; successful registrations are added
 *                 to `context.subscriptions` for automatic disposal.
 */
export function createCommandRegistrar(
  context: vscode.ExtensionContext,
): CommandRegistrar {
  const failures: string[] = [];

  const register: RegisterCommand = <TArgs extends unknown[]>(
    command: string,
    callback: (...args: TArgs) => unknown,
  ): vscode.Disposable => {
    try {
      const disposable = vscode.commands.registerCommand(command, callback);
      context.subscriptions.push(disposable);
      return disposable;
    } catch (err: unknown) {
      // Logged (vs. thrown) so a single mis-registered command does not
      // abort the rest of activation. The list is surfaced in the
      // Output Channel by the caller.
      logger.error(`Could not register command "${command}"`, err);
      failures.push(command);
      return { dispose: () => {} };
    }
  };

  return { register, failures };
}
