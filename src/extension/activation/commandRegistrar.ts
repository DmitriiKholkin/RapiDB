import * as vscode from "vscode";
import { logger } from "../utils/logger";

export type RegisterCommand = <TArgs extends unknown[]>(
  command: string,
  callback: (...args: TArgs) => unknown,
) => vscode.Disposable;

export interface CommandRegistrar {
  register: RegisterCommand;
  failures: readonly string[];
}

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
      logger.error(`Could not register command "${command}"`, err);
      failures.push(command);
      return { dispose: () => {} };
    }
  };

  return { register, failures };
}
