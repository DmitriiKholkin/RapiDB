import * as vscode from "vscode";
import { logger } from "./logger";

export async function readClipboardTextSafe(): Promise<string> {
  try {
    return await vscode.env.clipboard.readText();
  } catch (error: unknown) {
    logger.warn("Failed to read clipboard text");
    if (error instanceof Error) {
      logger.debug(error.message);
    }
    return "";
  }
}

export async function writeClipboardText(text: string): Promise<void> {
  await vscode.env.clipboard.writeText(text);
}
