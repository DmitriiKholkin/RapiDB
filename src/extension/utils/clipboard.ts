/**
 * Safe clipboard access for the RapiDB extension.
 *
 * Reads return an empty string on failure (with a warning log) so that
 * the calling feature can degrade gracefully; writes surface errors
 * normally so the caller can decide what to do.
 */

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
