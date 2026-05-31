import * as vscode from "vscode";

export async function readClipboardTextSafe(): Promise<string> {
  try {
    return await vscode.env.clipboard.readText();
  } catch {
    return "";
  }
}

export async function writeClipboardText(text: string): Promise<void> {
  await vscode.env.clipboard.writeText(text);
}
