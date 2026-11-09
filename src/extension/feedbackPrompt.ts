import { createHash } from "node:crypto";
import { mkdir } from "node:fs/promises";
import { join } from "node:path";
import * as vscode from "vscode";

export const LAST_SEEN_VERSION_STATE_KEY = "rapidb.lastSeenVersion";
export const FEEDBACK_PROMPT_DISABLED_STATE_KEY =
  "rapidb.feedbackPromptDisabled";
export const FEEDBACK_PROMPT_DISABLED_SETTING = "feedbackPrompt.disabled";
export const FEEDBACK_REVIEW_STORE_SETTING = "feedbackPrompt.reviewStore";

const EXTENSION_ID = "DmitriiKholkin.rapidb";
const MARKETPLACE_REVIEW_URL = `https://marketplace.visualstudio.com/items?itemName=${EXTENSION_ID}&ssr=false#review-details`;
const OPEN_VSX_REVIEW_URL =
  "https://open-vsx.org/extension/DmitriiKholkin/rapidb/reviews";
const NEW_ISSUE_URL = "https://github.com/DmitriiKholkin/RapiDB/issues/new";

const MARKETPLACE_URI_SCHEMES = new Set([
  "vscode",
  "vscode-insiders",
  "vscode-exploration",
]);

const LEAVE_REVIEW = "Leave a Review";
const REPORT_BUG = "Report a Bug";
const DONT_SHOW_AGAIN = "Don't Show Again";

export type ExtensionStore = "marketplace" | "openVsx";

export function detectExtensionStore(uriScheme: string): ExtensionStore {
  return MARKETPLACE_URI_SCHEMES.has(uriScheme) ? "marketplace" : "openVsx";
}

export function getReviewUrl(store: ExtensionStore): string {
  return store === "marketplace" ? MARKETPLACE_REVIEW_URL : OPEN_VSX_REVIEW_URL;
}

export function getBugReportUrl(extensionVersion: string): string {
  const body = [
    "**Describe the bug**",
    "",
    "",
    "**Steps to reproduce**",
    "",
    "",
    "**Environment**",
    `- RapiDB: ${extensionVersion}`,
    `- Editor: ${vscode.env.appName} ${vscode.version}`,
    `- OS: ${process.platform} ${process.arch}`,
  ].join("\n");
  return `${NEW_ISSUE_URL}?body=${encodeURIComponent(body)}`;
}

async function claimVersionPrompt(
  storagePath: string,
  version: string,
): Promise<boolean> {
  const directory = join(storagePath, "feedback-prompts");
  await mkdir(directory, { recursive: true });
  const versionKey = createHash("sha256").update(version).digest("hex");
  try {
    await mkdir(join(directory, versionKey));
    return true;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "EEXIST") return false;
    throw error;
  }
}

async function persistOptOut(context: vscode.ExtensionContext): Promise<void> {
  const configuration = vscode.workspace.getConfiguration("rapidb");
  await Promise.all([
    context.globalState.get<boolean>(FEEDBACK_PROMPT_DISABLED_STATE_KEY) ===
    true
      ? Promise.resolve()
      : context.globalState.update(FEEDBACK_PROMPT_DISABLED_STATE_KEY, true),
    configuration.get<boolean>(FEEDBACK_PROMPT_DISABLED_SETTING, false)
      ? Promise.resolve()
      : configuration.update(
          FEEDBACK_PROMPT_DISABLED_SETTING,
          true,
          vscode.ConfigurationTarget.Global,
        ),
  ]);
}

export async function maybeShowFeedbackPrompt(
  context: vscode.ExtensionContext,
): Promise<void> {
  const globalState = context.globalState;
  const currentVersion: unknown = context.extension?.packageJSON?.version;
  if (!globalState || typeof currentVersion !== "string") {
    return;
  }

  globalState.setKeysForSync([]);

  const previousVersion = globalState.get<string>(LAST_SEEN_VERSION_STATE_KEY);
  const isUpdate =
    typeof previousVersion === "string" && previousVersion !== currentVersion;
  const configuration = vscode.workspace.getConfiguration("rapidb");
  const disabled =
    globalState.get<boolean>(FEEDBACK_PROMPT_DISABLED_STATE_KEY) === true ||
    configuration.get<boolean>(FEEDBACK_PROMPT_DISABLED_SETTING, false);
  if (disabled) {
    await persistOptOut(context);
  }

  if (previousVersion !== currentVersion) {
    await globalState.update(LAST_SEEN_VERSION_STATE_KEY, currentVersion);
  }

  if (!isUpdate || disabled) {
    return;
  }

  const storagePath = context.globalStorageUri?.fsPath;
  if (
    !storagePath ||
    !(await claimVersionPrompt(storagePath, currentVersion))
  ) {
    return;
  }

  if (
    vscode.workspace
      .getConfiguration("rapidb")
      .get<boolean>(FEEDBACK_PROMPT_DISABLED_SETTING, false)
  ) {
    await persistOptOut(context);
    return;
  }

  const choice = await vscode.window.showInformationMessage(
    `[RapiDB] Updated to v${currentVersion}. Enjoying RapiDB? A quick review or a bug report helps the project and its author a lot.`,
    LEAVE_REVIEW,
    REPORT_BUG,
    DONT_SHOW_AGAIN,
  );

  if (choice === LEAVE_REVIEW) {
    const configuredStore = vscode.workspace
      .getConfiguration("rapidb")
      .get<string>(FEEDBACK_REVIEW_STORE_SETTING, "auto");
    const store =
      configuredStore === "marketplace" || configuredStore === "openVsx"
        ? configuredStore
        : detectExtensionStore(vscode.env.uriScheme);
    await vscode.env.openExternal(vscode.Uri.parse(getReviewUrl(store)));
  } else if (choice === REPORT_BUG) {
    await vscode.env.openExternal(
      vscode.Uri.parse(getBugReportUrl(currentVersion)),
    );
  } else if (choice === DONT_SHOW_AGAIN) {
    await persistOptOut(context);
  }
}
