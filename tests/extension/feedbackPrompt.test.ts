import { readFileSync } from "node:fs";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { createProjectTempDir } from "../runtime/tempDirectories";

let showInformationMessage: ReturnType<typeof vi.fn>;
let openExternal: ReturnType<typeof vi.fn>;
let configuredConnections: unknown[];
let uriScheme: string;
let storagePath: string;
let settings: Map<string, unknown>;
let updateConfiguration: ReturnType<typeof vi.fn>;

function createContext(
  version: string | undefined,
  initialState: Record<string, unknown> = {},
) {
  const state = new Map(Object.entries(initialState));
  const setKeysForSync = vi.fn();
  return {
    state,
    setKeysForSync,
    context: {
      globalStorageUri: { fsPath: storagePath },
      extension:
        version === undefined ? undefined : { packageJSON: { version } },
      globalState: {
        get: <T>(key: string, fallback?: T) =>
          (state.get(key) ?? fallback) as T,
        update: vi.fn(async (key: string, value: unknown) => {
          state.set(key, value);
        }),
        setKeysForSync,
      },
    },
  };
}

async function loadModule() {
  return import("../../src/extension/feedbackPrompt");
}

describe("feedback prompt", () => {
  beforeEach(async () => {
    vi.resetModules();
    showInformationMessage = vi.fn().mockResolvedValue(undefined);
    openExternal = vi.fn().mockResolvedValue(true);
    configuredConnections = [];
    uriScheme = "vscode";
    storagePath = await createProjectTempDir("extension-host", "feedback-");
    settings = new Map();
    updateConfiguration = vi.fn(async (key: string, value: unknown) => {
      settings.set(key, value);
    });
    vi.doMock("vscode", () => ({
      version: "1.101.0",
      window: { showInformationMessage },
      env: {
        appName: "Visual Studio Code",
        get uriScheme() {
          return uriScheme;
        },
        openExternal,
      },
      Uri: { parse: (value: string) => ({ toString: () => value }) },
      ConfigurationTarget: { Global: 1 },
      workspace: {
        getConfiguration: () => ({
          get: (key: string, fallback?: unknown) =>
            key === "connections"
              ? configuredConnections
              : (settings.get(key) ?? fallback),
          update: updateConfiguration,
        }),
      },
    }));
  });

  it("stays silent on a fresh install but records the version", async () => {
    const { maybeShowFeedbackPrompt, LAST_SEEN_VERSION_STATE_KEY } =
      await loadModule();
    const { context, state } = createContext("0.2.0");
    await maybeShowFeedbackPrompt(context as never);
    expect(showInformationMessage).not.toHaveBeenCalled();
    expect(state.get(LAST_SEEN_VERSION_STATE_KEY)).toBe("0.2.0");
  });

  it("stays silent when the version is unchanged", async () => {
    const { maybeShowFeedbackPrompt, LAST_SEEN_VERSION_STATE_KEY } =
      await loadModule();
    const { context } = createContext("0.2.0", {
      [LAST_SEEN_VERSION_STATE_KEY]: "0.2.0",
    });
    await maybeShowFeedbackPrompt(context as never);
    expect(showInformationMessage).not.toHaveBeenCalled();
  });

  it("prompts once after an update without writing user settings", async () => {
    const { maybeShowFeedbackPrompt, LAST_SEEN_VERSION_STATE_KEY } =
      await loadModule();
    const { context, state, setKeysForSync } = createContext("0.2.0", {
      [LAST_SEEN_VERSION_STATE_KEY]: "0.1.17",
    });
    await maybeShowFeedbackPrompt(context as never);
    await maybeShowFeedbackPrompt(context as never);
    expect(showInformationMessage).toHaveBeenCalledOnce();
    expect(showInformationMessage).toHaveBeenCalledWith(
      expect.stringContaining("Updated to v0.2.0"),
      "Leave a Review",
      "Report a Bug",
      "Don't Show Again",
    );
    expect(state.get(LAST_SEEN_VERSION_STATE_KEY)).toBe("0.2.0");
    expect(setKeysForSync).toHaveBeenCalledWith([]);
    expect(updateConfiguration).not.toHaveBeenCalled();
  });

  it("prompts on consecutive updates without a time interval", async () => {
    const { maybeShowFeedbackPrompt, LAST_SEEN_VERSION_STATE_KEY } =
      await loadModule();
    const { context } = createContext("0.2.0", {
      [LAST_SEEN_VERSION_STATE_KEY]: "0.1.17",
    });
    await maybeShowFeedbackPrompt(context as never);
    context.extension = { packageJSON: { version: "0.2.1" } };
    await maybeShowFeedbackPrompt(context as never);
    expect(showInformationMessage).toHaveBeenCalledTimes(2);
  });

  it.each([
    ["existing RapiDB state", { "rapidb.queryHistory": [] }, []],
    ["configured connections", {}, [{ id: "conn-1" }]],
  ])("establishes a silent baseline despite %s", async (_label, initialState, connections) => {
    const { maybeShowFeedbackPrompt } = await loadModule();
    configuredConnections = connections;
    const { context } = createContext("0.2.0", initialState);
    await maybeShowFeedbackPrompt(context as never);
    expect(showInformationMessage).not.toHaveBeenCalled();
  });

  it("never prompts after a local opt-out and publishes it to Settings Sync", async () => {
    const {
      maybeShowFeedbackPrompt,
      LAST_SEEN_VERSION_STATE_KEY,
      FEEDBACK_PROMPT_DISABLED_STATE_KEY,
    } = await loadModule();
    const { context } = createContext("0.2.0", {
      [LAST_SEEN_VERSION_STATE_KEY]: "0.1.17",
      [FEEDBACK_PROMPT_DISABLED_STATE_KEY]: true,
    });
    await maybeShowFeedbackPrompt(context as never);
    expect(showInformationMessage).not.toHaveBeenCalled();
    expect(settings.get("feedbackPrompt.disabled")).toBe(true);
  });

  it("remembers Don't Show Again locally and in user settings", async () => {
    const {
      maybeShowFeedbackPrompt,
      LAST_SEEN_VERSION_STATE_KEY,
      FEEDBACK_PROMPT_DISABLED_STATE_KEY,
    } = await loadModule();
    showInformationMessage.mockResolvedValue("Don't Show Again");
    const { context, state } = createContext("0.2.0", {
      [LAST_SEEN_VERSION_STATE_KEY]: "0.1.17",
    });
    await maybeShowFeedbackPrompt(context as never);
    expect(state.get(FEEDBACK_PROMPT_DISABLED_STATE_KEY)).toBe(true);
    expect(updateConfiguration).toHaveBeenCalledWith(
      "feedbackPrompt.disabled",
      true,
      1,
    );
    expect(openExternal).not.toHaveBeenCalled();
    context.extension = { packageJSON: { version: "0.2.1" } };
    await maybeShowFeedbackPrompt(context as never);
    expect(showInformationMessage).toHaveBeenCalledOnce();
  });

  it.each([
    [
      "vscode",
      "https://marketplace.visualstudio.com/items?itemName=DmitriiKholkin.rapidb&ssr=false#review-details",
    ],
    [
      "vscode-insiders",
      "https://marketplace.visualstudio.com/items?itemName=DmitriiKholkin.rapidb&ssr=false#review-details",
    ],
    ["cursor", "https://open-vsx.org/extension/DmitriiKholkin/rapidb/reviews"],
    [
      "vscodium",
      "https://open-vsx.org/extension/DmitriiKholkin/rapidb/reviews",
    ],
    [
      "windsurf",
      "https://open-vsx.org/extension/DmitriiKholkin/rapidb/reviews",
    ],
  ])("opens the review page of the %s editor's store", async (scheme, url) => {
    const { maybeShowFeedbackPrompt, LAST_SEEN_VERSION_STATE_KEY } =
      await loadModule();
    uriScheme = scheme;
    showInformationMessage.mockResolvedValue("Leave a Review");
    const { context } = createContext("0.2.0", {
      [LAST_SEEN_VERSION_STATE_KEY]: "0.1.17",
    });
    await maybeShowFeedbackPrompt(context as never);
    expect(openExternal).toHaveBeenCalledOnce();
    expect(String(openExternal.mock.calls[0][0])).toBe(url);
  });

  it("opens a prefilled GitHub issue for bug reports", async () => {
    const { maybeShowFeedbackPrompt, LAST_SEEN_VERSION_STATE_KEY } =
      await loadModule();
    showInformationMessage.mockResolvedValue("Report a Bug");
    const { context } = createContext("0.2.0", {
      [LAST_SEEN_VERSION_STATE_KEY]: "0.1.17",
    });
    await maybeShowFeedbackPrompt(context as never);
    const url = new URL(String(openExternal.mock.calls[0][0]));
    expect(`${url.origin}${url.pathname}`).toBe(
      "https://github.com/DmitriiKholkin/RapiDB/issues/new",
    );
    expect(url.searchParams.get("body")).toContain("- RapiDB: 0.2.0");
    expect(url.searchParams.get("body")).toContain(
      "- Editor: Visual Studio Code 1.101.0",
    );
  });

  it("ignores contexts without version information", async () => {
    const { maybeShowFeedbackPrompt } = await loadModule();
    const { context, state } = createContext(undefined, {
      "rapidb.queryHistory": [],
    });
    await maybeShowFeedbackPrompt(context as never);
    expect(showInformationMessage).not.toHaveBeenCalled();
    expect(state.size).toBe(1);
  });

  it("honors a synced opt-out independently of the extension version/state", async () => {
    const {
      maybeShowFeedbackPrompt,
      LAST_SEEN_VERSION_STATE_KEY,
      FEEDBACK_PROMPT_DISABLED_STATE_KEY,
    } = await loadModule();
    settings.set("feedbackPrompt.disabled", true);
    const { context, state } = createContext("0.3.0", {
      [LAST_SEEN_VERSION_STATE_KEY]: "0.2.0",
    });
    await maybeShowFeedbackPrompt(context as never);
    expect(showInformationMessage).not.toHaveBeenCalled();
    expect(state.get(FEEDBACK_PROMPT_DISABLED_STATE_KEY)).toBe(true);
  });

  it("retains an observed opt-out if synced settings later disappear", async () => {
    const { maybeShowFeedbackPrompt, LAST_SEEN_VERSION_STATE_KEY } =
      await loadModule();
    settings.set("feedbackPrompt.disabled", true);
    const { context } = createContext("0.2.0", {
      [LAST_SEEN_VERSION_STATE_KEY]: "0.1.17",
    });
    await maybeShowFeedbackPrompt(context as never);
    settings.clear();
    context.extension = { packageJSON: { version: "0.3.0" } };
    await maybeShowFeedbackPrompt(context as never);
    expect(showInformationMessage).not.toHaveBeenCalled();
    expect(settings.get("feedbackPrompt.disabled")).toBe(true);
  });

  it("only lets one independently loaded window claim the version", async () => {
    const firstModule = await loadModule();
    const firstWindow = createContext("0.2.0", {
      [firstModule.LAST_SEEN_VERSION_STATE_KEY]: "0.1.17",
    });
    vi.resetModules();
    const secondModule = await loadModule();
    const secondWindow = createContext("0.2.0", {
      [secondModule.LAST_SEEN_VERSION_STATE_KEY]: "0.1.17",
    });
    await Promise.all([
      firstModule.maybeShowFeedbackPrompt(firstWindow.context as never),
      secondModule.maybeShowFeedbackPrompt(secondWindow.context as never),
    ]);
    expect(showInformationMessage).toHaveBeenCalledOnce();
  });

  it("does not show if saving the version fails", async () => {
    const { maybeShowFeedbackPrompt, LAST_SEEN_VERSION_STATE_KEY } =
      await loadModule();
    const { context } = createContext("0.2.0", {
      [LAST_SEEN_VERSION_STATE_KEY]: "0.1.17",
    });
    context.globalState.update.mockRejectedValue(
      new Error("storage unavailable"),
    );
    await expect(maybeShowFeedbackPrompt(context as never)).rejects.toThrow(
      "storage unavailable",
    );
    expect(showInformationMessage).not.toHaveBeenCalled();
  });

  it.each([
    "local",
    "synced",
  ])("retries a failed %s opt-out write on the next activation", async (store) => {
    const {
      maybeShowFeedbackPrompt,
      LAST_SEEN_VERSION_STATE_KEY,
      FEEDBACK_PROMPT_DISABLED_STATE_KEY,
    } = await loadModule();
    const { context, state } = createContext("0.2.0", {
      [LAST_SEEN_VERSION_STATE_KEY]: "0.1.17",
    });
    showInformationMessage.mockImplementation(async () => {
      if (store === "local") {
        context.globalState.update.mockRejectedValueOnce(
          new Error("write failed"),
        );
      } else {
        updateConfiguration.mockRejectedValueOnce(new Error("write failed"));
      }
      return "Don't Show Again";
    });
    await expect(maybeShowFeedbackPrompt(context as never)).rejects.toThrow(
      "write failed",
    );
    expect(
      state.get(FEEDBACK_PROMPT_DISABLED_STATE_KEY) === true ||
        settings.get("feedbackPrompt.disabled") === true,
    ).toBe(true);
    await maybeShowFeedbackPrompt(context as never);
    expect(state.get(FEEDBACK_PROMPT_DISABLED_STATE_KEY)).toBe(true);
    expect(settings.get("feedbackPrompt.disabled")).toBe(true);
    expect(showInformationMessage).toHaveBeenCalledOnce();
  });

  it("rechecks opt-out received while saving the version", async () => {
    const { maybeShowFeedbackPrompt, LAST_SEEN_VERSION_STATE_KEY } =
      await loadModule();
    const { context } = createContext("0.2.0", {
      [LAST_SEEN_VERSION_STATE_KEY]: "0.1.17",
    });
    const update = context.globalState.update.getMockImplementation();
    context.globalState.update.mockImplementation(async (key, value) => {
      await update?.(key, value);
      settings.set("feedbackPrompt.disabled", true);
    });
    await maybeShowFeedbackPrompt(context as never);
    expect(showInformationMessage).not.toHaveBeenCalled();
  });

  it("allows overriding auto-detection for a custom gallery", async () => {
    const { maybeShowFeedbackPrompt, LAST_SEEN_VERSION_STATE_KEY } =
      await loadModule();
    settings.set("feedbackPrompt.reviewStore", "marketplace");
    uriScheme = "custom-editor";
    showInformationMessage.mockResolvedValue("Leave a Review");
    const { context } = createContext("0.2.0", {
      [LAST_SEEN_VERSION_STATE_KEY]: "0.1.17",
    });
    await maybeShowFeedbackPrompt(context as never);
    expect(String(openExternal.mock.calls[0][0])).toContain(
      "marketplace.visualstudio.com",
    );
  });

  it("declares feedback settings as user-only and syncable", () => {
    const manifest = JSON.parse(
      readFileSync(new URL("../../package.json", import.meta.url), "utf8"),
    );
    const properties = manifest.contributes.configuration.properties;
    for (const key of ["disabled", "reviewStore"]) {
      expect(properties[`rapidb.feedbackPrompt.${key}`].scope).toBe(
        "application",
      );
      expect(properties[`rapidb.feedbackPrompt.${key}`].ignoreSync).not.toBe(
        true,
      );
    }
    expect(properties["rapidb.feedbackPrompt.lastShownAt"]).toBeUndefined();
  });
});
