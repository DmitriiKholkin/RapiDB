import { beforeEach, describe, expect, it, vi } from "vitest";
import { ConnectionManager } from "../../src/extension/connectionManager";
import { VSCodeConnectionManagerStore } from "../../src/extension/connectionManagerStore";
import { ConnectionFormPanel } from "../../src/extension/panels/connectionFormPanel";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";
import { createExtensionContextStub } from "../support/fakeConnectionManagerStore";
import type { MockWebviewPanel } from "../support/mockVscode";

vi.mock("vscode", async () => {
  const { createMockVscodeModule } = await import("../support/mockVscode");
  return createMockVscodeModule().module;
});
vi.mock("../../src/extension/panels/webviewShell", () => ({
  createWebviewShell: vi.fn(() => "<html></html>"),
}));

import * as vscode from "vscode";
import { createWebviewShell } from "../../src/extension/panels/webviewShell";

function fixture(configs: ConnectionConfig[]) {
  let connections = structuredClone(configs);
  const context = createExtensionContextStub();
  const configuration = {
    get: (key: string) => (key === "connections" ? connections : undefined),
    inspect: () => ({ globalValue: connections }),
    update: vi.fn(async (_key: string, next: ConnectionConfig[]) => {
      connections = structuredClone(next);
    }),
  };
  vi.mocked(vscode.workspace.getConfiguration).mockReturnValue(
    configuration as never,
  );
  const store = new VSCodeConnectionManagerStore(context as never);
  const manager = new ConnectionManager(context as never, store);
  const open = async (existing?: ConnectionConfig) => {
    const count = vi.mocked(vscode.window.createWebviewPanel).mock.calls.length;
    const result = ConnectionFormPanel.show(
      context as never,
      manager,
      existing && structuredClone(existing),
    );
    await vi.waitFor(() =>
      expect(vscode.window.createWebviewPanel).toHaveBeenCalledTimes(count + 1),
    );
    const panel = vi.mocked(vscode.window.createWebviewPanel).mock.results[
      count
    ].value as MockWebviewPanel;
    return { panel, result };
  };
  const drainMigration = async () => {
    manager.getConnections();
    await store.mutateConnections(() => ({ result: undefined }));
    await store.mutateConnections(() => ({ result: undefined }));
  };
  return {
    context,
    store,
    manager,
    open,
    drainMigration,
    connections: () => connections,
  };
}

function saveResults(panel: MockWebviewPanel) {
  return vi
    .mocked(panel.webview.postMessage)
    .mock.calls.map(
      ([message]) => message as { type: string; payload: unknown },
    )
    .filter((message) => message.type === "saveResult");
}

const pg: ConnectionConfig = {
  id: "a",
  name: "Original",
  type: "pg",
  host: "db.local",
  database: "app",
  username: "reader",
};

const tofuSsh = {
  host: "bastion.local",
  port: 22,
  username: "tunnel",
  authMethod: "password" as const,
  hostVerificationMode: "trustOnFirstUse" as const,
};

describe("edit precondition ignores extension-managed background writes", () => {
  beforeEach(() => vi.clearAllMocks());

  it("saves a rename after legacy secret migration flags useSecretStorage", async () => {
    const f = fixture([pg]);
    await f.context.secrets.store("a", JSON.stringify({ password: "stored" }));
    const edit = await f.open(pg);
    await f.drainMigration();
    expect(f.connections()[0].useSecretStorage).toBe(true);

    await edit.panel.webview.dispatchMessage({
      type: "saveConnection",
      payload: {
        ...pg,
        name: "Renamed",
        password: "",
        hasStoredSecret: true,
        useSecretStorage: true,
      },
    });
    await edit.result;
    expect(saveResults(edit.panel)).toEqual([]);
    expect(f.connections()[0]).toMatchObject({
      name: "Renamed",
      useSecretStorage: true,
    });
    expect(JSON.parse((await f.context.secrets.get("a")) ?? "null")).toEqual({
      password: "stored",
    });
  });

  it("still rejects a concurrent rename after the migration", async () => {
    const f = fixture([pg]);
    await f.context.secrets.store("a", JSON.stringify({ password: "stored" }));
    const edit = await f.open(pg);
    await f.drainMigration();
    await f.manager.saveConnection({
      ...pg,
      name: "Other editor",
      useSecretStorage: true,
    });

    await edit.panel.webview.dispatchMessage({
      type: "saveConnection",
      payload: { ...pg, name: "Stale", password: "", hasStoredSecret: true },
    });
    expect(saveResults(edit.panel)).toEqual([
      {
        type: "saveResult",
        payload: {
          success: false,
          error: expect.stringContaining("changed since"),
        },
      },
    ]);
    expect(f.connections()[0].name).toBe("Other editor");
    edit.panel.dispose();
    await edit.result;
  });

  async function openTofu() {
    const config: ConnectionConfig = {
      ...pg,
      useSecretStorage: true,
      ssh: tofuSsh,
    };
    const f = fixture([config]);
    await f.context.secrets.store(
      "a",
      JSON.stringify({ password: "db-secret", sshPassword: "ssh-secret" }),
    );
    const edit = await f.open(config);
    await (
      f.manager as unknown as {
        persistTrustedSshFingerprintIfNeeded(
          config: ConnectionConfig,
          fingerprint: string,
          signal: AbortSignal,
        ): Promise<void>;
      }
    ).persistTrustedSshFingerprintIfNeeded(
      config,
      "SHA256:trusted",
      new AbortController().signal,
    );
    expect(f.connections()[0].ssh?.hostFingerprintSha256).toBe(
      "SHA256:trusted",
    );
    const payload = {
      ...config,
      password: "",
      hasStoredSecret: true,
      hasStoredSshPassword: true,
      ssh: { ...tofuSsh, password: "" },
    };
    return { f, edit, config, payload };
  }

  it("saves after a trust-on-first-use fingerprint was persisted and keeps it", async () => {
    const { f, edit, payload } = await openTofu();
    await edit.panel.webview.dispatchMessage({
      type: "saveConnection",
      payload: { ...payload, name: "Renamed" },
    });
    await edit.result;
    expect(saveResults(edit.panel)).toEqual([]);
    expect(f.connections()[0]).toMatchObject({
      name: "Renamed",
      ssh: { ...tofuSsh, hostFingerprintSha256: "SHA256:trusted" },
    });
    expect(JSON.parse((await f.context.secrets.get("a")) ?? "null")).toEqual({
      password: "db-secret",
      sshPassword: "ssh-secret",
    });
  });

  it("does not attach the stored fingerprint when the SSH host is edited", async () => {
    const { f, edit, payload } = await openTofu();
    await edit.panel.webview.dispatchMessage({
      type: "saveConnection",
      payload: { ...payload, ssh: { ...payload.ssh, host: "other.local" } },
    });
    await edit.result;
    expect(f.connections()[0].ssh).toMatchObject({ host: "other.local" });
    expect(f.connections()[0].ssh?.hostFingerprintSha256).toBeUndefined();
  });

  it("keeps the background-trusted fingerprint when only the SSH username is edited", async () => {
    const { f, edit, payload } = await openTofu();
    await edit.panel.webview.dispatchMessage({
      type: "saveConnection",
      payload: { ...payload, ssh: { ...payload.ssh, username: "other" } },
    });
    await edit.result;
    expect(saveResults(edit.panel)).toEqual([]);
    expect(f.connections()[0].ssh).toMatchObject({
      host: tofuSsh.host,
      port: tofuSsh.port,
      username: "other",
      hostFingerprintSha256: "SHA256:trusted",
    });
  });

  it.each([
    ["host change", { host: "moved.local" }],
    ["secret change", { password: "rotated" }],
  ])("still rejects a concurrent %s next to the fingerprint write", async (_label, change) => {
    const { f, edit, config, payload } = await openTofu();
    await f.manager.saveConnection({
      ...config,
      ssh: {
        ...tofuSsh,
        password: "ssh-secret",
        hostFingerprintSha256: "SHA256:trusted",
      },
      ...change,
    });
    await edit.panel.webview.dispatchMessage({
      type: "saveConnection",
      payload: { ...payload, name: "Stale" },
    });
    expect(saveResults(edit.panel)).toEqual([
      {
        type: "saveResult",
        payload: {
          success: false,
          error: expect.stringContaining("changed since"),
        },
      },
    ]);
    expect(f.connections()[0].name).toBe("Original");
    edit.panel.dispose();
    await edit.result;
  });
});

describe("legacy DynamoDB awsEndpoint", () => {
  beforeEach(() => vi.clearAllMocks());

  it.each([
    true,
    false,
  ])("keeps endpoint credentials on a rename-only edit (secretStorage=%s)", async (useSecretStorage) => {
    const secretEndpoint = "https://dynamo.local:8000/?token=endpoint-secret";
    const config: ConnectionConfig = {
      id: "a",
      name: "Dynamo",
      type: "dynamodb",
      awsRegion: "us-east-1",
      awsEndpoint: useSecretStorage
        ? "https://dynamo.local:8000/"
        : secretEndpoint,
      useSecretStorage,
    };
    const f = fixture([config]);
    if (useSecretStorage) {
      await f.context.secrets.store(
        "a",
        JSON.stringify({ awsEndpoint: secretEndpoint }),
      );
    } else {
      vi.mocked(vscode.window.showWarningMessage).mockResolvedValueOnce(
        "Save in Plaintext" as never,
      );
    }
    const testConnection = vi
      .spyOn(f.manager, "testConnection")
      .mockResolvedValue({ success: true });
    const edit = await f.open(config);
    const initial = vi.mocked(createWebviewShell).mock.calls.at(-1)?.[0]
      .initialState as { existing: ConnectionConfig };
    expect(JSON.stringify(initial)).not.toContain("endpoint-secret");
    const payload = {
      id: "a",
      name: "Renamed",
      type: "dynamodb",
      awsRegion: "us-east-1",
      endpoint: initial.existing.awsEndpoint,
      useSecretStorage,
      clearAwsCredentials: true,
    };

    await edit.panel.webview.dispatchMessage({
      type: "testConnection",
      payload: { ...payload, requestId: "r1" },
    });
    expect(testConnection.mock.calls[0]?.[0].endpoint).toBe(secretEndpoint);
    await edit.panel.webview.dispatchMessage({
      type: "saveConnection",
      payload,
    });
    await edit.result;
    const saved = f.connections()[0];
    expect(saved.name).toBe("Renamed");
    expect(saved.awsEndpoint).toBeUndefined();
    if (useSecretStorage) {
      expect(saved.endpoint).toBe("https://dynamo.local:8000/");
      expect(
        JSON.parse((await f.context.secrets.get("a")) ?? "null"),
      ).toMatchObject({ endpoint: secretEndpoint });
    } else {
      expect(saved.endpoint).toBe(secretEndpoint);
    }
    expect((await f.manager._hydratePassword(saved)).endpoint).toBe(
      secretEndpoint,
    );
  });
});

describe("SQLite never touches Secret Storage", () => {
  beforeEach(() => vi.clearAllMocks());

  it("opens, saves and hydrates a flagged SQLite profile while the keychain is locked", async () => {
    const config: ConnectionConfig = {
      id: "s",
      name: "Local",
      type: "sqlite",
      filePath: "/tmp/rapidb-local.db",
      useSecretStorage: true,
    };
    const f = fixture([config]);
    const get = vi
      .spyOn(f.context.secrets, "get")
      .mockRejectedValue(new Error("keychain locked"));
    const store = vi.spyOn(f.context.secrets, "store");
    const edit = await f.open(config);
    await edit.panel.webview.dispatchMessage({
      type: "saveConnection",
      payload: { ...config, name: "Renamed" },
    });
    await edit.result;
    expect(saveResults(edit.panel)).toEqual([]);
    expect(f.connections()[0].name).toBe("Renamed");
    await f.drainMigration();
    expect(await f.manager._hydratePassword(f.connections()[0])).toEqual(
      f.connections()[0],
    );
    expect(get).not.toHaveBeenCalled();
    expect(store).not.toHaveBeenCalled();
  });
});

describe("type switches through SQLite", () => {
  beforeEach(() => vi.clearAllMocks());

  it.each([
    false,
    true,
  ])("does not resurrect the old PostgreSQL secrets after pg -> sqlite -> pg (deleteFails=%s)", async (deleteFails) => {
    const original = { ...pg, useSecretStorage: true };
    const f = fixture([original]);
    await f.context.secrets.store(
      "a",
      JSON.stringify({ password: "old-pg-secret" }),
    );
    if (deleteFails) {
      vi.spyOn(f.context.secrets, "delete").mockRejectedValueOnce(
        new Error("keychain locked"),
      );
    }
    const toSqlite = await f.open(original);
    await toSqlite.panel.webview.dispatchMessage({
      type: "saveConnection",
      payload: {
        id: "a",
        name: "Original",
        type: "sqlite",
        filePath: "/tmp/rapidb-switch.db",
        useSecretStorage: true,
      },
    });
    await toSqlite.result;
    expect(saveResults(toSqlite.panel)).toEqual([]);
    expect(f.connections()[0].type).toBe("sqlite");
    expect(await f.context.secrets.get("a")).toBe(
      deleteFails ? JSON.stringify({ password: "old-pg-secret" }) : undefined,
    );

    const testConnection = vi
      .spyOn(f.manager, "testConnection")
      .mockResolvedValue({ success: true });
    const toPg = await f.open(f.connections()[0]);
    const payload = { ...original, password: "" };
    await toPg.panel.webview.dispatchMessage({
      type: "testConnection",
      payload: { ...payload, requestId: "r1" },
    });
    expect(testConnection.mock.calls[0]?.[0].password).toBe("");
    await toPg.panel.webview.dispatchMessage({
      type: "saveConnection",
      payload,
    });
    await toPg.result;
    expect(saveResults(toPg.panel)).toEqual([]);
    expect(f.connections()[0]).toMatchObject({ type: "pg" });
    expect(f.connections()[0].password).toBeUndefined();
    expect(await f.context.secrets.get("a")).toBeUndefined();
    expect(
      (await f.manager._hydratePassword(f.connections()[0])).password,
    ).toBe("");
  });
});

describe("connection form id binding", () => {
  beforeEach(() => vi.clearAllMocks());

  const other: ConnectionConfig = {
    ...pg,
    id: "b",
    name: "Other",
    useSecretStorage: true,
  };

  it.each([
    "testConnection",
    "saveConnection",
  ] as const)("rejects %s for another connection id in an edit form", async (type) => {
    const f = fixture([{ ...pg, useSecretStorage: true }, other]);
    await f.context.secrets.store(
      "b",
      JSON.stringify({ password: "b-secret" }),
    );
    const testConnection = vi
      .spyOn(f.manager, "testConnection")
      .mockResolvedValue({ success: true });
    const edit = await f.open({ ...pg, useSecretStorage: true });
    const get = vi.spyOn(f.context.secrets, "get");
    await edit.panel.webview.dispatchMessage({
      type,
      payload: {
        ...other,
        host: "attacker.example",
        password: "",
        hasStoredSecret: true,
        requestId: "r1",
      },
    });
    expect(edit.panel.webview.postMessage).toHaveBeenCalledWith({
      type: type === "testConnection" ? "testResult" : "saveResult",
      payload: expect.objectContaining({
        success: false,
        error: expect.stringContaining("different connection"),
      }),
    });
    expect(get).not.toHaveBeenCalled();
    expect(testConnection).not.toHaveBeenCalled();
    expect(f.connections().map(({ host }) => host)).toEqual([
      "db.local",
      "db.local",
    ]);
    edit.panel.dispose();
    await edit.result;
  });

  it("does not use saved secrets of an existing id in a create form", async () => {
    const f = fixture([other]);
    await f.context.secrets.store(
      "b",
      JSON.stringify({ password: "b-secret" }),
    );
    const testConnection = vi
      .spyOn(f.manager, "testConnection")
      .mockResolvedValue({ success: true });
    const get = vi.spyOn(f.context.secrets, "get");
    const create = await f.open();
    await create.panel.webview.dispatchMessage({
      type: "testConnection",
      payload: {
        ...other,
        host: "attacker.example",
        password: "",
        hasStoredSecret: true,
        requestId: "r1",
      },
    });
    expect(get).not.toHaveBeenCalled();
    expect(testConnection).toHaveBeenCalledOnce();
    expect(JSON.stringify(testConnection.mock.calls[0]?.[0])).not.toContain(
      "b-secret",
    );
    create.panel.dispose();
    await create.result;
  });
});
