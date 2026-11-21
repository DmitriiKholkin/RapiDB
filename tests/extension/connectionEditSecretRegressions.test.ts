import { beforeEach, describe, expect, it, vi } from "vitest";
import { ConnectionManager } from "../../src/extension/connectionManager";
import { VSCodeConnectionManagerStore } from "../../src/extension/connectionManagerStore";
import { bindConnectionSecret } from "../../src/extension/connectionSecretBinding";
import { parseStoredConnectionSecrets } from "../../src/extension/connectionSecretsData";
import { ConnectionFormPanel } from "../../src/extension/panels/connectionFormPanel";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";
import type { ConnectionFormExistingState } from "../../src/shared/webviewContracts";
import { registerConnectionStorageFixture } from "../support/connectionStorageContext";
import type { MockWebviewPanel } from "../support/mockVscode";
import { SSH_HOST_FINGERPRINT_SHA256 } from "../support/sshFingerprints";

vi.mock("vscode", async () => {
  const { createMockVscodeModule } = await import("../support/mockVscode");
  return createMockVscodeModule().module;
});
vi.mock("../../src/extension/panels/webviewShell", () => ({
  createWebviewShell: vi.fn(() => "<html></html>"),
}));

import * as vscode from "vscode";
import { createWebviewShell } from "../../src/extension/panels/webviewShell";

const createExtensionContextStub = registerConnectionStorageFixture();

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

  it.each([
    "legacy",
    "bound",
  ])("repairs a duplicate without inheriting %s credentials and lets the new profile save its own", async (kind) => {
    const owner =
      kind === "bound"
        ? {
            ...pg,
            useSecretStorage: true,
            secretStorageProfile: "original-link",
          }
        : pg;
    const duplicate = {
      ...owner,
      name: "Other profile",
      host: "other.invalid",
    };
    const f = fixture([owner, duplicate]);
    const original =
      kind === "bound"
        ? bindConnectionSecret(
            owner,
            JSON.stringify({ password: "owner-password" }),
          )
        : "owner-password";
    await f.context.secrets.store(owner.id, original);
    await f.drainMigration();
    const [retained, repaired] = f.manager.getConnections();
    expect(retained.id).toBe(owner.id);
    expect(repaired.id).not.toBe(owner.id);
    expect(repaired.secretStorageProfile).toBeUndefined();
    expect((await f.manager._hydratePassword(retained)).password).toBe(
      "owner-password",
    );
    expect((await f.manager._hydratePassword(repaired)).password ?? "").toBe(
      "",
    );
    expect(await f.context.secrets.get(repaired.id)).toBeUndefined();
    const edit = await f.open(repaired);
    await edit.panel.webview.dispatchMessage({
      type: "saveConnection",
      payload: {
        ...repaired,
        password: "new-password",
        useSecretStorage: true,
      },
    });
    await edit.result;
    const saved = f.manager.getConnection(repaired.id, true);
    expect(saved?.host).toBe(duplicate.host);
    expect(
      parseStoredConnectionSecrets(await f.context.secrets.get(repaired.id)),
    ).toEqual({ password: "new-password" });
    expect(await f.context.secrets.get(owner.id)).toBe(original);
    await f.manager.dispose();
  });

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
      SSH_HOST_FINGERPRINT_SHA256,
      new AbortController().signal,
    );
    expect(f.connections()[0].ssh?.hostFingerprintSha256).toBe(
      SSH_HOST_FINGERPRINT_SHA256,
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
      ssh: { ...tofuSsh, hostFingerprintSha256: SSH_HOST_FINGERPRINT_SHA256 },
    });
    expect(JSON.parse((await f.context.secrets.get("a")) ?? "null")).toEqual({
      password: "db-secret",
      sshPassword: "ssh-secret",
    });
  });

  it("does not attach the stored fingerprint when the SSH host is edited", async () => {
    const { f, edit, payload } = await openTofu();
    const testConnection = vi
      .spyOn(f.manager, "testConnection")
      .mockResolvedValue({ success: true });
    await edit.panel.webview.dispatchMessage({
      type: "testConnection",
      payload: {
        ...payload,
        requestId: "changed-ssh",
        ssh: { ...payload.ssh, host: "other.local" },
      },
    });
    expect(testConnection).not.toHaveBeenCalled();
    expect(edit.panel.webview.postMessage).toHaveBeenCalledWith({
      type: "testResult",
      payload: expect.objectContaining({
        requestId: "changed-ssh",
        success: false,
        error: expect.stringContaining("ssh.password"),
      }),
    });
    await edit.panel.webview.dispatchMessage({
      type: "saveConnection",
      payload: {
        ...payload,
        password: "new-db-secret",
        ssh: {
          ...payload.ssh,
          host: "other.local",
          password: "new-ssh-secret",
        },
      },
    });
    await edit.result;
    expect(f.connections()[0].ssh).toMatchObject({ host: "other.local" });
    expect(f.connections()[0].ssh?.hostFingerprintSha256).toBeUndefined();
  });

  it("keeps the background-trusted fingerprint when only the SSH username is edited", async () => {
    const { f, edit, payload } = await openTofu();
    await edit.panel.webview.dispatchMessage({
      type: "saveConnection",
      payload: {
        ...payload,
        password: "new-db-secret",
        ssh: { ...payload.ssh, username: "other", password: "new-ssh-secret" },
      },
    });
    await edit.result;
    expect(saveResults(edit.panel)).toEqual([]);
    expect(f.connections()[0].ssh).toMatchObject({
      host: tofuSsh.host,
      port: tofuSsh.port,
      username: "other",
      hostFingerprintSha256: SSH_HOST_FINGERPRINT_SHA256,
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
        hostFingerprintSha256: SSH_HOST_FINGERPRINT_SHA256,
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

describe("signed Elasticsearch endpoint", () => {
  beforeEach(() => vi.clearAllMocks());

  it("keeps the original signed URL in Secret Storage through a rename-only edit", async () => {
    const base = "https://es.example.test/api";
    const endpoint = `${base}?x-aMz-%53ignature=fake%2bsignature&tag=a%20b&X-Amz-Security-Token=fake%2Ftoken&tag=%7e&X-Amz-Credential=fake%2Fcredential`;
    const safeEndpoint = `${base}?tag=a%20b&tag=%7e`;
    const config: ConnectionConfig = {
      id: "signed",
      name: "Elastic",
      type: "elasticsearch",
      endpoint,
      useSecretStorage: true,
    };
    const f = fixture([]);
    const testConnection = vi
      .spyOn(f.manager, "testConnection")
      .mockResolvedValue({ success: true });
    await f.manager.saveConnection(config);
    const [persisted] = f.connections();
    expect(persisted.endpoint).toBe(safeEndpoint);
    expect(
      JSON.parse((await f.context.secrets.get(config.id)) ?? "null"),
    ).toMatchObject({
      endpoint,
    });
    const edit = await f.open(persisted);
    const initial = vi.mocked(createWebviewShell).mock.calls.at(-1)?.[0]
      .initialState as { existing: ConnectionConfig };
    expect(initial.existing.endpoint).toBe(safeEndpoint);
    expect(JSON.stringify(initial)).not.toContain("fake");
    const payload = {
      ...persisted,
      name: "Renamed",
      keepConnectionUriCredentials: true,
    };

    await edit.panel.webview.dispatchMessage({
      type: "testConnection",
      payload: { ...payload, requestId: "r1" },
    });
    expect(testConnection.mock.calls[0]?.[0].endpoint).toBe(endpoint);
    await edit.panel.webview.dispatchMessage({
      type: "saveConnection",
      payload,
    });
    await edit.result;
    const [saved] = f.connections();
    expect(saved).toMatchObject({ name: "Renamed", endpoint: safeEndpoint });
    expect((await f.manager._hydratePassword(saved)).endpoint).toBe(endpoint);
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

  it("retains but does not resurrect the old PostgreSQL secrets after pg -> sqlite -> pg", async () => {
    const original = { ...pg, useSecretStorage: true };
    const f = fixture([original]);
    await f.context.secrets.store(
      "a",
      JSON.stringify({ password: "old-pg-secret" }),
    );
    const deletes = vi.spyOn(f.context.secrets, "delete");
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
      JSON.stringify({ password: "old-pg-secret" }),
    );
    expect(deletes).not.toHaveBeenCalled();

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
    expect(
      parseStoredConnectionSecrets(await f.context.secrets.get("a")),
    ).toEqual({});
    expect(
      (await f.manager._hydratePassword(f.connections()[0])).password,
    ).toBe("");
  });
});

describe("password resolution for the submitted engine", () => {
  beforeEach(() => vi.clearAllMocks());

  const legacyPassword = "synthetic-legacy-pg";
  const mysqlPassword = "synthetic-mysql";
  const replacement = "synthetic-replacement-pg";

  it.each([
    ["create MySQL with a blank draft", "create", "mysql", "", false, ""],
    [
      "create MySQL with a typed draft",
      "create",
      "mysql",
      mysqlPassword,
      false,
      mysqlPassword,
    ],
    ["switch from a keychain profile", "keychain", "mysql", "", false, ""],
    ["switch from a plaintext profile", "plaintext", "mysql", "", false, ""],
    [
      "switch and supply a new MySQL password",
      "keychain",
      "mysql",
      mysqlPassword,
      false,
      mysqlPassword,
    ],
    [
      "keep the hidden password on the original engine",
      "keychain",
      "pg",
      "",
      false,
      legacyPassword,
    ],
    ["remove the original password", "keychain", "pg", "", true, ""],
    [
      "replace the original password",
      "keychain",
      "pg",
      replacement,
      false,
      replacement,
    ],
  ] as const)("uses the same password for Test and Save: %s", async (_label, source, type, password, clearPassword, expected) => {
    const original =
      source === "create"
        ? undefined
        : {
            ...pg,
            useSecretStorage: source === "keychain",
            password: source === "plaintext" ? legacyPassword : undefined,
          };
    const f = fixture(original ? [original] : []);
    if (source === "keychain") {
      await f.context.secrets.store(pg.id, legacyPassword);
    }
    const testConnection = vi
      .spyOn(f.manager, "testConnection")
      .mockResolvedValue({ success: true });
    const { panel, result } = await f.open(original);
    const initial = vi.mocked(createWebviewShell).mock.calls.at(-1)?.[0]
      .initialState as { existing?: ConnectionFormExistingState };
    expect(initial.existing?.hasStoredSecret).toBe(original ? true : undefined);
    expect(JSON.stringify(initial)).not.toContain(legacyPassword);

    const payload = {
      ...pg,
      type,
      password,
      useSecretStorage: source !== "plaintext",
      hasStoredSecret: type === original?.type || undefined,
      clearPassword: type === original?.type ? clearPassword : undefined,
    };
    await panel.webview.dispatchMessage({
      type: "testConnection",
      payload: { ...payload, requestId: "r1" },
    });
    expect(testConnection).toHaveBeenCalledOnce();
    expect(testConnection.mock.calls[0]?.[0]).toMatchObject({
      type,
      password: expected,
    });

    await panel.webview.dispatchMessage({ type: "saveConnection", payload });
    expect(saveResults(panel)).toEqual([]);
    await expect(result).resolves.toMatchObject({ id: pg.id, type });
    const [saved] = f.connections();
    expect(saved.password).toBe(source === "plaintext" ? "" : undefined);
    expect(
      parseStoredConnectionSecrets(await f.context.secrets.get(pg.id)).password,
    ).toBe(expected || undefined);
    expect((await f.manager._hydratePassword(saved)).password).toBe(expected);
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
