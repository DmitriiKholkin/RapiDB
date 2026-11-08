import { act, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";
import type { ConnectionFormSubmission } from "../../src/shared/webviewContracts/connection";
import { ConnectionFormView } from "../../src/webview/components/ConnectionFormView";
import {
  clearPostedMessages,
  dispatchIncomingMessage,
  getLastPostedMessage,
} from "./testUtils";

const pg = {
  id: "legacy",
  name: "Legacy",
  type: "pg" as const,
  host: "localhost",
  database: "postgres",
  username: "qa",
};
async function save(user: ReturnType<typeof userEvent.setup>) {
  await user.click(screen.getByRole("button", { name: "Save Changes" }));
  act(() => dispatchIncomingMessage("saveResult", { success: true }));
}

const payload = () =>
  getLastPostedMessage()?.payload as ConnectionFormSubmission;

describe("connection method selection and legacy profiles", () => {
  it("distinguishes MongoDB URI TLS defaults from an explicit disabled mode", async () => {
    const user = userEvent.setup();
    render(
      <ConnectionFormView
        existing={{
          id: "mongo",
          name: "Mongo",
          type: "mongodb",
          mongoConnectionMode: "uri",
          connectionUri: "mongodb+srv://mongo.example.com/db",
        }}
      />,
    );
    await user.click(
      screen.getByRole("switch", {
        name: "Use MongoDB TLS defaults / URI settings",
      }),
    );
    await save(user);
    expect(payload().tls?.mode).toBe("disabled");
    await user.click(
      screen.getByRole("switch", {
        name: "Use MongoDB TLS defaults / URI settings",
      }),
    );
    await save(user);
    expect(payload().tls).toBeUndefined();
    expect(payload().port).toBeUndefined();
  });

  it("preserves TLS settings when clicking the already selected database type", async () => {
    const user = userEvent.setup();
    render(
      <ConnectionFormView
        existing={{
          ...pg,
          tls: { mode: "requireVerifyFull", caFilePath: "/tmp/ca.pem" },
        }}
      />,
    );
    await user.click(screen.getByRole("button", { name: "PostgreSQL" }));
    await save(user);
    expect(payload().tls).toMatchObject({
      mode: "requireVerifyFull",
      caFilePath: "/tmp/ca.pem",
    });
  });

  it("sends explicit passphrase deletion without clearing the SSH key or TLS certificates", async () => {
    const user = userEvent.setup();
    render(
      <ConnectionFormView
        existing={{
          ...pg,
          ssh: {
            host: "bastion",
            username: "qa",
            authMethod: "privateKey",
            hostVerificationMode: "trustOnFirstUse",
          },
          hasStoredSshPrivateKey: true,
          hasStoredSshPassphrase: true,
          hasStoredTlsKeyPassphrase: true,
          tls: {
            mode: "mutualTls",
            certFilePath: "/tmp/client.crt",
            keyFilePath: "/tmp/client.key",
          },
        }}
      />,
    );
    await user.click(
      screen.getByRole("switch", { name: "Remove saved SSH passphrase" }),
    );
    await user.click(
      screen.getByRole("switch", { name: "Remove saved TLS key passphrase" }),
    );
    await save(user);
    expect(payload()).toMatchObject({
      clearSshPassphrase: true,
      clearTlsKeyPassphrase: true,
      hasStoredSshPrivateKey: true,
      tls: { certFilePath: "/tmp/client.crt", keyFilePath: "/tmp/client.key" },
    });
  });
  it("keeps legacy PostgreSQL environment defaults until explicitly disabled", async () => {
    const user = userEvent.setup();
    render(<ConnectionFormView existing={pg} />);
    expect((screen.getByLabelText("Port") as HTMLInputElement).value).toBe("");
    expect((screen.getByLabelText("TLS mode") as HTMLSelectElement).value).toBe(
      "inherit",
    );
    await user.type(screen.getByLabelText("Password"), "replacement");
    await save(user);
    expect(payload().tls).toBeUndefined();
    await user.selectOptions(screen.getByLabelText("TLS mode"), "disabled");
    await save(user);
    expect(payload().tls?.mode).toBe("disabled");
    await user.type(screen.getByLabelText("Port"), "5544");
    await save(user);
    expect(payload().port).toBe(5544);
    await user.clear(screen.getByLabelText("Port"));
    await save(user);
    expect(payload().port).toBeUndefined();
  });

  it("preserves legacy Elasticsearch priorities and explicitly switches to API key", async () => {
    const user = userEvent.setup();
    render(
      <ConnectionFormView
        existing={{
          id: "elastic",
          name: "Elastic",
          type: "elasticsearch",
          host: "unused",
          endpoint: "https://old.local",
          cloudId: "deployment:ZXM=",
          username: "elastic",
          hasStoredSecret: true,
          hasStoredApiKey: true,
        }}
      />,
    );
    expect(
      (
        screen.getByLabelText(
          "Elasticsearch connection method",
        ) as HTMLSelectElement
      ).value,
    ).toBe("cloud");
    expect(
      (
        screen.getByLabelText(
          "Elasticsearch authentication",
        ) as HTMLSelectElement
      ).value,
    ).toBe("password");
    expect(screen.queryByLabelText("Host")).toBeNull();
    expect(screen.queryByLabelText("Elasticsearch endpoint")).toBeNull();
    await save(user);
    expect(payload()).toMatchObject({
      cloudId: "deployment:ZXM=",
      username: "elastic",
      clearApiKey: false,
    });
    await user.selectOptions(
      screen.getByLabelText("Elasticsearch authentication"),
      "apiKey",
    );
    expect(screen.queryByLabelText("Username")).toBeNull();
    expect(screen.queryByLabelText("Password")).toBeNull();
    await user.type(
      screen.getByLabelText("Elasticsearch API key"),
      "replacement-key",
    );
    await save(user);
    expect(payload()).toMatchObject({
      username: undefined,
      password: "",
      apiKey: "replacement-key",
      clearPassword: true,
    });
  });

  it("keeps endpoint and cloud drafts but submits only the selected address", async () => {
    const user = userEvent.setup();
    render(
      <ConnectionFormView
        existing={{
          id: "es",
          name: "Elastic",
          type: "elasticsearch",
          endpoint: "https://es.local:9443",
        }}
      />,
    );
    await user.selectOptions(
      screen.getByLabelText("Elasticsearch connection method"),
      "cloud",
    );
    await user.type(
      screen.getByLabelText("Elasticsearch cloud id"),
      "deployment:ZXM=",
    );
    await user.selectOptions(
      screen.getByLabelText("Elasticsearch connection method"),
      "endpoint",
    );
    expect(
      (screen.getByLabelText("Elasticsearch endpoint") as HTMLInputElement)
        .value,
    ).toBe("https://es.local:9443");
    expect(
      screen.getByText(/HTTPS enables TLS with the client defaults/),
    ).toBeTruthy();
    await save(user);
    expect(payload()).toMatchObject({
      endpoint: "https://es.local:9443",
      cloudId: undefined,
      host: undefined,
      port: undefined,
    });
  });

  it("switches legacy AWS keys to a profile and then the default chain", async () => {
    const user = userEvent.setup();
    render(
      <ConnectionFormView
        existing={{
          id: "aws",
          name: "AWS",
          type: "dynamodb",
          awsRegion: "eu-west-1",
          awsProfile: "dormant-profile",
          hasStoredAwsKeyPair: true,
          hasStoredAwsCredentials: true,
        }}
      />,
    );
    expect(
      (screen.getByLabelText("AWS credential source") as HTMLSelectElement)
        .value,
    ).toBe("static");
    await save(user);
    expect(payload()).toMatchObject({
      awsProfile: "dormant-profile",
      clearAwsCredentials: false,
    });
    await user.selectOptions(
      screen.getByLabelText("AWS credential source"),
      "profile",
    );
    expect(screen.queryByLabelText("AWS access key id")).toBeNull();
    await save(user);
    expect(payload()).toMatchObject({
      awsProfile: "dormant-profile",
      clearAwsCredentials: true,
      awsAccessKeyId: undefined,
    });
    await user.selectOptions(
      screen.getByLabelText("AWS credential source"),
      "default",
    );
    await save(user);
    expect(payload()).toMatchObject({
      awsProfile: undefined,
      clearAwsCredentials: true,
    });
  });

  it("does not silently save incomplete selected authentication", async () => {
    const user = userEvent.setup();
    render(
      <ConnectionFormView
        existing={{
          id: "es",
          name: "Elastic",
          type: "elasticsearch",
          endpoint: "https://es.local",
        }}
      />,
    );
    await user.selectOptions(
      screen.getByLabelText("Elasticsearch authentication"),
      "apiKey",
    );
    clearPostedMessages();
    await save(user);
    expect(getLastPostedMessage()).toBeUndefined();
    expect(
      screen.getByText(/API key is required for API key authentication/),
    ).toBeTruthy();
  });
});
