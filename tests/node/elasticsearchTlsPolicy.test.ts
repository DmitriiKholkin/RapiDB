import { Client } from "@elastic/elasticsearch";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ElasticsearchDriver } from "../../src/extension/dbDrivers/elasticsearch";
import type {
  ConnectionConfig,
  ConnectionTlsMode,
} from "../../src/shared/connectionConfig";
import { validateConnectionConfig } from "../../src/shared/connectionValidation";

afterEach(() => vi.restoreAllMocks());

const config: ConnectionConfig = {
  id: "elastic-tls-policy",
  name: "Elasticsearch",
  type: "elasticsearch",
};
const requiredModes: ConnectionTlsMode[] = [
  "requireTrustServerCertificate",
  "requireVerifyCa",
  "requireVerifyFull",
  "mutualTls",
];

describe("Elasticsearch TLS endpoint policy", () => {
  it.each(
    requiredModes,
  )("rejects HTTP before creating a connection with %s", async (mode) => {
    const ping = vi.spyOn(Client.prototype, "ping");
    for (const field of ["connectionUri", "endpoint"] as const) {
      const connection = {
        ...config,
        [field]: "http://alice:secret@db.internal:9200",
        tls: { mode, certFilePath: "/cert", keyFilePath: "/key" },
      };
      const validation = validateConnectionConfig(connection);
      expect(validation.valid).toBe(false);
      expect(validation.message).toContain("requires an HTTPS");
      expect(validation.message).not.toContain("secret");
      const driver = new ElasticsearchDriver(connection);
      await expect(driver.connect()).rejects.toThrow("requires an HTTPS");
      expect(Reflect.get(driver, "client")).toBeNull();
    }
    expect(ping).not.toHaveBeenCalled();
  });

  it("uses the same endpoint precedence as the SDK and leaves Cloud ID routing alone", () => {
    const tls = { mode: "requireVerifyFull" as const };
    expect(
      validateConnectionConfig({
        ...config,
        tls,
        connectionUri: "http://db:9200",
        endpoint: "https://db:9200",
      }).valid,
    ).toBe(false);
    expect(
      validateConnectionConfig({
        ...config,
        tls,
        connectionUri: "https://db:9200",
        endpoint: "http://db:9200",
      }).valid,
    ).toBe(true);
    expect(
      validateConnectionConfig({
        ...config,
        tls,
        cloudId: "deployment:ZXM=",
        connectionUri: "http://unused:9200",
      }).valid,
    ).toBe(true);
  });

  it.each([
    ["https://db.internal:9200", "requireVerifyFull", "https:"],
    ["http://db.internal:9200", "disabled", "http:"],
  ] as const)("preserves an explicitly allowed %s endpoint", async (endpoint, mode, protocol) => {
    vi.spyOn(Client.prototype, "ping").mockResolvedValue(true);
    const connection = { ...config, endpoint, tls: { mode } };
    expect(validateConnectionConfig(connection).valid).toBe(true);
    const driver = new ElasticsearchDriver(connection);
    try {
      await driver.connect();
      const client = Reflect.get(driver, "client") as Client;
      expect(client.connectionPool.connections[0].url.protocol).toBe(protocol);
    } finally {
      await driver.disconnect();
    }
  });
});
