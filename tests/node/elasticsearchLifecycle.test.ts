import { describe, expect, it, vi } from "vitest";
import { ElasticsearchDriver } from "../../src/extension/dbDrivers/elasticsearch";

describe("ElasticsearchDriver — disconnect()", () => {
  it("closes the active client and clears connection state", async () => {
    const driver = new ElasticsearchDriver({
      id: "elasticsearch-disconnect-test",
      name: "Elasticsearch Disconnect Test",
      type: "elasticsearch",
      host: "localhost",
    });
    const close = vi.fn().mockResolvedValue(undefined);
    const driverState = driver as unknown as {
      client: { close: typeof close } | null;
      connected: boolean;
    };

    driverState.client = { close };
    driverState.connected = true;

    await driver.disconnect();

    expect(close).toHaveBeenCalledTimes(1);
    expect(driverState.client).toBeNull();
    expect(driver.isConnected()).toBe(false);
  });
});
