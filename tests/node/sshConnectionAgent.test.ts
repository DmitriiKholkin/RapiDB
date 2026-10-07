import type * as http from "node:http";
import { PassThrough } from "node:stream";
import { describe, expect, it, vi } from "vitest";
import { createOwnedSshConnectionAgentFactory } from "../../src/extension/services/sshConnectionAgent";

describe("connection-owned SSH agent lifecycle", () => {
  it("destroys a late-created socket, not the runtime agent, after close", async () => {
    const { Agent } = await import("agent-base");
    const createAgent = await createOwnedSshConnectionAgentFactory();
    let complete!: (socket: PassThrough) => void;
    const socketCreation = new Promise<PassThrough>((resolve) => {
      complete = resolve;
    });
    class RuntimeAgent extends Agent {
      override connect = vi.fn(() => socketCreation);
    }
    const runtime = new RuntimeAgent();
    const destroyRuntime = vi.spyOn(runtime, "destroy");
    const owned = createAgent(runtime, false) as InstanceType<typeof Agent>;
    const options = {
      host: "db.internal",
      port: 80,
      secureEndpoint: false as const,
    };
    const pending = Promise.resolve(
      owned.connect({} as http.ClientRequest, options),
    );
    expect(runtime.connect).toHaveBeenCalledOnce();
    owned.destroy();
    const lateSocket = new PassThrough();
    complete(lateSocket);
    await expect(pending).rejects.toThrow("SSH connection agent closed");
    expect(lateSocket.destroyed).toBe(true);
    expect(destroyRuntime).not.toHaveBeenCalled();
    await expect(
      owned.connect({} as http.ClientRequest, options),
    ).rejects.toThrow("SSH connection agent closed");
    expect(runtime.connect).toHaveBeenCalledOnce();
    runtime.destroy();
  });
});
