import type * as http from "node:http";
import type * as tls from "node:tls";

export async function createOwnedSshConnectionAgentFactory() {
  const { Agent: AgentBase } = await import("agent-base");
  type SshAgent = InstanceType<typeof AgentBase>;
  return function createOwnedSshConnectionAgent(
    runtimeAgent: http.Agent,
    secureEndpoint: boolean,
    tlsOptions?: tls.ConnectionOptions | null,
  ): http.Agent {
    class ConnectionAgent extends AgentBase {
      private closed = false;

      override destroy(): void {
        this.closed = true;
        super.destroy();
      }

      override async connect(
        request: http.ClientRequest,
        options: Parameters<SshAgent["connect"]>[1],
      ) {
        if (this.closed)
          throw new Error("[RapiDB] SSH connection agent closed");
        const socket = await (runtimeAgent as unknown as SshAgent).connect(
          request,
          {
            ...options,
            secureEndpoint,
          },
        );
        if (this.closed) {
          socket.destroy();
          throw new Error("[RapiDB] SSH connection agent closed");
        }
        return socket;
      }
    }

    const agent = new ConnectionAgent({
      keepAlive: true,
      ...(secureEndpoint ? tlsOptions : {}),
    });
    agent.protocol = secureEndpoint ? "https:" : "http:";
    agent.defaultPort = secureEndpoint ? 443 : 80;
    return agent;
  };
}
