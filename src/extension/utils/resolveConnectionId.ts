import type { ConnectionManager } from "../connectionManager";
import { pickConnectionWithPrompt } from "../connectionManagerPrompts";
import type { RapiDBNode } from "../providers/connectionProvider";

export async function resolveConnectionId(
  node: RapiDBNode | undefined,
  connectionManager: ConnectionManager,
): Promise<string | undefined> {
  if (node?.connectionId) {
    return node.connectionId;
  }
  return pickConnectionWithPrompt(connectionManager);
}
