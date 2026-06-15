/**
 * Shared helper for VSCode commands that need to derive a connection id
 * from either a tree-view `RapiDBNode` argument or an interactive
 * `pickConnection` prompt.
 *
 * Previously this helper was copy-pasted into both `connectionCommands.ts`
 * and `explorerCommands.ts`; centralising it removes the duplication and
 * the redundant `await import(...)` for `pickConnectionWithPrompt`
 * (which was being lazy-loaded to avoid an apparent cycle that, on
 * inspection, did not exist).
 */

import type { ConnectionManager } from "../connectionManager";
import { pickConnectionWithPrompt } from "../connectionManagerPrompts";
import type { RapiDBNode } from "../providers/connectionProvider";

/**
 * Resolves the connection id to act on.
 *
 * @returns The connection id, or `undefined` when the caller should bail
 *          out (no node argument AND the user cancelled the prompt).
 */
export async function resolveConnectionId(
  node: RapiDBNode | undefined,
  connectionManager: ConnectionManager,
): Promise<string | undefined> {
  if (node?.connectionId) {
    return node.connectionId;
  }
  return pickConnectionWithPrompt(connectionManager);
}
