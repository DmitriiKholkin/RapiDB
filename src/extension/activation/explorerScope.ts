/**
 * Maps a tree-view `RapiDBNode` to a durable schema-scope descriptor.
 *
 * Why this lives in its own module:
 *  - the mapping is pure (no I/O, no `vscode` references)
 *  - it can be unit-tested without spinning up the extension host
 *  - it keeps `extension.ts` focused on lifecycle wiring
 */

import type { ExplorerSchemaScope } from "../connectionManager";
import type { RapiDBNode } from "../providers/connectionProvider";

export function getExplorerSchemaScopeForNode(
  node: RapiDBNode | undefined,
): ExplorerSchemaScope | undefined {
  if (!node?.connectionId) {
    return undefined;
  }

  if (
    node.kind === "connectionNode_connected" ||
    node.kind === "connectionNode_disconnected"
  ) {
    return { kind: "connectionRoot" };
  }

  if (node.kind === "database" && node.database) {
    return {
      kind: "database",
      database: node.database,
    };
  }

  if (node.kind === "schema" && node.database && node.schema) {
    return {
      kind: "schema",
      database: node.database,
      schema: node.schema,
    };
  }

  return undefined;
}

/** True when a node represents a connection root (connected or not). */
export function isConnectionRootNode(node: RapiDBNode | undefined): boolean {
  return (
    node?.kind === "connectionNode_connected" ||
    node?.kind === "connectionNode_disconnected"
  );
}
