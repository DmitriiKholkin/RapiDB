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
    node.kind === "connectionNode_connecting" ||
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

export function isConnectionRootNode(node: RapiDBNode | undefined): boolean {
  return (
    node?.kind === "connectionNode_connected" ||
    node?.kind === "connectionNode_connecting" ||
    node?.kind === "connectionNode_disconnected"
  );
}
