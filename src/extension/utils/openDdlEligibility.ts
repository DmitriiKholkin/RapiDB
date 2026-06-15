/**
 * Decide whether a given tree-view node kind supports a "show DDL" action.
 *
 * Two layers of gating:
 *  1. **Driver manifest** — the per-driver entity manifest declares which
 *     object kinds it can produce DDL for.
 *  2. **Per-connection overrides** — some drivers (DynamoDB, Redis,
 *     Elasticsearch) report support for a kind in their manifest but
 *     cannot actually emit meaningful DDL for it; the override table
 *     forces a no-DDL state.
 *
 * The function `composeOpenDdlAwareContextValue` is used to build the
 * `contextValue` string VSCode sees — adding a `_noDdl` suffix that
 * the `package.json` menus key on to hide the command.
 */

import type { ConnectionType } from "../../shared/connectionTypes";
import type { DbObjectKind } from "../../shared/dbObjectKinds";
import type { IndexDdlSupport } from "../../shared/tableTypes";
import type { DriverEntityManifest } from "../dbDrivers/types";

export type OpenDdlNodeKind =
  | DbObjectKind
  | "table_detail_constraint"
  | "table_detail_index"
  | "table_detail_trigger";

export interface OpenDdlSupportHints {
  indexDdlSupport?: IndexDdlSupport;
}

const OPEN_DDL_CONTEXT_VALUE_UNSUPPORTED_SUFFIX = "_noDdl";

/** Per-connection overrides that demote otherwise-supported kinds. */
const OPEN_DDL_UNSUPPORTED_BY_CONNECTION_TYPE: Readonly<
  Partial<Record<ConnectionType, readonly OpenDdlNodeKind[]>>
> = {
  dynamodb: ["table", "table_detail_index"],
  elasticsearch: ["table_detail_index"],
  redis: ["table"],
};

const OPEN_DDL_NODE_KINDS: ReadonlySet<OpenDdlNodeKind> =
  new Set<OpenDdlNodeKind>([
    "table",
    "view",
    "materializedView",
    "function",
    "procedure",
    "sequence",
    "type",
    "table_detail_constraint",
    "table_detail_index",
    "table_detail_trigger",
  ]);

export function isOpenDdlNodeKind(kind: string): kind is OpenDdlNodeKind {
  return OPEN_DDL_NODE_KINDS.has(kind as OpenDdlNodeKind);
}

function isSupportedByManifest(
  kind: OpenDdlNodeKind,
  manifest: DriverEntityManifest,
): boolean {
  if (kind === "table_detail_constraint") {
    return manifest.tableSections.constraints === "supported";
  }

  if (kind === "table_detail_index") {
    return manifest.tableSections.indexes === "supported";
  }

  if (kind === "table_detail_trigger") {
    return manifest.tableSections.triggers === "supported";
  }

  return manifest.dbObjectKinds.includes(kind);
}

function isOverriddenAsUnsupported(
  kind: OpenDdlNodeKind,
  connectionType: ConnectionType | undefined,
  hints: OpenDdlSupportHints | undefined,
): boolean {
  if (
    connectionType &&
    (OPEN_DDL_UNSUPPORTED_BY_CONNECTION_TYPE[connectionType]?.includes(kind) ??
      false)
  ) {
    return true;
  }

  if (kind === "table_detail_index") {
    if (hints?.indexDdlSupport === "supported") {
      return false;
    }

    if (hints?.indexDdlSupport === "unsupported") {
      return true;
    }
  }

  return false;
}

export function isOpenDdlSupportedForNode(
  kind: string,
  connectionType: ConnectionType | undefined,
  manifest: DriverEntityManifest,
  hints?: OpenDdlSupportHints,
): boolean {
  if (!isOpenDdlNodeKind(kind)) {
    return false;
  }

  return (
    isSupportedByManifest(kind, manifest) &&
    !isOverriddenAsUnsupported(kind, connectionType, hints)
  );
}

export function composeOpenDdlAwareContextValue(
  kind: string,
  connectionType: ConnectionType | undefined,
  manifest: DriverEntityManifest,
  hints?: OpenDdlSupportHints,
): string {
  if (!isOpenDdlNodeKind(kind)) {
    return kind;
  }

  if (isOpenDdlSupportedForNode(kind, connectionType, manifest, hints)) {
    return kind;
  }

  return `${kind}${OPEN_DDL_CONTEXT_VALUE_UNSUPPORTED_SUFFIX}`;
}
