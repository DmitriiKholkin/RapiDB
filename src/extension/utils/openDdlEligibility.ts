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

const OPEN_DDL_UNSUPPORTED_BY_CONNECTION_TYPE: Readonly<
  Partial<Record<ConnectionType, readonly OpenDdlNodeKind[]>>
> = {
  dynamodb: ["table", "table_detail_index"],
  elasticsearch: ["table_detail_index"],
  redis: ["table"],
};

export function isOpenDdlNodeKind(kind: string): kind is OpenDdlNodeKind {
  return (
    kind === "table" ||
    kind === "view" ||
    kind === "materializedView" ||
    kind === "function" ||
    kind === "procedure" ||
    kind === "sequence" ||
    kind === "type" ||
    kind === "table_detail_constraint" ||
    kind === "table_detail_index" ||
    kind === "table_detail_trigger"
  );
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
  connectionType?: ConnectionType,
  hints?: OpenDdlSupportHints,
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
