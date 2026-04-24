import { randomUUID } from "node:crypto";
import type {
  ApplyResultPayload,
  TableMutationPreviewPayload,
} from "../../shared/webviewContracts";
import type { ConnectionManager } from "../connectionManager";
import {
  executePreparedApplyPlan,
  type PreparedApplyPlan,
  type PreparedInsertPlan,
  type TableDataService,
} from "../tableDataService";
import { normalizeUnknownError } from "../utils/errorHandling";
import { formatMutationPreviewSql } from "../utils/mutationPreview";

type PendingTableMutationPreview =
  | {
      kind: "applyChanges";
      plan: PreparedApplyPlan;
    }
  | {
      kind: "insertRow";
      plan: PreparedInsertPlan;
    };

type MutationPreviewExecutionResult =
  | {
      type: "applyResult";
      payload: ApplyResultPayload;
    }
  | {
      type: "insertResult";
      payload: {
        success: boolean;
        error?: string;
      };
    };

interface TableMutationPreviewControllerOptions {
  connectionId: string;
  tableName: string;
  connectionManager: ConnectionManager;
  tableDataService: Pick<TableDataService, "executePreparedInsertPlan">;
  notifyWarning: (message: string) => void;
}

export class TableMutationPreviewController {
  private readonly pendingMutationPreviews = new Map<
    string,
    PendingTableMutationPreview
  >();

  private readonly connectionId: string;
  private readonly tableName: string;
  private readonly connectionManager: ConnectionManager;
  private readonly tableDataService: Pick<
    TableDataService,
    "executePreparedInsertPlan"
  >;
  private readonly notifyWarning: (message: string) => void;

  constructor(options: TableMutationPreviewControllerOptions) {
    this.connectionId = options.connectionId;
    this.tableName = options.tableName;
    this.connectionManager = options.connectionManager;
    this.tableDataService = options.tableDataService;
    this.notifyWarning = options.notifyWarning;
  }

  clear(): void {
    this.pendingMutationPreviews.clear();
  }

  createApplyChangesPreview(
    plan: PreparedApplyPlan,
  ): TableMutationPreviewPayload {
    return this.storePreview({ kind: "applyChanges", plan });
  }

  createInsertPreview(plan: PreparedInsertPlan): TableMutationPreviewPayload {
    return this.storePreview({ kind: "insertRow", plan });
  }

  async confirm(
    previewToken: string,
  ): Promise<MutationPreviewExecutionResult | null> {
    const preview = this.pendingMutationPreviews.get(previewToken);
    if (!preview) {
      return null;
    }

    this.pendingMutationPreviews.delete(previewToken);

    if (preview.kind === "applyChanges") {
      const result: ApplyResultPayload = await executePreparedApplyPlan(
        this.connectionManager,
        preview.plan,
      );

      if (result.warning) {
        this.notifyWarning(result.warning);
      }

      return {
        type: "applyResult",
        payload: result,
      };
    }

    try {
      await this.tableDataService.executePreparedInsertPlan(preview.plan);
      return {
        type: "insertResult",
        payload: { success: true },
      };
    } catch (error: unknown) {
      const normalized = normalizeUnknownError(error);
      return {
        type: "insertResult",
        payload: {
          success: false,
          error: normalized.message,
        },
      };
    }
  }

  cancel(previewToken: string): void {
    this.pendingMutationPreviews.delete(previewToken);
  }

  private storePreview(
    preview: PendingTableMutationPreview,
  ): TableMutationPreviewPayload {
    const previewToken = randomUUID();
    this.pendingMutationPreviews.set(previewToken, preview);
    return this.buildPreviewPayload(previewToken, preview);
  }

  private buildPreviewPayload(
    previewToken: string,
    preview: PendingTableMutationPreview,
  ): TableMutationPreviewPayload {
    const connectionType = this.connectionManager.getConnection(
      this.connectionId,
    )?.type;
    const previewStatements = preview.plan.previewStatements;
    const title =
      preview.kind === "applyChanges"
        ? `Apply changes to ${this.tableName}`
        : `Insert row into ${this.tableName}`;

    return {
      previewToken,
      kind: preview.kind,
      title,
      sql: formatMutationPreviewSql(previewStatements, connectionType),
      statementCount: previewStatements.length,
    };
  }
}
