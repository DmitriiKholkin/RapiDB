import { randomUUID } from "node:crypto";
import type {
  ApplyResultPayload,
  DeleteResultPayload,
  TableMutationPreviewPayload,
} from "../../shared/webviewContracts";
import type { ConnectionManager } from "../connectionManager";
import {
  buildDeleteResult,
  DeleteExecutionError,
  getDeleteEvidence,
} from "../dbDrivers/deleteOutcomes";
import {
  executeAtomicSqlApplyPlan,
  executePreparedApplyPlan,
  type PreparedApplyPlan,
  type PreparedDeletePlan,
  type PreparedInsertPlan,
  type TableDataService,
} from "../tableDataService";
import { normalizeUnknownError } from "../utils/errorHandling";
import { formatMutationPreviewSql } from "../utils/mutationPreview";

const MAX_PENDING_MUTATION_PREVIEWS = 50;

function resolvePreviewContentType(
  editorPresentation:
    | import("../../shared/webviewContracts").QueryEditorPresentation
    | undefined,
): "application/sql" | "application/json" | "text/plain" {
  switch (editorPresentation?.editorLanguage) {
    case "json":
      return "application/json";
    case "sql":
      return "application/sql";
    default:
      return "text/plain";
  }
}

type PendingTableMutationPreview =
  | {
      operationId: string;
      kind: "applyChanges";
      plan: {
        apply: PreparedApplyPlan | null;
        applyResultWhenEmpty: ApplyResultPayload | null;
        inserts: PreparedInsertPlan[];
      };
    }
  | {
      operationId: string;
      kind: "insertRow";
      plan: PreparedInsertPlan;
    }
  | {
      operationId: string;
      kind: "deleteRows";
      plan: PreparedDeletePlan;
    };

type MutationPreviewExecutionResult =
  | { type: "deleteResult"; payload: DeleteResultPayload }
  | {
      type: "applyResult";
      payload: ApplyResultPayload;
    }
  | {
      type: "insertResult";
      payload: {
        operationId: string;
        success: boolean;
        error?: string;
      };
    };

interface TableMutationPreviewControllerOptions {
  connectionId: string;
  tableName: string;
  connectionManager: ConnectionManager;
  tableDataService: Pick<
    TableDataService,
    "executePreparedInsertPlan" | "executePreparedDeletePlan"
  >;
  notifyWarning: (message: string) => void;
}

export class TableMutationPreviewController {
  private readonly executedOperationIds = new Set<string>();
  private readonly pendingMutationPreviews = new Map<
    string,
    PendingTableMutationPreview
  >();

  private readonly connectionId: string;
  private readonly tableName: string;
  private readonly connectionManager: ConnectionManager;
  private readonly tableDataService: Pick<
    TableDataService,
    "executePreparedInsertPlan" | "executePreparedDeletePlan"
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
    operationId: string,
    plan: {
      apply: PreparedApplyPlan | null;
      applyResultWhenEmpty: ApplyResultPayload | null;
      inserts: PreparedInsertPlan[];
    },
  ): TableMutationPreviewPayload {
    return this.storePreview({ operationId, kind: "applyChanges", plan });
  }

  createInsertPreview(
    operationId: string,
    plan: PreparedInsertPlan,
  ): TableMutationPreviewPayload {
    return this.storePreview({ operationId, kind: "insertRow", plan });
  }

  createDeleteRowsPreview(
    operationId: string,
    plan: PreparedDeletePlan,
  ): TableMutationPreviewPayload {
    return this.storePreview({ operationId, kind: "deleteRows", plan });
  }

  async confirm(
    previewToken: string,
    operationId?: string,
  ): Promise<MutationPreviewExecutionResult | null> {
    const preview = this.pendingMutationPreviews.get(previewToken);
    if (!preview || (operationId && preview.operationId !== operationId)) {
      return null;
    }

    this.pendingMutationPreviews.delete(previewToken);
    if (preview.kind === "deleteRows") {
      if (this.executedOperationIds.has(preview.operationId)) return null;
      this.executedOperationIds.add(preview.operationId);
    }

    if (preview.kind === "applyChanges") {
      const operationCount =
        preview.plan.inserts.length +
        ((preview.plan.apply?.updates.length ?? 0) -
          (preview.plan.apply?.skippedRows.length ?? 0));
      if (operationCount > 1) {
        const payload = await executeAtomicSqlApplyPlan(
          this.connectionManager,
          preview.plan.apply,
          preview.plan.inserts,
        );
        return {
          type: "applyResult",
          payload: { ...payload, operationId: preview.operationId },
        };
      }
      let insertApplied = false;
      let succeededCount = 0;
      const errors: string[] = [];

      for (const insertPlan of preview.plan.inserts) {
        try {
          await this.tableDataService.executePreparedInsertPlan(insertPlan);
          succeededCount++;
          insertApplied = true;
        } catch (error: unknown) {
          const normalized = normalizeUnknownError(error);
          errors.push(normalized.message);
        }
      }

      if (errors.length > 0 && succeededCount === 0) {
        return {
          type: "applyResult",
          payload: {
            operationId: preview.operationId,
            success: false,
            error: `All inserts failed: ${errors.join("; ")}`,
          },
        };
      }

      if (errors.length > 0) {
        return {
          type: "applyResult",
          payload: {
            operationId: preview.operationId,
            success: false,
            insertApplied: true,
            error: `${succeededCount} row(s) inserted, ${errors.length} failed: ${errors.join("; ")}`,
          },
        };
      }

      const result: ApplyResultPayload = preview.plan.apply
        ? await executePreparedApplyPlan(
            this.connectionManager,
            preview.plan.apply,
          )
        : (preview.plan.applyResultWhenEmpty ?? {
            success: true,
            rowOutcomes: [],
          });

      const payload: ApplyResultPayload = insertApplied
        ? { ...result, operationId: preview.operationId, insertApplied: true }
        : { ...result, operationId: preview.operationId };

      if (payload.warning) {
        this.notifyWarning(payload.warning);
      }

      return {
        type: "applyResult",
        payload,
      };
    }

    if (preview.kind === "deleteRows") {
      try {
        await this.tableDataService.executePreparedDeletePlan(preview.plan);
        const identities =
          preview.plan.rowIdentities ?? preview.plan.verificationCriteriaList;
        return {
          type: "deleteResult",
          payload: {
            ...buildDeleteResult(identities, {
              affectedRows: identities.length,
            }),
            operationId: preview.operationId,
          },
        };
      } catch (error) {
        return {
          type: "deleteResult",
          payload: {
            ...(error instanceof DeleteExecutionError
              ? error.deleteResult
              : buildDeleteResult(
                  preview.plan.rowIdentities ??
                    preview.plan.verificationCriteriaList,
                  getDeleteEvidence(error) ?? { affectedRows: 0 },
                  normalizeUnknownError(error).message,
                )),
            operationId: preview.operationId,
          },
        };
      }
    }

    try {
      await this.tableDataService.executePreparedInsertPlan(preview.plan);

      return {
        type: "insertResult",
        payload: { operationId: preview.operationId, success: true },
      };
    } catch (error: unknown) {
      const normalized = normalizeUnknownError(error);
      return {
        type: "insertResult",
        payload: {
          operationId: preview.operationId,
          success: false,
          error: normalized.message,
        },
      };
    }
  }

  cancel(previewToken: string, operationId?: string): void {
    const preview = this.pendingMutationPreviews.get(previewToken);
    if (preview && (!operationId || preview.operationId === operationId)) {
      this.pendingMutationPreviews.delete(previewToken);
    }
  }

  private storePreview(
    preview: PendingTableMutationPreview,
  ): TableMutationPreviewPayload {
    const previewToken = randomUUID();
    if (this.pendingMutationPreviews.size >= MAX_PENDING_MUTATION_PREVIEWS) {
      const oldestToken = this.pendingMutationPreviews.keys().next().value;
      if (oldestToken !== undefined) {
        this.pendingMutationPreviews.delete(oldestToken);
      }
    }
    this.pendingMutationPreviews.set(previewToken, preview);
    return this.buildPreviewPayload(previewToken, preview);
  }

  private buildPreviewPayload(
    previewToken: string,
    preview: PendingTableMutationPreview,
  ): TableMutationPreviewPayload {
    const editorPresentation =
      this.connectionManager.getQueryEditorPresentation(this.connectionId);
    const previewStatements =
      preview.kind === "applyChanges"
        ? [
            ...preview.plan.inserts.flatMap((p) => p.previewStatements),
            ...(preview.plan.apply?.previewStatements ?? []),
          ]
        : preview.plan.previewStatements;
    const title =
      preview.kind === "applyChanges"
        ? `Apply changes to ${this.tableName}`
        : preview.kind === "insertRow"
          ? `Insert row into ${this.tableName}`
          : `Apply changes to ${this.tableName}`;
    const text = formatMutationPreviewSql(
      previewStatements,
      editorPresentation,
    );
    const contentType = resolvePreviewContentType(editorPresentation);

    return {
      operationId: preview.operationId,
      previewToken,
      kind: preview.kind,
      title,
      text,
      contentType,
      sql: text,
      statementCount: previewStatements.length,
    };
  }
}
