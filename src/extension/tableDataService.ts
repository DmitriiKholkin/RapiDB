import type { ConnectionManager } from "./connectionManager";
import type { ColumnTypeMeta, FilterExpression } from "./dbDrivers/types";
import type {
  PreparedInsertPlan,
  SortConfig,
  TablePage,
} from "./table/tableDataContracts";
import { TableMutationService } from "./table/tableMutationService";
import { TableReadService } from "./table/tableReadService";

export { formatDatetimeForDisplay } from "./dbDrivers/BaseDBDriver";

export type {
  ApplyResult,
  PreparedApplyPlan,
  PreparedApplyPlanResult,
  PreparedInsertPlan,
  RowUpdate,
  SortConfig,
  TablePage,
} from "./table/tableDataContracts";
export {
  /** @deprecated Use prepareApplyChangesPlan + executePreparedApplyPlan for preview-first flow. */
  applyChangesTransactional,
  executePreparedApplyPlan,
  prepareApplyChangesPlan,
} from "./table/tableMutationExecution";

export class TableDataService {
  private readonly readService: TableReadService;
  private readonly mutationService: TableMutationService;

  constructor(connectionManager: ConnectionManager) {
    this.readService = new TableReadService(connectionManager);
    this.mutationService = new TableMutationService(
      connectionManager,
      this.readService,
    );
  }

  clearForConnection(connectionId: string): void {
    this.readService.clearForConnection(connectionId);
  }

  getColumns(
    connectionId: string,
    database: string,
    schema: string,
    table: string,
  ): Promise<ColumnTypeMeta[]> {
    return this.readService.getColumns(connectionId, database, schema, table);
  }

  getPage(
    connectionId: string,
    database: string,
    schema: string,
    table: string,
    page: number,
    pageSize: number,
    filters: FilterExpression[],
    sort: SortConfig | null = null,
    skipCount = false,
  ): Promise<TablePage> {
    return this.readService.getPage(
      connectionId,
      database,
      schema,
      table,
      page,
      pageSize,
      filters,
      sort,
      skipCount,
    );
  }

  /** @deprecated Use prepareApplyChangesPlan + executePreparedApplyPlan for preview-first flow. */
  updateRow(
    connectionId: string,
    database: string,
    schema: string,
    table: string,
    primaryKeyValues: Record<string, unknown>,
    changes: Record<string, unknown>,
  ): Promise<void> {
    return this.mutationService.updateRow(
      connectionId,
      database,
      schema,
      table,
      primaryKeyValues,
      changes,
    );
  }

  /** @deprecated Use prepareInsertRow + executePreparedInsertPlan for preview-first flow. */
  insertRow(
    connectionId: string,
    database: string,
    schema: string,
    table: string,
    values: Record<string, unknown>,
  ): Promise<void> {
    return this.mutationService.insertRow(
      connectionId,
      database,
      schema,
      table,
      values,
    );
  }

  prepareInsertRow(
    connectionId: string,
    database: string,
    schema: string,
    table: string,
    values: Record<string, unknown>,
  ): Promise<PreparedInsertPlan> {
    return this.mutationService.prepareInsertRow(
      connectionId,
      database,
      schema,
      table,
      values,
    );
  }

  executePreparedInsertPlan(plan: PreparedInsertPlan): Promise<void> {
    return this.mutationService.executePreparedInsertPlan(plan);
  }

  deleteRows(
    connectionId: string,
    database: string,
    schema: string,
    table: string,
    primaryKeyValuesList: Record<string, unknown>[],
  ): Promise<void> {
    return this.mutationService.deleteRows(
      connectionId,
      database,
      schema,
      table,
      primaryKeyValuesList,
    );
  }

  exportAll(
    connectionId: string,
    database: string,
    schema: string,
    table: string,
    chunkSize = 500,
    sort: SortConfig | null = null,
    filters: FilterExpression[] = [],
    signal?: AbortSignal,
  ): AsyncGenerator<{
    columns: ColumnTypeMeta[];
    rows: Record<string, unknown>[];
  }> {
    return this.readService.exportAll(
      connectionId,
      database,
      schema,
      table,
      chunkSize,
      sort,
      filters,
      signal,
    );
  }
}
