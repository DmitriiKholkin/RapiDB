import { isDataDbObjectKind } from "../../shared/dbObjectKinds";
import type {
  ConnectionConfig,
  SchemaSnapshotSchemaEntry,
  TableDetailRequest,
  TableDetailState,
} from "../connectionManagerModels";
import {
  type DriverEntityAvailability,
  type IDBDriver,
  resolveDriverTableSectionAvailability,
} from "../dbDrivers/types";
import { pMapWithLimit } from "../utils/concurrency";
import {
  logErrorWithContext,
  normalizeUnknownError,
} from "../utils/errorHandling";
import {
  getConfiguredDefaultDatabaseName,
  resolveDriverEntityManifest,
} from "./schemaHelpers";

/**
 * Result of loading a database scope.
 */
export interface DatabaseScopeLoadResult {
  database: {
    name: string;
    schemas: SchemaSnapshotSchemaEntry[];
  };
  loadedSchemas: SchemaSnapshotSchemaEntry[];
}

/**
 * Load mode for database scopes.
 */
export type DatabaseLoadMode = "baseline" | "expanded";

/**
 * Loads the connection root catalog (list of databases).
 *
 * @param driver - The database driver
 * @param config - The connection configuration
 * @returns The catalog snapshot and default database name
 */
export async function loadConnectionRootCatalog(
  driver: IDBDriver,
  config: ConnectionConfig,
): Promise<{
  catalogSnapshot: { databases: Array<{ name: string; schemas: never[] }> };
  defaultDatabaseName: string;
}> {
  const configuredDb = getConfiguredDefaultDatabaseName(config);
  const allDbs = await driver.listDatabases().catch(() => []);
  const databaseNames = [
    ...new Set(
      [configuredDb, ...allDbs.map((database) => database.name)].filter(
        (name): name is string => typeof name === "string" && name.length > 0,
      ),
    ),
  ];

  return {
    catalogSnapshot: {
      databases: databaseNames.map((databaseName) => ({
        name: databaseName,
        schemas: [],
      })),
    },
    defaultDatabaseName: configuredDb || databaseNames[0] || "",
  };
}

/**
 * Loads a database scope (schemas within a database).
 *
 * @param driver - The database driver
 * @param databaseName - The database name
 * @param loadMode - The load mode (baseline or expanded)
 * @returns The database scope load result
 */
export async function loadDatabaseScope(
  driver: IDBDriver,
  databaseName: string,
  loadMode: DatabaseLoadMode,
): Promise<DatabaseScopeLoadResult> {
  const schemas = await driver.listSchemas(databaseName).catch(() => []);
  const schemaNames = [
    ...new Set(
      (schemas.length > 0 ? schemas : [{ name: databaseName }])
        .map((schema) => schema.name)
        .filter(
          (name): name is string => typeof name === "string" && name.length > 0,
        ),
    ),
  ];

  if (schemaNames.length <= 1) {
    const schema = await loadSchemaScope(
      driver,
      databaseName,
      schemaNames[0] ?? databaseName,
    );

    return {
      database: {
        name: databaseName,
        schemas: [schema],
      },
      loadedSchemas: [schema],
    };
  }

  if (loadMode === "expanded") {
    return {
      database: {
        name: databaseName,
        schemas: schemaNames.map((schemaName) => ({
          name: schemaName,
          objects: [],
        })),
      },
      loadedSchemas: [],
    };
  }

  const loadedSchemas = await pMapWithLimit(
    schemaNames,
    4,
    async (schemaName) => loadSchemaScope(driver, databaseName, schemaName),
  );

  return {
    database: {
      name: databaseName,
      schemas: loadedSchemas,
    },
    loadedSchemas,
  };
}

/**
 * Loads a schema scope (objects within a schema).
 *
 * @param driver - The database driver
 * @param databaseName - The database name
 * @param schemaName - The schema name
 * @returns The schema snapshot entry
 */
export async function loadSchemaScope(
  driver: IDBDriver,
  databaseName: string,
  schemaName: string,
): Promise<SchemaSnapshotSchemaEntry> {
  const manifest = resolveDriverEntityManifest(driver);
  const supportedKinds = new Set(manifest.dbObjectKinds);
  const objects = await driver
    .listObjects(databaseName, schemaName)
    .catch(() => []);
  const objectsForSchema = objects.filter((object) =>
    supportedKinds.has(object.type),
  );
  const describedColumns = await pMapWithLimit(
    objectsForSchema,
    10,
    async (object) => {
      if (!isDataDbObjectKind(object.type)) {
        return [];
      }

      try {
        return await driver.describeTable(
          databaseName,
          schemaName,
          object.name,
        );
      } catch {
        return [];
      }
    },
  );

  return {
    name: schemaName,
    objects: objectsForSchema.map((object, index) => ({
      name: object.name,
      type: object.type,
      ...(object.routineIdentity
        ? { routineIdentity: object.routineIdentity }
        : {}),
      columns: isDataDbObjectKind(object.type)
        ? describedColumns[index].map((column) => ({
            name: column.name,
            type: column.type,
          }))
        : [],
    })),
  };
}

/**
 * Loads table detail information (columns, constraints, indexes, triggers).
 *
 * @param driver - The database driver
 * @param request - The table detail request
 * @returns The table detail state
 */
export async function loadTableDetail(
  driver: IDBDriver,
  request: TableDetailRequest,
): Promise<
  Pick<TableDetailState, "snapshot" | "status" | "isPartial" | "error">
> {
  const manifest = resolveDriverEntityManifest(driver);

  const loadSection = async <T>(
    availability: DriverEntityAvailability,
    loader: () => Promise<T[] | null>,
  ): Promise<{
    status: "idle" | "loading" | "loaded" | "error";
    items: T[];
    error?: string;
  }> => {
    if (availability === "not_applicable") {
      return {
        status: "loaded",
        items: [],
      };
    }

    try {
      const items = await loader();
      if (items === null) {
        return {
          status: "loaded",
          items: [],
        };
      }
      return {
        status: "loaded",
        items,
      };
    } catch (error: unknown) {
      return {
        status: "error",
        items: [],
        error: normalizeUnknownError(error).message,
      };
    }
  };

  const [columns, constraints, indexes, triggers] = await Promise.all([
    loadSection(
      resolveDriverTableSectionAvailability(
        manifest,
        request.objectKind,
        "columns",
      ),
      async () =>
        driver.describeColumns(request.database, request.schema, request.table),
    ),
    loadSection(
      resolveDriverTableSectionAvailability(
        manifest,
        request.objectKind,
        "constraints",
      ),
      async () =>
        driver.getConstraints(request.database, request.schema, request.table),
    ),
    loadSection(
      resolveDriverTableSectionAvailability(
        manifest,
        request.objectKind,
        "indexes",
      ),
      async () =>
        driver.getIndexes(request.database, request.schema, request.table),
    ),
    loadSection(
      resolveDriverTableSectionAvailability(
        manifest,
        request.objectKind,
        "triggers",
      ),
      async () =>
        driver.getTriggers(request.database, request.schema, request.table),
    ),
  ]);

  const snapshot = {
    columns,
    constraints,
    indexes,
    triggers,
  };

  const sectionStates = [
    snapshot.columns,
    snapshot.constraints,
    snapshot.indexes,
    snapshot.triggers,
  ];
  const allErrored = sectionStates.every((state) => state.status === "error");
  const hasError = sectionStates.some((state) => state.status === "error");

  return {
    snapshot,
    status: allErrored ? "error" : "loaded",
    isPartial: hasError,
    error: allErrored
      ? sectionStates.find((state) => state.error)?.error
      : undefined,
  };
}
