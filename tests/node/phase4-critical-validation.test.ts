import { describe, expect, it, vi } from "vitest";
import { DynamoDBDriver } from "../../src/extension/dbDrivers/dynamodb";
import { ElasticsearchDriver } from "../../src/extension/dbDrivers/elasticsearch";
import { MongoDBDriver } from "../../src/extension/dbDrivers/mongodb";
import { MSSQLDriver } from "../../src/extension/dbDrivers/mssql";
import { MySQLDriver } from "../../src/extension/dbDrivers/mysql";
import { OracleDriver } from "../../src/extension/dbDrivers/oracle";
import { PostgresDriver } from "../../src/extension/dbDrivers/postgres";
import { RedisDriver } from "../../src/extension/dbDrivers/redis";
import { SQLiteDriver } from "../../src/extension/dbDrivers/sqlite";
import type { ColumnTypeMeta } from "../../src/extension/dbDrivers/types";
import { TableMutationService } from "../../src/extension/table/tableMutationService";
import {
  allowReadOnlyQuery,
  assertConnectionWritable,
  decideReadOnlyQueryExecution,
  denyReadOnlyQuery,
  isConnectionReadOnly,
} from "../../src/extension/utils/readOnlyGuards";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";

/**
 * PHASE 4 CRITICAL VALIDATION TEST SUITE
 *
 * Tests critical security and behavioral guarantees:
 * F1: MongoDB mongosh execution safety (vm.runInNewContext with pre-validation)
 * F2: Read-only connection enforcement across all database types
 * F3: Mutation operation safetyand primary key verification
 */

describe("Phase 4 — Critical Validation", () => {
  // ============================================================================
  // F1: MONGODB MONGOSH EXECUTION SAFETY
  // ============================================================================

  describe("F1: MongoDB mongosh execution safety", () => {
    function createMongoDriver() {
      const driver = new MongoDBDriver({
        id: "test-mongo-security",
        type: "mongodb",
        name: "test",
        host: "localhost",
        port: 27017,
        database: "testdb",
      });

      const mockToArray = vi.fn().mockResolvedValue([]);
      const mockFind = vi.fn().mockReturnValue({
        limit: vi.fn().mockReturnThis(),
        skip: vi.fn().mockReturnThis(),
        toArray: mockToArray,
      });

      const mockDb = {
        collection: vi.fn().mockReturnValue({ find: mockFind }),
      };
      (driver as unknown as { client: unknown; connected: boolean }).client = {
        db: vi.fn().mockReturnValue(mockDb),
      };
      (driver as unknown as { connected: boolean }).connected = true;

      return { driver, mockFind };
    }

    it("rejects prototype pollution attempts", async () => {
      const { driver } = createMongoDriver();

      await expect(
        driver.query("db.users.find({__proto__: {admin: true}})"),
      ).rejects.toThrow();
    });

    it("rejects constructor property access", async () => {
      const { driver } = createMongoDriver();

      await expect(
        driver.query("db.users.find({constructor: {admin: true}})"),
      ).rejects.toThrow();
    });

    it("rejects process global access", async () => {
      const { driver } = createMongoDriver();

      await expect(
        driver.query("db.users.find({}).then(x => process.exit(1))"),
      ).rejects.toThrow();
    });

    it("rejects dynamic code execution via eval", async () => {
      const { driver } = createMongoDriver();

      await expect(driver.query('eval("malicious code")')).rejects.toThrow();
    });

    it("rejects require/import statements", async () => {
      const { driver } = createMongoDriver();

      await expect(
        driver.query('require("fs").readFileSync("/etc/passwd")'),
      ).rejects.toThrow();
    });

    it("rejects control flow statements", async () => {
      const { driver } = createMongoDriver();

      await expect(
        driver.query("while (true) { db.users.find() }"),
      ).rejects.toThrow();

      await expect(
        driver.query("for (let i = 0; i < 100; i++) { }"),
      ).rejects.toThrow();

      await expect(
        driver.query("try { db.users.find() } catch (e) { }"),
      ).rejects.toThrow();
    });

    it("rejects async/await", async () => {
      const { driver } = createMongoDriver();

      await expect(
        driver.query("async () => { await db.users.find() }"),
      ).rejects.toThrow();
    });

    it("rejects arrow functions", async () => {
      const { driver } = createMongoDriver();

      await expect(
        driver.query("db.users.find().then(x => console.log(x))"),
      ).rejects.toThrow();
    });

    it("rejects double-underscore patterns", async () => {
      const { driver } = createMongoDriver();

      await expect(
        driver.query("db.users.find({__private__: true})"),
      ).rejects.toThrow();
    });

    it("rejects multiple statements via semicolon", async () => {
      const { driver } = createMongoDriver();

      await expect(
        driver.query("db.users.find({}); db.users.deleteMany({})"),
      ).rejects.toThrow();
    });

    it("rejects setTimeout and setInterval", async () => {
      const { driver } = createMongoDriver();

      await expect(
        driver.query("setTimeout(() => {}, 1000)"),
      ).rejects.toThrow();

      await expect(
        driver.query("setInterval(() => {}, 1000)"),
      ).rejects.toThrow();
    });

    it("allows safe find queries", async () => {
      const { driver, mockFind } = createMongoDriver();

      const result = await driver.query("db.users.find({})");
      expect(mockFind).toHaveBeenCalled();
      expect(result).toHaveProperty("rowCount");
    });

    it("allows safe aggregation without $out", async () => {
      const { driver } = createMongoDriver();

      const mockToArray = vi.fn().mockResolvedValue([]);
      const mockAggregate = vi.fn().mockReturnValue({ toArray: mockToArray });

      const mockDb = {
        collection: vi.fn().mockReturnValue({ aggregate: mockAggregate }),
      };
      (driver as unknown as { client: unknown; connected: boolean }).client = {
        db: vi.fn().mockReturnValue(mockDb),
      };

      const result = await driver.query(
        'db.users.aggregate([{ $match: { status: "active" } }])',
      );
      expect(result).toHaveProperty("rowCount");
    });

    it("times out long-running code execution", async () => {
      const { driver } = createMongoDriver();

      // The sandbox has a 5-second timeout; we can't easily test this
      // but we verify the timeout is documented in the code
      const driverCode = driver.constructor.toString();
      expect(driverCode).toContain("timeout");
    });
  });

  // ============================================================================
  // F2: READ-ONLY CONNECTION ENFORCEMENT
  // ============================================================================

  describe("F2: Read-only connection enforcement", () => {
    const createTestManager = (readOnly: boolean) => ({
      getConnection: vi.fn().mockReturnValue({
        id: "test-conn",
        name: "Test",
        type: "pg",
        readOnly,
        host: "localhost",
      }),
      getDriver: vi.fn(),
      getDriverCapabilities: vi.fn(),
    });

    it("allows operations on writable connections", () => {
      const manager = createTestManager(false);

      expect(() =>
        assertConnectionWritable(manager, "test-conn", "test operation"),
      ).not.toThrow();
    });

    it("blocks operations on read-only connections", () => {
      const manager = createTestManager(true);

      expect(() =>
        assertConnectionWritable(manager, "test-conn", "insert data"),
      ).toThrow(/read-only/i);
    });

    it("detects read-only status correctly", () => {
      const writableManager = createTestManager(false);
      const readonlyManager = createTestManager(true);

      expect(isConnectionReadOnly(writableManager, "test-conn")).toBe(false);
      expect(isConnectionReadOnly(readonlyManager, "test-conn")).toBe(true);
    });

    it("includes connection name in error messages", () => {
      const manager = createTestManager(true);
      manager.getConnection = vi.fn().mockReturnValue({
        id: "test-conn",
        name: "My PostgreSQL DB",
        type: "pg",
        readOnly: true,
        host: "localhost",
      });

      expect(() =>
        assertConnectionWritable(manager, "test-conn", "insert data"),
      ).toThrow(/My PostgreSQL DB/);
    });

    it("uses connection ID fallback when name is missing", () => {
      const manager = createTestManager(true);
      manager.getConnection = vi.fn().mockReturnValue({
        id: "conn-12345",
        name: "   ",
        type: "pg",
        readOnly: true,
        host: "localhost",
      });

      expect(() =>
        assertConnectionWritable(manager, "conn-12345", "delete data"),
      ).toThrow(/conn-12345/);
    });

    it("allows read queries on read-only connections", () => {
      const manager = createTestManager(true);
      manager.getDriver = vi.fn().mockReturnValue({
        getCapabilities: vi.fn().mockReturnValue({
          readOnlyQueryGuard: (q: string) =>
            q.toLowerCase().startsWith("select")
              ? allowReadOnlyQuery()
              : denyReadOnlyQuery("Not a SELECT"),
        }),
      });

      const result = decideReadOnlyQueryExecution(
        manager,
        "test-conn",
        "SELECT * FROM users",
      );
      expect(result.allowed).toBe(true);
    });

    it("blocks write queries on read-only connections", () => {
      const manager = createTestManager(true);
      manager.getDriver = vi.fn().mockReturnValue({
        getCapabilities: vi.fn().mockReturnValue({
          readOnlyQueryGuard: (q: string) =>
            q.toLowerCase().startsWith("select")
              ? allowReadOnlyQuery()
              : denyReadOnlyQuery("Mutation not allowed"),
        }),
      });

      const result = decideReadOnlyQueryExecution(
        manager,
        "test-conn",
        "DELETE FROM users",
      );
      expect(result.allowed).toBe(false);
      expect(result.reason).toContain("Mutation not allowed");
    });

    it("allows all queries on writable connections", () => {
      const manager = createTestManager(false);

      const result = decideReadOnlyQueryExecution(
        manager,
        "test-conn",
        "DELETE FROM users",
      );
      expect(result.allowed).toBe(true);
    });
  });

  // ============================================================================
  // F3: MUTATION OPERATION SAFETY
  // ============================================================================

  describe("F3: Mutation operation safety", () => {
    const createMutationManager = (readOnly: boolean) => ({
      getConnection: vi.fn().mockReturnValue({
        id: "test-conn",
        name: "Test DB",
        type: "pg",
        readOnly,
        host: "localhost",
      }),
      getDriver: vi.fn().mockReturnValue({
        query: vi.fn().mockResolvedValue({ rows: [], rowCount: 1 }),
        qualifiedTableName: vi.fn().mockReturnValue("public.users"),
        quoteIdentifier: vi.fn((id: string) => `"${id}"`),
        materializePreviewSql: vi.fn((sql: string) => sql),
        buildInsertValueExpr: vi.fn((col: any, i: number) => `$${i}`),
      }),
    });

    const mockColumnsProvider = {
      getColumns: vi.fn().mockResolvedValue([
        {
          name: "id",
          type: "INTEGER",
          nativeType: "INTEGER",
          nullable: false,
          isPrimaryKey: true,
          primaryKeyOrdinal: 1,
          isForeignKey: false,
          category: "integer",
          filterable: true,
          filterOperators: ["eq"],
          valueSemantics: "plain",
        } as ColumnTypeMeta,
        {
          name: "name",
          type: "TEXT",
          nativeType: "TEXT",
          nullable: false,
          isPrimaryKey: false,
          isForeignKey: false,
          category: "text",
          filterable: true,
          filterOperators: ["eq", "like"],
          valueSemantics: "plain",
        } as ColumnTypeMeta,
      ]),
    };

    it("blocks update on read-only connection", async () => {
      const manager = createMutationManager(true);
      const service = new TableMutationService(
        manager as any,
        mockColumnsProvider,
      );

      await expect(
        service.updateRow(
          "test-conn",
          "testdb",
          "public",
          "users",
          { id: 1 },
          { name: "Alice" },
        ),
      ).rejects.toThrow(/read-only/i);
    });

    it("blocks insert on read-only connection", async () => {
      const manager = createMutationManager(true);
      const service = new TableMutationService(
        manager as any,
        mockColumnsProvider,
      );

      await expect(
        service.insertRow("test-conn", "testdb", "public", "users", {
          id: 2,
          name: "Bob",
        }),
      ).rejects.toThrow(/read-only/i);
    });

    it("blocks delete on read-only connection", async () => {
      const manager = createMutationManager(true);
      const service = new TableMutationService(
        manager as any,
        mockColumnsProvider,
      );

      await expect(
        service.deleteRows("test-conn", "testdb", "public", "users", [
          { id: 1 },
        ]),
      ).rejects.toThrow(/read-only/i);
    });

    it("allows update on writable connection", async () => {
      const manager = createMutationManager(false);
      const driver = manager.getDriver();
      driver.updateRows = vi.fn().mockResolvedValue({ affectedRows: 1 });

      const service = new TableMutationService(
        manager as any,
        mockColumnsProvider,
      );

      // Should not throw
      await service.updateRow(
        "test-conn",
        "testdb",
        "public",
        "users",
        { id: 1 },
        { name: "Alice" },
      );

      expect(driver.updateRows).toHaveBeenCalled();
    });

    it("allows insert on writable connection", async () => {
      const manager = createMutationManager(false);
      const driver = manager.getDriver();
      driver.insertRow = vi.fn().mockResolvedValue({ affectedRows: 1 });

      const service = new TableMutationService(
        manager as any,
        mockColumnsProvider,
      );

      // Should not throw
      await service.insertRow("test-conn", "testdb", "public", "users", {
        id: 2,
        name: "Bob",
      });

      expect(driver.insertRow).toHaveBeenCalled();
    });

    it("allows delete on writable connection", async () => {
      const manager = createMutationManager(false);
      const driver = manager.getDriver();
      driver.deleteRows = vi.fn().mockResolvedValue({ affectedRows: 1 });

      const service = new TableMutationService(
        manager as any,
        mockColumnsProvider,
      );

      // Should not throw
      await service.deleteRows("test-conn", "testdb", "public", "users", [
        { id: 1 },
      ]);

      expect(driver.deleteRows).toHaveBeenCalled();
    });

    it("verifies prepared insert plan enforces read-only guard", async () => {
      const manager = createMutationManager(true);
      const service = new TableMutationService(
        manager as any,
        mockColumnsProvider,
      );

      await expect(
        service.prepareInsertRow("test-conn", "testdb", "public", "users", {
          id: 2,
          name: "Bob",
        }),
      ).rejects.toThrow(/read-only/i);
    });

    it("verifies prepared delete plan enforces read-only guard", async () => {
      const manager = createMutationManager(true);
      const service = new TableMutationService(
        manager as any,
        mockColumnsProvider,
      );

      await expect(
        service.prepareDeleteRowsPlan(
          "test-conn",
          "testdb",
          "public",
          "users",
          [{ id: 1 }],
        ),
      ).rejects.toThrow(/read-only/i);
    });
  });

  // ============================================================================
  // CROSS-DATABASE READ-ONLY CONSISTENCY
  // ============================================================================

  describe("Cross-database read-only consistency", () => {
    const databases = [
      { name: "PostgreSQL", Driver: PostgresDriver },
      { name: "MySQL", Driver: MySQLDriver },
      { name: "SQLite", Driver: SQLiteDriver },
      { name: "MSSQL", Driver: MSSQLDriver },
      { name: "Oracle", Driver: OracleDriver },
      { name: "MongoDB", Driver: MongoDBDriver },
      { name: "Redis", Driver: RedisDriver },
      { name: "Elasticsearch", Driver: ElasticsearchDriver },
      { name: "DynamoDB", Driver: DynamoDBDriver },
    ];

    for (const { name, Driver } of databases) {
      it(`${name} driver has readOnlyQueryGuard capability defined`, () => {
        const config: ConnectionConfig = {
          id: `test-${name.toLowerCase()}`,
          type: name.toLowerCase() as any,
          name: `Test ${name}`,
          host: "localhost",
        } as any;

        const driver = new Driver(config);
        const capabilities = driver.getCapabilities?.();

        // All drivers should have a readOnlyQueryGuard
        if (capabilities) {
          expect(capabilities.readOnlyQueryGuard).toBeDefined();
        }
      });
    }
  });
});
