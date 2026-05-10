import { describe, expect, it, vi } from "vitest";
import { MongoDBDriver } from "../../src/extension/dbDrivers/mongodb";

function createMockDriver() {
  const driver = new MongoDBDriver({
    id: "test",
    type: "mongodb",
    name: "test",
    host: "localhost",
    port: 27017,
    database: "testdb",
  });

  const mockToArray = vi.fn().mockResolvedValue([{ _id: "abc", name: "Alice" }]);
  const mockFind = vi.fn().mockReturnValue({
    limit: vi.fn().mockReturnThis(),
    skip: vi.fn().mockReturnThis(),
    toArray: mockToArray,
  });
  const mockInsertOne = vi
    .fn()
    .mockResolvedValue({ acknowledged: true, insertedId: "newid" });
  const mockInsertMany = vi
    .fn()
    .mockResolvedValue({ acknowledged: true, insertedCount: 2 });
  const mockUpdateOne = vi
    .fn()
    .mockResolvedValue({ matchedCount: 1, modifiedCount: 1 });
  const mockUpdateMany = vi
    .fn()
    .mockResolvedValue({ matchedCount: 2, modifiedCount: 2 });
  const mockDeleteOne = vi.fn().mockResolvedValue({ deletedCount: 1 });
  const mockDeleteMany = vi.fn().mockResolvedValue({ deletedCount: 3 });
  const mockCountDocuments = vi.fn().mockResolvedValue(5);
  const mockAggregateToArray = vi.fn().mockResolvedValue([]);
  const mockAggregate = vi.fn().mockReturnValue({ toArray: mockAggregateToArray });
  const mockCommand = vi.fn().mockResolvedValue({ ok: 1 });
  const mockCreateCollection = vi.fn().mockResolvedValue({});

  const mockCollection = {
    find: mockFind,
    insertOne: mockInsertOne,
    insertMany: mockInsertMany,
    updateOne: mockUpdateOne,
    updateMany: mockUpdateMany,
    deleteOne: mockDeleteOne,
    deleteMany: mockDeleteMany,
    countDocuments: mockCountDocuments,
    aggregate: mockAggregate,
  };

  const mockDb = {
    collection: vi.fn().mockReturnValue(mockCollection),
    command: mockCommand,
    createCollection: mockCreateCollection,
  };

  (driver as unknown as { client: unknown; connected: boolean }).client = {
    db: vi.fn().mockReturnValue(mockDb),
  };
  (driver as unknown as { connected: boolean }).connected = true;

  return {
    driver,
    mockDb,
    mockCollection,
    mockFind,
    mockToArray,
    mockInsertOne,
    mockInsertMany,
    mockUpdateOne,
    mockUpdateMany,
    mockDeleteOne,
    mockDeleteMany,
    mockCountDocuments,
    mockAggregate,
    mockCommand,
    mockCreateCollection,
  };
}

describe("MongoDBDriver — mongosh query()", () => {
  it("executes db.collection.find({})", async () => {
    const { driver, mockFind } = createMockDriver();
    const result = await driver.query("db.users.find({})");
    expect(mockFind).toHaveBeenCalledWith({});
    expect(result.columns).toContain("name");
    expect(result.rowCount).toBe(1);
  });

  it("executes find with filter and limit chain", async () => {
    const { driver, mockFind } = createMockDriver();
    const findReturn = mockFind.mockReturnValue({
      limit: vi.fn().mockReturnThis(),
      skip: vi.fn().mockReturnThis(),
      toArray: vi.fn().mockResolvedValue([]),
    });
    await driver.query('db.users.find({ name: "Alice" }).limit(5)');
    expect(findReturn).toHaveBeenCalledWith({ name: "Alice" });
  });

  it("executes findOne", async () => {
    const { driver, mockFind } = createMockDriver();
    const mockFindChain = {
      limit: vi.fn().mockReturnThis(),
      skip: vi.fn().mockReturnThis(),
      toArray: vi.fn().mockResolvedValue([{ _id: "abc" }]),
    };
    mockFind.mockReturnValue(mockFindChain);
    const result = await driver.query("db.users.findOne({ _id: ObjectId(\"507f1f77bcf86cd799439011\") })");
    expect(mockFindChain.limit).toHaveBeenCalledWith(1);
    expect(result.rowCount).toBe(1);
  });

  it("executes insertOne", async () => {
    const { driver, mockInsertOne } = createMockDriver();
    const result = await driver.query('db.users.insertOne({ name: "Bob", age: 30 })');
    expect(mockInsertOne).toHaveBeenCalledWith({ name: "Bob", age: 30 });
    expect(result.columns).toContain("acknowledged");
    expect(result.columns).toContain("insertedId");
    expect(result.affectedRows).toBe(1);
  });

  it("executes insertMany", async () => {
    const { driver, mockInsertMany } = createMockDriver();
    const result = await driver.query("db.items.insertMany([{ x: 1 }, { x: 2 }])");
    expect(mockInsertMany).toHaveBeenCalledWith([{ x: 1 }, { x: 2 }]);
    expect(result.affectedRows).toBe(2);
  });

  it("executes updateMany", async () => {
    const { driver, mockUpdateMany } = createMockDriver();
    const result = await driver.query(
      'db.users.updateMany({ status: "active" }, { $set: { updated: true } })',
    );
    expect(mockUpdateMany).toHaveBeenCalledWith(
      { status: "active" },
      { $set: { updated: true } },
    );
    expect(result.affectedRows).toBe(2);
  });

  it("executes updateOne", async () => {
    const { driver, mockUpdateOne } = createMockDriver();
    await driver.query('db.users.updateOne({ _id: "abc" }, { $set: { name: "Updated" } })');
    expect(mockUpdateOne).toHaveBeenCalledWith(
      { _id: "abc" },
      { $set: { name: "Updated" } },
    );
  });

  it("executes deleteMany", async () => {
    const { driver, mockDeleteMany } = createMockDriver();
    const result = await driver.query("db.users.deleteMany({ active: false })");
    expect(mockDeleteMany).toHaveBeenCalledWith({ active: false });
    expect(result.affectedRows).toBe(3);
  });

  it("executes deleteOne", async () => {
    const { driver, mockDeleteOne } = createMockDriver();
    const result = await driver.query('db.users.deleteOne({ _id: "abc" })');
    expect(mockDeleteOne).toHaveBeenCalledWith({ _id: "abc" });
    expect(result.affectedRows).toBe(1);
  });

  it("executes countDocuments", async () => {
    const { driver, mockCountDocuments } = createMockDriver();
    const result = await driver.query("db.users.countDocuments({ active: true })");
    expect(mockCountDocuments).toHaveBeenCalledWith({ active: true });
    expect(result.columns).toEqual(["count"]);
    expect(result.rows[0]?.__col_0).toBe(5);
  });

  it("executes db.runCommand", async () => {
    const { driver, mockCommand } = createMockDriver();
    const result = await driver.query("db.runCommand({ ping: 1 })");
    expect(mockCommand).toHaveBeenCalledWith({ ping: 1 });
    expect(result.rowCount).toBe(1);
  });

  it("executes createCollection", async () => {
    const { driver, mockCreateCollection } = createMockDriver();
    const result = await driver.query('db.createCollection("newcoll")');
    expect(mockCreateCollection).toHaveBeenCalledWith("newcoll");
    expect(result.columns).toContain("ok");
  });

  it("supports getSiblingDB for cross-database queries", async () => {
    const { driver, mockDb } = createMockDriver();
    await driver.query('db.getSiblingDB("otherdb").orders.find({})');
    expect(mockDb.collection).toHaveBeenCalledWith("orders");
  });

  it("handles ObjectId() in filter", async () => {
    const { driver, mockFind } = createMockDriver();
    const { ObjectId } = await import("mongodb");
    await driver.query(
      'db.users.find({ _id: ObjectId("507f1f77bcf86cd799439011") })',
    );
    expect(mockFind).toHaveBeenCalledWith({
      _id: new ObjectId("507f1f77bcf86cd799439011"),
    });
  });

  it("returns empty result for empty input", async () => {
    const { driver } = createMockDriver();
    const result = await driver.query("   ");
    expect(result.rowCount).toBe(0);
    expect(result.columns).toEqual([]);
  });

  it("returns empty result for comment-only input", async () => {
    const { driver } = createMockDriver();
    const result = await driver.query("// just a comment");
    expect(result.rowCount).toBe(0);
  });

  it("throws a descriptive error for unsupported operations", async () => {
    const { driver } = createMockDriver();
    await expect(driver.query("db.users.drop()")).rejects.toThrow(
      /Unsupported mongosh operation/,
    );
  });

  it("throws a descriptive error for invalid syntax", async () => {
    const { driver } = createMockDriver();
    await expect(driver.query("SELECT * FROM users")).rejects.toThrow(
      /mongosh error/,
    );
  });
});

describe("MongoDBDriver — buildMutationPreviewStatement()", () => {
  const driver = new MongoDBDriver({
    id: "preview-test",
    type: "mongodb",
    name: "test",
    host: "localhost",
    port: 27017,
    database: "mydb",
  });

  it("generates insertOne preview", () => {
    const preview = driver.buildMutationPreviewStatement(
      "insert",
      "mydb",
      "mydb",
      "users",
      { values: { name: "Alice", age: 30 } },
    );
    expect(preview).toContain('db.getSiblingDB("mydb").users.insertOne');
    expect(preview).toContain("Alice");
  });

  it("generates updateMany preview with ObjectId", () => {
    const preview = driver.buildMutationPreviewStatement(
      "update",
      "mydb",
      "mydb",
      "users",
      {
        primaryKeys: { _id: "507f1f77bcf86cd799439011" },
        changes: { name: "Updated" },
      },
    );
    expect(preview).toContain("updateMany");
    expect(preview).toContain("ObjectId");
    expect(preview).toContain("$set");
    expect(preview).toContain("Updated");
  });

  it("generates deleteMany preview for single key", () => {
    const preview = driver.buildMutationPreviewStatement(
      "delete",
      "mydb",
      "mydb",
      "users",
      { primaryKeyValuesList: [{ _id: "507f1f77bcf86cd799439011" }] },
    );
    expect(preview).toContain("deleteMany");
    expect(preview).toContain("ObjectId");
  });

  it("generates deleteMany preview with $or for multiple keys", () => {
    const preview = driver.buildMutationPreviewStatement(
      "delete",
      "mydb",
      "mydb",
      "users",
      {
        primaryKeyValuesList: [
          { _id: "507f1f77bcf86cd799439011" },
          { _id: "507f1f77bcf86cd799439012" },
        ],
      },
    );
    expect(preview).toContain("deleteMany");
    expect(preview).toContain("$or");
  });
});
