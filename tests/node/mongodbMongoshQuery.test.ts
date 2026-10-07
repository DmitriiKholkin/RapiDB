import {
  Binary,
  BSONSymbol,
  Code,
  DBRef,
  Decimal128,
  Int32,
  Long,
  MaxKey,
  MinKey,
  ObjectId,
  Timestamp,
} from "mongodb";
import { describe, expect, it, vi } from "vitest";
import { MongoDBDriver } from "../../src/extension/dbDrivers/mongodb";
import type { ColumnTypeMeta } from "../../src/extension/dbDrivers/types";
import { TableMutationService } from "../../src/extension/table/tableMutationService";

function createMockDriver() {
  const driver = new MongoDBDriver({
    id: "test",
    type: "mongodb",
    name: "test",
    host: "localhost",
    port: 27017,
    database: "testdb",
  });

  const mockToArray = vi
    .fn()
    .mockResolvedValue([{ _id: "abc", name: "Alice" }]);
  const mockSort = vi.fn().mockReturnThis();
  const mockFind = vi.fn().mockReturnValue({
    sort: mockSort,
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
  const mockDeleteOne = vi
    .fn()
    .mockResolvedValue({ acknowledged: true, deletedCount: 1 });
  const mockDeleteMany = vi.fn().mockResolvedValue({ deletedCount: 3 });
  const mockCountDocuments = vi.fn().mockResolvedValue(5);
  const mockAggregateToArray = vi.fn().mockResolvedValue([]);
  const mockAggregate = vi
    .fn()
    .mockReturnValue({ toArray: mockAggregateToArray });
  const mockCommand = vi.fn().mockResolvedValue({ ok: 1 });
  const mockCreateCollection = vi.fn().mockResolvedValue({});
  const mockCreateIndex = vi.fn().mockResolvedValue("users_by_email");
  const mockListCollections = vi.fn().mockReturnValue({
    toArray: vi
      .fn()
      .mockResolvedValue([{ name: "users" }, { name: "sales_view" }]),
  });

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
    createIndex: mockCreateIndex,
  };

  const mockDb = {
    collection: vi.fn().mockReturnValue(mockCollection),
    command: mockCommand,
    createCollection: mockCreateCollection,
    listCollections: mockListCollections,
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
    mockSort,
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
    mockCreateIndex,
    mockListCollections,
  };
}

describe("MongoDBDriver — mongosh query()", () => {
  it("executes db.collection.find({})", async () => {
    const { driver, mockFind } = createMockDriver();
    const result = await driver.query("db.users.find({})");
    expect(mockFind).toHaveBeenCalledWith(
      {},
      {
        promoteValues: false,
        bsonRegExp: false,
      },
    );
    expect(result.columns).toContain("name");
    expect(result.rowCount).toBe(1);
  });

  it("formats MongoDB readRows output with canonical nested values", async () => {
    const driver = new MongoDBDriver({
      id: "test-read-rows",
      type: "mongodb",
      name: "test",
      host: "localhost",
      port: 27017,
      database: "testdb",
    });

    const findMock = vi.fn().mockReturnValue({
      limit: vi.fn().mockReturnThis(),
      toArray: vi.fn().mockResolvedValue([
        {
          _id: new ObjectId("64a1b2c3d4e5f67890abcdef"),
          t_array: [
            new Int32(1),
            "two",
            new Int32(3),
            true,
            null,
            { k: "v" },
            [new Int32(7), new Int32(8), new Int32(9)],
          ],
          t_nested_array: [
            [new Int32(1), new Int32(2)],
            [new Int32(3), new Int32(4)],
            [new Int32(5), null, new Int32(7)],
          ],
          t_regex: /quick\s+fox/gi,
        },
      ]),
    });
    const mockDb = {
      collection: vi.fn().mockReturnValue({ find: findMock }),
    };
    (driver as unknown as { client: unknown; connected: boolean }).client = {
      db: vi.fn().mockReturnValue(mockDb),
    };
    (driver as unknown as { connected: boolean }).connected = true;

    const rows = await (
      driver as unknown as {
        readRows: (
          database: string,
          table: string,
          limit: number,
        ) => Promise<Record<string, unknown>[]>;
      }
    ).readRows("testdb", "bson_types", 10);

    expect(rows[0]).toEqual({
      _id: "64a1b2c3d4e5f67890abcdef",
      t_array: '[1,"two",3,true,null,{"k":"v"},[7,8,9]]',
      t_nested_array: "[[1,2],[3,4],[5,null,7]]",
      t_regex: "/quick\\s+fox/gi",
    });
  });

  it("executes find with filter and limit chain", async () => {
    const { driver, mockFind } = createMockDriver();
    const findReturn = mockFind.mockReturnValue({
      limit: vi.fn().mockReturnThis(),
      skip: vi.fn().mockReturnThis(),
      toArray: vi.fn().mockResolvedValue([]),
    });
    await driver.query('db.users.find({ name: "Alice" }).limit(5)');
    expect(findReturn).toHaveBeenCalledWith(
      { name: "Alice" },
      { promoteValues: false, bsonRegExp: false },
    );
  });

  it("applies the find projection and sort cursor modifier", async () => {
    const { driver, mockFind, mockSort, mockToArray } = createMockDriver();
    mockToArray.mockResolvedValue([{ name: "Alice" }]);

    const result = await driver.query(
      "db.users.find({ active: true }, { name: 1, _id: 0 }).sort({ score: -1 })",
    );

    expect(mockFind).toHaveBeenCalledWith(
      { active: true },
      {
        projection: { name: 1, _id: 0 },
        promoteValues: false,
        bsonRegExp: false,
      },
    );
    expect(mockSort).toHaveBeenCalledWith({ score: -1 });
    expect(result.columns).toEqual(["name"]);
    expect(result.rows[0]?.__col_0).toBe("Alice");
  });

  it.each([
    ["find", "null"],
    ["find", "undefined"],
    ["findOne", "null"],
    ["findOne", "undefined"],
  ])("treats %s({}, %s) as having no projection", async (method, projection) => {
    const { driver, mockFind } = createMockDriver();

    const result = await driver.query(`db.users.${method}({}, ${projection})`);

    expect(mockFind).toHaveBeenCalledWith(
      {},
      { promoteValues: false, bsonRegExp: false },
    );
    expect(result.rowCount).toBe(1);
  });

  it.each([
    '"name"',
    "1",
    "true",
    "[]",
  ])("rejects the invalid find projection %s", async (projection) => {
    const { driver, mockFind } = createMockDriver();

    await expect(
      driver.query(`db.users.find({}, ${projection})`),
    ).rejects.toThrow("MongoDB find projection must be an object.");
    expect(mockFind).not.toHaveBeenCalled();
  });

  it("executes findOne", async () => {
    const { driver, mockFind } = createMockDriver();
    const mockFindChain = {
      limit: vi.fn().mockReturnThis(),
      skip: vi.fn().mockReturnThis(),
      toArray: vi.fn().mockResolvedValue([{ _id: "abc" }]),
    };
    mockFind.mockReturnValue(mockFindChain);
    const result = await driver.query(
      'db.users.findOne({ _id: ObjectId("507f1f77bcf86cd799439011") })',
    );
    expect(mockFindChain.limit).toHaveBeenCalledWith(1);
    expect(result.rowCount).toBe(1);
  });

  it("formats BSON values in query results as plain display values", async () => {
    const { driver, mockFind } = createMockDriver();
    const rawDoc = {
      _id: "64a1b2c3d4e5f67890abcdef",
      t_binary: new Binary(Buffer.from([1, 2, 3, 4, 5, 6, 7, 255]), 0),
      t_binary_uuid: new Binary(
        Buffer.from("112233445566778899aabbccddeeffff", "hex"),
        4,
      ),
      t_date: new Date("2024-07-04T12:00:00.000Z"),
      t_decimal128: Decimal128.fromString("123456789.987654321"),
      t_int64: Long.fromString("9223372036854775807"),
      t_array: [
        new Int32(1),
        "two",
        new Int32(3),
        true,
        null,
        { k: "v" },
        [new Int32(7), new Int32(8), new Int32(9)],
      ],
      t_nested_array: [
        [new Int32(1), new Int32(2)],
        [new Int32(3), new Int32(4)],
        [new Int32(5), null, new Int32(7)],
      ],
      t_regex: /quick\s+fox/gi,
      t_timestamp: new Timestamp({ t: 1720094400, i: 1 }),
    };
    const mockFindChain = {
      limit: vi.fn().mockReturnThis(),
      skip: vi.fn().mockReturnThis(),
      toArray: vi.fn().mockResolvedValue([rawDoc]),
    };
    mockFind.mockReturnValue(mockFindChain);

    const result = await driver.query("db.users.findOne({})");
    const firstRow = result.rows[0] ?? {};
    const byColumn = Object.fromEntries(
      result.columns.map((column, index) => [
        column,
        firstRow[`__col_${index}`],
      ]),
    );

    expect(byColumn).toMatchObject({
      _id: "64a1b2c3d4e5f67890abcdef",
      t_binary: "0x01020304050607ff",
      t_binary_uuid: "0x112233445566778899aabbccddeeffff",
      t_date: "2024-07-04 12:00:00",
      t_decimal128: "123456789.987654321",
      t_int64: "9223372036854775807",
      t_array: '[1,"two",3,true,null,{"k":"v"},[7,8,9]]',
      t_nested_array: "[[1,2],[3,4],[5,null,7]]",
      t_regex: "/quick\\s+fox/gi",
      t_timestamp: "2024-07-04 12:00:00",
    });
  });

  it("executes insertOne", async () => {
    const { driver, mockInsertOne } = createMockDriver();
    const result = await driver.query(
      'db.users.insertOne({ name: "Bob", age: 30 })',
    );
    expect(mockInsertOne).toHaveBeenCalledWith({ name: "Bob", age: 30 });
    expect(result.columns).toContain("acknowledged");
    expect(result.columns).toContain("insertedId");
    expect(result.affectedRows).toBe(1);
  });

  it("executes insertMany", async () => {
    const { driver, mockInsertMany } = createMockDriver();
    const result = await driver.query(
      "db.items.insertMany([{ x: 1 }, { x: 2 }])",
    );
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

  it("executes multiple updateMany statements sequentially without semicolons", async () => {
    const { driver, mockUpdateMany } = createMockDriver();
    const result =
      await driver.query(`db.getSiblingDB("rapidb_mongo_db").bson_types.updateMany(
  { "_id": ObjectId("6a0b8a4021e40394d6f8796d") },
  { "$set": { "t_string": "first" } }
)

db.getSiblingDB("rapidb_mongo_db").bson_types.updateMany(
  { "_id": ObjectId("6a0b8a5321e40394d6f8796e") },
  { "$set": { "t_bool_false": true } }
)`);

    expect(mockUpdateMany).toHaveBeenCalledTimes(2);
    expect(mockUpdateMany).toHaveBeenNthCalledWith(
      1,
      { _id: new ObjectId("6a0b8a4021e40394d6f8796d") },
      { $set: { t_string: "first" } },
    );
    expect(mockUpdateMany).toHaveBeenNthCalledWith(
      2,
      { _id: new ObjectId("6a0b8a5321e40394d6f8796e") },
      { $set: { t_bool_false: true } },
    );
    expect(result.rowCount).toBe(2);
    expect(result.affectedRows).toBe(4);
  });

  it("executes updateOne", async () => {
    const { driver, mockUpdateOne } = createMockDriver();
    await driver.query(
      'db.users.updateOne({ _id: "abc" }, { $set: { name: "Updated" } })',
    );
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

  it("does not turn user-supplied BSON-marker-shaped filters into undefined", async () => {
    const { driver, mockDeleteMany } = createMockDriver();
    const filter = { tag: { __rapidbBsonMarker: "Undefined", args: [] } };
    const taggedFilter = {
      tag: { kind: "marker", value: "Undefined", args: [] },
    };

    await driver.query(
      'db.users.deleteMany({ tag: { __rapidbBsonMarker: "Undefined", args: [] } })',
    );
    await driver.query(
      'db.users.deleteMany({ tag: { kind: "marker", value: "Undefined", args: [] } })',
    );

    expect(mockDeleteMany).toHaveBeenNthCalledWith(1, filter);
    expect(mockDeleteMany).toHaveBeenNthCalledWith(2, taggedFilter);
  });

  it("preserves bigint values in mongosh arguments", async () => {
    const { driver, mockInsertOne, mockFind } = createMockDriver();

    await driver.query("db.users.insertOne({ n: 9007199254740993n })");
    await driver.query("db.users.find({ n: 9007199254740993n })");

    expect(mockInsertOne).toHaveBeenCalledWith({ n: 9007199254740993n });
    expect(mockFind).toHaveBeenCalledWith(
      { n: 9007199254740993n },
      { promoteValues: false, bsonRegExp: false },
    );
  });

  it("keeps destructive filters when query code replaces VM serialization built-ins", async () => {
    const { driver, mockDeleteMany } = createMockDriver();

    await driver.query(`db.users.deleteMany({ active: false });
      Object.entries = function() { return []; };
      JSON.stringify = function() { return '{}'; };
      Array["proto"+"type"].toJSON = function() { return []; }`);

    expect(mockDeleteMany).toHaveBeenCalledWith({ active: false });
  });

  it("does not let inherited array setters remove fields from a delete filter", async () => {
    const { driver, mockDeleteMany } = createMockDriver();
    await driver.query(`db.users.deleteMany({ active: false });
      Object.defineProperty(Array["proto"+"type"], "0", { set: function() {} })`);
    expect(mockDeleteMany).toHaveBeenCalledWith({ active: false });
  });

  it("uses the native RegExp source even if query code changes its getter", async () => {
    const { driver, mockDeleteMany } = createMockDriver();
    await driver.query(`db.users.deleteMany({ name: /^Alice$/ });
      Object.defineProperty(RegExp["proto"+"type"], "source", { get: function() { return ".*"; } })`);
    expect(mockDeleteMany).toHaveBeenCalledWith({ name: /^Alice$/ });
  });

  it("uses native RegExp flag getters after query code changes ignoreCase", async () => {
    const { driver, mockDeleteMany } = createMockDriver();
    await driver.query(`db.users.deleteMany({ name: /^Alice$/ });
      Object.defineProperty(nativeRegExp["proto"+"type"], "ignoreCase", { get: function() { return true; } })`);
    expect(mockDeleteMany).toHaveBeenCalledWith({ name: /^Alice$/ });
  });

  it("preserves RegExp filters after query code replaces Symbol.hasInstance", async () => {
    const { driver, mockDeleteMany } = createMockDriver();
    await driver.query(`db.users.deleteMany({ name: { $nin: [/^Alice$/] } });
      Object.defineProperty(nativeRegExp, Symbol.hasInstance, { value: function() { return false; } })`);
    expect(mockDeleteMany).toHaveBeenCalledWith({
      name: { $nin: [/^Alice$/] },
    });
  });

  it("rejects Proxy-wrapped RegExp filters before deleting documents", async () => {
    const { driver, mockDeleteMany } = createMockDriver();
    await expect(
      driver.query("db.users.deleteMany(new Proxy(/^Alice$/, {}))"),
    ).rejects.toThrow(/Proxy/);
    await expect(
      driver.query(
        "db.users.deleteMany({ name: { $nin: [new Proxy(/^Alice$/, {})] } })",
      ),
    ).rejects.toThrow(/Proxy/);
    await expect(
      driver.query(`db.users.deleteMany(new Proxy(/^Alice$/, {
        getPrototypeOf: function() { return Object["proto"+"type"]; }
      }))`),
    ).rejects.toThrow(/Proxy/);
    expect(mockDeleteMany).not.toHaveBeenCalled();
  });

  it("checks the parsed operation again at execution on read-only connections", async () => {
    const { driver, mockDeleteMany } = createMockDriver();
    await expect(
      driver.query("db.users.deleteMany({})", undefined, { readOnly: true }),
    ).rejects.toThrow(/Read-only MongoDB/);

    for (let i = 0; i < 20; i++) {
      try {
        await driver.query(
          'db.users[Math.random() < 0.5 ? "find" : "deleteMany"]({})',
          undefined,
          { readOnly: true },
        );
      } catch (error) {
        expect(error).toBeInstanceOf(Error);
        expect((error as Error).message).toMatch(/Read-only MongoDB/);
      }
    }
    expect(mockDeleteMany).not.toHaveBeenCalled();
  });

  it("round-trips bigint through a MongoDB mutation preview", async () => {
    const { driver, mockUpdateOne } = createMockDriver();
    const preview = driver.buildMutationPreviewStatement(
      "update",
      "testdb",
      "testdb",
      "users",
      {
        primaryKeys: { _id: 9007199254740993n },
        changes: { value: 9007199254740993n },
      },
    );

    expect(preview).toContain("9007199254740993n");
    await driver.query(preview);
    expect(mockUpdateOne).toHaveBeenCalledWith(
      { _id: { $eq: 9007199254740993n } },
      { $set: { value: 9007199254740993n } },
    );
  });

  it("executes deleteOne", async () => {
    const { driver, mockDeleteOne } = createMockDriver();
    const result = await driver.query('db.users.deleteOne({ _id: "abc" })');
    expect(mockDeleteOne).toHaveBeenCalledWith({ _id: "abc" });
    expect(result.affectedRows).toBe(1);
  });

  it("executes countDocuments", async () => {
    const { driver, mockCountDocuments } = createMockDriver();
    const result = await driver.query(
      "db.users.countDocuments({ active: true })",
    );
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

  it("executes db.getCollectionNames", async () => {
    const { driver, mockDb, mockListCollections } = createMockDriver();

    const result = await driver.query(
      'db.getSiblingDB("otherdb").getCollectionNames()',
    );

    expect(mockDb.listCollections).toHaveBeenCalledWith(
      {},
      { authorizedCollections: true, nameOnly: true },
    );
    expect(mockListCollections).toHaveBeenCalledTimes(1);
    expect(result.columns).toEqual(["name"]);
    expect(result.rows.map((row) => row.__col_0)).toEqual([
      "users",
      "sales_view",
    ]);
  });

  it("executes createCollection", async () => {
    const { driver, mockCreateCollection } = createMockDriver();
    const result = await driver.query('db.createCollection("newcoll")');
    expect(mockCreateCollection).toHaveBeenCalledWith("newcoll");
    expect(result.columns).toContain("ok");
  });

  it("executes createCollection with options from generated DDL", async () => {
    const { driver, mockCreateCollection } = createMockDriver();
    await driver.query(
      'db.createCollection("users", { validator: { $jsonSchema: { bsonType: "object" } } })',
    );
    expect(mockCreateCollection).toHaveBeenCalledWith("users", {
      validator: { $jsonSchema: { bsonType: "object" } },
    });
  });

  it("executes createView from generated DDL", async () => {
    const { driver, mockCreateCollection } = createMockDriver();
    const result = await driver.query(
      'db.getSiblingDB("otherdb").createView("active_users", "users", [{ $match: { active: true } }])',
    );
    expect(mockCreateCollection).toHaveBeenCalledWith("active_users", {
      viewOn: "users",
      pipeline: [{ $match: { active: true } }],
    });
    expect(result.columns).toContain("viewOn");
  });

  it("executes createIndex via getCollection from generated DDL", async () => {
    const { driver, mockDb, mockCreateIndex } = createMockDriver();
    const result = await driver.query(
      'db.getSiblingDB("otherdb").getCollection("users").createIndex({ email: 1 }, { name: "users_by_email", unique: true })',
    );
    expect(mockDb.collection).toHaveBeenCalledWith("users");
    expect(mockCreateIndex).toHaveBeenCalledWith(
      { email: 1 },
      { name: "users_by_email", unique: true },
    );
    expect(result.rows[0]?.__col_1).toBe("users_by_email");
  });

  it("supports getSiblingDB for cross-database queries", async () => {
    const { driver, mockDb } = createMockDriver();
    await driver.query('db.getSiblingDB("otherdb").orders.find({})');
    expect(mockDb.collection).toHaveBeenCalledWith("orders");
  });

  it("handles ObjectId() in filter", async () => {
    const { driver, mockFind } = createMockDriver();
    await driver.query(
      'db.users.find({ _id: ObjectId("507f1f77bcf86cd799439011") })',
    );
    expect(mockFind).toHaveBeenCalledWith(
      {
        _id: new ObjectId("507f1f77bcf86cd799439011"),
      },
      {
        promoteValues: false,
        bsonRegExp: false,
      },
    );
  });

  it("generates an ObjectId when ObjectId() has no argument", async () => {
    const { driver, mockInsertOne } = createMockDriver();
    await driver.query("db.users.insertOne({ _id: ObjectId() })");

    const [document] = mockInsertOne.mock.calls[0] ?? [];
    expect(document).toMatchObject({ _id: expect.any(ObjectId) });
  });

  it("preserves numeric milliseconds passed to Date()", async () => {
    const { driver, mockInsertOne } = createMockDriver();
    await driver.query("db.users.insertOne({ createdAt: new Date(0) })");

    const [document] = mockInsertOne.mock.calls[0] ?? [];
    expect(document).toEqual({ createdAt: new Date(0) });
  });

  it.each([
    'Date["con"+"structor"]["con"+"structor"]',
    'this["con"+"structor"]["con"+"structor"]',
    'db.users.find["con"+"structor"]["con"+"structor"]',
    'ObjectId("507f1f77bcf86cd799439011")["con"+"structor"]["con"+"structor"]',
  ])("cannot reach the extension host through %s", async (accessPath) => {
    const { driver, mockFind } = createMockDriver();
    await expect(
      driver.query(
        `db.users.find({ leak: ${accessPath}("return pro"+"cess.version")() })`,
      ),
    ).rejects.toThrow();
    expect(mockFind).not.toHaveBeenCalled();
  });

  it("applies the VM timeout to recursively queued Promise microtasks", async () => {
    const { driver, mockFind } = createMockDriver();
    const query = `function loop() { Promise.resolve().then(loop); }
      Promise.resolve().then(loop);
      db.users.find({})`;

    await expect(driver.query(query)).rejects.toThrow(/timed out/i);
    expect(mockFind).not.toHaveBeenCalled();
  }, 10_000);

  it.each([
    "NaN",
    "Infinity",
    "-Infinity",
  ])("rejects %s before passing a changed filter to MongoDB", async (value) => {
    const { driver, mockDeleteMany } = createMockDriver();
    await expect(
      driver.query(`db.users.deleteMany({ score: ${value} })`),
    ).rejects.toThrow(/Non-finite numbers/);
    expect(mockDeleteMany).not.toHaveBeenCalled();
  });

  it("executes generated mutation preview with BSON literals", async () => {
    const { driver, mockUpdateOne } = createMockDriver();
    const preview = driver.buildMutationPreviewStatement(
      "update",
      "rapidb_mongo_db",
      "rapidb_mongo_db",
      "bson_types",
      {
        primaryKeys: { _id: new ObjectId("6a0412be9e2b63ce6b3d8c69") },
        changes: {
          t_binary_uuid: new Binary(
            Buffer.from("ESIzRFVmd4iZqrvM3e6//w==", "base64"),
            4,
          ),
          t_dbpointer: new DBRef(
            "users",
            new ObjectId("507f1f77bcf86cd799439011"),
            "rapidb_mongo_db",
          ),
          t_date: new Date("2024-07-04T12:00:00.000Z"),
          t_timestamp: new Timestamp({ t: 1720094400, i: 1 }),
          t_decimal128: Decimal128.fromString("123456789.987654321"),
          t_int32: new Int32(42),
          t_int64: Long.fromString("9223372036854775807"),
          t_js: new Code("function() { return true; }"),
          t_js_scope: new Code("function() { return x; }", { x: 1 }),
          t_symbol: new BSONSymbol("alpha"),
          t_minkey: new MinKey(),
          t_maxkey: new MaxKey(),
          t_undefined: undefined,
        },
      },
    );

    await driver.query(preview);

    expect(mockUpdateOne).toHaveBeenCalledTimes(1);
    const [criteria, update] = mockUpdateOne.mock.calls[0] ?? [];
    expect(criteria).toEqual({
      _id: { $eq: new ObjectId("6a0412be9e2b63ce6b3d8c69") },
    });

    const setClause = (update as { $set?: Record<string, unknown> }).$set ?? {};
    const binaryValue = setClause.t_binary_uuid as Binary;
    const binaryWithBuffer = binaryValue as unknown as {
      buffer?: Buffer;
      position?: number;
      sub_type?: number;
    };

    expect(binaryValue).toBeInstanceOf(Binary);
    expect(binaryWithBuffer.sub_type).toBe(4);
    expect(
      binaryWithBuffer.buffer
        ?.subarray(0, binaryWithBuffer.position)
        .toString("base64"),
    ).toBe("ESIzRFVmd4iZqrvM3e6//w==");
    expect(setClause.t_date).toBeInstanceOf(Date);
    expect(setClause.t_timestamp).toBeInstanceOf(Timestamp);
    expect(setClause.t_decimal128).toBeInstanceOf(Decimal128);
    expect(setClause.t_int32).toBeInstanceOf(Int32);
    expect(setClause.t_int64).toBeInstanceOf(Long);
    expect(setClause.t_dbpointer).toBeInstanceOf(DBRef);
    expect(setClause.t_js).toBeInstanceOf(Code);
    expect((setClause.t_js_scope as Code).scope).toEqual({ x: 1 });
    expect(setClause.t_symbol).toBeInstanceOf(BSONSymbol);
    expect(setClause.t_minkey).toBeInstanceOf(MinKey);
    expect(setClause.t_maxkey).toBeInstanceOf(MaxKey);
    expect(setClause).toHaveProperty("t_undefined", undefined);
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

describe("MongoDBDriver — readTablePage()", () => {
  it("uses MongoDB server-side filters, sort, and pagination", async () => {
    const { driver, mockFind, mockCountDocuments } = createMockDriver();

    await driver.readTablePage({
      database: "testdb",
      schema: "",
      table: "users",
      page: 2,
      pageSize: 5,
      filters: [
        {
          column: "name",
          operator: "ilike",
          value: "ali",
        },
      ],
      sort: { column: "name", direction: "desc" },
      skipCount: false,
    });

    expect(mockFind).toHaveBeenNthCalledWith(
      2,
      {
        name: {
          $regex: "ali",
          $options: "i",
        },
      },
      {
        promoteValues: false,
        bsonRegExp: false,
      },
    );
    const readCursor = mockFind.mock.results.at(-1)?.value as {
      sort: ReturnType<typeof vi.fn>;
      skip: ReturnType<typeof vi.fn>;
      limit: ReturnType<typeof vi.fn>;
    };
    expect(readCursor.sort).toHaveBeenCalledWith([
      ["name", -1],
      ["_id", 1],
    ]);
    expect(readCursor.skip).toHaveBeenCalledWith(5);
    expect(readCursor.limit).toHaveBeenCalledWith(5);
    expect(mockCountDocuments).toHaveBeenCalledWith({
      name: {
        $regex: "ali",
        $options: "i",
      },
    });
  });
});

describe("MongoDB table mutations", () => {
  const id = "507f1f77bcf86cd799439011";

  it("rejects missing primary keys before preparing a deletion", async () => {
    const { driver, mockDeleteMany } = createMockDriver();
    const idColumn: ColumnTypeMeta = {
      name: "_id",
      type: "string",
      nativeType: "string",
      category: "text",
      nullable: false,
      isPrimaryKey: true,
      isForeignKey: false,
      filterable: true,
      filterOperators: ["eq"],
      valueSemantics: "plain",
    };
    const manager = {
      getConnection: () => ({ id: "test", name: "test", type: "mongodb" }),
      getDriver: () => driver,
    };
    const service = new TableMutationService(manager as never, {
      getColumns: async () => [idColumn],
    });

    await expect(
      service.prepareDeleteRowsPlan("test", "testdb", "", "users", [{}]),
    ).rejects.toThrow(/full primary key/);
    await expect(
      driver.deleteRows({
        database: "testdb",
        schema: "",
        table: "users",
        primaryKeyValuesList: [{}],
      }),
    ).rejects.toThrow(/complete _id/);
    expect(mockDeleteMany).not.toHaveBeenCalled();
  });

  it("preserves a hexadecimal string ID when updating and deleting", async () => {
    const { driver, mockUpdateOne, mockDeleteOne } = createMockDriver();
    await driver.updateRows({
      database: "testdb",
      schema: "",
      table: "users",
      updates: [{ primaryKeys: { _id: id }, changes: { name: "Bob" } }],
    });
    await driver.deleteRows({
      database: "testdb",
      schema: "",
      table: "users",
      primaryKeyValuesList: [{ _id: id }],
    });

    expect(mockUpdateOne).toHaveBeenCalledWith(
      { _id: { $eq: id } },
      { $set: { name: "Bob" } },
    );
    expect(mockDeleteOne).toHaveBeenCalledWith({ _id: { $eq: id } });
  });

  it("reports identities for completed updates when a later Mongo update conflicts", async () => {
    const { driver, mockUpdateOne } = createMockDriver();
    mockUpdateOne
      .mockResolvedValueOnce({ acknowledged: true, matchedCount: 1 })
      .mockResolvedValueOnce({ acknowledged: true, matchedCount: 0 });

    await expect(
      driver.updateRows({
        database: "testdb",
        schema: "",
        table: "users",
        updates: [
          { primaryKeys: { _id: "row-1" }, changes: { name: "One" } },
          { primaryKeys: { _id: "row-2" }, changes: { name: "Two" } },
        ],
      }),
    ).resolves.toEqual({
      affectedRows: 1,
      updateRowOutcomes: [
        { rowIndex: 0, status: "applied" },
        { rowIndex: 1, status: "not_applied" },
      ],
    });
  });

  it("retains a confirmed Mongo update when a later sequential update errors", async () => {
    const { driver, mockUpdateOne } = createMockDriver();
    mockUpdateOne
      .mockResolvedValueOnce({ acknowledged: true, matchedCount: 1 })
      .mockRejectedValueOnce(new Error("Connection lost"));

    await expect(
      driver.updateRows({
        database: "testdb",
        schema: "",
        table: "users",
        updates: [
          { primaryKeys: { _id: "row-1" }, changes: { name: "One" } },
          { primaryKeys: { _id: "row-2" }, changes: { name: "Two" } },
          { primaryKeys: { _id: "row-3" }, changes: { name: "Three" } },
        ],
      }),
    ).rejects.toMatchObject({
      result: {
        affectedRows: 1,
        updateRowOutcomes: [
          { rowIndex: 0, status: "applied" },
          { rowIndex: 1, status: "unknown" },
          { rowIndex: 2, status: "not_applied" },
        ],
      },
    });
  });

  it("keeps per-row BSON _id types across table reads, previews, updates and deletes", async () => {
    const { driver, mockToArray, mockUpdateOne, mockDeleteOne } =
      createMockDriver();
    const objectId = new ObjectId(id);
    mockToArray.mockResolvedValueOnce([
      { _id: objectId, name: "object row" },
      { _id: id, name: "string row" },
    ]);
    const idColumn: ColumnTypeMeta = {
      name: "_id",
      type: "objectId",
      nativeType: "objectId",
      category: "text",
      nullable: false,
      isPrimaryKey: true,
      isForeignKey: false,
      filterable: true,
      filterOperators: ["eq"],
      valueSemantics: "plain",
    };
    const nameColumn: ColumnTypeMeta = {
      ...idColumn,
      name: "name",
      type: "string",
      nativeType: "string",
      isPrimaryKey: false,
    };
    Object.assign(driver, {
      describeSchemaColumns: async () => [idColumn, nameColumn],
    });
    const page = await driver.readTablePage({
      database: "testdb",
      schema: "",
      table: "users",
      page: 1,
      pageSize: 10,
      filters: [],
      sort: null,
      skipCount: false,
    });
    expect(page.rows.map((row) => row._id)).toEqual([id, id]);
    expect(page.mongoIdTypes).toEqual(["objectId", "string"]);

    const service = new TableMutationService(
      {
        getConnection: () => ({ id: "test", type: "mongodb" }),
        getDriver: () => driver,
      } as never,
      { getColumns: async () => [idColumn, nameColumn] },
    );
    const taggedStringId = {
      $rapidbMongoId: { type: "string", value: page.rows[1]._id },
    };
    const taggedObjectId = {
      $rapidbMongoId: { type: "objectId", value: page.rows[0]._id },
    };
    const plan = await service.prepareDeleteRowsPlan(
      "test",
      "testdb",
      "",
      "users",
      [{ _id: taggedStringId }],
    );
    expect(plan?.primaryKeyValuesList).toEqual([{ _id: id }]);
    expect(plan?.previewStatements[0]).toContain(`"${id}"`);
    expect(plan?.previewStatements[0]).not.toContain("ObjectId(");
    if (!plan) throw new Error("Expected delete plan");
    await service.executePreparedDeletePlan(plan);
    expect(mockDeleteOne).toHaveBeenCalledWith({ _id: { $eq: id } });
    await service.updateRow(
      "test",
      "testdb",
      "",
      "users",
      { _id: taggedObjectId },
      { name: "updated" },
    );
    expect(mockUpdateOne).toHaveBeenCalledWith(
      { _id: { $eq: objectId } },
      { $set: { name: "updated" } },
    );
  });

  it("rejects unsupported tagged table ID types instead of targeting another document", async () => {
    const { driver, mockDeleteMany } = createMockDriver();
    const idColumn: ColumnTypeMeta = {
      name: "_id",
      type: "objectId",
      nativeType: "objectId",
      category: "text",
      nullable: false,
      isPrimaryKey: true,
      isForeignKey: false,
      filterable: true,
      filterOperators: ["eq"],
      valueSemantics: "plain",
    };
    const service = new TableMutationService(
      {
        getConnection: () => ({ id: "test", type: "mongodb" }),
        getDriver: () => driver,
      } as never,
      { getColumns: async () => [idColumn] },
    );
    await expect(
      service.prepareDeleteRowsPlan("test", "testdb", "", "users", [
        { _id: { $rapidbMongoId: { type: "unsupported", value: "123" } } },
      ]),
    ).rejects.toThrow("Invalid MongoDB _id type hint");
    expect(mockDeleteMany).not.toHaveBeenCalled();
  });

  it("treats an object-valued ID as a literal, not a delete operator", async () => {
    const { driver, mockDeleteOne } = createMockDriver();
    const idWithOperator = { $ne: null };
    await driver.deleteRows({
      database: "testdb",
      schema: "",
      table: "users",
      primaryKeyValuesList: [{ _id: idWithOperator }],
    });
    expect(mockDeleteOne).toHaveBeenCalledWith({
      _id: { $eq: idWithOperator },
    });
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

  it("generates updateOne preview with ObjectId", () => {
    const preview = driver.buildMutationPreviewStatement(
      "update",
      "mydb",
      "mydb",
      "users",
      {
        primaryKeys: { _id: new ObjectId("507f1f77bcf86cd799439011") },
        changes: { name: "Updated" },
      },
    );
    expect(preview).toContain("updateOne");
    expect(preview).not.toContain("updateMany");
    expect(preview).toContain("ObjectId");
    expect(preview).toContain("$set");
    expect(preview).toContain("Updated");
  });

  it("generates deleteOne preview for single key", () => {
    const preview = driver.buildMutationPreviewStatement(
      "delete",
      "mydb",
      "mydb",
      "users",
      {
        primaryKeyValuesList: [
          { _id: new ObjectId("507f1f77bcf86cd799439011") },
        ],
      },
    );
    expect(preview).toContain("deleteOne");
    expect(preview).toContain("ObjectId");
  });

  it("generates individual deleteOne previews for multiple keys", () => {
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
    expect(preview.match(/deleteOne/g)).toHaveLength(2);
    expect(preview).not.toContain("$or");
  });

  it("keeps hexadecimal string IDs as strings in the preview", () => {
    const preview = driver.buildMutationPreviewStatement(
      "delete",
      "mydb",
      "mydb",
      "users",
      { primaryKeyValuesList: [{ _id: "507f1f77bcf86cd799439011" }] },
    );
    expect(preview).toContain('"_id": { "$eq": "507f1f77bcf86cd799439011" }');
    expect(preview).not.toContain("ObjectId(");
  });

  it("serializes coerced BSON values as valid mongosh literals", () => {
    const binData4Column: ColumnTypeMeta = {
      name: "t_binary_uuid",
      type: "binData",
      nativeType: "binData",
      bsonSubtype: 4,
      nullable: true,
      isPrimaryKey: false,
      isForeignKey: false,
      category: "binary",
      filterable: false,
      filterOperators: [],
      valueSemantics: "plain",
    };
    const dateColumn: ColumnTypeMeta = {
      name: "t_date",
      type: "date",
      nativeType: "date",
      nullable: true,
      isPrimaryKey: false,
      isForeignKey: false,
      category: "datetime",
      filterable: true,
      filterOperators: ["eq"],
      valueSemantics: "plain",
    };
    const timestampColumn: ColumnTypeMeta = {
      name: "t_timestamp",
      type: "timestamp",
      nativeType: "timestamp",
      nullable: true,
      isPrimaryKey: false,
      isForeignKey: false,
      category: "datetime",
      filterable: true,
      filterOperators: ["eq"],
      valueSemantics: "plain",
    };
    const preview = driver.buildMutationPreviewStatement(
      "insert",
      "mydb",
      "mydb",
      "users",
      {
        values: {
          t_binary_uuid: driver.coerceInputValue(
            "ESIzRFVmd4iZqrvM3e7//w==",
            binData4Column,
          ),
          t_date: driver.coerceInputValue("2024-07-04 12:00:00", dateColumn),
          t_timestamp: driver.coerceInputValue(
            "2024-07-04 12:00:00",
            timestampColumn,
          ),
        },
      },
    );

    expect(preview).toContain('new Date("2024-07-04T12:00:00.000Z")');
    expect(preview).toContain("new Timestamp(1720094400, 1)");
    expect(preview).toContain("new BinData(4,");
  });

  it("round-trips displayed Mongo values back into canonical display format", () => {
    const binData4Column: ColumnTypeMeta = {
      name: "t_binary_uuid",
      type: "binData",
      nativeType: "binData",
      bsonSubtype: 4,
      nullable: true,
      isPrimaryKey: false,
      isForeignKey: false,
      category: "binary",
      filterable: false,
      filterOperators: [],
      valueSemantics: "plain",
    };
    const dateColumn: ColumnTypeMeta = {
      name: "t_date",
      type: "date",
      nativeType: "date",
      nullable: true,
      isPrimaryKey: false,
      isForeignKey: false,
      category: "datetime",
      filterable: true,
      filterOperators: ["eq"],
      valueSemantics: "plain",
    };
    const timestampColumn: ColumnTypeMeta = {
      name: "t_timestamp",
      type: "timestamp",
      nativeType: "timestamp",
      nullable: true,
      isPrimaryKey: false,
      isForeignKey: false,
      category: "datetime",
      filterable: true,
      filterOperators: ["eq"],
      valueSemantics: "plain",
    };
    const regexColumn: ColumnTypeMeta = {
      name: "t_regex",
      type: "regex",
      nativeType: "regex",
      nullable: true,
      isPrimaryKey: false,
      isForeignKey: false,
      category: "other",
      filterable: true,
      filterOperators: ["eq"],
      valueSemantics: "plain",
    };
    const arrayColumn: ColumnTypeMeta = {
      name: "t_array",
      type: "array",
      nativeType: "array",
      nullable: true,
      isPrimaryKey: false,
      isForeignKey: false,
      category: "array",
      filterable: true,
      filterOperators: ["eq"],
      valueSemantics: "plain",
    };

    expect(
      driver.coerceInputValue(
        'BinData(4, "ESIzRFVmd4iZqrvM3e7//w==")',
        binData4Column,
      ),
    ).toBeInstanceOf(Binary);
    expect(
      driver.formatOutputValue(
        driver.coerceInputValue(
          'BinData(4, "ESIzRFVmd4iZqrvM3e7//w==")',
          binData4Column,
        ),
        binData4Column,
      ),
    ).toBe("0x112233445566778899aabbccddeeffff");
    expect(
      driver.formatOutputValue(
        driver.coerceInputValue("2024-07-04T12:00:00.000Z", dateColumn),
        dateColumn,
      ),
    ).toBe("2024-07-04 12:00:00");
    expect(
      driver.formatOutputValue(
        driver.coerceInputValue("Timestamp(1720094400, 1)", timestampColumn),
        timestampColumn,
      ),
    ).toBe("2024-07-04 12:00:00");
    expect(
      driver.formatOutputValue(
        driver.coerceInputValue("/quick\\s+fox/gi", regexColumn),
        regexColumn,
      ),
    ).toBe("/quick\\s+fox/gi");
    expect(
      driver.formatOutputValue(
        driver.coerceInputValue(
          '[1,"two",3,true,null,{"k":"v"},[7,8,9]]',
          arrayColumn,
        ),
        arrayColumn,
      ),
    ).toBe('[1,"two",3,true,null,{"k":"v"},[7,8,9]]');
  });
});
