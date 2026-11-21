import type { Document } from "mongodb";

export function mockStandaloneMongoClient(collection?: object) {
  return {
    db: (database: string) => ({
      command: async (command: Document) => {
        if (database !== "admin" || command.hello !== 1) {
          throw new Error("Expected a MongoDB hello command on admin");
        }
        return { ok: 1, isWritablePrimary: true, maxWireVersion: 21 };
      },
      collection: () => {
        if (!collection)
          throw new Error("Unexpected MongoDB collection access");
        return collection;
      },
    }),
  };
}
