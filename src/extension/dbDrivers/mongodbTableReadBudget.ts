const MiB = 1024 * 1024;
export const MONGO_TABLE_COLUMN_LIMIT = 1024;

const limits = {
  "schema sample": { document: 2 * MiB, total: 4 * MiB },
  page: { document: 2 * MiB, total: 8 * MiB },
  "client-side filter or sort": { document: 2 * MiB, total: 16 * MiB },
} as const;

type MongoTableRead = keyof typeof limits;

export class MongoTableReadBudget {
  private bsonBytes = 0;
  private textBytes = 0;

  constructor(private readonly scope: MongoTableRead) {}

  acceptBson = (bytes: number): void => {
    const { document, total } = limits[this.scope];
    this.bsonBytes = this.check(bytes, this.bsonBytes, document, total, "BSON");
  };

  acceptDisplay(row: Record<string, unknown>): void {
    // All large display representations (including nested JSON, binary and
    // DBRef editor text) are strings. Check their UTF-8 size without another
    // serialization. BSON has a separate budget for the source documents.
    let bytes = 0;
    for (const [name, value] of Object.entries(row)) {
      bytes += Buffer.byteLength(name);
      if (typeof value === "string") bytes += Buffer.byteLength(value);
    }
    this.textBytes = this.check(
      bytes,
      this.textBytes,
      4 * MiB,
      this.scope === "page" ? 16 * MiB : 24 * MiB,
      "display text",
    );
  }

  private check(
    bytes: number,
    current: number,
    perDocument: number,
    total: number,
    unit: string,
  ): number {
    const reason =
      bytes > perDocument
        ? `a document exceeds the ${perDocument / MiB} MiB ${unit} limit`
        : current + bytes > total
          ? `the result exceeds the ${total / MiB} MiB ${unit} limit`
          : undefined;
    if (reason) {
      const advice =
        this.scope === "schema sample"
          ? ""
          : this.scope === "page" && bytes <= perDocument
            ? " Try a smaller page."
            : this.scope === "client-side filter or sort"
              ? " Try a server-side filter and sort."
              : "";
      throw new Error(
        `MongoDB Table Viewer stopped reading the ${this.scope}: ${reason}. No partial result was returned.${advice} Use a projected query or a database-native export for larger documents or results.`,
      );
    }
    return current + bytes;
  }
}
