import type { ElasticsearchDriver } from "../../src/extension/dbDrivers/elasticsearch";

export function elasticsearchKeys(
  driver: ElasticsearchDriver,
  id: string,
  index = "users",
  table = index,
  routing?: string,
): Record<string, unknown> {
  const row = (
    driver as unknown as {
      hitsToRows(hits: unknown[], table: string): Record<string, unknown>[];
    }
  ).hitsToRows(
    [
      {
        _id: id,
        _index: index,
        _routing: routing,
        _seq_no: 3,
        _primary_term: 1,
        _source: {},
      },
    ],
    table,
  )[0];
  return { _id: row._id, _identity: row._identity };
}
