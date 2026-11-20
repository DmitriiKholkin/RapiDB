import type { ElasticsearchDriver } from "../../src/extension/dbDrivers/elasticsearch";

export const elasticsearchTestIndexUuid = "ee2gDXoIRFia9j-PSY4Mqw";

export function elasticsearchIndexSettings(
  index: string,
  uuid = elasticsearchTestIndexUuid,
) {
  return { [index]: { settings: { index: { uuid } } } };
}

export function elasticsearchKeys(
  driver: ElasticsearchDriver,
  id: string,
  index = "users",
  table = index,
  routing?: string,
): Record<string, unknown> {
  const fixture = driver as unknown as {
    connectionEpoch: number;
    hitsToRows(
      hits: unknown[],
      identity: {
        table: string;
        epoch: number;
        indexUuids: Map<string, string>;
      },
    ): Record<string, unknown>[];
  };
  const row = fixture.hitsToRows(
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
    {
      table,
      epoch: fixture.connectionEpoch,
      indexUuids: new Map([[index, elasticsearchTestIndexUuid]]),
    },
  )[0];
  return { _id: row._id, _identity: row._identity };
}
