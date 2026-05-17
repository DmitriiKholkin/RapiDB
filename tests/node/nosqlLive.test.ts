import { describe, it } from "vitest";
import { runNoSqlLiveCheck } from "../scripts/nosql-live-check";

const liveIt = process.env.RAPIDB_LIVE_NOSQL === "1" ? it : it.skip;

describe("NoSQL live driver verification", () => {
  liveIt(
    "verifies Redis, MongoDB, Elasticsearch, and DynamoDB against live services",
    async () => {
      await runNoSqlLiveCheck();
    },
    600_000,
  );
});
