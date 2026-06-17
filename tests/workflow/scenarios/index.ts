import dynamo from "./dynamoScenario";
import elastic from "./elasticScenario";
import mongo from "./mongoScenario";
import mssql from "./mssqlScenario";
import mysql from "./mysqlScenario";
import oracle from "./oracleScenario";
import postgres from "./postgresScenario";
import redis from "./redisScenario";
import sqlite from "./sqliteScenario";
import type { EngineScenario, WorkflowEngineId } from "./types";

export type { EngineScenario, WorkflowEngineId } from "./types";

export const engineScenarios: Record<WorkflowEngineId, () => EngineScenario> = {
  sqlite,
  postgres,
  mysql,
  mssql,
  oracle,
  mongodb: mongo,
  redis,
  elasticsearch: elastic,
  dynamodb: dynamo,
};

export const workflowEngines: readonly WorkflowEngineId[] = [
  "sqlite",
  "postgres",
  "mysql",
  "mssql",
  "oracle",
  "mongodb",
  "redis",
  "elasticsearch",
  "dynamodb",
];

export function getScenario(engineId: WorkflowEngineId): EngineScenario {
  return engineScenarios[engineId]();
}
