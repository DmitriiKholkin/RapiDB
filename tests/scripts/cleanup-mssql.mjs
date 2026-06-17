import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
const mssql = require("mssql");

async function main() {
  const pool = new mssql.ConnectionPool({
    server: "localhost",
    port: 1433,
    database: "master",
    user: "sa",
    password: "Rapidb_Pass123!",
    connectionTimeout: 10_000,
    requestTimeout: 15_000,
    options: {
      encrypt: false,
      trustServerCertificate: true,
    },
  });
  await pool.connect();

  await pool.request().query(`
    DECLARE @kill NVARCHAR(MAX) = '';
    SELECT @kill = @kill + 'KILL ' + CAST(session_id AS VARCHAR(10)) + ';'
    FROM sys.dm_exec_sessions
    WHERE login_name = 'rapidb_test_user' AND session_id <> @@SPID;
    EXEC sp_executesql @kill;
  `);

  console.log("[cleanup-mssql] Killed rapidb_test_user sessions");
  await pool.close();
}

main().catch((err) => {
  console.error("[cleanup-mssql] Error:", err.message);
  process.exit(0);
});
