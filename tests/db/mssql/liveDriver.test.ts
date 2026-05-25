import { registerLiveDriverConformanceTests } from "../shared/driverConformance";
import { registerTableServiceIntegrationTests } from "../shared/tableServiceIntegration";

registerLiveDriverConformanceTests("mssql");
registerLiveDriverConformanceTests("mssql", { transport: "ssh" });
registerTableServiceIntegrationTests("mssql");
