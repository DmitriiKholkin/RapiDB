import { registerLiveDriverConformanceTests } from "../shared/driverConformance";
import { registerTableServiceIntegrationTests } from "../shared/tableServiceIntegration";

registerLiveDriverConformanceTests("mssql");
registerTableServiceIntegrationTests("mssql");
