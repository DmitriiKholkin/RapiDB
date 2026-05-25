import { registerLiveDriverConformanceTests } from "../shared/driverConformance";
import { registerTableServiceIntegrationTests } from "../shared/tableServiceIntegration";

registerLiveDriverConformanceTests("mysql");
registerLiveDriverConformanceTests("mysql", { transport: "ssh" });
registerTableServiceIntegrationTests("mysql");
