import { registerLiveDriverConformanceTests } from "../shared/driverConformance";
import { registerTableServiceIntegrationTests } from "../shared/tableServiceIntegration";

registerLiveDriverConformanceTests("oracle");
registerLiveDriverConformanceTests("oracle", { transport: "ssh" });
registerTableServiceIntegrationTests("oracle");
