import { registerLiveDriverConformanceTests } from "../shared/driverConformance";
import { registerTableServiceIntegrationTests } from "../shared/tableServiceIntegration";

registerLiveDriverConformanceTests("postgres");
registerLiveDriverConformanceTests("postgres", { transport: "ssh" });
registerTableServiceIntegrationTests("postgres");
