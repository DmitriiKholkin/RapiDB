import type { ConnectionConfig } from "../../shared/connectionConfig";
import {
  type ConnectionValidationResult,
  validateConnectionConfig,
} from "../../shared/connectionValidation";

export class ConnectionValidationService {
  validate(config: Partial<ConnectionConfig>): ConnectionValidationResult {
    return validateConnectionConfig(config);
  }
}
