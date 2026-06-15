export type { ActivationServices, ActivationState } from "./activationContext";
export {
  hasActiveState,
  setActiveState,
  takeActiveState,
} from "./activationContext";
export type { ExplorerBadgeUpdater } from "./badgeUpdater";
export { createBadgeUpdater } from "./badgeUpdater";
export type { CommandRegistrar, RegisterCommand } from "./commandRegistrar";
export { createCommandRegistrar } from "./commandRegistrar";
export {
  getExplorerSchemaScopeForNode,
  isConnectionRootNode,
} from "./explorerScope";
