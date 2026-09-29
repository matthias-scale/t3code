import { WS_METHODS } from "@t3tools/contracts";
import {
  createEnvironmentRpcCommand,
  createEnvironmentRpcQueryAtomFamily,
} from "@t3tools/client-runtime/state/runtime";

import { connectionAtomRuntime } from "../connection/runtime";

/**
 * Open agent-inbox requests from the shared records. The server answers
 * `available: false` instead of failing when agent-inbox is missing, so a
 * poll never surfaces an error. Polled because the records change on another host.
 */
export const agentInboxStatus = createEnvironmentRpcQueryAtomFamily(connectionAtomRuntime, {
  label: "environment-data:agent-inbox:status",
  tag: WS_METHODS.agentInboxStatus,
  staleTimeMs: 0,
  refreshIntervalMs: 10_000,
});

export const agentInboxMarkSeen = createEnvironmentRpcCommand(connectionAtomRuntime, {
  label: "environment-data:agent-inbox:mark-seen",
  tag: WS_METHODS.agentInboxMarkSeen,
});

export const agentInboxPull = createEnvironmentRpcCommand(connectionAtomRuntime, {
  label: "environment-data:agent-inbox:pull",
  tag: WS_METHODS.agentInboxPull,
});
