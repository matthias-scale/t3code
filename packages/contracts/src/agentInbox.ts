import * as Schema from "effect/Schema";
import { IsoDateTime, TrimmedNonEmptyString } from "./baseSchemas.ts";

/**
 * One open request from the fleet `agent-inbox` CLI (`agent-inbox status --json`, schema 1).
 * The records live on one shared host, so every T3 server reports the same list and seen state.
 */
export const AgentInboxItem = Schema.Struct({
  id: TrimmedNonEmptyString,
  title: Schema.String,
  state: Schema.String,
  source: Schema.String,
  createdAt: Schema.NullOr(IsoDateTime),
  seen: Schema.Boolean,
  seenAt: Schema.NullOr(IsoDateTime),
  seenByHost: Schema.NullOr(Schema.String),
  t3ThreadId: Schema.NullOr(Schema.String),
  t3Host: Schema.NullOr(Schema.String),
});
export type AgentInboxItem = typeof AgentInboxItem.Type;

/**
 * `available: false` means agent-inbox is not installed, not configured, or unreachable.
 * Clients hide the inbox section instead of treating that as an error.
 */
export const AgentInboxStatus = Schema.Struct({
  available: Schema.Boolean,
  /** Hostname of the T3 server that answered, compared against `t3Host` to find local threads. */
  host: Schema.String,
  items: Schema.Array(AgentInboxItem),
  unseenCount: Schema.Number,
});
export type AgentInboxStatus = typeof AgentInboxStatus.Type;

export const AgentInboxStatusInput = Schema.Struct({});
export type AgentInboxStatusInput = typeof AgentInboxStatusInput.Type;

export const AgentInboxItemInput = Schema.Struct({ id: TrimmedNonEmptyString });
export type AgentInboxItemInput = typeof AgentInboxItemInput.Type;

export class AgentInboxCommandError extends Schema.TaggedError<AgentInboxCommandError>()(
  "AgentInboxCommandError",
  {
    operation: Schema.Literals(["seen", "pull"]),
    detail: Schema.String,
  },
) {
  override get message(): string {
    return `agent-inbox ${this.operation} failed: ${this.detail}`;
  }
}
