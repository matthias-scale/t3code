// @effect-diagnostics nodeBuiltinImport:off - the local hostname decides whether the inbox host is this machine.
import * as NodeOS from "node:os";
import {
  AgentInboxCommandError,
  type AgentInboxItem,
  type AgentInboxStatus,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import * as ProcessRunner from "../processRunner.ts";

/** Where `agent-inbox` keeps its records. Unset means this machine. */
export const AGENT_INBOX_HOST_ENV = "T3_AGENT_INBOX_HOST";
const COMMAND_TIMEOUT = "10 seconds";
const SSH_CONNECT_TIMEOUT_SECONDS = 5;

export interface AgentInboxCommand {
  readonly command: string;
  readonly args: ReadonlyArray<string>;
}

const shortHost = (host: string) => host.trim().toLowerCase().split(".")[0] ?? "";

/**
 * Runs the CLI where the records live: locally when no inbox host is set or it names this
 * machine, otherwise over non-interactive ssh so a missing key fails fast instead of prompting.
 */
export function buildAgentInboxCommand(input: {
  readonly inboxHost: string | undefined;
  readonly localHost: string;
  readonly args: ReadonlyArray<string>;
}): AgentInboxCommand {
  const inboxHost = input.inboxHost?.trim();
  if (!inboxHost || shortHost(inboxHost) === shortHost(input.localHost)) {
    return { command: "agent-inbox", args: input.args };
  }
  return {
    command: "ssh",
    args: [
      "-o",
      "BatchMode=yes",
      "-o",
      `ConnectTimeout=${SSH_CONNECT_TIMEOUT_SECONDS}`,
      inboxHost,
      "agent-inbox",
      ...input.args,
    ],
  };
}

const str = (value: unknown): string | null => (typeof value === "string" ? value : null);

/** Parses `agent-inbox status --json`; anything off-contract returns null so the section hides. */
export function parseAgentInboxStatusJson(
  stdout: string,
): { readonly items: ReadonlyArray<AgentInboxItem> } | null {
  let parsed: unknown;
  try {
    parsed = JSON.parse(stdout);
  } catch {
    return null;
  }
  if (typeof parsed !== "object" || parsed === null) return null;
  const record = parsed as Record<string, unknown>;
  if (record.schema !== 1 || !Array.isArray(record.items)) return null;
  const items: AgentInboxItem[] = [];
  for (const raw of record.items as unknown[]) {
    if (typeof raw !== "object" || raw === null) continue;
    const item = raw as Record<string, unknown>;
    const id = str(item.id)?.trim();
    if (!id) continue;
    items.push({
      id,
      title: str(item.title) ?? id,
      state: str(item.state) ?? "unknown",
      source: str(item.source) ?? "box",
      createdAt: str(item.created_at),
      seen: item.seen === true,
      seenAt: str(item.seen_at),
      seenByHost: str(item.seen_by_host),
      t3ThreadId: str(item.t3_thread_id),
      t3Host: str(item.t3_host),
    });
  }
  return { items };
}

export class AgentInbox extends Context.Service<
  AgentInbox,
  {
    readonly status: Effect.Effect<AgentInboxStatus>;
    readonly markSeen: (id: string) => Effect.Effect<void, AgentInboxCommandError>;
    readonly pull: (id: string) => Effect.Effect<void, AgentInboxCommandError>;
  }
>()("t3/agentInbox/AgentInbox") {}

export const make = (options?: {
  readonly inboxHost?: string | undefined;
  readonly localHost?: string | undefined;
}) =>
  Effect.gen(function* () {
    const processRunner = yield* ProcessRunner.ProcessRunner;
    const localHost = options?.localHost ?? NodeOS.hostname();
    const inboxHost =
      options && "inboxHost" in options ? options.inboxHost : process.env[AGENT_INBOX_HOST_ENV];
    const run = (args: ReadonlyArray<string>) =>
      processRunner.run({
        ...buildAgentInboxCommand({ inboxHost, localHost, args }),
        timeout: COMMAND_TIMEOUT,
        timeoutBehavior: "timedOutResult",
        maxOutputBytes: 1024 * 1024,
      });
    const unavailable: AgentInboxStatus = {
      available: false,
      host: localHost,
      items: [],
      unseenCount: 0,
    };

    const status: AgentInbox["Service"]["status"] = run(["status", "--json"]).pipe(
      Effect.map((result) => {
        if (result.timedOut || result.code !== 0) return unavailable;
        const parsed = parseAgentInboxStatusJson(result.stdout);
        if (parsed === null) return unavailable;
        return {
          available: true,
          host: localHost,
          items: parsed.items,
          unseenCount: parsed.items.filter((item) => !item.seen).length,
        };
      }),
      // Missing binary, ssh failure or bad output all mean "no inbox here", never an error.
      Effect.orElseSucceed(() => unavailable),
    );

    const command =
      (operation: "seen" | "pull", args: (id: string) => ReadonlyArray<string>) => (id: string) =>
        run(args(id)).pipe(
          Effect.mapError(
            (cause) => new AgentInboxCommandError({ operation, detail: cause.message }),
          ),
          Effect.flatMap((result) =>
            result.timedOut || result.code !== 0
              ? Effect.fail(
                  new AgentInboxCommandError({
                    operation,
                    detail: result.timedOut
                      ? "timed out"
                      : result.stderr.trim().split("\n").at(-1) || `exit ${String(result.code)}`,
                  }),
                )
              : Effect.void,
          ),
        );

    return AgentInbox.of({
      status,
      markSeen: command("seen", (id) => ["seen", id]),
      // `pull` materialises every pending request as a T3 thread; the CLI takes no id.
      pull: command("pull", () => ["pull"]),
    });
  });

export const layer = Layer.effect(AgentInbox, make()).pipe(Layer.provide(ProcessRunner.layer));
