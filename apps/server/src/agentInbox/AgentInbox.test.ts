import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";

import * as ProcessRunner from "../processRunner.ts";
import * as AgentInbox from "./AgentInbox.ts";

const output = (stdout: string, code = 0) => ({
  stdout,
  stderr: code === 0 ? "" : "agent-inbox is not configured\n",
  code: ChildProcessSpawner.ExitCode(code),
  timedOut: false,
  stdoutTruncated: false,
  stderrTruncated: false,
  stdoutInvalidUtf8: false,
  stderrInvalidUtf8: false,
});

const statusJson = JSON.stringify({
  schema: 1,
  generated_at: "2026-09-29T10:00:00Z",
  unseen_count: 1,
  items: [
    {
      id: "r2",
      title: "Second",
      state: "ready",
      source: "slack",
      created_at: "2026-09-29T09:00:00Z",
      seen: false,
      seen_at: null,
      seen_by_host: null,
      t3_thread_id: "thread-2",
      t3_host: "ub1",
    },
    {
      id: "r1",
      title: "First",
      state: "delivered",
      source: "box",
      created_at: "2026-09-28T09:00:00Z",
      seen: true,
      seen_at: "2026-09-28T10:00:00Z",
      seen_by_host: "ub2",
      t3_thread_id: null,
      t3_host: null,
    },
  ],
});

const run = <A, E>(
  effect: Effect.Effect<A, E, AgentInbox.AgentInbox>,
  runner: ProcessRunner.ProcessRunner["Service"]["run"],
  inboxHost: string | undefined = undefined,
) =>
  effect.pipe(
    Effect.provide(
      Layer.effect(AgentInbox.AgentInbox, AgentInbox.make({ inboxHost, localHost: "ub2" })).pipe(
        Layer.provide(Layer.succeed(ProcessRunner.ProcessRunner, { run: runner })),
      ),
    ),
  );

it("builds a local command when no inbox host is set or it is this machine", () => {
  expect(
    AgentInbox.buildAgentInboxCommand({
      inboxHost: undefined,
      localHost: "ub1",
      args: ["seen", "x"],
    }),
  ).toEqual({ command: "agent-inbox", args: ["seen", "x"] });
  expect(
    AgentInbox.buildAgentInboxCommand({
      inboxHost: " UB1 ",
      localHost: "ub1.tail",
      args: ["pull"],
    }),
  ).toEqual({ command: "agent-inbox", args: ["pull"] });
});

it("builds a non-interactive ssh command for a remote inbox host", () => {
  expect(
    AgentInbox.buildAgentInboxCommand({
      inboxHost: "ub1",
      localHost: "ub2",
      args: ["status", "--json"],
    }),
  ).toEqual({
    command: "ssh",
    args: [
      "-o",
      "BatchMode=yes",
      "-o",
      "ConnectTimeout=5",
      "ub1",
      "agent-inbox",
      "status",
      "--json",
    ],
  });
});

it.effect("maps status json and counts unseen items", () =>
  Effect.gen(function* () {
    const calls: Array<ProcessRunner.ProcessRunInput> = [];
    const status = yield* run(
      Effect.flatMap(AgentInbox.AgentInbox, (inbox) => inbox.status),
      (input) =>
        Effect.sync(() => {
          calls.push(input);
          return output(statusJson);
        }),
      "ub1",
    );
    expect(calls[0]?.command).toBe("ssh");
    expect(status.available).toBe(true);
    expect(status.host).toBe("ub2");
    expect(status.unseenCount).toBe(1);
    expect(status.items.map((item) => [item.id, item.t3ThreadId, item.seenByHost])).toEqual([
      ["r2", "thread-2", null],
      ["r1", null, "ub2"],
    ]);
  }),
);

it.effect("reports unavailable on a non-zero exit, bad json, or a spawn failure", () =>
  Effect.gen(function* () {
    const status = Effect.flatMap(AgentInbox.AgentInbox, (inbox) => inbox.status);
    const failed = yield* run(status, () => Effect.succeed(output("", 1)));
    const garbage = yield* run(status, () => Effect.succeed(output("not json")));
    const missing = yield* run(status, () =>
      Effect.fail(
        new ProcessRunner.ProcessSpawnError({
          command: "agent-inbox",
          argumentCount: 2,
          cause: "ENOENT",
        }),
      ),
    );
    for (const result of [failed, garbage, missing]) {
      expect(result).toEqual({ available: false, host: "ub2", items: [], unseenCount: 0 });
    }
  }),
);

it.effect("marks seen by id and fails with the CLI's last stderr line", () =>
  Effect.gen(function* () {
    const calls: Array<ReadonlyArray<string>> = [];
    yield* run(
      Effect.flatMap(AgentInbox.AgentInbox, (inbox) => inbox.markSeen("r2")),
      (input) =>
        Effect.sync(() => {
          calls.push(input.args);
          return output("");
        }),
    );
    expect(calls).toEqual([["seen", "r2"]]);
    const error = yield* run(
      Effect.flatMap(AgentInbox.AgentInbox, (inbox) => inbox.pull("r2")),
      () => Effect.succeed(output("", 2)),
    ).pipe(Effect.flip);
    expect(error.operation).toBe("pull");
    expect(error.detail).toBe("agent-inbox is not configured");
  }),
);
