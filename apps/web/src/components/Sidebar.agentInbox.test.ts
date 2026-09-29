import { describe, expect, it } from "vite-plus/test";
import {
  EnvironmentId,
  ThreadId,
  type AgentInboxItem,
  type AgentInboxStatus,
} from "@t3tools/contracts";

import {
  agentInboxThreadIds,
  buildAgentInboxView,
  excludeAgentInboxThreads,
  resolveAgentInboxItemTarget,
} from "./Sidebar.logic";

const item = (id: string, overrides: Partial<AgentInboxItem> = {}): AgentInboxItem => ({
  id,
  title: `Request ${id}`,
  state: "ready",
  source: "box",
  createdAt: "2026-09-29T09:00:00Z",
  seen: false,
  seenAt: null,
  seenByHost: null,
  t3ThreadId: null,
  t3Host: null,
  ...overrides,
});

const status = (items: AgentInboxItem[], available = true): AgentInboxStatus => ({
  available,
  host: "ub2",
  items,
  unseenCount: items.filter((entry) => !entry.seen).length,
});

describe("buildAgentInboxView", () => {
  it("hides the section until the query answers or when agent-inbox is unavailable", () => {
    expect(buildAgentInboxView(null, new Set())).toBeNull();
    expect(buildAgentInboxView(status([item("a")], false), new Set())).toBeNull();
  });

  it("groups unseen before seen, newest first in each group", () => {
    const view = buildAgentInboxView(
      status([
        item("old", { createdAt: "2026-09-27T00:00:00Z" }),
        item("seen-new", { createdAt: "2026-09-29T12:00:00Z", seen: true }),
        item("new", { createdAt: "2026-09-29T10:00:00Z" }),
        item("undated", { createdAt: null }),
      ]),
      new Set(),
    );
    expect(view?.unseen.map((entry) => entry.id)).toEqual(["new", "old", "undated"]);
    expect(view?.seen.map((entry) => entry.id)).toEqual(["seen-new"]);
    expect(view?.unseenCount).toBe(3);
  });

  it("counts a new request and clears an item opened here before the next poll", () => {
    const first = status([item("a")]);
    expect(buildAgentInboxView(first, new Set())?.unseenCount).toBe(1);
    const arrived = status([item("b", { createdAt: "2026-09-29T11:00:00Z" }), item("a")]);
    expect(buildAgentInboxView(arrived, new Set())?.unseenCount).toBe(2);
    expect(buildAgentInboxView(arrived, new Set(["a"]))?.unseenCount).toBe(1);
  });
});

describe("agent inbox threads", () => {
  const env = EnvironmentId.make("env-primary");
  const other = EnvironmentId.make("env-other");
  const inbox = status([item("a", { t3ThreadId: "t-inbox", t3Host: "ub1" }), item("b")]);

  it("moves inbox threads out of the primary environment's project list only", () => {
    const ids = agentInboxThreadIds(inbox);
    expect([...ids]).toEqual(["t-inbox"]);
    const threads = [
      { id: ThreadId.make("t-inbox"), environmentId: env },
      { id: ThreadId.make("t-work"), environmentId: env },
      { id: ThreadId.make("t-inbox"), environmentId: other },
    ];
    expect(excludeAgentInboxThreads(threads, env, ids)).toEqual([threads[1], threads[2]]);
    expect(excludeAgentInboxThreads(threads, null, ids)).toBe(threads);
    expect(agentInboxThreadIds(status(inbox.items.slice(), false)).size).toBe(0);
  });

  it("opens a local thread, otherwise the summary with Open here", () => {
    const [withThread, withoutThread] = inbox.items as [AgentInboxItem, AgentInboxItem];
    expect(resolveAgentInboxItemTarget(withThread, new Set(["t-inbox"]))).toEqual({
      kind: "thread",
      threadId: "t-inbox",
    });
    expect(resolveAgentInboxItemTarget(withThread, new Set())).toEqual({ kind: "summary" });
    expect(resolveAgentInboxItemTarget(withoutThread, new Set(["t-inbox"]))).toEqual({
      kind: "summary",
    });
  });
});
