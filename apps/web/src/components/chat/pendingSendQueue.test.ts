import { describe, expect, it } from "vite-plus/test";
import { createPendingSendQueue, type PendingDelivery } from "./pendingSendQueue";

function setup() {
  const saved = new Map<string, PendingDelivery<string>>();
  const queue = createPendingSendQueue<string>({
    write: async (entry) => {
      saved.set(entry.id, entry);
    },
    remove: async (id) => {
      saved.delete(id);
    },
  });
  return { queue, saved };
}

describe("pending message delivery", () => {
  it("leaves later messages editable if readiness is lost during a dispatch", async () => {
    const { queue } = setup();
    await queue.add("1", "thread", "first");
    await queue.add("2", "thread", "second");
    let ready = true;
    await queue.flush(
      "thread",
      {},
      async () => {
        ready = false;
      },
      () => ready,
    );
    expect(queue.getSnapshot().map((entry) => entry.value)).toEqual(["second"]);
    expect(await queue.cancel("2")).toBe(true);
  });

  it("keeps send order when an earlier upload finishes after the next message is queued", async () => {
    const { queue } = setup();
    await queue.add("second", "thread", "second", "2026-10-22T12:00:02Z");
    await queue.add("first", "thread", "first", "2026-10-22T12:00:01Z");
    const delivered: string[] = [];
    await queue.flush("thread", {}, async (value) => {
      delivered.push(value);
    });
    expect(delivered).toEqual(["first", "second"]);
  });

  it("can cancel a waiting message while another request is in flight", async () => {
    const { queue } = setup();
    let finish!: () => void;
    const pending = new Promise<void>((resolve) => {
      finish = resolve;
    });
    await queue.add("first", "thread", "first");
    await queue.add("second", "thread", "second");
    const delivered: string[] = [];
    const flush = queue.flush("thread", {}, async (value) => {
      delivered.push(value);
      await pending;
    });
    expect(await queue.cancel("second")).toBe(true);
    finish();
    await flush;
    expect(delivered).toEqual(["first"]);
  });

  it("persists the prepared payload across an ambiguous response and reload", async () => {
    const { queue, saved } = setup();
    await queue.add("1", "thread", "local file");
    const committed = new Set<string>();
    let runs = 0;
    const server = async (payload: string) => {
      if (!committed.has(payload)) {
        committed.add(payload);
        runs++;
      }
    };
    await queue.flush("thread", {}, async () => {
      await queue.prepare("1", "stable-command:uploaded-attachment");
      await server("stable-command:uploaded-attachment");
      throw new Error("ack lost");
    });
    const restored = setup().queue;
    restored.restore([...saved.values()]);
    await restored.flush("thread", {}, server);
    expect(runs).toBe(1);
    expect(restored.getSnapshot()).toEqual([]);
  });

  it("keeps an accepted message if saving or dispatching fails", async () => {
    const { queue, saved } = setup();
    await queue.add("1", "thread", "keep me");
    await queue.flush("thread", {}, async () => {
      throw new Error("disconnected");
    });
    expect(queue.getSnapshot()[0]?.error).toBe("disconnected");
    expect(saved.get("1")?.value).toBe("keep me");
  });

  it("does not add the same accepted command twice", async () => {
    const { queue } = setup();
    await Promise.all([queue.add("1", "thread", "once"), queue.add("1", "thread", "once")]);
    expect(queue.getSnapshot()).toHaveLength(1);
  });

  it("accepts while disconnected or busy and flushes in order once ready", async () => {
    const { queue } = setup();
    const delivered: string[] = [];
    const deliver = async (value: string) => {
      delivered.push(value);
    };
    await queue.add("1", "thread", "first");
    await queue.add("2", "thread", "second");
    await queue.flush("thread", null, deliver);
    expect(delivered).toEqual([]);
    expect(queue.getSnapshot()).toHaveLength(2);
    await queue.flush("thread", {}, deliver);
    expect(delivered).toEqual(["first", "second"]);
    expect(queue.getSnapshot()).toEqual([]);
  });

  it("serializes concurrent flushes and accepts another send in flight", async () => {
    const { queue } = setup();
    let finish!: () => void;
    const pending = new Promise<void>((resolve) => {
      finish = resolve;
    });
    const delivered: string[] = [];
    await queue.add("1", "thread", "first");
    const flush = queue.flush("thread", {}, async (value) => {
      delivered.push(value);
      await pending;
    });
    await queue.add("2", "thread", "second");
    await queue.flush("thread", {}, async () => {
      throw new Error("duplicate flush");
    });
    finish();
    await flush;
    expect(delivered).toEqual(["first", "second"]);
  });

  it("retries an ambiguous result with the same payload only after reconnect", async () => {
    const { queue, saved } = setup();
    await queue.add("command-1", "thread", "stable-command-and-message");
    const connection = {};
    let attempts = 0;
    const deliver = async () => {
      attempts++;
      throw new Error("socket closed after commit");
    };
    await queue.flush("thread", connection, deliver);
    await queue.flush("thread", connection, deliver);
    expect(attempts).toBe(1);
    expect(await queue.cancel("command-1")).toBe(false);
    expect(await queue.pause("command-1", true)).toBe(false);
    const restored = setup().queue;
    restored.restore([...saved.values()]);
    const delivered: string[] = [];
    await restored.flush("thread", {}, async (value) => {
      delivered.push(value);
    });
    expect(delivered).toEqual(["stable-command-and-message"]);
    expect(restored.getSnapshot()).toEqual([]);
  });

  it("edits or cancels messages before dispatch and holds an edit across readiness", async () => {
    const { queue } = setup();
    await queue.add("1", "thread", "original");
    expect(await queue.pause("1", true)).toBe(true);
    const delivered: string[] = [];
    await queue.flush("thread", {}, async (value) => {
      delivered.push(value);
    });
    expect(delivered).toEqual([]);
    expect(await queue.edit("1", "edited")).toBe(true);
    await queue.add("2", "thread", "cancel me");
    expect(await queue.cancel("2")).toBe(true);
    await queue.flush("thread", {}, async (value) => {
      delivered.push(value);
    });
    expect(delivered).toEqual(["edited"]);
  });
});
