export interface PendingDelivery<T> {
  readonly id: string;
  readonly key: string;
  readonly order: string;
  readonly value: T;
  readonly attempted: boolean;
  readonly paused: boolean;
  readonly error: string | null;
}

/** Buffers delivery until the existing server queue is reachable. */
export function createPendingSendQueue<T>(storage: {
  write: (entry: PendingDelivery<T>) => Promise<void>;
  remove: (id: string) => Promise<void>;
}) {
  let entries: ReadonlyArray<PendingDelivery<T>> = [];
  const listeners = new Set<() => void>();
  const running = new Set<string>();
  const locked = new Set<string>();
  const failedConnections = new Map<string, object>();
  const publish = (next: ReadonlyArray<PendingDelivery<T>>) => {
    entries = next;
    for (const listener of listeners) listener();
  };
  const replace = async (entry: PendingDelivery<T>) => {
    await storage.write(entry);
    publish(entries.map((current) => (current.id === entry.id ? entry : current)));
  };
  return {
    getSnapshot: () => entries,
    wake: () => publish([...entries]),
    subscribe: (listener: () => void) => {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
    restore(saved: ReadonlyArray<PendingDelivery<T>>) {
      publish(saved.map((entry) => ({ ...entry, paused: false })));
    },
    async add(id: string, key: string, value: T, order = "") {
      if (entries.some((entry) => entry.id === id) || locked.has(id)) return;
      const entry = { id, key, order, value, attempted: false, paused: false, error: null };
      locked.add(id);
      try {
        await storage.write(entry);
      } finally {
        locked.delete(id);
      }
      publish([...entries, entry].toSorted((a, b) => a.order.localeCompare(b.order)));
    },
    async pause(id: string, paused: boolean) {
      const entry = entries.find((entry) => entry.id === id);
      if (!entry || entry.attempted || locked.has(entry.id)) return false;
      locked.add(id);
      try {
        await storage.write({ ...entry, paused });
      } finally {
        locked.delete(id);
      }
      publish(entries.map((current) => (current.id === id ? { ...entry, paused } : current)));
      return true;
    },
    async edit(id: string, value: T) {
      const entry = entries.find((entry) => entry.id === id);
      if (!entry || entry.attempted || !entry.paused || locked.has(id)) return false;
      locked.add(id);
      try {
        await storage.write({ ...entry, value, paused: false });
      } finally {
        locked.delete(id);
      }
      publish(
        entries.map((current) =>
          current.id === id ? { ...entry, value, paused: false } : current,
        ),
      );
      return true;
    },
    async cancel(id: string) {
      const entry = entries.find((entry) => entry.id === id);
      if (!entry || entry.attempted || locked.has(entry.id)) return false;
      locked.add(id);
      try {
        await storage.remove(id);
      } finally {
        locked.delete(id);
      }
      publish(entries.filter((entry) => entry.id !== id));
      return true;
    },
    async prepare(id: string, value: T) {
      const entry = entries.find((entry) => entry.id === id);
      if (!entry) throw new Error("Queued message is no longer available.");
      await replace({ ...entry, value });
    },
    async flush(
      key: string,
      connection: object | null,
      deliver: (value: T) => Promise<void>,
      isReady = () => true,
    ) {
      if (!connection || running.has(key)) return;
      running.add(key);
      let attempted = false;
      try {
        while (true) {
          const entry = entries.find((entry) => entry.key === key);
          if (
            !entry ||
            !isReady() ||
            locked.has(entry.id) ||
            entry.paused ||
            failedConnections.get(entry.id) === connection
          )
            return;
          attempted = true;
          locked.add(entry.id);
          const sending = { ...entry, attempted: true, error: null };
          try {
            await replace(sending);
            await deliver(entry.value);
            await storage.remove(entry.id);
            locked.delete(entry.id);
            failedConnections.delete(entry.id);
            publish(entries.filter((current) => current.id !== entry.id));
          } catch (error) {
            locked.delete(entry.id);
            failedConnections.set(entry.id, connection);
            await replace({
              ...(entries.find((current) => current.id === entry.id) ?? sending),
              error: error instanceof Error ? error.message : "Waiting to reconnect",
            });
            return;
          }
        }
      } finally {
        for (const entry of entries) if (entry.key === key) locked.delete(entry.id);
        running.delete(key);
        if (attempted) publish([...entries]);
      }
    },
  };
}
