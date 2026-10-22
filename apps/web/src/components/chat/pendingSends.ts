import type { EnvironmentId } from "@t3tools/contracts";
import type { StartThreadTurnInput } from "@t3tools/client-runtime/operations";
import { useSyncExternalStore } from "react";
import type { ComposerSendSnapshot } from "../ChatView.logic";
import { createPendingSendQueue, type PendingDelivery } from "./pendingSendQueue";

export interface PendingSend {
  readonly environmentId: EnvironmentId;
  readonly prepared?: boolean;
  readonly input: StartThreadTurnInput;
  readonly snapshot: ComposerSendSnapshot;
}

// IndexedDB preserves File objects; JSON draft storage would silently drop attachments.
async function database() {
  return new Promise<IDBDatabase>((resolve, reject) => {
    const request = indexedDB.open("t3code:pending-sends", 1);
    request.addEventListener("upgradeneeded", () => {
      request.result.createObjectStore("messages", { keyPath: "id" });
    });
    request.addEventListener("success", () => resolve(request.result));
    request.addEventListener("error", () => reject(request.error));
  });
}
async function transact<A>(
  mode: IDBTransactionMode,
  run: (store: IDBObjectStore) => IDBRequest<A>,
) {
  const db = await database();
  try {
    return await new Promise<A>((resolve, reject) => {
      const tx = db.transaction("messages", mode);
      const request = run(tx.objectStore("messages"));
      tx.addEventListener("complete", () => resolve(request.result));
      tx.addEventListener("abort", () => reject(tx.error));
      tx.addEventListener("error", () => reject(tx.error));
    });
  } finally {
    db.close();
  }
}

export const pendingSends = createPendingSendQueue<PendingSend>({
  write: (entry) => transact("readwrite", (store) => store.put(entry)).then(() => {}),
  remove: (id) => transact("readwrite", (store) => store.delete(id)),
});
let hydration: Promise<void> | undefined;
export function hydratePendingSends() {
  return (hydration ??= transact("readonly", (store) => store.getAll())
    .then((saved: PendingDelivery<PendingSend>[]) => {
      for (const entry of saved) {
        for (const image of entry.value.snapshot.images)
          image.previewUrl = URL.createObjectURL(image.file);
      }
      pendingSends.restore(
        saved.toSorted((a, b) =>
          (a.value.input.createdAt ?? "").localeCompare(b.value.input.createdAt ?? ""),
        ),
      );
    })
    .catch((error: unknown) => {
      hydration = undefined;
      throw error;
    }));
}
export function usePendingSends() {
  return useSyncExternalStore(pendingSends.subscribe, pendingSends.getSnapshot);
}
