/**
 * The agent-inbox section: its own group above the projects list, fed by the
 * shared agent-inbox records so every host shows the same list and badge.
 * Rows open the local thread when this host has it; otherwise they expand to
 * the request summary with an "Open here" action that pulls it onto this host.
 */
import type { AgentInboxItem, AgentInboxStatus, EnvironmentId, ThreadId } from "@t3tools/contracts";
import { ChevronDownIcon, MailboxIcon } from "lucide-react";
import { useCallback, useMemo, useState } from "react";

import { cn } from "~/lib/utils";
import { agentInboxMarkSeen, agentInboxPull } from "../../state/agentInbox";
import { useAtomCommand } from "../../state/use-atom-command";
import { buildAgentInboxView, resolveAgentInboxItemTarget } from "../Sidebar.logic";
import { Badge } from "../ui/badge";
import { Button } from "../ui/button";

export interface SidebarAgentInboxProps {
  environmentId: EnvironmentId;
  status: AgentInboxStatus | null;
  /** Thread ids that exist on this host, including ones hidden from the projects list. */
  localThreadIds: ReadonlySet<string>;
  onOpenThread: (threadId: ThreadId) => void;
  onPulled: () => void;
}

export function SidebarAgentInbox(props: SidebarAgentInboxProps) {
  const { environmentId, status, localThreadIds, onOpenThread, onPulled } = props;
  const [expanded, setExpanded] = useState(true);
  const [locallySeen, setLocallySeen] = useState<ReadonlySet<string>>(() => new Set());
  const [openSummaryId, setOpenSummaryId] = useState<string | null>(null);
  const [pullingId, setPullingId] = useState<string | null>(null);
  // Seen is best effort: the next poll corrects the badge either way.
  const markSeen = useAtomCommand(agentInboxMarkSeen, { reportFailure: false });
  const pull = useAtomCommand(agentInboxPull);
  const view = useMemo(() => buildAgentInboxView(status, locallySeen), [status, locallySeen]);

  const openItem = useCallback(
    (item: AgentInboxItem) => {
      if (!item.seen && !locallySeen.has(item.id)) {
        setLocallySeen((previous) => new Set(previous).add(item.id));
        void markSeen({ environmentId, input: { id: item.id } });
      }
      const target = resolveAgentInboxItemTarget(item, localThreadIds);
      if (target.kind === "thread") {
        setOpenSummaryId(null);
        onOpenThread(target.threadId);
        return;
      }
      setOpenSummaryId((current) => (current === item.id ? null : item.id));
    },
    [environmentId, localThreadIds, locallySeen, markSeen, onOpenThread],
  );

  const pullHere = useCallback(
    async (item: AgentInboxItem) => {
      setPullingId(item.id);
      try {
        await pull({ environmentId, input: { id: item.id } });
      } finally {
        setPullingId(null);
        onPulled();
      }
    },
    [environmentId, onPulled, pull],
  );

  if (view === null) return null;
  const items = [...view.unseen, ...view.seen];
  const unseenIds = new Set(view.unseen.map((item) => item.id));

  return (
    <section aria-label="Agent inbox" data-testid="sidebar-agent-inbox" className="mb-2">
      <button
        type="button"
        onClick={() => setExpanded((value) => !value)}
        aria-expanded={expanded}
        className="flex h-8 w-full cursor-pointer items-center gap-2 px-2.5 text-left text-xs font-medium text-sidebar-muted-foreground"
      >
        <MailboxIcon aria-hidden className="size-3.5 shrink-0" />
        <span className="shrink-0">Inbox</span>
        {view.unseenCount > 0 ? (
          <Badge size="sm" aria-label={`${view.unseenCount} unread`}>
            {view.unseenCount}
          </Badge>
        ) : null}
        <span aria-hidden className="h-px min-w-2 flex-1 bg-sidebar-border/60" />
        <ChevronDownIcon
          aria-hidden
          className={cn("size-3 shrink-0 transition-transform", expanded && "rotate-180")}
        />
      </button>
      {expanded ? (
        items.length === 0 ? (
          <p className="px-2.5 py-1 text-xs text-sidebar-muted-foreground/60">No open requests</p>
        ) : (
          <ul className="flex flex-col gap-px">
            {items.map((item) => {
              const unseen = unseenIds.has(item.id);
              return (
                <li key={item.id}>
                  <button
                    type="button"
                    onClick={() => openItem(item)}
                    className={cn(
                      "flex h-7 w-full cursor-pointer items-center gap-2 rounded-md px-2.5 text-left text-sm hover:bg-accent",
                      unseen ? "font-medium text-sidebar-foreground" : "text-sidebar-foreground/70",
                    )}
                  >
                    <span
                      aria-hidden
                      className={cn("size-1.5 shrink-0 rounded-full", unseen && "bg-primary")}
                    />
                    <span className="min-w-0 flex-1 truncate">{item.title}</span>
                    <span className="shrink-0 text-xs text-sidebar-muted-foreground/60">
                      {item.source}
                    </span>
                  </button>
                  {openSummaryId === item.id ? (
                    <div className="mx-2.5 mb-1 flex flex-col gap-1.5 rounded-md border border-sidebar-border/60 p-2 text-xs text-sidebar-foreground/80">
                      <p className="break-words">{item.title}</p>
                      <p className="text-sidebar-muted-foreground/70">
                        {item.state}
                        {item.t3Host ? ` · thread on ${item.t3Host}` : " · no thread yet"}
                      </p>
                      <div>
                        <Button
                          size="xs"
                          variant="outline"
                          disabled={pullingId !== null}
                          onClick={() => void pullHere(item)}
                        >
                          {pullingId === item.id ? "Opening…" : "Open here"}
                        </Button>
                      </div>
                    </div>
                  ) : null}
                </li>
              );
            })}
          </ul>
        )
      ) : null}
    </section>
  );
}
