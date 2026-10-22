import { appAtomRegistry } from "../../rpc/atomRegistry";
import { useEffect, useRef } from "react";
import * as Option from "effect/Option";
import { AuthOrchestrationOperateScope, CommandId, type EnvironmentId } from "@t3tools/contracts";
import { squashAtomCommandFailure } from "@t3tools/client-runtime/state/runtime";
import { useEnvironmentPresentation, environmentPresentations } from "../../state/presentation";
import {
  readEnvironmentScope,
  readPreparedConnection,
  usePreparedConnection,
} from "../../state/session";
import { threadEnvironment, environmentThreadShells } from "../../state/threads";
import { useOrchestrationCommand } from "../../state/use-orchestration-command";
import {
  startAttachmentUpload,
  retryAttachmentUpload,
  readAttachmentUpload,
  releaseDraftAttachments,
} from "../../lib/attachmentUploadQueue";
import { buildMessageContext } from "../../lib/composerContextRecords";
import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import { boundAttachmentIds, withMessageContext } from "../ChatView.logic";
import { draftSendState, prepareTurnAttachments } from "./chatViewSend";
import { hydratePendingSends, pendingSends, usePendingSends } from "./pendingSends";
import { toastManager } from "../ui/toast";

function EnvironmentPendingSends({ environmentId }: { environmentId: EnvironmentId }) {
  const entries = usePendingSends();
  const createdThreadIds = useRef(new Set<string>());
  const { presentation } = useEnvironmentPresentation(environmentId);
  const connection = usePreparedConnection(environmentId);
  const setRuntimeMode = useOrchestrationCommand(threadEnvironment.setRuntimeMode, {
    reportFailure: false,
  });
  const setInteractionMode = useOrchestrationCommand(threadEnvironment.setInteractionMode, {
    reportFailure: false,
  });
  const start = useOrchestrationCommand(threadEnvironment.startTurn, { reportFailure: false });
  useEffect(() => {
    if (
      presentation?.connection.phase !== "connected" ||
      !readEnvironmentScope(environmentId, AuthOrchestrationOperateScope)
    )
      return;
    const prepared = Option.getOrNull(connection);
    if (!prepared) return;
    const keys = new Set(
      entries
        .filter((entry) => entry.value.environmentId === environmentId)
        .map((entry) => entry.key),
    );
    for (const key of keys) {
      if (draftSendState(key).sendInFlight.current) continue;
      void pendingSends
        .flush(
          key,
          prepared,
          async (value) => {
            const { input, snapshot } = value;
            if (value.prepared) {
              const result = await start({ environmentId, input });
              if (result._tag === "Failure") throw squashAtomCommandFailure(result);
              if (input.bootstrap?.createThread) createdThreadIds.current.add(input.threadId);
              return;
            }
            const uploads =
              presentation.serverConfig?.environment.capabilities.attachmentUploads === true;
            if (uploads) {
              for (const image of snapshot.attachments) {
                const upload =
                  readAttachmentUpload(image.id)?.status === "failed"
                    ? retryAttachmentUpload
                    : startAttachmentUpload;
                upload({
                  environmentId,
                  image,
                  draftTarget: scopeThreadRef(environmentId, input.threadId),
                });
              }
            }
            const existing =
              createdThreadIds.current.has(input.threadId) ||
              appAtomRegistry.get(
                environmentThreadShells.threadShellAtom(
                  scopeThreadRef(environmentId, input.threadId),
                ),
              );
            if (!input.bootstrap?.createThread || existing) {
              const runtime = await setRuntimeMode({
                environmentId,
                input: {
                  threadId: input.threadId,
                  commandId: CommandId.make(`${input.commandId}:runtime`),
                  runtimeMode: input.runtimeMode,
                },
              });
              if (runtime._tag === "Failure") throw squashAtomCommandFailure(runtime);
              const interaction = await setInteractionMode({
                environmentId,
                input: {
                  threadId: input.threadId,
                  commandId: CommandId.make(`${input.commandId}:interaction`),
                  interactionMode: input.interactionMode,
                },
              });
              if (interaction._tag === "Failure") throw squashAtomCommandFailure(interaction);
            }
            const attachments = await prepareTurnAttachments({
              environmentId,
              attachments: snapshot.attachments,
              usesAttachmentUploads: uploads,
            });
            const context = buildMessageContext({
              terminalContexts: snapshot.terminalContexts,
              reviewComments: snapshot.reviewComments,
              previewAnnotations: snapshot.previewAnnotations,
              threadContexts: snapshot.threadContexts,
              attachments: snapshot.attachments.map((attachment, index) => ({
                attachment,
                attachmentId:
                  boundAttachmentIds(attachments, snapshot.attachments)[index] ?? attachment.id,
              })),
            });
            const { bootstrap, ...turnInput } = input;
            const queuedBootstrap = existing && bootstrap?.createThread ? undefined : bootstrap;
            const readyValue = {
              ...value,
              prepared: true,
              input: {
                ...turnInput,
                ...(queuedBootstrap ? { bootstrap: queuedBootstrap } : {}),
                message: {
                  ...input.message,
                  attachments,
                  ...withMessageContext({
                    text: input.message.text,
                    context,
                    supportsInlineMessageContext:
                      presentation.serverConfig?.environment.capabilities.inlineMessageContext ===
                      true,
                  }),
                },
              },
            };
            await pendingSends.prepare(input.message.messageId, readyValue);
            const result = await start({ environmentId, input: readyValue.input });
            if (result._tag === "Failure") throw squashAtomCommandFailure(result);
            if (bootstrap?.createThread) createdThreadIds.current.add(input.threadId);
            releaseDraftAttachments(snapshot.attachments);
          },
          () =>
            readPreparedConnection(environmentId) === prepared &&
            appAtomRegistry.get(environmentPresentations.presentationAtom(environmentId))
              ?.connection.phase === "connected",
        )
        .catch((error: unknown) => {
          toastManager.add({
            type: "error",
            title: "Could not save queued message",
            description: error instanceof Error ? error.message : "Local storage is unavailable.",
          });
        });
    }
  }, [connection, entries, environmentId, presentation, start, setRuntimeMode, setInteractionMode]);
  return null;
}

export function PendingSendCoordinator() {
  const entries = usePendingSends();
  useEffect(() => {
    void hydratePendingSends().catch((error: unknown) => {
      toastManager.add({
        type: "error",
        title: "Could not restore queued messages",
        description: error instanceof Error ? error.message : "Local storage is unavailable.",
      });
    });
  }, []);
  return [...new Set(entries.map((entry) => entry.value.environmentId))].map((environmentId) => (
    <EnvironmentPendingSends key={environmentId} environmentId={environmentId} />
  ));
}
