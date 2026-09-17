import { adaptServerSettingsPatchForCapabilities } from "@t3tools/client-runtime/state/shared-settings";
import type {
  EnvironmentId,
  ExecutionEnvironmentCapabilities,
  ServerSettings,
} from "@t3tools/contracts";

export type AutoSettleSettings = Pick<
  ServerSettings,
  "sidebarAutoSettleAfterHours" | "sidebarAutoSettleOnMerge"
>;

interface AutoSettleSyncTarget {
  readonly environmentId: EnvironmentId;
  readonly label: string;
  readonly settings: AutoSettleSettings | null;
}

interface AutoSettleWriteTarget {
  readonly environmentId: EnvironmentId;
  readonly capabilities?: Pick<ExecutionEnvironmentCapabilities, "threadAutoSettlementHours">;
}

export function planAutoSettleSettingsWrites(
  patch: Partial<AutoSettleSettings>,
  targets: readonly AutoSettleWriteTarget[],
) {
  return targets.map((target) => ({
    environmentId: target.environmentId,
    patch: adaptServerSettingsPatchForCapabilities(patch, target.capabilities),
  }));
}

/** Receives connected, capable targets. Applying these defaults must preserve other settings. */
export function planAutoSettleSettingsSync(
  reference: { readonly environmentId: EnvironmentId; readonly settings: AutoSettleSettings },
  targets: readonly AutoSettleSyncTarget[],
) {
  const patch: AutoSettleSettings = {
    sidebarAutoSettleAfterHours: reference.settings.sidebarAutoSettleAfterHours,
    sidebarAutoSettleOnMerge: reference.settings.sidebarAutoSettleOnMerge,
  };
  const mismatches = targets.filter(
    (target) =>
      target.environmentId !== reference.environmentId &&
      target.settings !== null &&
      (target.settings.sidebarAutoSettleAfterHours !== patch.sidebarAutoSettleAfterHours ||
        target.settings.sidebarAutoSettleOnMerge !== patch.sidebarAutoSettleOnMerge),
  );
  return { patch, mismatches };
}
