import { filterAutoSettleSettingsPatchForCapabilities } from "@t3tools/client-runtime/state/shared-settings";
import type {
  EnvironmentId,
  ExecutionEnvironmentCapabilities,
  ServerSettings,
} from "@t3tools/contracts";

export type AutoSettleSettings = Pick<
  ServerSettings,
  "sidebarAutoSettleAfterHours" | "sidebarAutoSettleAfterDays" | "sidebarAutoSettleOnMerge"
>;

interface AutoSettleSyncTarget {
  readonly environmentId: EnvironmentId;
  readonly label: string;
  readonly settings: AutoSettleSettings | null;
  readonly capabilities?: Pick<ExecutionEnvironmentCapabilities, "threadAutoSettlementHours">;
}

interface AutoSettleWriteTarget {
  readonly environmentId: EnvironmentId;
  readonly capabilities?: Pick<ExecutionEnvironmentCapabilities, "threadAutoSettlementHours">;
}

export function planAutoSettleSettingsWrites(
  patch: Partial<AutoSettleSettings>,
  targets: readonly AutoSettleWriteTarget[],
) {
  return targets.flatMap((target) => {
    const filtered = filterAutoSettleSettingsPatchForCapabilities(patch, target.capabilities);
    return Object.keys(filtered).length === 0
      ? []
      : [{ environmentId: target.environmentId, patch: filtered }];
  });
}

/** Receives connected, capable targets. Applying these defaults must preserve other settings. */
export function planAutoSettleSettingsSync(
  reference: {
    readonly environmentId: EnvironmentId;
    readonly settings: AutoSettleSettings;
    readonly capabilities?: Pick<ExecutionEnvironmentCapabilities, "threadAutoSettlementHours">;
  },
  targets: readonly AutoSettleSyncTarget[],
) {
  const supportsHours = reference.capabilities?.threadAutoSettlementHours === true;
  const patch: Partial<AutoSettleSettings> = {
    ...(supportsHours
      ? { sidebarAutoSettleAfterHours: reference.settings.sidebarAutoSettleAfterHours }
      : reference.settings.sidebarAutoSettleAfterDays === undefined
        ? {}
        : { sidebarAutoSettleAfterDays: reference.settings.sidebarAutoSettleAfterDays }),
    sidebarAutoSettleOnMerge: reference.settings.sidebarAutoSettleOnMerge,
  };
  const mismatches = targets.filter(
    (target) =>
      target.environmentId !== reference.environmentId &&
      target.settings !== null &&
      (((target.capabilities?.threadAutoSettlementHours === true) === supportsHours &&
        (supportsHours
          ? target.settings.sidebarAutoSettleAfterHours !==
            reference.settings.sidebarAutoSettleAfterHours
          : target.settings.sidebarAutoSettleAfterDays !==
            reference.settings.sidebarAutoSettleAfterDays)) ||
        target.settings.sidebarAutoSettleOnMerge !== patch.sidebarAutoSettleOnMerge),
  );
  return { patch, mismatches };
}
