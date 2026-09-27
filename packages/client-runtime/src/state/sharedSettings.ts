/**
 * Shared server settings.
 *
 * Every server keeps its own `settings.json`, but some keys are user
 * preferences that only live on the server because the server has to act on
 * them (auto-settlement runs with no client attached). A user does not want
 * those to differ per machine. Clients write these keys to every shared-settings
 * sync target, and warn when another target still holds a different value so
 * the user can push their current value out.
 */
import {
  type EnvironmentId,
  type ExecutionEnvironmentCapabilities,
  type ProjectId,
  type ServerSettings,
  type ServerSettingsPatch,
} from "@t3tools/contracts";
import { isModelSelectionProviderEnabled } from "@t3tools/shared/serverSettings";
import * as Equal from "effect/Equal";
import * as Struct from "effect/Struct";

import type { EnvironmentConnectionPhase } from "../connection/presentation.ts";

/** Server keys that hold a user preference rather than machine config. */
const SHARED_SERVER_SETTING_KEYS = [
  "continueThreadsAfterServerUpdate",
  "sidebarAutoSettleAfterHours",
  "sidebarAutoSettleAfterDays",
  "sidebarAutoSettleOnMerge",
  "newWorktreesStartFromOrigin",
  "sourceControlWritingStyle",
  "textGenerationModelSelection",
] as const satisfies ReadonlyArray<keyof ServerSettings & keyof ServerSettingsPatch>;

export type SharedServerSettingKey = (typeof SHARED_SERVER_SETTING_KEYS)[number];

const SHARED_KEY_SET = new Set<string>(SHARED_SERVER_SETTING_KEYS);

type SharedSettingsCapabilities = Pick<
  ExecutionEnvironmentCapabilities,
  "threadAutoSettlementHours" | "threadRestartContinuation"
>;

export function selectAutoSettleThreshold(
  settings: Pick<ServerSettings, "sidebarAutoSettleAfterHours" | "sidebarAutoSettleAfterDays">,
  capabilities?: Pick<ExecutionEnvironmentCapabilities, "threadAutoSettlementHours">,
):
  | { readonly key: "sidebarAutoSettleAfterHours"; readonly value: number | null }
  | { readonly key: "sidebarAutoSettleAfterDays"; readonly value: number | null | undefined } {
  return capabilities?.threadAutoSettlementHours === true
    ? { key: "sidebarAutoSettleAfterHours", value: settings.sidebarAutoSettleAfterHours }
    : { key: "sidebarAutoSettleAfterDays", value: settings.sidebarAutoSettleAfterDays };
}

/** Keep only the auto-settlement keys the target server advertises. */
export function filterAutoSettleSettingsPatchForCapabilities(
  patch: ServerSettingsPatch,
  capabilities?: { readonly threadAutoSettlementHours?: boolean | undefined },
): ServerSettingsPatch {
  const { sidebarAutoSettleAfterHours, projectSettingsOverrides, ...rest } = patch;
  const { sidebarAutoSettleAfterDays, ...settings } = rest;
  const supportsHours = capabilities?.threadAutoSettlementHours === true;
  type ProjectOverridePatch = NonNullable<
    ServerSettingsPatch["projectSettingsOverrides"]
  >[ProjectId];
  const filteredProjectSettingsOverrides: Record<ProjectId, ProjectOverridePatch> = {};
  for (const [projectId, entry] of Object.entries(projectSettingsOverrides ?? {})) {
    const id = projectId as ProjectId;
    if (entry === null) {
      filteredProjectSettingsOverrides[id] = null;
      continue;
    }
    const {
      sidebarAutoSettleAfterHours: entryHours,
      sidebarAutoSettleAfterDays: entryDays,
      ...current
    } = entry;
    const filtered = supportsHours
      ? {
          ...current,
          ...(entryHours === undefined ? {} : { sidebarAutoSettleAfterHours: entryHours }),
        }
      : {
          ...current,
          ...(entryDays === undefined ? {} : { sidebarAutoSettleAfterDays: entryDays }),
        };
    if (Object.keys(filtered).length > 0) {
      filteredProjectSettingsOverrides[id] = filtered;
    }
  }
  const hasProjectSettingsOverrides =
    projectSettingsOverrides !== undefined &&
    Object.keys(filteredProjectSettingsOverrides).length > 0;

  return {
    ...settings,
    ...(supportsHours
      ? sidebarAutoSettleAfterHours === undefined
        ? {}
        : { sidebarAutoSettleAfterHours }
      : sidebarAutoSettleAfterDays === undefined
        ? {}
        : { sidebarAutoSettleAfterDays }),
    ...(hasProjectSettingsOverrides
      ? { projectSettingsOverrides: filteredProjectSettingsOverrides }
      : {}),
  };
}

/** Split a server patch into the keys every environment should receive and the primary-only rest. */
export function splitSharedServerPatch(patch: ServerSettingsPatch): {
  sharedPatch: ServerSettingsPatch;
  localPatch: ServerSettingsPatch;
} {
  const sharedPatch: Record<string, unknown> = {};
  const localPatch: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(patch)) {
    if (SHARED_KEY_SET.has(key)) {
      sharedPatch[key] = value;
    } else {
      localPatch[key] = value;
    }
  }
  return {
    sharedPatch: sharedPatch as ServerSettingsPatch,
    localPatch: localPatch as ServerSettingsPatch,
  };
}

function filterSupportedSharedServerPatch(
  patch: ServerSettingsPatch,
  capabilities: SharedSettingsCapabilities | undefined,
  settings?: ServerSettings,
  sourceSettings = settings,
  targetIsSource = false,
): ServerSettingsPatch {
  const instanceId =
    patch.textGenerationModelSelection?.instanceId ??
    sourceSettings?.textGenerationModelSelection.instanceId;
  if (
    !targetIsSource &&
    patch.textGenerationModelSelection &&
    (!settings ||
      (instanceId !== undefined &&
        (sourceSettings?.providerInstances[instanceId]?.driver ?? instanceId) !==
          (settings.providerInstances[instanceId]?.driver ?? instanceId)) ||
      !isModelSelectionProviderEnabled(settings, {
        ...settings.textGenerationModelSelection,
        ...patch.textGenerationModelSelection,
      }))
  ) {
    patch = Struct.omit(patch, ["textGenerationModelSelection"]);
  }
  return capabilities?.threadRestartContinuation === true
    ? patch
    : Struct.omit(patch, ["continueThreadsAfterServerUpdate"]);
}

/** Filter unsupported preferences and encode the patch for the target server. */
export function filterSharedServerPatch(
  patch: ServerSettingsPatch,
  capabilities: SharedSettingsCapabilities | undefined,
  settings?: ServerSettings,
  sourceSettings = settings,
  targetIsSource = false,
): ServerSettingsPatch {
  return filterAutoSettleSettingsPatchForCapabilities(
    filterSupportedSharedServerPatch(patch, capabilities, settings, sourceSettings, targetIsSource),
    capabilities,
  );
}

/** The shared subset supported by one environment. */
export function pickSharedServerSettings(
  settings: ServerSettings,
  capabilities?: SharedSettingsCapabilities,
): ServerSettingsPatch {
  return filterAutoSettleSettingsPatchForCapabilities(
    filterSupportedSharedServerPatch(
      Struct.pick(settings, SHARED_SERVER_SETTING_KEYS),
      capabilities,
      settings,
    ),
    capabilities,
  );
}

/**
 * Whether an environment can participate in shared-settings sync right now.
 * Auto-settlement establishes baseline support; newer preferences are filtered separately.
 */
export function supportsSharedSettingsSync(environment: {
  readonly connection: { readonly phase: EnvironmentConnectionPhase };
  readonly serverConfig: {
    readonly environment: {
      readonly capabilities: Pick<ExecutionEnvironmentCapabilities, "threadAutoSettlement">;
    };
  } | null;
}): boolean {
  return (
    environment.connection.phase === "connected" &&
    environment.serverConfig?.environment.capabilities.threadAutoSettlement === true
  );
}

export interface SharedSettingsEnvironment {
  readonly environmentId: EnvironmentId;
  readonly label: string;
  readonly syncEligible: boolean;
  readonly settings: ServerSettings | null;
  readonly capabilities?: SharedSettingsCapabilities | undefined;
}

/**
 * Shared-settings sync targets whose values differ from the primary
 * environment's. Other environments are skipped: nothing can be read from or
 * written to them, or their server lacks baseline shared-settings support. With no
 * primary settings loaded there is nothing to compare against, so nothing is
 * reported. Callers must pass the real loaded settings, never a default
 * fallback, or "apply to all" would push defaults over real values.
 */
export function findSharedSettingsMismatches(input: {
  readonly primaryEnvironmentId: EnvironmentId | null;
  readonly primarySettings: ServerSettings | null;
  readonly primaryCapabilities?: SharedSettingsCapabilities | undefined;
  readonly environments: ReadonlyArray<SharedSettingsEnvironment>;
}): ReadonlyArray<{ readonly environmentId: EnvironmentId; readonly label: string }> {
  if (input.primaryEnvironmentId === null || input.primarySettings === null) {
    return [];
  }
  const primarySettings = pickSharedServerSettings(
    input.primarySettings,
    input.primaryCapabilities,
  );
  return input.environments.flatMap((environment) => {
    if (
      environment.environmentId === input.primaryEnvironmentId ||
      !environment.syncEligible ||
      environment.settings === null
    ) {
      return [];
    }
    const expected = filterSharedServerPatch(
      primarySettings,
      environment.capabilities,
      environment.settings,
      input.primarySettings ?? undefined,
    );
    const targetSettings = pickSharedServerSettings(environment.settings, environment.capabilities);
    let actual = Object.fromEntries(
      Object.keys(expected).map((key) => [key, targetSettings[key as keyof ServerSettingsPatch]]),
    ) as ServerSettingsPatch;
    if (!expected.textGenerationModelSelection) {
      actual = Struct.omit(actual, ["textGenerationModelSelection"]);
    }
    return Equal.equals(actual, expected)
      ? []
      : [{ environmentId: environment.environmentId, label: environment.label }];
  });
}
