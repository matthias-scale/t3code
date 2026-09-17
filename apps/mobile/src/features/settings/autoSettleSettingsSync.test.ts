import { DEFAULT_SERVER_SETTINGS, EnvironmentId } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { planAutoSettleSettingsSync, planAutoSettleSettingsWrites } from "./autoSettleSettingsSync";

const reference = {
  environmentId: EnvironmentId.make("reference"),
  settings: {
    ...DEFAULT_SERVER_SETTINGS,
    sidebarAutoSettleAfterHours: 7,
    sidebarAutoSettleOnMerge: true,
    newWorktreesStartFromOrigin: false,
    continueThreadsAfterServerUpdate: false,
  },
};

describe("auto-settle settings sync", () => {
  it("writes legacy days and current hours per target", () => {
    const legacyId = EnvironmentId.make("legacy");
    const currentId = EnvironmentId.make("current");

    expect(
      planAutoSettleSettingsWrites({ sidebarAutoSettleAfterHours: 25 }, [
        { environmentId: legacyId, capabilities: {} },
        {
          environmentId: currentId,
          capabilities: { threadAutoSettlementHours: true },
        },
      ]),
    ).toEqual([
      { environmentId: legacyId, patch: { sidebarAutoSettleAfterDays: 2 } },
      { environmentId: currentId, patch: { sidebarAutoSettleAfterHours: 25 } },
    ]);
  });

  it("preserves a disabled inactivity threshold for legacy targets", () => {
    expect(
      planAutoSettleSettingsWrites({ sidebarAutoSettleAfterHours: null }, [
        { environmentId: EnvironmentId.make("legacy") },
      ])[0]?.patch,
    ).toEqual({ sidebarAutoSettleAfterDays: null });
  });

  it("ignores differences in independently configured environment settings", () => {
    const target = {
      environmentId: EnvironmentId.make("remote"),
      label: "Remote",
      settings: {
        ...reference.settings,
        newWorktreesStartFromOrigin: true,
        continueThreadsAfterServerUpdate: true,
        sourceControlWritingStyle: {
          ...reference.settings.sourceControlWritingStyle,
          customInstructions: "Keep this environment's writing instructions.",
        },
      },
    };

    const plan = planAutoSettleSettingsSync(reference, [target]);

    expect(plan.mismatches).toEqual([]);
    expect(plan.patch).toEqual({
      sidebarAutoSettleAfterHours: 7,
      sidebarAutoSettleOnMerge: true,
    });
  });

  it("applies only auto-settle defaults when another environment differs", () => {
    const target = {
      environmentId: EnvironmentId.make("remote"),
      label: "Remote",
      settings: {
        ...reference.settings,
        sidebarAutoSettleAfterHours: null,
        sidebarAutoSettleOnMerge: false,
        newWorktreesStartFromOrigin: true,
        continueThreadsAfterServerUpdate: true,
        sourceControlWritingStyle: {
          ...reference.settings.sourceControlWritingStyle,
          customInstructions: "Preserve these instructions.",
        },
      },
    };

    const plan = planAutoSettleSettingsSync(reference, [target]);
    const updated = { ...target.settings, ...plan.patch };

    expect(plan.mismatches).toEqual([target]);
    expect(updated.sidebarAutoSettleAfterHours).toBe(7);
    expect(updated.sidebarAutoSettleOnMerge).toBe(true);
    expect(updated.newWorktreesStartFromOrigin).toBe(true);
    expect(updated.continueThreadsAfterServerUpdate).toBe(true);
    expect(updated.sourceControlWritingStyle).toEqual(target.settings.sourceControlWritingStyle);
  });

  it("does not compare the reference or a target without loaded settings", () => {
    const plan = planAutoSettleSettingsSync(reference, [
      { ...reference, label: "Reference" },
      { environmentId: EnvironmentId.make("loading"), label: "Loading", settings: null },
    ]);

    expect(plan.mismatches).toEqual([]);
  });
});
