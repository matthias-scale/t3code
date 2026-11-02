import { ProviderDriverKind } from "@t3tools/contracts";
import { collectLimitPools, type LimitAccount } from "@t3tools/shared/usageLimits";
import { expect, it } from "vite-plus/test";

import { accountLimitWindow } from "./usageAccountWindow";

it("keeps each account's refill amount and reset time separate", () => {
  const now = Date.parse("2026-09-03T12:00:00Z");
  const accounts: LimitAccount[] = ["a", "b", "no-reset"].map((key, index) => ({
    key,
    driver: ProviderDriverKind.make("codex"),
    displayName: null,
    email: undefined,
    plan: undefined,
    accentColor: undefined,
    environments: [],
    sourceLabel: null,
    redeem: null,
    limits: {
      checkedAt: new Date(now).toISOString(),
      windows: [
        {
          id: "session",
          kind: "session",
          label: "Session",
          usedPercent: 20 + index * 10,
          ...(key === "no-reset"
            ? {}
            : { resetsAt: new Date(now + (index + 1) * 60_000).toISOString() }),
        },
      ],
    },
  }));
  const pool = collectLimitPools(accounts, now)[0]!.windows[0]!;
  expect(pool.resets).toHaveLength(2);
  for (const [index, account] of accounts.entries()) {
    const card = accountLimitWindow(pool, account.key);
    expect(card.members.map((member) => member.account.key)).toEqual([account.key]);
    expect(card.columns.map((column) => column.account.key)).toEqual([account.key]);
    expect(
      card.resets.map((reset) => [reset.member.account.key, reset.at, reset.restoresPercent]),
    ).toEqual(
      account.key === "no-reset"
        ? []
        : [[account.key, now + (index + 1) * 60_000, 20 + index * 10]],
    );
  }
  expect(pool.resets).toHaveLength(2);
});
