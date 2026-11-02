import type { LimitPoolWindow } from "@t3tools/shared/usageLimits";

export function accountLimitWindow(pool: LimitPoolWindow, accountKey: string): LimitPoolWindow {
  return {
    ...pool,
    members: pool.members.filter((member) => member.account.key === accountKey),
    columns: pool.columns.filter((column) => column.account.key === accountKey),
    resets: pool.resets.filter((reset) => reset.member.account.key === accountKey),
  };
}
