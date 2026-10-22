import { useState } from "react";
import {
  MAX_SIDEBAR_AUTO_SETTLE_AFTER_HOURS,
  MIN_SIDEBAR_AUTO_SETTLE_AFTER_HOURS,
} from "@t3tools/contracts";

import { AppTextInput } from "../../../components/AppText";

export interface AutoSettleHoursFieldProps {
  readonly value: number;
  readonly onValueChange: (value: number) => void;
}

export function AutoSettleHoursField(props: AutoSettleHoursFieldProps) {
  const [draft, setDraft] = useState<string | null>(null);
  const commit = () => {
    const text = (draft ?? "").trim();
    setDraft(null);
    // Validate the whole input; decimals and trailing text must not become whole hours.
    const parsed = /^\d+$/.test(text) ? Number(text) : Number.NaN;
    if (
      Number.isInteger(parsed) &&
      parsed >= MIN_SIDEBAR_AUTO_SETTLE_AFTER_HOURS &&
      parsed <= MAX_SIDEBAR_AUTO_SETTLE_AFTER_HOURS &&
      parsed !== props.value
    ) {
      props.onValueChange(parsed);
    }
  };
  return (
    <AppTextInput
      className="min-h-10 w-20 rounded-xl px-3 py-2 text-center text-base"
      keyboardType="number-pad"
      returnKeyType="done"
      value={draft ?? String(props.value)}
      onChangeText={setDraft}
      onBlur={commit}
      onSubmitEditing={commit}
      accessibilityLabel="Hours before auto-settle"
    />
  );
}
