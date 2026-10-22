import { Button, Host, HStack, Picker, Popover, Text, VStack } from "@expo/ui/swift-ui";
import {
  accessibilityLabel,
  buttonStyle,
  font,
  foregroundStyle,
  frame,
  padding,
  pickerStyle,
  presentationBackground,
  tag,
} from "@expo/ui/swift-ui/modifiers";
import { useState } from "react";

import { useAppearancePreferences } from "../appearance/AppearancePreferencesProvider";
import type { AutoSettleHoursFieldProps } from "./AutoSettleHoursField";

const commonHours = [4, 12, 24, 72, 168, 720, 2160] as const;

export function AutoSettleHoursField(props: AutoSettleHoursFieldProps) {
  const { themeAppearance, themeVariables: colors, appearance } = useAppearancePreferences();
  const [open, setOpen] = useState(false);
  const [draft, setDraft] = useState(props.value);
  const hours = [...new Set([props.value, ...commonHours])].sort((left, right) => left - right);
  return (
    <Host matchContents colorScheme={themeAppearance} seedColor={colors["--color-primary"]}>
      <Popover isPresented={open} onIsPresentedChange={setOpen}>
        <Popover.Trigger>
          <Button
            onPress={() => {
              setDraft(props.value);
              setOpen(true);
            }}
            modifiers={[
              buttonStyle("bordered"),
              accessibilityLabel(`Hours before auto-settle: ${props.value}`),
              frame({ minWidth: 64, minHeight: 44 }),
              foregroundStyle(colors["--color-primary"]),
              font({ size: appearance.baseFontSize }),
            ]}
          >
            <Text>{String(props.value)}</Text>
          </Button>
        </Popover.Trigger>
        <Popover.Content>
          <VStack
            modifiers={[
              padding({ all: 12 }),
              frame({ width: 240 }),
              presentationBackground(colors["--color-sheet-solid"]),
            ]}
          >
            <Picker
              label="Hours before auto-settle"
              selection={draft}
              onSelectionChange={setDraft}
              modifiers={[pickerStyle("wheel"), frame({ height: 180 })]}
            >
              {hours.map((value) => (
                <Text
                  key={value}
                  modifiers={[tag(value), foregroundStyle(colors["--color-foreground"])]}
                >
                  {`${value} ${value === 1 ? "hour" : "hours"}`}
                </Text>
              ))}
            </Picker>
            <HStack spacing={24}>
              <Button
                label="Cancel"
                onPress={() => setOpen(false)}
                modifiers={[foregroundStyle(colors["--color-primary"])]}
              />
              <Button
                label="Done"
                onPress={() => {
                  setOpen(false);
                  if (draft !== props.value) props.onValueChange(draft);
                }}
                modifiers={[foregroundStyle(colors["--color-primary"])]}
              />
            </HStack>
          </VStack>
        </Popover.Content>
      </Popover>
    </Host>
  );
}
