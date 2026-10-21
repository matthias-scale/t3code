import { describe, expect, it } from "vite-plus/test";
import { providerAuthReturnUrl } from "./providerAuthReturnUrl.ts";

describe("provider auth return destinations", () => {
  it.each(["t3code-attention", "t3code-attention-dev"])(
    "returns to %s Welcome and the selected settings instance",
    (scheme) => {
      expect(providerAuthReturnUrl(`${scheme}://app/welcome?code=secret#agents:machine-id`)).toBe(
        `${scheme}://app/welcome#agents:machine-id`,
      );
      expect(
        providerAuthReturnUrl(`${scheme}://app/settings/providers?instanceId=work&code=secret`),
      ).toBe(`${scheme}://app/settings/providers?instanceId=work`);
    },
  );
  it.each([
    "t3code-attention://attacker/welcome",
    "t3code-attention://app:123/welcome",
    "t3code-attention://app/auth/callback",
    "t3code-attention://user@ app/welcome",
    "t3code-attention://app/welcome/../evil",
    "https://attacker.example/welcome",
    "file:///welcome",
    "t3code://app/welcome",
    "t3code-dev://app/welcome",
    "t3code-attention-evil://app/welcome",
    "javascript:alert(1)",
  ])("rejects %s", (url) => expect(providerAuthReturnUrl(url)).toBeUndefined());
});
