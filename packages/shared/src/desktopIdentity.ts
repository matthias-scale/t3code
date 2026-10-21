/** Fixed identity for this fork; never reuse the upstream desktop profile or handlers. */
export const DESKTOP_APP_ID = "com.matthias-scale.t3code.attention";
export const DESKTOP_PRODUCT_NAME = "T3 Code (Attention)";
export const DESKTOP_RUNTIME_NAME = "T3 Code Attention";
export const DESKTOP_HOME_NAME = ".t3-attention";
export const DESKTOP_PROFILE_NAME = "t3code-attention";
export const DESKTOP_SCHEME = "t3code-attention";
export const DESKTOP_DEV_SCHEME = "t3code-attention-dev";

export function desktopScheme(development: boolean): string {
  return development ? DESKTOP_DEV_SCHEME : DESKTOP_SCHEME;
}

/** Linux launchers, window grouping and URL registration must share the fork identity. */
export function desktopLinuxIdentity(development: boolean): {
  readonly desktopEntryName: string;
  readonly wmClass: string;
} {
  return {
    desktopEntryName: `${DESKTOP_APP_ID}${development ? ".Development" : ""}.desktop`,
    wmClass: `${DESKTOP_PROFILE_NAME}${development ? "-dev" : ""}`,
  };
}
