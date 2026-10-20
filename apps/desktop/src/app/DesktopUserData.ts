import { DESKTOP_PROFILE_NAME } from "@t3tools/shared/desktopIdentity";
import * as Effect from "effect/Effect";
import * as Path from "effect/Path";

// Fresh profiles also keep Windows safeStorage keys separate; never migrate upstream keys.
export const resolveUserDataPath = Effect.fn("desktop.userData.resolveUserDataPath")(
  function* (input: {
    readonly appDataDirectory: string;
    readonly isDevelopment: boolean;
    readonly platform: NodeJS.Platform;
  }) {
    const path = yield* Path.Path;
    return path.join(
      input.appDataDirectory,
      `${DESKTOP_PROFILE_NAME}${input.isDevelopment ? "-dev" : ""}`,
    );
  },
);
