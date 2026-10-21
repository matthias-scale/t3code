import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import { resolveUserDataPath } from "./DesktopUserData.ts";

it.effect.each(["darwin", "win32", "linux"] as const)(
  "uses a fresh fork profile on %s without inspecting or migrating upstream state",
  (platform) =>
    Effect.gen(function* () {
      const path = yield* Path.Path;
      const actual = yield* resolveUserDataPath({
        appDataDirectory: "/profiles",
        isDevelopment: false,
        platform,
      });
      assert.equal(actual, path.join("/profiles", "t3code-attention"));
      assert.equal(
        yield* resolveUserDataPath({
          appDataDirectory: "/profiles",
          isDevelopment: true,
          platform,
        }),
        path.join("/profiles", "t3code-attention-dev"),
      );
    }).pipe(
      Effect.provide(NodeServices.layer),
      Effect.provideService(FileSystem.FileSystem, FileSystem.makeNoop({})),
    ),
);
