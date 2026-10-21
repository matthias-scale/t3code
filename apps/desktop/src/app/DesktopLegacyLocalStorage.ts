import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";

export class DesktopLegacyLocalStorage extends Context.Service<
  DesktopLegacyLocalStorage,
  {
    readonly load: (userDataPath: string) => Effect.Effect<void>;
    readonly take: Effect.Effect<Option.Option<Readonly<Record<string, string>>>>;
    readonly complete: Effect.Effect<void>;
  }
>()("@t3tools/desktop/app/DesktopLegacyLocalStorage") {}

// The fork starts fresh, including renderer settings and saved routing.
export const layer = Layer.succeed(
  DesktopLegacyLocalStorage,
  DesktopLegacyLocalStorage.of({
    load: () => Effect.void,
    take: Effect.succeed(Option.none()),
    complete: Effect.void,
  }),
);
