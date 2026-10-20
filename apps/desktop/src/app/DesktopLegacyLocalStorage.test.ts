import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import { DesktopLegacyLocalStorage, layer } from "./DesktopLegacyLocalStorage.ts";

it.effect("never imports upstream local storage or writes an import marker", () =>
  Effect.gen(function* () {
    const migration = yield* DesktopLegacyLocalStorage;
    // No filesystem/environment service is provided: any original state access fails.
    yield* migration.load("/profiles/t3code-attention");
    assert.isTrue(Option.isNone(yield* migration.take));
    yield* migration.complete;
    assert.isTrue(Option.isNone(yield* migration.take));
  }).pipe(Effect.provide(layer)),
);
