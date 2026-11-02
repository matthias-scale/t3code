import * as NodeSqlite from "node:sqlite";

import type { ServerProviderUsageWindow } from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Hex from "effect/encoding/Hex";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientRequest from "effect/http/HttpClientRequest";
import * as HttpClientResponse from "effect/http/HttpClientResponse";

import {
  clampPercent,
  makeUnavailableUsageLimits,
  makeUsageLimits,
} from "@t3tools/provider-core/server/usageLimits";
import * as HostProcess from "@t3tools/shared/HostProcess";

const AuthFile = Schema.Record(Schema.String, Schema.Unknown);
const ApiAuth = Schema.Struct({ type: Schema.Literal("api"), key: Schema.String });
const decodeAuthFile = Schema.decodeEffect(Schema.fromJsonString(AuthFile));
const decodeApiAuth = Schema.decodeUnknownOption(ApiAuth);
// OpenCode 2's database tags a stored key "key"; auth.json tags it "api".
const StoredKey = Schema.Struct({
  type: Schema.Literal("key"),
  key: Schema.String,
  metadata: Schema.optionalKey(
    Schema.NullOr(Schema.Struct({ server: Schema.optionalKey(Schema.Unknown) })),
  ),
});
const decodeStoredKey = Schema.decodeOption(Schema.fromJsonString(StoredKey));
const UsageWindow = Schema.Struct({
  percent: Schema.Finite,
  resetsAt: Schema.DateTimeUtcFromString,
});
const UsageResponse = Schema.Struct({
  usage: Schema.Struct({ rolling: UsageWindow, weekly: UsageWindow, monthly: UsageWindow }),
});

// Select before decoding so an OAuth or self-hosted connection cannot fall
// through to another stored account. NULL-active rows predate the selection flag.
const readStoredApiKey = (databasePath: string, integrationId: string) =>
  Effect.try(() => {
    const database = new NodeSqlite.DatabaseSync(databasePath, { readOnly: true });
    try {
      database.exec("PRAGMA busy_timeout = 100");
      const row = database
        .prepare(
          `SELECT value FROM credential
           WHERE integration_id = ? AND active IS NOT 0
           ORDER BY active DESC, time_created DESC, id DESC LIMIT 1`,
        )
        .get(integrationId);
      const stored =
        typeof row?.value === "string"
          ? Option.getOrUndefined(decodeStoredKey(row.value))
          : undefined;
      const server = stored?.metadata?.server;
      if (
        (integrationId === "opencode" || integrationId === "opencode-go") &&
        typeof server === "string"
      ) {
        const url = new URL(server);
        if (`${url.origin}${url.pathname.replace(/\/+$/, "")}` !== "https://opencode.ai/console")
          return undefined;
      }
      return stored?.key.trim();
    } finally {
      database.close();
    }
  }).pipe(Effect.orElseSucceed(() => undefined));

const ProviderConfig = Schema.Struct({
  model: Schema.optional(Schema.String),
  enabled_providers: Schema.optional(Schema.Array(Schema.String)),
  provider: Schema.optional(
    Schema.Record(
      Schema.String,
      Schema.Struct({
        options: Schema.optional(Schema.Struct({ apiKey: Schema.optional(Schema.String) })),
      }),
    ),
  ),
});
const decodeProviderConfig = Schema.decodeEffect(Schema.fromJsonString(ProviderConfig));
const KimiWindow = Schema.Struct({
  used_ratio: Schema.Finite.check(Schema.isGreaterThanOrEqualTo(0)),
  reset_time: Schema.optional(Schema.NullOr(Schema.String)),
});
const KimiUsage = Schema.Struct({
  usages: Schema.Struct({
    limit_5h: Schema.optional(Schema.NullOr(KimiWindow)),
    limit_month_total: Schema.optional(Schema.NullOr(KimiWindow)),
  }),
});

export const readOpenCodeUsageLimits = Effect.fn("readOpenCodeUsageLimits")(function* (input: {
  readonly enabled: boolean;
  readonly serverUrl: string;
  readonly environment: NodeJS.ProcessEnv;
}) {
  const checkedAt = DateTime.formatIso(yield* DateTime.now);
  const unsupported = makeUnavailableUsageLimits({ checkedAt, reason: "unsupported" });
  if (!input.enabled || input.serverUrl.trim()) return unsupported;
  return yield* Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const env = input.environment;
    const home = env.HOME || env.USERPROFILE || (yield* HostProcess.HomeDirectory);
    const configPath = path.join(
      env.XDG_CONFIG_HOME || path.join(home, ".config"),
      "opencode",
      "opencode.json",
    );
    const readFile = (file: string) =>
      fs.readFileString(file).pipe(
        Effect.catchTags({
          PlatformError: (error) =>
            error.reason._tag === "NotFound" ? Effect.succeed("{}") : Effect.fail(error),
        }),
      );
    const layers = [yield* readFile(configPath)];
    if (env.OPENCODE_CONFIG) layers.push(yield* readFile(env.OPENCODE_CONFIG));
    layers.push(env.OPENCODE_CONFIG_CONTENT ?? "{}");
    let config: typeof ProviderConfig.Type = {};
    for (const contents of layers) {
      const next = yield* decodeProviderConfig(contents);
      const providers = { ...config.provider };
      for (const [id, value] of Object.entries(next.provider ?? {})) {
        providers[id] = { options: { ...providers[id]?.options, ...value.options } };
      }
      config = { ...config, ...next, provider: providers };
    }
    const provider =
      config.model?.split("/")[0] ??
      (config.enabled_providers?.length === 1 ? config.enabled_providers[0] : undefined);
    if (provider === "opencode-go") return yield* readOpenCodeGoUsageLimits(input);
    if (provider !== "kimi-code" && provider !== "kimi-code-plan-global") return unsupported;
    const configuredKey = config.provider?.[provider]?.options?.apiKey;
    const reference = configuredKey?.match(/^\{env:([^}]+)\}$/);
    const dataHome = env.XDG_DATA_HOME || path.join(home, ".local", "share");
    const auth = yield* decodeAuthFile(
      env.OPENCODE_AUTH_CONTENT ?? (yield* readFile(path.join(dataHome, "opencode", "auth.json"))),
    );
    const apiAuth = decodeApiAuth(auth[provider]);
    const databaseFile = env.OPENCODE_DB ?? "opencode.db";
    const storedKey =
      env.OPENCODE_AUTH_CONTENT || databaseFile === ":memory:"
        ? undefined
        : yield* readStoredApiKey(path.resolve(dataHome, "opencode", databaseFile), provider);
    const apiKey = (
      reference
        ? env[reference[1]!]
        : (configuredKey ??
          storedKey ??
          (Option.isSome(apiAuth) ? apiAuth.value.key : env.KIMI_API_KEY))
    )?.trim();
    if (!apiKey)
      return makeUnavailableUsageLimits({
        checkedAt,
        reason: "probeFailed",
        message:
          "Kimi Code plan key is unavailable in this instance's provider config, credential store or environment.",
      });
    const client = yield* HttpClient.HttpClient;
    const response = yield* client.execute(
      HttpClientRequest.get("https://api.kimi.com/coding/v1/usages").pipe(
        HttpClientRequest.bearerToken(apiKey),
      ),
    );
    if (response.status !== 200)
      return makeUnavailableUsageLimits({
        checkedAt,
        reason: "probeFailed",
        message: `Kimi Code plan usage returned HTTP ${response.status}.`,
      });
    const body = yield* HttpClientResponse.schemaBodyJson(KimiUsage)(response);
    const windows: ServerProviderUsageWindow[] = [];
    for (const [id, kind, label, duration] of [
      ["limit_5h", "session", "Kimi Code plan · 5-hour", 300],
      ["limit_month_total", "monthly", "Kimi Code plan · Monthly", undefined],
    ] as const) {
      const window = body.usages[id];
      if (!window) continue;
      const reset = window.reset_time ? Date.parse(window.reset_time) : NaN;
      windows.push({
        id,
        kind,
        label,
        usedPercent: clampPercent(window.used_ratio * 100),
        ...(duration === undefined ? {} : { windowDurationMins: duration }),
        ...(Number.isFinite(reset) && reset > Date.parse(checkedAt)
          ? { resetsAt: DateTime.formatIso(DateTime.makeUnsafe(reset)) }
          : {}),
      });
    }
    const crypto = yield* Crypto.Crypto;
    const credentialFingerprint = yield* crypto
      .digest("SHA-256", new TextEncoder().encode(`kimi-code\0${apiKey}`))
      .pipe(Effect.map(Hex.encode), Effect.orDie);
    return { ...makeUsageLimits({ checkedAt, windows }), credentialFingerprint };
  }).pipe(
    Effect.timeout("5 seconds"),
    Effect.orElseSucceed(() =>
      makeUnavailableUsageLimits({
        checkedAt,
        reason: "probeFailed",
        message: "OpenCode could not read this instance's plan usage.",
      }),
    ),
  );
});

/** External OpenCode servers own their credentials; never read the host's account for them. */
export const readOpenCodeGoUsageLimits = Effect.fn("readOpenCodeGoUsageLimits")(function* (input: {
  readonly enabled: boolean;
  readonly serverUrl: string;
  readonly environment: NodeJS.ProcessEnv;
}) {
  const checkedAt = DateTime.formatIso(yield* DateTime.now);
  const unsupported = makeUnavailableUsageLimits({ checkedAt, reason: "unsupported" });
  if (!input.enabled || input.serverUrl.trim()) return unsupported;

  return yield* Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const env = input.environment;
    const dataHome =
      env.XDG_DATA_HOME ||
      path.join(
        env.HOME || env.USERPROFILE || (yield* HostProcess.HomeDirectory),
        ".local",
        "share",
      );
    const authPath = path.join(dataHome, "opencode", "auth.json");
    const contents =
      env.OPENCODE_AUTH_CONTENT ||
      (yield* fs.readFileString(authPath).pipe(
        Effect.catchTags({
          PlatformError: (error) =>
            error.reason._tag === "NotFound" ? Effect.succeed("{}") : Effect.fail(error),
        }),
      ));
    const auth = yield* decodeAuthFile(contents);
    const apiAuth = decodeApiAuth(auth["opencode-go"]);
    // OpenCode overlays stored API credentials after environment credentials.
    const goKey = (Option.isSome(apiAuth) ? apiAuth.value.key : env.OPENCODE_API_KEY)?.trim();
    // Console supplies Go credentials in OpenCode 2. Inline auth explicitly
    // overrides local credentials, including the Console connection.
    const databaseFile = env.OPENCODE_DB ?? "opencode.db";
    const databasePath = path.resolve(dataHome, "opencode", databaseFile);
    const storedKeys =
      env.OPENCODE_AUTH_CONTENT || databaseFile === ":memory:"
        ? []
        : [
            yield* readStoredApiKey(databasePath, "opencode"),
            // OpenCode 2 imports auth.json once, so a replaced Go key only exists here.
            yield* readStoredApiKey(databasePath, "opencode-go"),
          ];
    const keys = [...new Set([...storedKeys, goKey].filter((key): key is string => Boolean(key)))];
    let apiKey = keys[0];
    if (!apiKey) return unsupported;

    const client = yield* HttpClient.HttpClient;
    const readUsage = (key: string) =>
      client.execute(
        HttpClientRequest.get("https://opencode.ai/zen/go/v1/usage").pipe(
          HttpClientRequest.bearerToken(key),
        ),
      );
    let response = yield* readUsage(apiKey);
    for (const key of keys.slice(1)) {
      if (response.status !== 401 && response.status !== 403) break;
      apiKey = key;
      response = yield* readUsage(apiKey);
    }
    // A valid Zen key can exist without a Go subscription.
    if (response.status === 403) return unsupported;
    const body = yield* HttpClientResponse.filterStatusOk(response).pipe(
      Effect.flatMap(HttpClientResponse.schemaBodyJson(UsageResponse)),
    );
    const windows: ServerProviderUsageWindow[] = [
      {
        id: "go_rolling",
        kind: "session",
        label: "Go · Session",
        windowDurationMins: 5 * 60,
        usedPercent: clampPercent(body.usage.rolling.percent),
        resetsAt: DateTime.formatIso(body.usage.rolling.resetsAt),
      },
      {
        id: "go_weekly",
        kind: "weekly",
        label: "Go · Weekly",
        windowDurationMins: 7 * 24 * 60,
        usedPercent: clampPercent(body.usage.weekly.percent),
        resetsAt: DateTime.formatIso(body.usage.weekly.resetsAt),
      },
      {
        id: "go_monthly",
        kind: "monthly",
        label: "Go · Monthly",
        usedPercent: clampPercent(body.usage.monthly.percent),
        resetsAt: DateTime.formatIso(body.usage.monthly.resetsAt),
      },
    ];
    const crypto = yield* Crypto.Crypto;
    // Unkeyed hashes match across environments; random Go keys resist offline guessing.
    const credentialFingerprint = yield* crypto
      .digest("SHA-256", new TextEncoder().encode(`opencode-go\0${apiKey}`))
      .pipe(Effect.map(Hex.encode), Effect.orDie);
    return {
      ...makeUsageLimits({ checkedAt, windows }),
      credentialFingerprint,
    };
  }).pipe(
    Effect.timeout("5 seconds"),
    Effect.orElseSucceed(() =>
      makeUnavailableUsageLimits({
        checkedAt,
        reason: "probeFailed",
        message: "OpenCode Go could not read usage.",
      }),
    ),
  );
});
