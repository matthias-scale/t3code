import * as NodeSqlite from "node:sqlite";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import { HttpClient, HttpClientResponse } from "effect/http";
import { readOpenCodeUsageLimits } from "./usageLimits.ts";

const read = (
  config: unknown,
  body: unknown,
  status = 200,
  environment: NodeJS.ProcessEnv = {},
  expectedKey = "kimi-key",
) => {
  const requests: string[] = [];
  const result = readOpenCodeUsageLimits({
    enabled: true,
    serverUrl: "",
    environment: {
      OPENCODE_CONFIG_CONTENT: JSON.stringify(config),
      OPENCODE_AUTH_CONTENT: JSON.stringify({ "opencode-go": { type: "api", key: "go-key" } }),
      KIMI_API_KEY: "kimi-key",
      ...environment,
    },
  }).pipe(
    Effect.provide(NodeServices.layer),
    Effect.provideService(
      HttpClient.HttpClient,
      HttpClient.make((request) => {
        requests.push(request.url);
        expect(request.headers.authorization).toBe(`Bearer ${expectedKey}`);
        return Effect.succeed(HttpClientResponse.fromWeb(request, Response.json(body, { status })));
      }),
    ),
  );
  return { result, requests };
};

describe("instance plan usage", () => {
  it.effect(
    "reads Kimi ratios, preserves missing windows and never reads Go for other providers",
    () =>
      Effect.gen(function* () {
        const test = read(
          { model: "kimi-code-plan-global/k3" },
          {
            usages: {
              limit_5h: { used_ratio: 0.2046, reset_time: "2099-01-01T00:00:00Z" },
              limit_month_total: { used_ratio: 0.7171, reset_time: "1960-01-01T00:00:00Z" },
            },
          },
        );
        const result = yield* test.result;
        expect(test.requests).toEqual(["https://api.kimi.com/coding/v1/usages"]);
        expect(result.windows.map((w) => [w.kind, w.usedPercent])).toEqual([
          ["session", 20.46],
          ["monthly", 71.71],
        ]);
        expect(result.windows[0]?.resetsAt).toBe("2099-01-01T00:00:00.000Z");
        expect(result.windows[1]?.resetsAt).toBeUndefined();
        expect(result.credentialFingerprint).toHaveLength(64);
        const missing = yield* read({ model: "kimi-code/k3" }, { usages: {} }).result;
        expect(missing.windows).toEqual([]);
        const unrelated = read({ model: "anthropic/claude" }, {});
        expect((yield* unrelated.result).unavailable?.reason).toBe("unsupported");
        expect(unrelated.requests).toEqual([]);
      }),
  );
  it.effect("merges file config with partial inline overrides", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const home = yield* fs.makeTempDirectoryScoped();
      yield* fs.makeDirectory(`${home}/opencode`);
      yield* fs.writeFileString(
        `${home}/opencode/opencode.json`,
        JSON.stringify({
          model: "kimi-code/k3",
          provider: { "kimi-code": { options: { apiKey: "file-fixture" } } },
        }),
      );
      const configPath = `${home}/custom.json`;
      yield* fs.writeFileString(
        configPath,
        JSON.stringify({ provider: { "kimi-code": { options: {} } } }),
      );
      for (const inline of [{}, { provider: { "kimi-code": { options: {} } } }]) {
        const result = yield* read(
          inline,
          { usages: {} },
          200,
          {
            XDG_CONFIG_HOME: home,
            OPENCODE_CONFIG: configPath,
          },
          "file-fixture",
        ).result;
        expect(result.unavailable).toBeUndefined();
      }
      yield* fs.writeFileString(
        `${home}/opencode/opencode.json`,
        JSON.stringify({ model: "opencode-go/model" }),
      );
      const goWindow = { percent: 10, resetsAt: "2099-01-01T00:00:00Z" };
      const go = read(
        {},
        { usage: { rolling: goWindow, weekly: goWindow, monthly: goWindow } },
        200,
        { XDG_CONFIG_HOME: home },
        "go-key",
      );
      expect((yield* go.result).windows.map((window) => window.id)).toEqual([
        "go_rolling",
        "go_weekly",
        "go_monthly",
      ]);
      const overridden = yield* read({ model: "anthropic/claude" }, {}, 200, {
        XDG_CONFIG_HOME: home,
      }).result;
      expect(overridden.unavailable?.reason).toBe("unsupported");
    }).pipe(Effect.provide(NodeServices.layer)),
  );
  it.effect("reads only the selected Kimi provider's stored credentials", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const dataHome = yield* fs.makeTempDirectoryScoped();
      yield* fs.makeDirectory(`${dataHome}/opencode`);
      yield* fs.writeFileString(
        `${dataHome}/opencode/auth.json`,
        JSON.stringify({
          "kimi-code": { type: "api", key: "auth-fixture" },
          "kimi-code-plan-global": { type: "api", key: "global-fixture" },
        }),
      );
      const db = yield* Effect.acquireRelease(
        Effect.sync(() => new NodeSqlite.DatabaseSync(`${dataHome}/opencode/opencode.db`)),
        (db) => Effect.sync(() => db.close()),
      );
      db.exec(
        "CREATE TABLE credential (id text, integration_id text, value text, active integer, time_created integer)",
      );
      for (const provider of ["kimi-code", "kimi-code-plan-global"]) {
        const environment = {
          XDG_DATA_HOME: dataHome,
          KIMI_API_KEY: undefined,
          OPENCODE_AUTH_CONTENT: undefined,
        };
        const expected = provider === "kimi-code" ? "auth-fixture" : "global-fixture";
        expect(
          (yield* read({ model: `${provider}/k3` }, { usages: {} }, 200, environment, expected)
            .result).unavailable,
        ).toBeUndefined();
        db.prepare("INSERT INTO credential VALUES (?, ?, ?, 1, 1)").run(
          provider,
          provider,
          JSON.stringify({ type: "key", key: `${provider}-fixture` }),
        );
        expect(
          (yield* read(
            { model: `${provider}/k3` },
            { usages: {} },
            200,
            environment,
            `${provider}-fixture`,
          ).result).unavailable,
        ).toBeUndefined();
      }
      yield* fs.writeFileString(`${dataHome}/opencode/auth.json`, "{}");
      db.exec("DELETE FROM credential WHERE integration_id = 'kimi-code'");
      const missing = read({ model: "kimi-code/k3" }, {}, 200, {
        XDG_DATA_HOME: dataHome,
        KIMI_API_KEY: undefined,
        OPENCODE_AUTH_CONTENT: undefined,
      });
      expect((yield* missing.result).unavailable?.reason).toBe("probeFailed");
      expect(missing.requests).toEqual([]);
    }).pipe(Effect.provide(NodeServices.layer)),
  );
  it.effect("keeps Kimi errors visible without exposing provider bodies", () =>
    Effect.gen(function* () {
      const result = yield* read({ model: "kimi-code/k3" }, { secret: "private" }, 401).result;
      expect(result.windows).toEqual([]);
      expect(result.unavailable?.message).toContain("HTTP 401");
      expect(JSON.stringify(result)).not.toContain("private");
      const invalid = yield* read(
        { model: "kimi-code/k3" },
        { usages: { limit_5h: { used_ratio: -1 } } },
      ).result;
      expect(invalid.unavailable?.reason).toBe("probeFailed");
    }),
  );
});
