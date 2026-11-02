import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import { HttpClient, HttpClientResponse } from "effect/http";
import { readOpenCodeUsageLimits } from "./usageLimits.ts";

const read = (config: unknown, body: unknown, status = 200) => {
  const requests: string[] = [];
  const result = readOpenCodeUsageLimits({
    enabled: true,
    serverUrl: "",
    environment: {
      OPENCODE_CONFIG_CONTENT: JSON.stringify(config),
      OPENCODE_AUTH_CONTENT: JSON.stringify({ "opencode-go": { type: "api", key: "go-key" } }),
      KIMI_API_KEY: "kimi-key",
    },
  }).pipe(
    Effect.provide(NodeServices.layer),
    Effect.provideService(
      HttpClient.HttpClient,
      HttpClient.make((request) => {
        requests.push(request.url);
        expect(request.headers.authorization).toBe("Bearer kimi-key");
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
