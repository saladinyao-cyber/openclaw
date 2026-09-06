import type { Model } from "@openclaw/llm-core";
import { describe, expect, it } from "vitest";
import {
  buildOpenAIResponsesCompactSystemMessage,
  sanitizeOpenAICodexResponsesParams,
} from "./openai-responses-params-internal.js";

const reasoningModel = {
  id: "gpt-5.6-luna",
  name: "GPT-5.6 Luna",
  api: "openai-responses",
  provider: "openai",
  baseUrl: "https://api.openai.com/v1",
  reasoning: true,
  input: ["text"],
  cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
  contextWindow: 256_000,
  maxTokens: 8_192,
} satisfies Model<"openai-responses">;

describe("sanitizeOpenAICodexResponsesParams", () => {
  it.each([
    ["openai-chatgpt-responses", "https://chatgpt.com/backend-api/codex"],
    ["openai-chatgpt-responses", "https://chatgpt.com/backend-api/codex/responses"],
    [
      "openclaw-provider-stream:openai:gpt-5.6-luna:openai-chatgpt-responses:https%3A%2F%2Fchatgpt.com%2Fbackend-api%2Fcodex",
      "https://chatgpt.com/backend-api/codex",
    ],
    [
      "openclaw-provider-stream:openai:gpt-5.6-luna:openai-chatgpt-responses:https%3A%2F%2Fchatgpt.com%2Fbackend-api%2Fcodex%2Fresponses",
      "https://chatgpt.com/backend-api/codex/responses",
    ],
  ])("enforces final stateless Codex payload policy for %s at %s", (api, baseUrl) => {
    const providerStreamModel = {
      ...reasoningModel,
      api,
      baseUrl,
    } as Model;
    const params = sanitizeOpenAICodexResponsesParams(providerStreamModel, {
      model: providerStreamModel.id,
      store: true,
      max_output_tokens: 128,
      metadata: { purpose: "dashboard-title" },
      text: { format: { type: "text" }, verbosity: "low" },
    });

    expect(params.store).toBe(false);
    expect(params).not.toHaveProperty("max_output_tokens");
    expect(params).not.toHaveProperty("metadata");
    expect(params.text).toEqual({ verbosity: "low" });
  });

  it("does not rewrite non-Codex Responses endpoints", () => {
    const params = sanitizeOpenAICodexResponsesParams(reasoningModel, {
      store: true,
      max_output_tokens: 128,
    });

    expect(params).toEqual({ store: true, max_output_tokens: 128 });
  });

  it.each([
    ["openai-responses", "https://chatgpt.com/backend-api/codex"],
    ["openai-chatgpt-responses", "https://chatgpt.com.evil.example/backend-api/codex"],
    ["openai-chatgpt-responses", "http://chatgpt.com/backend-api/codex"],
    [
      "openclaw-provider-stream:openai:model:openai-responses:encoded",
      "https://chatgpt.com/backend-api/codex",
    ],
  ])("does not rewrite non-native route %s at %s", (api, baseUrl) => {
    const model = { ...reasoningModel, api, baseUrl } as Model;
    const params = { store: true, max_output_tokens: 128 };

    expect(sanitizeOpenAICodexResponsesParams(model, params)).toEqual({
      store: true,
      max_output_tokens: 128,
    });
  });
});

describe("buildOpenAIResponsesCompactSystemMessage", () => {
  it("uses the developer role for reasoning models that support it", () => {
    expect(
      buildOpenAIResponsesCompactSystemMessage(reasoningModel, "Retain the conversation."),
    ).toEqual({
      type: "message",
      role: "developer",
      content: [{ type: "input_text", text: "Retain the conversation." }],
    });
  });

  it("falls back to the system role for xAI's native route, which disables the developer role", () => {
    const message = buildOpenAIResponsesCompactSystemMessage(
      { ...reasoningModel, provider: "xai", baseUrl: "https://api.x.ai/v1" },
      "Retain the conversation.",
    );
    expect(message.role).toBe("system");
  });

  it("uses the system role for non-reasoning models", () => {
    const message = buildOpenAIResponsesCompactSystemMessage(
      { ...reasoningModel, reasoning: false },
      "Retain the conversation.",
    );
    expect(message.role).toBe("system");
  });
});
