import { describe, expect, it } from "vitest";
import {
  classifyOpenAIBaseUrl,
  isOpenAIPlatformResponsesBaseUrl,
  normalizeCodexResponsesBaseUrl,
  resolveCodexResponsesUrl,
} from "./openai-endpoint.js";

describe("OpenAI endpoint classification", () => {
  it.each([
    ["https://api.openai.com/v1", "platform"],
    ["https://api.openai.com:443/v1/", "platform"],
    ["https://chatgpt.com/backend-api", "chatgpt"],
    ["https://chatgpt.com/backend-api/v1", "chatgpt"],
    ["https://chatgpt.com/backend-api/codex", "chatgpt"],
    ["https://chatgpt.com/backend-api/codex/v1", "chatgpt"],
    ["https://chatgpt.com/backend-api/codex/responses", "chatgpt"],
    ["https://proxy.example.test/v1?tenant=one", "custom"],
  ] as const)("classifies %s as %s", (baseUrl, endpointKind) => {
    expect(classifyOpenAIBaseUrl(baseUrl)).toBe(endpointKind);
  });

  it.each([
    "http://api.openai.com/v1",
    "http://chatgpt.com/backend-api/codex",
    "https://api.openai.com.evil.example/v1",
    "https://chatgpt.com.evil.example/backend-api/codex",
    "https://api.openai.com/v1/responses",
    "https://chatgpt.com/backend-api/codex/v2",
    "https://chatgpt.com/backend-api/codex?tenant=one",
    "https://user@chatgpt.com/backend-api/codex",
  ])("rejects unsafe native-endpoint lookalike %s", (baseUrl) => {
    expect(classifyOpenAIBaseUrl(baseUrl)).not.toBe("platform");
    expect(classifyOpenAIBaseUrl(baseUrl)).not.toBe("chatgpt");
  });

  it("limits native Responses WebSockets to the versioned platform base", () => {
    expect(isOpenAIPlatformResponsesBaseUrl("https://api.openai.com/v1")).toBe(true);
    expect(isOpenAIPlatformResponsesBaseUrl("https://api.openai.com")).toBe(false);
    expect(isOpenAIPlatformResponsesBaseUrl("https://chatgpt.com/backend-api/codex")).toBe(false);
  });

  it.each([
    "https://chatgpt.com/backend-api",
    "https://chatgpt.com/backend-api/v1",
    "https://chatgpt.com/backend-api/codex",
    "https://chatgpt.com/backend-api/codex/v1",
    "https://chatgpt.com/backend-api/codex/responses",
  ])("normalizes native Codex route %s for SDK and final dispatch", (baseUrl) => {
    expect(normalizeCodexResponsesBaseUrl(baseUrl)).toBe("https://chatgpt.com/backend-api/codex");
    expect(resolveCodexResponsesUrl(baseUrl)).toBe(
      "https://chatgpt.com/backend-api/codex/responses",
    );
  });
});
