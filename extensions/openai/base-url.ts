// Openai plugin module implements base url behavior.
import { classifyOpenAIBaseUrl } from "openclaw/plugin-sdk/provider-tools";
import { normalizeOptionalString } from "openclaw/plugin-sdk/string-coerce-runtime";

export const OPENAI_CODEX_RESPONSES_BASE_URL = "https://chatgpt.com/backend-api/codex";
export const OPENAI_API_BASE_URL = "https://api.openai.com/v1";
export { classifyOpenAIBaseUrl };

export function resolveOpenAIDefaultBaseUrl(
  env: Record<string, string | undefined> = process.env,
): string {
  return normalizeOptionalString(env.OPENAI_BASE_URL) ?? OPENAI_API_BASE_URL;
}

export function isOpenAIApiBaseUrl(baseUrl?: string): boolean {
  return classifyOpenAIBaseUrl(baseUrl) === "platform";
}

export function isOpenAICodexBaseUrl(baseUrl?: string): boolean {
  return classifyOpenAIBaseUrl(baseUrl) === "chatgpt";
}

/** True only for an HTTPS OpenAI Platform endpoint eligible for native transport hooks. */
export function isOpenAIHttpsApiBaseUrl(baseUrl?: string): boolean {
  if (typeof baseUrl !== "string" || classifyOpenAIBaseUrl(baseUrl) !== "platform") {
    return false;
  }
  return new URL(baseUrl.trim()).protocol === "https:";
}

export function canonicalizeCodexResponsesBaseUrl(baseUrl?: string): string | undefined {
  return isOpenAICodexBaseUrl(baseUrl) ? OPENAI_CODEX_RESPONSES_BASE_URL : baseUrl;
}
