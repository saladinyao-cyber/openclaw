/** Canonical OpenAI endpoint classification shared by transport and provider-plugin policy. */
export const OPENAI_API_BASE_URL = "https://api.openai.com/v1";
export const OPENAI_CODEX_RESPONSES_BASE_URL = "https://chatgpt.com/backend-api/codex";

export type OpenAIEndpointKind = "unresolved" | "platform" | "chatgpt" | "custom" | "invalid";

const OPENAI_PLATFORM_PATHS = new Set(["/", "/v1", "/v1/"]);
const OPENAI_CHATGPT_PATHS = new Set([
  "/backend-api",
  "/backend-api/",
  "/backend-api/v1",
  "/backend-api/v1/",
  "/backend-api/codex",
  "/backend-api/codex/",
  "/backend-api/codex/v1",
  "/backend-api/codex/v1/",
  "/backend-api/codex/responses",
  "/backend-api/codex/responses/",
]);

/** Classifies exact native endpoints, valid custom URLs, and unsafe/invalid input. */
export function classifyOpenAIBaseUrl(baseUrl: unknown): OpenAIEndpointKind {
  if (baseUrl === undefined || baseUrl === null || baseUrl === "") {
    return "unresolved";
  }
  if (typeof baseUrl !== "string") {
    return "invalid";
  }
  const trimmed = baseUrl.trim();
  if (!trimmed) {
    return "unresolved";
  }
  try {
    const url = new URL(trimmed);
    if (
      (url.protocol !== "http:" && url.protocol !== "https:") ||
      !url.hostname ||
      url.username ||
      url.password
    ) {
      return "invalid";
    }
    const rawHost = url.hostname.toLowerCase();
    const host = rawHost.endsWith(".") ? rawHost.slice(0, -1) : rawHost;
    if (host === "api.openai.com" || host === "chatgpt.com") {
      // Official remote endpoints carry API keys or subscription bearers. Never
      // reinterpret malformed or plaintext official URLs as custom proxy routes.
      if (url.protocol !== "https:" || url.port || url.search || url.hash) {
        return "invalid";
      }
      if (host === "api.openai.com" && OPENAI_PLATFORM_PATHS.has(url.pathname)) {
        return "platform";
      }
      if (host === "chatgpt.com" && OPENAI_CHATGPT_PATHS.has(url.pathname)) {
        return "chatgpt";
      }
      return "invalid";
    }
    return "custom";
  } catch {
    return "invalid";
  }
}

/** True only for the versioned OpenAI Platform endpoint used by Responses transports. */
export function isOpenAIPlatformResponsesBaseUrl(baseUrl: unknown): boolean {
  if (classifyOpenAIBaseUrl(baseUrl) !== "platform" || typeof baseUrl !== "string") {
    return false;
  }
  return new URL(baseUrl.trim()).pathname.replace(/\/+$/u, "") === "/v1";
}

/** Normalizes a Codex Responses SDK base while preserving custom proxy behavior. */
export function normalizeCodexResponsesBaseUrl(baseUrl?: string): string {
  const normalized = baseUrl?.trim() || "https://chatgpt.com/backend-api";
  if (classifyOpenAIBaseUrl(normalized) === "chatgpt") {
    return OPENAI_CODEX_RESPONSES_BASE_URL;
  }
  if (normalized.includes("?") || normalized.includes("#")) {
    throw new Error(
      "OpenAI Codex Responses baseUrl must not include query parameters or fragments",
    );
  }
  try {
    const parsed = new URL(normalized);
    const pathname = parsed.pathname.replace(/\/+$/u, "");
    parsed.pathname = pathname.toLowerCase().endsWith("/codex/responses")
      ? pathname.slice(0, -"/responses".length)
      : pathname.toLowerCase().endsWith("/codex")
        ? pathname
        : `${pathname}/codex`;
    return parsed.toString().replace(/\/$/u, "");
  } catch {
    const path = normalized.replace(/\/+$/u, "");
    if (path.endsWith("/codex/responses")) {
      return path.slice(0, -"/responses".length);
    }
    return path.endsWith("/codex") ? path : `${path}/codex`;
  }
}

/** Resolves the final HTTP endpoint shared by native Codex SSE and WebSocket dispatch. */
export function resolveCodexResponsesUrl(baseUrl?: string): string {
  return `${normalizeCodexResponsesBaseUrl(baseUrl)}/responses`;
}
