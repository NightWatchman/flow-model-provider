import {
  type Api,
  type AssistantMessage,
  type AssistantMessageEventStream,
  type Context,
  calculateCost,
  createAssistantMessageEventStream,
  type ImageContent,
  type Message,
  type Model,
  type SimpleStreamOptions,
  type TextContent,
  type ToolCall,
  type ToolResultMessage,
} from "@earendil-works/pi-ai";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import * as fs from "fs";
import * as path from "path";

// Change this to the URL of your model provider
const BASE_URL = "https://my-model-provider.com/v1";

const LOG_FILE = path.join(
  process.env.HOME ?? process.env.USERPROFILE ?? ".",
  ".pi", "agent", "flow-model-provider.log"
);

function log(tag: string, data: unknown) {
  const line = `[${new Date().toISOString()}] [${tag}] ${JSON.stringify(data, null, 2)}\n`;
  fs.appendFileSync(LOG_FILE, line);
}

// =============================================================================
// Message conversion (pi internal format → OpenAI format)
// =============================================================================

function convertMessages(messages: Message[], systemPrompt?: string): any[] {
  const result: any[] = [];

  if (systemPrompt) {
    result.push({ role: "system", content: systemPrompt });
  }

  for (const msg of messages) {
    if (msg.role === "user") {
      if (typeof msg.content === "string") {
        result.push({ role: "user", content: msg.content });
      } else {
        const blocks = (msg.content as (TextContent | ImageContent)[]).map((c) => {
          if (c.type === "text") return { type: "text", text: c.text };
          return { type: "image_url", image_url: { url: `data:${c.mimeType};base64,${c.data}` } };
        });
        result.push({ role: "user", content: blocks });
      }
    } else if (msg.role === "assistant") {
      const textContent = msg.content
        .filter((c) => c.type === "text")
        .map((c) => (c as TextContent).text)
        .join("\n") || null;

      const toolCalls = msg.content.filter((c) => c.type === "toolCall") as ToolCall[];
      const openAiToolCalls = toolCalls.length > 0
        ? toolCalls.map((tc) => ({
            id: tc.id,
            type: "function",
            function: { name: tc.name, arguments: JSON.stringify(tc.arguments) },
          }))
        : undefined;

      const assistantMsg: any = { role: "assistant" };
      if (textContent) assistantMsg.content = textContent;
      if (openAiToolCalls) assistantMsg.tool_calls = openAiToolCalls;
      result.push(assistantMsg);
    } else if (msg.role === "toolResult") {
      const m = msg as ToolResultMessage;
      const text = m.content.map((c) => (c.type === "text" ? c.text : "[image]")).join("\n");
      result.push({ role: "tool", tool_call_id: m.toolCallId, content: text });
    }
  }

  return result;
}

// =============================================================================
// Custom stream function (package non-streaming JSON response)
// =============================================================================
function streamFlowModelProvider(
  model: Model<Api>,
  context: Context,
  options?: SimpleStreamOptions,
): AssistantMessageEventStream {
  const stream = createAssistantMessageEventStream();

  (async () => {
    const output: AssistantMessage = {
      role: "assistant",
      content: [],
      api: model.api,
      provider: model.provider,
      model: model.id,
      usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } },
      stopReason: "stop",
      timestamp: Date.now(),
    };

    try {
      const apiKey = options?.apiKey ?? process.env.FLOW_MODEL_PROVIDER_KEY ?? "";
      const messages = convertMessages(context.messages, context.systemPrompt);

      const tools = context.tools?.map((t) => ({
        type: "function",
        function: { name: t.name, description: t.description, parameters: t.parameters, strict: false },
      }));

      const body: Record<string, unknown> = {
        model: model.id,
        messages,
        max_tokens: options?.maxTokens ?? model.maxTokens,
      };
      if (tools?.length) body.tools = tools;

      log("REQUEST", body);

      const response = await fetch(`${BASE_URL}/chat/completions`, {
        method: "POST",
        headers: { "Content-Type": "application/json", Authorization: `Bearer ${apiKey}` },
        body: JSON.stringify(body),
        signal: options?.signal,
      });

      log("RESPONSE", { status: response.status, contentType: response.headers.get("content-type") });

      if (!response.ok) {
        const text = await response.text();
        log("HTTP_ERROR", { status: response.status, body: text });
        throw new Error(`HTTP ${response.status}: ${text}`);
      }

      const data = (await response.json()) as {
        choices?: Array<{
          message?: { role: string; content?: string | null; tool_calls?: any[] };
          finish_reason?: string;
        }>;
        usage?: { prompt_tokens?: number; completion_tokens?: number; total_tokens?: number };
      };

      const choice = data.choices?.[0];
      const message = choice?.message;

      stream.push({ type: "start", partial: output });

      if (message?.content) {
        output.content.push({ type: "text", text: "" });
        const ci = output.content.length - 1;
        stream.push({ type: "text_start", contentIndex: ci, partial: output });
        (output.content[ci] as TextContent).text = message.content;
        stream.push({ type: "text_delta", contentIndex: ci, delta: message.content, partial: output });
        stream.push({ type: "text_end", contentIndex: ci, content: message.content, partial: output });
      }

      if (message?.tool_calls?.length) {
        for (const tc of message.tool_calls) {
          let args: Record<string, unknown>;
          try { args = JSON.parse(tc.function.arguments); } catch { args = {}; }
          output.content.push({ type: "toolCall", id: tc.id, name: tc.function.name, arguments: args });
          const ci = output.content.length - 1;
          stream.push({ type: "toolcall_start", contentIndex: ci, partial: output });
          stream.push({ type: "toolcall_delta", contentIndex: ci, delta: tc.function.arguments ?? "", partial: output });
          stream.push({ type: "toolcall_end", contentIndex: ci, toolCall: output.content[ci] as ToolCall, partial: output });
        }
      }

      if (choice?.finish_reason === "tool_calls") output.stopReason = "toolUse";
      else if (choice?.finish_reason === "length") output.stopReason = "length";
      else output.stopReason = "stop";

      if (data.usage) {
        output.usage.input = data.usage.prompt_tokens ?? 0;
        output.usage.output = data.usage.completion_tokens ?? 0;
        output.usage.totalTokens = data.usage.total_tokens ?? output.usage.input + output.usage.output;
        calculateCost(model, output.usage);
      }

      stream.push({ type: "done", reason: output.stopReason as "stop" | "length" | "toolUse", message: output });
      stream.end();
    } catch (error) {
      output.stopReason = options?.signal?.aborted ? "aborted" : "error";
      output.errorMessage = error instanceof Error ? error.message : String(error);
      log("ERROR", { stopReason: output.stopReason, errorMessage: output.errorMessage });
      stream.push({ type: "error", reason: output.stopReason, error: output });
      stream.end();
    }
  })();

  return stream;
}

// =============================================================================
// Extension
// =============================================================================

export default function (pi: ExtensionAPI) {
  pi.registerProvider("flow-model-provider", {
    name: "Flow Model Provider",
    baseUrl: BASE_URL,
    apiKey: "FLOW_MODEL_PROVIDER_KEY",
    api: "flow-model-provider-api",
    streamSimple: streamFlowModelProvider,
    models: [
      // Amazon Nova
      { id: "amazon-nova-lite",    name: "Amazon Nova Lite",    reasoning: false, input: ["text"], cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, contextWindow: 300000, maxTokens: 5120  },
      { id: "amazon-nova-micro",   name: "Amazon Nova Micro",   reasoning: false, input: ["text"], cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, contextWindow: 128000, maxTokens: 5120  },
      { id: "amazon-nova-pro",     name: "Amazon Nova Pro",     reasoning: false, input: ["text"], cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, contextWindow: 300000, maxTokens: 5120  },
      // Claude
      { id: "claude-v3-opus",      name: "Claude 3 Opus",       reasoning: false, input: ["text"], cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, contextWindow: 200000, maxTokens: 4096  },
      { id: "claude-v3.5-haiku",   name: "Claude 3.5 Haiku",    reasoning: false, input: ["text"], cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, contextWindow: 200000, maxTokens: 8192  },
      { id: "claude-v3.7-sonnet",  name: "Claude 3.7 Sonnet",   reasoning: false, input: ["text"], cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, contextWindow: 200000, maxTokens: 16000 },
      { id: "claude-v4.5-haiku",   name: "Claude 4.5 Haiku",    reasoning: false, input: ["text"], cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, contextWindow: 200000, maxTokens: 8192  },
      { id: "claude-v4.6-opus",    name: "Claude 4.6 Opus",     reasoning: true,  input: ["text"], cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, contextWindow: 200000, maxTokens: 16000 },
      { id: "claude-v4.6-sonnet",  name: "Claude 4.6 Sonnet",   reasoning: true,  input: ["text"], cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, contextWindow: 200000, maxTokens: 16000 },
      // DeepSeek
      { id: "deepseek-r1",         name: "DeepSeek R1",          reasoning: false, input: ["text"], cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, contextWindow: 64000,  maxTokens: 8192  },
      // Llama
      { id: "llama-4-maverick-17b-instruct", name: "Llama 4 Maverick 17B", reasoning: false, input: ["text"], cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, contextWindow: 128000, maxTokens: 4096 },
      { id: "llama-4-scout-17b-instruct",    name: "Llama 4 Scout 17B",    reasoning: true,  input: ["text"], cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, contextWindow: 128000, maxTokens: 4096 },
      { id: "llama3-3-70b-instruct",         name: "Llama 3.3 70B",        reasoning: false, input: ["text"], cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, contextWindow: 128000, maxTokens: 4096 },
      // Mistral
      { id: "mistral-7b-instruct", name: "Mistral 7B Instruct",  reasoning: false, input: ["text"], cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, contextWindow: 32000,  maxTokens: 4096  },
      { id: "mistral-large",       name: "Mistral Large",         reasoning: false, input: ["text"], cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, contextWindow: 128000, maxTokens: 4096  },
      { id: "mistral-large-2",     name: "Mistral Large 2",       reasoning: false, input: ["text"], cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, contextWindow: 128000, maxTokens: 4096  },
      { id: "mixtral-8x7b-instruct", name: "Mixtral 8x7B",        reasoning: false, input: ["text"], cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, contextWindow: 32000,  maxTokens: 4096  },
      // Qwen
      { id: "qwen3-32b",           name: "Qwen3 32B",             reasoning: false, input: ["text"], cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, contextWindow: 128000, maxTokens: 8192  },
      // OpenAI aliases
      { id: "gpt-3.5-turbo",       name: "GPT-3.5 Turbo",         reasoning: false, input: ["text"], cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, contextWindow: 16385,  maxTokens: 4096  },
      { id: "gpt-3.5-turbo-16k",   name: "GPT-3.5 Turbo 16K",     reasoning: false, input: ["text"], cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, contextWindow: 16385,  maxTokens: 4096  },
      { id: "gpt-4",               name: "GPT-4",                  reasoning: false, input: ["text"], cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, contextWindow: 8192,   maxTokens: 4096  },
      { id: "gpt-4-turbo",         name: "GPT-4 Turbo",            reasoning: false, input: ["text"], cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, contextWindow: 128000, maxTokens: 4096  },
      { id: "gpt-4o",              name: "GPT-4o",                 reasoning: false, input: ["text"], cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, contextWindow: 128000, maxTokens: 4096  },
    ],
  });
}
