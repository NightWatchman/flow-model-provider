# flow-model-provider

A pi extension that integrates a custom OpenAI-compatible model provider. It translates pi's internal message format into OpenAI Chat Completions API requests, surfaces a curated list of models (Amazon Nova, Claude, DeepSeek, Llama, Mistral, Qwen, and OpenAI aliases), and emits a fully-typed `AssistantMessageEventStream` back to the agent.

---

## Setup

### 1. Set the API key environment variable

The extension reads your API key from the environment variable **`FLOW_MODEL_PROVIDER_KEY`**.

```bash
export FLOW_MODEL_PROVIDER_KEY="your-api-key-here"
```

Add it to your shell profile (e.g. `~/.bashrc`, `~/.zshrc`) or however you manage secrets so it is available at runtime.

The key is passed as a `Bearer` token in the `Authorization` header of every request. If the variable is not set the header will be sent with an empty string, which will cause authentication errors.

### 2. Update the base URL

The base URL is hardcoded at the top of `extensions/flow-model-provider.ts` and **must be changed** to point to your actual model provider endpoint before use:

```ts
// Change this to the URL of your model provider
const BASE_URL = "https://my-model-provider.com/v1";
```

Replace `https://my-model-provider.com/v1` with the root URL of your provider's OpenAI-compatible API. All requests are sent to `${BASE_URL}/chat/completions`.

---

## Available models

The following model IDs are registered out of the box. All cost fields default to `0` and should be updated to match your provider's pricing.

| Family | Model ID | Name | Context window | Max output tokens |
|---|---|---|---|---|
| Amazon Nova | `amazon-nova-lite` | Amazon Nova Lite | 300 000 | 5 120 |
| | `amazon-nova-micro` | Amazon Nova Micro | 128 000 | 5 120 |
| | `amazon-nova-pro` | Amazon Nova Pro | 300 000 | 5 120 |
| Claude | `claude-v3-opus` | Claude 3 Opus | 200 000 | 4 096 |
| | `claude-v3.5-haiku` | Claude 3.5 Haiku | 200 000 | 8 192 |
| | `claude-v3.7-sonnet` | Claude 3.7 Sonnet | 200 000 | 16 000 |
| | `claude-v4.5-haiku` | Claude 4.5 Haiku | 200 000 | 8 192 |
| | `claude-v4.6-opus` | Claude 4.6 Opus | 200 000 | 16 000 |
| | `claude-v4.6-sonnet` | Claude 4.6 Sonnet | 200 000 | 16 000 |
| DeepSeek | `deepseek-r1` | DeepSeek R1 | 64 000 | 8 192 |
| Llama | `llama-4-maverick-17b-instruct` | Llama 4 Maverick 17B | 128 000 | 4 096 |
| | `llama-4-scout-17b-instruct` | Llama 4 Scout 17B | 128 000 | 4 096 |
| | `llama3-3-70b-instruct` | Llama 3.3 70B | 128 000 | 4 096 |
| Mistral | `mistral-7b-instruct` | Mistral 7B Instruct | 32 000 | 4 096 |
| | `mistral-large` | Mistral Large | 128 000 | 4 096 |
| | `mistral-large-2` | Mistral Large 2 | 128 000 | 4 096 |
| | `mixtral-8x7b-instruct` | Mixtral 8x7B | 32 000 | 4 096 |
| Qwen | `qwen3-32b` | Qwen3 32B | 128 000 | 8 192 |
| OpenAI | `gpt-3.5-turbo` | GPT-3.5 Turbo | 16 385 | 4 096 |
| | `gpt-3.5-turbo-16k` | GPT-3.5 Turbo 16K | 16 385 | 4 096 |
| | `gpt-4` | GPT-4 | 8 192 | 4 096 |
| | `gpt-4-turbo` | GPT-4 Turbo | 128 000 | 4 096 |
| | `gpt-4o` | GPT-4o | 128 000 | 4 096 |

To add, remove, or modify models, edit the `models` array inside `extensions/flow-model-provider.ts`.

---

## Logging

All requests, responses, and errors are appended to a log file for debugging:

```
~/.pi/agent/flow-model-provider.log
```

---

## How it works

1. The extension registers itself with pi under the provider ID `flow-model-provider`.
2. When a model is invoked, `streamFlowModelProvider` converts pi's internal `Message[]` (including text, images, and tool results) into the OpenAI Chat Completions message format.
3. A single non-streaming `POST /chat/completions` request is made to your provider.
4. The JSON response is unpacked and re-emitted as an `AssistantMessageEventStream` so the rest of the pi agent pipeline sees a consistent streaming interface.
