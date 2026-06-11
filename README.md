# flow-model-provider

A pi extension for consuming OpenAI-compatible model providers that do not support streaming
(i.e. providers where the [`stream` parameter](https://developers.openai.com/api/reference/resources/chat/subresources/completions/methods/create#(resource)%20chat.completions%20%3E%20(method)%20create%20%3E%20(params)%200.non_streaming%20%3E%20(param)%20stream%20%3E%20(schema))
is unsupported or unavailable). Instead of opening a Server-Sent Events stream,
it issues a single non-streaming `POST /chat/completions` request, waits for the complete response, and then re-emits
it as a fully-typed `AssistantMessageEventStream` so the rest of the pi agent pipeline sees a consistent streaming 
interface — no changes to downstream code required.

It also translates pi's internal message format into OpenAI Chat Completions API requests.

---

## Setup

### 1. Set the API key environment variable

The extension reads your API key from the environment variable **`FLOW_MODEL_PROVIDER_KEY`**.

```bash
export FLOW_MODEL_PROVIDER_KEY="your-api-key-here"
```

Add it to your shell profile (e.g. `~/.bashrc`, `~/.zshrc`) or however you manage secrets so it is available at runtime.

The key is passed as a `Bearer` token in the `Authorization` header of every request. If the variable is not set, pi will display an error when a model is first invoked.

### 2. Set the base URL environment variable

The base URL is read from the environment variable **`FLOW_MODEL_PROVIDER_URL`**.

```bash
export FLOW_MODEL_PROVIDER_URL="https://your-provider.com/v1"
```

Add it to your shell profile (e.g. `~/.bashrc`, `~/.zshrc`) so it is available at runtime. All requests are sent to `${FLOW_MODEL_PROVIDER_URL}/chat/completions`.

If the variable is not set, pi will refuse to load the extension and display an error at startup.

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
