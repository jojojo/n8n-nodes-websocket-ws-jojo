# n8n-nodes-websocket-ws-prueba

Community node for n8n that connects to any WebSocket server and triggers your workflow on incoming messages. Supports authentication, extra query parameters, and automatic reconnection.

## Features

- **Events triggered:** `open`, `message`, `close`, `error`
- **Authentication:** Header (Bearer / API Key) or Query Parameter — stored as n8n credentials
- **Extra query parameters:** Add any number of key-value pairs appended to the URL
- **Auto-reconnect:** Configurable interval and max attempt limit (0 = unlimited)
- **Initial message:** Send a payload immediately after connecting
- **Return WS resource:** Expose the raw WebSocket object for sending reply messages

## Installation

In n8n go to **Settings → Community Nodes → Install** and enter:

```
n8n-nodes-websocket-ws-prueba
```

## Node Parameters

| Parameter | Description |
|---|---|
| WebSocket URL | Target WebSocket server (e.g. `wss://example.com/ws`) |
| Authentication | None / Header Auth / Query Auth |
| Query Parameters | Extra key-value pairs appended to the URL |
| Auto Reconnect | Re-establish connection on drop |
| Reconnect Interval | Seconds between reconnect attempts |
| Max Reconnect Attempts | 0 = unlimited |
| Send Initial Message | Send a message right after connecting |
| Initial Message | Payload to send on connect |
| Return WS Resource | Include the ws object in output |

## Output

Each trigger execution returns a JSON object:

```json
{ "event": "message", "message": { ... } }
{ "event": "open" }
{ "event": "close" }
{ "event": "error", "message": "..." }
```

## Credentials

- **WebSocket Header Auth** — header name + value (e.g. `Authorization: Bearer token`)
- **WebSocket Query Auth** — parameter name + value (e.g. `?token=abc123`)

## License

MIT
