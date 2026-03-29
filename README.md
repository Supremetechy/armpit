# ARMPIT
## Agentic Relay Messaging Protocol Interactive Transport v1.0

ARMPIT is an open, SMTP-inspired protocol for agent-to-agent messaging. Agents
send structured messages through relays using globally addressable identities,
capability manifests for discovery, and a transport-agnostic envelope that works
over HTTP, TCP, WebSocket, AMQP, SQS, or any custom delivery layer.


## Table of Contents

1. [Core Concepts](#1-core-concepts)
2. [Addressing](#2-addressing)
3. [The Message Schema](#3-the-message-schema)
4. [Capability Manifests](#4-capability-manifests)
5. [Discovery](#5-discovery)
6. [Sending a Message — HTTP Transport](#6-sending-a-message--http-transport)
7. [Sending a Message — TCP Transport](#7-sending-a-message--tcp-transport)
8. [Receiving Messages](#8-receiving-messages)
9. [Pub/Sub — Topics and Subscriptions](#9-pubsub--topics-and-subscriptions)
10. [Idempotency and Retries](#10-idempotency-and-retries)
11. [Federation — Cross-Relay Messaging](#11-federation--cross-relay-messaging)
12. [Running the Reference Relay](#12-running-the-reference-relay)
13. [Implementing an Agent](#13-implementing-an-agent)
14. [Security](#14-security)
15. [Transport Reference](#15-transport-reference)
16. [Message Type Reference](#16-message-type-reference)
17. [Status Code Reference — TCP](#17-status-code-reference--tcp)
18. [Protocol Files in This Repo](#18-protocol-files-in-this-repo)

---

## 1. Core Concepts

| Concept | Description |
|---------|-------------|
| **Agent** | Any autonomous process that can send or receive ARP messages — an AI assistant, a newsletter service, a tool runner, etc. |
| **Relay** | A server that accepts, queues, and delivers messages between agents. Analogous to an SMTP mail server. |
| **Message** | A HTML, text or JSON envelope with sender, recipient, payload, and presentation hints. |
| **Manifest** | A machine-readable JSON document hosted by each agent describing its identity, capabilities, and reachable endpoints. |
| **Topic** | A named pub/sub channel registered on a relay. Agents subscribe to topics; publishers push to them. |

The design is deliberately email-like:

```
Agent A  ──send──►  Relay A  ──(federate if needed)──►  Relay B  ──deliver──►  Agent B
```

---

## 2. Addressing

Every agent has a globally unique address:

```
agent_local@relay_domain
```

Examples:
```
grahm@relay.m-net
newsletter_agent@news.relay.net
research_tool@tools.example.com
```

- `agent_local` — unique within the relay it is registered on.
- `relay_domain` — resolves to the relay host via DNS SRV records.
- `user_id` — an opaque handle scoped *inside* a relay, never exposed globally.
  It maps a message to a specific user the agent is acting on behalf of.

---

## 3. The Message Schema

Every message — regardless of transport — is the same JSON object:

```json
{
  "id":          "msg_01j9z4kq0000000000000000",
  "from_agent":  "newsletter_agent@news.relay.net",
  "to_agent":    "agent@relay.m-net",
  "user_id":     "usr_abc123",
  "type":        "NOTIFICATION",
  "thread_id":   "thread_news_2026_03_27",
  "reply_to":    null,
  "payload": {
    "title": "Today in AI",
    "body":  "Check out the new newsletter.",
    "actions": [
      { "id": "open_link", "label": "Read full edition", "url": "https://example.com/today-in-ai" }
    ]
  },
  "presentation": {
    "channel":      "text+voice",
    "voice_style":  "conversational",
    "summary_hint": "brief"
  },
  "created_at":  "2026-03-27T21:53:00Z",
  "expires_at":  null,
  "status":      "queued"
}
```

### Required fields

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | Relay-assigned or client-supplied. Used for deduplication. |
| `from_agent` | AgentAddress | Sender (`local@domain`). |
| `to_agent` | AgentAddress | Recipient (`local@domain`). |
| `user_id` | string | Opaque user handle scoped to the relay. |
| `type` | MessageType | See [§16](#16-message-type-reference). |
| `payload` | object | Type-specific body. Schema defined per `type`. |
| `created_at` | ISO 8601 | Relay timestamp of acceptance. |
| `status` | enum | `queued` \| `delivered` \| `failed` \| `expired` |

### Optional fields

| Field | Description |
|-------|-------------|
| `thread_id` | Groups a conversation. Carry forward from the first message. |
| `reply_to` | `id` of the message being replied to. |
| `presentation` | Rendering hints (`channel`, `voice_style`, `summary_hint`). |
| `expires_at` | Relay drops the message undelivered after this time. |

---

## 4. Capability Manifests

Every agent publishes a manifest at a well-known URL so relays and peers can
discover it without prior coordination.

**Host at:**
```
https://<your-agent-domain>/.well-known/armpit-manifest.json
```

**Minimal manifest:**

```json
{
  "schema_version": "armpit-manifest-1.0",
  "agent_id":       "myagent@relay.example.com",
  "display_name":   "My Agent",
  "description":    "Does useful things.",
  "owner": {
    "name":    "Your Name",
    "contact": "mailto:you@example.com"
  },
  "endpoints": {
    "primary": {
      "transport": "https",
      "address":   "https://armpit/myagent.example.com",
      "auth": {
        "scheme":     "bearer",
        "value_hint": "https://relay.example.com/token"
      },
      "tls": true
    }
  },
  "capabilities": {
    "message_types": {
      "send":    ["TASK_REQUEST", "PING"],
      "receive": ["TASK_RESULT", "PONG", "NOTIFICATION"]
    }
  },
  "security": {
    "allowed_relays": ["relay.example.com"]
  }
}
```

**Full manifest with TCP + E2E encryption:**

```json
{
  "schema_version": "armpit-manifest-1.0",
  "agent_id":       "agent@relay.m-net",
  "display_name":   "Agent",
  "description":    "Personal AI Assistant",
  "owner": {
    "name":    "M",
    "contact": "mailto:m@example.com"
  },
  "endpoints": {
    "primary": {
      "transport": "https",
      "address":   "https://armpit/aggent.example.com",
      "auth": { "scheme": "bearer", "value_hint": "https://relay.m-net/token" },
      "tls": true
    },
    "tcp": {
      "transport": "tcp",
      "address":   "agent.example.com:2525",
      "auth": { "scheme": "bearer", "value_hint": "https://relay.m-net/token" },
      "tls": true
    }
  },
  "capabilities": {
    "message_types": {
      "send":    ["SUBSCRIBE", "UNSUBSCRIBE", "RESPONSE", "TASK_REQUEST", "PING"],
      "receive": ["SUBSCRIPTION_CONFIRMED", "NOTIFICATION", "TASK_RESULT", "PONG"]
    },
    "topics": [
      { "id": "ai_newsletter", "direction": "receive", "description": "Daily AI news digest." },
      { "id": "system_alerts",  "direction": "receive", "description": "Device and system alerts." }
    ],
    "presentation_profiles": [
      { "id": "voice_primary", "channels": ["voice", "text+voice"], "voice_styles": ["conversational", "brief"] }
    ]
  },
  "security": {
    "allowed_relays": ["relay.m-net"],
    "encryption": {
      "supports_e2e": true,
      "public_keys": [{ "alg": "ed25519", "key": "<BASE64_PUBLIC_KEY>" }]
    }
  },
  "metadata": {
    "homepage": "https://agent.example.com",
    "tags": ["assistant", "personal", "voice"]
  }
}
```

**Set `Cache-Control: max-age=3600`** on this endpoint — manifests change rarely
and every relay that talks to you will fetch it.


## 5. Discovery

### DNS records

Publish these for your relay domain so other relays can find you:

```dns
; How to reach this relay over HTTP and TCP
armpit._http.relay.example.com.  IN SRV  0 5 443  relay.example.com.
armpit._tcp.relay.example.com.   IN SRV  0 5 2525 relay.example.com.

; Where to find a specific agent's manifest
armpit-manifest.myagent.relay.example.com.  IN TXT  "https://myagent.example.com/.well-known/arp-manifest.json"
```

### Resolution flow

When Agent A wants to message `newsletter_agent@news.relay.net`:

1. Parse domain: `news.relay.net`
2. Query `armpit._http.news.relay.net` SRV → relay host + port
3. `POST /agents` on that relay to confirm `newsletter_agent` is registered
4. Optionally fetch `armpit-manifest.newsletter_agent.news.relay.net` TXT to get the
   manifest URL, then GET the manifest for capability negotiation
5. Send the message

For simple cases, senders can skip DNS and pass the relay URL directly.

---

## 6. Sending a Message — HTTP Transport

### Step 1: Register your agent

```bash
curl -X POST https://relay.example.com/armpit/v1/agents \
  -H "Content-Type: application/json" \
  -d '{
    "agent_id":     "myagent@relay.example.com",
    "display_name": "My Agent",
    "manifest_url": "https://myagent.example.com/.well-known/armpit-manifest.json",
    "callback": {
      "transport": "https",
      "address":   "https://myagent.example.com/armpit/inbound",
      "auth":      { "scheme": "bearer" }
    }
  }'
```

Response:
```json
{
  "agent_id":   "myagent@relay.example.com",
  "token":      "eyJhbGciOiJIUzI1NiJ9...",
  "expires_at": null
}
```

Save the token. Pass it as `Authorization: Bearer <token>` on every subsequent
request.

### Step 2: Send a message

```bash
curl -X POST https://relay.example.com/armpit/v1/messages \
  -H "Authorization: Bearer eyJhbGciOiJIUzI1NiJ9..." \
  -H "Content-Type: application/json" \
  -d '{
    "id":         "client-ref-7f3a9b",
    "to_agent":   "agent@relay.m-net",
    "user_id":    "usr_abc123",
    "type":       "NOTIFICATION",
    "payload": {
      "title": "Deploy complete",
      "body":  "Production deployment v2.4.1 succeeded."
    },
    "presentation": { "channel": "text+voice", "summary_hint": "brief" }
  }'
```

Response (`202 Accepted`):
```json
{
  "id":        "msg_01j9z4kq0000000000000000",
  "status":    "queued",
  "federated": true
}
```

If `from_agent` is omitted on the HTTP API, the relay fills it from the
authenticated bearer token. If supplied, it must still match the token subject.

`federated: true` means `agent@relay.m-net` is on a different relay and the
message was forwarded automatically.

### Step 3: Retry safely

Retrying with the same `id` is safe — the relay deduplicates within 24 hours and
returns the same `202` without enqueuing a second copy.

---

## 7. Sending a Message — TCP Transport

Connect to the relay on port **2525** (TLS recommended).

```
# Full session example

S: 220 relay.m-net ARMPIT/1.0 Ready
C: HELO myagent@relay.example.com
S: 250 Hello myagent@relay.example.com
C: AUTH BEARER eyJhbGciOiJIUzI1NiJ9...
S: 235 Auth OK
C: FROM myagent@relay.example.com
S: 250 OK
C: TO grahm@relay.m-net
S: 250 OK
C: USER usr_abc123
S: 250 OK
C: TYPE NOTIFICATION
S: 250 OK
C: ID client-ref-7f3a9b
S: 250 OK
C: DATA
S: 354 Start input; end with <CRLF>.<CRLF>
C: {
C:   "title": "Deploy complete",
C:   "body":  "Production deployment v2.4.1 succeeded."
C: }
C: .
S: 250 Message accepted msg_01j9z4kq0000000000000000
C: QUIT
S: 221 Bye
```

**Rules:**
- Commands and responses are single lines, `CRLF`-terminated.
- `DATA` body is JSON, terminated by a lone `.` on its own line.
- If a JSON line starts with `.`, escape it as `..` (dot-stuffing, identical to SMTP).
- `ID` is optional but enables deduplication — use it when retrying.
- After `DATA` completes, the envelope resets. Send another message in the same
  session with a new `FROM`/`TO`/`USER`/`TYPE`/`DATA` sequence.

**Python example (connecting client):**

```python
import socket, ssl, json

def send_armpit_message(relay_host, token, from_agent, to_agent, user_id,
                     msg_type, payload, msg_id=None):
    ctx = ssl.create_default_context()
    with socket.create_connection((relay_host, 2525)) as raw:
        with ctx.wrap_socket(raw, server_hostname=relay_host) as conn:
            def recv():
                return conn.recv(4096).decode().strip()
            def send(line):
                conn.sendall((line + "\r\n").encode())

            recv()  # 220 banner
            send(f"HELO {from_agent}");       recv()
            send(f"AUTH BEARER {token}");     recv()
            send(f"FROM {from_agent}");       recv()
            send(f"TO {to_agent}");           recv()
            send(f"USER {user_id}");          recv()
            send(f"TYPE {msg_type}");         recv()
            if msg_id:
                send(f"ID {msg_id}");         recv()
            send("DATA");                     recv()  # 354
            body = json.dumps(payload)
            for line in body.splitlines():
                send(".." + line if line.startswith(".") else line)
            send(".");
            print(recv())  # 250 Message accepted ...
            send("QUIT");  recv()
```

---

## 8. Receiving Messages

Agents receive messages in one of two ways:

### Push (webhook / callback)

Register a `callback` endpoint when you call `POST /agents`. The relay will
`POST` each new message to that address as it arrives.

Your inbound handler must:
1. Accept the `Message` JSON body.
2. Return `200 OK` within a reasonable timeout.
3. Call `POST /messages/{msg_id}/ack` after processing (or inline if preferred).

The relay retries delivery on non-2xx responses, so **ack is idempotent** — safe
to call multiple times.

### Poll (long-poll)

If no `callback` is registered, poll your inbox:

```bash
# Immediate poll
curl https://relay.example.com/armpit/v1/messages \
  -H "Authorization: Bearer <token>"

# Long-poll — relay holds connection up to 30s waiting for a message
curl "https://relay.example.com/armpit/v1/messages?wait=30&limit=10" \
  -H "Authorization: Bearer <token>"
```

Response:
```json
{
  "messages": [ { ...Message... }, { ...Message... } ],
  "next_cursor": "cur_xyz"
}
```

After processing each message, acknowledge it:

```bash
curl -X POST https://relay.example.com/armpit/v1/messages/msg_01j9z4kq.../ack \
  -H "Authorization: Bearer <token>"
```

Unacknowledged messages stay in the queue and reappear on the next poll.

---

## 9. Pub/Sub — Topics and Subscriptions

Topics are named channels registered on a relay. A publisher agent sends
`PUBLISH` messages to a topic; all subscribed agents receive a `NOTIFICATION`.

### List available topics

```bash
curl https://relay.example.com/arp/v1/topics
```

```json
{
  "topics": [
    {
      "id":              "ai_newsletter",
      "description":     "Daily AI news digest.",
      "publisher_agent": "newsletter_agent@news.relay.net",
      "tags":            ["news", "ai", "daily"]
    }
  ]
}
```

### Subscribe

```bash
curl -X POST https://relay.example.com/armpit/v1/topics/ai_newsletter/subscriptions \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{ "preferences": { "frequency": "daily" } }'
```

- Returns `201 Created` on first subscription.
- Returns `200 OK` if already subscribed (idempotent).

### Unsubscribe

```bash
curl -X DELETE https://relay.example.com/armpit/v1/topics/ai_newsletter/subscriptions \
  -H "Authorization: Bearer <token>"
```

### Subscribe via message (any transport)

Alternatively, send a `SUBSCRIBE` message directly to the publisher agent:

```json
{
  "from_agent": "agent@relay.m-net",
  "to_agent":   "newsletter_agent@news.relay.net",
  "user_id":    "usr_abc123",
  "type":       "SUBSCRIBE",
  "payload": {
    "topic":       "ai_newsletter",
    "preferences": { "frequency": "daily" }
  }
}
```

The publisher confirms with a `SUBSCRIPTION_CONFIRMED` message back to you.

---

## 10. Idempotency and Retries

ARMPIT is designed for unreliable networks. Every mutating operation is safe to
retry.

| Operation | How to retry safely |
|-----------|---------------------|
| `POST /messages` | Include a client-generated `id`. Same `id` within 24 h = same result, no double-send. |
| `POST /messages/{id}/ack` | Returns `204` whether new or already acked. |
| `POST /topics/.../subscriptions` | Returns `200` if already subscribed. |
| `POST /agents` | Returns `200` if same parameters, refreshes token. |
| TCP `DATA` with `ID` set | Relay responds `250` to duplicate without re-enqueuing. |

**Recommended client pattern:**

```python
import uuid, httpx, time

def send_with_retry(relay, token, message, max_attempts=3):
    message["id"] = message.get("id") or f"client-{uuid.uuid4().hex}"
    for attempt in range(max_attempts):
        try:
            r = httpx.post(
                f"{relay}/armpit/v1/messages",
                json=message,
                headers={"Authorization": f"Bearer {token}"},
                timeout=10,
            )
            r.raise_for_status()
            return r.json()
        except (httpx.TimeoutException, httpx.HTTPStatusError) as e:
            if attempt == max_attempts - 1:
                raise
            time.sleep(2 ** attempt)   # exponential backoff
```

Generate `id` once per logical message, before the first attempt. Reuse it on
retries. Never generate a new `id` on retry — that defeats deduplication.

---

## 11. Federation — Cross-Relay Messaging

Relays forward messages to peer relays automatically when `to_agent` is on a
different domain.

```
myagent@relay.example.com  →  relay.example.com  →  relay.m-net  →  agent@relay.m-net
```

From the sender's perspective this is invisible — send to `grahm@relay.m-net`
from any relay and it routes. `SendMessageResponse.federated = true` confirms
cross-relay delivery was used.

**For relay operators:** publish SRV records (see §5) and include your relay in
`RelayInfo.federation.peers`. Relays resolve the recipient's domain via DNS to
find the peer relay.

---

## 12. Running the Reference Relay

`armpit.py` is a minimal but functional ARP/TCP relay.

### Requirements

- Python 3.11+
- No external dependencies

### Start

```bash
python3 armpit.py
# ARP/TCP relay listening on 0.0.0.0:2525  (relay.m-net)
```

### Configuration

Edit the constants at the top of `armpit.py`:

```python
HOST      = "0.0.0.0"     # bind address
PORT      = 2525           # TCP port
RELAY_ID  = "relay.m-net" # your relay's domain identity
DEDUP_TTL = 86400          # dedup window in seconds (24 h)
```

### What it does

- Accepts TCP connections, one thread per connection.
- Implements the full `HELO → AUTH → FROM → TO → USER → TYPE → [ID] → DATA → QUIT`
  session flow.
- In-memory message queue (swap `enqueue_message()` for Redis, Postgres, SQS, etc.).
- Token validation stub in `validate_token()` — replace with real JWT verification.

### Swap the queue backend

```python
# armpit.py — replace enqueue_message() for production
import redis

r = redis.Redis()

def enqueue_message(msg: dict) -> None:
    r.lpush(f"armpit:inbox:{msg['to_agent']}", json.dumps(msg))
```

---

## 13. Implementing an Agent

A minimal agent needs four things:

### 1. A manifest

Host `/.well-known/armpit-manifest.json` on your domain. Declare what message types
you can send and receive. See [§4](#4-capability-manifests).

### 2. Registration

On startup, `POST /agents` to your relay with your `agent_id`, `manifest_url`,
and optional `callback` endpoint.

### 3. A message sender

Build the `Message` object, generate a client `id`, call `POST /messages` (or
open a TCP session). Retry with exponential backoff using the same `id`.

### 4. A message receiver

Either register a `callback` endpoint that accepts `POST` with a `Message` body,
or run a poll loop against `GET /messages?wait=30`.

### Minimal Python agent skeleton

```python
import uuid, time, httpx

RELAY    = "https://relay.example.com/armpit/v1"
AGENT_ID = "myagent@relay.example.com"
TOKEN    = None  # set after registration

def register(manifest_url: str, callback_url: str | None = None) -> str:
    global TOKEN
    body = {
        "agent_id":     AGENT_ID,
        "display_name": "My Agent",
        "manifest_url": manifest_url,
    }
    if callback_url:
        body["callback"] = {"transport": "https", "address": callback_url,
                            "auth": {"scheme": "bearer"}}
    r = httpx.post(f"{RELAY}/agents", json=body)
    r.raise_for_status()
    TOKEN = r.json()["token"]
    return TOKEN

def send_message(to_agent: str, user_id: str, msg_type: str,
                 payload: dict, msg_id: str | None = None) -> dict:
    msg_id = msg_id or f"client-{uuid.uuid4().hex}"
    r = httpx.post(
        f"{RELAY}/messages",
        json={
            "id":         msg_id,
            "from_agent": AGENT_ID,
            "to_agent":   to_agent,
            "user_id":    user_id,
            "type":       msg_type,
            "payload":    payload,
        },
        headers={"Authorization": f"Bearer {TOKEN}"},
    )
    r.raise_for_status()
    return r.json()

def poll_inbox(wait: int = 30) -> list[dict]:
    r = httpx.get(
        f"{RELAY}/messages",
        params={"wait": wait, "limit": 20},
        headers={"Authorization": f"Bearer {TOKEN}"},
        timeout=wait + 5,
    )
    r.raise_for_status()
    return r.json()["messages"]

def ack(msg_id: str) -> None:
    httpx.post(
        f"{RELAY}/messages/{msg_id}/ack",
        headers={"Authorization": f"Bearer {TOKEN}"},
    ).raise_for_status()

def run_poll_loop(handler):
    """handler(msg: dict) -> None"""
    while True:
        for msg in poll_inbox(wait=30):
            try:
                handler(msg)
            finally:
                ack(msg["id"])  # ack is idempotent — safe even if handler raises

# Usage:
# register("https://myagent.example.com/.well-known/armpit-manifest.json")
# run_poll_loop(lambda msg: print(msg["type"], msg["payload"]))
```

---

## 14. Security

### Authentication

Every request to the relay requires `Authorization: Bearer <token>`. The token
is issued by the relay at registration (`POST /agents`). For TCP sessions, it is
passed via `AUTH BEARER <token>`.

Replace `validate_token()` in `armpit.py` with real JWT verification:

```python
import jwt  # PyJWT

SECRET = "your-relay-signing-secret"

def validate_token(token: str) -> bool:
    try:
        jwt.decode(token, SECRET, algorithms=["HS256"])
        return True
    except jwt.InvalidTokenError:
        return False
```

### SPF-equivalent: `allowed_relays`

The manifest field `security.allowed_relays` lists relay domains authorized to
deliver messages on behalf of this agent. Receiving agents and relays SHOULD
reject or flag messages arriving from unlisted relays — analogous to SPF in email.

### End-to-end encryption

If `security.encryption.supports_e2e = true`, publish an `ed25519` public key in
your manifest. Senders encrypt `payload` before sending; only the recipient can
decrypt it. The relay never sees the plaintext payload.

### TLS

Always use TLS for HTTP (`https://`) and TCP (set `tls: true` in your manifest
endpoint). The reference relay supports `STARTTLS` via the `CAPA` advertised
feature list.

### Sender verification

The relay MUST verify that `from_agent` in the message matches the authenticated
agent's registered `agent_id`. Mismatches return `550 Sender mismatch`.

---

## 15. Transport Reference

The ARMPIT `Message` schema is transport-agnostic. Agents advertise which transports
they support in their manifest `endpoints` map. The relay picks the best mutual
option.

| Transport | Address format | Default port | Notes |
|-----------|---------------|--------------|-------|
| `https` | `https://host/path` | 443 | Recommended default. |
| `http` | `http://host/path` | 80 | Non-TLS, internal networks only. |
| `tcp` | `host:port` | 2525 | Native ARMPIT line protocol (see `armpt_tcp_protocol.md`). |
| `websocket` | `wss://host/path` | 443 | Persistent bidirectional channel. |
| `amqp` | `amqps://host/vhost/queue` | 5671 | `options.exchange`, `options.routing_key` |
| `smtp` | `mailto:agent@domain` | 25/587 | Wraps ARP message in email body. |
| `sqs` | Queue URL or ARN | — | `options.region` required. |
| `custom` | Any string | — | Relay and agent must share out-of-band knowledge. |

**Selecting a transport:**

1. Sender reads recipient's manifest `endpoints`.
2. Sender picks the first entry whose `transport` it supports.
3. Auth hint (`auth.value_hint`) tells the sender where to get credentials.
4. `options` carries transport-specific config (AMQP exchange, SQS region, etc.).

---

## 16. Message Type Reference

| Type | Direction | Description |
|------|-----------|-------------|
| `SUBSCRIBE` | sender → publisher | Request subscription to a topic. |
| `UNSUBSCRIBE` | sender → publisher | Cancel a subscription. |
| `SUBSCRIPTION_CONFIRMED` | publisher → sender | Subscription acknowledged. |
| `PUBLISH` | publisher → relay | Push a message to all topic subscribers. |
| `NOTIFICATION` | any → any | Deliver a notification or alert. |
| `RESPONSE` | any → any | Reply to a prior message (set `reply_to`). |
| `TASK_REQUEST` | any → tool agent | Delegate a task. |
| `TASK_RESULT` | tool agent → any | Return task output. |
| `ERROR` | any → any | Signal a protocol or application-level error. |
| `PING` | any → any | Liveness check. |
| `PONG` | any → any | Liveness reply to `PING`. |

**Extension types:** use reverse-domain notation to avoid collisions:
```
com.example.SUMMARIZE_REQUEST
io.myapp.CALENDAR_EVENT
```

Agents declare supported types in their manifest under
`capabilities.message_types.send` and `capabilities.message_types.receive`.
Relays SHOULD pass unknown types through; receiving agents SHOULD ignore types
not listed in their manifest.

---

## 17. Status Code Reference — TCP

| Code | Meaning |
|------|---------|
| `220` | Service ready — server banner |
| `221` | Closing connection |
| `235` | Authentication successful |
| `250` | OK / command accepted / message accepted |
| `354` | Start input; end with `<CRLF>.<CRLF>` |
| `450` | Temporary failure — try again |
| `500` | Syntax error or unknown command |
| `503` | Command out of sequence |
| `530` | Authentication required or failed |
| `550` | Permanent failure (agent not found, sender mismatch) |

---

## 18. Protocol Files in This Repo

| File | Description |
|------|-------------|
| `OpenAPI.yaml` | Full HTTP/JSON API spec — schemas, endpoints, examples. |
| `armpit_tcp_protocol.md` | ARP/TCP line protocol spec — commands, status codes, session flow. |
| `armpit.py` | Reference ARP/TCP relay implementation (Python 3.11+). |
| `manifest.json` | Example capability manifest for `grahm@relay.m-net`. |
| `example_message.json` | Example `NOTIFICATION` message. |
| `EndPoints.yaml` | Human-readable endpoint quick-reference. |

---

## Quick-start checklist

- [ ] Host `/.well-known/armpit-manifest.json` with your `agent_id`, endpoints, and capability types
- [ ] Publish DNS SRV records for your relay domain
- [ ] `POST /agents` to register on your relay — save the token
- [ ] Choose a receive strategy: webhook callback or poll loop
- [ ] Generate a client `id` per message; reuse it on retries
- [ ] Acknowledge every received message with `POST /messages/{id}/ack`
- [ ] Declare `allowed_relays` in your manifest to prevent spoofing
- [ ] Use TLS on all transports in production
# armpit
