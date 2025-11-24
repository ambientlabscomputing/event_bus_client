# Event Bus V4 - Client Optimization Guide

## Overview

This guide helps you choose the optimal connection strategy for your Event Bus V4 client application. The event bus now supports multiple publish methods with different performance characteristics and use cases.

## Connection Methods Comparison

| Feature | WebSocket | HTTP POST (`/api/v1/publish`) |
|---------|-----------|-------------------------------|
| **Connection Overhead** | High (TLS + WS upgrade + JWT verification) | Medium (TLS + JWT verification) |
| **Latency (first message)** | 76-182ms | 10-30ms |
| **Latency (subsequent)** | ~5-15ms (connection reused) | 10-30ms (new connection) |
| **Bidirectional** | ✅ Yes | ❌ No (publish only) |
| **Real-time updates** | ✅ Yes | ❌ No |
| **Subscription support** | ✅ Yes | ❌ No |
| **Best for** | Interactive applications, subscribers | Fire-and-forget publishers |
| **Connection persistence** | Yes (with ping/pong keep-alive) | No (closes immediately) |
| **Memory footprint** | Higher (persistent connection) | Lower (ephemeral) |

## When to Use HTTP POST

✅ **Use HTTP POST (`/api/v1/publish`) when:**

- You only need to **publish messages** (no subscriptions)
- Your application publishes **infrequently** (<1 message/second per client)
- You want **minimal client-side complexity** (no WebSocket handling)
- Your client is **stateless** or **short-lived** (serverless functions, batch jobs)
- You're publishing from **many distributed clients** (IoT devices, mobile apps)
- You want **fire-and-forget** semantics with no delivery confirmation needed

### HTTP POST Example

```bash
# Using curl
curl -X POST https://eventbus.example.com/api/v1/publish \
  -H "Authorization: Bearer YOUR_JWT_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "topic": "events",
    "content": "Hello World",
    "org_id": "org-123",
    "trace_id": "trace-456"
  }'

# Response: 202 Accepted
{
  "status": "accepted",
  "message": "Message accepted for publishing",
  "topic": "events"
}
```

```python
# Python example
import requests

def publish_event(token, topic, content):
    response = requests.post(
        'https://eventbus.example.com/api/v1/publish',
        headers={'Authorization': f'Bearer {token}'},
        json={
            'topic': topic,
            'content': content,
            'org_id': 'org-123'
        }
    )
    return response.status_code == 202

# Usage
publish_event(jwt_token, 'events', 'Hello World')
```

## When to Use WebSocket

✅ **Use WebSocket when:**

- You need to **publish and subscribe** (bidirectional communication)
- Your application publishes **frequently** (>1 message/second)
- You need **real-time updates** or **live subscriptions**
- You can **maintain persistent connections** (long-lived clients)
- You want **publish confirmation** (offset returned)
- You're building **interactive applications** (dashboards, chat, collaboration tools)

### WebSocket Connection Reuse Best Practices

**❌ Anti-pattern: Create connection per message**
```python
# DON'T DO THIS - Creates 100 connections!
for i in range(100):
    conn = websocket.connect('wss://eventbus.example.com/ws?token=...')
    conn.send(json.dumps({
        'message_type': 'event',
        'payload': {'topic': 'events', 'content': f'Message {i}'}
    }))
    conn.close()  # Wastes connection overhead
```

**✅ Best practice: Reuse single connection**
```python
# DO THIS - Reuses 1 connection for 100 messages
conn = websocket.connect('wss://eventbus.example.com/ws?token=...')
for i in range(100):
    conn.send(json.dumps({
        'message_type': 'event',
        'payload': {'topic': 'events', 'content': f'Message {i}'}
    }))
# Connection stays open for future messages
```

### WebSocket Keep-Alive

The event bus sends **ping messages every 30 seconds** (configurable). Your client must:

1. **Respond to pings with pongs** (most WebSocket libraries do this automatically)
2. **Handle ping/pong timeouts** (connection closes after 60 seconds without pong)
3. **Implement reconnection logic** for network interruptions

```python
# Python websocket-client library handles ping/pong automatically
import websocket

ws = websocket.WebSocketApp(
    'wss://eventbus.example.com/ws?token=...',
    on_message=lambda ws, msg: print(f'Received: {msg}'),
    on_error=lambda ws, err: print(f'Error: {err}'),
    on_close=lambda ws, code, msg: print('Connection closed'),
)

# Run with automatic ping/pong handling
ws.run_forever(ping_interval=30, ping_timeout=10)
```

## Performance Optimization Tips

### 1. JWT Token Caching

The event bus caches validated JWT tokens for **5 minutes** (configurable). To benefit:

- **Reuse the same token** across multiple requests/connections
- Don't generate a new token for every publish
- Tokens are cached by value, so same token = instant validation

```python
# ✅ Good: Single token, multiple requests
token = get_jwt_token()  # Fetch once
for i in range(1000):
    publish_http(token, 'events', f'Message {i}')  # JWT cached after first request

# ❌ Bad: New token every time
for i in range(1000):
    token = get_jwt_token()  # Fetches new token - defeats cache
    publish_http(token, 'events', f'Message {i}')
```

### 2. Connection Affinity

The event bus routes connections by **user ID hash** (from JWT `sub` claim). This means:

- **Same user always routes to same broker worker**
- Improves JWT cache hit rate
- Reduces cross-worker coordination

To benefit:
- Use **consistent tokens** (don't rotate mid-session)
- **Pool connections per user** in multi-tenant applications

### 3. HTTP/2 Connection Pooling

If using HTTP POST extensively, enable **HTTP/2 connection pooling** in your client:

```python
import httpx  # Modern HTTP client with HTTP/2 support

# Create persistent client with connection pooling
client = httpx.Client(http2=True)

# Reuse client for multiple requests
for i in range(1000):
    response = client.post(
        'https://eventbus.example.com/api/v1/publish',
        headers={'Authorization': f'Bearer {token}'},
        json={'topic': 'events', 'content': f'Message {i}'}
    )
```

### 4. Batch Publishing (Future Enhancement)

For high-throughput scenarios, consider batching messages:

```python
# Future API (not yet implemented)
POST /api/v1/publish/batch
[
  {"topic": "events", "content": "Message 1"},
  {"topic": "events", "content": "Message 2"},
  {"topic": "events", "content": "Message 3"}
]
```

## Decision Flowchart

```
Start
  |
  v
Need subscriptions? ──Yes──> Use WebSocket
  |
  No
  |
  v
Publish >10 msg/sec? ──Yes──> Use WebSocket
  |
  No
  |
  v
Long-lived client? ──Yes──> Use WebSocket
  |
  No
  |
  v
Use HTTP POST /api/v1/publish
```

## Real-World Use Cases

### Use Case 1: Mobile App (Push Notifications)
**Recommendation:** HTTP POST

- Mobile apps connect infrequently
- Battery-conscious (persistent connections drain battery)
- Simple fire-and-forget semantics
- Handles network interruptions naturally

```swift
// iOS example
func publishEvent(topic: String, content: String) {
    let url = URL(string: "https://eventbus.example.com/api/v1/publish")!
    var request = URLRequest(url: url)
    request.httpMethod = "POST"
    request.addValue("Bearer \(jwtToken)", forHTTPHeaderField: "Authorization")
    request.httpBody = try? JSONEncoder().encode([
        "topic": topic,
        "content": content
    ])
    
    URLSession.shared.dataTask(with: request).resume()
}
```

### Use Case 2: Real-Time Dashboard
**Recommendation:** WebSocket

- Needs live updates (subscriptions)
- High message volume
- Long-lived browser session
- Bidirectional communication

```javascript
// Browser example
const ws = new WebSocket('wss://eventbus.example.com/ws?token=' + jwtToken);

// Subscribe to events
ws.send(JSON.stringify({
  message_type: 'subscription',
  payload: {
    action: 'subscribe',
    topic: 'metrics',
    org_id: 'org-123'
  }
}));

// Receive updates
ws.onmessage = (event) => {
  const data = JSON.parse(event.data);
  updateDashboard(data);
};

// Publish events on same connection
ws.send(JSON.stringify({
  message_type: 'event',
  payload: {
    topic: 'user-actions',
    content: 'Button clicked'
  }
}));
```

### Use Case 3: Serverless Function (AWS Lambda)
**Recommendation:** HTTP POST

- Stateless, short-lived execution
- No persistent connections possible
- Simple request/response model
- Cost-optimized (no idle connections)

```javascript
// AWS Lambda example
exports.handler = async (event) => {
    const response = await fetch('https://eventbus.example.com/api/v1/publish', {
        method: 'POST',
        headers: {
            'Authorization': `Bearer ${process.env.JWT_TOKEN}`,
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({
            topic: 'lambda-events',
            content: JSON.stringify(event),
            trace_id: event.requestContext.requestId
        })
    });
    
    return {
        statusCode: response.status === 202 ? 200 : 500,
        body: JSON.stringify({ published: response.status === 202 })
    };
};
```

### Use Case 4: High-Throughput Data Ingestion
**Recommendation:** WebSocket (with connection pooling)

- Thousands of messages per second
- Connection overhead would dominate HTTP POST
- Maintains a pool of persistent connections

```python
# Connection pool for high throughput
import queue
import threading

class WebSocketPool:
    def __init__(self, size=10):
        self.pool = queue.Queue(maxsize=size)
        for _ in range(size):
            conn = websocket.create_connection(
                'wss://eventbus.example.com/ws?token=...'
            )
            self.pool.put(conn)
    
    def publish(self, topic, content):
        conn = self.pool.get()  # Get connection from pool
        try:
            conn.send(json.dumps({
                'message_type': 'event',
                'payload': {'topic': topic, 'content': content}
            }))
        finally:
            self.pool.put(conn)  # Return to pool

# Usage
pool = WebSocketPool(size=10)
for i in range(10000):
    pool.publish('events', f'High-throughput message {i}')
```

## Monitoring & Metrics

Track these metrics to optimize your client:

### HTTP POST Metrics
- **202 response rate** (should be >99%)
- **503 errors** (indicates queue backpressure)
- **P50/P95/P99 latency** (should be 10-30ms)

### WebSocket Metrics
- **Connection duration** (longer = better amortization of overhead)
- **Messages per connection** (higher = more efficient)
- **Reconnection rate** (lower = more stable)
- **Ping timeout rate** (should be near 0%)

## Configuration Reference

Server-side settings (configured in `config.yaml`):

```yaml
connection_optimization:
  jwt_cache_ttl_minutes: 5              # JWT validation cache duration
  websocket_ping_interval_seconds: 30   # How often server sends pings
  websocket_pong_wait_seconds: 60       # Timeout for client pong response
  http2_enabled: true                   # HTTP/2 multiplexing
  tls_session_cache_size: 1024          # TLS session resumption cache
  websocket_read_buffer_size: 4096      # WebSocket buffer (bytes)
  websocket_write_buffer_size: 4096     # WebSocket buffer (bytes)
```

## Troubleshooting

### "Publish queue full" (HTTP POST 503)
**Cause:** System overloaded or network partition
**Solution:**
- Implement exponential backoff retry
- Reduce publish rate
- Check system metrics (CPU, memory)

### Frequent WebSocket disconnections
**Cause:** Ping/pong timeout or network instability
**Solution:**
- Ensure client properly handles ping frames
- Increase `websocket_pong_wait_seconds` on server
- Implement reconnection logic with exponential backoff

### High JWT verification latency
**Cause:** JWT cache misses (new tokens)
**Solution:**
- Reuse JWT tokens across requests
- Increase `jwt_cache_ttl_minutes` on server (if token revocation latency acceptable)

### HTTP POST slower than expected
**Cause:** TLS handshake overhead
**Solution:**
- Enable HTTP/2 with connection pooling in client
- Use persistent HTTP client (don't create new client per request)
- Consider WebSocket if publishing >10 msg/sec

## Migration Guide

### Migrating from WebSocket-only to HTTP POST

**Before:**
```python
# Every publish creates WebSocket connection
for event in events:
    ws = websocket.create_connection('wss://...')
    ws.send(json.dumps({'message_type': 'event', ...}))
    ws.close()
```

**After:**
```python
# Use HTTP POST for fire-and-forget
import requests

session = requests.Session()  # Connection pooling
for event in events:
    session.post(
        'https://eventbus.example.com/api/v1/publish',
        headers={'Authorization': f'Bearer {token}'},
        json=event
    )
```

**Performance improvement:** 3-5x faster for low-frequency publishing

---

## Summary

- **HTTP POST**: Best for fire-and-forget, infrequent publishes, stateless clients
- **WebSocket**: Best for subscriptions, high-frequency publishes, interactive apps
- **Reuse connections** whenever possible
- **Enable HTTP/2** and connection pooling for HTTP clients
- **Monitor metrics** to validate optimization effectiveness

For questions or support, contact the Event Bus team or file an issue in the repository.
