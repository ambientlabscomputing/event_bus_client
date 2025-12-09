# mTLS Authentication Guide

The event bus client now supports dual authentication: **JWT tokens** (existing) and **mTLS certificates** (new).

## Authentication Methods

### Option 1: JWT Token Authentication (Existing)

```go
opts := event_bus_client.EventClientOpts{
    Endpoint:       "ws://localhost:9000",
    AuthToken:      "your-jwt-token",
    CommitInterval: "5s",
    GroupID:        "my-consumer-group",
}

client, err := event_bus_client.NewEventClient(opts)
if err != nil {
    log.Fatal(err)
}
```

### Option 2: mTLS Certificate Authentication (New)

```go
opts := event_bus_client.EventClientOpts{
    Endpoint:       "ws://localhost:9000",
    CertPath:       "/path/to/client-cert.pem",
    KeyPath:        "/path/to/private-key.pem",
    CommitInterval: "5s",
    GroupID:        "my-consumer-group",
    // AuthToken is not required when using mTLS
}

client, err := event_bus_client.NewEventClient(opts)
if err != nil {
    log.Fatal(err)
}
```

## CLI Usage

### JWT Authentication
```bash
eventbus_cli subscribe my-topic \
  --endpoint ws://localhost:9000 \
  --token "your-jwt-token" \
  --group-id my-group
```

### mTLS Authentication
```bash
eventbus_cli subscribe my-topic \
  --endpoint ws://localhost:9000 \
  --cert /path/to/client-cert.pem \
  --key /path/to/private-key.pem \
  --group-id my-group
```

### Configuration File

Create `~/.eventbus_cli/config.yaml`:

**For JWT:**
```yaml
endpoint: ws://localhost:9000
token: your-jwt-token
group_id: my-group
```

**For mTLS:**
```yaml
endpoint: ws://localhost:9000
cert_path: /path/to/client-cert.pem
key_path: /path/to/private-key.pem
group_id: my-group
```

## How mTLS Works

### WebSocket Connections
- Certificate sent via `X-Client-Certificate` header (base64 encoded)
- No query parameter `?token=` needed
- Server validates certificate against its CA

### HTTP Publishing
- Certificate sent via `X-Client-Certificate` header
- Request signed with private key, signature in `X-Client-Signature` header
- Timestamp in `X-Request-Timestamp` header for replay protection
- Signature payload: `METHOD\nPATH\nTIMESTAMP\nBODY`

## Certificate Requirements

- **Format**: PEM-encoded ECDSA certificates
- **Algorithm**: P-256 (secp256r1) curve
- **Organization Field**: Must contain organization UUID
- **Common Name**: Server/client identifier
- **Signed by**: Event bus server's CA

## Obtaining Certificates

Certificates are typically obtained through the server's registration flow:

1. Generate CSR with organization ID in Organization field
2. Submit CSR to server's CA endpoint
3. Receive signed certificate
4. Use certificate + private key for authentication

## Error Handling

```go
client, err := event_bus_client.NewEventClient(opts)
if err != nil {
    // Handle errors:
    // - Invalid certificate format
    // - Private key mismatch
    // - Missing files
    log.Fatal(err)
}

err = client.Connect(ctx, nil)
if err != nil {
    // Handle connection errors:
    // - Certificate rejected by server
    // - Certificate expired
    // - CA validation failed
    log.Fatal(err)
}
```

## Migration from JWT to mTLS

1. **Dual mode** (recommended for transition):
   ```go
   // Try mTLS first, fallback to JWT
   opts := event_bus_client.EventClientOpts{
       Endpoint:       "ws://localhost:9000",
       AuthToken:      jwtToken,      // Fallback
       CertPath:       certPath,      // Primary
       KeyPath:        keyPath,       // Primary
       CommitInterval: "5s",
       GroupID:        "my-group",
   }
   ```

2. **Pure mTLS** (after full migration):
   ```go
   opts := event_bus_client.EventClientOpts{
       Endpoint:       "ws://localhost:9000",
       CertPath:       certPath,
       KeyPath:        keyPath,
       CommitInterval: "5s",
       GroupID:        "my-group",
       // No AuthToken
   }
   ```

## Security Notes

- Private keys should never be committed to version control
- Use file permissions `0600` for private key files
- Rotate certificates periodically
- Monitor certificate expiration dates
- Use separate certificates per server/client
- Organization ID in certificate enables multi-tenant authorization
