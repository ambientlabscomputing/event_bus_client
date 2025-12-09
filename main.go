package event_bus_client

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// TODO: get logger from context
var logger = slog.Default()

// message types
const (
	MessageTypePing          = "ping"
	MessageTypePong          = "pong"
	MessageTypeSubscribe     = "subscribe"
	MessageTypeSubscribeResp = "subscribe_response"
	MessageTypeEvent         = "event"
	MessageTypeEventAck      = "event_ack"
	MessageTypeCommitOffset  = "commit_offset"
	MessageTypeFetch         = "fetch"
	MessageTypeFetchResp     = "fetch_response"
)

type EventClientOpts struct {
	Endpoint       string
	AuthToken      string // JWT token for authentication (optional if using mTLS)
	CommitInterval string
	GroupID        string
	// mTLS Authentication (optional, alternative to AuthToken)
	CertPath string // Path to client certificate PEM file
	KeyPath  string // Path to private key PEM file
}

type EventClient interface {
	Connect(ctx context.Context, startingSubs *[]SubscriptionRequest) error
	IncomingMsgChannel() chan Message
	Publish(
		ctx context.Context,
		topic, content string,
		targetType, targetID, traceID, orgID *string,
	) (*AppendMessageResponse, error)
	Subscribe(ctx context.Context, subReq SubscriptionRequest) error
}

type Client struct {
	opts              EventClientOpts
	commitInterval    time.Duration
	conn              *websocket.Conn
	httpClient        *http.Client            // for HTTP POST publishing
	writeMu           sync.Mutex              // protects WebSocket writes
	ackMu             sync.Mutex              // protects ackChans map
	subscriptionsMu   sync.RWMutex            // protects subscriptions map
	subscriptions     map[string]Subscription // topic, subscription
	incomingMsgChan   chan Message
	offsetTracker     map[string]int                        // partition_id -> last processed message offset
	lastCommittedOff  map[string]int                        // partition_id -> last committed offset
	ackChans          map[string]chan AppendMessageResponse // topic -> ack channel
	rawMsgChan        chan WebsocketFrame                   // for verbose mode
	verbose           bool
	pollingCtx        context.Context
	pollingCancel     context.CancelFunc
	fetchTrigger      chan struct{} // signals when to start fetching
	fetchResponseChan chan struct{} // signals when fetch response is received
	// mTLS fields
	privateKey     *ecdsa.PrivateKey
	certificatePEM []byte
	mtlsEnabled    bool
	// Reconnection fields
	reconnectMu       sync.RWMutex
	reconnecting      bool
	reconnectAttempts int
	maxReconnectDelay time.Duration
	mainCtx           context.Context     // main context for entire client lifecycle
	mainCancel        context.CancelFunc  // cancels everything including reconnection
	connCtx           context.Context     // context for current connection
	connCancel        context.CancelFunc  // cancels current connection only
}

// buildWebSocketURL constructs the WebSocket endpoint from base endpoint
func (ec *Client) buildWebSocketURL() string {
	endpoint := strings.TrimSuffix(ec.opts.Endpoint, "/")
	// Remove /ws or /api suffix if present
	endpoint = strings.TrimSuffix(endpoint, "/ws")
	endpoint = strings.TrimSuffix(endpoint, "/api")
	return endpoint + "/ws"
}

// buildHTTPBaseURL constructs the HTTP base URL from WebSocket or base endpoint
func (ec *Client) buildHTTPBaseURL() string {
	endpoint := strings.TrimSuffix(ec.opts.Endpoint, "/")
	// Remove /ws or /api suffix if present
	endpoint = strings.TrimSuffix(endpoint, "/ws")
	endpoint = strings.TrimSuffix(endpoint, "/api")

	// Convert ws:// to http:// or wss:// to https://
	if strings.HasPrefix(endpoint, "ws://") {
		endpoint = "http://" + strings.TrimPrefix(endpoint, "ws://")
	} else if strings.HasPrefix(endpoint, "wss://") {
		endpoint = "https://" + strings.TrimPrefix(endpoint, "wss://")
	}
	return endpoint
}

func NewEventClient(opts EventClientOpts) (*Client, error) {
	cInterval, err := time.ParseDuration(opts.CommitInterval)
	if err != nil {
		cInterval = 5 * time.Second
	}
	opts.CommitInterval = cInterval.String()

	client := &Client{
		opts:              opts,
		commitInterval:    cInterval,
		incomingMsgChan:   make(chan Message, 100),
		subscriptions:     make(map[string]Subscription),
		offsetTracker:     make(map[string]int),
		lastCommittedOff:  make(map[string]int),
		ackChans:          make(map[string]chan AppendMessageResponse),
		rawMsgChan:        make(chan WebsocketFrame, 100),
		verbose:           false,
		fetchTrigger:      make(chan struct{}, 1),
		fetchResponseChan: make(chan struct{}, 1),
		maxReconnectDelay: 5 * time.Minute, // max backoff delay
	}

	// Initialize mTLS if certificate and key paths provided
	if opts.CertPath != "" && opts.KeyPath != "" {
		if err := client.initMTLS(); err != nil {
			return nil, fmt.Errorf("failed to initialize mTLS: %w", err)
		}
		logger.Info("mTLS authentication enabled")
	} else if opts.AuthToken == "" {
		return nil, fmt.Errorf("either AuthToken or CertPath/KeyPath must be provided")
	}

	// Create HTTP client with appropriate transport
	baseTransport := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 100,
		IdleConnTimeout:     90 * time.Second,
	}

	var transport http.RoundTripper = baseTransport
	if client.mtlsEnabled {
		transport = &MTLSTransport{
			base:           baseTransport,
			privateKey:     client.privateKey,
			certificatePEM: client.certificatePEM,
		}
	}

	client.httpClient = &http.Client{
		Timeout:   10 * time.Second,
		Transport: transport,
	}

	return client, nil
}

// Connect establishes the connection to the event bus
// and sets up initial subscriptions if provided.
func (ec *Client) Connect(ctx context.Context, startingSubs *[]SubscriptionRequest) error {
	// Store initial subscriptions if this is the first connect
	if startingSubs != nil && len(ec.subscriptions) == 0 {
		ec.subscriptionsMu.Lock()
		for _, subReq := range *startingSubs {
			ec.subscriptions[subReq.Topic] = Subscription{
				SubscriptionRequest: subReq,
				ID:                  "",
			}
		}
		ec.subscriptionsMu.Unlock()
	}

	// Create main context that lives for the entire client lifecycle
	ec.mainCtx, ec.mainCancel = context.WithCancel(ctx)

	// Start connection with auto-reconnect
	if err := ec.connectWithRetry(ec.mainCtx); err != nil {
		return err
	}

	// Start background services that persist across reconnections
	go ec.OffsetManager(ec.mainCtx)
	go ec.reconnectionMonitor(ec.mainCtx)

	return nil
}

// connectWithRetry attempts to establish a WebSocket connection with exponential backoff
func (ec *Client) connectWithRetry(ctx context.Context) error {
	ec.reconnectMu.Lock()
	if ec.reconnecting {
		ec.reconnectMu.Unlock()
		return fmt.Errorf("reconnection already in progress")
	}
	ec.reconnecting = true
	ec.reconnectMu.Unlock()

	defer func() {
		ec.reconnectMu.Lock()
		ec.reconnecting = false
		ec.reconnectMu.Unlock()
	}()

	attempt := 0
	maxAttempts := 10 // After 10 attempts, keep retrying with max delay

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("connection cancelled: %w", ctx.Err())
		default:
		}

		attempt++
		if attempt > 1 {
			// Calculate exponential backoff: 1s, 2s, 4s, 8s, 16s, 32s, 64s, 128s, 256s, 300s (max)
			backoff := time.Duration(1<<uint(attempt-2)) * time.Second
			if backoff > ec.maxReconnectDelay {
				backoff = ec.maxReconnectDelay
			}
			
			logger.Info("reconnection attempt", "attempt", attempt, "backoff", backoff)
			
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return fmt.Errorf("connection cancelled during backoff: %w", ctx.Err())
			}
		} else {
			logger.Info("establishing initial connection")
		}

		// Attempt connection
		if err := ec.establishConnection(ctx); err != nil {
			logger.Warn("connection attempt failed", "attempt", attempt, "error", err)
			
			// Continue retrying unless max attempts reached (for initial connect only)
			if attempt >= maxAttempts && ec.reconnectAttempts == 0 {
				return fmt.Errorf("failed to establish initial connection after %d attempts: %w", maxAttempts, err)
			}
			continue
		}

		// Connection successful
		ec.reconnectMu.Lock()
		ec.reconnectAttempts = 0
		ec.reconnectMu.Unlock()
		
		logger.Info("connection established successfully")
		return nil
	}
}

// establishConnection creates a single WebSocket connection attempt
func (ec *Client) establishConnection(ctx context.Context) error {
	// Build WebSocket URL
	wsEndpoint := ec.buildWebSocketURL()

	// Build connection URL with token if using JWT, otherwise use mTLS headers
	var connectionURL string
	var headers http.Header

	if ec.mtlsEnabled {
		// mTLS authentication - no token in query params
		connectionURL = wsEndpoint
		headers = ec.getWebSocketHeaders()
		logger.Debug("connecting with mTLS authentication")
	} else {
		// JWT authentication - token in query params
		connectionURL = fmt.Sprintf("%s?token=%s", wsEndpoint, ec.opts.AuthToken)
		logger.Debug("connecting with JWT authentication")
	}

	// Create a dialer with handshake timeout to prevent hanging indefinitely
	dialer := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
		Proxy:            websocket.DefaultDialer.Proxy,
		TLSClientConfig:  websocket.DefaultDialer.TLSClientConfig,
	}

	conn, _, err := dialer.DialContext(ctx, connectionURL, headers)
	if err != nil {
		return fmt.Errorf("failed to dial: %w", err)
	}

	// Store connection
	ec.writeMu.Lock()
	ec.conn = conn
	ec.writeMu.Unlock()

	logger.Debug("websocket connection established")

	// Create context for this specific connection
	ec.connCtx, ec.connCancel = context.WithCancel(ctx)

	// Start connection handler
	go ec.HandleConnection(ec.connCtx)

	// Re-subscribe to all topics
	if err := ec.resubscribeAll(); err != nil {
		conn.Close()
		return fmt.Errorf("failed to resubscribe: %w", err)
	}

	// Start polling loop for this connection
	ec.pollingCtx, ec.pollingCancel = context.WithCancel(ec.connCtx)
	go ec.PollingLoop(ec.pollingCtx)

	// Trigger polling to start
	select {
	case ec.fetchTrigger <- struct{}{}:
		logger.Debug("triggered polling loop")
	default:
	}

	return nil
}

// resubscribeAll sends subscription requests for all stored subscriptions
func (ec *Client) resubscribeAll() error {
	ec.subscriptionsMu.RLock()
	subs := make([]SubscriptionRequest, 0, len(ec.subscriptions))
	for _, sub := range ec.subscriptions {
		subs = append(subs, sub.SubscriptionRequest)
	}
	ec.subscriptionsMu.RUnlock()

	if len(subs) == 0 {
		return nil
	}

	logger.Info("resubscribing to topics", "count", len(subs))

	for _, subReq := range subs {
		wfSubReq := NewWebsocketFramedSubscriptionReq(subReq)
		ec.writeMu.Lock()
		err := ec.conn.WriteJSON(wfSubReq)
		ec.writeMu.Unlock()
		if err != nil {
			return fmt.Errorf("failed to send subscription request for topic %s: %w", subReq.Topic, err)
		}
		
		// Reset subscription ID (will be set when response received)
		ec.subscriptionsMu.Lock()
		if sub, exists := ec.subscriptions[subReq.Topic]; exists {
			sub.ID = ""
			ec.subscriptions[subReq.Topic] = sub
		}
		ec.subscriptionsMu.Unlock()
		
		logger.Debug("sent resubscription request", "topic", subReq.Topic)
	}

	return nil
}

// reconnectionMonitor watches for connection failures and triggers reconnection
func (ec *Client) reconnectionMonitor(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			logger.Info("reconnection monitor stopped")
			return
		default:
		}

		// Wait for connection context to be cancelled (indicates connection lost)
		if ec.connCtx != nil {
			<-ec.connCtx.Done()
		} else {
			// No connection context yet, wait a bit
			time.Sleep(time.Second)
			continue
		}

		// Check if main context is still active
		select {
		case <-ctx.Done():
			logger.Info("main context cancelled, stopping reconnection monitor")
			return
		default:
		}

		logger.Warn("connection lost, initiating reconnection")

		// Stop polling loop for old connection
		if ec.pollingCancel != nil {
			ec.pollingCancel()
		}

		// Increment reconnect attempts
		ec.reconnectMu.Lock()
		ec.reconnectAttempts++
		ec.reconnectMu.Unlock()

		// Attempt to reconnect
		if err := ec.connectWithRetry(ctx); err != nil {
			logger.Error("failed to reconnect", "error", err)
			return
		}

		logger.Info("reconnection successful")
	}
}

// Close gracefully shuts down the event bus client
func (ec *Client) Close() error {
	// Cancel main context to stop all background services including reconnection
	if ec.mainCancel != nil {
		ec.mainCancel()
	}

	// Cancel current connection
	if ec.connCancel != nil {
		ec.connCancel()
	}

	// Cancel polling loop
	if ec.pollingCancel != nil {
		ec.pollingCancel()
	}

	// Close WebSocket connection
	if ec.conn != nil {
		ec.conn.Close()
	}

	logger.Info("event bus client closed")
	return nil
}

// Stop is an alias for Close for backward compatibility
func (ec *Client) Stop() error {
	return ec.Close()
}

func (ec *Client) IncomingMsgChannel() chan Message {
	return ec.incomingMsgChan
}

func (ec *Client) RawMsgChannel() chan WebsocketFrame {
	return ec.rawMsgChan
}

func (ec *Client) SetVerbose(verbose bool) {
	ec.verbose = verbose
}

// Subscribe adds a new subscription to an existing connection
func (ec *Client) Subscribe(ctx context.Context, subReq SubscriptionRequest) error {
	// Check if already subscribed
	ec.subscriptionsMu.RLock()
	_, exists := ec.subscriptions[subReq.Topic]
	ec.subscriptionsMu.RUnlock()
	if exists {
		logger.Debug("already subscribed to topic", "topic", subReq.Topic)
		return nil // Not an error, just idempotent
	}

	// Store subscription first (will be sent/resent on connect/reconnect)
	ec.subscriptionsMu.Lock()
	ec.subscriptions[subReq.Topic] = Subscription{
		SubscriptionRequest: subReq,
		ID:                  "", // ID will be set upon receiving subscription response
	}
	ec.subscriptionsMu.Unlock()

	// Try to send immediately if connected
	ec.writeMu.Lock()
	conn := ec.conn
	ec.writeMu.Unlock()

	if conn == nil {
		logger.Debug("not connected yet, subscription will be sent on connect", "topic", subReq.Topic)
		return nil
	}

	wfSubReq := NewWebsocketFramedSubscriptionReq(subReq)
	ec.writeMu.Lock()
	err := ec.conn.WriteJSON(wfSubReq)
	ec.writeMu.Unlock()
	if err != nil {
		logger.Warn("failed to send subscription request, will retry on reconnect", "topic", subReq.Topic, "error", err)
		// Don't return error - subscription is stored and will be retried
		return nil
	}

	logger.Debug("sent subscription request", "topic", subReq.Topic)

	// Trigger polling to start if this is the first subscription
	select {
	case ec.fetchTrigger <- struct{}{}:
	default:
	}

	return nil
}

func (ec *Client) HandleConnection(ctx context.Context) {
	defer func() {
		if ec.conn != nil {
			ec.conn.Close()
		}
		logger.Info("HandleConnection exiting - WebSocket connection closed")
		
		// Cancel connection context to trigger reconnection
		if ec.connCancel != nil {
			ec.connCancel()
		}
	}()

	// Circuit breaker: track consecutive invalid frames to detect connection issues
	const maxConsecutiveInvalidFrames = 10
	consecutiveInvalidFrames := 0

	for {
		select {
		case <-ctx.Done():
			logger.Info("HandleConnection context canceled")
			return
		default:
			var wfMsg WebsocketFrame
			if err := ec.conn.ReadJSON(&wfMsg); err != nil {
				logger.Warn("WebSocket read error - connection lost", "error", err)
				return
			}

			// Validate frame has required fields to prevent processing empty/malformed frames
			// This prevents the "no subscribers" log spam when connection is failing
			if wfMsg.MessageType == "" || wfMsg.Version == "" {
				consecutiveInvalidFrames++
				logger.Debug("received empty or invalid frame",
					"message_type", wfMsg.MessageType,
					"version", wfMsg.Version,
					"consecutive_invalid", consecutiveInvalidFrames)

				// Circuit breaker: if we get too many invalid frames in a row,
				// the connection is likely broken
				if consecutiveInvalidFrames >= maxConsecutiveInvalidFrames {
					logger.Warn("too many consecutive invalid frames, connection appears broken",
						"count", consecutiveInvalidFrames)
					return
				}
				continue
			}

			// Reset counter on valid frame
			consecutiveInvalidFrames = 0

			// Send to raw channel for verbose mode (non-blocking)
			if ec.verbose {
				select {
				case ec.rawMsgChan <- wfMsg:
				default:
					// Don't block if channel is full
				}
			}

			// Process message in goroutine for non-blocking
			go ec.processMessage(wfMsg)
		}
	}
}

func (ec *Client) processMessage(wfMsg WebsocketFrame) {
	switch wfMsg.MessageType {
	case MessageTypeEvent:
		wfEvent := wfMsg.ToMessage()
		eventPayload := wfEvent.Payload
		msg := eventPayload.Message

		// Track offset for consumed messages per partition
		if eventPayload.Offset >= 0 && eventPayload.PartitionID != "" {
			ec.offsetTracker[eventPayload.PartitionID] = eventPayload.Offset
			logger.Debug("tracked consumed message offset", "topic", msg.Topic, "partition", eventPayload.PartitionID, "offset", eventPayload.Offset)
		}

		select {
		case ec.incomingMsgChan <- msg:
		default:
			logger.Warn("incoming message channel full, dropping message")
		}
	case MessageTypeEventAck:
		appendMsgResp := wfMsg.ToAppendMessageResp()
		// Don't track offset for published messages - only for consumed ones

		logger.Debug("received event ack", "topic", appendMsgResp.Payload.Topic, "offset", appendMsgResp.Payload.Offset, "partition", appendMsgResp.Payload.PartitionID)

		// Send to waiting publish goroutine if exists
		ec.ackMu.Lock()
		ackChan, exists := ec.ackChans[appendMsgResp.Payload.Topic]
		ec.ackMu.Unlock()

		if exists {
			logger.Debug("found ack channel for topic", "topic", appendMsgResp.Payload.Topic)
			select {
			case ackChan <- appendMsgResp.Payload:
				logger.Debug("sent ack to channel", "topic", appendMsgResp.Payload.Topic)
			default:
				logger.Warn("ack channel full or closed", "topic", appendMsgResp.Payload.Topic)
			}
		} else {
			ec.ackMu.Lock()
			availableChannels := len(ec.ackChans)
			ec.ackMu.Unlock()
			logger.Warn("no ack channel found for topic", "topic", appendMsgResp.Payload.Topic, "available_channels", availableChannels)
		}
	case MessageTypeSubscribeResp:
		logger.Debug("raw subscription response", "payload", wfMsg.Payload)
		wfSubResp := wfMsg.ToSubscriptionResp()
		subResp := wfSubResp.Payload
		logger.Debug("parsed subscription response", "topic", subResp.Topic, "subID", subResp.SubscriptionID, "groupID", subResp.GroupID)
		ec.subscriptionsMu.Lock()
		if sub, exists := ec.subscriptions[subResp.Topic]; exists {
			sub.ID = subResp.SubscriptionID
			ec.subscriptions[subResp.Topic] = sub
			logger.Debug("subscription response received client updated", "topic", subResp.Topic, "subID", sub.ID)
		} else {
			logger.Warn("received subscription response for unknown topic", "topic", subResp.Topic)
		}
		ec.subscriptionsMu.Unlock()
	case MessageTypeFetchResp:
		// Parse the fetch response to extract events
		wfFetchResp := wfMsg.ToFetchResponse()
		fetchPayload := wfFetchResp.Payload

		logger.Debug("received fetch response", "message_count", fetchPayload.Count)

		// Process each event in the response
		for _, event := range fetchPayload.Messages {
			msg := event.Message

			// Track offset for consumed messages per partition
			if event.Offset >= 0 && event.PartitionID != "" {
				ec.offsetTracker[event.PartitionID] = event.Offset
				logger.Debug("tracked consumed message offset", "topic", msg.Topic, "partition", event.PartitionID, "offset", event.Offset)
			}

			// Send message to incoming channel
			select {
			case ec.incomingMsgChan <- msg:
			default:
				logger.Warn("incoming message channel full, dropping message")
			}

			// CRITICAL: Commit offset immediately after processing each message
			// This prevents receiving the same message again
			if event.SubscriptionID != "" && event.PartitionID != "" && event.Offset >= 0 {
				nextOffset := event.Offset + 1 // Next offset to read
				commitMsg := WFOffsetCommitMsg{
					WebsocketFrame: WebsocketFrame{
						MessageType: MessageTypeCommitOffset,
						Version:     "1.0",
					},
					Payload: OffsetCommitMsg{
						SubscriptionID: event.SubscriptionID,
						PartitionID:    event.PartitionID,
						GroupID:        ec.opts.GroupID,
						Offset:         nextOffset,
					},
				}

				ec.writeMu.Lock()
				err := ec.conn.WriteJSON(commitMsg)
				ec.writeMu.Unlock()

				if err != nil {
					logger.Error("failed to commit offset after processing message", "partition", event.PartitionID, "offset", nextOffset, "error", err)
				} else {
					ec.lastCommittedOff[event.PartitionID] = nextOffset
					logger.Debug("committed offset immediately after processing", "partition", event.PartitionID, "committed_offset", nextOffset)
				}
			}
		}

		// Signal that we received a fetch response so polling loop can continue
		select {
		case ec.fetchResponseChan <- struct{}{}:
		default:
			// Channel already has a signal, no need to add another
		}

	default:
		logger.Debug("unknown message type", "messageType", wfMsg.MessageType, "message", wfMsg)
	}
}

func (ec *Client) Publish(
	ctx context.Context,
	topic, content string,
	targetType, targetID, traceID, orgID *string,
) (*AppendMessageResponse, error) {
	// Build HTTP POST request
	reqBody := HTTPPublishRequest{
		Topic:   topic,
		Content: content,
	}
	if orgID != nil {
		reqBody.OrgID = *orgID
	}
	if traceID != nil {
		reqBody.TraceID = *traceID
	}
	if targetType != nil {
		reqBody.TargetType = *targetType
	}
	if targetID != nil {
		reqBody.TargetID = *targetID
	}

	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal publish request: %w", err)
	}

	// Build HTTP publish URL
	baseURL := ec.buildHTTPBaseURL()
	publishURL := baseURL + "/api/v1/publish"

	// Create HTTP request
	req, err := http.NewRequestWithContext(ctx, "POST", publishURL, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to create publish request: %w", err)
	}

	// Set headers
	req.Header.Set("Content-Type", "application/json")

	// Add authentication - either JWT or mTLS (handled by transport)
	if !ec.mtlsEnabled {
		req.Header.Set("Authorization", "Bearer "+ec.opts.AuthToken)
	}
	// Note: mTLS headers are added automatically by MTLSTransport

	// Send request
	resp, err := ec.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send publish request: %w", err)
	}
	defer resp.Body.Close()

	// Read response body
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read publish response: %w", err)
	}

	// Check for 202 Accepted
	if resp.StatusCode != http.StatusAccepted {
		return nil, fmt.Errorf("publish failed with status %d: %s", resp.StatusCode, string(respBody))
	}

	// Parse response
	var httpResp HTTPPublishResponse
	if err := json.Unmarshal(respBody, &httpResp); err != nil {
		return nil, fmt.Errorf("failed to parse publish response: %w", err)
	}

	logger.Debug("publish accepted", "topic", httpResp.Topic, "status", httpResp.Status)

	// Return minimal response (HTTP publish doesn't provide offset/partition immediately)
	return &AppendMessageResponse{
		Topic: httpResp.Topic,
	}, nil
}

func (ec *Client) CommitOffsets(ctx context.Context) error {
	for partitionID, offset := range ec.offsetTracker {
		// Only commit if offset has changed since last commit
		if lastCommitted, exists := ec.lastCommittedOff[partitionID]; exists && lastCommitted == offset {
			continue
		}

		// Extract topic from partition_id (format: topic-partition-N)
		// Find the subscription that matches this partition's topic
		var subscriptionID string
		ec.subscriptionsMu.RLock()
		for topic, sub := range ec.subscriptions {
			if len(partitionID) > len(topic) && partitionID[:len(topic)] == topic {
				subscriptionID = sub.ID
				break
			}
		}
		ec.subscriptionsMu.RUnlock()

		if subscriptionID == "" {
			logger.Warn("no subscription found for partition", "partition", partitionID)
			continue
		}

		// Commit offset + 1 (the next offset to read, not the one we just read)
		nextOffset := offset + 1

		msg := WFOffsetCommitMsg{
			WebsocketFrame: WebsocketFrame{
				MessageType: MessageTypeCommitOffset,
				Version:     "1.0",
			},
			Payload: OffsetCommitMsg{
				SubscriptionID: subscriptionID,
				PartitionID:    partitionID,
				GroupID:        ec.opts.GroupID,
				Offset:         nextOffset,
			},
		}
		ec.writeMu.Lock()
		err := ec.conn.WriteJSON(msg)
		ec.writeMu.Unlock()
		if err != nil {
			return fmt.Errorf("failed to commit offset for partition %s: %w", partitionID, err)
		}

		// Update last committed offset
		ec.lastCommittedOff[partitionID] = nextOffset
		logger.Debug("offset committed (periodic)", "partition", partitionID, "committed_offset", nextOffset)
	}
	return nil
}

func (ec *Client) OffsetManager(ctx context.Context) {
	ticker := time.NewTicker(ec.commitInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			// Skip if not connected
			ec.writeMu.Lock()
			connected := ec.conn != nil
			ec.writeMu.Unlock()
			
			if !connected {
				logger.Debug("skipping offset commit, not connected")
				continue
			}
			
			if err := ec.CommitOffsets(ctx); err != nil {
				logger.Warn("failed to commit offsets", "error", err)
			}
		case <-ctx.Done():
			logger.Debug("offset manager stopped")
			return
		}
	}
}

// PollingLoop continuously sends fetch requests using long polling
// The server holds each request for up to 5 seconds, providing near-instant
// message delivery when available while minimizing network overhead
func (ec *Client) PollingLoop(ctx context.Context) {
	logger.Debug("polling loop started")
	defer logger.Debug("polling loop stopped")
	
	// Wait for first subscription before starting to poll
	select {
	case <-ec.fetchTrigger:
		logger.Debug("polling loop activated")
	case <-ctx.Done():
		return
	}

	// Continuous long polling loop - no delays between requests
	for {
		select {
		case <-ctx.Done():
			return
		default:
			// Check if still connected
			ec.writeMu.Lock()
			connected := ec.conn != nil
			ec.writeMu.Unlock()
			
			if !connected {
				logger.Debug("polling loop paused, not connected")
				time.Sleep(time.Second)
				continue
			}
			
			// Only send fetch if we have active subscriptions
			ec.subscriptionsMu.RLock()
			hasSubscriptions := len(ec.subscriptions) > 0
			ec.subscriptionsMu.RUnlock()

			if !hasSubscriptions {
				// Wait for a subscription to be added
				select {
				case <-ec.fetchTrigger:
				case <-ctx.Done():
					return
				}
				continue
			}

			// Send fetch request - server will hold for up to 5 seconds
			if err := ec.sendFetchRequest(); err != nil {
				logger.Warn("failed to send fetch request", "error", err)
				// Brief delay on error to avoid tight error loop
				time.Sleep(time.Second)
				continue
			}

			// Wait for fetch response before sending next request
			// This prevents flooding the server with requests
			// Add timeout to prevent hanging if connection dies
			select {
			case <-ec.fetchResponseChan:
				// Got response, add minimum 100ms delay as required by server rate limiting
				time.Sleep(100 * time.Millisecond)
				// Loop will send next fetch
			case <-time.After(15 * time.Second):
				// Timeout waiting for response - connection may be dead
				logger.Warn("fetch response timeout - connection may be dead")
				return
			case <-ctx.Done():
				return
			}
		}
	}
}

// sendFetchRequest sends a fetch message to pull new messages for all subscriptions
// Server will hold the request for up to 5 seconds (long polling)
func (ec *Client) sendFetchRequest() error {
	fetchMsg := WebsocketFrame{
		MessageType: MessageTypeFetch,
		Version:     "1.0",
		Payload:     nil,
	}

	ec.writeMu.Lock()
	err := ec.conn.WriteJSON(fetchMsg)
	ec.writeMu.Unlock()
	if err != nil {
		return fmt.Errorf("failed to send fetch request: %w", err)
	}

	logger.Debug("sent long polling fetch request")
	return nil
}
