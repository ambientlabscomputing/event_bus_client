package event_bus_client

import (
	"bytes"
	"context"
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
	AuthToken      string
	CommitInterval string
	GroupID        string
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

func NewEventClient(opts EventClientOpts) *Client {
	cInterval, err := time.ParseDuration(opts.CommitInterval)
	if err != nil {
		cInterval = 5 * time.Second
	}
	opts.CommitInterval = cInterval.String()
	return &Client{
		opts:           opts,
		commitInterval: cInterval,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 100,
				IdleConnTimeout:     90 * time.Second,
			},
		},
		incomingMsgChan:   make(chan Message, 100),
		subscriptions:     make(map[string]Subscription),
		offsetTracker:     make(map[string]int),
		lastCommittedOff:  make(map[string]int),
		ackChans:          make(map[string]chan AppendMessageResponse),
		rawMsgChan:        make(chan WebsocketFrame, 100),
		verbose:           false,
		fetchTrigger:      make(chan struct{}, 1),
		fetchResponseChan: make(chan struct{}, 1),
	}
}

// Connect establishes the connection to the event bus
// and sets up initial subscriptions if provided.
func (ec *Client) Connect(ctx context.Context, startingSubs *[]SubscriptionRequest) error {
	// start websocket connection
	// Build WebSocket URL: append /ws to base endpoint
	wsEndpoint := ec.buildWebSocketURL()
	connectionURL := fmt.Sprintf("%s?token=%s", wsEndpoint, ec.opts.AuthToken)
	logger.Debug("connection URL", "connURL", connectionURL)
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, connectionURL, nil)
	if err != nil {
		return fmt.Errorf("failed to connect to event bus: %w", err)
	}
	ec.conn = conn
	logger.Debug("websocket connection established", "conn", ec.conn)
	go ec.HandleConnection(ctx)

	// set up initial subscriptions
	if startingSubs != nil {
		logger.Debug("setting up initial subscriptions", "subs", startingSubs)
		for _, subReq := range *startingSubs {
			wfSubReq := NewWebsocketFramedSubscriptionReq(subReq)
			ec.writeMu.Lock()
			err := ec.conn.WriteJSON(wfSubReq)
			ec.writeMu.Unlock()
			if err != nil {
				return fmt.Errorf("failed to send subscription request: %w", err)
			}
			ec.subscriptions[subReq.Topic] = Subscription{
				SubscriptionRequest: subReq,
				ID:                  "", // ID will be set upon receiving subscription response
			}
		}

		// Trigger polling loop to start fetching messages for initial subscriptions
		select {
		case ec.fetchTrigger <- struct{}{}:
			logger.Debug("triggered polling loop for initial subscriptions")
		default:
			// Channel already has a signal
		}
	}

	// start offset commit manager
	go ec.OffsetManager(ctx)

	// start polling loop for fetch requests
	ec.pollingCtx, ec.pollingCancel = context.WithCancel(ctx)
	go ec.PollingLoop(ec.pollingCtx)

	return nil
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
	if ec.conn == nil {
		return fmt.Errorf("not connected to event bus")
	}

	// Check if already subscribed
	if _, exists := ec.subscriptions[subReq.Topic]; exists {
		return fmt.Errorf("already subscribed to topic: %s", subReq.Topic)
	}

	wfSubReq := NewWebsocketFramedSubscriptionReq(subReq)
	ec.writeMu.Lock()
	err := ec.conn.WriteJSON(wfSubReq)
	ec.writeMu.Unlock()
	if err != nil {
		return fmt.Errorf("failed to send subscription request: %w", err)
	}

	ec.subscriptions[subReq.Topic] = Subscription{
		SubscriptionRequest: subReq,
		ID:                  "", // ID will be set upon receiving subscription response
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
	defer ec.conn.Close()

	// Circuit breaker: track consecutive invalid frames to detect connection issues
	const maxConsecutiveInvalidFrames = 10
	consecutiveInvalidFrames := 0

	for {
		select {
		case <-ctx.Done():
			close(ec.incomingMsgChan)
			close(ec.rawMsgChan)
			return
		default:
			var wfMsg WebsocketFrame
			if err := ec.conn.ReadJSON(&wfMsg); err != nil {
				logger.Warn("failed to read message", "err", err)
				close(ec.incomingMsgChan)
				close(ec.rawMsgChan)
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
					logger.Error("too many consecutive invalid frames, connection appears broken",
						"count", consecutiveInvalidFrames)
					close(ec.incomingMsgChan)
					close(ec.rawMsgChan)
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
		if sub, exists := ec.subscriptions[subResp.Topic]; exists {
			sub.ID = subResp.SubscriptionID
			ec.subscriptions[subResp.Topic] = sub
			logger.Debug("subscription response received client updated", "topic", subResp.Topic, "subID", sub.ID)
		} else {
			logger.Warn("received subscription response for unknown topic", "topic", subResp.Topic)
		}
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
	req.Header.Set("Authorization", "Bearer "+ec.opts.AuthToken)

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
		for topic, sub := range ec.subscriptions {
			if len(partitionID) > len(topic) && partitionID[:len(topic)] == topic {
				subscriptionID = sub.ID
				break
			}
		}

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
			if err := ec.CommitOffsets(ctx); err != nil {
				logger.Error("failed to commit offsets", "error", err)
			}
		case <-ctx.Done():
			return
		}
	}
}

// PollingLoop continuously sends fetch requests using long polling
// The server holds each request for up to 5 seconds, providing near-instant
// message delivery when available while minimizing network overhead
func (ec *Client) PollingLoop(ctx context.Context) {
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
			// Only send fetch if we have active subscriptions
			if len(ec.subscriptions) == 0 {
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
				logger.Error("failed to send fetch request", "error", err)
				// Brief delay on error to avoid tight error loop
				time.Sleep(time.Second)
				continue
			}

			// Wait for fetch response before sending next request
			// This prevents flooding the server with requests
			select {
			case <-ec.fetchResponseChan:
				// Got response, add minimum 100ms delay as required by server rate limiting
				time.Sleep(100 * time.Millisecond)
				// Loop will send next fetch
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
