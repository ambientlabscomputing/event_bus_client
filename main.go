package event_bus_client

import (
	"context"
	"fmt"
	"log/slog"
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
}

type Client struct {
	opts             EventClientOpts
	commitInterval   time.Duration
	conn             *websocket.Conn
	subscriptions    map[string]Subscription // topic, subscription
	incomingMsgChan  chan Message
	offsetTracker    map[string]int                        // partition_id -> last processed message offset
	lastCommittedOff map[string]int                        // partition_id -> last committed offset
	ackChans         map[string]chan AppendMessageResponse // topic -> ack channel
	rawMsgChan       chan WebsocketFrame                   // for verbose mode
	verbose          bool
}

func NewEventClient(opts EventClientOpts) *Client {
	cInterval, err := time.ParseDuration(opts.CommitInterval)
	if err != nil {
		cInterval = 5 * time.Second
	}
	opts.CommitInterval = cInterval.String()
	return &Client{
		opts:             opts,
		commitInterval:   cInterval,
		incomingMsgChan:  make(chan Message, 100),
		subscriptions:    make(map[string]Subscription),
		offsetTracker:    make(map[string]int),
		lastCommittedOff: make(map[string]int),
		ackChans:         make(map[string]chan AppendMessageResponse),
		rawMsgChan:       make(chan WebsocketFrame, 100),
		verbose:          false,
	}
}

// Connect establishes the connection to the event bus
// and sets up initial subscriptions if provided.
func (ec *Client) Connect(ctx context.Context, startingSubs *[]SubscriptionRequest) error {
	// start websocket connection
	connectionURL := fmt.Sprintf("%s?token=%s", ec.opts.Endpoint, ec.opts.AuthToken)
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
			if err := ec.conn.WriteJSON(wfSubReq); err != nil {
				return fmt.Errorf("failed to send subscription request: %w", err)
			}
			ec.subscriptions[subReq.Topic] = Subscription{
				SubscriptionRequest: subReq,
				ID:                  "", // ID will be set upon receiving subscription response
			}
		}
	}

	// start offset commit manager
	go ec.OffsetManager(ctx)

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
	if err := ec.conn.WriteJSON(wfSubReq); err != nil {
		return fmt.Errorf("failed to send subscription request: %w", err)
	}

	ec.subscriptions[subReq.Topic] = Subscription{
		SubscriptionRequest: subReq,
		ID:                  "", // ID will be set upon receiving subscription response
	}

	logger.Debug("sent subscription request", "topic", subReq.Topic)
	return nil
}

func (ec *Client) HandleConnection(ctx context.Context) {
	defer ec.conn.Close()

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

		// Send to waiting publish goroutine if exists
		if ackChan, exists := ec.ackChans[appendMsgResp.Payload.Topic]; exists {
			select {
			case ackChan <- appendMsgResp.Payload:
			default:
				// Channel might be closed or full
			}
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
	default:
		logger.Debug("unknown message type", "messageType", wfMsg.MessageType, "message", wfMsg)
	}
}

func (ec *Client) Publish(
	ctx context.Context,
	topic, content string,
	targetType, targetID, traceID *string,
) (*AppendMessageResponse, error) {
	msg := Message{
		Topic:      topic,
		Content:    content,
		TargetType: targetType,
		TargetID:   targetID,
		TraceID:    traceID,
	}
	wfMsg := NewWebsocketFramedMessage(msg)

	// Create ack channel for this topic
	ackChan := make(chan AppendMessageResponse, 1)
	ec.ackChans[topic] = ackChan
	defer func() {
		delete(ec.ackChans, topic)
		close(ackChan)
	}()

	// Send message (non-blocking write)
	if err := ec.conn.WriteJSON(wfMsg); err != nil {
		return nil, fmt.Errorf("failed to publish message: %w", err)
	}

	// Wait for acknowledgment with timeout
	ackCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	select {
	case ack := <-ackChan:
		logger.Debug("publish acknowledged", "offset", ack.Offset, "partition", ack.PartitionID, "topic", ack.Topic)
		return &ack, nil
	case <-ackCtx.Done():
		return nil, fmt.Errorf("timeout waiting for publish acknowledgment")
	}
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

		msg := WFOffsetCommitMsg{
			WebsocketFrame: WebsocketFrame{
				MessageType: MessageTypeCommitOffset,
				Version:     "1.0",
			},
			Payload: OffsetCommitMsg{
				SubscriptionID: subscriptionID,
				PartitionID:    partitionID,
				GroupID:        ec.opts.GroupID,
				Offset:         offset,
			},
		}
		if err := ec.conn.WriteJSON(msg); err != nil {
			return fmt.Errorf("failed to commit offset for partition %s: %w", partitionID, err)
		}

		// Update last committed offset
		ec.lastCommittedOff[partitionID] = offset
		logger.Debug("offset committed", "partition", partitionID, "offset", offset)
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
