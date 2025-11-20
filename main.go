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
	opts            EventClientOpts
	commitInterval  time.Duration
	conn            *websocket.Conn
	subscriptions   map[string]Subscription // topic, subscription
	incomingMsgChan chan Message
	offsetTracker   map[string]int // topic, last processed message ID
}

func NewEventClient(opts EventClientOpts) *Client {
	cInterval, err := time.ParseDuration(opts.CommitInterval)
	if err != nil {
		cInterval = 5 * time.Second
	}
	opts.CommitInterval = cInterval.String()
	return &Client{
		opts:            opts,
		commitInterval:  cInterval,
		incomingMsgChan: make(chan Message),
		subscriptions:   make(map[string]Subscription),
		offsetTracker:   make(map[string]int),
	}
}

// Connect establishes the connection to the event bus
// and sets up initial subscriptions if provided.
func (ec *Client) Connect(ctx context.Context, startingSubs *[]SubscriptionRequest) error {
	// start websocket connection
	connectionURL := fmt.Sprintf("%s?token=%s", ec.opts.Endpoint, ec.opts.AuthToken)
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, connectionURL, nil)
	if err != nil {
		return fmt.Errorf("failed to connect to event bus: %w", err)
	}
	ec.conn = conn
	go ec.HandleConnection(ctx)

	// set up initial subscriptions
	if startingSubs != nil {
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

func (ec *Client) HandleConnection(ctx context.Context) {
	defer ec.conn.Close()

	for {
		var wfMsg WebsocketFrame
		if err := ec.conn.ReadJSON(&wfMsg); err != nil {
			close(ec.incomingMsgChan)
			return
		}

		switch wfMsg.MessageType {
		case MessageTypeEvent:
			wfEvent := wfMsg.ToMessage()
			ec.incomingMsgChan <- wfEvent.Payload
		case MessageTypeEventAck:
			appendMsgResp := wfMsg.ToAppendMessageResp()
			ec.offsetTracker[appendMsgResp.Payload.Topic] = appendMsgResp.Payload.Offset
		case MessageTypeSubscribeResp:
			wfSubResp := wfMsg.ToSubscriptionResp()
			subResp := wfSubResp.Payload
			if sub, exists := ec.subscriptions[subResp.Topic]; exists {
				sub.ID = subResp.SubscriptionID
				ec.subscriptions[subResp.Topic] = sub
				logger.Debug("subscription response received client updated", "topic", subResp.Topic, "subID", sub.ID)
			} else {
				logger.Warn("received subscription response for unknown topic", "topic", subResp.Topic)
			}
		default:
			// ignore other message types for now
			logger.Debug("unknown message type", "messageType", wfMsg.MessageType, "message", wfMsg)
		}
	}
}

func (ec *Client) Publish(
	ctx context.Context,
	topic, content string,
	targetType, targetID, traceID *string,
) error {
	msg := Message{
		Topic:      topic,
		Content:    content,
		TargetType: targetType,
		TargetID:   targetID,
		TraceID:    traceID,
	}
	wfMsg := NewWebsocketFramedMessage(msg)
	if err := ec.conn.WriteJSON(wfMsg); err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}
	return nil
}

func (ec *Client) CommitOffsets(ctx context.Context) error {
	for topic, offset := range ec.offsetTracker {
		msg := WFOffsetCommitMsg{
			WebsocketFrame: WebsocketFrame{
				MessageType: MessageTypeCommitOffset,
				Version:     "1.0",
			},
			Payload: OffsetCommitMsg{
				SubscriptionID: ec.subscriptions[topic].ID,
				GroupID:        ec.opts.GroupID,
				Offset:         offset,
			},
		}
		if err := ec.conn.WriteJSON(msg); err != nil {
			return fmt.Errorf("failed to commit offset for topic %s: %w", topic, err)
		}
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
