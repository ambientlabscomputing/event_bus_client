package event_bus_client

// HTTP Publishing Types
type HTTPPublishRequest struct {
	Topic      string `json:"topic"`
	Content    string `json:"content"`
	OrgID      string `json:"org_id,omitempty"`
	TraceID    string `json:"trace_id,omitempty"`
	TargetType string `json:"target_type,omitempty"`
	TargetID   string `json:"target_id,omitempty"`
}

type HTTPPublishResponse struct {
	Status  string `json:"status"`
	Message string `json:"message"`
	Topic   string `json:"topic"`
}

// WebSocket Types
type WebsocketFrame struct {
	MessageType string `json:"message_type"`
	Version     string `json:"version"`
	Payload     any    `json:"payload"`
}

func (wf *WebsocketFrame) ToPing() WFPingMessage {
	return WFPingMessage{
		WebsocketFrame: WebsocketFrame{
			MessageType: "ping",
			Version:     wf.Version,
			Payload:     nil,
		},
	}
}

func (wf *WebsocketFrame) ToMessage() WFMessage {
	// Payload comes in as map[string]interface{} from JSON unmarshaling
	payloadMap, ok := wf.Payload.(map[string]interface{})
	if !ok {
		return WFMessage{}
	}

	eventPayload := EventPayload{}

	// Extract offset and partition_id from top level
	if offset, ok := payloadMap["offset"].(float64); ok {
		eventPayload.Offset = int(offset)
	}
	if partitionID, ok := payloadMap["partition_id"].(string); ok {
		eventPayload.PartitionID = partitionID
	}

	// Extract nested message object
	msgMap, ok := payloadMap["message"].(map[string]interface{})
	if !ok {
		return WFMessage{}
	}

	msg := Message{}
	if id, ok := msgMap["id"].(string); ok {
		msg.ID = id
	}
	if topic, ok := msgMap["topic"].(string); ok {
		msg.Topic = topic
	}
	if content, ok := msgMap["content"].(string); ok {
		msg.Content = content
	}
	// Store offset and partition in message for convenience
	msg.Offset = eventPayload.Offset
	msg.PartitionID = eventPayload.PartitionID

	if targetType, ok := msgMap["target_type"].(string); ok {
		msg.TargetType = &targetType
	}
	if targetID, ok := msgMap["target_id"].(string); ok {
		msg.TargetID = &targetID
	}
	if traceID, ok := msgMap["trace_id"].(string); ok {
		msg.TraceID = &traceID
	}
	if orgID, ok := msgMap["org_id"].(string); ok {
		msg.OrgID = &orgID
	}

	eventPayload.Message = msg

	return WFMessage{
		WebsocketFrame: *wf,
		Payload:        eventPayload,
	}
}

type WFPingMessage struct {
	WebsocketFrame `json:",inline"`
}

type WFPongMessage struct {
	WebsocketFrame `json:",inline"`
}

func NewPingPongMessage(msgTyp string) WFPingMessage {
	if msgTyp != "ping" && msgTyp != "pong" {
		msgTyp = "ping"
	}
	return WFPingMessage{
		WebsocketFrame: WebsocketFrame{
			MessageType: msgTyp,
			Version:     "1.0",
			Payload:     nil,
		},
	}
}

type SubscriptionRequest struct {
	GroupID    string  `json:"group_id"`
	Topic      string  `json:"topic"`
	TargetType *string `json:"target_type"`
	TargetID   *string `json:"target_id"`
	TraceID    *string `json:"trace_id,omitempty"`
	OrgID      *string `json:"org_id,omitempty"`
	Limit      int     `json:"limit,omitempty"`
}

type Subscription struct {
	SubscriptionRequest `json:",inline"`
	ID                  string `json:"id"`
}

type WFSubscriptionReq struct {
	WebsocketFrame `json:",inline"`
	Payload        SubscriptionRequest `json:"payload"`
}

func NewWebsocketFramedSubscriptionReq(subReq SubscriptionRequest) WFSubscriptionReq {
	return WFSubscriptionReq{
		WebsocketFrame: WebsocketFrame{
			MessageType: "subscribe",
			Version:     "1.0",
		},
		Payload: subReq,
	}
}

type SubscriptionResponse struct {
	SubscriptionID string `json:"subscription_id"`
	Topic          string `json:"topic"`
	GroupID        string `json:"group_id"`
	Err            error  `json:"error,omitempty"`
}

type WFSubscriptionResp struct {
	WebsocketFrame `json:",inline"`
	Payload        SubscriptionResponse `json:"payload"`
}

func (wf *WebsocketFrame) ToSubscriptionResp() WFSubscriptionResp {
	// Payload comes in as map[string]interface{} from JSON unmarshaling
	payloadMap, ok := wf.Payload.(map[string]interface{})
	if !ok {
		return WFSubscriptionResp{}
	}

	subResp := SubscriptionResponse{}
	if subID, ok := payloadMap["subscription_id"].(string); ok {
		subResp.SubscriptionID = subID
	}
	if topic, ok := payloadMap["topic"].(string); ok {
		subResp.Topic = topic
	}
	if groupID, ok := payloadMap["group_id"].(string); ok {
		subResp.GroupID = groupID
	}

	return WFSubscriptionResp{
		WebsocketFrame: *wf,
		Payload:        subResp,
	}
}

// the main message structure used in the event bus
type Message struct {
	ID          string  `bson:"_id,omitempty" json:"id"`
	Topic       string  `bson:"topic" json:"topic"`
	Content     string  `bson:"content" json:"content"`
	Offset      int     `bson:"offset,omitempty" json:"offset,omitempty"`
	PartitionID string  `bson:"partition_id,omitempty" json:"partition_id,omitempty"`
	TargetType  *string `bson:"target_type" json:"target_type"`
	TargetID    *string `bson:"target_id" json:"target_id"`
	TraceID     *string `bson:"trace_id,omitempty" json:"trace_id,omitempty"`
	OrgID       *string `bson:"org_id,omitempty" json:"org_id,omitempty"`
}

// EventPayload wraps a message with offset and partition metadata (for consuming)
type EventPayload struct {
	Offset      int     `json:"offset"`
	PartitionID string  `json:"partition_id"`
	Message     Message `json:"message"`
}

// WFMessage for consuming events (with offset and partition wrapper)
type WFMessage struct {
	WebsocketFrame `json:",inline"`
	Payload        EventPayload `json:"payload"`
}

// WFPublishMessage for publishing events (direct message fields)
type WFPublishMessage struct {
	WebsocketFrame `json:",inline"`
	Payload        Message `json:"payload"`
}

func NewWebsocketFramedMessage(msg Message) WFPublishMessage {
	// When publishing, send message fields directly in payload
	return WFPublishMessage{
		WebsocketFrame: WebsocketFrame{
			MessageType: "event",
			Version:     "1.0",
		},
		Payload: msg,
	}
}

type AppendMessageResponse struct {
	Offset      int    `json:"offset"`
	PartitionID string `json:"partition_id"`
	Topic       string `json:"topic"`
	Err         error
}

type WFAppendMessageResp struct {
	WebsocketFrame `json:",inline"`
	Payload        AppendMessageResponse `json:"payload"`
}

func (wf *WebsocketFrame) ToAppendMessageResp() WFAppendMessageResp {
	// Payload comes in as map[string]interface{} from JSON unmarshaling
	payloadMap, ok := wf.Payload.(map[string]interface{})
	if !ok {
		return WFAppendMessageResp{}
	}

	appendResp := AppendMessageResponse{}
	if offset, ok := payloadMap["offset"].(float64); ok {
		appendResp.Offset = int(offset)
	}
	if partitionID, ok := payloadMap["partition_id"].(string); ok {
		appendResp.PartitionID = partitionID
	}
	if topic, ok := payloadMap["topic"].(string); ok {
		appendResp.Topic = topic
	}

	return WFAppendMessageResp{
		WebsocketFrame: *wf,
		Payload:        appendResp,
	}
}

type OffsetCommitMsg struct {
	SubscriptionID string `json:"subscription_id"`
	PartitionID    string `json:"partition_id"`
	Offset         int    `json:"offset"`
	GroupID        string `json:"group_id"`
}

type WFOffsetCommitMsg struct {
	WebsocketFrame `json:",inline"`
	Payload        OffsetCommitMsg `json:"payload"`
}

// FetchPayload is an empty payload for fetch requests
type FetchPayload struct{}

// WFFetchMessage represents a fetch request to pull new messages
type WFFetchMessage struct {
	WebsocketFrame `json:",inline"`
	Payload        FetchPayload `json:"payload"`
}

// FetchResponseEvent represents a single event in a fetch response
type FetchResponseEvent struct {
	SubscriptionID string  `json:"subscription_id"`
	PartitionID    string  `json:"partition_id"`
	Offset         int     `json:"offset"`
	Message        Message `json:"message"`
}

// FetchResponsePayload contains the array of events from a fetch request
type FetchResponsePayload struct {
	Count    int                  `json:"count"`
	Messages []FetchResponseEvent `json:"messages"`
}

// WFFetchResponse represents a fetch response with messages
type WFFetchResponse struct {
	WebsocketFrame `json:",inline"`
	Payload        FetchResponsePayload `json:"payload"`
}

func (wf *WebsocketFrame) ToFetchResponse() WFFetchResponse {
	// Payload comes in as map[string]interface{} from JSON unmarshaling
	payloadMap, ok := wf.Payload.(map[string]interface{})
	if !ok {
		return WFFetchResponse{}
	}

	fetchResp := FetchResponsePayload{}

	if count, ok := payloadMap["count"].(float64); ok {
		fetchResp.Count = int(count)
	}

	if messagesArray, ok := payloadMap["messages"].([]interface{}); ok {
		for _, msgInterface := range messagesArray {
			msgMap, ok := msgInterface.(map[string]interface{})
			if !ok {
				continue
			}

			event := FetchResponseEvent{}

			if subID, ok := msgMap["subscription_id"].(string); ok {
				event.SubscriptionID = subID
			}
			if partID, ok := msgMap["partition_id"].(string); ok {
				event.PartitionID = partID
			}
			if offset, ok := msgMap["offset"].(float64); ok {
				event.Offset = int(offset)
			}

			// Extract message object
			if messageMap, ok := msgMap["message"].(map[string]interface{}); ok {
				msg := Message{}

				if id, ok := messageMap["id"].(string); ok {
					msg.ID = id
				}
				if topic, ok := messageMap["topic"].(string); ok {
					msg.Topic = topic
				}
				if content, ok := messageMap["content"].(string); ok {
					msg.Content = content
				}

				// Store offset and partition in message for convenience
				msg.Offset = event.Offset
				msg.PartitionID = event.PartitionID

				if targetType, ok := messageMap["target_type"].(string); ok {
					msg.TargetType = &targetType
				}
				if targetID, ok := messageMap["target_id"].(string); ok {
					msg.TargetID = &targetID
				}
				if traceID, ok := messageMap["trace_id"].(string); ok {
					msg.TraceID = &traceID
				}
				if orgID, ok := messageMap["org_id"].(string); ok {
					msg.OrgID = &orgID
				}

				event.Message = msg
			}

			fetchResp.Messages = append(fetchResp.Messages, event)
		}
	}

	return WFFetchResponse{
		WebsocketFrame: *wf,
		Payload:        fetchResp,
	}
}
