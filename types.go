package event_bus_client

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
	message, ok := wf.Payload.(WFMessage)
	if !ok {
		return WFMessage{}
	}
	return message
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
	resp, ok := wf.Payload.(WFSubscriptionResp)
	if !ok {
		return WFSubscriptionResp{}
	}
	return resp
}

// the main message structure used in the event bus
type Message struct {
	ID         string  `bson:"_id,omitempty" json:"id"`
	Topic      string  `bson:"topic" json:"topic"`
	Content    string  `bson:"content" json:"content"`
	TargetType *string `bson:"target_type" json:"target_type"`
	TargetID   *string `bson:"target_id" json:"target_id"`
	TraceID    *string `bson:"trace_id,omitempty" json:"trace_id,omitempty"`
	OrgID      *string `bson:"org_id,omitempty" json:"org_id,omitempty"`
}

type WFMessage struct {
	WebsocketFrame `json:",inline"`
	Payload        Message `json:"payload"`
}

func NewWebsocketFramedMessage(msg Message) WFMessage {
	return WFMessage{
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
	resp, ok := wf.Payload.(WFAppendMessageResp)
	if !ok {
		return WFAppendMessageResp{}
	}
	return resp
}

type OffsetCommitMsg struct {
	SubscriptionID string `json:"subscription_id"`
	Offset         int    `json:"offset"`
	GroupID        string `json:"group_id"`
}

type WFOffsetCommitMsg struct {
	WebsocketFrame `json:",inline"`
	Payload        OffsetCommitMsg `json:"payload"`
}
