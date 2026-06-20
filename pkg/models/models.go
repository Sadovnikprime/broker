package models

import "time"

type StoredMessage struct {
	ID        int64     `json:"id"`
	Topic     string    `json:"topic"`
	Payload   string    `json:"payload"`
	Timestamp time.Time `json:"timestamp"`
}

type PublishRequest struct {
	Payload string `json:"payload"`
}

type PublishResponse struct {
	ID    int64  `json:"id"`
	Topic string `json:"topic"`
}

type TopicStats struct {
	Topic        string `json:"topic"`
	MessageCount int64  `json:"message_count"`
}

type ConsumeRequest struct {
	Topic      string `json:"topic"`
	Group      string `json:"group"`
	ConsumerID string `json:"consumer_id"`
	Limit      int    `json:"limit"`
}

type ConsumedMessage struct {
	ID      int64  `json:"id"`
	Topic   string `json:"topic"`
	Payload string `json:"payload"`
}

type ConsumeResponse struct {
	Messages []ConsumedMessage `json:"messages"`
}

type AckRequest struct {
	Topic      string  `json:"topic"`
	Group      string  `json:"group"`
	ConsumerID string  `json:"consumer_id"`
	Offsets    []int64 `json:"offsets"`
}
 
type RegisterBrokerRequest struct {
	URL string `json:"url"`
}

type BrokerInfo struct {
	URL      string    `json:"url"`
	Active   bool      `json:"active"`
	LastSeen time.Time `json:"last_seen"`
}

type GroupStatus struct {
	Group           string `json:"group"`
	CommittedOffset int64  `json:"committed_offset"`
	AssignOffset    int64  `json:"assign_offset"`
	PendingCount    int    `json:"pending_count"`
	ConsumerCount   int    `json:"consumer_count"`
}

type TopicStatus struct {
	Topic        string        `json:"topic"`
	MessageCount int64         `json:"message_count"`
	Groups       []GroupStatus `json:"groups"`
}

type StatusResponse struct {
	Brokers []BrokerInfo  `json:"brokers"`
	Topics  []TopicStatus `json:"topics"`
}
