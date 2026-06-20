package sdk

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/practic/message-broker/pkg/models"
)

type Publisher struct {
	brokerURL  string
	httpClient *http.Client
}

func NewPublisher(brokerURL string) *Publisher {
	return &Publisher{
		brokerURL: brokerURL,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (p *Publisher) Publish(topic string, payload string) (*models.PublishResponse, error) {
	body, err := json.Marshal(models.PublishRequest{Payload: payload})
	if err != nil {
		return nil, err
	}

	url := fmt.Sprintf("%s/api/v1/topics/%s/messages", p.brokerURL, topic)
	resp, err := p.httpClient.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("publish request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("publish failed (%d): %s", resp.StatusCode, string(data))
	}

	var result models.PublishResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

type Subscriber struct {
	managerURL string
	topic      string
	group      string
	consumerID string
	limit      int
	httpClient *http.Client
}

type SubscriberConfig struct {
	ManagerURL string
	Topic      string
	Group      string
	ConsumerID string
	Limit      int
}

func NewSubscriber(cfg SubscriberConfig) *Subscriber {
	limit := cfg.Limit
	if limit <= 0 {
		limit = 10
	}
	return &Subscriber{
		managerURL: cfg.ManagerURL,
		topic:      cfg.Topic,
		group:      cfg.Group,
		consumerID: cfg.ConsumerID,
		limit:      limit,
		httpClient: &http.Client{Timeout: 30 * time.Second},
	}
}

func (s *Subscriber) Poll() ([]models.ConsumedMessage, error) {
	req := models.ConsumeRequest{
		Topic:      s.topic,
		Group:      s.group,
		ConsumerID: s.consumerID,
		Limit:      s.limit,
	}
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	resp, err := s.httpClient.Post(s.managerURL+"/api/v1/consume", "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("consume request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("consume failed (%d): %s", resp.StatusCode, string(data))
	}

	var result models.ConsumeResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	return result.Messages, nil
}

func (s *Subscriber) Ack(offsets ...int64) error {
	req := models.AckRequest{
		Topic:      s.topic,
		Group:      s.group,
		ConsumerID: s.consumerID,
		Offsets:    offsets,
	}
	body, err := json.Marshal(req)
	if err != nil {
		return err
	}

	resp, err := s.httpClient.Post(s.managerURL+"/api/v1/ack", "application/json", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("ack request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("ack failed (%d): %s", resp.StatusCode, string(data))
	}
	return nil
}

type MessageHandler func(msg models.ConsumedMessage) error

func (s *Subscriber) Run(handler MessageHandler, pollInterval time.Duration) error {
	if pollInterval <= 0 {
		pollInterval = time.Second
	}

	for {
		messages, err := s.Poll()
		if err != nil {
			return err
		}

		if len(messages) == 0 {
			time.Sleep(pollInterval)
			continue
		}

		for _, msg := range messages {
			if err := handler(msg); err != nil {
				return err
			}
			if err := s.Ack(msg.ID); err != nil {
				return err
			}
		}
	}
}

func RegisterBroker(managerURL, brokerURL string) error {
	body, _ := json.Marshal(models.RegisterBrokerRequest{URL: brokerURL})
	resp, err := http.Post(managerURL+"/api/v1/brokers/register", "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("register failed: %s", string(data))
	}
	return nil
}

func GetStatus(managerURL string) (*models.StatusResponse, error) {
	resp, err := http.Get(managerURL + "/status")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("status failed: %s", string(data))
	}

	var status models.StatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return nil, err
	}
	return &status, nil
}
