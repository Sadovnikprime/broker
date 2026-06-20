package storage

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/practic/message-broker/pkg/models"
)

type TopicStore struct {
	topic        string
	dir          string
	mu           sync.RWMutex
	file         *os.File
	messageCount int64
}


type Manager struct {
	baseDir string
	topics  map[string]*TopicStore
	mu      sync.RWMutex
}

func NewManager(baseDir string) (*Manager, error) {
	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		return nil, fmt.Errorf("create storage dir: %w", err)
	}
	return &Manager{
		baseDir: baseDir,
		topics:  make(map[string]*TopicStore),
	}, nil
}

func (m *Manager) getOrCreateTopic(topic string) (*TopicStore, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if store, ok := m.topics[topic]; ok {
		return store, nil
	}

	topicDir := filepath.Join(m.baseDir, sanitizeTopic(topic))
	if err := os.MkdirAll(topicDir, 0o755); err != nil {
		return nil, fmt.Errorf("create topic dir: %w", err)
	}

	logPath := filepath.Join(topicDir, "messages.log")
	file, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open log file: %w", err)
	}

	count, err := countLines(logPath)
	if err != nil {
		file.Close()
		return nil, err
	}

	store := &TopicStore{
		topic:        topic,
		dir:          topicDir,
		file:         file,
		messageCount: count,
	}
	m.topics[topic] = store
	return store, nil
}

func (m *Manager) Append(topic, payload string) (models.StoredMessage, error) {
	store, err := m.getOrCreateTopic(topic)
	if err != nil {
		return models.StoredMessage{}, err
	}

	store.mu.Lock()
	defer store.mu.Unlock()

	msg := models.StoredMessage{
		ID:        store.messageCount,
		Topic:     topic,
		Payload:   payload,
		Timestamp: time.Now().UTC(),
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return models.StoredMessage{}, err
	}

	if _, err := store.file.Write(append(data, '\n')); err != nil {
		return models.StoredMessage{}, fmt.Errorf("write message: %w", err)
	}
	if err := store.file.Sync(); err != nil {
		return models.StoredMessage{}, fmt.Errorf("sync message: %w", err)
	}

	store.messageCount++
	return msg, nil
}

func (m *Manager) Read(topic string, offset int64, limit int) ([]models.StoredMessage, error) {
	store, err := m.getOrCreateTopic(topic)
	if err != nil {
		return nil, err
	}

	store.mu.RLock()
	defer store.mu.RUnlock()

	logPath := filepath.Join(store.dir, "messages.log")
	file, err := os.Open(logPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer file.Close()

	var messages []models.StoredMessage
	scanner := bufio.NewScanner(file)
	var current int64

	for scanner.Scan() {
		if current < offset {
			current++
			continue
		}
		if len(messages) >= limit {
			break
		}

		var msg models.StoredMessage
		if err := json.Unmarshal(scanner.Bytes(), &msg); err != nil {
			return nil, fmt.Errorf("decode message at offset %d: %w", current, err)
		}
		messages = append(messages, msg)
		current++
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return messages, nil
}

func (m *Manager) MessageCount(topic string) (int64, error) {
	store, err := m.getOrCreateTopic(topic)
	if err != nil {
		return 0, err
	}

	store.mu.RLock()
	defer store.mu.RUnlock()
	return store.messageCount, nil
}

func (m *Manager) ListTopics() ([]string, error) {
	entries, err := os.ReadDir(m.baseDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var topics []string
	for _, entry := range entries {
		if entry.IsDir() {
			topics = append(topics, entry.Name())
		}
	}
	return topics, nil
}

func countLines(path string) (int64, error) {
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	defer file.Close()

	var count int64
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		count++
	}
	return count, scanner.Err()
}

func sanitizeTopic(topic string) string {
	result := make([]byte, 0, len(topic))
	for i := 0; i < len(topic); i++ {
		c := topic[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
			(c >= '0' && c <= '9') || c == '-' || c == '_' {
			result = append(result, c)
		} else {
			result = append(result, '_')
		}
	}
	if len(result) == 0 {
		return "default"
	}
	return string(result)
}
