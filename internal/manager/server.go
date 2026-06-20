package manager

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/practic/message-broker/pkg/models"
)

type pendingAssignment struct {
	ConsumerID string    `json:"consumer_id"`
	AssignedAt time.Time `json:"assigned_at"`
}

type groupState struct {
	CommittedOffset int64                        `json:"committed_offset"`
	AssignOffset    int64                        `json:"assign_offset"`
	Pending         map[string]pendingAssignment `json:"pending"`
	Consumers       map[string]time.Time           `json:"consumers"`
}

type topicState struct {
	Groups map[string]*groupState `json:"groups"`
}

type registryState struct {
	Brokers []models.BrokerInfo `json:"brokers"`
	Topics  map[string]*topicState `json:"topics"`
}

type Server struct {
	mu          sync.Mutex
	state       registryState
	statePath   string
	httpClient  *http.Client
	mux         *http.ServeMux
	addr        string
	server      *http.Server
	brokerTTL   time.Duration
}

func NewServer(addr, stateDir string) (*Server, error) {
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		return nil, fmt.Errorf("create state dir: %w", err)
	}

	statePath := filepath.Join(stateDir, "registry.json")
	s := &Server{
		statePath: statePath,
		httpClient: &http.Client{Timeout: 5 * time.Second},
		mux:       http.NewServeMux(),
		addr:      addr,
		brokerTTL: 15 * time.Second,
	}

	if err := s.loadState(); err != nil {
		return nil, err
	}

	s.routes()
	go s.healthCheckLoop()
	return s, nil
}

func (s *Server) routes() {
	s.mux.HandleFunc("GET /health", s.handleHealth)
	s.mux.HandleFunc("GET /status", s.handleStatus)
	s.mux.HandleFunc("POST /api/v1/brokers/register", s.handleRegisterBroker)
	s.mux.HandleFunc("POST /api/v1/consume", s.handleConsume)
	s.mux.HandleFunc("POST /api/v1/ack", s.handleAck)
}

func (s *Server) Start() error {
	s.server = &http.Server{
		Addr:    s.addr,
		Handler: s.mux,
	}
	log.Printf("queue manager listening on %s", s.addr)
	return s.server.ListenAndServe()
}

func (s *Server) Shutdown() error {
	if err := s.saveState(); err != nil {
		log.Printf("save state on shutdown: %v", err)
	}
	if s.server != nil {
		return s.server.Close()
	}
	return nil
}

func (s *Server) loadState() error {
	data, err := os.ReadFile(s.statePath)
	if err != nil {
		if os.IsNotExist(err) {
			s.state = registryState{
				Topics: make(map[string]*topicState),
			}
			return nil
		}
		return err
	}

	var state registryState
	if err := json.Unmarshal(data, &state); err != nil {
		return fmt.Errorf("parse registry: %w", err)
	}
	if state.Topics == nil {
		state.Topics = make(map[string]*topicState)
	}
	s.state = state
	return nil
}

func (s *Server) saveState() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.saveStateLocked()
}

func (s *Server) saveStateLocked() error {
	data, err := json.MarshalIndent(s.state, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.statePath + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.statePath)
}

func (s *Server) getOrCreateGroup(topic, group string) *groupState {
	topicSt, ok := s.state.Topics[topic]
	if !ok {
		topicSt = &topicState{Groups: make(map[string]*groupState)}
		s.state.Topics[topic] = topicSt
	}
	gs, ok := topicSt.Groups[group]
	if !ok {
		gs = &groupState{
			Pending:   make(map[string]pendingAssignment),
			Consumers: make(map[string]time.Time),
		}
		topicSt.Groups[group] = gs
	}
	return gs
}

func (s *Server) activeBroker() (string, error) {
	now := time.Now().UTC()
	for _, b := range s.state.Brokers {
		if b.Active && now.Sub(b.LastSeen) < s.brokerTTL {
			return b.URL, nil
		}
	}
	return "", fmt.Errorf("no active broker available")
}

func (s *Server) fetchFromBroker(brokerURL, topic string, from int64, limit int) ([]models.StoredMessage, error) {
	url := fmt.Sprintf("%s/api/v1/topics/%s/messages?from=%d&limit=%d", brokerURL, topic, from, limit)
	resp, err := s.httpClient.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("broker returned %d: %s", resp.StatusCode, string(body))
	}

	var result struct {
		Messages []models.StoredMessage `json:"messages"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	return result.Messages, nil
}

func (s *Server) topicMessageCount(brokerURL, topic string) (int64, error) {
	url := fmt.Sprintf("%s/api/v1/topics/%s/stats", brokerURL, topic)
	resp, err := s.httpClient.Get(url)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("broker stats returned %d", resp.StatusCode)
	}

	var stats models.TopicStats
	if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
		return 0, err
	}
	return stats.MessageCount, nil
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleRegisterBroker(w http.ResponseWriter, r *http.Request) {
	var req models.RegisterBrokerRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if req.URL == "" {
		writeError(w, http.StatusBadRequest, "url is required")
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().UTC()
	found := false
	for i, b := range s.state.Brokers {
		if b.URL == req.URL {
			s.state.Brokers[i].Active = true
			s.state.Brokers[i].LastSeen = now
			found = true
			break
		}
	}
	if !found {
		s.state.Brokers = append(s.state.Brokers, models.BrokerInfo{
			URL:      req.URL,
			Active:   true,
			LastSeen: now,
		})
	}

	if err := s.saveStateLocked(); err != nil {
		log.Printf("save state: %v", err)
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "registered"})
}

func (s *Server) handleConsume(w http.ResponseWriter, r *http.Request) {
	var req models.ConsumeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if req.Topic == "" || req.Group == "" || req.ConsumerID == "" {
		writeError(w, http.StatusBadRequest, "topic, group and consumer_id are required")
		return
	}
	if req.Limit <= 0 {
		req.Limit = 10
	}
	if req.Limit > 100 {
		req.Limit = 100
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	brokerURL, err := s.activeBroker()
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err.Error())
		return
	}

	gs := s.getOrCreateGroup(req.Topic, req.Group)
	gs.Consumers[req.ConsumerID] = time.Now().UTC()

	var delivered []models.ConsumedMessage
	now := time.Now().UTC()

	for len(delivered) < req.Limit {
		messages, err := s.fetchFromBroker(brokerURL, req.Topic, gs.AssignOffset, 1)
		if err != nil {
			if len(delivered) == 0 {
				writeError(w, http.StatusBadGateway, fmt.Sprintf("broker fetch failed: %v", err))
				return
			}
			break
		}
		if len(messages) == 0 {
			break
		}

		msg := messages[0]
		offsetKey := fmt.Sprintf("%d", msg.ID)

		if _, pending := gs.Pending[offsetKey]; pending {
			gs.AssignOffset = msg.ID + 1
			continue
		}

		gs.Pending[offsetKey] = pendingAssignment{
			ConsumerID: req.ConsumerID,
			AssignedAt: now,
		}
		gs.AssignOffset = msg.ID + 1

		delivered = append(delivered, models.ConsumedMessage{
			ID:      msg.ID,
			Topic:   msg.Topic,
			Payload: msg.Payload,
		})
	}

	if err := s.saveStateLocked(); err != nil {
		log.Printf("save state: %v", err)
	}

	writeJSON(w, http.StatusOK, models.ConsumeResponse{Messages: delivered})
}

func (s *Server) handleAck(w http.ResponseWriter, r *http.Request) {
	var req models.AckRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if req.Topic == "" || req.Group == "" || req.ConsumerID == "" {
		writeError(w, http.StatusBadRequest, "topic, group and consumer_id are required")
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	topicSt, ok := s.state.Topics[req.Topic]
	if !ok {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
		return
	}
	gs, ok := topicSt.Groups[req.Group]
	if !ok {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
		return
	}

	for _, offset := range req.Offsets {
		key := fmt.Sprintf("%d", offset)
		pending, exists := gs.Pending[key]
		if !exists || pending.ConsumerID != req.ConsumerID {
			continue
		}
		delete(gs.Pending, key)
	}

	for {
		key := fmt.Sprintf("%d", gs.CommittedOffset)
		if _, stillPending := gs.Pending[key]; stillPending {
			break
		}
		if gs.CommittedOffset >= gs.AssignOffset {
			break
		}
		gs.CommittedOffset++
	}

	if err := s.saveStateLocked(); err != nil {
		log.Printf("save state: %v", err)
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleStatus(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()

	resp := models.StatusResponse{
		Brokers: append([]models.BrokerInfo(nil), s.state.Brokers...),
	}

	brokerURL, brokerErr := s.activeBroker()

	for topic, topicSt := range s.state.Topics {
		ts := models.TopicStatus{Topic: topic}
		if brokerErr == nil {
			if count, err := s.topicMessageCount(brokerURL, topic); err == nil {
				ts.MessageCount = count
			}
		}

		for groupName, gs := range topicSt.Groups {
			ts.Groups = append(ts.Groups, models.GroupStatus{
				Group:           groupName,
				CommittedOffset: gs.CommittedOffset,
				AssignOffset:    gs.AssignOffset,
				PendingCount:    len(gs.Pending),
				ConsumerCount:   len(gs.Consumers),
			})
		}
		resp.Topics = append(resp.Topics, ts)
	}

	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) healthCheckLoop() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		s.mu.Lock()
		now := time.Now().UTC()
		for i, b := range s.state.Brokers {
			active := s.pingBroker(b.URL)
			s.state.Brokers[i].Active = active
			if active {
				s.state.Brokers[i].LastSeen = now
			}
		}
		_ = s.saveStateLocked()
		s.mu.Unlock()
	}
}

func (s *Server) pingBroker(url string) bool {
	resp, err := s.httpClient.Get(url + "/health")
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
