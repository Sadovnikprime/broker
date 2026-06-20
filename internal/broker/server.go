package broker

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/practic/message-broker/internal/storage"
	"github.com/practic/message-broker/pkg/models"
)

type Server struct {
	store  *storage.Manager
	mux    *http.ServeMux
	addr   string
	server *http.Server
}

func NewServer(addr, dataDir string) (*Server, error) {
	store, err := storage.NewManager(dataDir)
	if err != nil {
		return nil, err
	}

	s := &Server{
		store: store,
		mux:   http.NewServeMux(),
		addr:  addr,
	}
	s.routes()
	return s, nil
}

func (s *Server) routes() {
	s.mux.HandleFunc("GET /health", s.handleHealth)
	s.mux.HandleFunc("POST /api/v1/topics/{topic}/messages", s.handlePublish)
	s.mux.HandleFunc("GET /api/v1/topics/{topic}/messages", s.handleRead)
	s.mux.HandleFunc("GET /api/v1/topics/{topic}/stats", s.handleStats)
}

func (s *Server) Start() error {
	s.server = &http.Server{
		Addr:    s.addr,
		Handler: s.mux,
	}
	log.Printf("broker listening on %s", s.addr)
	return s.server.ListenAndServe()
}

func (s *Server) Shutdown() error {
	if s.server != nil {
		return s.server.Close()
	}
	return nil
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handlePublish(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	if topic == "" {
		writeError(w, http.StatusBadRequest, "topic is required")
		return
	}

	var req models.PublishRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if strings.TrimSpace(req.Payload) == "" {
		writeError(w, http.StatusBadRequest, "payload is required")
		return
	}

	msg, err := s.store.Append(topic, req.Payload)
	if err != nil {
		log.Printf("publish error: %v", err)
		writeError(w, http.StatusInternalServerError, "failed to store message")
		return
	}

	writeJSON(w, http.StatusOK, models.PublishResponse{
		ID:    msg.ID,
		Topic: msg.Topic,
	})
}

func (s *Server) handleRead(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	if topic == "" {
		writeError(w, http.StatusBadRequest, "topic is required")
		return
	}

	offset, err := parseIntQuery(r, "from", 0)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	limit, err := parseIntQuery(r, "limit", 10)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if limit <= 0 || limit > 1000 {
		writeError(w, http.StatusBadRequest, "limit must be between 1 and 1000")
		return
	}

	messages, err := s.store.Read(topic, offset, int(limit))
	if err != nil {
		log.Printf("read error: %v", err)
		writeError(w, http.StatusInternalServerError, "failed to read messages")
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"messages": messages,
		"from":     offset,
		"count":    len(messages),
	})
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	if topic == "" {
		writeError(w, http.StatusBadRequest, "topic is required")
		return
	}

	count, err := s.store.MessageCount(topic)
	if err != nil {
		log.Printf("stats error: %v", err)
		writeError(w, http.StatusInternalServerError, "failed to get stats")
		return
	}

	writeJSON(w, http.StatusOK, models.TopicStats{
		Topic:        topic,
		MessageCount: count,
	})
}

func parseIntQuery(r *http.Request, key string, defaultVal int) (int64, error) {
	val := r.URL.Query().Get(key)
	if val == "" {
		return int64(defaultVal), nil
	}
	parsed, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return 0, err
	}
	return parsed, nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
