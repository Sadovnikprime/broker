package main

import (
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/practic/message-broker/internal/manager"
)

func main() {
	addr := flag.String("addr", ":8080", "HTTP listen address")
	stateDir := flag.String("state", "./data/manager", "Directory for offset registry")
	flag.Parse()

	server, err := manager.NewServer(*addr, *stateDir)
	if err != nil {
		log.Fatalf("failed to create manager: %v", err)
	}

	go func() {
		if err := server.Start(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("manager error: %v", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("shutting down manager...")
	if err := server.Shutdown(); err != nil {
		log.Printf("shutdown error: %v", err)
	}
}
