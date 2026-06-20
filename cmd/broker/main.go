package main

import (
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/practic/message-broker/internal/broker"
	"github.com/practic/message-broker/pkg/sdk"
)

func main() {
	addr := flag.String("addr", ":8081", "HTTP listen address")
	dataDir := flag.String("data", "./data/broker", "Directory for message storage")
	managerURL := flag.String("manager", "http://localhost:8080", "Queue manager URL for auto-registration")
	publicURL := flag.String("public-url", "http://localhost:8081", "Public URL reported to manager")
	flag.Parse()

	server, err := broker.NewServer(*addr, *dataDir)
	if err != nil {
		log.Fatalf("failed to create broker: %v", err)
	}

	go func() {
		registerLoop(*managerURL, *publicURL)
	}()

	go func() {
		if err := server.Start(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("broker error: %v", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("shutting down broker...")
	if err := server.Shutdown(); err != nil {
		log.Printf("shutdown error: %v", err)
	}
}

func registerLoop(managerURL, brokerURL string) {
	for {
		if err := sdk.RegisterBroker(managerURL, brokerURL); err != nil {
			log.Printf("broker registration failed: %v", err)
		} else {
			log.Printf("registered with manager at %s", managerURL)
		}
		time.Sleep(5 * time.Second)
	}
}
