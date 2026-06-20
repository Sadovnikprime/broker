package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/practic/message-broker/pkg/sdk"
)

func main() {
	managerURL := flag.String("manager", "http://localhost:8080", "Queue manager URL")
	topic := flag.String("topic", "default", "Topic name")
	group := flag.String("group", "default", "Consumer group ID")
	consumerID := flag.String("id", "", "Consumer ID (defaults to hostname)")
	limit := flag.Int("limit", 1, "Messages per poll")
	once := flag.Bool("once", false, "Poll once and exit")
	flag.Parse()

	if *consumerID == "" {
		host, err := os.Hostname()
		if err != nil {
			host = "consumer"
		}
		*consumerID = host
	}

	sub := sdk.NewSubscriber(sdk.SubscriberConfig{
		ManagerURL: *managerURL,
		Topic:      *topic,
		Group:      *group,
		ConsumerID: *consumerID,
		Limit:      *limit,
	})

	if *once {
		runOnce(sub, *consumerID)
		return
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		for {
			messages, err := sub.Poll()
			if err != nil {
				log.Printf("poll error: %v", err)
				time.Sleep(time.Second)
				continue
			}
			for _, msg := range messages {
				fmt.Printf("[%s] received id=%d payload=%s\n", *consumerID, msg.ID, msg.Payload)
				if err := sub.Ack(msg.ID); err != nil {
					log.Printf("ack error: %v", err)
				}
			}
			if len(messages) == 0 {
				time.Sleep(500 * time.Millisecond)
			}
		}
	}()

	<-sigCh
	fmt.Println("subscriber stopped")
}

func runOnce(sub *sdk.Subscriber, consumerID string) {
	messages, err := sub.Poll()
	if err != nil {
		log.Fatalf("poll failed: %v", err)
	}
	for _, msg := range messages {
		fmt.Printf("[%s] received id=%d payload=%s\n", consumerID, msg.ID, msg.Payload)
		if err := sub.Ack(msg.ID); err != nil {
			log.Fatalf("ack failed: %v", err)
		}
	}
	fmt.Printf("processed %d messages\n", len(messages))
}
