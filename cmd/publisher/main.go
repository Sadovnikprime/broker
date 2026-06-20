package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/practic/message-broker/pkg/sdk"
)

func main() {
	brokerURL := flag.String("broker", "http://localhost:8081", "Broker URL")
	topic := flag.String("topic", "default", "Topic name")
	count := flag.Int("count", 1, "Number of messages to send")
	prefix := flag.String("prefix", "msg", "Message prefix")
	flag.Parse()

	pub := sdk.NewPublisher(*brokerURL)

	for i := 0; i < *count; i++ {
		payload := fmt.Sprintf("%s-%d", *prefix, i)
		resp, err := pub.Publish(*topic, payload)
		if err != nil {
			log.Fatalf("publish failed: %v", err)
		}
		fmt.Printf("published id=%d topic=%s payload=%s\n", resp.ID, resp.Topic, payload)
	}
}
