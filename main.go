package main

import (
	"context"
	"encoding/json"
	"log"

	"github.com/ShareFrame/post-handler/models"
	"github.com/ShareFrame/post-handler/processor"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
)

func handler(ctx context.Context, sqsEvent events.SQSEvent) error {
	var messages []models.FirehoseMessage

	for _, raw := range sqsEvent.Records {
		var msg models.FirehoseMessage
		if err := json.Unmarshal([]byte(raw.Body), &msg); err != nil {
			log.Printf("bad message format: %v", err)
			continue
		}
		if msg.RKey != "" && len(msg.Record) > 0 {
			messages = append(messages, msg)
		}
	}

	if len(messages) == 0 {
		log.Println("no valid messages to process")
		return nil
	}

	if err := processor.BatchWritePosts(ctx, messages); err != nil {
		log.Printf("batch write failed: %v", err)
		return err
	}

	log.Printf("wrote %d records", len(messages))
	return nil
}

func main() {
	lambda.Start(handler)
}
