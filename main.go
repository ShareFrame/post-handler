package main

import (
	"context"
	"encoding/json"
	"log"
	"os"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

var (
	dynamoClient *dynamodb.Client
	tableName    string
)

type FirehoseMessage struct {
	RKey   string          `json:"rkey"`
	Seq    int             `json:"seq"`
	Record json.RawMessage `json:"record"`
}

func init() {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatalf("Failed to load AWS config: %v", err)
	}
	dynamoClient = dynamodb.NewFromConfig(cfg)

	tableName = os.Getenv("DYNAMODB_TABLE")
	if tableName == "" {
		log.Fatal("DYNAMODB_TABLE environment variable not set")
	}
}

func handler(ctx context.Context, sqsEvent events.SQSEvent) error {
	for _, msg := range sqsEvent.Records {
		var firehose FirehoseMessage
		if err := json.Unmarshal([]byte(msg.Body), &firehose); err != nil {
			log.Printf("Failed to parse message: %v", err)
			continue
		}

		if firehose.RKey == "" || len(firehose.Record) == 0 {
			log.Println("Invalid message: missing rkey or record")
			continue
		}

		var item map[string]types.AttributeValue
		var record map[string]any
		if err := json.Unmarshal(firehose.Record, &record); err != nil {
			log.Printf("Failed to parse record JSON: %v", err)
			continue
		}

		record["tid"] = firehose.RKey
		record["seq"] = firehose.Seq

		item, err := attributevalue.MarshalMap(record)
		if err != nil {
			log.Printf("Failed to convert to DynamoDB format: %v", err)
			continue
		}

		_, err = dynamoClient.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item:      item,
		})
		if err != nil {
			log.Printf("Failed to put item to DynamoDB: %v", err)
			continue
		}

		log.Printf("Successfully stored post: %s", firehose.RKey)
	}

	return nil
}

func main() {
	lambda.Start(handler)
}
