package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/ShareFrame/post-handler/models"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

var (
	dynamoClient DynamoDBAPI
	tableName    = os.Getenv("DYNAMODB_TABLE")
)

type DynamoDBAPI interface {
	BatchWriteItem(ctx context.Context, params *dynamodb.BatchWriteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error)
}

func init() {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatalf("failed to load AWS config: %v", err)
	}
	dynamoClient = dynamodb.NewFromConfig(cfg)
}

func BatchWritePosts(ctx context.Context, messages []models.FirehoseMessage) error {
	const maxBatchSize = 25

	for i := 0; i < len(messages); i += maxBatchSize {
		end := i + maxBatchSize
		if end > len(messages) {
			end = len(messages)
		}
		batch := messages[i:end]

		writeReqs := make([]types.WriteRequest, 0, len(batch))

		for _, msg := range batch {
			var post map[string]interface{}
			if err := json.Unmarshal(msg.Record, &post); err != nil {
				log.Printf("invalid record JSON: %v", err)
				continue
			}
			post["tid"] = msg.RKey
			post["seq"] = msg.Seq
			if _, ok := post["createdAt"]; !ok {
				post["createdAt"] = time.Now().Format(time.RFC3339)
			}

			item, err := attributevalue.MarshalMap(post)
			if err != nil {
				log.Printf("marshal error: %v", err)
				continue
			}

			writeReqs = append(writeReqs, types.WriteRequest{
				PutRequest: &types.PutRequest{Item: item},
			})
		}

		if len(writeReqs) == 0 {
			continue
		}

		_, err := dynamoClient.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]types.WriteRequest{
				tableName: writeReqs,
			},
		})
		if err != nil {
			return fmt.Errorf("dynamodb batch write error: %w", err)
		}
	}

	return nil
}
