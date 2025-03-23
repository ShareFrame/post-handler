package processor

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/ShareFrame/post-handler/models"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
)

type mockDynamoClient struct {
	calls     int
	lastBatch []types.WriteRequest
	err       error
}

func (m *mockDynamoClient) BatchWriteItem(ctx context.Context, input *dynamodb.BatchWriteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error) {
	m.calls++
	for _, wrs := range input.RequestItems {
		m.lastBatch = wrs
	}
	if m.err != nil {
		return nil, m.err
	}
	return &dynamodb.BatchWriteItemOutput{}, nil
}

func mustJSON(v any) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}

func TestBatchWritePosts(t *testing.T) {
	tests := []struct {
		name     string
		input    []models.FirehoseMessage
		mockErr  error
		wantErr  bool
		expected int
	}{
		{
			name: "single valid record",
			input: []models.FirehoseMessage{
				{
					RKey: "abc123",
					Seq:  1,
					Record: mustJSON(map[string]any{
						"text":      "hello world",
						"createdAt": time.Now().Format(time.RFC3339),
					}),
				},
			},
			expected: 1,
		},
		{
			name: "empty record skipped",
			input: []models.FirehoseMessage{
				{RKey: "abc", Seq: 1, Record: nil},
			},
			expected: 0,
		},
		{
			name: "invalid JSON skipped",
			input: []models.FirehoseMessage{
				{RKey: "abc", Seq: 1, Record: json.RawMessage(`{this is broken}`)},
			},
			expected: 0,
		},
		{
			name: "batch of 30 chunks into two",
			input: func() []models.FirehoseMessage {
				var msgs []models.FirehoseMessage
				for i := 0; i < 30; i++ {
					msgs = append(msgs, models.FirehoseMessage{
						RKey:   "id" + string(rune(i)),
						Seq:    i,
						Record: mustJSON(map[string]any{"createdAt": time.Now().Format(time.RFC3339)}),
					})
				}
				return msgs
			}(),
			expected: 2,
		},
		{
			name: "DynamoDB failure returns error",
			input: []models.FirehoseMessage{
				{RKey: "abc", Seq: 1, Record: mustJSON(map[string]any{"createdAt": "now"})},
			},
			mockErr:  errors.New("boom"),
			wantErr:  true,
			expected: 1,
		},
	}

	realClient := dynamoClient
	defer func() { dynamoClient = realClient }()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := &mockDynamoClient{err: tt.mockErr}
			dynamoClient = mock
			tableName = "FakeTable"

			err := BatchWritePosts(context.Background(), tt.input)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tt.expected, mock.calls)
		})
	}
}
