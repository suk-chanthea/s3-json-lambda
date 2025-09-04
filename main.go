package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Message represents a single message (NO filename field inside JSON)
type Message struct {
	Sender    string `json:"sender"`
	Receiver  string `json:"receiver"`
	Message   string `json:"message"`
	Date      string `json:"date"`
}

type AllMessages []Message

var bucketName = "https://mongkol.s3.ap-southeast-1.amazonaws.com/data" // ✅ REPLACE with your real S3 bucket

// buildS3Key: filename=file1 → file1.json
func buildS3Key(filenameParam string) string {
	clean := strings.TrimSpace(filenameParam)
	if clean == "" {
		clean = "default"
	}
	return clean + ".json"
}

// getS3JSON: fetch JSON array from S3 file (e.g. file1.json)
func getS3JSON(ctx context.Context, s3Key string) (AllMessages, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to load SDK config: %v", err)
	}

	s3Client := s3.NewFromConfig(cfg)

	// Check if object exists
	_, err = s3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(s3Key),
	})

	if err != nil {
		if isS3NotFoundErr(err) {
			// File does not exist yet → return empty array
			return []Message{}, nil
		}
		return nil, fmt.Errorf("failed to head S3 object: %v", err)
	}

	// Fetch the file
	resp, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(s3Key),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get S3 object: %v", err)
	}
	defer resp.Body.Close()

	var messages AllMessages
	if err := json.NewDecoder(resp.Body).Decode(&messages); err != nil {
		return nil, fmt.Errorf("failed to decode JSON: %v", err)
	}

	return messages, nil
}

// isS3NotFoundErr: rough check for "file not found"
func isS3NotFoundErr(err error) bool {
	return err != nil && (err.Error() == "NotFound" || err.Error() == "no such key" || err.Error() == "Not Found" || err.Error() == "s3.ErrCodeNoSuchKey")
}

// putS3JSON: save JSON array back to S3 file
func putS3JSON(ctx context.Context, s3Key string, messages AllMessages) error {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return fmt.Errorf("unable to load SDK config: %v", err)
	}

	s3Client := s3.NewFromConfig(cfg)

	jsonData, err := json.MarshalIndent(messages, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal JSON: %v", err)
	}

	_, err = s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(s3Key),
		Body:   bytes.NewReader(jsonData),
	})
	if err != nil {
		return fmt.Errorf("failed to put S3 object: %v", err)
	}

	return nil
}

// Handler: GET or UPDATE messages in fileX.json
func Handler(ctx context.Context, request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	action := request.QueryStringParameters["action"]
	if action == "" {
		return clientError(400, "Missing 'action' parameter (use 'get' or 'update')")
	}

	filenameParam := request.QueryStringParameters["filename"]
	if filenameParam == "" {
		return clientError(400, "Missing 'filename' parameter")
	}

	s3Key := buildS3Key(filenameParam)

	switch action {
	case "get":
		messages, err := getS3JSON(ctx, s3Key)
		if err != nil {
			return clientError(500, fmt.Sprintf("Failed to get messages: %v", err))
		}
		jsonData, err := json.Marshal(messages)
		if err != nil {
			return clientError(500, fmt.Sprintf("Failed to marshal: %v", err))
		}
		return events.APIGatewayProxyResponse{
			StatusCode: 200,
			Body:       string(jsonData),
			Headers:    map[string]string{"Content-Type": "application/json"},
		}, nil

	case "update":
		sender := request.QueryStringParameters["sender"]
		receiver := request.QueryStringParameters["receiver"]
		msg := request.QueryStringParameters["message"]
		date := request.QueryStringParameters["date"]

		if sender == "" || receiver == "" || msg == "" || date == "" {
			return clientError(400, "Missing required fields for update: sender, receiver, message, date")
		}

		messages, err := getS3JSON(ctx, s3Key)
		if err != nil {
			return clientError(500, fmt.Sprintf("Failed to retrieve existing messages: %v", err))
		}

		newMessage := Message{
			Sender:   sender,
			Receiver: receiver,
			Message:  msg,
			Date:     date,
		}
		messages = append(messages, newMessage)

		if err := putS3JSON(ctx, s3Key, messages); err != nil {
			return clientError(500, fmt.Sprintf("Failed to save messages: %v", err))
		}

		return events.APIGatewayProxyResponse{
			StatusCode: 200,
			Body:       `{"status": "updated"}`,
			Headers:    map[string]string{"Content-Type": "application/json"},
		}, nil

	default:
		return clientError(400, "Invalid action. Use 'get' or 'update'")
	}
}

func clientError(status int, body string) (events.APIGatewayProxyResponse, error) {
	return events.APIGatewayProxyResponse{
		StatusCode: status,
		Body:       `{ "error": "` + body + `" }`,
		Headers:    map[string]string{"Content-Type": "application/json"},
	}, nil
}

func main() {
	lambda.Start(Handler)
}