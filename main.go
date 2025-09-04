package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/awslabs/aws-lambda-go-api-proxy/gin"
	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
)

// ======================
// 📦 Types
// ======================

type Message struct {
	ID       int    `json:"id"`
	Sender   string `json:"sender" binding:"required"`
	Receiver string `json:"receiver" binding:"required"`
	Message  string `json:"message" binding:"required"`
	Date     string `json:"date" binding:"required"`
}

type AllMessages []Message

// ======================
// 🌍 Env & S3 Setup
// ======================

var (
	bucketName string
	ginLambda  *ginadapter.GinLambda
)

func init() {
	err := godotenv.Load(".env")
	if err != nil {
		log.Println("⚠️ .env file not found, using OS environment variables")
	}

	bucketName = os.Getenv("S3_BUCKET_NAME")
	if bucketName == "" {
		log.Fatalf("❌ S3_BUCKET_NAME environment variable not set")
	}
}

func buildS3Key(filename string) string {
	return filename + ".json"
}

// ======================
// 📤 S3: Get JSON
// ======================

func getS3JSON(ctx context.Context, cfg aws.Config, s3Key string) (AllMessages, error) {
	s3Client := s3.NewFromConfig(cfg)

	_, err := s3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(s3Key),
	})
	if err != nil {
		if isS3NotFoundErr(err) {
			return []Message{}, nil // File not found → return empty array
		}
		return nil, fmt.Errorf("head failed: %v", err)
	}

	resp, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(s3Key),
	})
	if err != nil {
		return nil, fmt.Errorf("get failed: %v", err)
	}
	defer resp.Body.Close()

	var messages AllMessages
	if err := json.NewDecoder(resp.Body).Decode(&messages); err != nil {
		return nil, fmt.Errorf("decode failed: %v", err)
	}

	return messages, nil
}

// ======================
// 📥 S3: Save JSON
// ======================

func putS3JSON(ctx context.Context, cfg aws.Config, s3Key string, messages AllMessages) error {
	s3Client := s3.NewFromConfig(cfg)

	data, err := json.MarshalIndent(messages, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal failed: %v", err)
	}

	_, err = s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(s3Key),
		Body:   bytes.NewReader(data),
	})
	if err != nil {
		return fmt.Errorf("put failed: %v", err)
	}

	return nil
}

// ======================
// ❓ S3: Is Not Found?
// ======================

func isS3NotFoundErr(err error) bool {
	return err != nil && (err.Error() == "NotFound" || err.Error() == "no such key" || err.Error() == "s3.ErrCodeNoSuchKey")
}

// ======================
// 🧩 Gin Handlers (Normal Mode)
// ======================

func setupGinHandlers() *gin.Engine {
	r := gin.Default()

	r.GET("/", func(c *gin.Context) {
		c.JSON(200, gin.H{"message": "Gin + Lambda + S3 CRUD API"})
	})

	// Example routes (you can expand these or use API Gateway proxy)
	// Normally you'd use API Gateway for Lambda, but here's how you'd structure them in Gin:
	// r.GET("/messages", getMessagesHandler)
	// r.POST("/messages?filename=file1", addMessageHandler)
	// etc.

	return r
}

// ======================
// 🧠 Lambda Handler (API Gateway Proxy)
// ======================

type APIRequest struct {
	Action   string `json:"action"`   // "get", "add", "update", "delete"
	Filename string `json:"filename"` // → file1.json
	// For ADD:
	Sender   string `json:"sender,omitempty"`
	Receiver string `json:"receiver,omitempty"`
	Message  string `json:"message,omitempty"`
	Date     string `json:"date,omitempty"`
	// For UPDATE / DELETE: you can add "id" or "index"
	ID int `json:"id,omitempty"` // Used to update/delete specific item
}

type APIResponse struct {
	Status  string      `json:"status,omitempty"`
	Data    interface{} `json:"data,omitempty"`
	Message string      `json:"message,omitempty"`
}

func Handler(ctx context.Context, req events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	// Parse body
	var input APIRequest
	if err := json.Unmarshal([]byte(req.Body), &input); err != nil {
		return clientError(400, "Invalid JSON body"), nil
	}

	if input.Action == "" || input.Filename == "" {
		return clientError(400, "Missing 'action' or 'filename'"), nil
	}

	s3Key := buildS3Key(input.Filename)

	switch input.Action {
	case "get":
		messages, err := getS3JSON(ctx, cfg, s3Key)
		if err != nil {
			return clientError(500, fmt.Sprintf("Get failed: %v", err)), nil
		}
		return successResponse(messages), nil

	case "add":
		if input.Sender == "" || input.Receiver == "" || input.Message == "" || input.Date == "" {
			return clientError(400, "Missing fields for add: sender, receiver, message, date"), nil
		}

		messages, err := getS3JSON(ctx, cfg, s3Key)
		if err != nil {
			return clientError(500, fmt.Sprintf("Get failed: %v", err)), nil
		}

		newID := 1
		if len(messages) > 0 {
			newID = messages[len(messages)-1].ID + 1
		}

		newMsg := Message{
			ID:       newID,
			Sender:   input.Sender,
			Receiver: input.Receiver,
			Message:  input.Message,
			Date:     input.Date,
		}

		messages = append(messages, newMsg)
		if err := putS3JSON(ctx, cfg, s3Key, messages); err != nil {
			return clientError(500, fmt.Sprintf("Save failed: %v", err)), nil
		}

		return successResponse(newMsg), nil

	case "update":
		// TODO: Find message by ID and update fields
		return clientError(501, "Update not implemented yet"), nil

	case "delete":
		// TODO: Find message by ID and remove it
		return clientError(501, "Delete not implemented yet"), nil

	default:
		return clientError(400, "Invalid action. Use: get, add, update, delete"), nil
	}
}
// ======================
// 🧩 Helpers
// ======================

func successResponse(data interface{}) events.APIGatewayProxyResponse {
	return events.APIGatewayProxyResponse{
		StatusCode: 200,
		Body:       toJson(data),
		Headers:    map[string]string{"Content-Type": "application/json"},
	}
}

func clientError(status int, msg string) events.APIGatewayProxyResponse {
	return events.APIGatewayProxyResponse{
		StatusCode: status,
		Body:       toJson(map[string]string{"error": msg}),
		Headers:    map[string]string{"Content-Type": "application/json"},
	}
}

func toJson(v interface{}) string {
	b, _ := json.Marshal(v)
	return string(b)
}

var cfg aws.Config

func main() {
	var err error
	cfg, err = config.LoadDefaultConfig(context.Background())
	if err != nil {
		log.Fatalf("❌ AWS config error: %v", err)
	}

	// ✅ Detect: running on Lambda or locally?
	if os.Getenv("AWS_LAMBDA_FUNCTION_NAME") != "" {
		// Lambda mode
		lambda.Start(Handler)
	} else {
		// Local mode: run Gin HTTP server
		r := setupGinHandlers()
		r.Run(":8080")
	}
}