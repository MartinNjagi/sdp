package connections

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"sdp/data"
)

// InitStorageClient initializes an S3 client configured for either standard AWS S3 or local MinIO.
// It uses S3 if configured, falls back to MinIO, and errors if neither is valid.
func InitStorageClient(ctx context.Context, env *data.AppConfig) (*s3.Client, error) {
	// 1. Evaluate what is currently configured in the environment
	// (Adjust these field names if your AppConfig struct uses different naming)
	hasMinio := env.MinioEndpoint != "" && env.MinioAccessKey != "" && env.MinioSecretKey != ""

	// Assuming that if an AWS Region is provided (and no MinIO endpoint), the user wants standard S3.
	// Standard AWS S3 credentials (AWS_ACCESS_KEY_ID) are usually picked up automatically from env vars.
	hasS3 := env.AWSRegion != "" && env.MinioEndpoint == ""

	// 2. Error if neither is configured
	if !hasS3 && !hasMinio {
		return nil, errors.New("storage configuration missing: neither AWS S3 nor MinIO is configured")
	}

	// 3. Initialize Standard AWS S3 (takes precedence as requested)
	if hasS3 {
		cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(env.AWSRegion))
		if err != nil {
			return nil, fmt.Errorf("unable to load AWS S3 config: %w", err)
		}

		// Create standard client (uses virtual-hosted style by default: bucket.s3.amazonaws.com)
		return s3.NewFromConfig(cfg), nil
	}

	// 4. Initialize MinIO Client (Fallback)
	creds := credentials.NewStaticCredentialsProvider(env.MinioAccessKey, env.MinioSecretKey, "")

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"), // Region doesn't strictly matter for local MinIO, but SDK requires one
		config.WithCredentialsProvider(creds),
	)
	if err != nil {
		return nil, fmt.Errorf("unable to load MinIO SDK config: %w", err)
	}

	// Create the MinIO client, explicitly overriding the endpoint and enabling Path-Style
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(env.MinioEndpoint)
		o.UsePathStyle = true // CRITICAL FOR MINIO: Forces http://localhost:9000/bucket
	})

	return client, nil
}
