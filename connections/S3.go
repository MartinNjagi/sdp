package connections

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"sdp/data"
)

// InitMinioClient initializes an S3 client configured for local MinIO
func InitMinioClient(env *data.AppConfig) (*s3.Client, error) {

	ctx := context.Background()
	accessKey := env.MinioAccessKey
	secretKey := env.MinioSecretKey
	endpoint := env.MinioEndpoint

	// Create a static credentials provider using your MinIO keys
	creds := credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")

	// Load the AWS config, overriding it with our custom MinIO settings
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"), // Region doesn't strictly matter for local MinIO, but SDK requires one
		config.WithCredentialsProvider(creds),
	)
	if err != nil {
		return nil, fmt.Errorf("unable to load AWS SDK config: %w", err)
	}

	// Create the S3 client, explicitly overriding the endpoint and enabling Path-Style
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true // CRITICAL FOR MINIO: Forces http://localhost:9000/bucket instead of http://bucket.localhost:9000
	})

	return client, nil
}
