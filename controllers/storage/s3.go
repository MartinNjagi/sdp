package storage

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// S3Service wraps a configured s3.Client (which could be AWS or MinIO)
// to provide high-level storage operations for the SDP.
type S3Service struct {
	client *s3.Client
}

// NewS3Service injects the raw AWS/MinIO client into the storage service.
func NewS3Service(client *s3.Client) *S3Service {
	return &S3Service{
		client: client,
	}
}

// DownloadStream returns an io.ReadCloser for a file in MinIO/S3.
// Expects an S3 URI like "s3://my-bucket/path/to/contacts.csv"
func (s *S3Service) DownloadStream(ctx context.Context, s3URI string) (io.ReadCloser, error) {
	bucket, key, err := parseS3URI(s3URI)
	if err != nil {
		return nil, err
	}

	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("storage get object: %w", err)
	}

	return out.Body, nil
}

// UploadReport streams a generated CSV/report back to MinIO/S3.
func (s *S3Service) UploadReport(ctx context.Context, bucket, key string, body io.Reader) error {
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   body,
	})
	if err != nil {
		return fmt.Errorf("storage put object: %w", err)
	}
	return nil
}

// DownloadByKey returns an io.ReadCloser for a file using its exact bucket and key.
func (s *S3Service) DownloadByKey(ctx context.Context, bucket, key string) (io.ReadCloser, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("storage get object (bucket=%s, key=%s): %w", bucket, key, err)
	}

	return out.Body, nil
}

// parseS3URI converts "s3://bucket-name/path/to/file.csv" into bucket and key
func parseS3URI(uri string) (bucket, key string, err error) {
	if !strings.HasPrefix(uri, "s3://") {
		return "", "", fmt.Errorf("invalid s3 uri: must start with s3://")
	}
	parts := strings.SplitN(strings.TrimPrefix(uri, "s3://"), "/", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("invalid s3 uri: must contain bucket and key")
	}
	return parts[0], parts[1], nil
}
