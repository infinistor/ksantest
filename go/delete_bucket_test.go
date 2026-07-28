package s3tests

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

func TestBucketDeleteNotExist(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	_, err := s.client.DeleteBucket(context.Background(), &s3.DeleteBucketInput{Bucket: aws.String("missing-" + uniqueBucketSuffix(t))})
	assertS3Error(t, err, 404, "NoSuchBucket")
}
func TestBucketDeleteNonempty(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t)
	put(t, s, bucket, "foo", "foo", nil)
	_, err := s.client.DeleteBucket(context.Background(), &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
	assertS3Error(t, err, 409, "BucketNotEmpty")
}
func TestBucketCreateDelete(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t)
	if _, err := s.client.DeleteBucket(context.Background(), &s3.DeleteBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("first DeleteBucket: %v", err)
	}
	_, err := s.client.DeleteBucket(context.Background(), &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
	assertS3Error(t, err, 404, "NoSuchBucket")
}

func assertS3Error(t *testing.T, err error, status int, code string) {
	t.Helper()
	assertAPIErrorCode(t, err, code)
	var responseErr *smithyhttp.ResponseError
	if !errors.As(err, &responseErr) {
		t.Fatalf("error type = %T, want HTTP response error: %v", err, err)
	}
	if got := responseErr.HTTPStatusCode(); got != status {
		t.Fatalf("HTTP status = %d, want %d", got, status)
	}
}
