package s3tests

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

func TestDeleteBucket(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		run  func(*testing.T)
	}{
		// 존재하지 않는 버킷을 삭제하려 했을 경우 실패 확인
		{"test_bucket_delete_not_exist", func(t *testing.T) {
			s := newSuite(t)
			_, err := s.client.DeleteBucket(context.Background(), &s3.DeleteBucketInput{Bucket: aws.String("missing-" + uniqueBucketSuffix(t))})
			assertS3Error(t, err, 404, "NoSuchBucket")
		}},
		// 내용이 비어있지 않은 버킷을 삭제하려 했을 경우 실패 확인
		{"test_bucket_delete_nonempty", func(t *testing.T) {
			s := newSuite(t)
			bucket := s.bucket(t)
			put(t, s, bucket, "foo", "foo", nil)
			_, err := s.client.DeleteBucket(context.Background(), &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
			assertS3Error(t, err, 409, "BucketNotEmpty")
		}},
		// 이미 삭제된 버킷을 다시 삭제 시도할 경우 실패 확인
		{"test_bucket_create_delete", func(t *testing.T) {
			s := newSuite(t)
			bucket := s.bucket(t)
			if _, err := s.client.DeleteBucket(context.Background(), &s3.DeleteBucketInput{Bucket: aws.String(bucket)}); err != nil {
				t.Fatalf("first DeleteBucket: %v", err)
			}
			_, err := s.client.DeleteBucket(context.Background(), &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
			assertS3Error(t, err, 404, "NoSuchBucket")
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, tc.run)
	}
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
