package s3tests

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func TestAccelerate(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		run  func(*testing.T)
	}{
		// 버킷 가속 설정이 가능한지 확인
		{"test_put_bucket_accelerate", func(t *testing.T) {
			t.Skip("Java wrapper intentionally disables Accelerate tests")
			s := newSuite(t)
			putAccelerate(t, s, s.bucket(t), types.BucketAccelerateStatusEnabled)
		}},
		// 버킷 가속 설정이 올바르게 적용되는지 확인
		{"test_get_bucket_accelerate", func(t *testing.T) {
			t.Skip("Java wrapper intentionally disables Accelerate tests")
			s := newSuite(t)
			bucket := s.bucket(t)
			putAccelerate(t, s, bucket, types.BucketAccelerateStatusEnabled)
			out, err := s.client.GetBucketAccelerateConfiguration(context.Background(), &s3.GetBucketAccelerateConfigurationInput{Bucket: aws.String(bucket)})
			if err != nil {
				t.Fatalf("GetBucketAccelerateConfiguration: %v", err)
			}
			if out.Status != types.BucketAccelerateStatusEnabled {
				t.Fatalf("status = %q, want Enabled", out.Status)
			}
		}},
		// 버킷 가속 설정이 변경되는지 확인
		{"test_change_bucket_accelerate", func(t *testing.T) {
			t.Skip("Java wrapper intentionally disables Accelerate tests")
			s := newSuite(t)
			bucket := s.bucket(t)
			putAccelerate(t, s, bucket, types.BucketAccelerateStatusEnabled)
			putAccelerate(t, s, bucket, types.BucketAccelerateStatusSuspended)
			out, err := s.client.GetBucketAccelerateConfiguration(context.Background(), &s3.GetBucketAccelerateConfigurationInput{Bucket: aws.String(bucket)})
			if err != nil {
				t.Fatalf("GetBucketAccelerateConfiguration: %v", err)
			}
			if out.Status != types.BucketAccelerateStatusSuspended {
				t.Fatalf("status = %q, want Suspended", out.Status)
			}
		}},
		// 버킷 가속 설정을 잘못 입력했을 때 에러가 발생하는지 확인
		{"test_put_bucket_accelerate_invalid", func(t *testing.T) {
			t.Skip("Java wrapper intentionally disables Accelerate tests")
			s := newSuite(t)
			bucket := s.bucket(t)
			_, err := s.client.PutBucketAccelerateConfiguration(context.Background(), &s3.PutBucketAccelerateConfigurationInput{Bucket: aws.String(bucket), AccelerateConfiguration: &types.AccelerateConfiguration{Status: types.BucketAccelerateStatus("Invalid")}})
			assertS3Error(t, err, 400, "MalformedXML")
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, tc.run)
	}
}

func putAccelerate(t *testing.T, s *suite, bucket string, status types.BucketAccelerateStatus) {
	t.Helper()
	_, err := s.client.PutBucketAccelerateConfiguration(context.Background(), &s3.PutBucketAccelerateConfigurationInput{Bucket: aws.String(bucket), AccelerateConfiguration: &types.AccelerateConfiguration{Status: status}})
	if err != nil {
		t.Fatalf("PutBucketAccelerateConfiguration: %v", err)
	}
}
