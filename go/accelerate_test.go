package s3tests

// import (
// 	"context"
// 	"testing"

// 	"github.com/aws/aws-sdk-go-v2/aws"
// 	"github.com/aws/aws-sdk-go-v2/service/s3"
// 	"github.com/aws/aws-sdk-go-v2/service/s3/types"
// )

// func TestPutBucketAccelerate(t *testing.T) {
// 	t.Parallel()

// 	s := newSuite(t)
// 	putAccelerate(t, s, s.bucket(t), types.BucketAccelerateStatusEnabled)
// }
// func TestGetBucketAccelerate(t *testing.T) {
// 	t.Parallel()

// 	s := newSuite(t)
// 	bucket := s.bucket(t)
// 	putAccelerate(t, s, bucket, types.BucketAccelerateStatusEnabled)
// 	out, err := s.client.GetBucketAccelerateConfiguration(context.Background(), &s3.GetBucketAccelerateConfigurationInput{Bucket: aws.String(bucket)})
// 	if err != nil {
// 		t.Fatalf("GetBucketAccelerateConfiguration: %v", err)
// 	}
// 	if out.Status != types.BucketAccelerateStatusEnabled {
// 		t.Fatalf("status = %q, want Enabled", out.Status)
// 	}
// }
// func TestChangeBucketAccelerate(t *testing.T) {
// 	t.Parallel()

// 	s := newSuite(t)
// 	bucket := s.bucket(t)
// 	putAccelerate(t, s, bucket, types.BucketAccelerateStatusEnabled)
// 	putAccelerate(t, s, bucket, types.BucketAccelerateStatusSuspended)
// 	out, err := s.client.GetBucketAccelerateConfiguration(context.Background(), &s3.GetBucketAccelerateConfigurationInput{Bucket: aws.String(bucket)})
// 	if err != nil {
// 		t.Fatalf("GetBucketAccelerateConfiguration: %v", err)
// 	}
// 	if out.Status != types.BucketAccelerateStatusSuspended {
// 		t.Fatalf("status = %q, want Suspended", out.Status)
// 	}
// }
// func TestPutBucketAccelerateInvalid(t *testing.T) {
// 	t.Parallel()

// 	s := newSuite(t)
// 	bucket := s.bucket(t)
// 	_, err := s.client.PutBucketAccelerateConfiguration(context.Background(), &s3.PutBucketAccelerateConfigurationInput{Bucket: aws.String(bucket), AccelerateConfiguration: &types.AccelerateConfiguration{Status: types.BucketAccelerateStatus("Invalid")}})
// 	assertS3Error(t, err, 400, "MalformedXML")
// }

// func putAccelerate(t *testing.T, s *suite, bucket string, status types.BucketAccelerateStatus) {
// 	t.Helper()
// 	_, err := s.client.PutBucketAccelerateConfiguration(context.Background(), &s3.PutBucketAccelerateConfigurationInput{Bucket: aws.String(bucket), AccelerateConfiguration: &types.AccelerateConfiguration{Status: status}})
// 	if err != nil {
// 		t.Fatalf("PutBucketAccelerateConfiguration: %v", err)
// 	}
// }
