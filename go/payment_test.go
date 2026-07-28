package s3tests

// import (
// 	"context"
// 	"testing"

// 	"github.com/aws/aws-sdk-go-v2/aws"
// 	"github.com/aws/aws-sdk-go-v2/service/s3"
// 	"github.com/aws/aws-sdk-go-v2/service/s3/types"
// )

// func TestPutBucketRequestPayment(t *testing.T) {
// 	t.Parallel()

// 	s := newSuite(t)
// 	putRequestPayment(t, s, s.bucket(t), types.PayerRequester)
// }
// func TestGetBucketRequestPayment(t *testing.T) {
// 	t.Parallel()

// 	s := newSuite(t)
// 	bucket := s.bucket(t)
// 	out, err := s.client.GetBucketRequestPayment(context.Background(), &s3.GetBucketRequestPaymentInput{Bucket: aws.String(bucket)})
// 	if err != nil {
// 		t.Fatalf("GetBucketRequestPayment: %v", err)
// 	}
// 	if out.Payer != types.PayerBucketOwner {
// 		t.Fatalf("payer = %q, want BucketOwner", out.Payer)
// 	}
// }
// func TestSetGetBucketRequestPayment(t *testing.T) {
// 	t.Parallel()

// 	s := newSuite(t)
// 	bucket := s.bucket(t)
// 	putRequestPayment(t, s, bucket, types.PayerRequester)
// 	out, err := s.client.GetBucketRequestPayment(context.Background(), &s3.GetBucketRequestPaymentInput{Bucket: aws.String(bucket)})
// 	if err != nil {
// 		t.Fatalf("GetBucketRequestPayment: %v", err)
// 	}
// 	if out.Payer != types.PayerRequester {
// 		t.Fatalf("payer = %q, want Requester", out.Payer)
// 	}
// }

// func putRequestPayment(t *testing.T, s *suite, bucket string, payer types.Payer) {
// 	t.Helper()
// 	_, err := s.client.PutBucketRequestPayment(context.Background(), &s3.PutBucketRequestPaymentInput{Bucket: aws.String(bucket), RequestPaymentConfiguration: &types.RequestPaymentConfiguration{Payer: payer}})
// 	if err != nil {
// 		t.Fatalf("PutBucketRequestPayment: %v", err)
// 	}
// }
