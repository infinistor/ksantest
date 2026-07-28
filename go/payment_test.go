package s3tests

// import (
// 	"context"
// 	"testing"

// 	"github.com/aws/aws-sdk-go-v2/aws"
// 	"github.com/aws/aws-sdk-go-v2/service/s3"
// 	"github.com/aws/aws-sdk-go-v2/service/s3/types"
// )

// // 버킷 과금 설정이 가능한지 확인
// func TestPutBucketRequestPayment(t *testing.T) {
// 	t.Parallel()

// 	s := newSuite(t)
// 	putRequestPayment(t, s, s.bucket(t), types.PayerRequester)
// }
// // 버킷 과금 설정 조회 확인
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
// // 버킷 과금 설정이 올바르게 적용되는지 확인
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
