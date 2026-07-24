package s3tests

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func TestPayment(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		run  func(*testing.T)
	}{
		// 버킷 과금 설정이 가능한지 확인
		{"test_put_bucket_request_payment", func(t *testing.T) {
			t.Skip("Java wrapper intentionally disables Payment tests")
			s := newSuite(t)
			putRequestPayment(t, s, s.bucket(t), types.PayerRequester)
		}},
		// 버킷 과금 설정 조회 확인
		{"test_get_bucket_request_payment", func(t *testing.T) {
			t.Skip("Java wrapper intentionally disables Payment tests")
			s := newSuite(t)
			bucket := s.bucket(t)
			out, err := s.client.GetBucketRequestPayment(context.Background(), &s3.GetBucketRequestPaymentInput{Bucket: aws.String(bucket)})
			if err != nil {
				t.Fatalf("GetBucketRequestPayment: %v", err)
			}
			if out.Payer != types.PayerBucketOwner {
				t.Fatalf("payer = %q, want BucketOwner", out.Payer)
			}
		}},
		// 버킷 과금 설정이 올바르게 적용되는지 확인
		{"test_set_get_bucket_request_payment", func(t *testing.T) {
			t.Skip("Java wrapper intentionally disables Payment tests")
			s := newSuite(t)
			bucket := s.bucket(t)
			putRequestPayment(t, s, bucket, types.PayerRequester)
			out, err := s.client.GetBucketRequestPayment(context.Background(), &s3.GetBucketRequestPaymentInput{Bucket: aws.String(bucket)})
			if err != nil {
				t.Fatalf("GetBucketRequestPayment: %v", err)
			}
			if out.Payer != types.PayerRequester {
				t.Fatalf("payer = %q, want Requester", out.Payer)
			}
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, tc.run)
	}
}

func putRequestPayment(t *testing.T, s *suite, bucket string, payer types.Payer) {
	t.Helper()
	_, err := s.client.PutBucketRequestPayment(context.Background(), &s3.PutBucketRequestPaymentInput{Bucket: aws.String(bucket), RequestPaymentConfiguration: &types.RequestPaymentConfiguration{Payer: payer}})
	if err != nil {
		t.Fatalf("PutBucketRequestPayment: %v", err)
	}
}
