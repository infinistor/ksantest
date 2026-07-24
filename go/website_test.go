package s3tests

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func TestWebsite(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		run  func(*testing.T)
	}{
		// 버킷의 Website 설정 조회 확인
		{"test_website_get_buckets", func(t *testing.T) {
			s := newSuite(t)
			bucket := s.bucket(t)
			_, err := s.client.GetBucketWebsite(context.Background(), &s3.GetBucketWebsiteInput{Bucket: aws.String(bucket)})
			assertS3Error(t, err, 404, "NoSuchWebsiteConfiguration")
		}},
		// 버킷의 Website 설정이 가능한지 확인
		{"test_website_put_buckets", func(t *testing.T) {
			s := newSuite(t)
			bucket := s.bucket(t)
			want := websiteConfiguration()
			putWebsite(t, s, bucket, want)
			out, err := s.client.GetBucketWebsite(context.Background(), &s3.GetBucketWebsiteInput{Bucket: aws.String(bucket)})
			if err != nil {
				t.Fatalf("GetBucketWebsite: %v", err)
			}
			if out.ErrorDocument == nil || aws.ToString(out.ErrorDocument.Key) != aws.ToString(want.ErrorDocument.Key) || out.IndexDocument == nil || aws.ToString(out.IndexDocument.Suffix) != aws.ToString(want.IndexDocument.Suffix) {
				t.Fatalf("website configuration = %#v, want %#v", out, want)
			}
		}},
		// 버킷의 Website 설정이 삭제가능한지 확인
		{"test_website_delete_buckets", func(t *testing.T) {
			s := newSuite(t)
			bucket := s.bucket(t)
			putWebsite(t, s, bucket, websiteConfiguration())
			if _, err := s.client.DeleteBucketWebsite(context.Background(), &s3.DeleteBucketWebsiteInput{Bucket: aws.String(bucket)}); err != nil {
				t.Fatalf("DeleteBucketWebsite: %v", err)
			}
			_, err := s.client.GetBucketWebsite(context.Background(), &s3.GetBucketWebsiteInput{Bucket: aws.String(bucket)})
			assertS3Error(t, err, 404, "NoSuchWebsiteConfiguration")
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, tc.run)
	}
}

func websiteConfiguration() *types.WebsiteConfiguration {
	return &types.WebsiteConfiguration{ErrorDocument: &types.ErrorDocument{Key: aws.String("HttpStatus.SC_BAD_REQUEST")}, IndexDocument: &types.IndexDocument{Suffix: aws.String("a")}}
}

func putWebsite(t *testing.T, s *suite, bucket string, configuration *types.WebsiteConfiguration) {
	t.Helper()
	_, err := s.client.PutBucketWebsite(context.Background(), &s3.PutBucketWebsiteInput{Bucket: aws.String(bucket), WebsiteConfiguration: configuration})
	if err != nil {
		t.Fatalf("PutBucketWebsite: %v", err)
	}
}
