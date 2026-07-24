package s3tests

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func TestLogging(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		run  func(*testing.T)
	}{
		// 버킷에 로깅 설정 조회 가능한지 확인
		{"test_logging_get", testLoggingGet},
		// 버킷에 로깅 설정 가능한지 확인
		{"test_logging_set", func(t *testing.T) { testLoggingConfigure(t, "", false, false, false) }},
		// 버킷에 설정한 로깅 정보 조회가 가능한지 확인
		{"test_logging_set_get", func(t *testing.T) { testLoggingConfigure(t, "", true, false, false) }},
		// 버킷의 로깅에 Prefix가 설정되는지 확인
		{"test_logging_prefix", func(t *testing.T) { testLoggingConfigure(t, "logs/", true, false, false) }},
		// 버저닝 설정된 버킷의 로깅이 설정되는지 확인
		{"test_logging_versioning", func(t *testing.T) { testLoggingConfigure(t, "logs/", true, true, false) }},
		// SSE-s3설정된 버킷의 로깅이 설정되는지 확인
		{"test_logging_encryption", func(t *testing.T) { testLoggingConfigure(t, "logs/", true, false, true) }},
		// 존재하지 않는 버킷에 로깅 설정 실패 확인
		{"test_logging_bucket_not_found", testLoggingSourceMissing},
		// 타깃 버킷이 존재하지 않을때 로깅 설정 실패 확인
		{"test_logging_target_bucket_not_found", testLoggingTargetMissing},
	}
	for _, tc := range tests {
		t.Run(tc.name, tc.run)
	}
}

func testLoggingGet(t *testing.T) {
	s := newSuite(t)
	out, err := s.client.GetBucketLogging(context.Background(), &s3.GetBucketLoggingInput{Bucket: aws.String(s.bucket(t))})
	if err != nil || out.LoggingEnabled != nil {
		t.Fatalf("LoggingEnabled=%#v err=%v", out.LoggingEnabled, err)
	}
}

func testLoggingConfigure(t *testing.T, prefix string, verify, versioning, encryption bool) {
	t.Helper()
	s := newSuite(t)
	source, target := s.bucket(t), s.bucket(t)
	if versioning {
		enableVersioning(t, s, source)
	}
	if encryption {
		putAESBucketEncryption(t, s.client, source)
		getAndAssertAESBucketEncryption(t, s.client, source)
	}
	putLogging(t, s, source, target, prefix)
	if !verify {
		return
	}
	out, err := s.client.GetBucketLogging(context.Background(), &s3.GetBucketLoggingInput{Bucket: aws.String(source)})
	if err != nil || out.LoggingEnabled == nil || aws.ToString(out.LoggingEnabled.TargetBucket) != target || aws.ToString(out.LoggingEnabled.TargetPrefix) != prefix {
		t.Fatalf("LoggingEnabled=%#v err=%v", out.LoggingEnabled, err)
	}
}

func testLoggingSourceMissing(t *testing.T) {
	s := newSuite(t)
	source, target := "missing-source-"+uniqueBucketSuffix(t), "missing-target-"+uniqueBucketSuffix(t)
	_, err := putLoggingError(s, source, target, "logs/")
	assertS3Error(t, err, 404, "NoSuchBucket")
}

func testLoggingTargetMissing(t *testing.T) {
	s := newSuite(t)
	source := s.bucket(t)
	target := "missing-target-" + uniqueBucketSuffix(t)
	_, err := putLoggingError(s, source, target, "logs/")
	assertS3Error(t, err, 400, "InvalidTargetBucketForLogging")
}

func putLogging(t *testing.T, s *suite, source, target, prefix string) {
	t.Helper()
	if _, err := putLoggingError(s, source, target, prefix); err != nil {
		t.Fatal(err)
	}
}

func putLoggingError(s *suite, source, target, prefix string) (*s3.PutBucketLoggingOutput, error) {
	return s.client.PutBucketLogging(context.Background(), &s3.PutBucketLoggingInput{Bucket: aws.String(source), BucketLoggingStatus: &types.BucketLoggingStatus{LoggingEnabled: &types.LoggingEnabled{TargetBucket: aws.String(target), TargetPrefix: aws.String(prefix)}}})
}
