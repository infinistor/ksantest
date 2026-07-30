package s3tests

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// 버킷에 로깅 설정 조회 가능한지 확인
func TestLoggingGet(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	out, err := s.client.GetBucketLogging(context.Background(), &s3.GetBucketLoggingInput{Bucket: aws.String(s.bucket(t))})
	if err != nil || out.LoggingEnabled != nil {
		t.Fatalf("LoggingEnabled=%#v err=%v", out.LoggingEnabled, err)
	}
}

// 버킷에 로깅 설정 가능한지 확인
func TestLoggingSet(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	source, target := s.bucket(t), s.bucket(t)
	putLogging(t, s, source, target, "")
}

// 버킷에 설정한 로깅 정보 조회가 가능한지 확인
func TestLoggingSetGet(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	source, target := s.bucket(t), s.bucket(t)
	putLogging(t, s, source, target, "")
	out, err := s.client.GetBucketLogging(context.Background(), &s3.GetBucketLoggingInput{Bucket: aws.String(source)})
	if err != nil || out.LoggingEnabled == nil || aws.ToString(out.LoggingEnabled.TargetBucket) != target || aws.ToString(out.LoggingEnabled.TargetPrefix) != "" {
		t.Fatalf("LoggingEnabled=%#v err=%v", out.LoggingEnabled, err)
	}
}

// 버킷의 로깅에 Prefix가 설정되는지 확인
func TestLoggingPrefix(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	source, target := s.bucket(t), s.bucket(t)
	putLogging(t, s, source, target, "logs/")
	out, err := s.client.GetBucketLogging(context.Background(), &s3.GetBucketLoggingInput{Bucket: aws.String(source)})
	if err != nil || out.LoggingEnabled == nil || aws.ToString(out.LoggingEnabled.TargetBucket) != target || aws.ToString(out.LoggingEnabled.TargetPrefix) != "logs/" {
		t.Fatalf("LoggingEnabled=%#v err=%v", out.LoggingEnabled, err)
	}
}

// 버저닝 설정된 버킷의 로깅이 설정되는지 확인
func TestLoggingVersioning(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	source, target := s.bucket(t), s.bucket(t)
	enableVersioning(t, s, source)
	putLogging(t, s, source, target, "logs/")
	out, err := s.client.GetBucketLogging(context.Background(), &s3.GetBucketLoggingInput{Bucket: aws.String(source)})
	if err != nil || out.LoggingEnabled == nil || aws.ToString(out.LoggingEnabled.TargetBucket) != target || aws.ToString(out.LoggingEnabled.TargetPrefix) != "logs/" {
		t.Fatalf("LoggingEnabled=%#v err=%v", out.LoggingEnabled, err)
	}
}

// SSE-s3설정된 버킷의 로깅이 설정되는지 확인
func TestLoggingEncryption(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	source, target := s.bucket(t), s.bucket(t)
	putAESBucketEncryption(t, s.client, source)
	getAndAssertAESBucketEncryption(t, s.client, source)
	putLogging(t, s, source, target, "logs/")
	out, err := s.client.GetBucketLogging(context.Background(), &s3.GetBucketLoggingInput{Bucket: aws.String(source)})
	if err != nil || out.LoggingEnabled == nil || aws.ToString(out.LoggingEnabled.TargetBucket) != target || aws.ToString(out.LoggingEnabled.TargetPrefix) != "logs/" {
		t.Fatalf("LoggingEnabled=%#v err=%v", out.LoggingEnabled, err)
	}
}

// 존재하지 않는 버킷에 로깅 설정 실패 확인
func TestLoggingBucketNotFound(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	source, target := "missing-source-"+uniqueBucketSuffix(t), "missing-target-"+uniqueBucketSuffix(t)
	_, err := putLoggingError(s, source, target, "logs/")
	assertS3Error(t, err, 404, "NoSuchBucket")
}

// 타깃 버킷이 존재하지 않을때 로깅 설정 실패 확인
func TestLoggingTargetBucketNotFound(t *testing.T) {
	t.Parallel()
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
