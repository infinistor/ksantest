package s3tests

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func TestLoggingGet(t *testing.T) {
	t.Parallel()

	testLoggingGet(t)
}
func TestLoggingSet(t *testing.T) {
	t.Parallel()

	testLoggingConfigure(t, "", false, false, false)
}
func TestLoggingSetGet(t *testing.T) {
	t.Parallel()

	testLoggingConfigure(t, "", true, false, false)
}
func TestLoggingPrefix(t *testing.T) {
	t.Parallel()

	testLoggingConfigure(t, "logs/", true, false, false)
}
func TestLoggingVersioning(t *testing.T) {
	t.Parallel()

	testLoggingConfigure(t, "logs/", true, true, false)
}
func TestLoggingEncryption(t *testing.T) {
	t.Parallel()

	testLoggingConfigure(t, "logs/", true, false, true)
}
func TestLoggingBucketNotFound(t *testing.T) {
	t.Parallel()

	testLoggingSourceMissing(t)
}
func TestLoggingTargetBucketNotFound(t *testing.T) {
	t.Parallel()

	testLoggingTargetMissing(t)
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
