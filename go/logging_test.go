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
	out, err := s.client.GetBucketLogging(context.Background(), &s3.GetBucketLoggingInput{Bucket: aws.String(s.bucket(t, 1))})
	if err != nil || out.LoggingEnabled != nil {
		t.Fatalf("LoggingEnabled=%#v err=%v", out.LoggingEnabled, err)
	}
}

// 버킷에 로깅 설정 가능한지 확인
func TestLoggingSet(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	source, target := s.bucket(t, 2), s.bucket(t, 2)
	putLogging(t, s, source, target, "")
}

// 버킷에 설정한 로깅 정보 조회가 가능한지 확인
func TestLoggingSetGet(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	source, target := s.bucket(t, 3), s.bucket(t, 3)
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
	source, target := s.bucket(t, 4), s.bucket(t, 4)
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
	source, target := s.bucket(t, 5), s.bucket(t, 5)
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
	source, target := s.bucket(t, 6), s.bucket(t, 6)
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
	source := s.bucket(t, 8)
	target := "missing-target-" + uniqueBucketSuffix(t)
	_, err := putLoggingError(s, source, target, "logs/")
	assertS3Error(t, err, 400, "InvalidTargetBucketForLogging")
}

// 로깅 설정을 해제할 수 있는지 확인
func TestLoggingDisable(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	source, target := s.bucket(t, 9), s.bucket(t, 9)
	putLogging(t, s, source, target, "logs/")
	if got := getLogging(t, s, source); got == nil {
		t.Fatal("LoggingEnabled = nil, want configured")
	}

	// 빈 설정을 넣으면 로깅이 해제된다
	if err := putLoggingStatus(s.client, source, nil); err != nil {
		t.Fatal(err)
	}
	if got := getLogging(t, s, source); got != nil {
		t.Fatalf("LoggingEnabled after disable = %#v", got)
	}
}

// 로깅 설정을 덮어쓸 수 있는지 확인
func TestLoggingOverwrite(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	source, target, newTarget := s.bucket(t, 10), s.bucket(t, 10), s.bucket(t, 10)
	putLogging(t, s, source, target, "logs/")
	putLogging(t, s, source, newTarget, "other/")
	got := getLogging(t, s, source)
	if got == nil || aws.ToString(got.TargetBucket) != newTarget || aws.ToString(got.TargetPrefix) != "other/" {
		t.Fatalf("LoggingEnabled=%#v", got)
	}
}

// 로그 키 형식을 SimplePrefix 로 설정할 수 있는지 확인
func TestLoggingSimplePrefix(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	source, target := s.bucket(t, 11), s.bucket(t, 11)
	err := putLoggingStatus(s.client, source, &types.LoggingEnabled{
		TargetBucket: aws.String(target), TargetPrefix: aws.String("logs/"),
		TargetObjectKeyFormat: &types.TargetObjectKeyFormat{SimplePrefix: &types.SimplePrefix{}},
	})
	if err != nil {
		t.Fatal(err)
	}
	got := getLogging(t, s, source)
	if got == nil || got.TargetObjectKeyFormat == nil || got.TargetObjectKeyFormat.SimplePrefix == nil || got.TargetObjectKeyFormat.PartitionedPrefix != nil {
		t.Fatalf("LoggingEnabled=%#v", got)
	}
}

// 로그 키 형식을 PartitionedPrefix 로 설정할 수 있는지 확인
func TestLoggingPartitionedPrefix(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	source, target := s.bucket(t, 12), s.bucket(t, 12)
	for _, dateSource := range []types.PartitionDateSource{types.PartitionDateSourceEventTime, types.PartitionDateSourceDeliveryTime} {
		err := putLoggingStatus(s.client, source, &types.LoggingEnabled{
			TargetBucket: aws.String(target), TargetPrefix: aws.String("logs/"),
			TargetObjectKeyFormat: &types.TargetObjectKeyFormat{PartitionedPrefix: &types.PartitionedPrefix{PartitionDateSource: dateSource}},
		})
		if err != nil {
			t.Fatal(err)
		}
		got := getLogging(t, s, source)
		if got == nil || got.TargetObjectKeyFormat == nil || got.TargetObjectKeyFormat.PartitionedPrefix == nil || got.TargetObjectKeyFormat.PartitionedPrefix.PartitionDateSource != dateSource || got.TargetObjectKeyFormat.SimplePrefix != nil {
			t.Fatalf("%s: LoggingEnabled=%#v", dateSource, got)
		}
	}
}

// 소스 버킷 자신을 타깃으로 로깅 설정이 가능한지 확인
func TestLoggingSameBucket(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket := s.bucket(t, 13)
	// 소스 버킷 자신을 타깃으로 지정할 수 있다
	putLogging(t, s, bucket, bucket, "self/")
	got := getLogging(t, s, bucket)
	if got == nil || aws.ToString(got.TargetBucket) != bucket || aws.ToString(got.TargetPrefix) != "self/" {
		t.Fatalf("LoggingEnabled=%#v", got)
	}
}

// 타깃 버킷의 소유자가 다를때 로깅 설정 실패 확인
func TestLoggingTargetBucketOtherOwner(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	requireAltUser(t, s)
	altClient := s3Client(s.cfg, s.cfg.Alt)
	source := s.bucket(t, 14)
	target := buildBucketName(s.cfg.BucketPrefix, s.suiteID, 14)
	if _, err := altClient.CreateBucket(context.Background(), createBucketInput(s.cfg, target)); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	t.Cleanup(func() {
		if !s.cfg.NotDelete {
			if _, err := altClient.DeleteBucket(context.Background(), &s3.DeleteBucketInput{Bucket: aws.String(target)}); err != nil {
				t.Logf("DeleteBucket cleanup %s: %v", target, err)
			}
		}
	})
	// 소스와 타깃 버킷의 소유자가 다르면 거부된다
	_, err := putLoggingError(s, source, target, "logs/")
	assertS3Error(t, err, 400, "InvalidTargetBucketForLogging")
}

// 존재하지 않는 버킷의 로깅 설정 조회 실패 확인
func TestLoggingGetBucketNotExist(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket := buildBucketName(s.cfg.BucketPrefix, s.suiteID, 15)
	_, err := s.client.GetBucketLogging(context.Background(), &s3.GetBucketLoggingInput{Bucket: aws.String(bucket)})
	assertS3Error(t, err, 404, "NoSuchBucket")
}

// 버킷 소유자가 아닌 사용자의 로깅 설정/조회 실패 확인
func TestLoggingAltUser(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	requireAltUser(t, s)
	altClient := s3Client(s.cfg, s.cfg.Alt)
	source, target := s.bucket(t, 16), s.bucket(t, 16)
	_, err := altClient.GetBucketLogging(context.Background(), &s3.GetBucketLoggingInput{Bucket: aws.String(source)})
	assertS3Error(t, err, 403, "AccessDenied")
	err = putLoggingStatus(altClient, source, &types.LoggingEnabled{TargetBucket: aws.String(target), TargetPrefix: aws.String("logs/")})
	assertS3Error(t, err, 403, "AccessDenied")
}

// loggingEnabled 가 nil 이면 빈 설정을 보내 로깅을 해제한다.
func putLoggingStatus(client *s3.Client, source string, loggingEnabled *types.LoggingEnabled) error {
	_, err := client.PutBucketLogging(context.Background(), &s3.PutBucketLoggingInput{Bucket: aws.String(source), BucketLoggingStatus: &types.BucketLoggingStatus{LoggingEnabled: loggingEnabled}})
	return err
}

func getLogging(t *testing.T, s *suite, source string) *types.LoggingEnabled {
	t.Helper()
	out, err := s.client.GetBucketLogging(context.Background(), &s3.GetBucketLoggingInput{Bucket: aws.String(source)})
	if err != nil {
		t.Fatalf("GetBucketLogging: %v", err)
	}
	return out.LoggingEnabled
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
