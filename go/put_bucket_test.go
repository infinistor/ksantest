package s3tests

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"ksantest/go-s3tests/internal/testconfig"
)

func TestBucketListEmpty(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t)
	out, err := s.client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String(bucket)})
	if err != nil {
		t.Fatalf("ListObjectsV2: %v", err)
	}
	if got := len(out.Contents); got != 0 {
		t.Fatalf("contents length = %d, want 0", got)
	}
}
func TestBucketCreateNamingBadStartsNonAlpha(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	_, err := s.client.CreateBucket(context.Background(), createBucketInput(s.cfg, "_go-bucket"))
	assertAPIError(t, err)
}
func TestBucketCreateNamingBadShortOne(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	_, err := s.client.CreateBucket(context.Background(), createBucketInput(s.cfg, "a"))
	assertAPIError(t, err)
}
func TestBucketCreateNamingBadShortTwo(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	_, err := s.client.CreateBucket(context.Background(), createBucketInput(s.cfg, "aa"))
	assertAPIError(t, err)
}
func TestBucketCreateNamingGoodLong64(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	_, err := s.client.CreateBucket(context.Background(), createBucketInput(s.cfg, strings.Repeat("a", 64)))
	assertAPIError(t, err)
}
func TestBucketCreateNamingBadIp(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	_, err := s.client.CreateBucket(context.Background(), createBucketInput(s.cfg, "192.168.11.123"))
	assertAPIError(t, err)
}
func TestBucketCreateNamingDnsUnderscore(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	_, err := s.client.CreateBucket(context.Background(), createBucketInput(s.cfg, "foo_bar"))
	assertAPIError(t, err)
}
func TestBucketCreateNamingDnsDashAtEnd(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	_, err := s.client.CreateBucket(context.Background(), createBucketInput(s.cfg, "foo-"))
	assertAPIError(t, err)
}
func TestBucketCreateNamingDnsDotDot(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	_, err := s.client.CreateBucket(context.Background(), createBucketInput(s.cfg, "foo..bar"))
	assertAPIError(t, err)
}
func TestBucketCreateNamingDnsDotDash(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	_, err := s.client.CreateBucket(context.Background(), createBucketInput(s.cfg, "foo.-bar"))
	assertAPIError(t, err)
}
func TestBucketCreateNamingDnsDashDot(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	_, err := s.client.CreateBucket(context.Background(), createBucketInput(s.cfg, "foo-.bar"))
	assertAPIError(t, err)
}
func TestBucketCreateNamingGoodLong60(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := bucketNameOfLength(uniqueBucketSuffix(t), 60)
	if len(bucket) > 63 {
		bucket = bucket[:63]
	}
	createAndCleanupBucket(t, s, bucket)
}
func TestBucketCreateNamingGoodLong61(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := bucketNameOfLength(uniqueBucketSuffix(t), 61)
	if len(bucket) > 63 {
		bucket = bucket[:63]
	}
	createAndCleanupBucket(t, s, bucket)
}
func TestBucketCreateNamingGoodLong62(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := bucketNameOfLength(uniqueBucketSuffix(t), 62)
	if len(bucket) > 63 {
		bucket = bucket[:63]
	}
	createAndCleanupBucket(t, s, bucket)
}
func TestBucketCreateNamingGoodLong63(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := bucketNameOfLength(uniqueBucketSuffix(t), 63)
	if len(bucket) > 63 {
		bucket = bucket[:63]
	}
	createAndCleanupBucket(t, s, bucket)
}
func TestBucketCreateNamingDnsLong(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := bucketNameOfLength(uniqueBucketSuffix(t), 63)
	if len(bucket) > 63 {
		bucket = bucket[:63]
	}
	createAndCleanupBucket(t, s, bucket)
}
func TestBucketCreateNamingGoodStartsAlpha(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	createAndCleanupBucket(t, s, "a"+s.cfg.BucketPrefix+"foo")
}
func TestBucketCreateNamingGoodStartsDigit(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	createAndCleanupBucket(t, s, "0"+s.cfg.BucketPrefix+"foo")
}
func TestBucketCreateNamingGoodContainsPeriod(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	createAndCleanupBucket(t, s, s.cfg.BucketPrefix+"aaa.111")
}
func TestBucketCreateNamingGoodContainsHyphen(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	createAndCleanupBucket(t, s, s.cfg.BucketPrefix+"aaa-111")
}
func TestBucketCreateExists(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t)
	_, err := s.client.CreateBucket(context.Background(), createBucketInput(s.cfg, bucket))
	assertAPIErrorCode(t, err, "BucketAlreadyOwnedByYou")
}
func TestBucketCreateExistsNonowner(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	if s.cfg.Alt.AccessKey == "" || s.cfg.Alt.SecretKey == "" {
		t.Skip("configure Alt User credentials in config.ini")
	}
	bucket := s.bucket(t)
	altClient := s3Client(s.cfg, s.cfg.Alt)
	_, err := altClient.CreateBucket(context.Background(), createBucketInput(s.cfg, bucket))
	assertAPIErrorCode(t, err, "BucketAlreadyExists")
}
func TestBucketRecreateNotOverriding(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t)
	want := []string{"my_key1", "my_key2"}
	for _, key := range want {
		put(t, s, bucket, key, key, nil)
	}
	_, err := s.client.CreateBucket(context.Background(), createBucketInput(s.cfg, bucket))
	assertAPIErrorCode(t, err, "BucketAlreadyOwnedByYou")

	out, err := s.client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String(bucket)})
	if err != nil {
		t.Fatalf("ListObjectsV2: %v", err)
	}
	got := make([]string, 0, len(out.Contents))
	for _, object := range out.Contents {
		got = append(got, aws.ToString(object.Key))
	}
	sort.Strings(got)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("objects after recreate = %v, want %v", got, want)
	}
}
func TestGetBucketLocation(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t)
	if _, err := s.client.GetBucketLocation(context.Background(), &s3.GetBucketLocationInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("GetBucketLocation: %v", err)
	}
}

func uniqueBucketSuffix(t *testing.T) string {
	t.Helper()
	name := strings.ToLower(strings.NewReplacer("/", "-", "_", "-").Replace(t.Name()))
	if len(name) > 24 {
		name = name[len(name)-24:]
	}
	return fmt.Sprintf("%s-%x", name, len(t.Name()))
}

func bucketNameOfLength(seed string, length int) string {
	digest := fmt.Sprintf("%x", sha256.Sum256([]byte(seed)))
	return strings.Repeat(digest, (length/len(digest))+1)[:length]
}

func createAndCleanupBucket(t *testing.T, s *suite, bucket string) {
	t.Helper()
	if _, err := s.client.CreateBucket(context.Background(), createBucketInput(s.cfg, bucket)); err != nil {
		t.Fatalf("CreateBucket(%q): %v", bucket, err)
	}
	t.Cleanup(func() {
		if s.cfg.NotDelete {
			return
		}
		_, _ = s.client.DeleteBucket(context.Background(), &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
	})
}

func assertAPIError(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("operation succeeded, want S3 API error")
	}
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("error type = %T, want smithy.APIError: %v", err, err)
	}
}

func assertAPIErrorCode(t *testing.T, err error, want string) {
	t.Helper()
	assertAPIError(t, err)
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("error type = %T, want smithy.APIError: %v", err, err)
	}
	if got := apiErr.ErrorCode(); got != want {
		t.Fatalf("error code = %q, want %q", got, want)
	}
}

func s3Client(cfg testconfig.Config, user testconfig.User) *s3.Client {
	options := s3.Options{
		Region:       cfg.Region,
		Credentials:  aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(user.AccessKey, user.SecretKey, "")),
		UsePathStyle: cfg.Endpoint() != "",
	}
	if endpoint := cfg.Endpoint(); endpoint != "" {
		options.BaseEndpoint = aws.String(endpoint)
	}
	applyCompatibleS3Options(&options)
	return s3.New(options)
}
