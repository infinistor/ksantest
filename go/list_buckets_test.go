package s3tests

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"ksantest/go-s3tests/internal/testconfig"
)

// 여러개의 버킷 생성해서 목록 조회 확인
func TestBucketsCreateThenList(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	created := createNamedBuckets(t, s, "created", 5)
	out, err := s.client.ListBuckets(context.Background(), &s3.ListBucketsInput{})
	if err != nil {
		t.Fatalf("ListBuckets: %v", err)
	}
	listed := bucketNames(out)
	for _, name := range created {
		if !slices.Contains(listed, name) {
			t.Errorf("created bucket %q was not listed", name)
		}
	}
}

// 존재하지 않는 사용자가 버킷목록 조회시 에러 확인
func TestListBucketsInvalidAuth(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	client := s3Client(s.cfg, testconfig.User{AccessKey: "invalid-access-key", SecretKey: "invalid-secret-key"})
	_, err := client.ListBuckets(context.Background(), &s3.ListBucketsInput{})
	assertS3Error(t, err, 403, "InvalidAccessKeyId")
}

// 로그인정보를 잘못입력한 사용자가 버킷목록 조회시 에러 확인
func TestListBucketsBadAuth(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	client := s3Client(s.cfg, testconfig.User{AccessKey: s.cfg.Main.AccessKey, SecretKey: "invalid-secret-key"})
	_, err := client.ListBuckets(context.Background(), &s3.ListBucketsInput{})
	assertS3Error(t, err, 403, "SignatureDoesNotMatch")
}

// 버킷의 메타데이터를 가져올 수 있는지 확인
func TestHeadBucket(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t)
	if _, err := s.client.HeadBucket(context.Background(), &s3.HeadBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("HeadBucket: %v", err)
	}
}

// 버킷 목록 조회시 Prefix를 이용한 필터링 확인
func TestListBucketsPrefix(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	prefix := "prefix-" + uniqueBucketSuffix(t)
	created := createNamedBuckets(t, s, prefix, 1)
	createNamedBuckets(t, s, "other", 5)
	out, err := s.client.ListBuckets(context.Background(), &s3.ListBucketsInput{Prefix: aws.String(prefix)})
	if err != nil {
		t.Fatalf("ListBuckets prefix: %v", err)
	}
	if got := bucketNames(out); !slices.Equal(got, created) {
		t.Fatalf("buckets = %v, want %v", got, created)
	}
}

// 버킷 목록 조회시 MaxBuckets를 이용한 필터링 확인
func TestListBucketsMaxBuckets(t *testing.T) {
	t.Parallel()

	testBucketPages(t, false)
}

// 버킷 목록 조회시 ContinuationToken를 이용한 필터링 확인
func TestListBucketsContinuationToken(t *testing.T) {
	t.Parallel()

	testBucketPages(t, true)
}

func createNamedBuckets(t *testing.T, s *suite, prefix string, count int) []string {
	t.Helper()
	names := make([]string, 0, count)
	for i := range count {
		name := randomBucketName(fmt.Sprintf("%s-%d-", prefix, i))
		createAndCleanupBucket(t, s, name)
		names = append(names, name)
	}
	return names
}

func bucketNames(out *s3.ListBucketsOutput) []string {
	names := make([]string, 0, len(out.Buckets))
	for _, bucket := range out.Buckets {
		names = append(names, aws.ToString(bucket.Name))
	}
	return names
}

func testBucketPages(t *testing.T, secondPage bool) {
	t.Helper()
	s := newSuite(t)
	prefix := "pages-" + uniqueBucketSuffix(t)
	created := createNamedBuckets(t, s, prefix, 5)
	sort.Strings(created)
	first, err := s.client.ListBuckets(context.Background(), &s3.ListBucketsInput{Prefix: aws.String(prefix), MaxBuckets: aws.Int32(2)})
	if err != nil {
		t.Fatalf("ListBuckets first page: %v", err)
	}
	if got := bucketNames(first); !slices.Equal(got, created[:2]) {
		t.Fatalf("first page = %v, want %v", got, created[:2])
	}
	if !secondPage {
		return
	}
	if first.ContinuationToken == nil {
		t.Fatal("missing continuation token")
	}
	next, err := s.client.ListBuckets(context.Background(), &s3.ListBucketsInput{Prefix: aws.String(prefix), MaxBuckets: aws.Int32(2), ContinuationToken: first.ContinuationToken})
	if err != nil {
		t.Fatalf("ListBuckets second page: %v", err)
	}
	if got := bucketNames(next); !slices.Equal(got, created[2:4]) {
		t.Fatalf("second page = %v, want %v", got, created[2:4])
	}
}
