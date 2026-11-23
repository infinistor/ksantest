package s3tests

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	smithyhttp "github.com/aws/smithy-go/transport/http"
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
	bucket := s.bucket(t, 4)
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
}

// 버킷 목록 조회시 ContinuationToken를 이용한 필터링 확인
func TestListBucketsContinuationToken(t *testing.T) {
	t.Parallel()

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

// 존재하지 않는 버킷의 메타데이터 조회가 실패하는지 확인
func TestHeadBucketNotExist(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := buildBucketName(s.cfg.BucketPrefix, s.suiteID, 8)
	_, err := s.client.HeadBucket(context.Background(), &s3.HeadBucketInput{Bucket: aws.String(bucket)})
	assertHTTPStatus(t, err, 404)
}

// 버킷 소유자가 아닌 사용자의 버킷 메타데이터 조회가 실패하는지 확인
func TestHeadBucketAltUser(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	alt := s3Client(s.cfg, s.cfg.Alt)
	bucket := s.bucket(t, 9)
	_, err := alt.HeadBucket(context.Background(), &s3.HeadBucketInput{Bucket: aws.String(bucket)})
	assertHTTPStatus(t, err, 403)
}

// 버킷 메타데이터 조회시 버킷의 리전을 돌려주는지 확인
func TestHeadBucketRegion(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 10)
	out, err := s.client.HeadBucket(context.Background(), &s3.HeadBucketInput{Bucket: aws.String(bucket)})
	if err != nil {
		t.Fatalf("HeadBucket: %v", err)
	}
	region := aws.ToString(out.BucketRegion)
	if region == "" {
		t.Fatal("x-amz-bucket-region 헤더가 없습니다")
	}
	if s.cfg.Region != "" && region != s.cfg.Region {
		t.Fatalf("bucket region = %q, want %q", region, s.cfg.Region)
	}
}

// 버킷 목록 조회시 일치하는 버킷이 없는 Prefix를 주면 빈 목록을 돌려주는지 확인
func TestListBucketsPrefixNotMatch(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	s.bucket(t, 11)
	out, err := s.client.ListBuckets(context.Background(), &s3.ListBucketsInput{Prefix: aws.String("zzzz-no-such-bucket-prefix-" + s.suiteID)})
	if err != nil {
		t.Fatalf("ListBuckets: %v", err)
	}
	if len(out.Buckets) != 0 {
		t.Fatalf("buckets = %v, want empty", bucketNames(out))
	}
	if out.ContinuationToken != nil {
		t.Fatalf("continuation token = %q, want none", aws.ToString(out.ContinuationToken))
	}
}

// 버킷 목록 조회시 허용 범위를 벗어난 MaxBuckets를 주면 실패하는지 확인
func TestListBucketsMaxBucketsInvalid(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	// max-buckets 는 1 이상 10000 이하만 허용된다
	_, err := s.client.ListBuckets(context.Background(), &s3.ListBucketsInput{MaxBuckets: aws.Int32(10001)})
	assertS3Error(t, err, 400, "InvalidArgument")
}

// 버킷 목록 조회시 잘못된 ContinuationToken을 주면 실패하는지 확인
func TestListBucketsInvalidContinuationToken(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	_, err := s.client.ListBuckets(context.Background(), &s3.ListBucketsInput{ContinuationToken: aws.String("invalid-continuation-token")})
	assertS3Error(t, err, 400, "InvalidArgument")
}

// 버킷 목록을 ContinuationToken으로 끝까지 넘기면 전체 목록과 같은지 확인
func TestListBucketsPaging(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	ctx := context.Background()
	for range 5 {
		s.bucket(t, 14)
	}
	prefix := fmt.Sprintf("%s%s-14-", s.cfg.BucketPrefix, s.suiteID)
	full, err := s.client.ListBuckets(ctx, &s3.ListBucketsInput{Prefix: aws.String(prefix)})
	if err != nil {
		t.Fatalf("ListBuckets: %v", err)
	}
	fullList := bucketNames(full)
	if len(fullList) != 5 {
		t.Fatalf("buckets = %v, want 5 buckets", fullList)
	}

	// 토큰이 없어질 때까지 끝까지 넘기면 전체 목록과 순서까지 같다
	var pagedList []string
	pageCount := 0
	var token *string
	for {
		out, err := s.client.ListBuckets(ctx, &s3.ListBucketsInput{Prefix: aws.String(prefix), MaxBuckets: aws.Int32(2), ContinuationToken: token})
		if err != nil {
			t.Fatalf("ListBuckets page %d: %v", pageCount+1, err)
		}
		page := bucketNames(out)
		if len(page) > 2 {
			t.Fatalf("page %d = %v, want at most 2 buckets", pageCount+1, page)
		}
		pagedList = append(pagedList, page...)
		pageCount++
		token = out.ContinuationToken
		if aws.ToString(token) == "" {
			break
		}
	}
	if pageCount != 3 {
		t.Fatalf("page count = %d, want 3", pageCount)
	}
	if !slices.Equal(pagedList, fullList) {
		t.Fatalf("paged = %v, want %v", pagedList, fullList)
	}
	if !slices.IsSorted(pagedList) {
		t.Fatalf("paged list is not sorted: %v", pagedList)
	}
}

// 버킷 목록 조회시 버킷 정보와 소유자를 돌려주는지 확인
func TestListBucketsOwner(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 15)
	out, err := s.client.ListBuckets(context.Background(), &s3.ListBucketsInput{Prefix: aws.String(bucket)})
	if err != nil {
		t.Fatalf("ListBuckets: %v", err)
	}
	if len(out.Buckets) != 1 || aws.ToString(out.Buckets[0].Name) != bucket {
		t.Fatalf("buckets = %v, want [%s]", bucketNames(out), bucket)
	}
	if out.Buckets[0].CreationDate == nil {
		t.Fatal("CreationDate 가 없습니다")
	}
	if out.Owner == nil || aws.ToString(out.Owner.ID) != s.cfg.Main.ID {
		t.Fatalf("owner = %#v, want ID %q", out.Owner, s.cfg.Main.ID)
	}
}

// 다른 사용자의 버킷 목록에는 내 버킷이 나오지 않는지 확인
func TestListBucketsAltUser(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	alt := s3Client(s.cfg, s.cfg.Alt)
	bucket := s.bucket(t, 16)
	// 다른 사용자의 목록에는 내 버킷이 나오지 않는다
	out, err := alt.ListBuckets(context.Background(), &s3.ListBucketsInput{Prefix: aws.String(bucket)})
	if err != nil {
		t.Fatalf("Alt ListBuckets: %v", err)
	}
	if len(out.Buckets) != 0 {
		t.Fatalf("buckets = %v, want empty", bucketNames(out))
	}
}

// HEAD 요청은 응답 본문이 없어 에러 코드 대신 상태 코드만 확인한다.
func assertHTTPStatus(t *testing.T, err error, status int) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected HTTP %d error, got nil", status)
	}
	var responseErr *smithyhttp.ResponseError
	if !errors.As(err, &responseErr) {
		t.Fatalf("error type = %T, want HTTP response error: %v", err, err)
	}
	if got := responseErr.HTTPStatusCode(); got != status {
		t.Fatalf("HTTP status = %d, want %d", got, status)
	}
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
