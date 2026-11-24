package s3tests

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// 버킷의 Lifecycle 규칙을 추가 가능한지 확인
func TestLifecycleSet(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	putLifecycle(t, s, s.bucket(t, 1), []types.LifecycleRule{lifecycleRule("rule1", "test1/", 1, types.ExpirationStatusEnabled), lifecycleRule("rule2", "test2/", 2, types.ExpirationStatusDisabled)})
}

// 버킷에 설정한 Lifecycle 규칙을 가져올 수 있는지 확인
func TestLifecycleGet(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 2)
	want := []types.LifecycleRule{lifecycleRule("rule1", "test1/", 31, types.ExpirationStatusEnabled), lifecycleRule("rule2", "test2/", 120, types.ExpirationStatusEnabled)}
	putLifecycle(t, s, bucket, want)
	got := getLifecycle(t, s, bucket)
	assertLifecycleRules(t, got, want, true)
}

// ID 없이 버킷에 Lifecycle 규칙을 설정 할 수 있는지 확인
func TestLifecycleGetNoId(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 3)
	rules := []types.LifecycleRule{lifecycleRule("", "test1/", 31, types.ExpirationStatusEnabled), lifecycleRule("", "test2/", 120, types.ExpirationStatusEnabled)}
	for index := range rules {
		rules[index].ID = nil
	}
	putLifecycle(t, s, bucket, rules)
	got := getLifecycle(t, s, bucket)
	if len(got) != 2 {
		t.Fatalf("rules=%d", len(got))
	}
	for index := range got {
		if got[index].ID == nil || aws.ToInt32(got[index].Expiration.Days) != aws.ToInt32(rules[index].Expiration.Days) || aws.ToString(got[index].Filter.Prefix) != aws.ToString(rules[index].Filter.Prefix) {
			t.Fatalf("rule[%d]=%#v", index, got[index])
		}
	}
}

// 버킷에 버저닝 설정이 되어있는 상태에서 Lifecycle 규칙을 추가 가능한지 확인
func TestLifecycleExpirationVersioningEnabled(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 4)
	enableVersioning(t, s, bucket)
	put(t, s, bucket, "test1/a", "one", nil)
	if _, err := s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String("test1/a")}); err != nil {
		t.Fatal(err)
	}
	putLifecycle(t, s, bucket, []types.LifecycleRule{lifecycleRule("rule1", "expire1/", 1, types.ExpirationStatusEnabled)})
	out, err := s.client.ListObjectVersions(context.Background(), &s3.ListObjectVersionsInput{Bucket: aws.String(bucket)})
	if err != nil || len(out.Versions) != 1 || len(out.DeleteMarkers) != 1 {
		t.Fatalf("versions=%d markers=%d err=%v", len(out.Versions), len(out.DeleteMarkers), err)
	}
}

// 버킷에 Lifecycle 규칙을 설정할때 ID의 길이가 너무 길면 실패하는지 확인
func TestLifecycleIdTooLong(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 5)
	rule := lifecycleRule(strings.Repeat("a", 256), "test1/", 2, types.ExpirationStatusEnabled)
	_, err := putLifecycleError(s, bucket, []types.LifecycleRule{rule})
	assertS3Error(t, err, 400, "InvalidArgument")
}

// 버킷에 Lifecycle 규칙을 설정할때 같은 ID로 규칙을 여러개 설정할경우 실패하는지 확인
func TestLifecycleSameId(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 6)
	_, err := putLifecycleError(s, bucket, []types.LifecycleRule{lifecycleRule("rule1", "test1/", 1, types.ExpirationStatusEnabled), lifecycleRule("rule1", "test2/", 2, types.ExpirationStatusDisabled)})
	assertS3Error(t, err, 400, "InvalidArgument")
}

// 버킷에 Lifecycle 규칙중 status를 잘못 설정할때 실패하는지 확인
func TestLifecycleInvalidStatus(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 7)
	_, err := putLifecycleError(s, bucket, []types.LifecycleRule{lifecycleRule("rule1", "test1/", 2, types.ExpirationStatus("invalid"))})
	assertS3Error(t, err, 400, "MalformedXML")
}

// 버킷의 Lifecycle규칙에 날짜를 입력가능한지 확인
func TestLifecycleSetDate(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 8)
	date := time.Date(2099, 11, 10, 0, 0, 0, 0, time.UTC)
	rule := lifecycleRule("rule1", "test1/", 1, types.ExpirationStatusEnabled)
	rule.Expiration = &types.LifecycleExpiration{Date: &date}
	putLifecycle(t, s, bucket, []types.LifecycleRule{rule})
}

// 버킷의 Lifecycle규칙에 날짜를 올바르지 않은 형식으로 입력했을때 실패 확인
func TestLifecycleSetInvalidDate(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 9)
	date := time.Date(2099, 11, 10, 12, 0, 0, 0, time.UTC)
	rule := lifecycleRule("rule1", "test1/", 1, types.ExpirationStatusEnabled)
	rule.Expiration = &types.LifecycleExpiration{Date: &date}
	_, err := putLifecycleError(s, bucket, []types.LifecycleRule{rule})
	assertS3Error(t, err, 400, "InvalidArgument")
}

// 버킷의 버저닝설정이 없는 환경에서 버전관리용 Lifecycle이 올바르게 설정되는지 확인
func TestLifecycleSetNoncurrent(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 10)
	put(t, s, bucket, "past/foo", "past", nil)
	put(t, s, bucket, "future/bar", "future", nil)
	putLifecycle(t, s, bucket, []types.LifecycleRule{noncurrentRule("rule1", "past/", 2), noncurrentRule("rule2", "future/", 3)})
}

// 버킷의 버저닝설정이 되어있는 환경에서 Lifecycle 이 올바르게 동작하는지 확인
func TestLifecycleNoncurrentExpiration(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 11)
	enableVersioning(t, s, bucket)
	for _, key := range []string{"test1/a", "test2/abc"} {
		for version := 0; version < 3; version++ {
			put(t, s, bucket, key, string(rune('a'+version)), nil)
		}
	}
	out, err := s.client.ListObjectVersions(context.Background(), &s3.ListObjectVersionsInput{Bucket: aws.String(bucket)})
	if err != nil || len(out.Versions) != 6 {
		t.Fatalf("versions=%d err=%v", len(out.Versions), err)
	}
	putLifecycle(t, s, bucket, []types.LifecycleRule{noncurrentRule("rule1", "test1/", 2)})
}

// DeleteMarker에 대한 Lifecycle 규칙을 설정 할 수 있는지 확인
func TestLifecycleSetDeleteMarker(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	putLifecycle(t, s, s.bucket(t, 12), []types.LifecycleRule{deleteMarkerRule("test1/")})
}

// Lifecycle 규칙에 필터링값을 설정 할 수 있는지 확인
func TestLifecycleSetFilter(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	putLifecycle(t, s, s.bucket(t, 13), []types.LifecycleRule{deleteMarkerRule("foo")})
}

// Lifecycle 규칙에 필터링에 비어있는 값을 설정 할 수 있는지 확인
func TestLifecycleSetEmptyFilter(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	putLifecycle(t, s, s.bucket(t, 14), []types.LifecycleRule{deleteMarkerRule("")})
}

// DeleteMarker에 대한 Lifecycle 규칙이 올바르게 동작하는지 확인
func TestLifecycleDeleteMarkerExpiration(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 15)
	enableVersioning(t, s, bucket)
	for _, key := range []string{"test1/a", "test2/abc"} {
		put(t, s, bucket, key, key, nil)
		if _, err := s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}); err != nil {
			t.Fatal(err)
		}
	}
	out, err := s.client.ListObjectVersions(context.Background(), &s3.ListObjectVersionsInput{Bucket: aws.String(bucket)})
	if err != nil || len(out.Versions) != 2 || len(out.DeleteMarkers) != 2 {
		t.Fatalf("versions=%d markers=%d err=%v", len(out.Versions), len(out.DeleteMarkers), err)
	}
	rule := deleteMarkerRule("test1/")
	rule.NoncurrentVersionExpiration = &types.NoncurrentVersionExpiration{NoncurrentDays: aws.Int32(1)}
	putLifecycle(t, s, bucket, []types.LifecycleRule{rule})
}

// AbortIncompleteMultipartUpload에 대한 Lifecycle 규칙을 설정 할 수 있는지 확인
func TestLifecycleSetMultipart(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	putLifecycle(t, s, s.bucket(t, 16), []types.LifecycleRule{multipartLifecycleRule("rule1", "test1/", 2), multipartLifecycleRule("rule2", "test2/", 3)})
}

// AbortIncompleteMultipartUpload에 대한 Lifecycle 규칙이 올바르게 동작하는지 확인
func TestLifecycleMultipartExpiration(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 17)
	for _, key := range []string{"test1/a", "test2/b"} {
		created, err := s.client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(key)})
		if err != nil {
			t.Fatal(err)
		}
		uploadID := aws.ToString(created.UploadId)
		t.Cleanup(func() {
			_, _ = s.client.AbortMultipartUpload(context.Background(), &s3.AbortMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(key), UploadId: aws.String(uploadID)})
		})
	}
	out, err := s.client.ListMultipartUploads(context.Background(), &s3.ListMultipartUploadsInput{Bucket: aws.String(bucket)})
	if err != nil || len(out.Uploads) != 2 {
		t.Fatalf("uploads=%d err=%v", len(out.Uploads), err)
	}
	putLifecycle(t, s, bucket, []types.LifecycleRule{multipartLifecycleRule("rule1", "test1/", 2)})
}

// 버킷의 Lifecycle 규칙을 삭제 가능한지 확인
func TestLifecycleDelete(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 18)
	putLifecycle(t, s, bucket, []types.LifecycleRule{lifecycleRule("rule1", "test1/", 1, types.ExpirationStatusEnabled), lifecycleRule("rule2", "test2/", 2, types.ExpirationStatusDisabled)})
	if _, err := s.client.DeleteBucketLifecycle(context.Background(), &s3.DeleteBucketLifecycleInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatal(err)
	}
}

// Lifecycle 규칙에 0일을 설정할때 실패하는지 확인
func TestLifecycleSetExpirationZero(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 19)
	_, err := putLifecycleError(s, bucket, []types.LifecycleRule{lifecycleRule("rule1", "test1/", 0, types.ExpirationStatusEnabled)})
	assertS3Error(t, err, 400, "InvalidArgument")
}

// Lifecycle 규칙을 적용한 오브젝트의 만료 시각을 HeadObject와 GetObject에서 확인
func TestLifecycleSetExpiration(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket, key := s.bucket(t, 20), "test1/a"
	putLifecycle(t, s, bucket, []types.LifecycleRule{lifecycleRule("rule1", "test1/", 1, types.ExpirationStatusEnabled)})
	put(t, s, bucket, key, "test", nil)

	head, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}
	if head.LastModified == nil {
		t.Fatal("HeadObject LastModified is nil")
	}
	headExpiration := parseLifecycleExpiration(t, aws.ToString(head.Expiration))
	expected := head.LastModified.Add(24 * time.Hour)
	if s.cfg.Endpoint() == "" {
		expected = head.LastModified.Add(48 * time.Hour).UTC().Truncate(24 * time.Hour)
	}
	if !headExpiration.Equal(expected) {
		t.Fatalf("HeadObject expiration=%s want=%s header=%q", headExpiration, expected, aws.ToString(head.Expiration))
	}

	object, err := s.client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}
	defer object.Body.Close()
	getExpiration := parseLifecycleExpiration(t, aws.ToString(object.Expiration))
	if !getExpiration.Equal(expected) {
		t.Fatalf("GetObject expiration=%s want=%s header=%q", getExpiration, expected, aws.ToString(object.Expiration))
	}
}

func parseLifecycleExpiration(t *testing.T, value string) time.Time {
	t.Helper()
	const prefix = `expiry-date="`
	start := strings.Index(value, prefix)
	if start < 0 {
		t.Fatalf("invalid lifecycle expiration header %q", value)
	}
	start += len(prefix)
	end := strings.IndexByte(value[start:], '"')
	if end < 0 {
		t.Fatalf("invalid lifecycle expiration header %q", value)
	}
	expiration, err := time.Parse(time.RFC1123, value[start:start+end])
	if err != nil {
		t.Fatalf("parse lifecycle expiration %q: %v", value, err)
	}
	return expiration
}

func lifecycleRule(id, prefix string, days int32, status types.ExpirationStatus) types.LifecycleRule {
	return types.LifecycleRule{ID: aws.String(id), Filter: &types.LifecycleRuleFilter{Prefix: aws.String(prefix)}, Expiration: &types.LifecycleExpiration{Days: aws.Int32(days)}, Status: status}
}

func putLifecycle(t *testing.T, s *suite, bucket string, rules []types.LifecycleRule) {
	t.Helper()
	_, err := s.client.PutBucketLifecycleConfiguration(context.Background(), &s3.PutBucketLifecycleConfigurationInput{Bucket: aws.String(bucket), LifecycleConfiguration: &types.BucketLifecycleConfiguration{Rules: rules}})
	if err != nil {
		t.Fatal(err)
	}
}

func noncurrentRule(id, prefix string, days int32) types.LifecycleRule {
	return types.LifecycleRule{ID: aws.String(id), Filter: &types.LifecycleRuleFilter{Prefix: aws.String(prefix)}, NoncurrentVersionExpiration: &types.NoncurrentVersionExpiration{NoncurrentDays: aws.Int32(days)}, Status: types.ExpirationStatusEnabled}
}

func deleteMarkerRule(prefix string) types.LifecycleRule {
	return types.LifecycleRule{ID: aws.String("rule1"), Expiration: &types.LifecycleExpiration{ExpiredObjectDeleteMarker: aws.Bool(true)}, Filter: &types.LifecycleRuleFilter{Prefix: aws.String(prefix)}, Status: types.ExpirationStatusEnabled}
}

func multipartLifecycleRule(id, prefix string, days int32) types.LifecycleRule {
	return types.LifecycleRule{ID: aws.String(id), Filter: &types.LifecycleRuleFilter{Prefix: aws.String(prefix)}, Status: types.ExpirationStatusEnabled, AbortIncompleteMultipartUpload: &types.AbortIncompleteMultipartUpload{DaysAfterInitiation: aws.Int32(days)}}
}

func putLifecycleError(s *suite, bucket string, rules []types.LifecycleRule) (*s3.PutBucketLifecycleConfigurationOutput, error) {
	return s.client.PutBucketLifecycleConfiguration(context.Background(), &s3.PutBucketLifecycleConfigurationInput{Bucket: aws.String(bucket), LifecycleConfiguration: &types.BucketLifecycleConfiguration{Rules: rules}})
}

func getLifecycle(t *testing.T, s *suite, bucket string) []types.LifecycleRule {
	t.Helper()
	out, err := s.client.GetBucketLifecycleConfiguration(context.Background(), &s3.GetBucketLifecycleConfigurationInput{Bucket: aws.String(bucket)})
	if err != nil {
		t.Fatal(err)
	}
	return out.Rules
}

func assertLifecycleRules(t *testing.T, got, want []types.LifecycleRule, compareID bool) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("rules=%d want=%d", len(got), len(want))
	}
	for index := range want {
		if compareID && aws.ToString(got[index].ID) != aws.ToString(want[index].ID) || got[index].Status != want[index].Status || aws.ToString(got[index].Filter.Prefix) != aws.ToString(want[index].Filter.Prefix) || aws.ToInt32(got[index].Expiration.Days) != aws.ToInt32(want[index].Expiration.Days) {
			t.Fatalf("rule[%d]=%#v want=%#v", index, got[index], want[index])
		}
	}
}

func enableVersioning(t *testing.T, s *suite, bucket string) {
	t.Helper()
	_, err := s.client.PutBucketVersioning(context.Background(), &s3.PutBucketVersioningInput{Bucket: aws.String(bucket), VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled}})
	if err != nil {
		t.Fatal(err)
	}
}

// baseLifecycleRule은 ID rule1, 전체 대상(Prefix ""), Enabled 인 규칙을 돌려준다. 동작은 호출한 쪽에서 채운다.
func baseLifecycleRule() types.LifecycleRule {
	return types.LifecycleRule{ID: aws.String("rule1"), Status: types.ExpirationStatusEnabled, Filter: &types.LifecycleRuleFilter{Prefix: aws.String("")}}
}

// expirationRule은 baseLifecycleRule에 필터와 Days 만료를 채운 규칙을 돌려준다.
func expirationRule(filter *types.LifecycleRuleFilter, days int32) types.LifecycleRule {
	rule := baseLifecycleRule()
	rule.Filter = filter
	rule.Expiration = &types.LifecycleExpiration{Days: aws.Int32(days)}
	return rule
}

// 설정이 없는 버킷의 Lifecycle 조회는 실패한다
func TestLifecycleGetNotExist(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	_, err := s.client.GetBucketLifecycleConfiguration(context.Background(), &s3.GetBucketLifecycleConfigurationInput{Bucket: aws.String(s.bucket(t, 21))})
	assertS3Error(t, err, 404, "NoSuchLifecycleConfiguration")
}

// 설정이 없어도 삭제는 성공한다
func TestLifecycleDeleteNotExist(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	out, err := s.client.DeleteBucketLifecycle(context.Background(), &s3.DeleteBucketLifecycleInput{Bucket: aws.String(s.bucket(t, 22))})
	if err != nil {
		t.Fatal(err)
	}
	if got := responseStatus(t, out.ResultMetadata); got != 204 {
		t.Fatalf("HTTP status = %d, want 204", got)
	}
}

// 없는 버킷에 대한 Lifecycle 요청은 실패한다
func TestLifecycleBucketNotExist(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	ctx := context.Background()
	bucket := s.bucketNameOnly(23)
	rules := []types.LifecycleRule{expirationRule(&types.LifecycleRuleFilter{Prefix: aws.String("")}, 1)}
	_, err := s.client.GetBucketLifecycleConfiguration(ctx, &s3.GetBucketLifecycleConfigurationInput{Bucket: aws.String(bucket)})
	assertS3Error(t, err, 404, "NoSuchBucket")
	_, err = putLifecycleError(s, bucket, rules)
	assertS3Error(t, err, 404, "NoSuchBucket")
	_, err = s.client.DeleteBucketLifecycle(ctx, &s3.DeleteBucketLifecycleInput{Bucket: aws.String(bucket)})
	assertS3Error(t, err, 404, "NoSuchBucket")
}

// 다른 사용자의 Lifecycle 요청은 거부된다
func TestLifecycleAccessDenied(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	alt := altClientOrSkip(t, s)
	ctx := context.Background()
	bucket := s.bucket(t, 24)
	rules := []types.LifecycleRule{expirationRule(&types.LifecycleRuleFilter{Prefix: aws.String("")}, 1)}
	putLifecycle(t, s, bucket, rules)

	_, err := alt.GetBucketLifecycleConfiguration(ctx, &s3.GetBucketLifecycleConfigurationInput{Bucket: aws.String(bucket)})
	assertS3Error(t, err, 403, "AccessDenied")
	_, err = alt.PutBucketLifecycleConfiguration(ctx, &s3.PutBucketLifecycleConfigurationInput{Bucket: aws.String(bucket), LifecycleConfiguration: &types.BucketLifecycleConfiguration{Rules: rules}})
	assertS3Error(t, err, 403, "AccessDenied")
	_, err = alt.DeleteBucketLifecycle(ctx, &s3.DeleteBucketLifecycleInput{Bucket: aws.String(bucket)})
	assertS3Error(t, err, 403, "AccessDenied")
	// 설정은 그대로 남아 있다
	if got := getLifecycle(t, s, bucket); len(got) != 1 {
		t.Fatalf("rules=%d want=1", len(got))
	}
}

// 동작이 하나도 없는 규칙은 거부된다
func TestLifecycleNoAction(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	_, err := putLifecycleError(s, s.bucket(t, 25), []types.LifecycleRule{baseLifecycleRule()})
	assertS3Error(t, err, 400, "InvalidRequest")
}

// Filter 도 Prefix 도 없는 규칙은 거부된다
func TestLifecycleNoFilter(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	_, err := putLifecycleError(s, s.bucket(t, 26), []types.LifecycleRule{expirationRule(nil, 1)})
	assertS3Error(t, err, 400, "MalformedXML")
}

// 규칙이 하나도 없는 설정은 거부된다
func TestLifecycleEmptyRules(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	_, err := putLifecycleError(s, s.bucket(t, 27), []types.LifecycleRule{})
	assertS3Error(t, err, 400, "MalformedXML")
}

// 만료에 Days 와 Date 를 함께 줄 수 없다
func TestLifecycleDaysAndDate(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	rule := baseLifecycleRule()
	rule.Expiration = &types.LifecycleExpiration{Days: aws.Int32(1), Date: aws.Time(time.Date(2099, 11, 10, 0, 0, 0, 0, time.UTC))}
	_, err := putLifecycleError(s, s.bucket(t, 28), []types.LifecycleRule{rule})
	assertS3Error(t, err, 400, "MalformedXML")
}

// 만료일이 음수이면 거부된다
func TestLifecycleNegativeDays(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	_, err := putLifecycleError(s, s.bucket(t, 29), []types.LifecycleRule{expirationRule(&types.LifecycleRuleFilter{Prefix: aws.String("")}, -1)})
	assertS3Error(t, err, 400, "InvalidArgument")
}

// 전환 규칙이 그대로 저장·조회된다
func TestLifecycleTransition(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 30)
	want := []types.Transition{
		{Days: aws.Int32(30), StorageClass: types.TransitionStorageClassStandardIa},
		{Days: aws.Int32(60), StorageClass: types.TransitionStorageClassGlacier},
	}
	rule := expirationRule(&types.LifecycleRuleFilter{Prefix: aws.String("")}, 90)
	rule.Transitions = want
	putLifecycle(t, s, bucket, []types.LifecycleRule{rule})

	rules := getLifecycle(t, s, bucket)
	if len(rules) != 1 || len(rules[0].Transitions) != len(want) {
		t.Fatalf("rules=%#v", rules)
	}
	got := rules[0].Transitions
	sort.Slice(got, func(i, j int) bool { return aws.ToInt32(got[i].Days) < aws.ToInt32(got[j].Days) })
	for index := range want {
		if aws.ToInt32(got[index].Days) != aws.ToInt32(want[index].Days) || got[index].StorageClass != want[index].StorageClass {
			t.Fatalf("transition[%d]=%d/%s want=%d/%s", index, aws.ToInt32(got[index].Days), got[index].StorageClass, aws.ToInt32(want[index].Days), want[index].StorageClass)
		}
	}
	if rules[0].Expiration == nil || aws.ToInt32(rules[0].Expiration.Days) != 90 {
		t.Fatalf("expiration=%#v", rules[0].Expiration)
	}
}

// 전환할 수 없는 스토리지 클래스는 거부된다
func TestLifecycleTransitionInvalidStorageClass(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 31)
	// STANDARD 로는 전환할 수 없다
	rule := baseLifecycleRule()
	rule.Transitions = []types.Transition{{Days: aws.Int32(30), StorageClass: types.TransitionStorageClass("STANDARD")}}
	_, err := putLifecycleError(s, bucket, []types.LifecycleRule{rule})
	assertS3Error(t, err, 400, "InvalidArgument")
	// 없는 스토리지 클래스
	rule.Transitions = []types.Transition{{Days: aws.Int32(30), StorageClass: types.TransitionStorageClass("INVALID")}}
	_, err = putLifecycleError(s, bucket, []types.LifecycleRule{rule})
	assertS3Error(t, err, 400, "MalformedXML")
}

// 만료일은 전환일보다 커야 한다
func TestLifecycleExpirationBeforeTransition(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 32)
	transitions := []types.Transition{{Days: aws.Int32(30), StorageClass: types.TransitionStorageClassStandardIa}}
	rule := expirationRule(&types.LifecycleRuleFilter{Prefix: aws.String("")}, 30)
	rule.Transitions = transitions
	_, err := putLifecycleError(s, bucket, []types.LifecycleRule{rule})
	assertS3Error(t, err, 400, "InvalidArgument")

	rule = expirationRule(&types.LifecycleRuleFilter{Prefix: aws.String("")}, 31)
	rule.Transitions = transitions
	putLifecycle(t, s, bucket, []types.LifecycleRule{rule})
}

// 이전 버전 전환 규칙이 그대로 저장·조회된다
func TestLifecycleNoncurrentTransition(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 34)
	rule := baseLifecycleRule()
	rule.NoncurrentVersionTransitions = []types.NoncurrentVersionTransition{{NoncurrentDays: aws.Int32(30), StorageClass: types.TransitionStorageClassStandardIa}}
	putLifecycle(t, s, bucket, []types.LifecycleRule{rule})

	rules := getLifecycle(t, s, bucket)
	if len(rules) != 1 || len(rules[0].NoncurrentVersionTransitions) != 1 {
		t.Fatalf("rules=%#v", rules)
	}
	got := rules[0].NoncurrentVersionTransitions[0]
	if aws.ToInt32(got.NoncurrentDays) != 30 || got.StorageClass != types.TransitionStorageClassStandardIa || got.NewerNoncurrentVersions != nil {
		t.Fatalf("noncurrent transition=%#v", got)
	}
}

// 이전 버전 만료일이 0이면 거부된다
func TestLifecycleNoncurrentZeroDays(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	rule := baseLifecycleRule()
	rule.NoncurrentVersionExpiration = &types.NoncurrentVersionExpiration{NoncurrentDays: aws.Int32(0)}
	_, err := putLifecycleError(s, s.bucket(t, 35), []types.LifecycleRule{rule})
	assertS3Error(t, err, 400, "InvalidArgument")
}

// 보존할 이전 버전 수(NewerNoncurrentVersions)를 설정할 수 있다
func TestLifecycleNewerNoncurrentVersions(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 36)
	rule := baseLifecycleRule()
	rule.NoncurrentVersionExpiration = &types.NoncurrentVersionExpiration{NoncurrentDays: aws.Int32(1), NewerNoncurrentVersions: aws.Int32(5)}
	putLifecycle(t, s, bucket, []types.LifecycleRule{rule})

	rules := getLifecycle(t, s, bucket)
	if len(rules) != 1 || rules[0].NoncurrentVersionExpiration == nil {
		t.Fatalf("rules=%#v", rules)
	}
	got := rules[0].NoncurrentVersionExpiration
	if aws.ToInt32(got.NoncurrentDays) != 1 || aws.ToInt32(got.NewerNoncurrentVersions) != 5 {
		t.Fatalf("noncurrent expiration=%d/%d", aws.ToInt32(got.NoncurrentDays), aws.ToInt32(got.NewerNoncurrentVersions))
	}

	// 보존할 버전 수는 1~100 만 허용한다
	for _, count := range []int32{0, 101} {
		rule.NoncurrentVersionExpiration = &types.NoncurrentVersionExpiration{NoncurrentDays: aws.Int32(1), NewerNoncurrentVersions: aws.Int32(count)}
		_, err := putLifecycleError(s, bucket, []types.LifecycleRule{rule})
		assertS3Error(t, err, 400, "InvalidArgument")
	}
}

// ExpiredObjectDeleteMarker 는 Days 와 함께 쓸 수 없다
func TestLifecycleDeleteMarkerWithDays(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	rule := baseLifecycleRule()
	rule.Expiration = &types.LifecycleExpiration{Days: aws.Int32(1), ExpiredObjectDeleteMarker: aws.Bool(true)}
	_, err := putLifecycleError(s, s.bucket(t, 37), []types.LifecycleRule{rule})
	assertS3Error(t, err, 400, "MalformedXML")
}

// ExpiredObjectDeleteMarker 는 태그 필터와 함께 쓸 수 없다
func TestLifecycleDeleteMarkerWithTag(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	rule := baseLifecycleRule()
	rule.Filter = &types.LifecycleRuleFilter{Tag: &types.Tag{Key: aws.String("key"), Value: aws.String("value")}}
	rule.Expiration = &types.LifecycleExpiration{ExpiredObjectDeleteMarker: aws.Bool(true)}
	_, err := putLifecycleError(s, s.bucket(t, 38), []types.LifecycleRule{rule})
	assertS3Error(t, err, 400, "InvalidRequest")
}

// AbortIncompleteMultipartUpload 는 태그·크기 필터와 함께 쓸 수 없다
func TestLifecycleAbortMultipartWithFilter(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 39)
	filters := []*types.LifecycleRuleFilter{
		{Tag: &types.Tag{Key: aws.String("key"), Value: aws.String("value")}},
		{ObjectSizeGreaterThan: aws.Int64(10)},
	}
	for _, filter := range filters {
		rule := baseLifecycleRule()
		rule.Filter = filter
		rule.AbortIncompleteMultipartUpload = &types.AbortIncompleteMultipartUpload{DaysAfterInitiation: aws.Int32(1)}
		_, err := putLifecycleError(s, bucket, []types.LifecycleRule{rule})
		assertS3Error(t, err, 400, "InvalidRequest")
	}
}

// AbortIncompleteMultipartUpload 의 일수가 0이면 거부된다
func TestLifecycleAbortMultipartZeroDays(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	rule := baseLifecycleRule()
	rule.AbortIncompleteMultipartUpload = &types.AbortIncompleteMultipartUpload{DaysAfterInitiation: aws.Int32(0)}
	_, err := putLifecycleError(s, s.bucket(t, 40), []types.LifecycleRule{rule})
	assertS3Error(t, err, 400, "InvalidArgument")
}

// 태그 필터가 그대로 저장·조회된다
func TestLifecycleTagFilter(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 41)
	putLifecycle(t, s, bucket, []types.LifecycleRule{expirationRule(&types.LifecycleRuleFilter{Tag: &types.Tag{Key: aws.String("key"), Value: aws.String("value")}}, 1)})

	rules := getLifecycle(t, s, bucket)
	if len(rules) != 1 || rules[0].Filter == nil {
		t.Fatalf("rules=%#v", rules)
	}
	filter := rules[0].Filter
	if filter.Tag == nil || aws.ToString(filter.Tag.Key) != "key" || aws.ToString(filter.Tag.Value) != "value" || filter.Prefix != nil || filter.And != nil || filter.ObjectSizeGreaterThan != nil || filter.ObjectSizeLessThan != nil {
		t.Fatalf("filter=%#v", filter)
	}
}

// And 필터(접두어·태그·크기)가 그대로 저장·조회된다
func TestLifecycleAndFilter(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 42)
	want := []types.Tag{{Key: aws.String("key1"), Value: aws.String("value1")}, {Key: aws.String("key2"), Value: aws.String("value2")}}
	and := &types.LifecycleRuleAndOperator{Prefix: aws.String("test1/"), Tags: want, ObjectSizeGreaterThan: aws.Int64(10), ObjectSizeLessThan: aws.Int64(100)}
	putLifecycle(t, s, bucket, []types.LifecycleRule{expirationRule(&types.LifecycleRuleFilter{And: and}, 1)})

	rules := getLifecycle(t, s, bucket)
	if len(rules) != 1 || rules[0].Filter == nil || rules[0].Filter.And == nil {
		t.Fatalf("rules=%#v", rules)
	}
	got := rules[0].Filter.And
	if aws.ToString(got.Prefix) != "test1/" || aws.ToInt64(got.ObjectSizeGreaterThan) != 10 || aws.ToInt64(got.ObjectSizeLessThan) != 100 {
		t.Fatalf("and=%#v", got)
	}
	assertTags(t, got.Tags, want)
}

// 올바르지 않은 And 필터는 거부된다
func TestLifecycleAndFilterInvalid(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 43)
	// And 안에 조건이 하나뿐이면 거부된다
	single := &types.LifecycleRuleFilter{And: &types.LifecycleRuleAndOperator{Prefix: aws.String("test1/")}}
	_, err := putLifecycleError(s, bucket, []types.LifecycleRule{expirationRule(single, 1)})
	assertS3Error(t, err, 400, "MalformedXML")
	// And 안의 태그 키는 중복될 수 없다
	duplicated := &types.LifecycleRuleFilter{And: &types.LifecycleRuleAndOperator{Prefix: aws.String("test1/"), Tags: []types.Tag{{Key: aws.String("key"), Value: aws.String("1")}, {Key: aws.String("key"), Value: aws.String("2")}}}}
	_, err = putLifecycleError(s, bucket, []types.LifecycleRule{expirationRule(duplicated, 1)})
	assertS3Error(t, err, 400, "InvalidRequest")
}

// 크기 필터가 그대로 저장·조회된다
func TestLifecycleObjectSizeFilter(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 44)
	putLifecycle(t, s, bucket, []types.LifecycleRule{expirationRule(&types.LifecycleRuleFilter{ObjectSizeGreaterThan: aws.Int64(500)}, 1)})
	rules := getLifecycle(t, s, bucket)
	if len(rules) != 1 || rules[0].Filter == nil {
		t.Fatalf("rules=%#v", rules)
	}
	filter := rules[0].Filter
	if aws.ToInt64(filter.ObjectSizeGreaterThan) != 500 || filter.ObjectSizeLessThan != nil || filter.Prefix != nil || filter.Tag != nil || filter.And != nil {
		t.Fatalf("filter=%#v", filter)
	}

	putLifecycle(t, s, bucket, []types.LifecycleRule{expirationRule(&types.LifecycleRuleFilter{ObjectSizeLessThan: aws.Int64(500)}, 1)})
	rules = getLifecycle(t, s, bucket)
	if len(rules) != 1 || rules[0].Filter == nil {
		t.Fatalf("rules=%#v", rules)
	}
	filter = rules[0].Filter
	if aws.ToInt64(filter.ObjectSizeLessThan) != 500 || filter.ObjectSizeGreaterThan != nil || filter.Prefix != nil || filter.Tag != nil || filter.And != nil {
		t.Fatalf("filter=%#v", filter)
	}
}

// 올바르지 않은 크기 필터는 거부된다
func TestLifecycleObjectSizeFilterInvalid(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 45)
	// ObjectSizeLessThan 은 1 이상이어야 한다
	_, err := putLifecycleError(s, bucket, []types.LifecycleRule{expirationRule(&types.LifecycleRuleFilter{ObjectSizeLessThan: aws.Int64(0)}, 1)})
	assertS3Error(t, err, 400, "InvalidRequest")
	// ObjectSizeLessThan 은 ObjectSizeGreaterThan 보다 커야 한다
	size := &types.LifecycleRuleFilter{And: &types.LifecycleRuleAndOperator{ObjectSizeGreaterThan: aws.Int64(100), ObjectSizeLessThan: aws.Int64(100)}}
	_, err = putLifecycleError(s, bucket, []types.LifecycleRule{expirationRule(size, 1)})
	assertS3Error(t, err, 400, "InvalidRequest")
}

// Filter 대신 규칙 바로 아래에 Prefix 를 두는 예전 형식도 그대로 저장·조회된다
func TestLifecycleLegacyPrefix(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 46)
	rule := expirationRule(nil, 1)
	rule.Prefix = aws.String("legacy/")
	putLifecycle(t, s, bucket, []types.LifecycleRule{rule})
	rules := getLifecycle(t, s, bucket)
	if len(rules) != 1 || aws.ToString(rules[0].Prefix) != "legacy/" || rules[0].Filter != nil {
		t.Fatalf("rules=%#v", rules)
	}

	// Prefix 와 Filter 를 함께 줄 수는 없다
	rule.Filter = &types.LifecycleRuleFilter{Prefix: aws.String("test1/")}
	_, err := putLifecycleError(s, bucket, []types.LifecycleRule{rule})
	assertS3Error(t, err, 400, "MalformedXML")
}

// 규칙은 1000개까지 넣을 수 있다
func TestLifecycleMaxRules(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 47)
	makeRules := func(count int) []types.LifecycleRule {
		rules := make([]types.LifecycleRule, count)
		for index := range rules {
			rules[index] = lifecycleRule(fmt.Sprintf("rule%d", index), fmt.Sprintf("test%d/", index), 1, types.ExpirationStatusEnabled)
		}
		return rules
	}

	putLifecycle(t, s, bucket, makeRules(1000))
	if got := getLifecycle(t, s, bucket); len(got) != 1000 {
		t.Fatalf("rules=%d want=1000", len(got))
	}
	_, err := putLifecycleError(s, bucket, makeRules(1001))
	assertS3Error(t, err, 400, "MalformedXML")
}

// 만료 규칙에 걸리는 오브젝트는 x-amz-expiration 헤더로 만료 예정일과 규칙 ID 를 돌려준다
func TestLifecycleExpirationHeader(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	ctx := context.Background()
	bucket := s.bucket(t, 49)
	putLifecycle(t, s, bucket, []types.LifecycleRule{lifecycleRule("rule1", "test1/", 1, types.ExpirationStatusEnabled)})

	const ruleID = `rule-id="rule1"`
	key := "test1/TestLifecycleExpirationHeader"
	putOut := put(t, s, bucket, key, "test", nil)
	if expiration := aws.ToString(putOut.Expiration); !strings.Contains(expiration, ruleID) || !strings.Contains(expiration, "expiry-date=") {
		t.Fatalf("PutObject expiration=%q", expiration)
	}
	head, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(aws.ToString(head.Expiration), ruleID) {
		t.Fatalf("HeadObject expiration=%q", aws.ToString(head.Expiration))
	}
	object, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}
	defer object.Body.Close()
	if !strings.Contains(aws.ToString(object.Expiration), ruleID) {
		t.Fatalf("GetObject expiration=%q", aws.ToString(object.Expiration))
	}

	// 규칙에 걸리지 않는 오브젝트에는 헤더가 없다
	other := "test2/TestLifecycleExpirationHeader"
	if putOut = put(t, s, bucket, other, "test", nil); putOut.Expiration != nil {
		t.Fatalf("PutObject expiration=%q want none", aws.ToString(putOut.Expiration))
	}
	head, err = s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(other)})
	if err != nil {
		t.Fatal(err)
	}
	if head.Expiration != nil {
		t.Fatalf("HeadObject expiration=%q want none", aws.ToString(head.Expiration))
	}
}
