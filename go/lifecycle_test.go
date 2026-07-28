package s3tests

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func TestLifecycleSet(t *testing.T) {
	t.Parallel()

	testLifecycleSet(t)
}
func TestLifecycleGet(t *testing.T) {
	t.Parallel()

	testLifecycleGet(t)
}
func TestLifecycleGetNoId(t *testing.T) {
	t.Parallel()

	testLifecycleGetNoID(t)
}
func TestLifecycleExpirationVersioningEnabled(t *testing.T) {
	t.Parallel()

	testLifecycleVersioningEnabled(t)
}
func TestLifecycleIdTooLong(t *testing.T) {
	t.Parallel()

	testLifecycleIDTooLong(t)
}
func TestLifecycleSameId(t *testing.T) {
	t.Parallel()

	testLifecycleSameID(t)
}
func TestLifecycleInvalidStatus(t *testing.T) {
	t.Parallel()

	testLifecycleInvalidStatus(t)
}
func TestLifecycleSetDate(t *testing.T) {
	t.Parallel()

	testLifecycleSetDate(t)
}
func TestLifecycleSetInvalidDate(t *testing.T) {
	t.Parallel()

	testLifecycleInvalidDate(t)
}
func TestLifecycleSetNoncurrent(t *testing.T) {
	t.Parallel()

	testLifecycleSetNoncurrent(t)
}
func TestLifecycleNoncurrentExpiration(t *testing.T) {
	t.Parallel()

	testLifecycleNoncurrentExpiration(t)
}
func TestLifecycleSetDeleteMarker(t *testing.T) {
	t.Parallel()

	testLifecycleSetDeleteMarker(t)
}
func TestLifecycleSetFilter(t *testing.T) {
	t.Parallel()

	testLifecycleSetFilter(t)
}
func TestLifecycleSetEmptyFilter(t *testing.T) {
	t.Parallel()

	testLifecycleSetEmptyFilter(t)
}
func TestLifecycleDeleteMarkerExpiration(t *testing.T) {
	t.Parallel()

	testLifecycleDeleteMarkerExpiration(t)
}
func TestLifecycleSetMultipart(t *testing.T) {
	t.Parallel()

	testLifecycleSetMultipart(t)
}
func TestLifecycleMultipartExpiration(t *testing.T) {
	t.Parallel()

	testLifecycleMultipartExpiration(t)
}
func TestLifecycleDelete(t *testing.T) {
	t.Parallel()

	testLifecycleDelete(t)
}
func TestLifecycleSetExpirationZero(t *testing.T) {
	t.Parallel()

	testLifecycleExpirationZero(t)
}
func TestLifecycleSetExpiration(t *testing.T) {
	t.Parallel()

	testLifecycleExpirationHeaders(t)
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

func testLifecycleSet(t *testing.T) {
	s := newSuite(t)
	putLifecycle(t, s, s.bucket(t), []types.LifecycleRule{lifecycleRule("rule1", "test1/", 1, types.ExpirationStatusEnabled), lifecycleRule("rule2", "test2/", 2, types.ExpirationStatusDisabled)})
}

func testLifecycleGet(t *testing.T) {
	s := newSuite(t)
	bucket := s.bucket(t)
	want := []types.LifecycleRule{lifecycleRule("rule1", "test1/", 31, types.ExpirationStatusEnabled), lifecycleRule("rule2", "test2/", 120, types.ExpirationStatusEnabled)}
	putLifecycle(t, s, bucket, want)
	got := getLifecycle(t, s, bucket)
	assertLifecycleRules(t, got, want, true)
}

func testLifecycleGetNoID(t *testing.T) {
	s := newSuite(t)
	bucket := s.bucket(t)
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

func testLifecycleVersioningEnabled(t *testing.T) {
	s := newSuite(t)
	bucket := s.bucket(t)
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

func testLifecycleIDTooLong(t *testing.T) {
	s := newSuite(t)
	bucket := s.bucket(t)
	rule := lifecycleRule(strings.Repeat("a", 256), "test1/", 2, types.ExpirationStatusEnabled)
	_, err := putLifecycleError(s, bucket, []types.LifecycleRule{rule})
	assertS3Error(t, err, 400, "InvalidArgument")
}

func testLifecycleSameID(t *testing.T) {
	s := newSuite(t)
	bucket := s.bucket(t)
	_, err := putLifecycleError(s, bucket, []types.LifecycleRule{lifecycleRule("rule1", "test1/", 1, types.ExpirationStatusEnabled), lifecycleRule("rule1", "test2/", 2, types.ExpirationStatusDisabled)})
	assertS3Error(t, err, 400, "InvalidArgument")
}

func testLifecycleInvalidStatus(t *testing.T) {
	s := newSuite(t)
	bucket := s.bucket(t)
	_, err := putLifecycleError(s, bucket, []types.LifecycleRule{lifecycleRule("rule1", "test1/", 2, types.ExpirationStatus("invalid"))})
	assertS3Error(t, err, 400, "MalformedXML")
}

func testLifecycleSetDate(t *testing.T) {
	s := newSuite(t)
	bucket := s.bucket(t)
	date := time.Date(2099, 11, 10, 0, 0, 0, 0, time.UTC)
	rule := lifecycleRule("rule1", "test1/", 1, types.ExpirationStatusEnabled)
	rule.Expiration = &types.LifecycleExpiration{Date: &date}
	putLifecycle(t, s, bucket, []types.LifecycleRule{rule})
}

func testLifecycleInvalidDate(t *testing.T) {
	s := newSuite(t)
	bucket := s.bucket(t)
	date := time.Date(2099, 11, 10, 12, 0, 0, 0, time.UTC)
	rule := lifecycleRule("rule1", "test1/", 1, types.ExpirationStatusEnabled)
	rule.Expiration = &types.LifecycleExpiration{Date: &date}
	_, err := putLifecycleError(s, bucket, []types.LifecycleRule{rule})
	assertS3Error(t, err, 400, "InvalidArgument")
}

func noncurrentRule(id, prefix string, days int32) types.LifecycleRule {
	return types.LifecycleRule{ID: aws.String(id), Filter: &types.LifecycleRuleFilter{Prefix: aws.String(prefix)}, NoncurrentVersionExpiration: &types.NoncurrentVersionExpiration{NoncurrentDays: aws.Int32(days)}, Status: types.ExpirationStatusEnabled}
}

func testLifecycleSetNoncurrent(t *testing.T) {
	s := newSuite(t)
	bucket := s.bucket(t)
	put(t, s, bucket, "past/foo", "past", nil)
	put(t, s, bucket, "future/bar", "future", nil)
	putLifecycle(t, s, bucket, []types.LifecycleRule{noncurrentRule("rule1", "past/", 2), noncurrentRule("rule2", "future/", 3)})
}

func testLifecycleNoncurrentExpiration(t *testing.T) {
	s := newSuite(t)
	bucket := s.bucket(t)
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

func deleteMarkerRule(prefix string) types.LifecycleRule {
	return types.LifecycleRule{ID: aws.String("rule1"), Expiration: &types.LifecycleExpiration{ExpiredObjectDeleteMarker: aws.Bool(true)}, Filter: &types.LifecycleRuleFilter{Prefix: aws.String(prefix)}, Status: types.ExpirationStatusEnabled}
}

func testLifecycleSetDeleteMarker(t *testing.T) {
	s := newSuite(t)
	putLifecycle(t, s, s.bucket(t), []types.LifecycleRule{deleteMarkerRule("test1/")})
}

func testLifecycleSetFilter(t *testing.T) {
	s := newSuite(t)
	putLifecycle(t, s, s.bucket(t), []types.LifecycleRule{deleteMarkerRule("foo")})
}

func testLifecycleSetEmptyFilter(t *testing.T) {
	s := newSuite(t)
	putLifecycle(t, s, s.bucket(t), []types.LifecycleRule{deleteMarkerRule("")})
}

func testLifecycleDeleteMarkerExpiration(t *testing.T) {
	s := newSuite(t)
	bucket := s.bucket(t)
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

func multipartLifecycleRule(id, prefix string, days int32) types.LifecycleRule {
	return types.LifecycleRule{ID: aws.String(id), Filter: &types.LifecycleRuleFilter{Prefix: aws.String(prefix)}, Status: types.ExpirationStatusEnabled, AbortIncompleteMultipartUpload: &types.AbortIncompleteMultipartUpload{DaysAfterInitiation: aws.Int32(days)}}
}

func testLifecycleSetMultipart(t *testing.T) {
	s := newSuite(t)
	putLifecycle(t, s, s.bucket(t), []types.LifecycleRule{multipartLifecycleRule("rule1", "test1/", 2), multipartLifecycleRule("rule2", "test2/", 3)})
}

func testLifecycleMultipartExpiration(t *testing.T) {
	s := newSuite(t)
	bucket := s.bucket(t)
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

func testLifecycleDelete(t *testing.T) {
	s := newSuite(t)
	bucket := s.bucket(t)
	putLifecycle(t, s, bucket, []types.LifecycleRule{lifecycleRule("rule1", "test1/", 1, types.ExpirationStatusEnabled), lifecycleRule("rule2", "test2/", 2, types.ExpirationStatusDisabled)})
	if _, err := s.client.DeleteBucketLifecycle(context.Background(), &s3.DeleteBucketLifecycleInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatal(err)
	}
}

func testLifecycleExpirationZero(t *testing.T) {
	s := newSuite(t)
	bucket := s.bucket(t)
	_, err := putLifecycleError(s, bucket, []types.LifecycleRule{lifecycleRule("rule1", "test1/", 0, types.ExpirationStatusEnabled)})
	assertS3Error(t, err, 400, "InvalidArgument")
}

func testLifecycleExpirationHeaders(t *testing.T) {

	t.Skip("Java SDK V2에서는 expires값을 재대로 가져오지 못함")
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
