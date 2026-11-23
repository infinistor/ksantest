package s3tests

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// 존재하지 않는 버킷을 삭제하려 했을 경우 실패 확인
func TestBucketDeleteNotExist(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	_, err := s.client.DeleteBucket(context.Background(), &s3.DeleteBucketInput{Bucket: aws.String("missing-" + uniqueBucketSuffix(t))})
	assertS3Error(t, err, 404, "NoSuchBucket")
}

// 내용이 비어있지 않은 버킷을 삭제하려 했을 경우 실패 확인
func TestBucketDeleteNonempty(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 2)
	put(t, s, bucket, "foo", "foo", nil)
	_, err := s.client.DeleteBucket(context.Background(), &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
	assertS3Error(t, err, 409, "BucketNotEmpty")
}

// 이미 삭제된 버킷을 다시 삭제 시도할 경우 실패 확인
func TestBucketCreateDelete(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 3)
	if _, err := s.client.DeleteBucket(context.Background(), &s3.DeleteBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("first DeleteBucket: %v", err)
	}
	_, err := s.client.DeleteBucket(context.Background(), &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
	assertS3Error(t, err, 404, "NoSuchBucket")
}

// 버전 관리 버킷에 이전 버전과 삭제 마커가 남아 있으면 삭제할 수 없는지 확인
func TestBucketDeleteVersioned(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	ctx := context.Background()
	bucket := s.bucket(t, 4)
	key := "test_bucket_delete_versioned"
	checkConfigureVersioningRetry(t, s, bucket, types.BucketVersioningStatusEnabled)
	put(t, s, bucket, key, key, nil)
	if _, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}); err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}

	// 현재 오브젝트가 없어도 이전 버전과 삭제 마커가 남아 있으면 삭제할 수 없다
	assertBucketKeyCount(t, s, bucket, 0)
	_, err := s.client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
	assertS3Error(t, err, 409, "BucketNotEmpty")

	versions, err := s.client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{Bucket: aws.String(bucket)})
	if err != nil {
		t.Fatalf("ListObjectVersions: %v", err)
	}
	targets := make([]types.ObjectIdentifier, 0, len(versions.Versions)+len(versions.DeleteMarkers))
	for _, version := range versions.Versions {
		targets = append(targets, types.ObjectIdentifier{Key: version.Key, VersionId: version.VersionId})
	}
	for _, marker := range versions.DeleteMarkers {
		targets = append(targets, types.ObjectIdentifier{Key: marker.Key, VersionId: marker.VersionId})
	}
	for _, target := range targets {
		if _, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: target.Key, VersionId: target.VersionId}); err != nil {
			t.Fatalf("DeleteObject version: %v", err)
		}
	}
	if _, err := s.client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("DeleteBucket: %v", err)
	}
}

// 완료되지 않은 멀티파트 업로드만 남아 있는 버킷은 삭제할 수 있는지 확인
func TestBucketDeleteWithMultipartUpload(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	ctx := context.Background()
	bucket := s.bucket(t, 5)
	key := "test_bucket_delete_with_multipart_upload"
	upload, err := s.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}
	if _, err := s.client.UploadPart(ctx, &s3.UploadPartInput{Bucket: aws.String(bucket), Key: aws.String(key), UploadId: upload.UploadId, PartNumber: aws.Int32(1), Body: bytes.NewReader(bytes.Repeat([]byte("a"), 1024))}); err != nil {
		t.Fatalf("UploadPart: %v", err)
	}

	// 완료되지 않은 멀티파트 업로드만 남아 있는 버킷은 삭제할 수 있다
	if _, err := s.client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("DeleteBucket: %v", err)
	}
	_, err = s.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket)})
	assertS3Error(t, err, 404, "NoSuchBucket")
}

// 버킷 소유자가 아닌 사용자는 버킷을 삭제할 수 없는지 확인
func TestBucketDeleteAltUser(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	requireAltUser(t, s)
	ctx := context.Background()
	alt := s3Client(s.cfg, s.cfg.Alt)
	bucket := s.bucket(t, 6)
	_, err := alt.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
	assertS3Error(t, err, 403, "AccessDenied")
	// 버킷은 그대로 남아 있다
	if _, err := s.client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("HeadBucket: %v", err)
	}
}

// 버킷 설정이 남아 있어도 비어 있으면 삭제할 수 있는지 확인
func TestBucketDeleteWithConfigurations(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	ctx := context.Background()
	bucket := s.bucket(t, 7)
	putBucketKeyValueTag(t, s, bucket)
	if _, err := s.client.PutBucketCors(ctx, &s3.PutBucketCorsInput{Bucket: aws.String(bucket), CORSConfiguration: &types.CORSConfiguration{CORSRules: []types.CORSRule{{AllowedMethods: []string{"GET"}, AllowedOrigins: []string{"*"}}}}}); err != nil {
		t.Fatalf("PutBucketCors: %v", err)
	}
	rule := types.LifecycleRule{ID: aws.String("rule1"), Status: types.ExpirationStatusEnabled, Filter: &types.LifecycleRuleFilter{Prefix: aws.String("")}, Expiration: &types.LifecycleExpiration{Days: aws.Int32(1)}}
	if _, err := s.client.PutBucketLifecycleConfiguration(ctx, &s3.PutBucketLifecycleConfigurationInput{Bucket: aws.String(bucket), LifecycleConfiguration: &types.BucketLifecycleConfiguration{Rules: []types.LifecycleRule{rule}}}); err != nil {
		t.Fatalf("PutBucketLifecycleConfiguration: %v", err)
	}

	// 버킷 설정이 남아 있어도 비어 있으면 삭제할 수 있다
	if _, err := s.client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("DeleteBucket: %v", err)
	}
	_, err := s.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket)})
	assertS3Error(t, err, 404, "NoSuchBucket")
}

// 삭제한 버킷을 같은 이름으로 다시 만들면 이전 오브젝트와 설정이 남아 있지 않은지 확인
func TestBucketDeleteRecreate(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	ctx := context.Background()
	bucket := s.bucket(t, 8)
	key := "test_bucket_delete_recreate"
	putBucketKeyValueTag(t, s, bucket)
	put(t, s, bucket, key, key, nil)
	if _, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}); err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}
	if _, err := s.client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("DeleteBucket: %v", err)
	}

	// 같은 이름으로 다시 만들면 이전 오브젝트와 설정이 남아 있지 않다
	// (다시 만든 버킷은 s.bucket 이 등록한 정리 함수가 같은 이름으로 지운다)
	if _, err := s.client.CreateBucket(ctx, createBucketInput(s.cfg, bucket)); err != nil {
		t.Fatalf("CreateBucket again: %v", err)
	}
	assertBucketKeyCount(t, s, bucket, 0)
	_, err := s.client.GetBucketTagging(ctx, &s3.GetBucketTaggingInput{Bucket: aws.String(bucket)})
	assertS3Error(t, err, 404, "NoSuchTagSet")
}

// 버전 관리 상태를 바꾸고, 조회 결과에 반영될 때까지 확인한다
func checkConfigureVersioningRetry(t *testing.T, s *suite, bucket string, status types.BucketVersioningStatus) {
	t.Helper()
	ctx := context.Background()
	if _, err := s.client.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{Bucket: aws.String(bucket), VersioningConfiguration: &types.VersioningConfiguration{Status: status}}); err != nil {
		t.Fatalf("PutBucketVersioning: %v", err)
	}
	var got types.BucketVersioningStatus
	for range 5 {
		out, err := s.client.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{Bucket: aws.String(bucket)})
		if err != nil {
			got = ""
			continue
		}
		got = out.Status
		if got == status {
			return
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("versioning status = %q, want %q", got, status)
}

// 버킷에 key=value 태그 하나를 설정한다
func putBucketKeyValueTag(t *testing.T, s *suite, bucket string) {
	t.Helper()
	_, err := s.client.PutBucketTagging(context.Background(), &s3.PutBucketTaggingInput{Bucket: aws.String(bucket), Tagging: &types.Tagging{TagSet: []types.Tag{{Key: aws.String("key"), Value: aws.String("value")}}}})
	if err != nil {
		t.Fatalf("PutBucketTagging: %v", err)
	}
}

// ListObjectsV2 의 KeyCount 를 확인한다
func assertBucketKeyCount(t *testing.T, s *suite, bucket string, want int32) {
	t.Helper()
	out, err := s.client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String(bucket)})
	if err != nil {
		t.Fatalf("ListObjectsV2: %v", err)
	}
	if got := aws.ToInt32(out.KeyCount); got != want {
		t.Fatalf("KeyCount = %d, want %d", got, want)
	}
}

func assertS3Error(t *testing.T, err error, status int, code string) {
	t.Helper()
	assertAPIErrorCode(t, err, code)
	var responseErr *smithyhttp.ResponseError
	if !errors.As(err, &responseErr) {
		t.Fatalf("error type = %T, want HTTP response error: %v", err, err)
	}
	if got := responseErr.HTTPStatusCode(); got != status {
		t.Fatalf("HTTP status = %d, want %d", got, status)
	}
}
