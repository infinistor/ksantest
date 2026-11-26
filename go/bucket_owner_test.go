package s3tests

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// 응답 코드만 확인한다. HeadBucket 등 본문이 없는 요청은 에러 코드를 알 수 없다.
func assertS3Status(t *testing.T, err error, status int) {
	t.Helper()
	if err == nil {
		t.Fatal("operation succeeded, want S3 API error")
	}
	var responseErr *smithyhttp.ResponseError
	if !errors.As(err, &responseErr) {
		t.Fatalf("error type = %T, want HTTP response error: %v", err, err)
	}
	if got := responseErr.HTTPStatusCode(); got != status {
		t.Fatalf("HTTP status = %d, want %d", got, status)
	}
}

// 올바른 소유자 아이디로 HeadBucket이 가능한지 확인하는 테스트
func TestBucketOwnerHeadBucket(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t, 1)

	if _, err := s.client.HeadBucket(context.Background(), &s3.HeadBucketInput{Bucket: aws.String(b), ExpectedBucketOwner: s.owner()}); err != nil {
		t.Fatal(err)
	}
}

// 잘못된 소유자 아이디로 HeadBucket을 실패하는지 확인하는 테스트
func TestBucketOwnerHeadBucketMismatch(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t, 2)

	_, err := s.client.HeadBucket(context.Background(), &s3.HeadBucketInput{Bucket: aws.String(b), ExpectedBucketOwner: s.wrongOwner()})
	assertS3Status(t, err, 403)
}

// 올바른 소유자 아이디로 ListObjectsV2가 가능한지 확인하는 테스트
func TestBucketOwnerListObjectsV2(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t, 3)
	key := "TestBucketOwnerListObjectsV2"
	put(t, s, b, key, key, nil)

	out, err := s.client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String(b), ExpectedBucketOwner: s.owner()})
	if err != nil {
		t.Fatal(err)
	}
	if got := aws.ToInt32(out.KeyCount); got != 1 {
		t.Fatalf("KeyCount = %d, want 1", got)
	}
}

// 잘못된 소유자 아이디로 ListObjectsV2를 실패하는지 확인하는 테스트
func TestBucketOwnerListObjectsV2Mismatch(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t, 4)
	key := "TestBucketOwnerListObjectsV2Mismatch"
	put(t, s, b, key, key, nil)

	_, err := s.client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String(b), ExpectedBucketOwner: s.wrongOwner()})
	assertS3Error(t, err, 403, "AccessDenied")
}

// 올바른 소유자 아이디로 오브젝트 업로드가 가능한지 확인하는 테스트
func TestBucketOwnerPutObject(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t, 5)
	key := "TestBucketOwnerPutObject"

	if _, err := s.client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(b), Key: aws.String(key), Body: bytes.NewReader([]byte(key)), ExpectedBucketOwner: s.owner()}); err != nil {
		t.Fatal(err)
	}
	if got := read(t, s, b, key); got != key {
		t.Fatalf("body = %q, want %q", got, key)
	}
}

// 잘못된 소유자 아이디로 오브젝트 업로드를 실패하는지 확인하는 테스트
func TestBucketOwnerPutObjectMismatch(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t, 6)
	key := "TestBucketOwnerPutObjectMismatch"

	_, err := s.client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(b), Key: aws.String(key), Body: bytes.NewReader([]byte(key)), ExpectedBucketOwner: s.wrongOwner()})
	assertS3Error(t, err, 403, "AccessDenied")
}

// 올바른 소유자 아이디로 오브젝트 다운로드가 가능한지 확인하는 테스트
func TestBucketOwnerGetObject(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t, 7)
	key := "TestBucketOwnerGetObject"
	put(t, s, b, key, key, nil)

	out, err := s.client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String(b), Key: aws.String(key), ExpectedBucketOwner: s.owner()})
	if err != nil {
		t.Fatal(err)
	}
	defer out.Body.Close()
	body, err := io.ReadAll(out.Body)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(body); got != key {
		t.Fatalf("body = %q, want %q", got, key)
	}
}

// 잘못된 소유자 아이디로 오브젝트 다운로드를 실패하는지 확인하는 테스트
func TestBucketOwnerGetObjectMismatch(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t, 8)
	key := "TestBucketOwnerGetObjectMismatch"
	put(t, s, b, key, key, nil)

	_, err := s.client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String(b), Key: aws.String(key), ExpectedBucketOwner: s.wrongOwner()})
	assertS3Error(t, err, 403, "AccessDenied")
}

// 올바른 소유자 아이디로 오브젝트 삭제가 가능한지 확인하는 테스트
func TestBucketOwnerDeleteObject(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t, 9)
	key := "TestBucketOwnerDeleteObject"
	put(t, s, b, key, key, nil)

	if _, err := s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(b), Key: aws.String(key), ExpectedBucketOwner: s.owner()}); err != nil {
		t.Fatal(err)
	}
	_, err := s.client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String(b), Key: aws.String(key)})
	assertS3Error(t, err, 404, "NoSuchKey")
}

// 잘못된 소유자 아이디로 오브젝트 삭제를 실패하는지 확인하는 테스트
func TestBucketOwnerDeleteObjectMismatch(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t, 10)
	key := "TestBucketOwnerDeleteObjectMismatch"
	put(t, s, b, key, key, nil)

	_, err := s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(b), Key: aws.String(key), ExpectedBucketOwner: s.wrongOwner()})
	assertS3Error(t, err, 403, "AccessDenied")

	if got := read(t, s, b, key); got != key {
		t.Fatalf("body = %q, want %q", got, key)
	}
}

// 올바른 소스/대상 소유자 아이디로 오브젝트 복사가 가능한지 확인하는 테스트
func TestSourceBucketOwnerCopyObject(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	sourceBucket := s.bucket(t, 11)
	targetBucket := s.bucket(t, 11)
	source, target := "TestSourceBucketOwnerCopyObjectSource", "TestSourceBucketOwnerCopyObjectTarget"
	put(t, s, sourceBucket, source, source, nil)

	if _, err := s.client.CopyObject(context.Background(), &s3.CopyObjectInput{
		Bucket: aws.String(targetBucket), Key: aws.String(target), CopySource: copySource(sourceBucket, source, ""),
		ExpectedBucketOwner:       s.owner(),
		ExpectedSourceBucketOwner: s.owner(),
	}); err != nil {
		t.Fatal(err)
	}
	assertCopied(t, s.client, targetBucket, target, source, nil)
}

// 잘못된 소스 소유자 아이디로 오브젝트 복사를 실패하는지 확인하는 테스트
func TestSourceBucketOwnerCopyObjectMismatch(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	sourceBucket := s.bucket(t, 12)
	targetBucket := s.bucket(t, 12)
	source, target := "TestSourceBucketOwnerCopyObjectMismatchSource", "TestSourceBucketOwnerCopyObjectMismatchTarget"
	put(t, s, sourceBucket, source, source, nil)

	_, err := s.client.CopyObject(context.Background(), &s3.CopyObjectInput{
		Bucket: aws.String(targetBucket), Key: aws.String(target), CopySource: copySource(sourceBucket, source, ""),
		ExpectedBucketOwner:       s.owner(),
		ExpectedSourceBucketOwner: s.wrongOwner(),
	})
	assertS3Error(t, err, 403, "AccessDenied")

	_, err = s.client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String(targetBucket), Key: aws.String(target)})
	assertS3Error(t, err, 404, "NoSuchKey")
}

// 잘못된 대상 소유자 아이디로 오브젝트 복사를 실패하는지 확인하는 테스트
func TestBucketOwnerCopyObjectMismatch(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	sourceBucket := s.bucket(t, 13)
	targetBucket := s.bucket(t, 13)
	source, target := "TestBucketOwnerCopyObjectMismatchSource", "TestBucketOwnerCopyObjectMismatchTarget"
	put(t, s, sourceBucket, source, source, nil)

	_, err := s.client.CopyObject(context.Background(), &s3.CopyObjectInput{
		Bucket: aws.String(targetBucket), Key: aws.String(target), CopySource: copySource(sourceBucket, source, ""),
		ExpectedBucketOwner:       s.wrongOwner(),
		ExpectedSourceBucketOwner: s.owner(),
	})
	assertS3Error(t, err, 403, "AccessDenied")

	_, err = s.client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String(targetBucket), Key: aws.String(target)})
	assertS3Error(t, err, 404, "NoSuchKey")
}

// 올바른 소스/대상 소유자 아이디로 파트 복사가 가능한지 확인하는 테스트
func TestSourceBucketOwnerUploadPartCopy(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	sourceBucket := s.bucket(t, 14)
	targetBucket := s.bucket(t, 14)
	source, target := "TestSourceBucketOwnerUploadPartCopySource", "TestSourceBucketOwnerUploadPartCopyTarget"
	put(t, s, sourceBucket, source, source, nil)

	created, err := s.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: aws.String(targetBucket), Key: aws.String(target)})
	if err != nil {
		t.Fatal(err)
	}

	copied, err := s.client.UploadPartCopy(ctx, &s3.UploadPartCopyInput{
		Bucket: aws.String(targetBucket), Key: aws.String(target), UploadId: created.UploadId, PartNumber: aws.Int32(1),
		CopySource: copySource(sourceBucket, source, ""), CopySourceRange: aws.String(fmt.Sprintf("bytes=0-%d", len(source)-1)),
		ExpectedBucketOwner:       s.owner(),
		ExpectedSourceBucketOwner: s.owner(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if copied.CopyPartResult == nil || aws.ToString(copied.CopyPartResult.ETag) == "" {
		t.Fatal("CopyPartResult.ETag is empty")
	}

	if _, err := s.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: aws.String(targetBucket), Key: aws.String(target), UploadId: created.UploadId}); err != nil {
		t.Fatal(err)
	}
}

// 잘못된 소스 소유자 아이디로 파트 복사를 실패하는지 확인하는 테스트
func TestSourceBucketOwnerUploadPartCopyMismatch(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	sourceBucket := s.bucket(t, 15)
	targetBucket := s.bucket(t, 15)
	source, target := "TestSourceBucketOwnerUploadPartCopyMismatchSource", "TestSourceBucketOwnerUploadPartCopyMismatchTarget"
	put(t, s, sourceBucket, source, source, nil)

	created, err := s.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: aws.String(targetBucket), Key: aws.String(target)})
	if err != nil {
		t.Fatal(err)
	}

	_, err = s.client.UploadPartCopy(ctx, &s3.UploadPartCopyInput{
		Bucket: aws.String(targetBucket), Key: aws.String(target), UploadId: created.UploadId, PartNumber: aws.Int32(1),
		CopySource: copySource(sourceBucket, source, ""), CopySourceRange: aws.String(fmt.Sprintf("bytes=0-%d", len(source)-1)),
		ExpectedBucketOwner:       s.owner(),
		ExpectedSourceBucketOwner: s.wrongOwner(),
	})
	assertS3Error(t, err, 403, "AccessDenied")

	if _, err := s.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: aws.String(targetBucket), Key: aws.String(target), UploadId: created.UploadId}); err != nil {
		t.Fatal(err)
	}
}

// 잘못된 대상 소유자 아이디로 파트 복사를 실패하는지 확인하는 테스트
func TestBucketOwnerUploadPartCopyMismatch(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	sourceBucket := s.bucket(t, 16)
	targetBucket := s.bucket(t, 16)
	source, target := "TestBucketOwnerUploadPartCopyMismatchSource", "TestBucketOwnerUploadPartCopyMismatchTarget"
	put(t, s, sourceBucket, source, source, nil)

	created, err := s.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: aws.String(targetBucket), Key: aws.String(target)})
	if err != nil {
		t.Fatal(err)
	}

	_, err = s.client.UploadPartCopy(ctx, &s3.UploadPartCopyInput{
		Bucket: aws.String(targetBucket), Key: aws.String(target), UploadId: created.UploadId, PartNumber: aws.Int32(1),
		CopySource: copySource(sourceBucket, source, ""), CopySourceRange: aws.String(fmt.Sprintf("bytes=0-%d", len(source)-1)),
		ExpectedBucketOwner:       s.wrongOwner(),
		ExpectedSourceBucketOwner: s.owner(),
	})
	assertS3Error(t, err, 403, "AccessDenied")

	if _, err := s.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: aws.String(targetBucket), Key: aws.String(target), UploadId: created.UploadId}); err != nil {
		t.Fatal(err)
	}
}

// region 그 밖의 API (AWS 실측 기준)
// x-amz-expected-bucket-owner 는 버킷·오브젝트를 다루는 거의 모든 API 가 받는다.
// 소유자가 다르면 다른 검사보다 먼저 403 AccessDenied 다.

func assertOwnerMismatch(t *testing.T, err error) {
	t.Helper()
	assertS3Error(t, err, 403, "AccessDenied")
}

// 태그 목록이 a=b 하나인지 확인한다.
func assertOwnerTagSet(t *testing.T, got []types.Tag) {
	t.Helper()
	if len(got) != 1 || aws.ToString(got[0].Key) != "a" || aws.ToString(got[0].Value) != "b" {
		t.Fatalf("TagSet = %#v, want [a=b]", got)
	}
}

// 목록 조회 API 들이 소유자 아이디를 확인하는지 테스트
func TestBucketOwnerListOperations(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	b := s.bucket(t, 17)
	key := "TestBucketOwnerListOperations"
	put(t, s, b, key, key, nil)
	if _, err := s.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: aws.String(b), Key: aws.String("multi")}); err != nil {
		t.Fatal(err)
	}
	owner, wrong := s.owner(), s.wrongOwner()

	objects, err := s.client.ListObjects(ctx, &s3.ListObjectsInput{Bucket: aws.String(b), ExpectedBucketOwner: owner})
	if err != nil {
		t.Fatal(err)
	}
	if got := len(objects.Contents); got != 1 {
		t.Fatalf("len(Contents) = %d, want 1", got)
	}
	versions, err := s.client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{Bucket: aws.String(b), ExpectedBucketOwner: owner})
	if err != nil {
		t.Fatal(err)
	}
	if got := len(versions.Versions); got != 1 {
		t.Fatalf("len(Versions) = %d, want 1", got)
	}
	uploads, err := s.client.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{Bucket: aws.String(b), ExpectedBucketOwner: owner})
	if err != nil {
		t.Fatal(err)
	}
	if got := len(uploads.Uploads); got != 1 {
		t.Fatalf("len(Uploads) = %d, want 1", got)
	}

	_, err = s.client.ListObjects(ctx, &s3.ListObjectsInput{Bucket: aws.String(b), ExpectedBucketOwner: wrong})
	assertOwnerMismatch(t, err)
	_, err = s.client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{Bucket: aws.String(b), ExpectedBucketOwner: wrong})
	assertOwnerMismatch(t, err)
	_, err = s.client.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{Bucket: aws.String(b), ExpectedBucketOwner: wrong})
	assertOwnerMismatch(t, err)
}

// 버킷 위치·ACL·버저닝 API 가 소유자 아이디를 확인하는지 테스트
func TestBucketOwnerBucketConfig(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	b := s.bucket(t, 18)
	owner, wrong := s.owner(), s.wrongOwner()
	versioning := &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled}

	if _, err := s.client.GetBucketLocation(ctx, &s3.GetBucketLocationInput{Bucket: aws.String(b), ExpectedBucketOwner: owner}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.client.GetBucketAcl(ctx, &s3.GetBucketAclInput{Bucket: aws.String(b), ExpectedBucketOwner: owner}); err != nil {
		t.Fatal(err)
	}
	_, err := s.client.GetBucketLocation(ctx, &s3.GetBucketLocationInput{Bucket: aws.String(b), ExpectedBucketOwner: wrong})
	assertOwnerMismatch(t, err)
	_, err = s.client.GetBucketAcl(ctx, &s3.GetBucketAclInput{Bucket: aws.String(b), ExpectedBucketOwner: wrong})
	assertOwnerMismatch(t, err)

	// 소유자가 다르면 설정이 바뀌지 않는다
	_, err = s.client.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{Bucket: aws.String(b), VersioningConfiguration: versioning, ExpectedBucketOwner: wrong})
	assertOwnerMismatch(t, err)
	got, err := s.client.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{Bucket: aws.String(b), ExpectedBucketOwner: owner})
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != "" {
		t.Fatalf("Status = %q, want empty", got.Status)
	}
	if _, err := s.client.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{Bucket: aws.String(b), VersioningConfiguration: versioning, ExpectedBucketOwner: owner}); err != nil {
		t.Fatal(err)
	}
	_, err = s.client.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{Bucket: aws.String(b), ExpectedBucketOwner: wrong})
	assertOwnerMismatch(t, err)
}

// 버킷 태그·CORS API 가 소유자 아이디를 확인하는지 테스트
func TestBucketOwnerBucketTaggingCors(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	b := s.bucket(t, 19)
	owner, wrong := s.owner(), s.wrongOwner()
	tagging := &types.Tagging{TagSet: []types.Tag{{Key: aws.String("a"), Value: aws.String("b")}}}
	cors := &types.CORSConfiguration{CORSRules: []types.CORSRule{{AllowedMethods: []string{"GET"}, AllowedOrigins: []string{"*"}}}}

	_, err := s.client.PutBucketTagging(ctx, &s3.PutBucketTaggingInput{Bucket: aws.String(b), Tagging: tagging, ExpectedBucketOwner: wrong})
	assertOwnerMismatch(t, err)
	_, err = s.client.PutBucketCors(ctx, &s3.PutBucketCorsInput{Bucket: aws.String(b), CORSConfiguration: cors, ExpectedBucketOwner: wrong})
	assertOwnerMismatch(t, err)
	if _, err := s.client.PutBucketTagging(ctx, &s3.PutBucketTaggingInput{Bucket: aws.String(b), Tagging: tagging, ExpectedBucketOwner: owner}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.client.PutBucketCors(ctx, &s3.PutBucketCorsInput{Bucket: aws.String(b), CORSConfiguration: cors, ExpectedBucketOwner: owner}); err != nil {
		t.Fatal(err)
	}
	tags, err := s.client.GetBucketTagging(ctx, &s3.GetBucketTaggingInput{Bucket: aws.String(b), ExpectedBucketOwner: owner})
	if err != nil {
		t.Fatal(err)
	}
	assertOwnerTagSet(t, tags.TagSet)
	gotCors, err := s.client.GetBucketCors(ctx, &s3.GetBucketCorsInput{Bucket: aws.String(b), ExpectedBucketOwner: owner})
	if err != nil {
		t.Fatal(err)
	}
	if got := len(gotCors.CORSRules); got != 1 {
		t.Fatalf("len(CORSRules) = %d, want 1", got)
	}

	_, err = s.client.GetBucketTagging(ctx, &s3.GetBucketTaggingInput{Bucket: aws.String(b), ExpectedBucketOwner: wrong})
	assertOwnerMismatch(t, err)
	_, err = s.client.GetBucketCors(ctx, &s3.GetBucketCorsInput{Bucket: aws.String(b), ExpectedBucketOwner: wrong})
	assertOwnerMismatch(t, err)
	_, err = s.client.DeleteBucketTagging(ctx, &s3.DeleteBucketTaggingInput{Bucket: aws.String(b), ExpectedBucketOwner: wrong})
	assertOwnerMismatch(t, err)
	_, err = s.client.DeleteBucketCors(ctx, &s3.DeleteBucketCorsInput{Bucket: aws.String(b), ExpectedBucketOwner: wrong})
	assertOwnerMismatch(t, err)
	// 삭제되지 않았다
	tags, err = s.client.GetBucketTagging(ctx, &s3.GetBucketTaggingInput{Bucket: aws.String(b)})
	if err != nil {
		t.Fatal(err)
	}
	assertOwnerTagSet(t, tags.TagSet)
	if _, err := s.client.DeleteBucketTagging(ctx, &s3.DeleteBucketTaggingInput{Bucket: aws.String(b), ExpectedBucketOwner: owner}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.client.DeleteBucketCors(ctx, &s3.DeleteBucketCorsInput{Bucket: aws.String(b), ExpectedBucketOwner: owner}); err != nil {
		t.Fatal(err)
	}
	_, err = s.client.GetBucketTagging(ctx, &s3.GetBucketTaggingInput{Bucket: aws.String(b)})
	assertS3Error(t, err, 404, "NoSuchTagSet")
}

// 설정이 없는 버킷에서 소유자 확인이 설정 조회보다 먼저인지 테스트
func TestBucketOwnerNotConfigured(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	b := s.bucket(t, 20)
	owner, wrong := s.owner(), s.wrongOwner()
	requests := []struct {
		request func(o *string) error
		code    string
	}{
		{func(o *string) error {
			_, err := s.client.GetBucketPolicy(ctx, &s3.GetBucketPolicyInput{Bucket: aws.String(b), ExpectedBucketOwner: o})
			return err
		}, "NoSuchBucketPolicy"},
		{func(o *string) error {
			_, err := s.client.GetBucketLifecycleConfiguration(ctx, &s3.GetBucketLifecycleConfigurationInput{Bucket: aws.String(b), ExpectedBucketOwner: o})
			return err
		}, "NoSuchLifecycleConfiguration"},
		{func(o *string) error {
			_, err := s.client.GetBucketCors(ctx, &s3.GetBucketCorsInput{Bucket: aws.String(b), ExpectedBucketOwner: o})
			return err
		}, "NoSuchCORSConfiguration"},
		{func(o *string) error {
			_, err := s.client.GetBucketWebsite(ctx, &s3.GetBucketWebsiteInput{Bucket: aws.String(b), ExpectedBucketOwner: o})
			return err
		}, "NoSuchWebsiteConfiguration"},
	}
	for _, r := range requests {
		// 소유자가 맞으면 설정 없음(404), 다르면 설정을 보기 전에 403 이다
		assertS3Error(t, r.request(owner), 404, r.code)
		assertOwnerMismatch(t, r.request(wrong))
	}
}

// 오브젝트 조회 API 들이 소유자 아이디를 확인하는지 테스트
func TestBucketOwnerObjectRead(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	b := s.bucket(t, 21)
	key := "TestBucketOwnerObjectRead"
	put(t, s, b, key, key, nil)
	owner, wrong := s.owner(), s.wrongOwner()
	attributes := []types.ObjectAttributes{types.ObjectAttributesObjectSize}

	head, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String(key), ExpectedBucketOwner: owner})
	if err != nil {
		t.Fatal(err)
	}
	if got := aws.ToInt64(head.ContentLength); got != int64(len(key)) {
		t.Fatalf("ContentLength = %d, want %d", got, len(key))
	}
	acl, err := s.client.GetObjectAcl(ctx, &s3.GetObjectAclInput{Bucket: aws.String(b), Key: aws.String(key), ExpectedBucketOwner: owner})
	if err != nil {
		t.Fatal(err)
	}
	if got := len(acl.Grants); got != 1 {
		t.Fatalf("len(Grants) = %d, want 1", got)
	}
	attrs, err := s.client.GetObjectAttributes(ctx, &s3.GetObjectAttributesInput{Bucket: aws.String(b), Key: aws.String(key), ObjectAttributes: attributes, ExpectedBucketOwner: owner})
	if err != nil {
		t.Fatal(err)
	}
	if got := aws.ToInt64(attrs.ObjectSize); got != int64(len(key)) {
		t.Fatalf("ObjectSize = %d, want %d", got, len(key))
	}

	_, err = s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String(key), ExpectedBucketOwner: wrong})
	assertS3Status(t, err, 403)
	_, err = s.client.GetObjectAcl(ctx, &s3.GetObjectAclInput{Bucket: aws.String(b), Key: aws.String(key), ExpectedBucketOwner: wrong})
	assertOwnerMismatch(t, err)
	_, err = s.client.GetObjectAttributes(ctx, &s3.GetObjectAttributesInput{Bucket: aws.String(b), Key: aws.String(key), ObjectAttributes: attributes, ExpectedBucketOwner: wrong})
	assertOwnerMismatch(t, err)
}

// 오브젝트 태그 API 가 소유자 아이디를 확인하는지 테스트
func TestBucketOwnerObjectTagging(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	b := s.bucket(t, 22)
	key := "TestBucketOwnerObjectTagging"
	put(t, s, b, key, key, nil)
	owner, wrong := s.owner(), s.wrongOwner()
	tagging := &types.Tagging{TagSet: []types.Tag{{Key: aws.String("a"), Value: aws.String("b")}}}

	_, err := s.client.PutObjectTagging(ctx, &s3.PutObjectTaggingInput{Bucket: aws.String(b), Key: aws.String(key), Tagging: tagging, ExpectedBucketOwner: wrong})
	assertOwnerMismatch(t, err)
	tags, err := s.client.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{Bucket: aws.String(b), Key: aws.String(key), ExpectedBucketOwner: owner})
	if err != nil {
		t.Fatal(err)
	}
	if len(tags.TagSet) != 0 {
		t.Fatalf("TagSet = %#v, want empty", tags.TagSet)
	}
	if _, err := s.client.PutObjectTagging(ctx, &s3.PutObjectTaggingInput{Bucket: aws.String(b), Key: aws.String(key), Tagging: tagging, ExpectedBucketOwner: owner}); err != nil {
		t.Fatal(err)
	}

	_, err = s.client.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{Bucket: aws.String(b), Key: aws.String(key), ExpectedBucketOwner: wrong})
	assertOwnerMismatch(t, err)
	_, err = s.client.DeleteObjectTagging(ctx, &s3.DeleteObjectTaggingInput{Bucket: aws.String(b), Key: aws.String(key), ExpectedBucketOwner: wrong})
	assertOwnerMismatch(t, err)
	tags, err = s.client.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{Bucket: aws.String(b), Key: aws.String(key), ExpectedBucketOwner: owner})
	if err != nil {
		t.Fatal(err)
	}
	assertOwnerTagSet(t, tags.TagSet)
	if _, err := s.client.DeleteObjectTagging(ctx, &s3.DeleteObjectTaggingInput{Bucket: aws.String(b), Key: aws.String(key), ExpectedBucketOwner: owner}); err != nil {
		t.Fatal(err)
	}
	tags, err = s.client.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{Bucket: aws.String(b), Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}
	if len(tags.TagSet) != 0 {
		t.Fatalf("TagSet = %#v, want empty", tags.TagSet)
	}
}

// 없는 키 조회에서 소유자 확인이 키 조회보다 먼저인지 테스트
func TestBucketOwnerKeyNotExist(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	b := s.bucket(t, 23)
	key := "TestBucketOwnerKeyNotExist"

	// 소유자가 맞으면 없는 키(404), 다르면 키를 보기 전에 403 이다
	_, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(b), Key: aws.String(key), ExpectedBucketOwner: s.owner()})
	assertS3Error(t, err, 404, "NoSuchKey")
	_, err = s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(b), Key: aws.String(key), ExpectedBucketOwner: s.wrongOwner()})
	assertOwnerMismatch(t, err)
}

// DeleteObjects 가 소유자 아이디를 확인하는지 테스트
func TestBucketOwnerDeleteObjects(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	b := s.bucket(t, 24)
	put(t, s, b, "a", "a", nil)
	put(t, s, b, "b", "b", nil)
	deleteInput := &types.Delete{Objects: []types.ObjectIdentifier{{Key: aws.String("a")}, {Key: aws.String("b")}}}

	// 요청 전체가 거부되고 하나도 지워지지 않는다
	_, err := s.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: aws.String(b), Delete: deleteInput, ExpectedBucketOwner: s.wrongOwner()})
	assertOwnerMismatch(t, err)
	assertListV2KeyCount(t, s.client, &s3.ListObjectsV2Input{Bucket: aws.String(b)}, 2)

	out, err := s.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: aws.String(b), Delete: deleteInput, ExpectedBucketOwner: s.owner()})
	if err != nil {
		t.Fatal(err)
	}
	if got := len(out.Deleted); got != 2 {
		t.Fatalf("len(Deleted) = %d, want 2", got)
	}
	assertListV2KeyCount(t, s.client, &s3.ListObjectsV2Input{Bucket: aws.String(b)}, 0)
}

// 멀티파트 API 들이 소유자 아이디를 확인하는지 테스트
func TestBucketOwnerMultipart(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	b := s.bucket(t, 25)
	key := "TestBucketOwnerMultipart"
	owner, wrong := s.owner(), s.wrongOwner()

	_, err := s.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: aws.String(b), Key: aws.String(key), ExpectedBucketOwner: wrong})
	assertOwnerMismatch(t, err)
	created, err := s.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: aws.String(b), Key: aws.String(key), ExpectedBucketOwner: owner})
	if err != nil {
		t.Fatal(err)
	}

	_, err = s.client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket: aws.String(b), Key: aws.String(key), UploadId: created.UploadId, PartNumber: aws.Int32(1),
		Body: bytes.NewReader([]byte("content")), ExpectedBucketOwner: wrong,
	})
	assertOwnerMismatch(t, err)
	part, err := s.client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket: aws.String(b), Key: aws.String(key), UploadId: created.UploadId, PartNumber: aws.Int32(1),
		Body: bytes.NewReader([]byte("content")), ExpectedBucketOwner: owner,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = s.client.ListParts(ctx, &s3.ListPartsInput{Bucket: aws.String(b), Key: aws.String(key), UploadId: created.UploadId, ExpectedBucketOwner: wrong})
	assertOwnerMismatch(t, err)
	listed, err := s.client.ListParts(ctx, &s3.ListPartsInput{Bucket: aws.String(b), Key: aws.String(key), UploadId: created.UploadId, ExpectedBucketOwner: owner})
	if err != nil {
		t.Fatal(err)
	}
	if got := len(listed.Parts); got != 1 {
		t.Fatalf("len(Parts) = %d, want 1", got)
	}

	parts := &types.CompletedMultipartUpload{Parts: []types.CompletedPart{{ETag: part.ETag, PartNumber: aws.Int32(1)}}}
	_, err = s.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: aws.String(b), Key: aws.String(key), UploadId: created.UploadId, ExpectedBucketOwner: wrong})
	assertOwnerMismatch(t, err)
	_, err = s.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket: aws.String(b), Key: aws.String(key), UploadId: created.UploadId, MultipartUpload: parts, ExpectedBucketOwner: wrong,
	})
	assertOwnerMismatch(t, err)
	// 거부된 중단·완료는 업로드를 건드리지 않는다
	if _, err := s.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket: aws.String(b), Key: aws.String(key), UploadId: created.UploadId, MultipartUpload: parts, ExpectedBucketOwner: owner,
	}); err != nil {
		t.Fatal(err)
	}
	if got := read(t, s, b, key); got != "content" {
		t.Fatalf("body = %q, want %q", got, "content")
	}
}

// DeleteBucket 이 소유자 아이디를 확인하는지 테스트
func TestBucketOwnerDeleteBucket(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	b := s.bucket(t, 26)

	_, err := s.client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(b), ExpectedBucketOwner: s.wrongOwner()})
	assertOwnerMismatch(t, err)
	if _, err := s.client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(b)}); err != nil {
		t.Fatal(err)
	}

	if _, err := s.client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(b), ExpectedBucketOwner: s.owner()}); err != nil {
		t.Fatal(err)
	}
	_, err = s.client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(b)})
	assertS3Status(t, err, 404)
}

// 버킷이 없으면 소유자 아이디와 상관없이 NoSuchBucket 인지 테스트
func TestBucketOwnerBucketNotExist(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucketNameOnly(27)

	// 버킷이 없으면 소유자 값과 상관없이 404 다
	for _, owner := range []*string{s.owner(), s.wrongOwner()} {
		_, err := s.client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String(b), ExpectedBucketOwner: owner})
		assertS3Error(t, err, 404, "NoSuchBucket")
	}
}

// 다른 사용자는 소유자 아이디가 맞아도 접근할 수 없는지 테스트
func TestBucketOwnerOtherUser(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	alt := altClientOrSkip(t, s)
	b := s.bucket(t, 28)
	key := "TestBucketOwnerOtherUser"
	put(t, s, b, key, key, nil)

	// 소유자 값이 맞아도 권한이 생기지는 않는다
	for _, owner := range []*string{s.owner(), s.wrongOwner()} {
		_, err := alt.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(b), ExpectedBucketOwner: owner})
		assertOwnerMismatch(t, err)
		_, err = alt.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(b), Key: aws.String(key), ExpectedBucketOwner: owner})
		assertOwnerMismatch(t, err)
	}
}

// endregion
