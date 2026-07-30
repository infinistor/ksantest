package s3tests

import (
	"bytes"
	"context"
	"io"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// 오브젝트의 크기가 0일때 복사가 가능한지 확인하는 테스트
func TestObjectCopyZeroSize(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	b := s.bucket(t)
	source, target := "test_object_copy_zero_size-source", "test_object_copy_zero_size-target"
	if _, err := s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(b), Key: aws.String(source), Body: bytes.NewReader([]byte(""))}); err != nil {
		t.Fatal(err)
	}
	copyCall(t, s.client, &s3.CopyObjectInput{Bucket: aws.String(b), Key: aws.String(target), CopySource: copySource(b, source, "")})
	assertCopied(t, s.client, b, target, "", nil)
	if _, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String(target)}); err != nil {
		t.Fatal(err)
	}
}

// 동일한 버킷에서 오브젝트 복사가 가능한지 확인하는 테스트
func TestObjectCopySameBucket(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	b := s.bucket(t)
	source, target := "test_object_copy_same_bucket-source", "test_object_copy_same_bucket-target"
	body := source
	if _, err := s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(b), Key: aws.String(source), Body: bytes.NewReader([]byte(body))}); err != nil {
		t.Fatal(err)
	}
	copyCall(t, s.client, &s3.CopyObjectInput{Bucket: aws.String(b), Key: aws.String(target), CopySource: copySource(b, source, "")})
	assertCopied(t, s.client, b, target, body, nil)
	if _, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String(target)}); err != nil {
		t.Fatal(err)
	}
}

// ContentType을 설정한 오브젝트를 복사할 경우 복사된 오브젝트도 ContentType값이 일치하는지 확인하는 테스트
func TestObjectCopyVerifyContentType(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	b := s.bucket(t)
	source, target := "test_object_copy_verify_content_type-source", "test_object_copy_verify_content_type-target"
	body, contentType := source, "audio/ogg"
	metadata := map[string]string{"source": "value1", "target": "value2"}
	if _, err := s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(b), Key: aws.String(source), Body: bytes.NewReader([]byte(body)), Metadata: metadata, ContentType: aws.String(contentType)}); err != nil {
		t.Fatal(err)
	}
	copyCall(t, s.client, &s3.CopyObjectInput{Bucket: aws.String(b), Key: aws.String(target), CopySource: copySource(b, source, "")})
	assertCopied(t, s.client, b, target, body, nil)
	head, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String(target)})
	if err != nil {
		t.Fatal(err)
	}
	if aws.ToString(head.ContentType) != contentType {
		t.Fatalf("ContentType=%q", aws.ToString(head.ContentType))
	}
}

// 복사할 오브젝트와 복사될 오브젝트의 경로가 같을 경우 에러를 확인하는 테스트
func TestObjectCopyToItself(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	b := s.bucket(t)
	source := "test_object_copy_to_itself-source"
	body := source
	if _, err := s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(b), Key: aws.String(source), Body: bytes.NewReader([]byte(body))}); err != nil {
		t.Fatal(err)
	}
	_, err := s.client.CopyObject(ctx, &s3.CopyObjectInput{Bucket: aws.String(b), Key: aws.String(source), CopySource: copySource(b, source, "")})
	assertS3Error(t, err, 400, "InvalidRequest")
}

// 복사할 오브젝트와 복사될 오브젝트의 경로가 같지만 메타데이터를 덮어쓰기 모드로 추가하면 해당 오브젝트의 메타데이터가 업데이트되는지 확인하는 테스트
func TestObjectCopyToItselfWithMetadata(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	b := s.bucket(t)
	source := "test_object_copy_to_itself_with_metadata-source"
	body, contentType := source, "audio/ogg"
	metadata := map[string]string{"source": "value1", "target": "value2"}
	if _, err := s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(b), Key: aws.String(source), Body: bytes.NewReader([]byte(body)), Metadata: metadata, ContentType: aws.String(contentType)}); err != nil {
		t.Fatal(err)
	}
	copyCall(t, s.client, &s3.CopyObjectInput{Bucket: aws.String(b), Key: aws.String(source), CopySource: copySource(b, source, ""), Metadata: map[string]string{"foo": "bar2"}, MetadataDirective: types.MetadataDirectiveReplace})
	assertCopied(t, s.client, b, source, body, nil)
	head, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String(source)})
	if err != nil {
		t.Fatal(err)
	}
	if head.Metadata["foo"] != "bar2" {
		t.Fatalf("metadata=%v", head.Metadata)
	}
}

// 다른 버킷으로 오브젝트 복사가 가능한지 확인하는 테스트
func TestObjectCopyDiffBucket(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	sourceBucket, targetBucket := s.bucket(t), s.bucket(t)
	source, target := "test_object_copy_diff_bucket-source", "test_object_copy_diff_bucket-target"
	body := source
	if _, err := s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(sourceBucket), Key: aws.String(source), Body: bytes.NewReader([]byte(body))}); err != nil {
		t.Fatal(err)
	}
	copyCall(t, s.client, &s3.CopyObjectInput{Bucket: aws.String(targetBucket), Key: aws.String(target), CopySource: copySource(sourceBucket, source, "")})
	assertCopied(t, s.client, targetBucket, target, body, nil)
	if _, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(targetBucket), Key: aws.String(target)}); err != nil {
		t.Fatal(err)
	}
}

// [bucket1:created main user, object:created main user / bucket2:created sub user] 메인유저가 만든 버킷, 오브젝트를 서브유저가 만든 버킷으로 오브젝트 복사가 불가능한지 확인하는 테스트
func TestObjectCopyNotOwnedBucket(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	b := s.bucket(t)
	source := "test_object_copy_not_owned_bucket-source"
	body := source
	if _, err := s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(b), Key: aws.String(source), Body: bytes.NewReader([]byte(body))}); err != nil {
		t.Fatal(err)
	}
	alt := s3Client(s.cfg, s.cfg.Alt)
	if s.cfg.Alt.AccessKey == "" {
		t.Skip("Alt User credentials required")
	}
	altBucket := strings.ToLower("copy-alt-" + uniqueBucketSuffix(t))
	if _, err := alt.CreateBucket(ctx, createBucketInput(s.cfg, altBucket)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if !s.cfg.NotDelete {
			_, _ = alt.DeleteBucket(context.Background(), &s3.DeleteBucketInput{Bucket: aws.String(altBucket)})
		}
	})
	_, err := alt.CopyObject(ctx, &s3.CopyObjectInput{Bucket: aws.String(altBucket), Key: aws.String("test_object_copy_not_owned_bucket-target"), CopySource: copySource(b, source, "")})
	assertS3Error(t, err, 403, "AccessDenied")
}

// 다른유저의 버킷의 오브젝트를 권한이 충분할 경우 복사 가능한지 확인하는 테스트
func TestObjectCopyNotOwnedObjectBucket(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	b := s.bucket(t)
	source, target := "test_object_copy_not_owned_object_bucket-source", "test_object_copy_not_owned_object_bucket-target"
	body := source
	if _, err := s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(b), Key: aws.String(source), Body: bytes.NewReader([]byte(body))}); err != nil {
		t.Fatal(err)
	}
	copyCall(t, s.client, &s3.CopyObjectInput{Bucket: aws.String(b), Key: aws.String(target), CopySource: copySource(b, source, "")})
	assertCopied(t, s.client, b, target, body, nil)
	if _, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String(target)}); err != nil {
		t.Fatal(err)
	}
}

// 권한정보를 포함하여 복사할때 올바르게 적용되는지 확인하는 테스트
func TestObjectCopyCannedAcl(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	b := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter)
	source, target := "test_object_copy_canned_acl-source", "test_object_copy_canned_acl-target"
	body := source
	if _, err := s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(b), Key: aws.String(source), Body: bytes.NewReader([]byte(body))}); err != nil {
		t.Fatal(err)
	}
	copyCall(t, s.client, &s3.CopyObjectInput{Bucket: aws.String(b), Key: aws.String(target), CopySource: copySource(b, source, ""), ACL: types.ObjectCannedACLPublicRead})
	assertCopied(t, s.client, b, target, body, nil)
	if _, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String(target)}); err != nil {
		t.Fatal(err)
	}
	acl, aclErr := s.client.GetObjectAcl(ctx, &s3.GetObjectAclInput{Bucket: aws.String(b), Key: aws.String(target)})
	if aclErr != nil || len(acl.Grants) < 2 {
		t.Fatalf("grants=%v err=%v", acl.Grants, aclErr)
	}
}

// 크고 작은 용량의 오브젝트가 복사되는지 확인하는 테스트
func TestObjectCopyRetainingMetadata(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	b := s.bucket(t)
	source, target := "test_object_copy_retaining_metadata-source", "test_object_copy_retaining_metadata-target"
	body, contentType := source, "audio/ogg"
	metadata := map[string]string{"source": "value1", "target": "value2"}
	if _, err := s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(b), Key: aws.String(source), Body: bytes.NewReader([]byte(body)), Metadata: metadata, ContentType: aws.String(contentType)}); err != nil {
		t.Fatal(err)
	}
	copyCall(t, s.client, &s3.CopyObjectInput{Bucket: aws.String(b), Key: aws.String(target), CopySource: copySource(b, source, "")})
	assertCopied(t, s.client, b, target, body, nil)
	head, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String(target)})
	if err != nil {
		t.Fatal(err)
	}
	if aws.ToString(head.ContentType) != contentType {
		t.Fatalf("ContentType=%q", aws.ToString(head.ContentType))
	}
}

// 크고 작은 용량의 오브젝트및 메타데이터가 복사되는지 확인하는 테스트
func TestObjectCopyReplacingMetadata(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	b := s.bucket(t)
	source, target := "test_object_copy_replacing_metadata-source", "test_object_copy_replacing_metadata-target"
	body, contentType := source, "audio/ogg"
	metadata := map[string]string{"source": "value1", "target": "value2"}
	if _, err := s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(b), Key: aws.String(source), Body: bytes.NewReader([]byte(body)), Metadata: metadata, ContentType: aws.String(contentType)}); err != nil {
		t.Fatal(err)
	}
	copyCall(t, s.client, &s3.CopyObjectInput{Bucket: aws.String(b), Key: aws.String(target), CopySource: copySource(b, source, ""), Metadata: map[string]string{"key3": "value3", "key4": "value4"}, MetadataDirective: types.MetadataDirectiveReplace, ContentType: aws.String(contentType)})
	assertCopied(t, s.client, b, target, body, nil)
	head, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String(target)})
	if err != nil {
		t.Fatal(err)
	}
	if head.Metadata["key3"] != "value3" {
		t.Fatalf("metadata=%v", head.Metadata)
	}
}

// 존재하지 않는 버킷에서 존재하지 않는 오브젝트 복사 실패를 확인하는 테스트
func TestObjectCopyBucketNotFound(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	b := s.bucket(t)
	source := "test_object_copy_bucket_not_found-source"
	body := source
	if _, err := s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(b), Key: aws.String(source), Body: bytes.NewReader([]byte(body))}); err != nil {
		t.Fatal(err)
	}
	_, err := s.client.CopyObject(ctx, &s3.CopyObjectInput{Bucket: aws.String(b), Key: aws.String("test_object_copy_bucket_not_found-target"), CopySource: copySource(b+"-fake", source, "")})
	assertS3Error(t, err, 404, "NoSuchBucket")
}

// 존재하지않는 오브젝트 복사 실패를 확인하는 테스트
func TestObjectCopyKeyNotFound(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	b := s.bucket(t)
	source := "test_object_copy_key_not_found-source"
	body := source
	if _, err := s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(b), Key: aws.String(source), Body: bytes.NewReader([]byte(body))}); err != nil {
		t.Fatal(err)
	}
	_, err := s.client.CopyObject(ctx, &s3.CopyObjectInput{Bucket: aws.String(b), Key: aws.String("test_object_copy_key_not_found-target"), CopySource: copySource(b, "missing", "")})
	assertS3Error(t, err, 404, "NoSuchKey")
}

// 버저닝된 오브젝트 복사를 확인하는 테스트
func TestObjectCopyVersioningBucket(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	b := s.bucket(t)
	enableVersioning(t, s, b)
	source, target := "test_object_copy_versioning_bucket-source", "test_object_copy_versioning_bucket-target"
	body := source
	putOut, err := s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(b), Key: aws.String(source), Body: bytes.NewReader([]byte(body))})
	if err != nil {
		t.Fatal(err)
	}
	input := &s3.CopyObjectInput{Bucket: aws.String(b), Key: aws.String(target), CopySource: copySource(b, source, "")}
	if aws.ToString(putOut.VersionId) != "" {
		input.CopySource = copySource(b, source, aws.ToString(putOut.VersionId))
	}
	copyCall(t, s.client, input)
	assertCopied(t, s.client, b, target, body, nil)
	if _, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String(target)}); err != nil {
		t.Fatal(err)
	}
}

// [버킷이 버저닝 가능하고 오브젝트이름에 특수문자가 들어갔을 경우] 오브젝트 복사 성공을 확인하는 테스트
func TestObjectCopyVersioningUrlEncoding(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	b := s.bucket(t)
	enableVersioning(t, s, b)
	source, target := "source?encoded", "target&encoded"
	body := "test_object_copy_versioning_url_encoding-source"
	putOut, err := s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(b), Key: aws.String(source), Body: bytes.NewReader([]byte(body))})
	if err != nil {
		t.Fatal(err)
	}
	input := &s3.CopyObjectInput{Bucket: aws.String(b), Key: aws.String(target), CopySource: copySource(b, source, "")}
	if aws.ToString(putOut.VersionId) != "" {
		input.CopySource = copySource(b, source, aws.ToString(putOut.VersionId))
	}
	copyCall(t, s.client, input)
	assertCopied(t, s.client, b, target, body, nil)
	if _, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String(target)}); err != nil {
		t.Fatal(err)
	}
}

// [버킷에 버저닝 설정] 멀티파트로 업로드된 오브젝트 복사를 확인하는 테스트
func TestObjectCopyVersioningMultipartUpload(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	sourceBucket, targetBucket := s.bucket(t), s.bucket(t)
	enableVersioning(t, s, sourceBucket)
	enableVersioning(t, s, targetBucket)
	body := deterministicBody(6 * 1024 * 1024)
	completeMultipart(t, s.client, sourceBucket, "source", body, false, map[string]string{"foo": "bar"})
	head, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(sourceBucket), Key: aws.String("source")})
	if err != nil {
		t.Fatal(err)
	}
	copyCall(t, s.client, &s3.CopyObjectInput{Bucket: aws.String(targetBucket), Key: aws.String("target"), CopySource: copySource(sourceBucket, "source", aws.ToString(head.VersionId))})
	target, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(targetBucket), Key: aws.String("target")})
	if err != nil || aws.ToInt64(target.ContentLength) != int64(len(body)) || target.Metadata["foo"] != "bar" || aws.ToString(target.VersionId) == "" {
		t.Fatalf("target=%#v err=%v", target, err)
	}
	assertObjectBytes(t, s.client, targetBucket, "target", body)
}

// ifMatch 값을 추가하여 오브젝트를 복사할 경우 성공을 확인하는 테스트
func TestCopyObjectIfMatchGood(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t)
	source, target := "test_copy_object_if_match_good-source", "test_copy_object_if_match_good-target"
	src := put(t, s, b, source, source, nil)
	put(t, s, b, target, "old", nil)
	input := &s3.CopyObjectInput{Bucket: aws.String(b), Key: aws.String(target), CopySource: copySource(b, source, ""), CopySourceIfMatch: src.ETag}
	if _, err := s.client.CopyObject(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	assertCopied(t, s.client, b, target, source, nil)
}

// ifMatch에 잘못된 값을 입력하여 오브젝트를 복사할 경우 실패를 확인하는 테스트
func TestCopyObjectIfMatchFailed(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t)
	source, target := "test_copy_object_if_match_failed-source", "test_copy_object_if_match_failed-target"
	put(t, s, b, source, source, nil)
	put(t, s, b, target, "old", nil)
	input := &s3.CopyObjectInput{Bucket: aws.String(b), Key: aws.String(target), CopySource: copySource(b, source, ""), CopySourceIfMatch: aws.String("bad")}
	_, err := s.client.CopyObject(context.Background(), input)
	assertHTTPError(t, err, 412)
}

// 소스 오브젝트와 일치하지 않는 copy-source-if-none-match 조건으로 복사 성공 확인
func TestCopyObjectIfNoneMatchGood(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t)
	source, target := "test_copy_object_if_none_match_good-source", "test_copy_object_if_none_match_good-target"
	put(t, s, b, source, source, nil)
	put(t, s, b, target, "old", nil)
	input := &s3.CopyObjectInput{Bucket: aws.String(b), Key: aws.String(target), CopySource: copySource(b, source, ""), CopySourceIfNoneMatch: aws.String("bad")}
	if _, err := s.client.CopyObject(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	assertCopied(t, s.client, b, target, source, nil)
}

// 소스 오브젝트와 일치하는 copy-source-if-none-match 조건으로 복사 시 412 실패 확인
func TestCopyObjectIfNoneMatchFailed(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t)
	source, target := "test_copy_object_if_none_match_failed-source", "test_copy_object_if_none_match_failed-target"
	src := put(t, s, b, source, source, nil)
	put(t, s, b, target, "old", nil)
	input := &s3.CopyObjectInput{Bucket: aws.String(b), Key: aws.String(target), CopySource: copySource(b, source, ""), CopySourceIfNoneMatch: src.ETag}
	_, err := s.client.CopyObject(context.Background(), input)
	assertHTTPError(t, err, 412)
}

// 소스 오브젝트 업로드 이전 시간의 copy-source-if-modified-since 조건으로 복사 성공 확인
func TestCopyObjectIfModifiedSinceGood(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t)
	source, target := "test_copy_object_if_modified_since_good-source", "test_copy_object_if_modified_since_good-target"
	put(t, s, b, source, source, nil)
	put(t, s, b, target, "old", nil)
	past := time.Date(1994, 9, 29, 19, 43, 31, 0, time.UTC)
	input := &s3.CopyObjectInput{Bucket: aws.String(b), Key: aws.String(target), CopySource: copySource(b, source, ""), CopySourceIfModifiedSince: &past}
	if _, err := s.client.CopyObject(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	assertCopied(t, s.client, b, target, source, nil)
}

// 소스 오브젝트 업로드 이후 시간의 copy-source-if-modified-since 조건으로 복사 시 412 실패 확인
func TestCopyObjectIfModifiedSinceFailed(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t)
	source, target := "test_copy_object_if_modified_since_failed-source", "test_copy_object_if_modified_since_failed-target"
	put(t, s, b, source, source, nil)
	put(t, s, b, target, "old", nil)
	head := headObject(t, s.client, b, source)
	after := head.LastModified.Add(time.Second)
	input := &s3.CopyObjectInput{Bucket: aws.String(b), Key: aws.String(target), CopySource: copySource(b, source, ""), CopySourceIfModifiedSince: &after}
	time.Sleep(time.Second)
	_, err := s.client.CopyObject(context.Background(), input)
	assertHTTPError(t, err, 412)
}

// 소스 오브젝트 업로드 이후 시간의 copy-source-if-unmodified-since 조건으로 복사 성공 확인
func TestCopyObjectIfUnmodifiedSinceGood(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t)
	source, target := "test_copy_object_if_unmodified_since_good-source", "test_copy_object_if_unmodified_since_good-target"
	put(t, s, b, source, source, nil)
	put(t, s, b, target, "old", nil)
	future := time.Date(2100, 9, 29, 19, 43, 31, 0, time.UTC)
	input := &s3.CopyObjectInput{Bucket: aws.String(b), Key: aws.String(target), CopySource: copySource(b, source, ""), CopySourceIfUnmodifiedSince: &future}
	if _, err := s.client.CopyObject(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	assertCopied(t, s.client, b, target, source, nil)
}

// 소스 오브젝트 업로드 이전 시간의 copy-source-if-unmodified-since 조건으로 복사 시 412 실패 확인
func TestCopyObjectIfUnmodifiedSinceFailed(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t)
	source, target := "test_copy_object_if_unmodified_since_failed-source", "test_copy_object_if_unmodified_since_failed-target"
	put(t, s, b, source, source, nil)
	put(t, s, b, target, "old", nil)
	past := time.Date(1994, 9, 29, 19, 43, 31, 0, time.UTC)
	input := &s3.CopyObjectInput{Bucket: aws.String(b), Key: aws.String(target), CopySource: copySource(b, source, ""), CopySourceIfUnmodifiedSince: &past}
	_, err := s.client.CopyObject(context.Background(), input)
	assertHTTPError(t, err, 412)
}

// copy-source-if-match(일치)와 copy-source-if-unmodified-since(불일치)를 함께 사용할 경우 ETag 조건이 우선되어 복사에 성공하는지 확인
func TestCopyObjectIfMatchWithIfUnmodifiedSince(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t)
	source, target := "test_copy_object_if_match_with_if_unmodified_since-source", "test_copy_object_if_match_with_if_unmodified_since-target"
	src := put(t, s, b, source, source, nil)
	put(t, s, b, target, "old", nil)
	past := time.Date(1994, 9, 29, 19, 43, 31, 0, time.UTC)
	input := &s3.CopyObjectInput{Bucket: aws.String(b), Key: aws.String(target), CopySource: copySource(b, source, ""), CopySourceIfMatch: src.ETag, CopySourceIfUnmodifiedSince: &past}
	if _, err := s.client.CopyObject(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	assertCopied(t, s.client, b, target, source, nil)
}

// copy-source-if-none-match(불일치)와 copy-source-if-modified-since(일치)를 함께 사용할 경우 ETag 조건이 우선되어 412가 반환되는지 확인
func TestCopyObjectIfNoneMatchWithIfModifiedSince(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t)
	source, target := "test_copy_object_if_none_match_with_if_modified_since-source", "test_copy_object_if_none_match_with_if_modified_since-target"
	src := put(t, s, b, source, source, nil)
	put(t, s, b, target, "old", nil)
	past := time.Date(1994, 9, 29, 19, 43, 31, 0, time.UTC)
	input := &s3.CopyObjectInput{Bucket: aws.String(b), Key: aws.String(target), CopySource: copySource(b, source, ""), CopySourceIfNoneMatch: src.ETag, CopySourceIfModifiedSince: &past}
	_, err := s.client.CopyObject(context.Background(), input)
	assertHTTPError(t, err, 412)
}

// copy-source-if-match와 copy-source-if-none-match에 동일한 ETag를 지정하면 412가 반환되는지 확인
func TestCopyObjectIfMatchAndIfNoneMatch(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t)
	source, target := "test_copy_object_if_match_and_if_none_match-source", "test_copy_object_if_match_and_if_none_match-target"
	src := put(t, s, b, source, source, nil)
	put(t, s, b, target, "old", nil)
	input := &s3.CopyObjectInput{Bucket: aws.String(b), Key: aws.String(target), CopySource: copySource(b, source, ""), CopySourceIfMatch: src.ETag, CopySourceIfNoneMatch: src.ETag}
	_, err := s.client.CopyObject(context.Background(), input)
	assertHTTPError(t, err, 412)
}

// copy-source-if-match와 copy-source-if-none-match: * 를 함께 지정하면 412가 반환되는지 확인
func TestCopyObjectIfMatchAndIfNoneMatchAny(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t)
	source, target := "test_copy_object_if_match_and_if_none_match_any-source", "test_copy_object_if_match_and_if_none_match_any-target"
	src := put(t, s, b, source, source, nil)
	put(t, s, b, target, "old", nil)
	input := &s3.CopyObjectInput{Bucket: aws.String(b), Key: aws.String(target), CopySource: copySource(b, source, ""), CopySourceIfMatch: src.ETag, CopySourceIfNoneMatch: aws.String("*")}
	_, err := s.client.CopyObject(context.Background(), input)
	assertHTTPError(t, err, 412)
}

// 대상 오브젝트와 일치하는 If-Match 조건으로 덮어쓰기 복사 성공 확인
func TestCopyObjectDestinationIfMatchGood(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t)
	source, target := "test_copy_object_destination_if_match_good-source", "test_copy_object_destination_if_match_good-target"
	put(t, s, b, source, source, nil)
	dst := put(t, s, b, target, "old", nil)
	input := &s3.CopyObjectInput{Bucket: aws.String(b), Key: aws.String(target), CopySource: copySource(b, source, ""), IfMatch: dst.ETag}
	if _, err := s.client.CopyObject(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	assertCopied(t, s.client, b, target, source, nil)
}

// 대상 오브젝트와 일치하지 않는 If-Match 조건으로 덮어쓰기 복사 시 412 실패 확인
func TestCopyObjectDestinationIfMatchFailed(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t)
	source, target := "test_copy_object_destination_if_match_failed-source", "test_copy_object_destination_if_match_failed-target"
	put(t, s, b, source, source, nil)
	put(t, s, b, target, "old", nil)
	input := &s3.CopyObjectInput{Bucket: aws.String(b), Key: aws.String(target), CopySource: copySource(b, source, ""), IfMatch: aws.String("bad")}
	_, err := s.client.CopyObject(context.Background(), input)
	assertHTTPError(t, err, 412)
}

// 존재하지 않는 대상 키에 If-None-Match: * 조건으로 복사 성공 확인
func TestCopyObjectDestinationIfNoneMatchGood(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t)
	source, target := "test_copy_object_destination_if_none_match_good-source", "test_copy_object_destination_if_none_match_good-target-new"
	put(t, s, b, source, source, nil)
	input := &s3.CopyObjectInput{Bucket: aws.String(b), Key: aws.String(target), CopySource: copySource(b, source, ""), IfNoneMatch: aws.String("*")}
	if _, err := s.client.CopyObject(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	assertCopied(t, s.client, b, target, source, nil)
}

// 이미 존재하는 대상 키에 If-None-Match: * 조건으로 복사 시 412 실패 확인
func TestCopyObjectDestinationIfNoneMatchFailed(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t)
	source, target := "test_copy_object_destination_if_none_match_failed-source", "test_copy_object_destination_if_none_match_failed-target"
	put(t, s, b, source, source, nil)
	put(t, s, b, target, "old", nil)
	input := &s3.CopyObjectInput{Bucket: aws.String(b), Key: aws.String(target), CopySource: copySource(b, source, ""), IfNoneMatch: aws.String("*")}
	_, err := s.client.CopyObject(context.Background(), input)
	assertHTTPError(t, err, 412)
}

// 대상에 If-Match와 If-None-Match를 함께 지정하면 501로 거부되는지 확인
func TestCopyObjectDestinationIfMatchAndIfNoneMatch(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t)
	source, target := "test_copy_object_destination_if_match_and_if_none_match-source", "test_copy_object_destination_if_match_and_if_none_match-target"
	put(t, s, b, source, source, nil)
	dst := put(t, s, b, target, "old", nil)
	input := &s3.CopyObjectInput{Bucket: aws.String(b), Key: aws.String(target), CopySource: copySource(b, source, ""), IfMatch: dst.ETag, IfNoneMatch: aws.String("*")}
	_, err := s.client.CopyObject(context.Background(), input)
	assertHTTPError(t, err, 501)
}

// 대상에 If-Match와 If-None-Match: * 를 함께 지정하면 501로 거부되는지 확인
func TestCopyObjectDestinationIfMatchAndIfNoneMatchAny(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t)
	source, target := "test_copy_object_destination_if_match_and_if_none_match_any-source", "test_copy_object_destination_if_match_and_if_none_match_any-target"
	put(t, s, b, source, source, nil)
	dst := put(t, s, b, target, "old", nil)
	input := &s3.CopyObjectInput{Bucket: aws.String(b), Key: aws.String(target), CopySource: copySource(b, source, ""), IfMatch: dst.ETag, IfNoneMatch: dst.ETag}
	_, err := s.client.CopyObject(context.Background(), input)
	assertHTTPError(t, err, 501)
}

// 소스 If-Match와 대상 If-None-Match: * 를 함께 사용해 복사 성공 확인
func TestCopyObjectSourceIfMatchWithDestinationIfNoneMatch(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t)
	source, target := "test_copy_object_source_if_match_with_destination_if_none_match-source", "test_copy_object_source_if_match_with_destination_if_none_match-target-new"
	src := put(t, s, b, source, source, nil)
	input := &s3.CopyObjectInput{Bucket: aws.String(b), Key: aws.String(target), CopySource: copySource(b, source, ""), CopySourceIfMatch: src.ETag, IfNoneMatch: aws.String("*")}
	if _, err := s.client.CopyObject(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	assertCopied(t, s.client, b, target, source, nil)
}

// [source obj : normal, dest bucket : normal, dest obj : normal] 오브젝트 복사 성공을 확인하는 테스트
func TestCopyNorSrcToNorBucketAndObj(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	sourceBucket, targetBucket := s.bucket(t), s.bucket(t)
	source, target := "source", "target"
	body := []byte("encrypted copy")
	putInput := &s3.PutObjectInput{Bucket: aws.String(sourceBucket), Key: aws.String(source), Body: bytes.NewReader(body)}
	if _, err := s.client.PutObject(context.Background(), putInput); err != nil {
		t.Fatal(err)
	}
	copyInput := &s3.CopyObjectInput{Bucket: aws.String(targetBucket), Key: aws.String(target), CopySource: copySource(sourceBucket, source, "")}
	copyInput.ServerSideEncryption = types.ServerSideEncryptionAes256
	copyCall(t, s.client, copyInput)
	assertCopied(t, s.client, targetBucket, target, string(body), nil)
	headInput := &s3.HeadObjectInput{Bucket: aws.String(targetBucket), Key: aws.String(target)}
	head, err := s.client.HeadObject(context.Background(), headInput)
	if err != nil {
		t.Fatal(err)
	}
	if head.ServerSideEncryption != types.ServerSideEncryptionAes256 {
		t.Fatalf("target encryption=%q", head.ServerSideEncryption)
	}
}

// [source obj : normal, dest bucket : normal, dest obj : encryption] 오브젝트 복사 성공을 확인하는 테스트
func TestCopyNorSrcToNorBucketEncryptionObj(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	sourceBucket, targetBucket := s.bucket(t), s.bucket(t)
	source, target := "source", "target"
	body := []byte("encrypted copy")
	putInput := &s3.PutObjectInput{Bucket: aws.String(sourceBucket), Key: aws.String(source), Body: bytes.NewReader(body)}
	if _, err := s.client.PutObject(context.Background(), putInput); err != nil {
		t.Fatal(err)
	}
	copyInput := &s3.CopyObjectInput{Bucket: aws.String(targetBucket), Key: aws.String(target), CopySource: copySource(sourceBucket, source, "")}
	copyInput.ServerSideEncryption = types.ServerSideEncryptionAes256
	copyCall(t, s.client, copyInput)
	assertCopied(t, s.client, targetBucket, target, string(body), nil)
	headInput := &s3.HeadObjectInput{Bucket: aws.String(targetBucket), Key: aws.String(target)}
	head, err := s.client.HeadObject(context.Background(), headInput)
	if err != nil {
		t.Fatal(err)
	}
	if head.ServerSideEncryption != types.ServerSideEncryptionAes256 {
		t.Fatalf("target encryption=%q", head.ServerSideEncryption)
	}
}

// [source obj : normal, dest bucket : encryption, dest obj : normal] 오브젝트 복사 성공을 확인하는 테스트
func TestCopyNorSrcToEncryptionBucketNorObj(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	sourceBucket, targetBucket := s.bucket(t), s.bucket(t)
	source, target := "source", "target"
	body := []byte("encrypted copy")
	putAESBucketEncryption(t, s.client, sourceBucket)
	putAESBucketEncryption(t, s.client, targetBucket)
	putInput := &s3.PutObjectInput{Bucket: aws.String(sourceBucket), Key: aws.String(source), Body: bytes.NewReader(body)}
	if _, err := s.client.PutObject(context.Background(), putInput); err != nil {
		t.Fatal(err)
	}
	copyInput := &s3.CopyObjectInput{Bucket: aws.String(targetBucket), Key: aws.String(target), CopySource: copySource(sourceBucket, source, "")}
	copyCall(t, s.client, copyInput)
	assertCopied(t, s.client, targetBucket, target, string(body), nil)
	headInput := &s3.HeadObjectInput{Bucket: aws.String(targetBucket), Key: aws.String(target)}
	head, err := s.client.HeadObject(context.Background(), headInput)
	if err != nil {
		t.Fatal(err)
	}
	if head.ServerSideEncryption != types.ServerSideEncryptionAes256 {
		t.Fatalf("target encryption=%q", head.ServerSideEncryption)
	}
}

// [source obj : normal, dest bucket : encryption, dest obj : encryption] 오브젝트 복사 성공을 확인하는 테스트
func TestCopyNorSrcToEncryptionBucketAndObj(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	sourceBucket, targetBucket := s.bucket(t), s.bucket(t)
	source, target := "source", "target"
	body := []byte("encrypted copy")
	putAESBucketEncryption(t, s.client, sourceBucket)
	putAESBucketEncryption(t, s.client, targetBucket)
	putInput := &s3.PutObjectInput{Bucket: aws.String(sourceBucket), Key: aws.String(source), Body: bytes.NewReader(body)}
	putInput.ServerSideEncryption = types.ServerSideEncryptionAes256
	if _, err := s.client.PutObject(context.Background(), putInput); err != nil {
		t.Fatal(err)
	}
	copyInput := &s3.CopyObjectInput{Bucket: aws.String(targetBucket), Key: aws.String(target), CopySource: copySource(sourceBucket, source, "")}
	copyInput.ServerSideEncryption = types.ServerSideEncryptionAes256
	copyCall(t, s.client, copyInput)
	assertCopied(t, s.client, targetBucket, target, string(body), nil)
	headInput := &s3.HeadObjectInput{Bucket: aws.String(targetBucket), Key: aws.String(target)}
	head, err := s.client.HeadObject(context.Background(), headInput)
	if err != nil {
		t.Fatal(err)
	}
	if head.ServerSideEncryption != types.ServerSideEncryptionAes256 {
		t.Fatalf("target encryption=%q", head.ServerSideEncryption)
	}
}

// [source obj : encryption, dest bucket : normal, dest obj : normal] 오브젝트 복사 성공을 확인하는 테스트
func TestCopyEncryptionSrcToNorBucketAndObj(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	sourceBucket, targetBucket := s.bucket(t), s.bucket(t)
	source, target := "source", "target"
	body := []byte("encrypted copy")
	putInput := &s3.PutObjectInput{Bucket: aws.String(sourceBucket), Key: aws.String(source), Body: bytes.NewReader(body)}
	putInput.ServerSideEncryption = types.ServerSideEncryptionAes256
	if _, err := s.client.PutObject(context.Background(), putInput); err != nil {
		t.Fatal(err)
	}
	copyInput := &s3.CopyObjectInput{Bucket: aws.String(targetBucket), Key: aws.String(target), CopySource: copySource(sourceBucket, source, "")}
	copyInput.ServerSideEncryption = types.ServerSideEncryptionAes256
	copyCall(t, s.client, copyInput)
	assertCopied(t, s.client, targetBucket, target, string(body), nil)
	headInput := &s3.HeadObjectInput{Bucket: aws.String(targetBucket), Key: aws.String(target)}
	head, err := s.client.HeadObject(context.Background(), headInput)
	if err != nil {
		t.Fatal(err)
	}
	if head.ServerSideEncryption != types.ServerSideEncryptionAes256 {
		t.Fatalf("target encryption=%q", head.ServerSideEncryption)
	}
}

// [source obj : encryption, dest bucket : normal, dest obj : encryption] 오브젝트 복사 성공을 확인하는 테스트
func TestCopyEncryptionSrcToNorBucketEncryptionObj(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	sourceBucket, targetBucket := s.bucket(t), s.bucket(t)
	source, target := "source", "target"
	body := []byte("encrypted copy")
	putInput := &s3.PutObjectInput{Bucket: aws.String(sourceBucket), Key: aws.String(source), Body: bytes.NewReader(body)}
	putInput.ServerSideEncryption = types.ServerSideEncryptionAes256
	if _, err := s.client.PutObject(context.Background(), putInput); err != nil {
		t.Fatal(err)
	}
	copyInput := &s3.CopyObjectInput{Bucket: aws.String(targetBucket), Key: aws.String(target), CopySource: copySource(sourceBucket, source, "")}
	copyInput.ServerSideEncryption = types.ServerSideEncryptionAes256
	copyCall(t, s.client, copyInput)
	assertCopied(t, s.client, targetBucket, target, string(body), nil)
	headInput := &s3.HeadObjectInput{Bucket: aws.String(targetBucket), Key: aws.String(target)}
	head, err := s.client.HeadObject(context.Background(), headInput)
	if err != nil {
		t.Fatal(err)
	}
	if head.ServerSideEncryption != types.ServerSideEncryptionAes256 {
		t.Fatalf("target encryption=%q", head.ServerSideEncryption)
	}
}

// [source obj : encryption, dest bucket : encryption, dest obj : normal] 오브젝트 복사 성공을 확인하는 테스트
func TestCopyEncryptionSrcToEncryptionBucketNorObj(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	sourceBucket, targetBucket := s.bucket(t), s.bucket(t)
	source, target := "source", "target"
	body := []byte("encrypted copy")
	putAESBucketEncryption(t, s.client, sourceBucket)
	putAESBucketEncryption(t, s.client, targetBucket)
	putInput := &s3.PutObjectInput{Bucket: aws.String(sourceBucket), Key: aws.String(source), Body: bytes.NewReader(body)}
	putInput.ServerSideEncryption = types.ServerSideEncryptionAes256
	if _, err := s.client.PutObject(context.Background(), putInput); err != nil {
		t.Fatal(err)
	}
	copyInput := &s3.CopyObjectInput{Bucket: aws.String(targetBucket), Key: aws.String(target), CopySource: copySource(sourceBucket, source, "")}
	copyCall(t, s.client, copyInput)
	assertCopied(t, s.client, targetBucket, target, string(body), nil)
	headInput := &s3.HeadObjectInput{Bucket: aws.String(targetBucket), Key: aws.String(target)}
	head, err := s.client.HeadObject(context.Background(), headInput)
	if err != nil {
		t.Fatal(err)
	}
	if head.ServerSideEncryption != types.ServerSideEncryptionAes256 {
		t.Fatalf("target encryption=%q", head.ServerSideEncryption)
	}
}

// [source obj : encryption, dest bucket : encryption, dest obj : encryption] 오브젝트 복사 성공을 확인하는 테스트
func TestCopyEncryptionSrcToEncryptionBucketAndObj(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	sourceBucket, targetBucket := s.bucket(t), s.bucket(t)
	source, target := "source", "target"
	body := []byte("encrypted copy")
	putAESBucketEncryption(t, s.client, sourceBucket)
	putAESBucketEncryption(t, s.client, targetBucket)
	putInput := &s3.PutObjectInput{Bucket: aws.String(sourceBucket), Key: aws.String(source), Body: bytes.NewReader(body)}
	putInput.ServerSideEncryption = types.ServerSideEncryptionAes256
	if _, err := s.client.PutObject(context.Background(), putInput); err != nil {
		t.Fatal(err)
	}
	copyInput := &s3.CopyObjectInput{Bucket: aws.String(targetBucket), Key: aws.String(target), CopySource: copySource(sourceBucket, source, "")}
	copyInput.ServerSideEncryption = types.ServerSideEncryptionAes256
	copyCall(t, s.client, copyInput)
	assertCopied(t, s.client, targetBucket, target, string(body), nil)
	headInput := &s3.HeadObjectInput{Bucket: aws.String(targetBucket), Key: aws.String(target)}
	head, err := s.client.HeadObject(context.Background(), headInput)
	if err != nil {
		t.Fatal(err)
	}
	if head.ServerSideEncryption != types.ServerSideEncryptionAes256 {
		t.Fatalf("target encryption=%q", head.ServerSideEncryption)
	}
}

// [source bucket : encryption, source obj : normal, dest bucket : normal, dest obj : normal] 오브젝트 복사 성공을 확인하는 테스트
func TestCopyEncryptionBucketNorObjToNorBucketAndObj(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	sourceBucket, targetBucket := s.bucket(t), s.bucket(t)
	source, target := "source", "target"
	body := []byte("encrypted copy")
	putAESBucketEncryption(t, s.client, sourceBucket)
	putInput := &s3.PutObjectInput{Bucket: aws.String(sourceBucket), Key: aws.String(source), Body: bytes.NewReader(body)}
	if _, err := s.client.PutObject(context.Background(), putInput); err != nil {
		t.Fatal(err)
	}
	copyInput := &s3.CopyObjectInput{Bucket: aws.String(targetBucket), Key: aws.String(target), CopySource: copySource(sourceBucket, source, "")}
	copyInput.ServerSideEncryption = types.ServerSideEncryptionAes256
	copyCall(t, s.client, copyInput)
	assertCopied(t, s.client, targetBucket, target, string(body), nil)
	headInput := &s3.HeadObjectInput{Bucket: aws.String(targetBucket), Key: aws.String(target)}
	head, err := s.client.HeadObject(context.Background(), headInput)
	if err != nil {
		t.Fatal(err)
	}
	if head.ServerSideEncryption != types.ServerSideEncryptionAes256 {
		t.Fatalf("target encryption=%q", head.ServerSideEncryption)
	}
}

// [source obj : normal, dest bucket : normal, dest obj : encryption] 오브젝트 복사 성공을 확인하는 테스트
func TestCopyEncryptionBucketNorObjToNorBucketEncryptionObj(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	sourceBucket, targetBucket := s.bucket(t), s.bucket(t)
	source, target := "source", "target"
	body := []byte("encrypted copy")
	putAESBucketEncryption(t, s.client, sourceBucket)
	putInput := &s3.PutObjectInput{Bucket: aws.String(sourceBucket), Key: aws.String(source), Body: bytes.NewReader(body)}
	if _, err := s.client.PutObject(context.Background(), putInput); err != nil {
		t.Fatal(err)
	}
	copyInput := &s3.CopyObjectInput{Bucket: aws.String(targetBucket), Key: aws.String(target), CopySource: copySource(sourceBucket, source, "")}
	copyInput.ServerSideEncryption = types.ServerSideEncryptionAes256
	copyCall(t, s.client, copyInput)
	assertCopied(t, s.client, targetBucket, target, string(body), nil)
	headInput := &s3.HeadObjectInput{Bucket: aws.String(targetBucket), Key: aws.String(target)}
	head, err := s.client.HeadObject(context.Background(), headInput)
	if err != nil {
		t.Fatal(err)
	}
	if head.ServerSideEncryption != types.ServerSideEncryptionAes256 {
		t.Fatalf("target encryption=%q", head.ServerSideEncryption)
	}
}

// [source obj : normal, dest bucket : encryption, dest obj : normal] 오브젝트 복사 성공을 확인하는 테스트
func TestCopyEncryptionBucketNorObjToEncryptionBucketNorObj(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	sourceBucket, targetBucket := s.bucket(t), s.bucket(t)
	source, target := "source", "target"
	body := []byte("encrypted copy")
	putAESBucketEncryption(t, s.client, sourceBucket)
	putAESBucketEncryption(t, s.client, targetBucket)
	putInput := &s3.PutObjectInput{Bucket: aws.String(sourceBucket), Key: aws.String(source), Body: bytes.NewReader(body)}
	if _, err := s.client.PutObject(context.Background(), putInput); err != nil {
		t.Fatal(err)
	}
	copyInput := &s3.CopyObjectInput{Bucket: aws.String(targetBucket), Key: aws.String(target), CopySource: copySource(sourceBucket, source, "")}
	copyCall(t, s.client, copyInput)
	assertCopied(t, s.client, targetBucket, target, string(body), nil)
	headInput := &s3.HeadObjectInput{Bucket: aws.String(targetBucket), Key: aws.String(target)}
	head, err := s.client.HeadObject(context.Background(), headInput)
	if err != nil {
		t.Fatal(err)
	}
	if head.ServerSideEncryption != types.ServerSideEncryptionAes256 {
		t.Fatalf("target encryption=%q", head.ServerSideEncryption)
	}
}

// [source obj : normal, dest bucket : encryption, dest obj : encryption] 오브젝트 복사 성공을 확인하는 테스트
func TestCopyEncryptionBucketNorObjToEncryptionBucketAndObj(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	sourceBucket, targetBucket := s.bucket(t), s.bucket(t)
	source, target := "source", "target"
	body := []byte("encrypted copy")
	putAESBucketEncryption(t, s.client, sourceBucket)
	putAESBucketEncryption(t, s.client, targetBucket)
	putInput := &s3.PutObjectInput{Bucket: aws.String(sourceBucket), Key: aws.String(source), Body: bytes.NewReader(body)}
	putInput.ServerSideEncryption = types.ServerSideEncryptionAes256
	if _, err := s.client.PutObject(context.Background(), putInput); err != nil {
		t.Fatal(err)
	}
	copyInput := &s3.CopyObjectInput{Bucket: aws.String(targetBucket), Key: aws.String(target), CopySource: copySource(sourceBucket, source, "")}
	copyInput.ServerSideEncryption = types.ServerSideEncryptionAes256
	copyCall(t, s.client, copyInput)
	assertCopied(t, s.client, targetBucket, target, string(body), nil)
	headInput := &s3.HeadObjectInput{Bucket: aws.String(targetBucket), Key: aws.String(target)}
	head, err := s.client.HeadObject(context.Background(), headInput)
	if err != nil {
		t.Fatal(err)
	}
	if head.ServerSideEncryption != types.ServerSideEncryptionAes256 {
		t.Fatalf("target encryption=%q", head.ServerSideEncryption)
	}
}

// [source obj : encryption, dest bucket : normal, dest obj : normal] 오브젝트 복사 성공을 확인하는 테스트
func TestCopyEncryptionBucketAndObjToNorBucketAndObj(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	sourceBucket, targetBucket := s.bucket(t), s.bucket(t)
	source, target := "source", "target"
	body := []byte("encrypted copy")
	putAESBucketEncryption(t, s.client, sourceBucket)
	putInput := &s3.PutObjectInput{Bucket: aws.String(sourceBucket), Key: aws.String(source), Body: bytes.NewReader(body)}
	putInput.ServerSideEncryption = types.ServerSideEncryptionAes256
	if _, err := s.client.PutObject(context.Background(), putInput); err != nil {
		t.Fatal(err)
	}
	copyInput := &s3.CopyObjectInput{Bucket: aws.String(targetBucket), Key: aws.String(target), CopySource: copySource(sourceBucket, source, "")}
	copyInput.ServerSideEncryption = types.ServerSideEncryptionAes256
	copyCall(t, s.client, copyInput)
	assertCopied(t, s.client, targetBucket, target, string(body), nil)
	headInput := &s3.HeadObjectInput{Bucket: aws.String(targetBucket), Key: aws.String(target)}
	head, err := s.client.HeadObject(context.Background(), headInput)
	if err != nil {
		t.Fatal(err)
	}
	if head.ServerSideEncryption != types.ServerSideEncryptionAes256 {
		t.Fatalf("target encryption=%q", head.ServerSideEncryption)
	}
}

// [source obj : encryption, dest bucket : normal, dest obj : encryption] 오브젝트 복사 성공을 확인하는 테스트
func TestCopyEncryptionBucketAndObjToNorBucketEncryptionObj(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	sourceBucket, targetBucket := s.bucket(t), s.bucket(t)
	source, target := "source", "target"
	body := []byte("encrypted copy")
	putAESBucketEncryption(t, s.client, sourceBucket)
	putInput := &s3.PutObjectInput{Bucket: aws.String(sourceBucket), Key: aws.String(source), Body: bytes.NewReader(body)}
	putInput.ServerSideEncryption = types.ServerSideEncryptionAes256
	if _, err := s.client.PutObject(context.Background(), putInput); err != nil {
		t.Fatal(err)
	}
	copyInput := &s3.CopyObjectInput{Bucket: aws.String(targetBucket), Key: aws.String(target), CopySource: copySource(sourceBucket, source, "")}
	copyInput.ServerSideEncryption = types.ServerSideEncryptionAes256
	copyCall(t, s.client, copyInput)
	assertCopied(t, s.client, targetBucket, target, string(body), nil)
	headInput := &s3.HeadObjectInput{Bucket: aws.String(targetBucket), Key: aws.String(target)}
	head, err := s.client.HeadObject(context.Background(), headInput)
	if err != nil {
		t.Fatal(err)
	}
	if head.ServerSideEncryption != types.ServerSideEncryptionAes256 {
		t.Fatalf("target encryption=%q", head.ServerSideEncryption)
	}
}

// [source obj : encryption, dest bucket : encryption, dest obj : normal] 오브젝트 복사 성공을 확인하는 테스트
func TestCopyEncryptionBucketAndObjToEncryptionBucketNorObj(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	sourceBucket, targetBucket := s.bucket(t), s.bucket(t)
	source, target := "source", "target"
	body := []byte("encrypted copy")
	putAESBucketEncryption(t, s.client, sourceBucket)
	putAESBucketEncryption(t, s.client, targetBucket)
	putInput := &s3.PutObjectInput{Bucket: aws.String(sourceBucket), Key: aws.String(source), Body: bytes.NewReader(body)}
	putInput.ServerSideEncryption = types.ServerSideEncryptionAes256
	if _, err := s.client.PutObject(context.Background(), putInput); err != nil {
		t.Fatal(err)
	}
	copyInput := &s3.CopyObjectInput{Bucket: aws.String(targetBucket), Key: aws.String(target), CopySource: copySource(sourceBucket, source, "")}
	copyCall(t, s.client, copyInput)
	assertCopied(t, s.client, targetBucket, target, string(body), nil)
	headInput := &s3.HeadObjectInput{Bucket: aws.String(targetBucket), Key: aws.String(target)}
	head, err := s.client.HeadObject(context.Background(), headInput)
	if err != nil {
		t.Fatal(err)
	}
	if head.ServerSideEncryption != types.ServerSideEncryptionAes256 {
		t.Fatalf("target encryption=%q", head.ServerSideEncryption)
	}
}

// [source obj : encryption, dest bucket : encryption, dest obj : encryption] 오브젝트 복사 성공을 확인하는 테스트
func TestCopyEncryptionBucketAndObjToEncryptionBucketAndObj(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	sourceBucket, targetBucket := s.bucket(t), s.bucket(t)
	source, target := "source", "target"
	body := []byte("encrypted copy")
	putAESBucketEncryption(t, s.client, sourceBucket)
	putAESBucketEncryption(t, s.client, targetBucket)
	putInput := &s3.PutObjectInput{Bucket: aws.String(sourceBucket), Key: aws.String(source), Body: bytes.NewReader(body)}
	putInput.ServerSideEncryption = types.ServerSideEncryptionAes256
	if _, err := s.client.PutObject(context.Background(), putInput); err != nil {
		t.Fatal(err)
	}
	copyInput := &s3.CopyObjectInput{Bucket: aws.String(targetBucket), Key: aws.String(target), CopySource: copySource(sourceBucket, source, "")}
	copyInput.ServerSideEncryption = types.ServerSideEncryptionAes256
	copyCall(t, s.client, copyInput)
	assertCopied(t, s.client, targetBucket, target, string(body), nil)
	headInput := &s3.HeadObjectInput{Bucket: aws.String(targetBucket), Key: aws.String(target)}
	head, err := s.client.HeadObject(context.Background(), headInput)
	if err != nil {
		t.Fatal(err)
	}
	if head.ServerSideEncryption != types.ServerSideEncryptionAes256 {
		t.Fatalf("target encryption=%q", head.ServerSideEncryption)
	}
}

// 일반 오브젝트에서 다양한 방식으로 복사 성공을 확인하는 테스트
func TestCopyToNormalSource(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	for _, targetMode := range []string{"normal", "sse-s3", "sse-c"} {
		sourceBucket, targetBucket := s.bucket(t), s.bucket(t)
		if targetMode == "sse-c" {
			unblockSseC(t, s, targetBucket)
		}
		source, target := "source", "target"
		body := []byte("encrypted copy")
		putInput := &s3.PutObjectInput{Bucket: aws.String(sourceBucket), Key: aws.String(source), Body: bytes.NewReader(body)}
		if _, err := s.client.PutObject(context.Background(), putInput); err != nil {
			t.Fatal(err)
		}
		copyInput := &s3.CopyObjectInput{Bucket: aws.String(targetBucket), Key: aws.String(target), CopySource: copySource(sourceBucket, source, "")}
		if targetMode == "sse-s3" {
			copyInput.ServerSideEncryption = types.ServerSideEncryptionAes256
		}
		if targetMode == "sse-c" {
			copyInput.SSECustomerAlgorithm = aws.String(sseCAlgorithm)
			copyInput.SSECustomerKey = aws.String(sseCKey)
			copyInput.SSECustomerKeyMD5 = aws.String(sseCKeyMD5)
		}
		copyCall(t, s.client, copyInput)
		var getOptions func(*s3.GetObjectInput)
		if targetMode == "sse-c" {
			getOptions = func(in *s3.GetObjectInput) {
				in.SSECustomerAlgorithm = aws.String(sseCAlgorithm)
				in.SSECustomerKey = aws.String(sseCKey)
				in.SSECustomerKeyMD5 = aws.String(sseCKeyMD5)
			}
		}
		assertCopied(t, s.client, targetBucket, target, string(body), getOptions)
		headInput := &s3.HeadObjectInput{Bucket: aws.String(targetBucket), Key: aws.String(target)}
		if targetMode == "sse-c" {
			headInput.SSECustomerAlgorithm = aws.String(sseCAlgorithm)
			headInput.SSECustomerKey = aws.String(sseCKey)
			headInput.SSECustomerKeyMD5 = aws.String(sseCKeyMD5)
		}
		head, err := s.client.HeadObject(context.Background(), headInput)
		if err != nil {
			t.Fatal(err)
		}
		encrypted := targetMode != "normal"
		if encrypted && targetMode != "sse-c" && head.ServerSideEncryption != types.ServerSideEncryptionAes256 {
			t.Fatalf("target encryption=%q", head.ServerSideEncryption)
		}
	}
}

// SSE-S3암호화 된 오브젝트에서 다양한 방식으로 복사 성공을 확인하는 테스트
func TestCopyToSseS3Source(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	for _, targetMode := range []string{"normal", "sse-s3", "sse-c"} {
		sourceBucket, targetBucket := s.bucket(t), s.bucket(t)
		if targetMode == "sse-c" {
			unblockSseC(t, s, targetBucket)
		}
		source, target := "source", "target"
		body := []byte("encrypted copy")
		putInput := &s3.PutObjectInput{Bucket: aws.String(sourceBucket), Key: aws.String(source), Body: bytes.NewReader(body)}
		putInput.ServerSideEncryption = types.ServerSideEncryptionAes256
		if _, err := s.client.PutObject(context.Background(), putInput); err != nil {
			t.Fatal(err)
		}
		copyInput := &s3.CopyObjectInput{Bucket: aws.String(targetBucket), Key: aws.String(target), CopySource: copySource(sourceBucket, source, "")}
		if targetMode == "sse-s3" {
			copyInput.ServerSideEncryption = types.ServerSideEncryptionAes256
		}
		if targetMode == "sse-c" {
			copyInput.SSECustomerAlgorithm = aws.String(sseCAlgorithm)
			copyInput.SSECustomerKey = aws.String(sseCKey)
			copyInput.SSECustomerKeyMD5 = aws.String(sseCKeyMD5)
		}
		copyCall(t, s.client, copyInput)
		var getOptions func(*s3.GetObjectInput)
		if targetMode == "sse-c" {
			getOptions = func(in *s3.GetObjectInput) {
				in.SSECustomerAlgorithm = aws.String(sseCAlgorithm)
				in.SSECustomerKey = aws.String(sseCKey)
				in.SSECustomerKeyMD5 = aws.String(sseCKeyMD5)
			}
		}
		assertCopied(t, s.client, targetBucket, target, string(body), getOptions)
		headInput := &s3.HeadObjectInput{Bucket: aws.String(targetBucket), Key: aws.String(target)}
		if targetMode == "sse-c" {
			headInput.SSECustomerAlgorithm = aws.String(sseCAlgorithm)
			headInput.SSECustomerKey = aws.String(sseCKey)
			headInput.SSECustomerKeyMD5 = aws.String(sseCKeyMD5)
		}
		head, err := s.client.HeadObject(context.Background(), headInput)
		if err != nil {
			t.Fatal(err)
		}
		encrypted := targetMode != "normal"
		if encrypted && targetMode != "sse-c" && head.ServerSideEncryption != types.ServerSideEncryptionAes256 {
			t.Fatalf("target encryption=%q", head.ServerSideEncryption)
		}
	}
}

// SSE-C암호화 된 오브젝트에서 다양한 방식으로 복사 성공을 확인하는 테스트 SDK V1은 SSE-C 차단 해제(BlockedEncryptionTypes)를 지원하지 않아 V2만 테스트한다.
func TestCopyToSseCSource(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	for _, targetMode := range []string{"normal", "sse-s3", "sse-c"} {
		sourceBucket, targetBucket := s.bucket(t), s.bucket(t)
		unblockSseC(t, s, sourceBucket)
		if targetMode == "sse-c" {
			unblockSseC(t, s, targetBucket)
		}
		source, target := "source", "target"
		body := []byte("encrypted copy")
		putInput := sseCPutInput(sourceBucket, source, body)
		if _, err := s.client.PutObject(context.Background(), putInput); err != nil {
			t.Fatal(err)
		}
		copyInput := &s3.CopyObjectInput{Bucket: aws.String(targetBucket), Key: aws.String(target), CopySource: copySource(sourceBucket, source, "")}
		copyInput.CopySourceSSECustomerAlgorithm = aws.String(sseCAlgorithm)
		copyInput.CopySourceSSECustomerKey = aws.String(sseCKey)
		copyInput.CopySourceSSECustomerKeyMD5 = aws.String(sseCKeyMD5)
		if targetMode == "sse-s3" {
			copyInput.ServerSideEncryption = types.ServerSideEncryptionAes256
		}
		if targetMode == "sse-c" {
			copyInput.SSECustomerAlgorithm = aws.String(sseCAlgorithm)
			copyInput.SSECustomerKey = aws.String(sseCKey)
			copyInput.SSECustomerKeyMD5 = aws.String(sseCKeyMD5)
		}
		copyCall(t, s.client, copyInput)
		var getOptions func(*s3.GetObjectInput)
		if targetMode == "sse-c" {
			getOptions = func(in *s3.GetObjectInput) {
				in.SSECustomerAlgorithm = aws.String(sseCAlgorithm)
				in.SSECustomerKey = aws.String(sseCKey)
				in.SSECustomerKeyMD5 = aws.String(sseCKeyMD5)
			}
		}
		assertCopied(t, s.client, targetBucket, target, string(body), getOptions)
		headInput := &s3.HeadObjectInput{Bucket: aws.String(targetBucket), Key: aws.String(target)}
		if targetMode == "sse-c" {
			headInput.SSECustomerAlgorithm = aws.String(sseCAlgorithm)
			headInput.SSECustomerKey = aws.String(sseCKey)
			headInput.SSECustomerKeyMD5 = aws.String(sseCKeyMD5)
		}
		head, err := s.client.HeadObject(context.Background(), headInput)
		if err != nil {
			t.Fatal(err)
		}
		encrypted := targetMode != "normal"
		if encrypted && targetMode != "sse-c" && head.ServerSideEncryption != types.ServerSideEncryptionAes256 {
			t.Fatalf("target encryption=%q", head.ServerSideEncryption)
		}
	}
}

// 삭제된 오브젝트 복사 실패를 확인하는 테스트
func TestCopyToDeletedObject(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t)
	put(t, s, b, "source", "body", nil)
	if _, err := s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(b), Key: aws.String("source")}); err != nil {
		t.Fatal(err)
	}
	_, err := s.client.CopyObject(context.Background(), &s3.CopyObjectInput{Bucket: aws.String(b), Key: aws.String("target"), CopySource: copySource(b, "source", "")})
	assertS3Error(t, err, 404, "NoSuchKey")
}

// 버저닝된 버킷에서 삭제된 오브젝트 복사 실패를 확인하는 테스트
func TestCopyToDeleteMarkerObject(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t)
	enableVersioning(t, s, b)
	put(t, s, b, "source", "body", nil)
	if _, err := s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(b), Key: aws.String("source")}); err != nil {
		t.Fatal(err)
	}
	_, err := s.client.CopyObject(context.Background(), &s3.CopyObjectInput{Bucket: aws.String(b), Key: aws.String("target"), CopySource: copySource(b, "source", "")})
	assertS3Error(t, err, 404, "NoSuchKey")
}

// 버저닝된 버킷에서 copyObject로 덮어쓰기할 경우 메타데이터 덮어쓰기 모드로 메타데이터를 추가 가능한지 확인하는 테스트
func TestObjectVersioningCopyToItselfWithMetadata(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	b := s.bucket(t)
	enableVersioning(t, s, b)
	source := "test_object_versioning_copy_to_itself_with_metadata-source"
	body, contentType := source, "audio/ogg"
	metadata := map[string]string{"source": "value1", "target": "value2"}
	putOut, err := s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(b), Key: aws.String(source), Body: bytes.NewReader([]byte(body)), Metadata: metadata, ContentType: aws.String(contentType)})
	if err != nil {
		t.Fatal(err)
	}
	input := &s3.CopyObjectInput{Bucket: aws.String(b), Key: aws.String(source), CopySource: copySource(b, source, ""), Metadata: map[string]string{"foo": "bar2"}, MetadataDirective: types.MetadataDirectiveReplace}
	if aws.ToString(putOut.VersionId) != "" {
		input.CopySource = copySource(b, source, aws.ToString(putOut.VersionId))
	}
	copyCall(t, s.client, input)
	assertCopied(t, s.client, b, source, body, nil)
	head, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String(source)})
	if err != nil {
		t.Fatal(err)
	}
	if head.Metadata["foo"] != "bar2" {
		t.Fatalf("metadata=%v", head.Metadata)
	}
	listed := listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b), Prefix: aws.String(source)})
	if len(listed.Versions) != 2 {
		t.Fatalf("versions=%d", len(listed.Versions))
	}
}

// copyObject로 덮어쓰기할 경우 메타데이터 덮어쓰기 모드로 메타데이터를 변경 가능한지 확인하는 테스트
func TestObjectCopyToItselfWithMetadataOverwrite(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	b := s.bucket(t)
	source := "test_object_copy_to_itself_with_metadata_overwrite-source"
	body, contentType := source, "audio/ogg"
	metadata := map[string]string{"source": "value1", "target": "value2"}
	if _, err := s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(b), Key: aws.String(source), Body: bytes.NewReader([]byte(body)), Metadata: metadata, ContentType: aws.String(contentType)}); err != nil {
		t.Fatal(err)
	}
	copyCall(t, s.client, &s3.CopyObjectInput{Bucket: aws.String(b), Key: aws.String(source), CopySource: copySource(b, source, ""), Metadata: map[string]string{"foo": "bar2"}, MetadataDirective: types.MetadataDirectiveReplace})
	assertCopied(t, s.client, b, source, body, nil)
	head, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String(source)})
	if err != nil {
		t.Fatal(err)
	}
	if head.Metadata["foo"] != "bar2" {
		t.Fatalf("metadata=%v", head.Metadata)
	}
}

// 버저닝된 버킷에서 copyObject로 덮어쓰기할 경우 메타데이터 덮어쓰기 모드로 메타데이터를 변경 가능한지 확인하는 테스트
func TestObjectVersioningCopyToItselfWithMetadataOverwrite(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	b := s.bucket(t)
	enableVersioning(t, s, b)
	source := "test_object_versioning_copy_to_itself_with_metadata_overwrite-source"
	body, contentType := source, "audio/ogg"
	metadata := map[string]string{"source": "value1", "target": "value2"}
	putOut, err := s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(b), Key: aws.String(source), Body: bytes.NewReader([]byte(body)), Metadata: metadata, ContentType: aws.String(contentType)})
	if err != nil {
		t.Fatal(err)
	}
	input := &s3.CopyObjectInput{Bucket: aws.String(b), Key: aws.String(source), CopySource: copySource(b, source, ""), Metadata: map[string]string{"foo": "bar2"}, MetadataDirective: types.MetadataDirectiveReplace}
	if aws.ToString(putOut.VersionId) != "" {
		input.CopySource = copySource(b, source, aws.ToString(putOut.VersionId))
	}
	copyCall(t, s.client, input)
	assertCopied(t, s.client, b, source, body, nil)
	head, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String(source)})
	if err != nil {
		t.Fatal(err)
	}
	if head.Metadata["foo"] != "bar2" {
		t.Fatalf("metadata=%v", head.Metadata)
	}
	listed := listVersions(t, s.client, &s3.ListObjectVersionsInput{Bucket: aws.String(b), Prefix: aws.String(source)})
	if len(listed.Versions) != 2 {
		t.Fatalf("versions=%d", len(listed.Versions))
	}
}

// sse-c로 암호화된 오브젝트를 복사할때 Algorithm을 누락하면 오류가 발생하는지 확인하는 테스트 SDK V1은 SSE-C 차단 해제(BlockedEncryptionTypes)를 지원하지 않아 V2만 테스트한다.
func TestCopyRevokeSseAlgorithm(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := ssecBucket(t, s)
	if _, err := s.client.PutObject(context.Background(), sseCPutInput(b, "source", []byte("body"))); err != nil {
		t.Fatal(err)
	}
	_, err := s.client.CopyObject(context.Background(), &s3.CopyObjectInput{Bucket: aws.String(b), Key: aws.String("target"), CopySource: copySource(b, "source", ""), CopySourceSSECustomerKey: aws.String(sseCKey), CopySourceSSECustomerKeyMD5: aws.String(sseCKeyMD5)})
	assertHTTPError(t, err, 400)
}

// UseChunkEncoding을 사용하는 오브젝트 복사 시 체크섬 계산 및 검증을 확인하는 테스트
func TestCopyObjectChecksumUseChunkEncoding(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t)
	for _, algorithm := range []types.ChecksumAlgorithm{types.ChecksumAlgorithmCrc32, types.ChecksumAlgorithmCrc32c, types.ChecksumAlgorithmSha1, types.ChecksumAlgorithmSha256} {
		body := []byte(string(algorithm))
		input := &s3.PutObjectInput{Bucket: aws.String(b), Key: aws.String("source-" + string(algorithm)), Body: bytes.NewReader(body), ChecksumAlgorithm: algorithm}
		setPutChecksum(input, algorithm, body, false)
		if _, err := s.client.PutObject(context.Background(), input); err != nil {
			t.Fatal(err)
		}
		out := copyCall(t, s.client, &s3.CopyObjectInput{Bucket: aws.String(b), Key: aws.String("target-" + string(algorithm)), CopySource: copySource(b, "source-"+string(algorithm), "")})
		if out.CopyObjectResult == nil {
			t.Fatal("missing CopyObjectResult")
		}
		assertCopied(t, s.client, b, "target-"+string(algorithm), string(body), nil)
	}
}

// 메타데이터와 태그가 복사되는지 확인하는 테스트
func TestCopyObjectMetadataAndTags(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t)
	source, target := "source", "target"
	if _, err := s.client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(b), Key: aws.String(source), Body: bytes.NewReader([]byte(source)), Metadata: map[string]string{"foo": "bar"}, Tagging: aws.String("tag1=value1")}); err != nil {
		t.Fatal(err)
	}
	copyCall(t, s.client, &s3.CopyObjectInput{Bucket: aws.String(b), Key: aws.String(target), CopySource: copySource(b, source, "")})
	head, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String(target)})
	if err != nil || head.Metadata["foo"] != "bar" {
		t.Fatalf("metadata=%v err=%v", head.Metadata, err)
	}
	tags, err := s.client.GetObjectTagging(context.Background(), &s3.GetObjectTaggingInput{Bucket: aws.String(b), Key: aws.String(target)})
	if err != nil || len(tags.TagSet) != 1 || aws.ToString(tags.TagSet[0].Value) != "value1" {
		t.Fatalf("tags=%v err=%v", tags.TagSet, err)
	}
}

func copySource(bucket, key, version string) *string {
	encKey := strings.ReplaceAll(url.PathEscape(key), "%2F", "/")
	value := url.PathEscape(bucket) + "/" + encKey
	if version != "" {
		value += "?versionId=" + url.QueryEscape(version)
	}
	return aws.String(value)
}

func copyCall(t *testing.T, client *s3.Client, input *s3.CopyObjectInput) *s3.CopyObjectOutput {
	t.Helper()
	out, err := client.CopyObject(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func assertCopied(t *testing.T, client *s3.Client, bucket, key, want string, input func(*s3.GetObjectInput)) {
	t.Helper()
	request := &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}
	if input != nil {
		input(request)
	}
	out, err := client.GetObject(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(out.Body)
	out.Body.Close()
	if err != nil || string(body) != want {
		t.Fatalf("body=%q want=%q err=%v", body, want, err)
	}
}

