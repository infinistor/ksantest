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
	b := s.bucket(t, 1)
	source, target := "TestObjectCopyZeroSizeSource", "TestObjectCopyZeroSizeTarget"
	if _, err := s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(b), Key: aws.String(source), Body: bytes.NewReader([]byte(""))}); err != nil {
		t.Fatal(err)
	}
	copyCall(t, s.client, &s3.CopyObjectInput{Bucket: aws.String(b), Key: aws.String(target), CopySource: copySource(b, source, "")})
	assertCopied(t, s.client, b, target, "", nil)
	head, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String(target)})
	if err != nil || aws.ToInt64(head.ContentLength) != 0 {
		t.Fatalf("ContentLength=%d err=%v", aws.ToInt64(head.ContentLength), err)
	}
}

// 동일한 버킷에서 오브젝트 복사가 가능한지 확인하는 테스트
func TestObjectCopySameBucket(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	b := s.bucket(t, 2)
	source, target := "TestObjectCopySameBucketSource", "TestObjectCopySameBucketTarget"
	if _, err := s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(b), Key: aws.String(source), Body: bytes.NewReader([]byte(source))}); err != nil {
		t.Fatal(err)
	}
	copyCall(t, s.client, &s3.CopyObjectInput{Bucket: aws.String(b), Key: aws.String(target), CopySource: copySource(b, source, "")})
	assertCopied(t, s.client, b, target, source, nil)
	if _, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String(target)}); err != nil {
		t.Fatal(err)
	}
}

// ContentType을 설정한 오브젝트를 복사할 경우 복사된 오브젝트도 ContentType값이 일치하는지 확인하는 테스트
func TestObjectCopyVerifyContentType(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	b := s.bucket(t, 3)
	source, target := "TestObjectCopyVerifyContentTypeSource", "TestObjectCopyVerifyContentTypeTarget"
	contentType := "audio/ogg"
	metadata := map[string]string{"source": "value1", "target": "value2"}
	if _, err := s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(b), Key: aws.String(source), Body: bytes.NewReader([]byte(source)), Metadata: metadata, ContentType: aws.String(contentType)}); err != nil {
		t.Fatal(err)
	}
	copyCall(t, s.client, &s3.CopyObjectInput{Bucket: aws.String(b), Key: aws.String(target), CopySource: copySource(b, source, "")})
	assertCopied(t, s.client, b, target, source, nil)
	head, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String(target)})
	if err != nil {
		t.Fatal(err)
	}
	if aws.ToString(head.ContentType) != contentType {
		t.Fatalf("ContentType=%q", aws.ToString(head.ContentType))
	}
	if len(head.Metadata) != len(metadata) || head.Metadata["source"] != "value1" || head.Metadata["target"] != "value2" {
		t.Fatalf("metadata=%v", head.Metadata)
	}
}

// 복사할 오브젝트와 복사될 오브젝트의 경로가 같을 경우 에러를 확인하는 테스트
func TestObjectCopyToItself(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	b := s.bucket(t, 4)
	source := "TestObjectCopyToItselfSource"
	if _, err := s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(b), Key: aws.String(source), Body: bytes.NewReader([]byte(source))}); err != nil {
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
	b := s.bucket(t, 5)
	source := "TestObjectCopyToItselfWithMetadataSource"
	if _, err := s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(b), Key: aws.String(source), Body: bytes.NewReader([]byte(source))}); err != nil {
		t.Fatal(err)
	}
	copyCall(t, s.client, &s3.CopyObjectInput{Bucket: aws.String(b), Key: aws.String(source), CopySource: copySource(b, source, ""), Metadata: map[string]string{"foo": "bar"}, MetadataDirective: types.MetadataDirectiveReplace})
	assertCopied(t, s.client, b, source, source, nil)
	head, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String(source)})
	if err != nil {
		t.Fatal(err)
	}
	if len(head.Metadata) != 1 || head.Metadata["foo"] != "bar" {
		t.Fatalf("metadata=%v", head.Metadata)
	}
}

// 다른 버킷으로 오브젝트 복사가 가능한지 확인하는 테스트
func TestObjectCopyDiffBucket(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	sourceBucket, targetBucket := s.bucket(t, 6), s.bucket(t, 6)
	source, target := "TestObjectCopyDiffBucketSource", "TestObjectCopyDiffBucketTarget"
	if _, err := s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(sourceBucket), Key: aws.String(source), Body: bytes.NewReader([]byte(source))}); err != nil {
		t.Fatal(err)
	}
	copyCall(t, s.client, &s3.CopyObjectInput{Bucket: aws.String(targetBucket), Key: aws.String(target), CopySource: copySource(sourceBucket, source, "")})
	assertCopied(t, s.client, targetBucket, target, source, nil)
	if _, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(targetBucket), Key: aws.String(target)}); err != nil {
		t.Fatal(err)
	}
}

// [bucket1:created main user, object:created main user / bucket2:created sub user] 메인유저가 만든 버킷, 오브젝트를 서브유저가 만든 버킷으로 오브젝트 복사가 불가능한지 확인하는 테스트
func TestObjectCopyNotOwnedBucket(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	requireAltUser(t, s)
	ctx := context.Background()
	b := s.bucket(t, 7)
	source := "TestObjectCopyNotOwnedBucketSource"
	if _, err := s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(b), Key: aws.String(source), Body: bytes.NewReader([]byte(source))}); err != nil {
		t.Fatal(err)
	}
	alt := s3Client(s.cfg, s.cfg.Alt)
	altBucket := strings.ToLower("copy-alt-" + uniqueBucketSuffix(t))
	if _, err := alt.CreateBucket(ctx, createBucketInput(s.cfg, altBucket)); err != nil {
		t.Fatal(err)
	}
	target := "TestObjectCopyNotOwnedBucketTarget"
	t.Cleanup(func() {
		if !s.cfg.NotDelete {
			_, _ = alt.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(altBucket), Key: aws.String(target)})
			_, _ = alt.DeleteBucket(context.Background(), &s3.DeleteBucketInput{Bucket: aws.String(altBucket)})
		}
	})
	_, err := alt.CopyObject(ctx, &s3.CopyObjectInput{Bucket: aws.String(altBucket), Key: aws.String(target), CopySource: copySource(b, source, "")})
	assertS3Error(t, err, 403, "AccessDenied")
}

// 다른유저의 버킷의 오브젝트를 권한이 충분할 경우 복사 가능한지 확인하는 테스트
func TestObjectCopyNotOwnedObjectBucket(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	requireAltUser(t, s)
	ctx := context.Background()
	b := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 8)
	source, target := "TestObjectCopyNotOwnedObjectBucketSource", "TestObjectCopyNotOwnedObjectBucketTarget"
	if _, err := s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(b), Key: aws.String(source), Body: bytes.NewReader([]byte(source))}); err != nil {
		t.Fatal(err)
	}
	policy := aclPolicy(s, types.PermissionFullControl)
	if _, err := s.client.PutBucketAcl(ctx, &s3.PutBucketAclInput{Bucket: aws.String(b), AccessControlPolicy: policy}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.client.PutObjectAcl(ctx, &s3.PutObjectAclInput{Bucket: aws.String(b), Key: aws.String(source), AccessControlPolicy: policy}); err != nil {
		t.Fatal(err)
	}
	alt := s3Client(s.cfg, s.cfg.Alt)
	assertCopied(t, alt, b, source, source, nil)
	copyCall(t, alt, &s3.CopyObjectInput{Bucket: aws.String(b), Key: aws.String(target), CopySource: copySource(b, source, "")})
	assertCopied(t, alt, b, target, source, nil)
}

// 권한정보를 포함하여 복사할때 올바르게 적용되는지 확인하는 테스트
func TestObjectCopyCannedAcl(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	requireAltUser(t, s)
	ctx := context.Background()
	b := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 9)
	source, target := "TestObjectCopyCannedAclSource", "TestObjectCopyCannedAclTarget"
	if _, err := s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(b), Key: aws.String(source), Body: bytes.NewReader([]byte(source))}); err != nil {
		t.Fatal(err)
	}
	copyCall(t, s.client, &s3.CopyObjectInput{Bucket: aws.String(b), Key: aws.String(target), CopySource: copySource(b, source, ""), ACL: types.ObjectCannedACLPublicRead})
	alt := s3Client(s.cfg, s.cfg.Alt)
	assertCopied(t, alt, b, target, source, nil)

	metadata := map[string]string{"abc": "def"}
	copyCall(t, s.client, &s3.CopyObjectInput{
		Bucket:            aws.String(b),
		Key:               aws.String(source),
		CopySource:        copySource(b, target, ""),
		ACL:               types.ObjectCannedACLPublicRead,
		Metadata:          metadata,
		MetadataDirective: types.MetadataDirectiveReplace,
	})
	assertCopied(t, alt, b, source, source, nil)
	head, err := alt.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String(source)})
	if err != nil || len(head.Metadata) != 1 || head.Metadata["abc"] != "def" {
		t.Fatalf("metadata=%v err=%v", head.Metadata, err)
	}
}

// 크고 작은 용량의 오브젝트가 복사되는지 확인하는 테스트
func TestObjectCopyRetainingMetadata(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	for _, size := range []int{3, 1024 * 1024} {
		b := s.bucket(t, 10)
		source, target := "TestObjectCopyRetainingMetadataSource", "TestObjectCopyRetainingMetadataTarget"
		body, contentType := randomTextToLong(size), "audio/ogg"
		metadata := map[string]string{"source": "value1", "target": "value2"}
		if _, err := s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(b), Key: aws.String(source), Body: bytes.NewReader(body), Metadata: metadata, ContentType: aws.String(contentType), ContentLength: aws.Int64(int64(size))}); err != nil {
			t.Fatal(err)
		}
		copyCall(t, s.client, &s3.CopyObjectInput{Bucket: aws.String(b), Key: aws.String(target), CopySource: copySource(b, source, "")})
		assertObjectBytes(t, s.client, b, target, body)
		head, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String(target)})
		if err != nil || aws.ToString(head.ContentType) != contentType || aws.ToInt64(head.ContentLength) != int64(size) || len(head.Metadata) != len(metadata) || head.Metadata["source"] != "value1" || head.Metadata["target"] != "value2" {
			t.Fatalf("head=%#v err=%v", head, err)
		}
	}
}

// 크고 작은 용량의 오브젝트및 메타데이터가 복사되는지 확인하는 테스트
func TestObjectCopyReplacingMetadata(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	for _, size := range []int{3, 1024 * 1024} {
		b := s.bucket(t, 11)
		source, target := "TestObjectCopyReplacingMetadataSource", "TestObjectCopyReplacingMetadataTarget"
		body, contentType := randomTextToLong(size), "audio/ogg"
		metadata := map[string]string{"source": "value1", "target": "value2"}
		if _, err := s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(b), Key: aws.String(source), Body: bytes.NewReader(body), Metadata: metadata, ContentType: aws.String(contentType), ContentLength: aws.Int64(int64(size))}); err != nil {
			t.Fatal(err)
		}
		sourceHead, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String(source)})
		if err != nil || aws.ToString(sourceHead.ContentType) != contentType || len(sourceHead.Metadata) != len(metadata) || sourceHead.Metadata["source"] != "value1" || sourceHead.Metadata["target"] != "value2" {
			t.Fatalf("source head=%#v err=%v", sourceHead, err)
		}
		replacement := map[string]string{"key3": "value3", "key4": "value4"}
		copyCall(t, s.client, &s3.CopyObjectInput{Bucket: aws.String(b), Key: aws.String(target), CopySource: copySource(b, source, ""), Metadata: replacement, MetadataDirective: types.MetadataDirectiveReplace, ContentType: aws.String(contentType)})
		assertObjectBytes(t, s.client, b, target, body)
		head, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String(target)})
		if err != nil || aws.ToString(head.ContentType) != contentType || aws.ToInt64(head.ContentLength) != int64(size) || len(head.Metadata) != len(replacement) || head.Metadata["key3"] != "value3" || head.Metadata["key4"] != "value4" {
			t.Fatalf("target head=%#v err=%v", head, err)
		}
	}
}

// 존재하지 않는 버킷에서 존재하지 않는 오브젝트 복사 실패를 확인하는 테스트
func TestObjectCopyBucketNotFound(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	b := s.bucket(t, 12)
	source := "TestObjectCopyBucketNotFoundSource"
	if _, err := s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(b), Key: aws.String(source), Body: bytes.NewReader([]byte(source))}); err != nil {
		t.Fatal(err)
	}
	_, err := s.client.CopyObject(ctx, &s3.CopyObjectInput{Bucket: aws.String(b), Key: aws.String("TestObjectCopyBucketNotFoundTarget"), CopySource: copySource(b+"-fake", source, "")})
	assertS3Error(t, err, 404, "NoSuchBucket")
}

// 존재하지않는 오브젝트 복사 실패를 확인하는 테스트
func TestObjectCopyKeyNotFound(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	b := s.bucket(t, 13)
	source := "TestObjectCopyKeyNotFoundSource"
	if _, err := s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(b), Key: aws.String(source), Body: bytes.NewReader([]byte(source))}); err != nil {
		t.Fatal(err)
	}
	_, err := s.client.CopyObject(ctx, &s3.CopyObjectInput{Bucket: aws.String(b), Key: aws.String("TestObjectCopyKeyNotFoundTarget"), CopySource: copySource(b, "missing", "")})
	assertS3Error(t, err, 404, "NoSuchKey")
}

// 버저닝된 오브젝트 복사를 확인하는 테스트
func TestObjectCopyVersioningBucket(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	b := s.bucket(t, 14)
	size := 5 * 1024
	data := randomTextToLong(size)
	source := "TestObjectCopyVersioningBucketSource"
	target := "TestObjectCopyVersioningBucketTarget"

	enableVersioning(t, s, b)

	putOut, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(b),
		Key:           aws.String(source),
		Body:          bytes.NewReader(data),
		ContentLength: aws.Int64(int64(size)),
	})
	if err != nil {
		t.Fatal(err)
	}
	sourceVersion := aws.ToString(putOut.VersionId)
	if sourceVersion == "" {
		t.Fatal("missing source VersionId")
	}

	copyOut := copyCall(t, s.client, &s3.CopyObjectInput{
		Bucket:     aws.String(b),
		Key:        aws.String(target),
		CopySource: copySource(b, source, sourceVersion),
	})
	targetVersion := aws.ToString(copyOut.VersionId)
	if targetVersion == "" {
		t.Fatal("missing target VersionId")
	}

	assertCopy := func(bucket, key string) {
		t.Helper()
		assertObjectBytes(t, s.client, bucket, key, data)
		head, headErr := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
		if headErr != nil || aws.ToInt64(head.ContentLength) != int64(size) {
			t.Fatalf("%s/%s contentLength=%d want=%d err=%v", bucket, key, aws.ToInt64(head.ContentLength), size, headErr)
		}
	}

	assertCopy(b, target)

	target2 := "TestObjectCopyVersioningBucketTarget2"
	copyCall(t, s.client, &s3.CopyObjectInput{
		Bucket:     aws.String(b),
		Key:        aws.String(target2),
		CopySource: copySource(b, target, targetVersion),
	})
	assertCopy(b, target2)

	targetBucket := s.bucket(t, 14)
	enableVersioning(t, s, targetBucket)
	target3 := "TestObjectCopyVersioningBucketTarget3"
	copyCall(t, s.client, &s3.CopyObjectInput{
		Bucket:     aws.String(targetBucket),
		Key:        aws.String(target3),
		CopySource: copySource(b, source, sourceVersion),
	})
	assertCopy(targetBucket, target3)

	bucketName3 := s.bucket(t, 14)
	enableVersioning(t, s, bucketName3)
	target4 := "TestObjectCopyVersioningBucketTarget4"
	copyCall(t, s.client, &s3.CopyObjectInput{
		Bucket:     aws.String(bucketName3),
		Key:        aws.String(target4),
		CopySource: copySource(b, source, sourceVersion),
	})
	assertCopy(bucketName3, target4)

	target5 := "TestObjectCopyVersioningBucketTarget5"
	copyCall(t, s.client, &s3.CopyObjectInput{
		Bucket:     aws.String(b),
		Key:        aws.String(target5),
		CopySource: copySource(bucketName3, target4, ""),
	})
	assertCopy(b, target5)
}

// [버킷이 버저닝 가능하고 오브젝트이름에 특수문자가 들어갔을 경우] 오브젝트 복사 성공을 확인하는 테스트
func TestObjectCopyVersioningUrlEncoding(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	b := s.bucket(t, 15)
	enableVersioning(t, s, b)
	source := "TestObjectCopyVersioningUrlEncoding?Source"
	target := "TestObjectCopyVersioningUrlEncoding&Target"
	body := randomTextToLong(1024)
	putOut, err := s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(b), Key: aws.String(source), Body: bytes.NewReader(body)})
	if err != nil {
		t.Fatal(err)
	}
	input := &s3.CopyObjectInput{Bucket: aws.String(b), Key: aws.String(target), CopySource: copySource(b, source, "")}
	if aws.ToString(putOut.VersionId) != "" {
		input.CopySource = copySource(b, source, aws.ToString(putOut.VersionId))
	}
	copyCall(t, s.client, input)
	assertObjectBytes(t, s.client, b, target, body)
	if _, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String(target)}); err != nil {
		t.Fatal(err)
	}
}

// [버킷에 버저닝 설정] 멀티파트로 업로드된 오브젝트 복사를 확인하는 테스트
func TestObjectCopyVersioningMultipartUpload(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	sourceBucket := s.bucket(t, 16)
	enableVersioning(t, s, sourceBucket)
	body := randomTextToLong(50 * 1024 * 1024)
	metadata := map[string]string{"foo": "bar"}
	completeMultipart(t, s.client, sourceBucket, "source", body, false, metadata)
	head, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(sourceBucket), Key: aws.String("source")})
	if err != nil {
		t.Fatal(err)
	}
	sourceVersion := aws.ToString(head.VersionId)
	if sourceVersion == "" {
		t.Fatal("missing source VersionId")
	}
	assertTarget := func(bucket, key string) *s3.HeadObjectOutput {
		t.Helper()
		out, headErr := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
		if headErr != nil || aws.ToInt64(out.ContentLength) != int64(len(body)) || len(out.Metadata) != len(metadata) || out.Metadata["foo"] != "bar" || aws.ToString(out.VersionId) == "" {
			t.Fatalf("%s/%s head=%#v err=%v", bucket, key, out, headErr)
		}
		assertObjectBytes(t, s.client, bucket, key, body)
		return out
	}

	copyCall(t, s.client, &s3.CopyObjectInput{Bucket: aws.String(sourceBucket), Key: aws.String("target"), CopySource: copySource(sourceBucket, "source", sourceVersion)})
	targetHead := assertTarget(sourceBucket, "target")
	copyCall(t, s.client, &s3.CopyObjectInput{Bucket: aws.String(sourceBucket), Key: aws.String("target-2"), CopySource: copySource(sourceBucket, "target", aws.ToString(targetHead.VersionId))})
	assertTarget(sourceBucket, "target-2")

	targetBucket := s.bucket(t, 16)
	enableVersioning(t, s, targetBucket)
	copyCall(t, s.client, &s3.CopyObjectInput{Bucket: aws.String(targetBucket), Key: aws.String("target-3"), CopySource: copySource(sourceBucket, "source", sourceVersion)})
	assertTarget(targetBucket, "target-3")

	thirdBucket := s.bucket(t, 16)
	enableVersioning(t, s, thirdBucket)
	copyCall(t, s.client, &s3.CopyObjectInput{Bucket: aws.String(thirdBucket), Key: aws.String("target-4"), CopySource: copySource(sourceBucket, "source", sourceVersion)})
	assertTarget(thirdBucket, "target-4")

	copyCall(t, s.client, &s3.CopyObjectInput{Bucket: aws.String(sourceBucket), Key: aws.String("target-5"), CopySource: copySource(thirdBucket, "target-4", "")})
	assertTarget(sourceBucket, "target-5")
}

// ifMatch 값을 추가하여 오브젝트를 복사할 경우 성공을 확인하는 테스트
func TestCopyObjectIfMatchGood(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t, 17)
	source, target := "TestCopyObjectIfMatchGoodSource", "TestCopyObjectIfMatchGoodTarget"
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
	b := s.bucket(t, 18)
	source, target := "TestCopyObjectIfMatchFailedSource", "TestCopyObjectIfMatchFailedTarget"
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
	b := s.bucket(t, 19)
	source, target := "TestCopyObjectIfNoneMatchGoodSource", "TestCopyObjectIfNoneMatchGoodTarget"
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
	b := s.bucket(t, 20)
	source, target := "TestCopyObjectIfNoneMatchFailedSource", "TestCopyObjectIfNoneMatchFailedTarget"
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
	b := s.bucket(t, 21)
	source, target := "TestCopyObjectIfModifiedSinceGoodSource", "TestCopyObjectIfModifiedSinceGoodTarget"
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
	b := s.bucket(t, 22)
	source, target := "TestCopyObjectIfModifiedSinceFailedSource", "TestCopyObjectIfModifiedSinceFailedTarget"
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
	b := s.bucket(t, 23)
	source, target := "TestCopyObjectIfUnmodifiedSinceGoodSource", "TestCopyObjectIfUnmodifiedSinceGoodTarget"
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
	b := s.bucket(t, 24)
	source, target := "TestCopyObjectIfUnmodifiedSinceFailedSource", "TestCopyObjectIfUnmodifiedSinceFailedTarget"
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
	b := s.bucket(t, 25)
	source, target := "TestCopyObjectIfMatchWithIfUnmodifiedSinceSource", "TestCopyObjectIfMatchWithIfUnmodifiedSinceTarget"
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
	b := s.bucket(t, 26)
	source, target := "TestCopyObjectIfNoneMatchWithIfModifiedSinceSource", "TestCopyObjectIfNoneMatchWithIfModifiedSinceTarget"
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
	b := s.bucket(t, 27)
	source, target := "TestCopyObjectIfMatchAndIfNoneMatchSource", "TestCopyObjectIfMatchAndIfNoneMatchTarget"
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
	b := s.bucket(t, 28)
	source, target := "TestCopyObjectIfMatchAndIfNoneMatchAnySource", "TestCopyObjectIfMatchAndIfNoneMatchAnyTarget"
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
	b := s.bucket(t, 29)
	source, target := "TestCopyObjectDestinationIfMatchGoodSource", "TestCopyObjectDestinationIfMatchGoodTarget"
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
	b := s.bucket(t, 30)
	source, target := "TestCopyObjectDestinationIfMatchFailedSource", "TestCopyObjectDestinationIfMatchFailedTarget"
	put(t, s, b, source, source, nil)
	put(t, s, b, target, "old", nil)
	input := &s3.CopyObjectInput{Bucket: aws.String(b), Key: aws.String(target), CopySource: copySource(b, source, ""), IfMatch: aws.String("bad")}
	_, err := s.client.CopyObject(context.Background(), input)
	assertHTTPError(t, err, 412)
	assertCopied(t, s.client, b, target, "old", nil)
}

// 존재하지 않는 대상 키에 If-None-Match: * 조건으로 복사 성공 확인
func TestCopyObjectDestinationIfNoneMatchGood(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t, 31)
	source, target := "TestCopyObjectDestinationIfNoneMatchGoodSource", "TestCopyObjectDestinationIfNoneMatchGoodTargetNew"
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
	b := s.bucket(t, 32)
	source, target := "TestCopyObjectDestinationIfNoneMatchFailedSource", "TestCopyObjectDestinationIfNoneMatchFailedTarget"
	put(t, s, b, source, source, nil)
	put(t, s, b, target, "old", nil)
	input := &s3.CopyObjectInput{Bucket: aws.String(b), Key: aws.String(target), CopySource: copySource(b, source, ""), IfNoneMatch: aws.String("*")}
	_, err := s.client.CopyObject(context.Background(), input)
	assertHTTPError(t, err, 412)
	assertCopied(t, s.client, b, target, "old", nil)
}

// 대상에 If-Match와 If-None-Match를 함께 지정하면 501로 거부되는지 확인
func TestCopyObjectDestinationIfMatchAndIfNoneMatch(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t, 33)
	source, target := "TestCopyObjectDestinationIfMatchAndIfNoneMatchSource", "TestCopyObjectDestinationIfMatchAndIfNoneMatchTarget"
	put(t, s, b, source, source, nil)
	dst := put(t, s, b, target, "old", nil)
	input := &s3.CopyObjectInput{Bucket: aws.String(b), Key: aws.String(target), CopySource: copySource(b, source, ""), IfMatch: dst.ETag, IfNoneMatch: aws.String("*")}
	_, err := s.client.CopyObject(context.Background(), input)
	assertHTTPError(t, err, 501)
	assertCopied(t, s.client, b, target, "old", nil)
}

// 대상에 If-Match와 If-None-Match: * 를 함께 지정하면 501로 거부되는지 확인
func TestCopyObjectDestinationIfMatchAndIfNoneMatchAny(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t, 34)
	source, target := "TestCopyObjectDestinationIfMatchAndIfNoneMatchAnySource", "TestCopyObjectDestinationIfMatchAndIfNoneMatchAnyTarget"
	put(t, s, b, source, source, nil)
	dst := put(t, s, b, target, "old", nil)
	input := &s3.CopyObjectInput{Bucket: aws.String(b), Key: aws.String(target), CopySource: copySource(b, source, ""), IfMatch: dst.ETag, IfNoneMatch: dst.ETag}
	_, err := s.client.CopyObject(context.Background(), input)
	assertHTTPError(t, err, 501)
	assertCopied(t, s.client, b, target, "old", nil)
}

// 소스 If-Match와 대상 If-None-Match: * 를 함께 사용해 복사 성공 확인
func TestCopyObjectSourceIfMatchWithDestinationIfNoneMatch(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t, 35)
	source, target := "TestCopyObjectSourceIfMatchWithDestinationIfNoneMatchSource", "TestCopyObjectSourceIfMatchWithDestinationIfNoneMatchTargetNew"
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
	testCopyObjectEncryption(t, false, false, false, false, 1024, 36)
	testCopyObjectEncryption(t, false, false, false, false, 256*1024, 36)
	testCopyObjectEncryption(t, false, false, false, false, 1024*1024, 36)
}

// [source obj : normal, dest bucket : normal, dest obj : encryption] 오브젝트 복사 성공을 확인하는 테스트
func TestCopyNorSrcToNorBucketEncryptionObj(t *testing.T) {
	t.Parallel()
	testCopyObjectEncryption(t, false, false, false, true, 1024, 37)
	testCopyObjectEncryption(t, false, false, false, true, 256*1024, 37)
	testCopyObjectEncryption(t, false, false, false, true, 1024*1024, 37)
}

// [source obj : normal, dest bucket : encryption, dest obj : normal] 오브젝트 복사 성공을 확인하는 테스트
func TestCopyNorSrcToEncryptionBucketNorObj(t *testing.T) {
	t.Parallel()
	testCopyObjectEncryption(t, false, false, true, false, 1024, 38)
	testCopyObjectEncryption(t, false, false, true, false, 256*1024, 38)
	testCopyObjectEncryption(t, false, false, true, false, 1024*1024, 38)
}

// [source obj : normal, dest bucket : encryption, dest obj : encryption] 오브젝트 복사 성공을 확인하는 테스트
func TestCopyNorSrcToEncryptionBucketAndObj(t *testing.T) {
	t.Parallel()
	testCopyObjectEncryption(t, false, false, true, true, 1024, 39)
	testCopyObjectEncryption(t, false, false, true, true, 256*1024, 39)
	testCopyObjectEncryption(t, false, false, true, true, 1024*1024, 39)
}

// [source obj : encryption, dest bucket : normal, dest obj : normal] 오브젝트 복사 성공을 확인하는 테스트
func TestCopyEncryptionSrcToNorBucketAndObj(t *testing.T) {
	t.Parallel()
	testCopyObjectEncryption(t, true, false, false, false, 1024, 40)
	testCopyObjectEncryption(t, true, false, false, false, 256*1024, 40)
	testCopyObjectEncryption(t, true, false, false, false, 1024*1024, 40)
}

// [source obj : encryption, dest bucket : normal, dest obj : encryption] 오브젝트 복사 성공을 확인하는 테스트
func TestCopyEncryptionSrcToNorBucketEncryptionObj(t *testing.T) {
	t.Parallel()
	testCopyObjectEncryption(t, true, false, false, true, 1024, 41)
	testCopyObjectEncryption(t, true, false, false, true, 256*1024, 41)
	testCopyObjectEncryption(t, true, false, false, true, 1024*1024, 41)
}

// [source obj : encryption, dest bucket : encryption, dest obj : normal] 오브젝트 복사 성공을 확인하는 테스트
func TestCopyEncryptionSrcToEncryptionBucketNorObj(t *testing.T) {
	t.Parallel()
	testCopyObjectEncryption(t, true, false, true, false, 1024, 42)
	testCopyObjectEncryption(t, true, false, true, false, 256*1024, 42)
	testCopyObjectEncryption(t, true, false, true, false, 1024*1024, 42)
}

// [source obj : encryption, dest bucket : encryption, dest obj : encryption] 오브젝트 복사 성공을 확인하는 테스트
func TestCopyEncryptionSrcToEncryptionBucketAndObj(t *testing.T) {
	t.Parallel()
	testCopyObjectEncryption(t, true, false, true, true, 1024, 43)
	testCopyObjectEncryption(t, true, false, true, true, 256*1024, 43)
	testCopyObjectEncryption(t, true, false, true, true, 1024*1024, 43)
}

// [source bucket : encryption, source obj : normal, dest bucket : normal, dest obj : normal] 오브젝트 복사 성공을 확인하는 테스트
func TestCopyEncryptionBucketNorObjToNorBucketAndObj(t *testing.T) {
	t.Parallel()
	testCopyObjectEncryption(t, false, true, false, false, 1024, 44)
	testCopyObjectEncryption(t, false, true, false, false, 256*1024, 44)
	testCopyObjectEncryption(t, false, true, false, false, 1024*1024, 44)
}

// [source bucket : encryption, source obj : normal, dest bucket : normal, dest obj : encryption] 오브젝트 복사 성공을 확인하는 테스트
func TestCopyEncryptionBucketNorObjToNorBucketEncryptionObj(t *testing.T) {
	t.Parallel()
	testCopyObjectEncryption(t, false, true, false, true, 1024, 45)
	testCopyObjectEncryption(t, false, true, false, true, 256*1024, 45)
	testCopyObjectEncryption(t, false, true, false, true, 1024*1024, 45)
}

// [source bucket : encryption, source obj : normal, dest bucket : encryption, dest obj : normal] 오브젝트 복사 성공을 확인하는 테스트
func TestCopyEncryptionBucketNorObjToEncryptionBucketNorObj(t *testing.T) {
	t.Parallel()
	testCopyObjectEncryption(t, false, true, true, false, 1024, 46)
	testCopyObjectEncryption(t, false, true, true, false, 256*1024, 46)
	testCopyObjectEncryption(t, false, true, true, false, 1024*1024, 46)
}

// [source bucket : encryption, source obj : normal, dest bucket : encryption, dest obj : encryption] 오브젝트 복사 성공을 확인하는 테스트
func TestCopyEncryptionBucketNorObjToEncryptionBucketAndObj(t *testing.T) {
	t.Parallel()
	testCopyObjectEncryption(t, false, true, true, true, 1024, 47)
	testCopyObjectEncryption(t, false, true, true, true, 256*1024, 47)
	testCopyObjectEncryption(t, false, true, true, true, 1024*1024, 47)
}

// [source bucket : encryption, source obj : encryption, dest bucket : normal, dest obj : normal] 오브젝트 복사 성공을 확인하는 테스트
func TestCopyEncryptionBucketAndObjToNorBucketAndObj(t *testing.T) {
	t.Parallel()
	testCopyObjectEncryption(t, true, true, false, false, 1024, 48)
	testCopyObjectEncryption(t, true, true, false, false, 256*1024, 48)
	testCopyObjectEncryption(t, true, true, false, false, 1024*1024, 48)
}

// [source bucket : encryption, source obj : encryption, dest bucket : normal, dest obj : encryption] 오브젝트 복사 성공을 확인하는 테스트
func TestCopyEncryptionBucketAndObjToNorBucketEncryptionObj(t *testing.T) {
	t.Parallel()
	testCopyObjectEncryption(t, true, true, false, true, 1024, 49)
	testCopyObjectEncryption(t, true, true, false, true, 256*1024, 49)
	testCopyObjectEncryption(t, true, true, false, true, 1024*1024, 49)
}

// [source bucket : encryption, source obj : encryption, dest bucket : encryption, dest obj : normal] 오브젝트 복사 성공을 확인하는 테스트
func TestCopyEncryptionBucketAndObjToEncryptionBucketNorObj(t *testing.T) {
	t.Parallel()
	testCopyObjectEncryption(t, true, true, true, false, 1024, 50)
	testCopyObjectEncryption(t, true, true, true, false, 256*1024, 50)
	testCopyObjectEncryption(t, true, true, true, false, 1024*1024, 50)
}

// [source bucket : encryption, source obj : encryption, dest bucket : encryption, dest obj : encryption] 오브젝트 복사 성공을 확인하는 테스트
func TestCopyEncryptionBucketAndObjToEncryptionBucketAndObj(t *testing.T) {
	t.Parallel()
	testCopyObjectEncryption(t, true, true, true, true, 1024, 51)
	testCopyObjectEncryption(t, true, true, true, true, 256*1024, 51)
	testCopyObjectEncryption(t, true, true, true, true, 1024*1024, 51)
}

func testCopyObjectEncryption(t *testing.T, sourceObjectEncryption, sourceBucketEncryption, targetBucketEncryption, targetObjectEncryption bool, size int, id ...int) {
	t.Helper()
	s := newSuite(t)
	sourceBucket, targetBucket := s.bucket(t, id...), s.bucket(t, id...)
	source, target := "source", "target"
	body := bytes.Repeat([]byte("a"), size)

	if sourceBucketEncryption {
		putAESBucketEncryption(t, s.client, sourceBucket)
		getAndAssertAESBucketEncryption(t, s.client, sourceBucket)
	}
	if targetBucketEncryption {
		putAESBucketEncryption(t, s.client, targetBucket)
		getAndAssertAESBucketEncryption(t, s.client, targetBucket)
	}

	putInput := &s3.PutObjectInput{Bucket: aws.String(sourceBucket), Key: aws.String(source), Body: bytes.NewReader(body)}
	if sourceObjectEncryption {
		putInput.ServerSideEncryption = types.ServerSideEncryptionAes256
	}
	if _, err := s.client.PutObject(context.Background(), putInput); err != nil {
		t.Fatal(err)
	}
	assertCopyObjectEncryption(t, s, sourceBucket, source, sourceObjectEncryption || sourceBucketEncryption)

	copyInput := &s3.CopyObjectInput{Bucket: aws.String(targetBucket), Key: aws.String(target), CopySource: copySource(sourceBucket, source, "")}
	if targetObjectEncryption {
		copyInput.ServerSideEncryption = types.ServerSideEncryptionAes256
	}
	copyCall(t, s.client, copyInput)
	assertCopied(t, s.client, targetBucket, target, string(body), nil)
	assertCopyObjectEncryption(t, s, targetBucket, target, targetBucketEncryption || targetObjectEncryption)
}

func assertCopyObjectEncryption(t *testing.T, s *suite, bucket, key string, encrypted bool) {
	t.Helper()
	head, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}
	// AWS는 모든 신규 오브젝트에 SSE-S3 기본 암호화를 적용한다.
	if s.cfg.Endpoint() == "" {
		encrypted = true
	}
	if encrypted != (head.ServerSideEncryption == types.ServerSideEncryptionAes256) {
		t.Fatalf("%s/%s encryption=%q, want encrypted=%t", bucket, key, head.ServerSideEncryption, encrypted)
	}
}

// 일반 오브젝트에서 다양한 방식으로 복사 성공을 확인하는 테스트
func TestCopyToNormalSource(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	for _, targetMode := range []string{"normal", "sse-s3", "sse-c"} {
		for _, size := range []int{1024, 256 * 1024, 1024 * 1024} {
			sourceBucket, targetBucket := s.bucket(t, 52), s.bucket(t, 52)
			if targetMode == "sse-c" {
				unblockSseC(t, s, targetBucket)
			}
			source, target := "source", "target"
			body := randomTextToLong(size)
			putInput := &s3.PutObjectInput{Bucket: aws.String(sourceBucket), Key: aws.String(source), Body: bytes.NewReader(body)}
			if _, err := s.client.PutObject(context.Background(), putInput); err != nil {
				t.Fatal(err)
			}
			copyInput := &s3.CopyObjectInput{Bucket: aws.String(targetBucket), Key: aws.String(target), CopySource: copySource(sourceBucket, source, ""), MetadataDirective: types.MetadataDirectiveReplace}
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
}

// SSE-S3암호화 된 오브젝트에서 다양한 방식으로 복사 성공을 확인하는 테스트
func TestCopyToSseS3Source(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	for _, targetMode := range []string{"normal", "sse-s3", "sse-c"} {
		for _, size := range []int{1024, 256 * 1024, 1024 * 1024} {
			sourceBucket, targetBucket := s.bucket(t, 53), s.bucket(t, 53)
			if targetMode == "sse-c" {
				unblockSseC(t, s, targetBucket)
			}
			source, target := "source", "target"
			body := randomTextToLong(size)
			putInput := &s3.PutObjectInput{Bucket: aws.String(sourceBucket), Key: aws.String(source), Body: bytes.NewReader(body), ServerSideEncryption: types.ServerSideEncryptionAes256}
			if _, err := s.client.PutObject(context.Background(), putInput); err != nil {
				t.Fatal(err)
			}
			copyInput := &s3.CopyObjectInput{Bucket: aws.String(targetBucket), Key: aws.String(target), CopySource: copySource(sourceBucket, source, ""), MetadataDirective: types.MetadataDirectiveReplace}
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
}

// SSE-C암호화 된 오브젝트에서 다양한 방식으로 복사 성공을 확인하는 테스트 SDK V1은 SSE-C 차단 해제(BlockedEncryptionTypes)를 지원하지 않아 V2만 테스트한다.
func TestCopyToSseCSource(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	for _, targetMode := range []string{"normal", "sse-s3", "sse-c"} {
		for _, size := range []int{1024, 256 * 1024, 1024 * 1024} {
			sourceBucket, targetBucket := s.bucket(t, 54), s.bucket(t, 54)
			unblockSseC(t, s, sourceBucket)
			if targetMode == "sse-c" {
				unblockSseC(t, s, targetBucket)
			}
			source, target := "source", "target"
			body := randomTextToLong(size)
			putInput := sseCPutInput(sourceBucket, source, body)
			if _, err := s.client.PutObject(context.Background(), putInput); err != nil {
				t.Fatal(err)
			}
			copyInput := &s3.CopyObjectInput{Bucket: aws.String(targetBucket), Key: aws.String(target), CopySource: copySource(sourceBucket, source, ""), MetadataDirective: types.MetadataDirectiveReplace}
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
}

// 삭제된 오브젝트 복사 실패를 확인하는 테스트
func TestCopyToDeletedObject(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t, 55)
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
	b := s.bucket(t, 56)
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
	b := s.bucket(t, 57)
	enableVersioning(t, s, b)
	source := "TestObjectVersioningCopyToItselfWithMetadataSource"
	putOut, err := s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(b), Key: aws.String(source), Body: bytes.NewReader([]byte(source))})
	if err != nil {
		t.Fatal(err)
	}
	input := &s3.CopyObjectInput{Bucket: aws.String(b), Key: aws.String(source), CopySource: copySource(b, source, ""), Metadata: map[string]string{"foo": "bar"}, MetadataDirective: types.MetadataDirectiveReplace}
	if aws.ToString(putOut.VersionId) != "" {
		input.CopySource = copySource(b, source, aws.ToString(putOut.VersionId))
	}
	copyCall(t, s.client, input)
	assertCopied(t, s.client, b, source, source, nil)
	head, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String(source)})
	if err != nil {
		t.Fatal(err)
	}
	if len(head.Metadata) != 1 || head.Metadata["foo"] != "bar" {
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
	b := s.bucket(t, 58)
	source := "TestObjectCopyToItselfWithMetadataOverwriteSource"
	metadata := map[string]string{"foo": "bar"}
	if _, err := s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(b), Key: aws.String(source), Body: bytes.NewReader([]byte(source)), Metadata: metadata}); err != nil {
		t.Fatal(err)
	}
	head, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String(source)})
	if err != nil || len(head.Metadata) != 1 || head.Metadata["foo"] != "bar" {
		t.Fatalf("initial metadata=%v err=%v", head.Metadata, err)
	}
	metadata["foo"] = "bar2"
	copyCall(t, s.client, &s3.CopyObjectInput{Bucket: aws.String(b), Key: aws.String(source), CopySource: copySource(b, source, ""), Metadata: metadata, MetadataDirective: types.MetadataDirectiveReplace})
	assertCopied(t, s.client, b, source, source, nil)
	head, err = s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String(source)})
	if err != nil {
		t.Fatal(err)
	}
	if len(head.Metadata) != 1 || head.Metadata["foo"] != "bar2" {
		t.Fatalf("metadata=%v", head.Metadata)
	}
}

// 버저닝된 버킷에서 copyObject로 덮어쓰기할 경우 메타데이터 덮어쓰기 모드로 메타데이터를 변경 가능한지 확인하는 테스트
func TestObjectVersioningCopyToItselfWithMetadataOverwrite(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	b := s.bucket(t, 59)
	enableVersioning(t, s, b)
	source := "TestObjectVersioningCopyToItselfWithMetadataOverwriteSource"
	metadata := map[string]string{"foo": "bar"}
	putOut, err := s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(b), Key: aws.String(source), Body: bytes.NewReader([]byte(source)), Metadata: metadata})
	if err != nil {
		t.Fatal(err)
	}
	head, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String(source)})
	if err != nil || len(head.Metadata) != 1 || head.Metadata["foo"] != "bar" {
		t.Fatalf("initial metadata=%v err=%v", head.Metadata, err)
	}
	metadata["foo"] = "bar2"
	input := &s3.CopyObjectInput{Bucket: aws.String(b), Key: aws.String(source), CopySource: copySource(b, source, ""), Metadata: metadata, MetadataDirective: types.MetadataDirectiveReplace}
	if aws.ToString(putOut.VersionId) != "" {
		input.CopySource = copySource(b, source, aws.ToString(putOut.VersionId))
	}
	copyCall(t, s.client, input)
	assertCopied(t, s.client, b, source, source, nil)
	head, err = s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String(source)})
	if err != nil {
		t.Fatal(err)
	}
	if len(head.Metadata) != 1 || head.Metadata["foo"] != "bar2" {
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
	b := ssecBucket(t, s, 60)
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
	b := s.bucket(t, 61)
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
		got := map[types.ChecksumAlgorithm]string{
			types.ChecksumAlgorithmCrc32:  aws.ToString(out.CopyObjectResult.ChecksumCRC32),
			types.ChecksumAlgorithmCrc32c: aws.ToString(out.CopyObjectResult.ChecksumCRC32C),
			types.ChecksumAlgorithmSha1:   aws.ToString(out.CopyObjectResult.ChecksumSHA1),
			types.ChecksumAlgorithmSha256: aws.ToString(out.CopyObjectResult.ChecksumSHA256),
		}[algorithm]
		if want := checksumValue(algorithm, body); got != want {
			t.Fatalf("%s checksum=%q want=%q", algorithm, got, want)
		}
		assertCopied(t, s.client, b, "target-"+string(algorithm), string(body), nil)
	}
}

// 메타데이터와 태그가 복사되는지 확인하는 테스트
func TestCopyObjectMetadataAndTags(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t, 62)
	source, target := "source", "target"
	if _, err := s.client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(b), Key: aws.String(source), Body: bytes.NewReader([]byte(source)), Metadata: map[string]string{"foo": "bar"}, Tagging: aws.String("tag1=value1")}); err != nil {
		t.Fatal(err)
	}
	head, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String(source)})
	if err != nil || len(head.Metadata) != 1 || head.Metadata["foo"] != "bar" {
		t.Fatalf("source metadata=%v err=%v", head.Metadata, err)
	}
	tags, err := s.client.GetObjectTagging(context.Background(), &s3.GetObjectTaggingInput{Bucket: aws.String(b), Key: aws.String(source)})
	if err != nil || len(tags.TagSet) != 1 || aws.ToString(tags.TagSet[0].Key) != "tag1" || aws.ToString(tags.TagSet[0].Value) != "value1" {
		t.Fatalf("source tags=%v err=%v", tags.TagSet, err)
	}
	copyCall(t, s.client, &s3.CopyObjectInput{Bucket: aws.String(b), Key: aws.String(target), CopySource: copySource(b, source, "")})
	head, err = s.client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String(target)})
	if err != nil || len(head.Metadata) != 1 || head.Metadata["foo"] != "bar" {
		t.Fatalf("target metadata=%v err=%v", head.Metadata, err)
	}
	tags, err = s.client.GetObjectTagging(context.Background(), &s3.GetObjectTaggingInput{Bucket: aws.String(b), Key: aws.String(target)})
	if err != nil || len(tags.TagSet) != 1 || aws.ToString(tags.TagSet[0].Key) != "tag1" || aws.ToString(tags.TagSet[0].Value) != "value1" {
		t.Fatalf("target tags=%v err=%v", tags.TagSet, err)
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
