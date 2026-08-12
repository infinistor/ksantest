package s3tests

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"hash/crc32"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// 오브젝트가 올바르게 생성되는지 확인
func TestBucketListDistinct(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	first, second := s.bucket(t, 1), s.bucket(t, 1)
	key := "TestBucketListDistinct"
	put(t, s, first, key, key, nil)
	out, err := s.client.ListObjects(ctx, &s3.ListObjectsInput{Bucket: aws.String(second)})
	if err != nil || len(out.Contents) != 0 {
		t.Fatalf("contents=%v err=%v", out.Contents, err)
	}
}

// 존재하지 않는 버킷에 오브젝트 업로드할 경우 실패 확인
func TestObjectWriteToNonExistBucket(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	key := "TestObjectWriteToNonExistBucket"
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("missing-" + uniqueBucketSuffix(t)), Key: aws.String(key), Body: bytes.NewReader([]byte(key))})
	assertS3Error(t, err, 404, "NoSuchBucket")
}

// 0바이트로 업로드한 오브젝트가 실제로 0바이트인지 확인
func TestObjectHeadZeroBytes(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	bucket := s.bucket(t, 3)
	key := "TestObjectHeadZeroBytes"
	put(t, s, bucket, key, "", nil)
	out, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil || aws.ToInt64(out.ContentLength) != 0 {
		t.Fatalf("length=%v err=%v", out.ContentLength, err)
	}
}

// 업로드한 오브젝트의 ETag가 올바른지 확인
func TestObjectWriteCheckEtag(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	out := put(t, s, s.bucket(t, 4), "TestObjectWriteCheckEtag", "bar", nil)
	if strings.Trim(aws.ToString(out.ETag), `"`) != "37b51d194a7513e45b56f6524f2d51f2" {
		t.Fatalf("ETag=%q", aws.ToString(out.ETag))
	}
}

// 캐시(시간)를 설정하고 업로드한 오브젝트가 올바르게 반영되었는지 확인
func TestObjectWriteCacheControl(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	bucket, key, value := s.bucket(t, 5), "TestObjectWriteCacheControl", "public, max-age=14HttpStatus.SC_BAD_REQUEST"
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte(key)), ContentLength: aws.Int64(int64(len(key))), ContentType: aws.String("text/plain"), CacheControl: aws.String(value)})
	if err != nil {
		t.Fatal(err)
	}
	head, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil || aws.ToString(head.CacheControl) != value || read(t, s, bucket, key) != key {
		t.Fatalf("cache=%q err=%v", aws.ToString(head.CacheControl), err)
	}
}

// 캐시(날짜)를 설정하고 업로드한 오브젝트가 올바르게 반영되었는지 확인
func TestObjectWriteExpires(t *testing.T) {
	t.Parallel()
	t.Skip("JAVA에서는 헤더만료일시 설정이 내부전용으로 되어있어 설정되지 않음")
}

// 오브젝트의 기본 작업을 모드 올바르게 할 수 있는지 확인(read, write, update, delete)
func TestObjectWriteReadUpdateReadDelete(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	bucket := s.bucket(t, 7)
	key := "TestObjectWriteReadUpdateReadDelete"
	put(t, s, bucket, key, key, nil)
	if read(t, s, bucket, key) != key {
		t.Fatal("initial body mismatch")
	}
	updated := key + "Updated"
	put(t, s, bucket, key, updated, nil)
	if read(t, s, bucket, key) != updated {
		t.Fatal("updated body mismatch")
	}
	if _, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}); err != nil {
		t.Fatal(err)
	}
}

// 오브젝트에 메타데이터를 추가하여 업로드 할 경우 올바르게 적용되었는지 확인
func TestObjectSetGetMetadataNoneToGood(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	bucket, key := s.bucket(t, 8), "TestObjectSetGetMetadataNoneToGood"
	put(t, s, bucket, key, key, map[string]string{"meta1": "my"})
	head, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	got, ok := head.Metadata["meta1"]
	if err != nil || !ok || got != "my" {
		t.Fatalf("metadata=%v err=%v", head.Metadata, err)
	}
}

// 오브젝트에 빈 메타데이터를 추가하여 업로드 할 경우 올바르게 적용되었는지 확인
func TestObjectSetGetMetadataNoneToEmpty(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	bucket, key := s.bucket(t, 9), "TestObjectSetGetMetadataNoneToEmpty"
	put(t, s, bucket, key, key, map[string]string{"meta1": ""})
	head, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	got, ok := head.Metadata["meta1"]
	if err != nil || !ok || got != "" {
		t.Fatalf("metadata=%v err=%v", head.Metadata, err)
	}
}

// 메타 데이터 업데이트가 올바르게 적용되었는지 확인
func TestObjectSetGetMetadataOverwriteToEmpty(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	bucket, key := s.bucket(t, 10), "TestObjectSetGetMetadataOverwriteToEmpty"
	put(t, s, bucket, key, key, map[string]string{"meta1": "my"})
	put(t, s, bucket, key, key, map[string]string{"meta1": ""})
	head, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	got, ok := head.Metadata["meta1"]
	if err != nil || !ok || got != "" {
		t.Fatalf("metadata=%v err=%v", head.Metadata, err)
	}
}

// 메타데이터에 올바르지 않는 문자열[EOF(\x04)를 사용할 경우 실패 확인
func TestObjectSetGetNonUtf8Metadata(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	bucket, key := s.bucket(t, 11), "TestObjectSetGetNonUtf8Metadata"
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte(key)), Metadata: map[string]string{"meta1": "\nmy_meta"}})
	if err == nil {
		t.Fatal("invalid metadata was accepted")
	}
}

// 메타데이터에 올바르지 않는 문자[EOF(\x04)를 문자열 맨앞에 사용할 경우 실패 확인
func TestObjectSetGetMetadataEmptyToUnreadablePrefix(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	bucket, key := s.bucket(t, 12), "TestObjectSetGetMetadataEmptyToUnreadablePrefix"
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte(key)), Metadata: map[string]string{"meta1": "\nasdf"}})
	if err == nil {
		t.Fatal("invalid metadata was accepted")
	}
}

// 메타데이터에 올바르지 않는 문자[EOF(\x04)를 문자열 맨뒤에 사용할 경우 실패 확인
func TestObjectSetGetMetadataEmptyToUnreadableSuffix(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	bucket, key := s.bucket(t, 13), "TestObjectSetGetMetadataEmptyToUnreadableSuffix"
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte(key)), Metadata: map[string]string{"meta1": "asdf\n"}})
	if err == nil {
		t.Fatal("invalid metadata was accepted")
	}
}

// 오브젝트를 메타데이타 없이 덮어쓰기 했을 때, 메타데이타 값이 비어있는지 확인
func TestObjectMetadataReplacedOnPut(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	bucket, key := s.bucket(t, 14), "TestObjectMetadataReplacedOnPut"
	put(t, s, bucket, key, key, map[string]string{"meta1": "bar"})
	put(t, s, bucket, key, key, nil)
	head, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil || len(head.Metadata) != 0 {
		t.Fatalf("metadata=%v err=%v", head.Metadata, err)
	}
}

// body의 내용을utf-8로 인코딩한 오브젝트를 업로드 했을때 올바르게 업로드 되었는지 확인
func TestObjectWriteFile(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket := s.bucket(t, 15)
	key := "TestObjectWriteFile"
	put(t, s, bucket, key, string([]byte(key)), nil)
	if read(t, s, bucket, key) != key {
		t.Fatal("ASCII body mismatch")
	}
}

// 오브젝트 이름과 내용이 모두 특수문자인 오브젝트 여러개를 업로드 할 경우 모두 재대로 업로드 되는지 확인
func TestBucketCreateSpecialKeyNames(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	bucket := s.bucket(t, 16)
	keys := []string{"!", "-", "_", ".", "'", "()", "&", "$", "@", "=", ";", "/", ":", "+", "  ", ",", "?", "{}", "^", "%", "`", "[]", "<>", "~", "#", "|"}
	for _, key := range keys {
		put(t, s, bucket, key, strings.TrimSuffix(key, "/"), nil)
	}
	out, err := s.client.ListObjects(ctx, &s3.ListObjectsInput{Bucket: aws.String(bucket)})
	if err != nil || len(out.Contents) != len(keys) {
		t.Fatalf("contents=%d want=%d err=%v", len(out.Contents), len(keys), err)
	}
	for _, object := range out.Contents {
		key := aws.ToString(object.Key)
		if got := read(t, s, bucket, key); got != strings.TrimSuffix(key, "/") && !(strings.HasSuffix(key, "/") && got == "") {
			t.Fatalf("key=%q body=%q", key, got)
		}
	}
}

// [_], [/]가 포함된 이름을 가진 오브젝트를 업로드 한뒤 prefix정보를 설정한 GetObjectList가 가능한지 확인
func TestBucketListSpecialPrefix(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	bucket := s.bucket(t, 17)
	keys := []string{"Bla/1", "Bla/2", "Bla/3", "Bla/4", "abcd"}
	for _, key := range keys {
		put(t, s, bucket, key, key, nil)
	}
	out, err := s.client.ListObjects(ctx, &s3.ListObjectsInput{Bucket: aws.String(bucket), Prefix: aws.String("Bla/")})
	if err != nil || len(out.Contents) != 4 {
		t.Fatalf("contents=%d want=%d err=%v", len(out.Contents), 4, err)
	}
	for _, object := range out.Contents {
		key := aws.ToString(object.Key)
		if got := read(t, s, bucket, key); got != strings.TrimSuffix(key, "/") && !(strings.HasSuffix(key, "/") && got == "") {
			t.Fatalf("key=%q body=%q", key, got)
		}
	}
}

// [버킷의 Lock옵션을 활성화] LegalHold와 Lock유지기한을 설정하여 오브젝트 업로드할 경우 설정이 적용되는지 메타데이터를 통해 확인
func TestObjectLockUploadingObj(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	bucket := newBucketName(s.cfg.BucketPrefix)
	input := createBucketInput(s.cfg, bucket)
	input.ObjectLockEnabledForBucket = aws.Bool(true)
	_, err := s.client.CreateBucket(ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if !s.cfg.NotDelete {
			cleanupBucket(t, s, bucket)
		}
	})
	key := "TestObjectLockUploadingObj"
	retain := time.Date(2030, 2, 1, 0, 0, 0, 0, time.UTC)
	digest := md5.Sum([]byte(key))
	out, err := s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte(key)), ContentMD5: aws.String(base64.StdEncoding.EncodeToString(digest[:])), ContentType: aws.String("text/plain"), ObjectLockMode: types.ObjectLockModeGovernance, ObjectLockRetainUntilDate: &retain, ObjectLockLegalHoldStatus: types.ObjectLockLegalHoldStatusOn})
	if err != nil {
		t.Fatal(err)
	}
	head, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), VersionId: out.VersionId})
	if err != nil || head.ObjectLockMode != types.ObjectLockModeGovernance || head.ObjectLockLegalHoldStatus != types.ObjectLockLegalHoldStatusOn || head.ObjectLockRetainUntilDate == nil || !head.ObjectLockRetainUntilDate.Equal(retain) {
		t.Fatalf("lock=%v/%v/%v err=%v", head.ObjectLockMode, head.ObjectLockLegalHoldStatus, head.ObjectLockRetainUntilDate, err)
	}
	_, err = s.client.PutObjectLegalHold(ctx, &s3.PutObjectLegalHoldInput{Bucket: aws.String(bucket), Key: aws.String(key), VersionId: out.VersionId, LegalHold: &types.ObjectLockLegalHold{Status: types.ObjectLockLegalHoldStatusOff}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket:                    aws.String(bucket),
		Key:                       aws.String(key),
		VersionId:                 out.VersionId,
		BypassGovernanceRetention: aws.Bool(true),
	}); err != nil {
		t.Fatal(err)
	}
}

// 오브젝트의 중간에 공백문자가 들어갔을 경우 올바르게 동작하는지 확인
func TestObjectInfixSpace(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	bucket := s.bucket(t, 19)
	keys := []string{"a a/", "b b/f1", "c/f 2", "d d/f 3"}
	for _, key := range keys {
		put(t, s, bucket, key, strings.TrimSuffix(key, "/"), nil)
	}
	out, err := s.client.ListObjects(ctx, &s3.ListObjectsInput{Bucket: aws.String(bucket)})
	if err != nil || len(out.Contents) != len(keys) {
		t.Fatalf("contents=%d want=%d err=%v", len(out.Contents), len(keys), err)
	}
	for _, object := range out.Contents {
		key := aws.ToString(object.Key)
		if got := read(t, s, bucket, key); got != strings.TrimSuffix(key, "/") && !(strings.HasSuffix(key, "/") && got == "") {
			t.Fatalf("key=%q body=%q", key, got)
		}
	}
}

// 오브젝트의 마지막에 공백문자가 들어갔을 경우 올바르게 동작하는지 확인
func TestObjectSuffixSpace(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	bucket := s.bucket(t, 20)
	keys := []string{"a /", "b /f1", "c/f2 ", "d /f3 "}
	for _, key := range keys {
		put(t, s, bucket, key, strings.TrimSuffix(key, "/"), nil)
	}
	out, err := s.client.ListObjects(ctx, &s3.ListObjectsInput{Bucket: aws.String(bucket)})
	if err != nil || len(out.Contents) != len(keys) {
		t.Fatalf("contents=%d want=%d err=%v", len(out.Contents), len(keys), err)
	}
	for _, object := range out.Contents {
		key := aws.ToString(object.Key)
		if got := read(t, s, bucket, key); got != strings.TrimSuffix(key, "/") && !(strings.HasSuffix(key, "/") && got == "") {
			t.Fatalf("key=%q body=%q", key, got)
		}
	}
}

// [AWS SDK V2] 특수문자를 포함한 오브젝트 업로드 성공 확인
func TestPutObjectSpecialCharacters(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	bucket := s.bucket(t, 21)
	keys := []string{"!", "!/", "!/!", "$", "$/", "$/$", "'", "'/", "'/'", "(", "(/", "(/(", ")", ")/", ")/)", "*", "*/", "*/*", ":", ":/", ":/:", "[", "[/", "[/[", "]", "]/", "]/]"}
	for _, key := range keys {
		put(t, s, bucket, key, strings.TrimSuffix(key, "/"), nil)
	}
	out, err := s.client.ListObjects(ctx, &s3.ListObjectsInput{Bucket: aws.String(bucket)})
	if err != nil || len(out.Contents) != len(keys) {
		t.Fatalf("contents=%d want=%d err=%v", len(out.Contents), len(keys), err)
	}
	for _, object := range out.Contents {
		key := aws.ToString(object.Key)
		if got := read(t, s, bucket, key); got != strings.TrimSuffix(key, "/") && !(strings.HasSuffix(key, "/") && got == "") {
			t.Fatalf("key=%q body=%q", key, got)
		}
	}
}

// [AWS SDK V2] 특수문자를 포함한 오브젝트 업로드 성공 확인 (UseChunkEncoding = true)
// UseChunkEncoding = true는 aws-sdk-go-v2의 기본 전송 방식이므로 별도 토글 없이 일반 업로드로 검증한다.
// (java testPutObjectSpecialCharactersUseChunkEncoding 대응)
func TestPutObjectSpecialCharactersUseChunkEncoding(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	bucket := s.bucket(t, 22)
	keys := []string{"!", "!/", "!/!", "$", "$/", "$/$", "'", "'/", "'/'", "(", "(/", "(/(", ")", ")/", ")/)", "*", "*/", "*/*", ":", ":/", ":/:", "[", "[/", "[/[", "]", "]/", "]/]"}
	for _, key := range keys {
		put(t, s, bucket, key, strings.TrimSuffix(key, "/"), nil)
	}
	out, err := s.client.ListObjects(ctx, &s3.ListObjectsInput{Bucket: aws.String(bucket)})
	if err != nil || len(out.Contents) != len(keys) {
		t.Fatalf("contents=%d want=%d err=%v", len(out.Contents), len(keys), err)
	}
	for _, object := range out.Contents {
		key := aws.ToString(object.Key)
		if got := read(t, s, bucket, key); got != strings.TrimSuffix(key, "/") && !(strings.HasSuffix(key, "/") && got == "") {
			t.Fatalf("key=%q body=%q", key, got)
		}
	}
}

// 아래 3개 테스트(java testV2 PutObject)는 aws-sdk-go-v2로 재현할 수 없어 미구현한다.
//   - testPutObjectSpecialCharactersNotChunkEncoding
//   - testPutObjectSpecialCharactersNotChunkEncodingAndDisablePayloadSigning
//   - testPutObjectUseSpecialCharactersChunkEncodingAndDisablePayloadSigning
// 사유: java SDK v2의 S3Configuration.chunkedEncodingEnabled(false) / disablePayloadSigning에
// 대응하는 클라이언트 옵션이 aws-sdk-go-v2 s3.Options에 존재하지 않는다. 청크 인코딩 비활성화와
// 페이로드 서명 비활성화 토글을 표현할 방법이 없어 UseChunkEncoding(기본 동작) 변형만 구현했다.

// 폴더의 이름과 동일한 오브젝트 업로드가 가능한지 확인
func TestPutObjectDirAndFile(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	for _, keys := range [][]string{{"aaa", "aaa/"}, {"aaa/", "aaa"}, {"aaa", "aaa/bbb/ccc"}} {
		bucket := s.bucket(t, 26)
		for _, key := range keys {
			put(t, s, bucket, key, strings.TrimSuffix(key, "/"), nil)
		}
		out, err := s.client.ListObjects(ctx, &s3.ListObjectsInput{Bucket: aws.String(bucket)})
		if err != nil || len(out.Contents) != 2 {
			t.Fatalf("contents=%v err=%v", out.Contents, err)
		}
	}
}

// 오브젝트를 여러번 업로드 했을때 올바르게 반영되는지 확인
func TestObjectOverwrite(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket := s.bucket(t, 27)
	put(t, s, bucket, "temp", strings.Repeat("a", 10*1024), nil)
	want := strings.Repeat("b", 1024*1024)
	put(t, s, bucket, "temp", want, nil)
	if got := read(t, s, bucket, "temp"); got != want {
		t.Fatalf("length=%d want=%d", len(got), len(want))
	}
}

// 오브젝트 이름에 이모지가 포함될 경우 올바르게 업로드 되는지 확인
func TestObjectEmoji(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	bucket, key := s.bucket(t, 28), "test❤🍕🍔🚗"
	put(t, s, bucket, key, key, nil)
	out, err := s.client.ListObjects(ctx, &s3.ListObjectsInput{Bucket: aws.String(bucket)})
	if err != nil || len(out.Contents) != 1 || aws.ToString(out.Contents[0].Key) != key {
		t.Fatalf("contents=%v err=%v", out.Contents, err)
	}
}

// 메타데이터에 utf-8이 포함될 경우 올바르게 업로드 되는지 확인
func TestObjectSetGetMetadataUtf8(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	bucket, key := s.bucket(t, 29), "foo"
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte("bar")), ContentType: aws.String("text/plain; charset=UTF-8"), Metadata: map[string]string{"meta1": "utf-8", "meta2": "UTF-8"}})
	if err != nil {
		t.Fatal(err)
	}
	head, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil || head.Metadata["meta1"] != "utf-8" || head.Metadata["meta2"] != "UTF-8" {
		t.Fatalf("metadata=%v err=%v", head.Metadata, err)
	}
}

// 유저 메타데이터 키가 대소문자가 섞여 있어도 소문자로 반환되는지 확인
func TestObjectSetGetMetadataMixedCaseKey(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	bucket, key := s.bucket(t, 49), "foo"
	put(t, s, bucket, key, key, map[string]string{"Meta1": "value1", "META2": "value2", "mEtA3": "value3"})
	head, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil || len(head.Metadata) != 3 ||
		head.Metadata["meta1"] != "value1" ||
		head.Metadata["meta2"] != "value2" ||
		head.Metadata["meta3"] != "value3" {
		t.Fatalf("metadata=%v err=%v", head.Metadata, err)
	}
}

// 체크섬 계산 및 검증 클라이언트 옵션 조합별 오브젝트 업로드 성공 확인
func TestPutObjectChecksum(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket := s.bucket(t, 31)
	configs := []struct {
		name     string
		request  aws.RequestChecksumCalculation
		response aws.ResponseChecksumValidation
	}{
		{"required-required", aws.RequestChecksumCalculationWhenRequired, aws.ResponseChecksumValidationWhenRequired},
		{"required-supported", aws.RequestChecksumCalculationWhenRequired, aws.ResponseChecksumValidationWhenSupported},
		{"supported-required", aws.RequestChecksumCalculationWhenSupported, aws.ResponseChecksumValidationWhenRequired},
		{"supported-supported", aws.RequestChecksumCalculationWhenSupported, aws.ResponseChecksumValidationWhenSupported},
	}
	algorithms := []types.ChecksumAlgorithm{types.ChecksumAlgorithmCrc32, types.ChecksumAlgorithmCrc32c, types.ChecksumAlgorithmSha1, types.ChecksumAlgorithmSha256}
	for _, config := range configs {
		config := config
		t.Run(config.name, func(t *testing.T) {
			for _, algorithm := range algorithms {
				algorithm := algorithm
				t.Run(string(algorithm), func(t *testing.T) {
					key := "TestPutObjectChecksum/" + config.name + "/" + string(algorithm)
					body := []byte(key)
					input := &s3.PutObjectInput{
						Bucket:            aws.String(bucket),
						Key:               aws.String(key),
						Body:              bytes.NewReader(body),
						ChecksumAlgorithm: algorithm,
					}
					out, err := s.client.PutObject(context.Background(), input, func(options *s3.Options) {
						options.RequestChecksumCalculation = config.request
						options.ResponseChecksumValidation = config.response
					})
					if err != nil {
						t.Fatal(err)
					}
					want := checksumValue(algorithm, body)
					got := map[types.ChecksumAlgorithm]string{
						types.ChecksumAlgorithmCrc32:  aws.ToString(out.ChecksumCRC32),
						types.ChecksumAlgorithmCrc32c: aws.ToString(out.ChecksumCRC32C),
						types.ChecksumAlgorithmSha1:   aws.ToString(out.ChecksumSHA1),
						types.ChecksumAlgorithmSha256: aws.ToString(out.ChecksumSHA256),
					}[algorithm]
					if got != want {
						t.Fatalf("checksum=%q want=%q", got, want)
					}
				})
			}
		})
	}
}

// 사전 계산한 체크섬 값을 직접 지정하여 오브젝트 업로드 시 검증 성공 확인
func TestPutObjectChecksumWithValue(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket := s.bucket(t, 32)
	algorithms := []types.ChecksumAlgorithm{types.ChecksumAlgorithmCrc32, types.ChecksumAlgorithmCrc32c, types.ChecksumAlgorithmSha1, types.ChecksumAlgorithmSha256}
	for _, algorithm := range algorithms {
		algorithm := algorithm
		t.Run(string(algorithm), func(t *testing.T) {
			key := "TestPutObjectChecksumWithValue/" + string(algorithm)
			body := []byte(key)
			input := &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader(body), ChecksumAlgorithm: algorithm}
			setPutChecksum(input, algorithm, body, false)
			out, err := s.client.PutObject(context.Background(), input)
			if err != nil {
				t.Fatal(err)
			}
			want := checksumValue(algorithm, body)
			got := map[types.ChecksumAlgorithm]string{types.ChecksumAlgorithmCrc32: aws.ToString(out.ChecksumCRC32), types.ChecksumAlgorithmCrc32c: aws.ToString(out.ChecksumCRC32C), types.ChecksumAlgorithmSha1: aws.ToString(out.ChecksumSHA1), types.ChecksumAlgorithmSha256: aws.ToString(out.ChecksumSHA256)}[algorithm]
			if got != want {
				t.Fatalf("checksum=%q want=%q", got, want)
			}
		})
	}
}

// 체크섬 계산/검증 설정 조합에서 모든 알고리즘의 PutObject 체크섬이 올바른지 확인 (UseChunkEncoding = true)
// UseChunkEncoding = true는 aws-sdk-go-v2의 기본 전송 방식이므로 별도 토글 없이 검증한다.
// java testPutObjectChecksumUseChunkEncoding은 동일 시나리오를 동기/비동기 클라이언트로 각각 수행하나,
// aws-sdk-go-v2에는 별도의 S3AsyncClient가 없어 동기 경로만 구현한다.
func TestPutObjectChecksumUseChunkEncoding(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket := s.bucket(t, 30)
	configs := []struct {
		name     string
		request  aws.RequestChecksumCalculation
		response aws.ResponseChecksumValidation
	}{
		{"required-required", aws.RequestChecksumCalculationWhenRequired, aws.ResponseChecksumValidationWhenRequired},
		{"required-supported", aws.RequestChecksumCalculationWhenRequired, aws.ResponseChecksumValidationWhenSupported},
		{"supported-required", aws.RequestChecksumCalculationWhenSupported, aws.ResponseChecksumValidationWhenRequired},
		{"supported-supported", aws.RequestChecksumCalculationWhenSupported, aws.ResponseChecksumValidationWhenSupported},
	}
	algorithms := []types.ChecksumAlgorithm{types.ChecksumAlgorithmCrc32, types.ChecksumAlgorithmCrc32c, types.ChecksumAlgorithmSha1, types.ChecksumAlgorithmSha256}
	for _, config := range configs {
		config := config
		t.Run(config.name, func(t *testing.T) {
			for _, algorithm := range algorithms {
				algorithm := algorithm
				t.Run(string(algorithm), func(t *testing.T) {
					key := "TestPutObjectChecksumUseChunkEncoding/" + config.name + "/" + string(algorithm)
					body := []byte(key)
					input := &s3.PutObjectInput{
						Bucket:            aws.String(bucket),
						Key:               aws.String(key),
						Body:              bytes.NewReader(body),
						ChecksumAlgorithm: algorithm,
					}
					out, err := s.client.PutObject(context.Background(), input, func(options *s3.Options) {
						options.RequestChecksumCalculation = config.request
						options.ResponseChecksumValidation = config.response
					})
					if err != nil {
						t.Fatal(err)
					}
					want := checksumValue(algorithm, body)
					got := map[types.ChecksumAlgorithm]string{
						types.ChecksumAlgorithmCrc32:  aws.ToString(out.ChecksumCRC32),
						types.ChecksumAlgorithmCrc32c: aws.ToString(out.ChecksumCRC32C),
						types.ChecksumAlgorithmSha1:   aws.ToString(out.ChecksumSHA1),
						types.ChecksumAlgorithmSha256: aws.ToString(out.ChecksumSHA256),
					}[algorithm]
					if got != want {
						t.Fatalf("checksum=%q want=%q", got, want)
					}
				})
			}
		})
	}
}

// 잘못된 체크섬 값을 지정하여 오브젝트 업로드 시 BadDigest 실패 확인
func TestPutObjectChecksumFailure(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket := s.bucket(t, 33)
	algorithms := []types.ChecksumAlgorithm{types.ChecksumAlgorithmCrc32, types.ChecksumAlgorithmCrc32c, types.ChecksumAlgorithmSha1, types.ChecksumAlgorithmSha256}
	for _, algorithm := range algorithms {
		algorithm := algorithm
		t.Run(string(algorithm), func(t *testing.T) {
			key := "TestPutObjectChecksumFailure/" + string(algorithm)
			body := []byte(key)
			input := &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader(body), ChecksumAlgorithm: algorithm}
			setPutChecksum(input, algorithm, body, true)
			_, err := s.client.PutObject(context.Background(), input)
			assertS3Error(t, err, 400, "BadDigest")
		})
	}
}

// 일치하는 If-Match 조건으로 오브젝트 덮어쓰기 성공 확인
func TestPutObjectIfMatchGood(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket, key := s.bucket(t, 34), "TestPutObjectIfMatchGood"
	first := put(t, s, bucket, key, "old", nil)
	ctx := context.Background()
	input := &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte("new"))}
	input.IfMatch = first.ETag
	_, err := s.client.PutObject(ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	if got := read(t, s, bucket, key); got != "new" {
		t.Fatalf("body=%q want=%q", got, "new")
	}
}

// 일치하지 않는 If-Match 조건으로 오브젝트 덮어쓰기 시 412 실패 확인
func TestPutObjectIfMatchFailed(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket, key := s.bucket(t, 35), "TestPutObjectIfMatchFailed"
	put(t, s, bucket, key, "old", nil)
	ctx := context.Background()
	input := &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte("new"))}
	input.IfMatch = aws.String("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
	_, err := s.client.PutObject(ctx, input)
	assertS3Error(t, err, 412, "PreconditionFailed")
	if got := read(t, s, bucket, key); got != "old" {
		t.Fatalf("body=%q want=%q", got, "old")
	}
}

// 존재하지 않는 키에 If-None-Match: * 조건으로 업로드 성공 확인
func TestPutObjectIfNoneMatchGood(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket := s.bucket(t, 36)
	origKey := "TestPutObjectIfNoneMatchGood"
	put(t, s, bucket, origKey, "old", nil)
	ctx := context.Background()
	key := origKey + "-new"
	input := &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte("new"))}
	input.IfNoneMatch = aws.String("*")
	_, err := s.client.PutObject(ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	if got := read(t, s, bucket, key); got != "new" {
		t.Fatalf("body=%q want=%q", got, "new")
	}
}

// 이미 존재하는 키에 If-None-Match: * 조건으로 업로드 시 412 실패 확인
func TestPutObjectIfNoneMatchFailed(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket, key := s.bucket(t, 37), "TestPutObjectIfNoneMatchFailed"
	put(t, s, bucket, key, "old", nil)
	ctx := context.Background()
	input := &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte("new"))}
	input.IfNoneMatch = aws.String("*")
	_, err := s.client.PutObject(ctx, input)
	assertS3Error(t, err, 412, "PreconditionFailed")
	if got := read(t, s, bucket, key); got != "old" {
		t.Fatalf("body=%q want=%q", got, "old")
	}
}

// If-Match와 If-None-Match를 함께 지정하면 501로 거부되는지 확인
func TestPutObjectIfMatchAndIfNoneMatch(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket, key := s.bucket(t, 38), "TestPutObjectIfMatchAndIfNoneMatch"
	first := put(t, s, bucket, key, "old", nil)
	ctx := context.Background()
	input := &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte("new"))}
	input.IfMatch = first.ETag
	input.IfNoneMatch = aws.String("*")
	_, err := s.client.PutObject(ctx, input)
	assertS3Error(t, err, 501, "NotImplemented")
	if got := read(t, s, bucket, key); got != "old" {
		t.Fatalf("body=%q want=%q", got, "old")
	}
}

// 최대 길이(1024자)의 오브젝트 키로 업로드 성공 확인
func TestPutObjectKeyMaxLength(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket := s.bucket(t, 39)
	key := strings.Repeat("a", 1024)
	body := "test-max-length"
	out := put(t, s, bucket, key, body, nil)
	if aws.ToString(out.ETag) == "" || read(t, s, bucket, key) != body {
		t.Fatalf("key bytes=%d ETag=%q", len([]byte(key)), aws.ToString(out.ETag))
	}
}

// 최소 길이(1자)의 오브젝트 키로 업로드 성공 확인
func TestPutObjectKeyMinLength(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket := s.bucket(t, 40)
	key, body := "a", "test-min-length"
	out := put(t, s, bucket, key, body, nil)
	if aws.ToString(out.ETag) == "" || read(t, s, bucket, key) != body {
		t.Fatalf("key bytes=%d ETag=%q", len([]byte(key)), aws.ToString(out.ETag))
	}
}

// 최대 길이(1024바이트)를 초과하는 오브젝트 키로 업로드 실패 확인
func TestPutObjectKeyTooLong(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket := s.bucket(t, 41)
	key := strings.Repeat("a", 1025)
	_, err := s.client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader([]byte("test-too-long")),
	})
	assertS3Error(t, err, 400, "KeyTooLongError")
}

// 특수문자로 시작하는 오브젝트 키로 업로드 성공 확인
func TestPutObjectKeySpecialCharactersAtStart(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket := s.bucket(t, 42)
	specialChars := []string{"!", "@", "#", "$", "%", "^", "&", "*", "(", ")", "-", "_", "+", "=", "[", "]", "{", "}", "|", "\\", ":", ";", "\"", "'", "<", ">", ",", ".", "?", "/", "~", "`"}
	for _, specialChar := range specialChars {
		remaining := 1024 - len(specialChar)
		key := specialChar + randomText(remaining)
		body := "test-body-" + specialChar
		if len(key) != 1024 {
			t.Fatalf("key length=%d want=1024 special=%q", len(key), specialChar)
		}
		out := put(t, s, bucket, key, body, nil)
		if aws.ToString(out.ETag) == "" || read(t, s, bucket, key) != body {
			t.Fatalf("special=%q ETag=%q", specialChar, aws.ToString(out.ETag))
		}
	}
}

// 특수문자로 끝나는 오브젝트 키로 업로드 성공 확인
func TestPutObjectKeySpecialCharactersAtEnd(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket := s.bucket(t, 43)
	specialChars := []string{"!", "@", "#", "$", "%", "^", "&", "*", "(", ")", "-", "_", "+", "=", "[", "]", "{", "}", "|", "\\", ":", ";", "\"", "'", "<", ">", ",", ".", "?", "/", "~", "`"}
	for _, specialChar := range specialChars {
		remaining := 1024 - len(specialChar)
		key := randomText(remaining) + specialChar
		body := "test-body-" + specialChar
		if len(key) != 1024 {
			t.Fatalf("key length=%d want=1024 special=%q", len(key), specialChar)
		}
		out := put(t, s, bucket, key, body, nil)
		if aws.ToString(out.ETag) == "" || read(t, s, bucket, key) != body {
			t.Fatalf("special=%q ETag=%q", specialChar, aws.ToString(out.ETag))
		}
	}
}

// 유니코드 문자를 포함한 오브젝트 키로 업로드 성공 확인
func TestPutObjectKeyUnicodeCharacters(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket := s.bucket(t, 44)
	for _, char := range []string{"한", "中", "日", "а", "α", "ع", "т", "ф"} {
		count := 200/len([]byte(char)) - 1
		key := strings.Repeat(char, count)
		body := "unicode-" + char
		out := put(t, s, bucket, key, body, nil)
		if aws.ToString(out.ETag) == "" || read(t, s, bucket, key) != body {
			t.Fatalf("key bytes=%d ETag=%q", len([]byte(key)), aws.ToString(out.ETag))
		}
	}
}

// 1024바이트를 초과하는 유니코드 키로 업로드 실패 확인
func TestPutObjectKeyUnicodeCharactersTooLong(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket := s.bucket(t, 45)
	for _, char := range []string{"한", "中", "日", "а", "α", "ع", "т", "ф"} {
		charBytes := len([]byte(char))
		key := strings.Repeat(char, 1024/charBytes+1)
		if len([]byte(key)) <= 1024 {
			t.Fatalf("key bytes=%d want >1024 char=%q", len([]byte(key)), char)
		}
		_, err := s.client.PutObject(context.Background(), &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   bytes.NewReader([]byte("unicode-test-fail-" + char)),
		})
		assertS3Error(t, err, 400, "KeyTooLongError")
	}
}

// 앞뒤 공백문자를 포함한 오브젝트 키로 업로드 성공 확인
func TestPutObjectKeyWithLeadingAndTrailingSpaces(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket := s.bucket(t, 46)
	for _, n := range []int{1, 2, 3, 5} {
		key := strings.Repeat(" ", n) + strings.Repeat("a", 1024-2*n) + strings.Repeat(" ", n)
		body := fmt.Sprintf("space-%d", n)
		out := put(t, s, bucket, key, body, nil)
		if aws.ToString(out.ETag) == "" || read(t, s, bucket, key) != body {
			t.Fatalf("key bytes=%d ETag=%q", len([]byte(key)), aws.ToString(out.ETag))
		}
	}
}

// 연속된 슬래시를 포함한 오브젝트 키로 업로드 성공 확인
func TestPutObjectKeyWithConsecutiveSlashes(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket := s.bucket(t, 47)
	for _, key := range []string{"folder//double-slash", "folder///triple-slash", "//leading-double-slash", "trailing-double-slash//", "folder////multiple-slashes"} {
		body := "slash-" + strings.ReplaceAll(key, "/", "-")
		out := put(t, s, bucket, key, body, nil)
		if aws.ToString(out.ETag) == "" || read(t, s, bucket, key) != body {
			t.Fatalf("key bytes=%d ETag=%q", len([]byte(key)), aws.ToString(out.ETag))
		}
	}
}

// 다양한 경계 길이의 오브젝트 키로 업로드 성공 확인
func TestPutObjectKeyBoundaryLengths(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket := s.bucket(t, 48)
	for _, n := range []int{1023, 1024, 500, 100, 50} {
		key := strings.Repeat("a", n)
		body := fmt.Sprintf("boundary-%d", n)
		out := put(t, s, bucket, key, body, nil)
		if aws.ToString(out.ETag) == "" || read(t, s, bucket, key) != body {
			t.Fatalf("key bytes=%d ETag=%q", len([]byte(key)), aws.ToString(out.ETag))
		}
	}
}

func setPutChecksum(input *s3.PutObjectInput, algorithm types.ChecksumAlgorithm, body []byte, wrong bool) {
	valueBody := body
	if wrong {
		valueBody = append(append([]byte(nil), body...), []byte("-wrong")...)
	}
	value := checksumValue(algorithm, valueBody)
	switch algorithm {
	case types.ChecksumAlgorithmCrc32:
		input.ChecksumCRC32 = aws.String(value)
	case types.ChecksumAlgorithmCrc32c:
		input.ChecksumCRC32C = aws.String(value)
	case types.ChecksumAlgorithmSha1:
		input.ChecksumSHA1 = aws.String(value)
	case types.ChecksumAlgorithmSha256:
		input.ChecksumSHA256 = aws.String(value)
	}
}

func checksumValue(algorithm types.ChecksumAlgorithm, body []byte) string {
	var sum []byte
	switch algorithm {
	case types.ChecksumAlgorithmCrc32:
		value := crc32.ChecksumIEEE(body)
		sum = []byte{byte(value >> 24), byte(value >> 16), byte(value >> 8), byte(value)}
	case types.ChecksumAlgorithmCrc32c:
		value := crc32.Checksum(body, crc32.MakeTable(crc32.Castagnoli))
		sum = []byte{byte(value >> 24), byte(value >> 16), byte(value >> 8), byte(value)}
	case types.ChecksumAlgorithmSha1:
		value := sha1.Sum(body)
		sum = value[:]
	case types.ChecksumAlgorithmSha256:
		value := sha256.Sum256(body)
		sum = value[:]
	}
	return base64.StdEncoding.EncodeToString(sum)
}
