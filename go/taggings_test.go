package s3tests

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// 버킷에 사용자 추가 태그값을 설정할경우 성공확인
func TestSetTagging(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	b := s.bucket(t, 1)
	_, err := s.client.GetBucketTagging(context.Background(), &s3.GetBucketTaggingInput{Bucket: aws.String(b)})
	assertS3Error(t, err, 404, "NoSuchTagSet")
	want := []types.Tag{{Key: aws.String("Hello"), Value: aws.String("World")}}
	_, err = s.client.PutBucketTagging(context.Background(), &s3.PutBucketTaggingInput{Bucket: aws.String(b), Tagging: &types.Tagging{TagSet: want}})
	if err != nil {
		t.Fatal(err)
	}
	out, err := s.client.GetBucketTagging(context.Background(), &s3.GetBucketTaggingInput{Bucket: aws.String(b)})
	if err != nil {
		t.Fatal(err)
	}
	assertTags(t, out.TagSet, want)
	_, err = s.client.DeleteBucketTagging(context.Background(), &s3.DeleteBucketTaggingInput{Bucket: aws.String(b)})
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.client.GetBucketTagging(context.Background(), &s3.GetBucketTaggingInput{Bucket: aws.String(b)})
	assertS3Error(t, err, 404, "NoSuchTagSet")
}

// 오브젝트에 태그 설정이 올바르게 적용되는지 확인
func TestGetObjTagging(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	b, key := s.bucket(t, 2), "TestGetObjTagging"
	put(t, s, b, key, "", nil)
	want := tags(2, 0, 0)
	if err := taggingsPutObjectTags(t, s, b, key, want); err != nil {
		t.Fatal(err)
	}
	assertTags(t, getObjectTags(t, s, b, key), want)
}

// 오브젝트에 태그 설정이 올바르게 적용되는지 헤더정보를 통해 확인
func TestGetObjHeadTagging(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	b, key := s.bucket(t, 3), "TestGetObjHeadTagging"
	put(t, s, b, key, "", nil)
	want := tags(2, 0, 0)
	if err := taggingsPutObjectTags(t, s, b, key, want); err != nil {
		t.Fatal(err)
	}
	head, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String(key)})
	if err != nil || aws.ToInt32(head.TagCount) != 2 {
		t.Fatalf("TagCount=%v err=%v", head.TagCount, err)
	}
}

// 추가가능한 최대갯수까지 태그를 입력할 수 있는지 확인(max = 10)
func TestPutMaxTags(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	b, key := s.bucket(t, 4), "TestPutMaxTags"
	put(t, s, b, key, "", nil)
	want := tags(10, 0, 0)
	if err := taggingsPutObjectTags(t, s, b, key, want); err != nil {
		t.Fatal(err)
	}
	assertTags(t, getObjectTags(t, s, b, key), want)
}

// 추가가능한 최대갯수를 넘겨서 태그를 입력할때 에러 확인
func TestPutExcessTags(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	b, key := s.bucket(t, 5), "TestPutExcessTags"
	put(t, s, b, key, "", nil)
	err := taggingsPutObjectTags(t, s, b, key, tags(11, 0, 0))
	assertS3Error(t, err, 400, "BadRequest")
	assertTags(t, getObjectTags(t, s, b, key), nil)
}

// 태그의 key값의 길이가 최대(128) value값의 길이가 최대(256)일때 태그를 입력할 수 있는지 확인
func TestPutMaxSizeTags(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	b, key := s.bucket(t, 6), "TestPutMaxSizeTags"
	put(t, s, b, key, "", nil)
	want := tags(10, 128, 256)
	if err := taggingsPutObjectTags(t, s, b, key, want); err != nil {
		t.Fatal(err)
	}
	assertTags(t, getObjectTags(t, s, b, key), want)
}

// 태그의 key값의 길이가 최대(129) value값의 길이가 최대(256)일때 태그 입력 실패 확인
func TestPutExcessKeyTags(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	b, key := s.bucket(t, 7), "TestPutExcessKeyTags"
	put(t, s, b, key, "", nil)
	err := taggingsPutObjectTags(t, s, b, key, tags(10, 129, 256))
	assertS3Error(t, err, 400, "InvalidTag")
	assertTags(t, getObjectTags(t, s, b, key), nil)
}

// 태그의 key값의 길이가 최대(128) value값의 길이가 최대(257)일때 태그 입력 실패 확인
func TestPutExcessValTags(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	b, key := s.bucket(t, 8), "TestPutExcessValTags"
	put(t, s, b, key, "", nil)
	err := taggingsPutObjectTags(t, s, b, key, tags(10, 128, 259))
	assertS3Error(t, err, 400, "InvalidTag")
	assertTags(t, getObjectTags(t, s, b, key), nil)
}

// 오브젝트의 태그목록을 덮어쓰기 가능한지 확인
func TestPutModifyTags(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	b, key := s.bucket(t, 9), "TestPutModifyTags"
	put(t, s, b, key, "", nil)
	first, second := tags(2, 0, 0), tags(1, 128, 128)
	if err := taggingsPutObjectTags(t, s, b, key, first); err != nil {
		t.Fatal(err)
	}
	assertTags(t, getObjectTags(t, s, b, key), first)
	if err := taggingsPutObjectTags(t, s, b, key, second); err != nil {
		t.Fatal(err)
	}
	assertTags(t, getObjectTags(t, s, b, key), second)
}

// 오브젝트의 태그를 삭제 가능한지 확인
func TestPutDeleteTags(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	b, key := s.bucket(t, 10), "TestPutDeleteTags"
	put(t, s, b, key, "", nil)
	want := tags(2, 0, 0)
	if err := taggingsPutObjectTags(t, s, b, key, want); err != nil {
		t.Fatal(err)
	}
	_, err := s.client.DeleteObjectTagging(context.Background(), &s3.DeleteObjectTaggingInput{Bucket: aws.String(b), Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}
	assertTags(t, getObjectTags(t, s, b, key), nil)
}

// 헤더에 태그정보를 포함한 오브젝트 업로드 성공 확인
func TestPutObjWithTags(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	b, key := s.bucket(t, 11), "test tag obj1"
	body := strings.Repeat("x", 100)
	_, err := s.client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(b), Key: aws.String(key), Body: bytes.NewReader([]byte(body)), Tagging: aws.String("foo=bar&bar=")})
	if err != nil {
		t.Fatal(err)
	}
	if read(t, s, b, key) != body {
		t.Fatal("body mismatch")
	}
	assertTags(t, getObjectTags(t, s, b, key), []types.Tag{{Key: aws.String("bar"), Value: aws.String("")}, {Key: aws.String("foo"), Value: aws.String("bar")}})
}

// 로그인 정보가 있는 Post방식으로 태그정보, ACL을 포함한 오브젝트를 업로드 가능한지 확인
func TestPostObjectTagsAuthenticatedRequest(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	if s.cfg.Endpoint() == "" {
		t.Skip("authenticated POST tagging is disabled on AWS")
	}
	b := s.bucket(t, 12)
	expiration := time.Now().UTC().Add(100 * time.Minute).Format(time.RFC3339)
	document := map[string]any{"expiration": expiration, "conditions": []any{map[string]string{"bucket": b}, []string{"starts-with", "$key", "foo"}, map[string]string{"acl": "private"}, []string{"starts-with", "$Content-Type", "text/plain"}, []any{"content-length-range", 0, 1024}, []string{"starts-with", "$tagging", ""}}}
	encoded, _ := json.Marshal(document)
	policy := base64.StdEncoding.EncodeToString(encoded)
	fields := map[string]string{"key": "foo.txt", "AWSAccessKeyId": s.cfg.Main.AccessKey, "acl": "private", "signature": postV2Signature(policy, s.cfg.Main.SecretKey), "policy": policy, "tagging": "<Tagging><TagSet><Tag><Key>0</Key><Value>0</Value></Tag><Tag><Key>1</Key><Value>1</Value></Tag></TagSet></Tagging>", "Content-Type": "text/plain"}
	result := sendPostForm(t, postBucketURL(s, b), fields, "foo.txt", "text/plain", []byte("bar"))
	if result.status != 204 {
		t.Fatalf("POST status=%d body=%s", result.status, result.body)
	}
	if read(t, s, b, "foo.txt") != "bar" {
		t.Fatal("body mismatch")
	}
	assertTags(t, getObjectTags(t, s, b, "foo.txt"), tags(2, 0, 0))
}

// 업로드시 오브젝트의 태그 정보를 빈 값으로 올릴 경우 성공 확인
func TestGetObjNonTagging(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	b, key := s.bucket(t, 13), "TestGetObjNonTagging"
	put(t, s, b, key, "", nil)
	assertTags(t, getObjectTags(t, s, b, key), nil)
}

// x-amz-tagging 헤더에 URL 인코딩이 필요한 문자(공백, /, +, =)가 든 태그로 업로드하면 디코딩된 값으로 저장되는지 확인
func TestPutObjWithTagsEncoded(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	b, key := s.bucket(t, 14), "TestPutObjWithTagsEncoded"
	want := encodedTags()
	_, err := s.client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(b), Key: aws.String(key), Body: bytes.NewReader([]byte(key)), Tagging: aws.String(encodeTagging(want))})
	if err != nil {
		t.Fatal(err)
	}
	assertTags(t, getObjectTags(t, s, b, key), want)
}

// x-amz-tagging 헤더에 URL 인코딩이 필요한 문자가 든 태그로 멀티파트 업로드하면 디코딩된 값으로 저장되는지 확인
func TestMultipartUploadWithTagsEncoded(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	ctx := context.Background()
	b, key := s.bucket(t, 15), "TestMultipartUploadWithTagsEncoded"
	want := encodedTags()
	created, err := s.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: aws.String(b), Key: aws.String(key), Tagging: aws.String(encodeTagging(want))})
	if err != nil {
		t.Fatal(err)
	}
	part, err := s.client.UploadPart(ctx, &s3.UploadPartInput{Bucket: aws.String(b), Key: aws.String(key), UploadId: created.UploadId, PartNumber: aws.Int32(1), Body: bytes.NewReader([]byte(key))})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{Bucket: aws.String(b), Key: aws.String(key), UploadId: created.UploadId, MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{{ETag: part.ETag, PartNumber: aws.Int32(1)}}}}); err != nil {
		t.Fatal(err)
	}
	assertTags(t, getObjectTags(t, s, b, key), want)
}

// encodedTags는 x-amz-tagging 헤더에서 URL 인코딩이 필요한 문자(공백, /, +, =)가 든 태그 목록을 반환한다.
func encodedTags() []types.Tag {
	return []types.Tag{{Key: aws.String("k 1"), Value: aws.String("a/b+c d")}, {Key: aws.String("k2"), Value: aws.String("x=y")}}
}

// encodeTagging은 태그 목록을 x-amz-tagging 헤더 값(공백은 %20)으로 인코딩한다.
func encodeTagging(set []types.Tag) string {
	escape := func(v string) string { return strings.ReplaceAll(url.QueryEscape(v), "+", "%20") }
	pairs := make([]string, len(set))
	for i, tag := range set {
		pairs[i] = escape(aws.ToString(tag.Key)) + "=" + escape(aws.ToString(tag.Value))
	}
	return strings.Join(pairs, "&")
}

func tags(count, keySize, valueSize int) []types.Tag {
	result := make([]types.Tag, count)
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("%d", i)
		value := key
		if keySize > 0 {
			key = strings.Repeat(string(rune('a'+i%26)), keySize)
		}
		if valueSize > 0 {
			value = strings.Repeat(string(rune('A'+i%26)), valueSize)
		}
		result[i] = types.Tag{Key: aws.String(key), Value: aws.String(value)}
	}
	return result
}
func taggingsPutObjectTags(t *testing.T, s *suite, bucket, key string, set []types.Tag) error {
	t.Helper()
	_, err := s.client.PutObjectTagging(context.Background(), &s3.PutObjectTaggingInput{Bucket: aws.String(bucket), Key: aws.String(key), Tagging: &types.Tagging{TagSet: set}})
	return err
}
func getObjectTags(t *testing.T, s *suite, bucket, key string) []types.Tag {
	t.Helper()
	out, err := s.client.GetObjectTagging(context.Background(), &s3.GetObjectTaggingInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}
	return out.TagSet
}
func assertTags(t *testing.T, got, want []types.Tag) {
	t.Helper()
	sort.Slice(got, func(i, j int) bool { return aws.ToString(got[i].Key) < aws.ToString(got[j].Key) })
	sort.Slice(want, func(i, j int) bool { return aws.ToString(want[i].Key) < aws.ToString(want[j].Key) })
	if len(got) != len(want) {
		t.Fatalf("tags=%v want=%v", tagStrings(got), tagStrings(want))
	}
	for i := range want {
		if aws.ToString(got[i].Key) != aws.ToString(want[i].Key) || aws.ToString(got[i].Value) != aws.ToString(want[i].Value) {
			t.Fatalf("tags=%v want=%v", tagStrings(got), tagStrings(want))
		}
	}
}

// tagStrings는 실패 메시지용으로 태그를 "키=값" 문자열 목록으로 바꾼다.
func tagStrings(set []types.Tag) []string {
	result := make([]string, len(set))
	for i, tag := range set {
		result[i] = fmt.Sprintf("%q=%q", aws.ToString(tag.Key), aws.ToString(tag.Value))
	}
	return result
}

// tagSet은 키, 값 순서로 나열한 문자열에서 태그 목록을 만든다.
func tagSet(pairs ...string) []types.Tag {
	set := make([]types.Tag, 0, len(pairs)/2)
	for index := 0; index+1 < len(pairs); index += 2 {
		set = append(set, types.Tag{Key: aws.String(pairs[index]), Value: aws.String(pairs[index+1])})
	}
	return set
}

func putBucketTags(s *suite, bucket string, set []types.Tag) error {
	_, err := s.client.PutBucketTagging(context.Background(), &s3.PutBucketTaggingInput{Bucket: aws.String(bucket), Tagging: &types.Tagging{TagSet: set}})
	return err
}

func getBucketTags(t *testing.T, s *suite, bucket string) []types.Tag {
	t.Helper()
	out, err := s.client.GetBucketTagging(context.Background(), &s3.GetBucketTaggingInput{Bucket: aws.String(bucket)})
	if err != nil {
		t.Fatal(err)
	}
	return out.TagSet
}

// putObjectWithTagging은 x-amz-tagging 헤더를 붙여 빈 오브젝트를 올린다.
func putObjectWithTagging(s *suite, bucket, key, tagging string) error {
	_, err := s.client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader(nil), Tagging: aws.String(tagging)})
	return err
}

// 버킷 태그는 50개까지 넣을 수 있다
func TestBucketTaggingMax(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 16)
	want := tags(50, 0, 0)
	if err := putBucketTags(s, bucket, want); err != nil {
		t.Fatal(err)
	}
	assertTags(t, getBucketTags(t, s, bucket), want)

	assertS3Error(t, putBucketTags(s, bucket, tags(51, 0, 0)), 400, "BadRequest")
	if got := getBucketTags(t, s, bucket); len(got) != 50 {
		t.Fatalf("tags=%d want=50", len(got))
	}
}

// 버킷 태그의 키가 중복되면 거부된다
func TestBucketTaggingDuplicateKey(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	assertS3Error(t, putBucketTags(s, s.bucket(t, 17), tagSet("key", "value1", "key", "value2")), 400, "InvalidTag")
}

// 올바르지 않은 버킷 태그는 거부된다
func TestBucketTaggingInvalidTag(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 18)
	invalid := [][]types.Tag{
		tagSet("aws:key", "value"),                // aws: 접두어는 시스템 태그 전용
		tagSet(strings.Repeat("k", 129), "value"), // 키 128자 초과
		tagSet("key", strings.Repeat("v", 257)),   // 값 256자 초과
		tagSet("key?<>", "value"),                 // 허용하지 않는 문자
	}
	for _, set := range invalid {
		assertS3Error(t, putBucketTags(s, bucket, set), 400, "InvalidTag")
	}
	_, err := s.client.GetBucketTagging(context.Background(), &s3.GetBucketTaggingInput{Bucket: aws.String(bucket)})
	assertS3Error(t, err, 404, "NoSuchTagSet")
}

// 값이 빈 버킷 태그도 그대로 저장된다
func TestBucketTaggingEmptyValue(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 19)
	want := tagSet("key", "")
	if err := putBucketTags(s, bucket, want); err != nil {
		t.Fatal(err)
	}
	assertTags(t, getBucketTags(t, s, bucket), want)
}

// 빈 태그 목록을 넣으면 버킷 태그가 지워진다
func TestBucketTaggingEmptySet(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	ctx := context.Background()
	bucket := s.bucket(t, 20)
	if err := putBucketTags(s, bucket, tagSet("key", "value")); err != nil {
		t.Fatal(err)
	}

	if err := putBucketTags(s, bucket, []types.Tag{}); err != nil {
		t.Fatal(err)
	}
	_, err := s.client.GetBucketTagging(ctx, &s3.GetBucketTaggingInput{Bucket: aws.String(bucket)})
	assertS3Error(t, err, 404, "NoSuchTagSet")

	// 태그가 없어도 삭제는 성공한다
	out, err := s.client.DeleteBucketTagging(ctx, &s3.DeleteBucketTaggingInput{Bucket: aws.String(bucket)})
	if err != nil {
		t.Fatal(err)
	}
	if got := responseStatus(t, out.ResultMetadata); got != 204 {
		t.Fatalf("HTTP status = %d, want 204", got)
	}
}

// 없는 버킷에 대한 태그 요청은 실패한다
func TestBucketTaggingBucketNotExist(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	ctx := context.Background()
	bucket := s.bucketNameOnly(21)
	_, err := s.client.GetBucketTagging(ctx, &s3.GetBucketTaggingInput{Bucket: aws.String(bucket)})
	assertS3Error(t, err, 404, "NoSuchBucket")
	assertS3Error(t, putBucketTags(s, bucket, tagSet("key", "value")), 404, "NoSuchBucket")
	_, err = s.client.DeleteBucketTagging(ctx, &s3.DeleteBucketTaggingInput{Bucket: aws.String(bucket)})
	assertS3Error(t, err, 404, "NoSuchBucket")
}

// 오브젝트 태그의 키가 중복되면 거부된다
func TestObjTaggingDuplicateKey(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	b, key := s.bucket(t, 22), "TestObjTaggingDuplicateKey"
	put(t, s, b, key, "", nil)
	err := taggingsPutObjectTags(t, s, b, key, tagSet("key", "value1", "key", "value2"))
	assertS3Error(t, err, 400, "InvalidTag")
	assertTags(t, getObjectTags(t, s, b, key), nil)
}

// 올바르지 않은 오브젝트 태그는 거부된다
func TestObjTaggingInvalidTag(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	b, key := s.bucket(t, 23), "TestObjTaggingInvalidTag"
	put(t, s, b, key, "", nil)
	invalid := [][]types.Tag{
		tagSet("aws:key", "value"), // aws: 접두어는 시스템 태그 전용
		tagSet("key?<>", "value"),  // 허용하지 않는 문자
	}
	for _, set := range invalid {
		assertS3Error(t, taggingsPutObjectTags(t, s, b, key, set), 400, "InvalidTag")
	}
	assertTags(t, getObjectTags(t, s, b, key), nil)
}

// 허용 문자(영숫자, 공백, + - = . _ : / @)와 빈 값은 그대로 저장된다
func TestObjTaggingSpecialCharacters(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	b, key := s.bucket(t, 24), "TestObjTaggingSpecialCharacters"
	put(t, s, b, key, "", nil)
	want := tagSet("a+b-=._:/@ c", "v+-=._:/@ d", "empty", "")
	if err := taggingsPutObjectTags(t, s, b, key, want); err != nil {
		t.Fatal(err)
	}
	assertTags(t, getObjectTags(t, s, b, key), want)
}

// 빈 태그 목록을 넣으면 오브젝트 태그가 지워진다
func TestObjTaggingEmptySet(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	b, key := s.bucket(t, 25), "TestObjTaggingEmptySet"
	put(t, s, b, key, "", nil)
	if err := taggingsPutObjectTags(t, s, b, key, tagSet("key", "value")); err != nil {
		t.Fatal(err)
	}

	if err := taggingsPutObjectTags(t, s, b, key, []types.Tag{}); err != nil {
		t.Fatal(err)
	}
	assertTags(t, getObjectTags(t, s, b, key), nil)

	// 태그가 없어도 삭제는 성공한다
	out, err := s.client.DeleteObjectTagging(context.Background(), &s3.DeleteObjectTaggingInput{Bucket: aws.String(b), Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}
	if got := responseStatus(t, out.ResultMetadata); got != 204 {
		t.Fatalf("HTTP status = %d, want 204", got)
	}
}

// 없는 오브젝트에 대한 태그 요청은 실패한다
func TestObjTaggingKeyNotExist(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	ctx := context.Background()
	b, key := s.bucket(t, 26), "TestObjTaggingKeyNotExist"
	assertS3Error(t, taggingsPutObjectTags(t, s, b, key, tagSet("key", "value")), 404, "NoSuchKey")
	_, err := s.client.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{Bucket: aws.String(b), Key: aws.String(key)})
	assertS3Error(t, err, 404, "NoSuchKey")
	_, err = s.client.DeleteObjectTagging(ctx, &s3.DeleteObjectTaggingInput{Bucket: aws.String(b), Key: aws.String(key)})
	assertS3Error(t, err, 404, "NoSuchKey")
}

// 헤더로 태그를 10개 넘게 주면 업로드가 거부된다
func TestPutObjWithExcessTags(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	b, key := s.bucket(t, 27), "TestPutObjWithExcessTags"
	pairs := make([]string, 11)
	for index := range pairs {
		pairs[index] = fmt.Sprintf("key%d=value%d", index, index)
	}
	assertS3Error(t, putObjectWithTagging(s, b, key, strings.Join(pairs, "&")), 400, "BadRequest")
	_, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String(key)})
	assertS3Status(t, err, 404)
}

// 헤더로 준 태그가 올바르지 않으면 업로드가 거부된다
func TestPutObjWithInvalidTags(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	b, key := s.bucket(t, 28), "TestPutObjWithInvalidTags"
	// 헤더로 준 태그의 키가 중복되면 InvalidArgument
	assertS3Error(t, putObjectWithTagging(s, b, key, "key=value1&key=value2"), 400, "InvalidArgument")
	for _, tagging := range []string{"aws:key=value", "key%3F=value", strings.Repeat("k", 129) + "=value"} {
		assertS3Error(t, putObjectWithTagging(s, b, key, tagging), 400, "InvalidTag")
	}
	_, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String(key)})
	assertS3Status(t, err, 404)
}

// 버전을 지정하면 그 버전의 태그만 바뀐다
func TestObjTaggingVersion(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	ctx := context.Background()
	b, key := s.bucket(t, 29), "TestObjTaggingVersion"
	enableVersioning(t, s, b)
	version1 := aws.ToString(put(t, s, b, key, "1", nil).VersionId)
	version2 := aws.ToString(put(t, s, b, key, "2", nil).VersionId)
	want := tagSet("version", "1")

	putOut, err := s.client.PutObjectTagging(ctx, &s3.PutObjectTaggingInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: aws.String(version1), Tagging: &types.Tagging{TagSet: want}})
	if err != nil || aws.ToString(putOut.VersionId) != version1 {
		t.Fatalf("PutObjectTagging version=%v want=%q err=%v", putOut, version1, err)
	}

	getOut, err := s.client.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: aws.String(version1)})
	if err != nil || aws.ToString(getOut.VersionId) != version1 {
		t.Fatalf("GetObjectTagging version=%v want=%q err=%v", getOut, version1, err)
	}
	assertTags(t, getOut.TagSet, want)

	// 버전을 지정하지 않으면 최신 버전이 대상이다
	getOut, err = s.client.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{Bucket: aws.String(b), Key: aws.String(key)})
	if err != nil || aws.ToString(getOut.VersionId) != version2 || len(getOut.TagSet) != 0 {
		t.Fatalf("GetObjectTagging latest=%v want version=%q err=%v", getOut, version2, err)
	}

	deleteOut, err := s.client.DeleteObjectTagging(ctx, &s3.DeleteObjectTaggingInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: aws.String(version1)})
	if err != nil || aws.ToString(deleteOut.VersionId) != version1 {
		t.Fatalf("DeleteObjectTagging version=%v want=%q err=%v", deleteOut, version1, err)
	}
	getOut, err = s.client.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: aws.String(version1)})
	if err != nil || len(getOut.TagSet) != 0 {
		t.Fatalf("GetObjectTagging after delete=%v err=%v", getOut, err)
	}
}

// 올바르지 않은 버전 ID 로 보낸 태그 요청은 거부된다
func TestObjTaggingInvalidVersion(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	ctx := context.Background()
	b, key := s.bucket(t, 30), "TestObjTaggingInvalidVersion"
	enableVersioning(t, s, b)
	put(t, s, b, key, "1", nil)
	_, err := s.client.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: aws.String("invalid")})
	assertS3Error(t, err, 400, "InvalidArgument")
	_, err = s.client.PutObjectTagging(ctx, &s3.PutObjectTaggingInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: aws.String("invalid"), Tagging: &types.Tagging{TagSet: tagSet("key", "value")}})
	assertS3Error(t, err, 400, "InvalidArgument")
	_, err = s.client.DeleteObjectTagging(ctx, &s3.DeleteObjectTaggingInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: aws.String("invalid")})
	assertS3Error(t, err, 400, "InvalidArgument")
}

// 최신 버전이 삭제 마커이면 버전을 지정하든 않든 태그 요청은 405
func TestObjTaggingDeleteMarker(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	ctx := context.Background()
	b, key := s.bucket(t, 31), "TestObjTaggingDeleteMarker"
	enableVersioning(t, s, b)
	versionID := put(t, s, b, key, "1", nil).VersionId
	if err := taggingsPutObjectTags(t, s, b, key, tagSet("key", "value")); err != nil {
		t.Fatal(err)
	}
	deleted, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(b), Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}

	for _, version := range []*string{nil, deleted.VersionId} {
		_, err = s.client.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: version})
		assertS3Error(t, err, 405, "MethodNotAllowed")
		_, err = s.client.PutObjectTagging(ctx, &s3.PutObjectTaggingInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: version, Tagging: &types.Tagging{TagSet: tagSet("key", "value2")}})
		assertS3Error(t, err, 405, "MethodNotAllowed")
		_, err = s.client.DeleteObjectTagging(ctx, &s3.DeleteObjectTaggingInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: version})
		assertS3Error(t, err, 405, "MethodNotAllowed")
	}

	// 삭제 마커 아래의 실제 버전은 버전을 지정하면 그대로 조회된다
	out, err := s.client.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: versionID})
	if err != nil {
		t.Fatal(err)
	}
	assertTags(t, out.TagSet, tagSet("key", "value"))
}
