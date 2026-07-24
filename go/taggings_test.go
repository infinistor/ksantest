package s3tests

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func TestTaggings(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		run  func(*testing.T)
	}{
		// 버킷에 사용자 추가 태그값을 설정할경우 성공확인
		{"test_set_tagging", testBucketTaggingCRUD},
		// 오브젝트에 태그 설정이 올바르게 적용되는지 확인
		{"test_get_obj_tagging", func(t *testing.T) { testObjectTaggingCase(t, "test_get_obj_tagging") }},
		// 오브젝트에 태그 설정이 올바르게 적용되는지 헤더정보를 통해 확인
		{"test_get_obj_head_tagging", func(t *testing.T) { testObjectTaggingCase(t, "test_get_obj_head_tagging") }},
		// 추가가능한 최대갯수까지 태그를 입력할 수 있는지 확인(max = 10)
		{"test_put_max_tags", func(t *testing.T) { testObjectTaggingCase(t, "test_put_max_tags") }},
		// 추가가능한 최대갯수를 넘겨서 태그를 입력할때 에러 확인
		{"test_put_excess_tags", func(t *testing.T) { testObjectTaggingCase(t, "test_put_excess_tags") }},
		// 태그의 key값의 길이가 최대(128) value값의 길이가 최대(256)일때 태그를 입력할 수 있는지 확인
		{"test_put_max_size_tags", func(t *testing.T) { testObjectTaggingCase(t, "test_put_max_size_tags") }},
		// 태그의 key값의 길이가 최대(129) value값의 길이가 최대(256)일때 태그 입력 실패 확인
		{"test_put_excess_key_tags", func(t *testing.T) { testObjectTaggingCase(t, "test_put_excess_key_tags") }},
		// 태그의 key값의 길이가 최대(128) value값의 길이가 최대(257)일때 태그 입력 실패 확인
		{"test_put_excess_val_tags", func(t *testing.T) { testObjectTaggingCase(t, "test_put_excess_val_tags") }},
		// 오브젝트의 태그목록을 덮어쓰기 가능한지 확인
		{"test_put_modify_tags", func(t *testing.T) { testObjectTaggingCase(t, "test_put_modify_tags") }},
		// 오브젝트의 태그를 삭제 가능한지 확인
		{"test_put_delete_tags", func(t *testing.T) { testObjectTaggingCase(t, "test_put_delete_tags") }},
		// 헤더에 태그정보를 포함한 오브젝트 업로드 성공 확인
		{"test_put_obj_with_tags", testPutObjectWithTags},
		// 로그인 정보가 있는 Post방식으로 태그정보, ACL을 포함한 오브젝트를 업로드 가능한지 확인
		{"test_post_object_tags_authenticated_request", testPostObjectTags},
		// 업로드시 오브젝트의 태그 정보를 빈 값으로 올릴 경우 성공 확인
		{"test_get_obj_non_tagging", func(t *testing.T) { testObjectTaggingCase(t, "test_get_obj_non_tagging") }},
	}
	for _, tc := range tests {
		t.Run(tc.name, tc.run)
	}
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
		t.Fatalf("tags=%v want=%v", got, want)
	}
	for i := range want {
		if aws.ToString(got[i].Key) != aws.ToString(want[i].Key) || aws.ToString(got[i].Value) != aws.ToString(want[i].Value) {
			t.Fatalf("tags=%v want=%v", got, want)
		}
	}
}

func testBucketTaggingCRUD(t *testing.T) {
	s := newSuite(t)
	b := s.bucket(t)
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

func testObjectTaggingCase(t *testing.T, name string) {
	s := newSuite(t)
	b, key := s.bucket(t), name
	put(t, s, b, key, "", nil)
	switch name {
	case "test_get_obj_non_tagging":
		assertTags(t, getObjectTags(t, s, b, key), nil)
	case "test_get_obj_tagging":
		want := tags(2, 0, 0)
		if err := taggingsPutObjectTags(t, s, b, key, want); err != nil {
			t.Fatal(err)
		}
		assertTags(t, getObjectTags(t, s, b, key), want)
	case "test_get_obj_head_tagging":
		want := tags(2, 0, 0)
		if err := taggingsPutObjectTags(t, s, b, key, want); err != nil {
			t.Fatal(err)
		}
		head, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String(key)})
		if err != nil || aws.ToInt32(head.TagCount) != 2 {
			t.Fatalf("TagCount=%v err=%v", head.TagCount, err)
		}
	// [Max] put max tags
	case "test_put_max_tags", "test_put_max_size_tags":
		keySize, valueSize := 0, 0
		if strings.Contains(name, "size") {
			keySize, valueSize = 128, 256
		}
		want := tags(10, keySize, valueSize)
		if err := taggingsPutObjectTags(t, s, b, key, want); err != nil {
			t.Fatal(err)
		}
		assertTags(t, getObjectTags(t, s, b, key), want)
	case "test_put_excess_tags":
		err := taggingsPutObjectTags(t, s, b, key, tags(11, 0, 0))
		assertS3Error(t, err, 400, "BadRequest")
		assertTags(t, getObjectTags(t, s, b, key), nil)
	case "test_put_excess_key_tags":
		err := taggingsPutObjectTags(t, s, b, key, tags(10, 129, 256))
		assertS3Error(t, err, 400, "InvalidTag")
		assertTags(t, getObjectTags(t, s, b, key), nil)
	case "test_put_excess_val_tags":
		err := taggingsPutObjectTags(t, s, b, key, tags(10, 128, 259))
		assertS3Error(t, err, 400, "InvalidTag")
		assertTags(t, getObjectTags(t, s, b, key), nil)
	case "test_put_modify_tags":
		first, second := tags(2, 0, 0), tags(1, 128, 128)
		if err := taggingsPutObjectTags(t, s, b, key, first); err != nil {
			t.Fatal(err)
		}
		assertTags(t, getObjectTags(t, s, b, key), first)
		if err := taggingsPutObjectTags(t, s, b, key, second); err != nil {
			t.Fatal(err)
		}
		assertTags(t, getObjectTags(t, s, b, key), second)
	case "test_put_delete_tags":
		want := tags(2, 0, 0)
		if err := taggingsPutObjectTags(t, s, b, key, want); err != nil {
			t.Fatal(err)
		}
		_, err := s.client.DeleteObjectTagging(context.Background(), &s3.DeleteObjectTaggingInput{Bucket: aws.String(b), Key: aws.String(key)})
		if err != nil {
			t.Fatal(err)
		}
		assertTags(t, getObjectTags(t, s, b, key), nil)
	default:
		t.Fatalf("unimplemented tagging case %q", name)
	}
}

func testPutObjectWithTags(t *testing.T) {
	s := newSuite(t)
	b, key := s.bucket(t), "test tag obj1"
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

func testPostObjectTags(t *testing.T) {
	s := newSuite(t)
	if s.cfg.Endpoint() == "" {
		t.Skip("authenticated POST tagging is disabled on AWS")
	}
	b := s.bucket(t)
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
