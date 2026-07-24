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

func TestPutObject(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		run  func(*testing.T)
	}{
		// 오브젝트가 올바르게 생성되는지 확인
		{"test_bucket_list_distinct", func(t *testing.T) { testPutCore(t, "test_bucket_list_distinct") }},
		// 존재하지 않는 버킷에 오브젝트 업로드할 경우 실패 확인
		{"test_object_write_to_non_exist_bucket", func(t *testing.T) { testPutCore(t, "test_object_write_to_non_exist_bucket") }},
		// 0바이트로 업로드한 오브젝트가 실제로 0바이트인지 확인
		{"test_object_head_zero_bytes", func(t *testing.T) { testPutCore(t, "test_object_head_zero_bytes") }},
		// 업로드한 오브젝트의 ETag가 올바른지 확인
		{"test_object_write_check_etag", func(t *testing.T) { testPutCore(t, "test_object_write_check_etag") }},
		// 캐시(시간)를 설정하고 업로드한 오브젝트가 올바르게 반영되었는지 확인
		{"test_object_write_cache_control", func(t *testing.T) { testPutCore(t, "test_object_write_cache_control") }},
		// 캐시(날짜)를 설정하고 업로드한 오브젝트가 올바르게 반영되었는지 확인
		{"test_object_write_expires", func(t *testing.T) { testPutCore(t, "test_object_write_expires") }},
		// 오브젝트의 기본 작업을 모드 올바르게 할 수 있는지 확인(read, write, update, delete)
		{"test_object_write_read_update_read_delete", func(t *testing.T) { testPutCore(t, "test_object_write_read_update_read_delete") }},
		// 오브젝트에 메타데이터를 추가하여 업로드 할 경우 올바르게 적용되었는지 확인
		{"test_object_set_get_metadata_none_to_good", func(t *testing.T) { testPutCore(t, "test_object_set_get_metadata_none_to_good") }},
		// 오브젝트에 빈 메타데이터를 추가하여 업로드 할 경우 올바르게 적용되었는지 확인
		{"test_object_set_get_metadata_none_to_empty", func(t *testing.T) { testPutCore(t, "test_object_set_get_metadata_none_to_empty") }},
		// 메타 데이터 업데이트가 올바르게 적용되었는지 확인
		{"test_object_set_get_metadata_overwrite_to_empty", func(t *testing.T) { testPutCore(t, "test_object_set_get_metadata_overwrite_to_empty") }},
		// 메타데이터에 올바르지 않는 문자열[EOF(\x04)를 사용할 경우 실패 확인
		{"test_object_set_get_non_utf8_metadata", func(t *testing.T) { testPutCore(t, "test_object_set_get_non_utf8_metadata") }},
		// 메타데이터에 올바르지 않는 문자[EOF(\x04)를 문자열 맨앞에 사용할 경우 실패 확인
		{"test_object_set_get_metadata_empty_to_unreadable_prefix", func(t *testing.T) { testPutCore(t, "test_object_set_get_metadata_empty_to_unreadable_prefix") }},
		// 메타데이터에 올바르지 않는 문자[EOF(\x04)를 문자열 맨뒤에 사용할 경우 실패 확인
		{"test_object_set_get_metadata_empty_to_unreadable_suffix", func(t *testing.T) { testPutCore(t, "test_object_set_get_metadata_empty_to_unreadable_suffix") }},
		// 오브젝트를 메타데이타 없이 덮어쓰기 했을 때, 메타데이타 값이 비어있는지 확인
		{"test_object_metadata_replaced_on_put", func(t *testing.T) { testPutCore(t, "test_object_metadata_replaced_on_put") }},
		// body의 내용을utf-8로 인코딩한 오브젝트를 업로드 했을때 올바르게 업로드 되었는지 확인
		{"test_object_write_file", func(t *testing.T) { testPutCore(t, "test_object_write_file") }},
		// 오브젝트 이름과 내용이 모두 특수문자인 오브젝트 여러개를 업로드 할 경우 모두 재대로 업로드 되는지 확인
		{"test_bucket_create_special_key_names", func(t *testing.T) { testPutSpecialKeys(t, "test_bucket_create_special_key_names") }},
		// 확인
		{"test_bucket_list_special_prefix", func(t *testing.T) { testPutSpecialKeys(t, "test_bucket_list_special_prefix") }},
		// 메타데이터를 통해 확인
		{"test_object_lock_uploading_obj", func(t *testing.T) { testPutCore(t, "test_object_lock_uploading_obj") }},
		// 오브젝트의 중간에 공백문자가 들어갔을 경우 올바르게 동작하는지 확인
		{"test_object_infix_space", func(t *testing.T) { testPutSpecialKeys(t, "test_object_infix_space") }},
		// 오브젝트의 마지막에 공백문자가 들어갔을 경우 올바르게 동작하는지 확인
		{"test_object_suffix_space", func(t *testing.T) { testPutSpecialKeys(t, "test_object_suffix_space") }},
		// [AWS SDK V2] 특수문자를 포함한 오브젝트 업로드 성공 확인
		{"test_put_object_special_characters", func(t *testing.T) { testPutSpecialKeys(t, "test_put_object_special_characters") }},
		// [AWS SDK V2, UseChunkEncoding = true] 특수문자를 포함한 오브젝트 업로드 성공 확인
		{"test_put_object_special_characters_use_chunk_encoding", func(t *testing.T) { testPutSpecialKeys(t, "test_put_object_special_characters_use_chunk_encoding") }},
		// [AWS SDK V2, UseChunkEncoding = true, DisablePayloadSigning = true] 특수문자를 포함한 오브젝트 업로드 성공 확인
		{"test_put_object_use_special_characters_chunk_encoding_and_disable_payload_signing", func(t *testing.T) { testPutSpecialKeys(t, "test_put_object_use_special_characters_chunk_encoding_and_disable_payload_signing") }},
		// [AWS SDK V2, UseChunkEncoding = false] 특수문자를 포함한 오브젝트 업로드 성공 확인
		{"test_put_object_special_characters_not_chunk_encoding", func(t *testing.T) { testPutSpecialKeys(t, "test_put_object_special_characters_not_chunk_encoding") }},
		// [AWS SDK V2, UseChunkEncoding = false, DisablePayloadSigning = true] 특수문자를 포함한 오브젝트 업로드 성공 확인
		{"test_put_object_special_characters_not_chunk_encoding_and_disable_payload_signing", func(t *testing.T) { testPutSpecialKeys(t, "test_put_object_special_characters_not_chunk_encoding_and_disable_payload_signing") }},
		// 폴더의 이름과 동일한 오브젝트 업로드가 가능한지 확인
		{"test_put_object_dir_and_file", func(t *testing.T) { testPutCore(t, "test_put_object_dir_and_file") }},
		// 오브젝트를 여러번 업로드 했을때 올바르게 반영되는지 확인
		{"test_object_overwrite", func(t *testing.T) { testPutCore(t, "test_object_overwrite") }},
		// 오브젝트 이름에 이모지가 포함될 경우 올바르게 업로드 되는지 확인
		{"test_object_emoji", func(t *testing.T) { testPutCore(t, "test_object_emoji") }},
		// 메타데이터에 utf-8이 포함될 경우 올바르게 업로드 되는지 확인
		{"test_object_set_get_metadata_utf8", func(t *testing.T) { testPutCore(t, "test_object_set_get_metadata_utf8") }},
		// useChunkEncoding을 사용하는 오브젝트 업로드 시 체크섬 계산 및 검증 확인
		{"test_put_object_checksum_use_chunk_encoding", func(t *testing.T) { testPutChecksums(t, "test_put_object_checksum_use_chunk_encoding") }},
		// useChunkEncoding을 사용하지 않는 오브젝트 업로드 시 체크섬 계산 및 검증 확인
		{"test_put_object_checksum", func(t *testing.T) { testPutChecksums(t, "test_put_object_checksum") }},
		// 사전 계산한 체크섬 값을 직접 지정하여 오브젝트 업로드 시 검증 성공 확인
		{"test_put_object_checksum_with_value", func(t *testing.T) { testPutChecksums(t, "test_put_object_checksum_with_value") }},
		// 잘못된 체크섬 값을 지정하여 오브젝트 업로드 시 BadDigest 실패 확인
		{"test_put_object_checksum_failure", func(t *testing.T) { testPutChecksums(t, "test_put_object_checksum_failure") }},
		// 일치하는 If-Match 조건으로 오브젝트 덮어쓰기 성공 확인
		{"test_put_object_if_match_good", func(t *testing.T) { testPutConditions(t, "test_put_object_if_match_good") }},
		// 일치하지 않는 If-Match 조건으로 오브젝트 덮어쓰기 시 412 실패 확인
		{"test_put_object_if_match_failed", func(t *testing.T) { testPutConditions(t, "test_put_object_if_match_failed") }},
		// 존재하지 않는 키에 If-None-Match: * 조건으로 업로드 성공 확인
		{"test_put_object_if_none_match_good", func(t *testing.T) { testPutConditions(t, "test_put_object_if_none_match_good") }},
		// 이미 존재하는 키에 If-None-Match: * 조건으로 업로드 시 412 실패 확인
		{"test_put_object_if_none_match_failed", func(t *testing.T) { testPutConditions(t, "test_put_object_if_none_match_failed") }},
		// If-Match와 If-None-Match를 함께 지정하면 501로 거부되는지 확인
		{"test_put_object_if_match_and_if_none_match", func(t *testing.T) { testPutConditions(t, "test_put_object_if_match_and_if_none_match") }},
		// 최대 길이(1024자)의 오브젝트 키로 업로드 성공 확인
		{"test_put_object_key_max_length", func(t *testing.T) { testPutKeyBoundary(t, "test_put_object_key_max_length") }},
		// 최소 길이(1자)의 오브젝트 키로 업로드 성공 확인
		{"test_put_object_key_min_length", func(t *testing.T) { testPutKeyBoundary(t, "test_put_object_key_min_length") }},
		// 최대 길이(1024자)를 초과하는 오브젝트 키로 업로드 실패 확인
		{"test_put_object_key_too_long", func(t *testing.T) { testPutKeyBoundary(t, "test_put_object_key_too_long") }},
		// 특수문자로 시작하는 오브젝트 키로 업로드 성공 확인
		{"test_put_object_key_special_characters_at_start", func(t *testing.T) { testPutSpecialKeys(t, "test_put_object_key_special_characters_at_start") }},
		// 특수문자로 끝나는 오브젝트 키로 업로드 성공 확인
		{"test_put_object_key_special_characters_at_end", func(t *testing.T) { testPutSpecialKeys(t, "test_put_object_key_special_characters_at_end") }},
		// 유니코드 문자를 포함한 오브젝트 키로 업로드 성공 확인
		{"test_put_object_key_unicode_characters", func(t *testing.T) { testPutKeyBoundary(t, "test_put_object_key_unicode_characters") }},
		// 1024바이트를 초과하는 키로 업로드 실패 확인
		{"test_put_object_key_unicode_characters_too_long", func(t *testing.T) { testPutKeyBoundary(t, "test_put_object_key_unicode_characters_too_long") }},
		// 앞뒤 공백문자를 포함한 오브젝트 키로 업로드 성공 확인
		{"test_put_object_key_with_leading_and_trailing_spaces", func(t *testing.T) { testPutKeyBoundary(t, "test_put_object_key_with_leading_and_trailing_spaces") }},
		// 연속된 슬래시를 포함한 오브젝트 키로 업로드 성공 확인
		{"test_put_object_key_with_consecutive_slashes", func(t *testing.T) { testPutKeyBoundary(t, "test_put_object_key_with_consecutive_slashes") }},
		// 다양한 경계 길이의 오브젝트 키로 업로드 성공 확인
		{"test_put_object_key_boundary_lengths", func(t *testing.T) { testPutKeyBoundary(t, "test_put_object_key_boundary_lengths") }},
	}
	for _, tc := range tests {
		t.Run(tc.name, tc.run)
	}
}

func testPutCore(t *testing.T, name string) {
	t.Helper()
	s := newSuite(t)
	ctx := context.Background()
	switch name {
	case "test_bucket_list_distinct":
		first, second := s.bucket(t), s.bucket(t)
		put(t, s, first, "foo", "bar", nil)
		out, err := s.client.ListObjects(ctx, &s3.ListObjectsInput{Bucket: aws.String(second)})
		if err != nil || len(out.Contents) != 0 {
			t.Fatalf("contents=%v err=%v", out.Contents, err)
		}
	case "test_object_write_to_non_exist_bucket":
		_, err := s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("missing-" + uniqueBucketSuffix(t)), Key: aws.String("foo"), Body: bytes.NewReader([]byte("bar"))})
		assertS3Error(t, err, 404, "NoSuchBucket")
	case "test_object_head_zero_bytes":
		bucket := s.bucket(t)
		put(t, s, bucket, "foo", "", nil)
		out, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String("foo")})
		if err != nil || aws.ToInt64(out.ContentLength) != 0 {
			t.Fatalf("length=%v err=%v", out.ContentLength, err)
		}
	case "test_object_write_check_etag":
		out := put(t, s, s.bucket(t), "foo", "bar", nil)
		if strings.Trim(aws.ToString(out.ETag), `"`) != "37b51d194a7513e45b56f6524f2d51f2" {
			t.Fatalf("ETag=%q", aws.ToString(out.ETag))
		}
	case "test_object_write_cache_control":
		bucket, value := s.bucket(t), "public, max-age=14HttpStatus.SC_BAD_REQUEST"
		_, err := s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String("foo"), Body: bytes.NewReader([]byte("bar")), ContentLength: aws.Int64(3), ContentType: aws.String("text/plain"), CacheControl: aws.String(value)})
		if err != nil {
			t.Fatal(err)
		}
		head, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String("foo")})
		if err != nil || aws.ToString(head.CacheControl) != value || read(t, s, bucket, "foo") != "bar" {
			t.Fatalf("cache=%q err=%v", aws.ToString(head.CacheControl), err)
		}
	case "test_object_write_expires":
		// Java skips: Expires is not persisted by the Java SDK client path.
		t.Skip("JAVA에서는 헤더만료일시 설정이 내부전용으로 되어있어 설정되지 않음")
	case "test_object_write_read_update_read_delete":
		bucket := s.bucket(t)
		put(t, s, bucket, "foo", "bar", nil)
		if read(t, s, bucket, "foo") != "bar" {
			t.Fatal("initial body mismatch")
		}
		put(t, s, bucket, "foo", "soup", nil)
		if read(t, s, bucket, "foo") != "soup" {
			t.Fatal("updated body mismatch")
		}
		if _, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String("foo")}); err != nil {
			t.Fatal(err)
		}
	// [metadata] object set get metadata none to good
	case "test_object_set_get_metadata_none_to_good", "test_object_set_get_metadata_none_to_empty", "test_object_set_get_metadata_overwrite_to_empty", "test_object_set_get_non_utf8_metadata", "test_object_set_get_metadata_empty_to_unreadable_prefix", "test_object_set_get_metadata_empty_to_unreadable_suffix", "test_object_metadata_replaced_on_put", "test_object_set_get_metadata_utf8":
		testPutMetadata(t, s, name)
	case "test_object_write_file":
		bucket := s.bucket(t)
		put(t, s, bucket, "foo", string([]byte{'b', 'a', 'r'}), nil)
		if read(t, s, bucket, "foo") != "bar" {
			t.Fatal("ASCII body mismatch")
		}
	case "test_object_lock_uploading_obj":
		testPutObjectLock(t, s)
	case "test_put_object_dir_and_file":
		for _, keys := range [][]string{{"aaa", "aaa/"}, {"aaa/", "aaa"}, {"aaa", "aaa/bbb/ccc"}} {
			bucket := s.bucket(t)
			for _, key := range keys {
				put(t, s, bucket, key, strings.TrimSuffix(key, "/"), nil)
			}
			out, err := s.client.ListObjects(ctx, &s3.ListObjectsInput{Bucket: aws.String(bucket)})
			if err != nil || len(out.Contents) != 2 {
				t.Fatalf("contents=%v err=%v", out.Contents, err)
			}
		}
	case "test_object_overwrite":
		bucket := s.bucket(t)
		put(t, s, bucket, "temp", strings.Repeat("a", 10*1024), nil)
		want := strings.Repeat("b", 1024*1024)
		put(t, s, bucket, "temp", want, nil)
		if got := read(t, s, bucket, "temp"); got != want {
			t.Fatalf("length=%d want=%d", len(got), len(want))
		}
	case "test_object_emoji":
		bucket, key := s.bucket(t), "test❤🍕🍔🚗"
		put(t, s, bucket, key, key, nil)
		out, err := s.client.ListObjects(ctx, &s3.ListObjectsInput{Bucket: aws.String(bucket)})
		if err != nil || len(out.Contents) != 1 || aws.ToString(out.Contents[0].Key) != key {
			t.Fatalf("contents=%v err=%v", out.Contents, err)
		}
	default:
		t.Fatalf("unimplemented PutObject case %q", name)
	}
}

func testPutMetadata(t *testing.T, s *suite, name string) {
	t.Helper()
	ctx := context.Background()
	bucket, key := s.bucket(t), "foo"
	switch name {
	// [metadata] object set get non utf8 metadata
	case "test_object_set_get_non_utf8_metadata", "test_object_set_get_metadata_empty_to_unreadable_prefix", "test_object_set_get_metadata_empty_to_unreadable_suffix":
		value := "\nmy_meta"
		if strings.Contains(name, "prefix") {
			value = "\nasdf"
		}
		if strings.Contains(name, "suffix") {
			value = "asdf\n"
		}
		_, err := s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte("bar")), Metadata: map[string]string{"meta1": value}})
		if err == nil {
			t.Fatal("invalid metadata was accepted")
		}
		return
	case "test_object_metadata_replaced_on_put":
		put(t, s, bucket, key, "bar", map[string]string{"meta1": "bar"})
		put(t, s, bucket, key, "bar", nil)
		head, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
		if err != nil || len(head.Metadata) != 0 {
			t.Fatalf("metadata=%v err=%v", head.Metadata, err)
		}
		return
	case "test_object_set_get_metadata_utf8":
		_, err := s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte("bar")), ContentType: aws.String("text/plain; charset=UTF-8"), Metadata: map[string]string{"meta1": "utf-8", "meta2": "UTF-8"}})
		if err != nil {
			t.Fatal(err)
		}
		head, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
		if err != nil || head.Metadata["meta1"] != "utf-8" || head.Metadata["meta2"] != "UTF-8" {
			t.Fatalf("metadata=%v err=%v", head.Metadata, err)
		}
		return
	}
	value := "my"
	if strings.Contains(name, "empty") {
		value = ""
	}
	put(t, s, bucket, key, key, map[string]string{"meta1": "my"})
	if name != "test_object_set_get_metadata_none_to_good" {
		put(t, s, bucket, key, key, map[string]string{"meta1": value})
	}
	head, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	got, ok := head.Metadata["meta1"]
	if err != nil || !ok || got != value {
		t.Fatalf("metadata=%v err=%v", head.Metadata, err)
	}
}

func testPutObjectLock(t *testing.T, s *suite) {
	t.Helper()
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
	key := "testObjectLockUploadingObjV2"
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
}

func testPutSpecialKeys(t *testing.T, name string) {
	t.Helper()
	s := newSuite(t)
	ctx := context.Background()
	bucket := s.bucket(t)
	keys := []string{"!", "!/", "!/!", "$", "$/", "$/$", "'", "'/", "'/'", "(", "(/", "(/(", ")", ")/", ")/)", "*", "*/", "*/*", ":", ":/", ":/:", "[", "[/", "[/[", "]", "]/", "]/]"}
	switch name {
	case "test_bucket_create_special_key_names":
		keys = []string{"!", "-", "_", ".", "'", "()", "&", "$", "@", "=", ";", "/", ":", "+", "  ", ",", "?", "{}", "^", "%", "`", "[]", "<>", "~", "#", "|"}
	case "test_bucket_list_special_prefix":
		keys = []string{"Bla/1", "Bla/2", "Bla/3", "Bla/4", "abcd"}
	case "test_object_infix_space":
		keys = []string{"a a/", "b b/f1", "c/f 2", "d d/f 3"}
	case "test_object_suffix_space":
		keys = []string{"a /", "b /f1", "c/f2 ", "d /f3 "}
	}
	for _, key := range keys {
		body := key
		if strings.HasSuffix(key, "/") {
			body = ""
		}
		put(t, s, bucket, key, body, nil)
	}
	out, err := s.client.ListObjects(ctx, &s3.ListObjectsInput{Bucket: aws.String(bucket), Prefix: func() *string {
		if name == "test_bucket_list_special_prefix" {
			return aws.String("Bla/")
		}
		return nil
	}()})
	want := len(keys)
	if name == "test_bucket_list_special_prefix" {
		want = 4
	}
	if err != nil || len(out.Contents) != want {
		t.Fatalf("contents=%d want=%d err=%v", len(out.Contents), want, err)
	}
	for _, object := range out.Contents {
		key := aws.ToString(object.Key)
		if got := read(t, s, bucket, key); got != strings.TrimSuffix(key, "/") && !(strings.HasSuffix(key, "/") && got == "") {
			t.Fatalf("key=%q body=%q", key, got)
		}
	}
}

func testPutChecksums(t *testing.T, name string) {
	t.Helper()
	s := newSuite(t)
	bucket := s.bucket(t)
	algorithms := []types.ChecksumAlgorithm{types.ChecksumAlgorithmCrc32, types.ChecksumAlgorithmCrc32c, types.ChecksumAlgorithmSha1, types.ChecksumAlgorithmSha256}
	for _, algorithm := range algorithms {
		algorithm := algorithm
		t.Run(string(algorithm), func(t *testing.T) {
			key := name + "/" + string(algorithm)
			body := []byte(key)
			input := &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader(body), ChecksumAlgorithm: algorithm}
			setPutChecksum(input, algorithm, body, name == "test_put_object_checksum_failure")
			out, err := s.client.PutObject(context.Background(), input)
			if name == "test_put_object_checksum_failure" {
				assertS3Error(t, err, 400, "BadDigest")
				return
			}
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

func testPutConditions(t *testing.T, name string) {
	t.Helper()
	s := newSuite(t)
	bucket, key := s.bucket(t), name
	first := put(t, s, bucket, key, "old", nil)
	ctx := context.Background()
	input := &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte("new"))}
	wantStatus, wantCode, wantBody := 0, "", "new"
	switch name {
	case "test_put_object_if_match_good":
		input.IfMatch = first.ETag
	case "test_put_object_if_match_failed":
		input.IfMatch = aws.String("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
		wantStatus, wantCode, wantBody = 412, "PreconditionFailed", "old"
	case "test_put_object_if_none_match_good":
		key += "-new"
		input.Key = aws.String(key)
		input.IfNoneMatch = aws.String("*")
	case "test_put_object_if_none_match_failed":
		input.IfNoneMatch = aws.String("*")
		wantStatus, wantCode, wantBody = 412, "PreconditionFailed", "old"
	case "test_put_object_if_match_and_if_none_match":
		input.IfMatch = first.ETag
		input.IfNoneMatch = aws.String("*")
		wantStatus, wantCode, wantBody = 501, "NotImplemented", "old"
	}
	_, err := s.client.PutObject(ctx, input)
	if wantStatus == 0 {
		if err != nil {
			t.Fatal(err)
		}
	} else {
		assertS3Error(t, err, wantStatus, wantCode)
	}
	if got := read(t, s, bucket, key); got != wantBody {
		t.Fatalf("body=%q want=%q", got, wantBody)
	}
}

func testPutKeyBoundary(t *testing.T, name string) {
	t.Helper()
	s := newSuite(t)
	bucket := s.bucket(t)
	putAndRead := func(key, body string) {
		t.Helper()
		out := put(t, s, bucket, key, body, nil)
		if aws.ToString(out.ETag) == "" || read(t, s, bucket, key) != body {
			t.Fatalf("key bytes=%d ETag=%q", len([]byte(key)), aws.ToString(out.ETag))
		}
	}
	alpha := func(n int) string { return strings.Repeat("a", n) }
	switch name {
	case "test_put_object_key_max_length":
		putAndRead(alpha(1024), "test-max-length")
	case "test_put_object_key_min_length":
		putAndRead("a", "test-min-length")
	case "test_put_object_key_too_long":
		// KSAN accepts keys longer than 1024; Java intentional failure.
		t.Skip("KSAN accepts keys longer than 1024; Java intentional failure")
	// [KeyLength] put object key special characters at start
	case "test_put_object_key_special_characters_at_start", "test_put_object_key_special_characters_at_end":
		for _, special := range []string{"!", "@", "#", "$", "%", "^", "&", "*", "(", ")", "-", "_", "+", "=", "[", "]", "{", "}", "|", "\\", ":", ";", "\"", "'", "<", ">", ",", ".", "?", "/", "~", "`"} {
			key := special + alpha(1024-len([]byte(special)))
			if strings.HasSuffix(name, "at_end") {
				key = alpha(1024-len([]byte(special))) + special
			}
			putAndRead(key, "body-"+special)
		}
	case "test_put_object_key_unicode_characters":
		for _, char := range []string{"한", "中", "日", "а", "α", "ع", "т", "ф"} {
			count := 200/len([]byte(char)) - 1
			putAndRead(strings.Repeat(char, count), "unicode-"+char)
		}
	case "test_put_object_key_unicode_characters_too_long":
		// KSAN accepts oversize unicode keys; Java intentional failure.
		t.Skip("KSAN accepts oversize unicode keys; Java intentional failure")
	case "test_put_object_key_with_leading_and_trailing_spaces":
		for _, n := range []int{1, 2, 3, 5} {
			putAndRead(strings.Repeat(" ", n)+alpha(1024-2*n)+strings.Repeat(" ", n), fmt.Sprintf("space-%d", n))
		}
	case "test_put_object_key_with_consecutive_slashes":
		for _, key := range []string{"folder//double-slash", "folder///triple-slash", "//leading-double-slash", "trailing-double-slash//", "folder////multiple-slashes"} {
			putAndRead(key, "slash-"+strings.ReplaceAll(key, "/", "-"))
		}
	case "test_put_object_key_boundary_lengths":
		for _, n := range []int{1023, 1024, 500, 100, 50} {
			putAndRead(alpha(n), fmt.Sprintf("boundary-%d", n))
		}
	default:
		t.Fatalf("unimplemented key case %q", name)
	}
}
