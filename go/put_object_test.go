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

func TestBucketListDistinct(t *testing.T) {
	t.Parallel()

	testPutCore(t, "test_bucket_list_distinct")
}
func TestObjectWriteToNonExistBucket(t *testing.T) {
	t.Parallel()

	testPutCore(t, "test_object_write_to_non_exist_bucket")
}
func TestObjectHeadZeroBytes(t *testing.T) {
	t.Parallel()

	testPutCore(t, "test_object_head_zero_bytes")
}
func TestObjectWriteCheckEtag(t *testing.T) {
	t.Parallel()

	testPutCore(t, "test_object_write_check_etag")
}
func TestObjectWriteCacheControl(t *testing.T) {
	t.Parallel()

	testPutCore(t, "test_object_write_cache_control")
}
func TestObjectWriteExpires(t *testing.T) {
	t.Parallel()

	testPutCore(t, "test_object_write_expires")
}
func TestObjectWriteReadUpdateReadDelete(t *testing.T) {
	t.Parallel()

	testPutCore(t, "test_object_write_read_update_read_delete")
}
func TestObjectSetGetMetadataNoneToGood(t *testing.T) {
	t.Parallel()

	testPutCore(t, "test_object_set_get_metadata_none_to_good")
}
func TestObjectSetGetMetadataNoneToEmpty(t *testing.T) {
	t.Parallel()

	testPutCore(t, "test_object_set_get_metadata_none_to_empty")
}
func TestObjectSetGetMetadataOverwriteToEmpty(t *testing.T) {
	t.Parallel()

	testPutCore(t, "test_object_set_get_metadata_overwrite_to_empty")
}
func TestObjectSetGetNonUtf8Metadata(t *testing.T) {
	t.Parallel()

	testPutCore(t, "test_object_set_get_non_utf8_metadata")
}
func TestObjectSetGetMetadataEmptyToUnreadablePrefix(t *testing.T) {
	t.Parallel()

	testPutCore(t, "test_object_set_get_metadata_empty_to_unreadable_prefix")
}
func TestObjectSetGetMetadataEmptyToUnreadableSuffix(t *testing.T) {
	t.Parallel()

	testPutCore(t, "test_object_set_get_metadata_empty_to_unreadable_suffix")
}
func TestObjectMetadataReplacedOnPut(t *testing.T) {
	t.Parallel()

	testPutCore(t, "test_object_metadata_replaced_on_put")
}
func TestObjectWriteFile(t *testing.T) {
	t.Parallel()

	testPutCore(t, "test_object_write_file")
}
func TestBucketCreateSpecialKeyNames(t *testing.T) {
	t.Parallel()

	testPutSpecialKeys(t, "test_bucket_create_special_key_names")
}
func TestBucketListSpecialPrefix(t *testing.T) {
	t.Parallel()

	testPutSpecialKeys(t, "test_bucket_list_special_prefix")
}
func TestObjectLockUploadingObj(t *testing.T) {
	t.Parallel()

	testPutCore(t, "test_object_lock_uploading_obj")
}
func TestObjectInfixSpace(t *testing.T) {
	t.Parallel()

	testPutSpecialKeys(t, "test_object_infix_space")
}
func TestObjectSuffixSpace(t *testing.T) {
	t.Parallel()

	testPutSpecialKeys(t, "test_object_suffix_space")
}
func TestPutObjectSpecialCharacters(t *testing.T) {
	t.Parallel()

	testPutSpecialKeys(t, "test_put_object_special_characters")
}
func TestPutObjectSpecialCharactersUseChunkEncoding(t *testing.T) {
	t.Parallel()

	testPutSpecialKeys(t, "test_put_object_special_characters_use_chunk_encoding")
}
func TestPutObjectUseSpecialCharactersChunkEncodingAndDisablePayloadSigning(t *testing.T) {
	t.Parallel()

	testPutSpecialKeys(t, "test_put_object_use_special_characters_chunk_encoding_and_disable_payload_signing")
}
func TestPutObjectSpecialCharactersNotChunkEncoding(t *testing.T) {
	t.Parallel()

	testPutSpecialKeys(t, "test_put_object_special_characters_not_chunk_encoding")
}
func TestPutObjectSpecialCharactersNotChunkEncodingAndDisablePayloadSigning(t *testing.T) {
	t.Parallel()

	testPutSpecialKeys(t, "test_put_object_special_characters_not_chunk_encoding_and_disable_payload_signing")
}
func TestPutObjectDirAndFile(t *testing.T) {
	t.Parallel()

	testPutCore(t, "test_put_object_dir_and_file")
}
func TestObjectOverwrite(t *testing.T) {
	t.Parallel()

	testPutCore(t, "test_object_overwrite")
}
func TestObjectEmoji(t *testing.T) {
	t.Parallel()

	testPutCore(t, "test_object_emoji")
}
func TestObjectSetGetMetadataUtf8(t *testing.T) {
	t.Parallel()

	testPutCore(t, "test_object_set_get_metadata_utf8")
}
func TestPutObjectChecksumUseChunkEncoding(t *testing.T) {
	t.Parallel()

	testPutChecksums(t, "test_put_object_checksum_use_chunk_encoding")
}
func TestPutObjectChecksum(t *testing.T) {
	t.Parallel()

	testPutChecksums(t, "test_put_object_checksum")
}
func TestPutObjectChecksumWithValue(t *testing.T) {
	t.Parallel()

	testPutChecksums(t, "test_put_object_checksum_with_value")
}
func TestPutObjectChecksumFailure(t *testing.T) {
	t.Parallel()

	testPutChecksums(t, "test_put_object_checksum_failure")
}
func TestPutObjectIfMatchGood(t *testing.T) {
	t.Parallel()

	testPutConditions(t, "test_put_object_if_match_good")
}
func TestPutObjectIfMatchFailed(t *testing.T) {
	t.Parallel()

	testPutConditions(t, "test_put_object_if_match_failed")
}
func TestPutObjectIfNoneMatchGood(t *testing.T) {
	t.Parallel()

	testPutConditions(t, "test_put_object_if_none_match_good")
}
func TestPutObjectIfNoneMatchFailed(t *testing.T) {
	t.Parallel()

	testPutConditions(t, "test_put_object_if_none_match_failed")
}
func TestPutObjectIfMatchAndIfNoneMatch(t *testing.T) {
	t.Parallel()

	testPutConditions(t, "test_put_object_if_match_and_if_none_match")
}
func TestPutObjectKeyMaxLength(t *testing.T) {
	t.Parallel()

	testPutKeyBoundary(t, "test_put_object_key_max_length")
}
func TestPutObjectKeyMinLength(t *testing.T) {
	t.Parallel()

	testPutKeyBoundary(t, "test_put_object_key_min_length")
}
func TestPutObjectKeyTooLong(t *testing.T) {
	t.Parallel()

	testPutKeyBoundary(t, "test_put_object_key_too_long")
}
func TestPutObjectKeySpecialCharactersAtStart(t *testing.T) {
	t.Parallel()

	testPutSpecialKeys(t, "test_put_object_key_special_characters_at_start")
}
func TestPutObjectKeySpecialCharactersAtEnd(t *testing.T) {
	t.Parallel()

	testPutSpecialKeys(t, "test_put_object_key_special_characters_at_end")
}
func TestPutObjectKeyUnicodeCharacters(t *testing.T) {
	t.Parallel()

	testPutKeyBoundary(t, "test_put_object_key_unicode_characters")
}
func TestPutObjectKeyUnicodeCharactersTooLong(t *testing.T) {
	t.Parallel()

	testPutKeyBoundary(t, "test_put_object_key_unicode_characters_too_long")
}
func TestPutObjectKeyWithLeadingAndTrailingSpaces(t *testing.T) {
	t.Parallel()

	testPutKeyBoundary(t, "test_put_object_key_with_leading_and_trailing_spaces")
}
func TestPutObjectKeyWithConsecutiveSlashes(t *testing.T) {
	t.Parallel()

	testPutKeyBoundary(t, "test_put_object_key_with_consecutive_slashes")
}
func TestPutObjectKeyBoundaryLengths(t *testing.T) {
	t.Parallel()

	testPutKeyBoundary(t, "test_put_object_key_boundary_lengths")
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

		t.Skip("KSAN accepts keys longer than 1024; Java intentional failure")

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
