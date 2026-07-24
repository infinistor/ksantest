package s3tests

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func TestMultipart(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		run  func(*testing.T)
	}{
		// 비어있는 오브젝트를 멀티파트로 업로드 실패 확인
		{"test_multipart_upload_empty", func(t *testing.T) { testMultipartUploadCase(t, "test_multipart_upload_empty") }},
		// 파트 크기보다 작은 오브젝트를 멀티파트 업로드시 성공확인
		{"test_multipart_upload_small", func(t *testing.T) { testMultipartUploadCase(t, "test_multipart_upload_small") }},
		// 버킷a에서 버킷b로 멀티파트 복사 성공확인
		{"test_multipart_copy_small", func(t *testing.T) { testMultipartCopyCase(t, "test_multipart_copy_small") }},
		// 범위설정을 잘못한 멀티파트 복사 실패 확인
		{"test_multipart_copy_invalid_range", func(t *testing.T) { testMultipartCopyCase(t, "test_multipart_copy_invalid_range") }},
		// 범위를 지정한 멀티파트 복사 성공확인
		{"test_multipart_copy_without_range", func(t *testing.T) { testMultipartCopyCase(t, "test_multipart_copy_without_range") }},
		// 특수문자로 오브젝트 이름을 만들어 업로드한 오브젝트를 멀티파트 복사 성공 확인
		{"test_multipart_copy_special_names", func(t *testing.T) { testMultipartCopyCase(t, "test_multipart_copy_special_names") }},
		// Backend 헤더를 사용하여 멀티파트 업로드를 수행할 수 있는지 확인
		{"test_multipart_upload", func(t *testing.T) { testMultipartUploadCase(t, "test_multipart_upload") }},
		// 버저닝되어있는 버킷에서 오브젝트를 멀티파트로 복사 성공 확인
		{"test_multipart_copy_versioned", func(t *testing.T) { testMultipartCopyCase(t, "test_multipart_copy_versioned") }},
		// 멀티파트 업로드중 같은 파츠를 여러번 업로드시 성공 확인
		{"test_multipart_upload_resend_part", func(t *testing.T) { testMultipartUploadCase(t, "test_multipart_upload_resend_part") }},
		// 한 오브젝트에 대해 다양한 크기의 멀티파트 업로드 성공 확인
		{"test_multipart_upload_multiple_sizes", func(t *testing.T) { testMultipartUploadCase(t, "test_multipart_upload_multiple_sizes") }},
		// 한 오브젝트에 대해 다양한 크기의 오브젝트 멀티파트 복사 성공 확인
		{"test_multipart_copy_multiple_sizes", func(t *testing.T) { testMultipartCopyCase(t, "test_multipart_copy_multiple_sizes") }},
		// 멀티파트 업로드시에 파츠의 크기가 너무 작을 경우 업로드 실패 확인
		{"test_multipart_upload_size_too_small", func(t *testing.T) { testMultipartCompletionErrors(t, "test_multipart_upload_size_too_small") }},
		// 내용물을 채운 멀티파트 업로드 성공 확인
		{"test_multipart_upload_contents", func(t *testing.T) { testMultipartUploadCase(t, "test_multipart_upload_contents") }},
		// 업로드한 오브젝트를 멀티파트 업로드로 덮어쓰기 성공 확인
		{"test_multipart_upload_overwrite_existing_object", func(t *testing.T) { testMultipartUploadCase(t, "test_multipart_upload_overwrite_existing_object") }},
		// 멀티파트 업로드한 오브젝트를 PutObject로 덮어쓰기 성공 확인
		{"test_put_object_overwrite_multipart_upload", testPutObjectOverwriteMultipartUpload},
		// 멀티파트 업로드하는 도중 중단 성공 확인
		{"test_abort_multipart_upload", func(t *testing.T) { testMultipartLifecycle(t, "test_abort_multipart_upload") }},
		// 존재하지 않은 멀티파트 업로드 중단 실패 확인
		{"test_abort_multipart_upload_not_found", func(t *testing.T) { testMultipartLifecycle(t, "test_abort_multipart_upload_not_found") }},
		// 멀티파트 업로드 중인 목록 확인
		{"test_list_multipart_upload", func(t *testing.T) { testMultipartLifecycle(t, "test_list_multipart_upload") }},
		// 업로드 하지 않은 파츠가 있는 상태에서 멀티파트 완료 함수 실패 확인
		{"test_multipart_upload_missing_part", func(t *testing.T) { testMultipartCompletionErrors(t, "test_multipart_upload_missing_part") }},
		// 잘못된 eTag값을 입력한 멀티파트 완료 함수 실패 확인
		{"test_multipart_upload_incorrect_etag", func(t *testing.T) { testMultipartCompletionErrors(t, "test_multipart_upload_incorrect_etag") }},
		// 버킷에 존재하는 오브젝트와 동일한 이름으로 멀티파트 업로드를 시작 또는 중단했을때 오브젝트에 영향이 없음을 확인
		{"test_atomic_multipart_upload_write", func(t *testing.T) { testMultipartUploadCase(t, "test_atomic_multipart_upload_write") }},
		// 멀티파트 업로드 목록 확인
		{"test_multipart_upload_list", func(t *testing.T) { testMultipartLifecycle(t, "test_multipart_upload_list") }},
		// 멀티파트 업로드하는 도중 중단 성공 확인
		{"test_abort_multipart_upload_list", func(t *testing.T) { testMultipartLifecycle(t, "test_abort_multipart_upload_list") }},
		// 멀티파트업로드와 멀티파티 카피로 오브젝트가 업로드 가능한지 확인
		{"test_multipart_copy_many", func(t *testing.T) { testMultipartCopyCase(t, "test_multipart_copy_many") }},
		// 멀티파트 목록 확인
		{"test_multipart_list_parts", testMultipartListParts},
		// UseChunkEncoding을 사용하는 멀티파트 업로드 시 체크섬 계산 및 검증 확인
		{"test_multipart_upload_checksum_use_chunk_encoding", func(t *testing.T) { testMultipartChecksumCase(t, "test_multipart_upload_checksum_use_chunk_encoding") }},
		// UseChunkEncoding을 사용하지 않는 멀티파트 업로드 시 체크섬 계산 및 검증 확인
		{"test_multipart_upload_checksum", func(t *testing.T) { testMultipartChecksumCase(t, "test_multipart_upload_checksum") }},
		// 멀티파트 업로드 시 체크섬 계산 및 검증 실패 확인
		{"test_multipart_upload_checksum_failure", func(t *testing.T) { testMultipartChecksumCase(t, "test_multipart_upload_checksum_failure") }},
		// 멀티파트 업로드 시 체크섬 계산 및 검증 확인
		{"test_multipart_copy_checksum", testMultipartCopyChecksum},
		// 빈 값일 때 동작 확인
		{"test_create_multipart_upload_empty_checksum_algorithm", func(t *testing.T) { testMultipartChecksumCase(t, "test_create_multipart_upload_empty_checksum_algorithm") }},
		// 빈 값일 때 동작 확인
		{"test_create_multipart_upload_empty_checksum_type", func(t *testing.T) { testMultipartChecksumCase(t, "test_create_multipart_upload_empty_checksum_type") }},
		// 소스 오브젝트와 일치하는 copy-source-if-match 조건으로 UploadPartCopy 성공 확인
		{"test_upload_part_copy_if_match_good", func(t *testing.T) { testMultipartCopyCondition(t, "test_upload_part_copy_if_match_good") }},
		// 소스 오브젝트와 일치하지 않는 copy-source-if-match 조건으로 UploadPartCopy 시 412 실패 확인
		{"test_upload_part_copy_if_match_failed", func(t *testing.T) { testMultipartCopyCondition(t, "test_upload_part_copy_if_match_failed") }},
		// 소스 오브젝트와 일치하지 않는 copy-source-if-none-match 조건으로 UploadPartCopy 성공 확인
		{"test_upload_part_copy_if_none_match_good", func(t *testing.T) { testMultipartCopyCondition(t, "test_upload_part_copy_if_none_match_good") }},
		// 소스 오브젝트와 일치하는 copy-source-if-none-match 조건으로 UploadPartCopy 시 412 실패 확인
		{"test_upload_part_copy_if_none_match_failed", func(t *testing.T) { testMultipartCopyCondition(t, "test_upload_part_copy_if_none_match_failed") }},
		// UploadPartCopy 요청에 If-Match와 If-None-Match를 함께 지정하면 501로 거부되는지 확인
		{"test_upload_part_copy_if_match_and_if_none_match", func(t *testing.T) { testMultipartCopyCondition(t, "test_upload_part_copy_if_match_and_if_none_match") }},
		// UploadPartCopy 요청에 If-Match와 If-None-Match: * 를 함께 지정하면 501로 거부되는지 확인
		{"test_upload_part_copy_if_match_and_if_none_match_any", func(t *testing.T) { testMultipartCopyCondition(t, "test_upload_part_copy_if_match_and_if_none_match_any") }},
		// 소스 오브젝트 업로드 이전 시간의 copy-source-if-modified-since 조건으로 UploadPartCopy 성공 확인
		{"test_upload_part_copy_if_modified_since_good", func(t *testing.T) { testMultipartCopyCondition(t, "test_upload_part_copy_if_modified_since_good") }},
		// 소스 오브젝트 업로드 이후 시간의 copy-source-if-modified-since 조건으로 UploadPartCopy 시 412 실패 확인
		{"test_upload_part_copy_if_modified_since_failed", func(t *testing.T) { testMultipartCopyCondition(t, "test_upload_part_copy_if_modified_since_failed") }},
		// 소스 오브젝트 업로드 이후 시간의 copy-source-if-unmodified-since 조건으로 UploadPartCopy 성공 확인
		{"test_upload_part_copy_if_unmodified_since_good", func(t *testing.T) { testMultipartCopyCondition(t, "test_upload_part_copy_if_unmodified_since_good") }},
		// 소스 오브젝트 업로드 이전 시간의 copy-source-if-unmodified-since 조건으로 UploadPartCopy 시 412 실패 확인
		{"test_upload_part_copy_if_unmodified_since_failed", func(t *testing.T) { testMultipartCopyCondition(t, "test_upload_part_copy_if_unmodified_since_failed") }},
		// 대상 오브젝트와 일치하는 If-Match 조건으로 CompleteMultipartUpload 덮어쓰기 성공 확인
		{"test_complete_multipart_upload_if_match_good", func(t *testing.T) { testMultipartCompleteCondition(t, "test_complete_multipart_upload_if_match_good") }},
		// 대상 오브젝트와 일치하지 않는 If-Match 조건으로 CompleteMultipartUpload 시 412 실패 확인
		{"test_complete_multipart_upload_if_match_failed", func(t *testing.T) { testMultipartCompleteCondition(t, "test_complete_multipart_upload_if_match_failed") }},
		// 존재하지 않는 키에 If-None-Match: * 조건으로 CompleteMultipartUpload 성공 확인
		{"test_complete_multipart_upload_if_none_match_good", func(t *testing.T) { testMultipartCompleteCondition(t, "test_complete_multipart_upload_if_none_match_good") }},
		// 이미 존재하는 키에 If-None-Match: * 조건으로 CompleteMultipartUpload 시 412 실패 확인
		{"test_complete_multipart_upload_if_none_match_failed", func(t *testing.T) { testMultipartCompleteCondition(t, "test_complete_multipart_upload_if_none_match_failed") }},
		// If-Match와 If-None-Match를 함께 지정하면 501로 거부되는지 확인
		{"test_complete_multipart_upload_if_match_and_if_none_match", func(t *testing.T) { testMultipartCompleteCondition(t, "test_complete_multipart_upload_if_match_and_if_none_match") }},
		// If-Match와 If-None-Match: * 를 함께 지정하면 501로 거부되는지 확인
		{"test_complete_multipart_upload_if_match_and_if_none_match_any", func(t *testing.T) { testMultipartCompleteCondition(t, "test_complete_multipart_upload_if_match_and_if_none_match_any") }},
		// 멀티파티 업로드 abort 이후 uploadPart가 실패하는지 확인
		{"test_multipart_upload_abort_during_upload", func(t *testing.T) { testMultipartLifecycle(t, "test_multipart_upload_abort_during_upload") }},
	}
	for _, tc := range tests {
		t.Run(tc.name, tc.run)
	}
}

type multipartFixture struct {
	s       *suite
	bucket  string
	key     string
	created *s3.CreateMultipartUploadOutput
}

func newMultipartFixture(t *testing.T, key string) *multipartFixture {
	t.Helper()
	s := newSuite(t)
	b := s.bucket(t)
	created, err := s.client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: aws.String(b), Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = s.client.AbortMultipartUpload(context.Background(), &s3.AbortMultipartUploadInput{Bucket: aws.String(b), Key: aws.String(key), UploadId: created.UploadId})
	})
	return &multipartFixture{s, b, key, created}
}
func uploadMultipartPart(t *testing.T, f *multipartFixture, number int32, body []byte) *s3.UploadPartOutput {
	t.Helper()
	out, err := f.s.client.UploadPart(context.Background(), &s3.UploadPartInput{Bucket: aws.String(f.bucket), Key: aws.String(f.key), UploadId: f.created.UploadId, PartNumber: aws.Int32(number), Body: bytes.NewReader(body)})
	if err != nil {
		t.Fatal(err)
	}
	return out
}
func completeMultipartParts(t *testing.T, f *multipartFixture, parts []types.CompletedPart, options ...func(*s3.Options)) *s3.CompleteMultipartUploadOutput {
	t.Helper()
	out, err := f.s.client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{Bucket: aws.String(f.bucket), Key: aws.String(f.key), UploadId: f.created.UploadId, MultipartUpload: &types.CompletedMultipartUpload{Parts: parts}}, options...)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func testPutObjectOverwriteMultipartUpload(t *testing.T) {
	s := newSuite(t)
	bucket := s.bucket(t)
	key := "test_put_object_overwrite_multipart_upload"
	completeMultipart(t, s.client, bucket, key, deterministicBody(10*1024*1024), false, nil)
	content := deterministicBody(1 * 1024 * 1024)
	putBytes(t, s.client, bucket, key, content)
	head, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil || aws.ToInt64(head.ContentLength) != int64(len(content)) {
		t.Fatalf("HeadObject length=%d err=%v want=%d", aws.ToInt64(head.ContentLength), err, len(content))
	}
	assertObjectBytes(t, s.client, bucket, key, content)
	assertObjectRanges(t, s.client, bucket, key, content, []int{1024})
}

func testMultipartUploadCase(t *testing.T, name string) {
	f := newMultipartFixture(t, name)
	if name == "test_multipart_upload_empty" {
		_, err := f.s.client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{Bucket: aws.String(f.bucket), Key: aws.String(f.key), UploadId: f.created.UploadId, MultipartUpload: &types.CompletedMultipartUpload{}})
		assertHTTPError(t, err, 400)
		return
	}
	if name == "test_atomic_multipart_upload_write" {
		put(t, f.s, f.bucket, f.key, "old", nil)
	}
	sizes := []int{5 * 1024 * 1024}
	if name == "test_multipart_upload_small" {
		sizes = []int{1024}
	}
	if name == "test_multipart_upload_multiple_sizes" {
		sizes = []int{5 * 1024 * 1024, 6 * 1024 * 1024, 1024}
	}
	parts := make([]types.CompletedPart, 0, len(sizes))
	var want []byte
	for i, size := range sizes {
		body := bytes.Repeat([]byte{byte('a' + i)}, size)
		part := uploadMultipartPart(t, f, int32(i+1), body)
		parts = append(parts, types.CompletedPart{ETag: part.ETag, PartNumber: aws.Int32(int32(i + 1))})
		want = append(want, body...)
	}
	if name == "test_multipart_upload_resend_part" {
		replacement := bytes.Repeat([]byte("z"), sizes[0])
		part := uploadMultipartPart(t, f, 1, replacement)
		parts[0].ETag = part.ETag
		want = replacement
	}
	completeMultipartParts(t, f, parts)
	assertObjectBytes(t, f.s.client, f.bucket, f.key, want)
	if name == "test_multipart_upload_overwrite_existing_object" || name == "test_atomic_multipart_upload_write" {
		if string(want) == "old" {
			t.Fatal("multipart did not replace object")
		}
	}
}

func testMultipartCopyCase(t *testing.T, name string) {
	s := newSuite(t)
	b := s.bucket(t)
	source, target := name+"?source", name+"&target"
	body := deterministicBody(6 * 1024 * 1024)
	if name == "test_multipart_copy_small" {
		body = []byte("small")
	}
	put(t, s, b, source, string(body), nil)
	if name == "test_multipart_copy_versioned" {
		enableVersioning(t, s, b)
		put(t, s, b, source, string(body), nil)
	}
	created, err := s.client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: aws.String(b), Key: aws.String(target)})
	if err != nil {
		t.Fatal(err)
	}
	input := &s3.UploadPartCopyInput{Bucket: aws.String(b), Key: aws.String(target), UploadId: created.UploadId, PartNumber: aws.Int32(1), CopySource: copySource(b, source, "")}
	if name == "test_multipart_copy_invalid_range" {
		input.CopySourceRange = aws.String("bytes=99999999-100000000")
		_, err = s.client.UploadPartCopy(context.Background(), input)
		if err == nil {
			t.Fatal("invalid range accepted")
		}
		return
	}
	if name != "test_multipart_copy_without_range" {
		input.CopySourceRange = aws.String(fmt.Sprintf("bytes=0-%d", len(body)-1))
	}
	copied, err := s.client.UploadPartCopy(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{Bucket: aws.String(b), Key: aws.String(target), UploadId: created.UploadId, MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{{ETag: copied.CopyPartResult.ETag, PartNumber: aws.Int32(1)}}}})
	if err != nil {
		t.Fatal(err)
	}
	assertObjectBytes(t, s.client, b, target, body)
}

func testMultipartLifecycle(t *testing.T, name string) {
	f := newMultipartFixture(t, name)
	uploadMultipartPart(t, f, 1, bytes.Repeat([]byte("x"), 5*1024*1024))
	if name == "test_abort_multipart_upload_not_found" {
		_, err := f.s.client.AbortMultipartUpload(context.Background(), &s3.AbortMultipartUploadInput{Bucket: aws.String(f.bucket), Key: aws.String(f.key), UploadId: aws.String("nonexistent")})
		assertS3Error(t, err, 404, "NoSuchUpload")
		return
	}
	listed, err := f.s.client.ListMultipartUploads(context.Background(), &s3.ListMultipartUploadsInput{Bucket: aws.String(f.bucket)})
	if err != nil || len(listed.Uploads) == 0 {
		t.Fatalf("uploads=%v err=%v", listed.Uploads, err)
	}
	if strings.Contains(name, "abort") {
		_, err = f.s.client.AbortMultipartUpload(context.Background(), &s3.AbortMultipartUploadInput{Bucket: aws.String(f.bucket), Key: aws.String(f.key), UploadId: f.created.UploadId})
		if err != nil {
			t.Fatal(err)
		}
		listed, _ = f.s.client.ListMultipartUploads(context.Background(), &s3.ListMultipartUploadsInput{Bucket: aws.String(f.bucket)})
		if len(listed.Uploads) != 0 {
			t.Fatalf("uploads=%v", listed.Uploads)
		}
	}
}

func testMultipartCompletionErrors(t *testing.T, name string) {
	f := newMultipartFixture(t, name)
	one := uploadMultipartPart(t, f, 1, bytes.Repeat([]byte("a"), 1024))
	if name == "test_multipart_upload_size_too_small" {
		two := uploadMultipartPart(t, f, 2, bytes.Repeat([]byte("b"), 1024))
		_, err := f.s.client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{Bucket: aws.String(f.bucket), Key: aws.String(f.key), UploadId: f.created.UploadId, MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{{ETag: one.ETag, PartNumber: aws.Int32(1)}, {ETag: two.ETag, PartNumber: aws.Int32(2)}}}})
		assertS3Error(t, err, 400, "EntityTooSmall")
		return
	}
	parts := []types.CompletedPart{{ETag: one.ETag, PartNumber: aws.Int32(1)}}
	if name == "test_multipart_upload_missing_part" {
		parts = append(parts, types.CompletedPart{ETag: aws.String(`"missing"`), PartNumber: aws.Int32(2)})
	} else {
		parts[0].ETag = aws.String(`"incorrect"`)
	}
	_, err := f.s.client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{Bucket: aws.String(f.bucket), Key: aws.String(f.key), UploadId: f.created.UploadId, MultipartUpload: &types.CompletedMultipartUpload{Parts: parts}})
	if err == nil {
		t.Fatal("invalid completion accepted")
	}
}

func testMultipartCopyCondition(t *testing.T, name string) {
	s := newSuite(t)
	b := s.bucket(t)
	source, target := name+"-source", name+"-target"
	createdSource := put(t, s, b, source, source, nil)
	created, err := s.client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: aws.String(b), Key: aws.String(target)})
	if err != nil {
		t.Fatal(err)
	}
	input := &s3.UploadPartCopyInput{Bucket: aws.String(b), Key: aws.String(target), UploadId: created.UploadId, PartNumber: aws.Int32(1), CopySource: copySource(b, source, "")}
	past, future := time.Date(1994, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)
	status := 0
	switch name {
	case "test_upload_part_copy_if_match_good":
		input.CopySourceIfMatch = createdSource.ETag
	case "test_upload_part_copy_if_match_failed":
		input.CopySourceIfMatch = aws.String("ABC")
		status = 412
	case "test_upload_part_copy_if_none_match_good":
		input.CopySourceIfNoneMatch = aws.String("ABC")
	case "test_upload_part_copy_if_none_match_failed":
		input.CopySourceIfNoneMatch = createdSource.ETag
		status = 412
	case "test_upload_part_copy_if_match_and_if_none_match":
		input.CopySourceIfMatch, input.CopySourceIfNoneMatch = createdSource.ETag, createdSource.ETag
		status = 412
	case "test_upload_part_copy_if_match_and_if_none_match_any":
		input.CopySourceIfMatch, input.CopySourceIfNoneMatch = createdSource.ETag, aws.String("*")
		status = 412
	case "test_upload_part_copy_if_modified_since_good":
		input.CopySourceIfModifiedSince = &past
	case "test_upload_part_copy_if_modified_since_failed":
		input.CopySourceIfModifiedSince = &future
		status = 412
	case "test_upload_part_copy_if_unmodified_since_good":
		input.CopySourceIfUnmodifiedSince = &future
	case "test_upload_part_copy_if_unmodified_since_failed":
		input.CopySourceIfUnmodifiedSince = &past
		status = 412
	}
	_, err = s.client.UploadPartCopy(context.Background(), input)
	if status != 0 {
		assertHTTPError(t, err, status)
	} else if err != nil {
		t.Fatal(err)
	}
}

func testMultipartCompleteCondition(t *testing.T, name string) {
	f := newMultipartFixture(t, name)
	existing := put(t, f.s, f.bucket, f.key, "old", nil)
	part := uploadMultipartPart(t, f, 1, bytes.Repeat([]byte("n"), 5*1024*1024))
	input := &s3.CompleteMultipartUploadInput{Bucket: aws.String(f.bucket), Key: aws.String(f.key), UploadId: f.created.UploadId, MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{{ETag: part.ETag, PartNumber: aws.Int32(1)}}}}
	status := 0
	switch name {
	case "test_complete_multipart_upload_if_match_good":
		input.IfMatch = existing.ETag
	case "test_complete_multipart_upload_if_match_failed":
		input.IfMatch = aws.String("bad")
		status = 412
	case "test_complete_multipart_upload_if_none_match_good":
		input.Key = aws.String(f.key + "-new")
		status = 0
		input.IfNoneMatch = aws.String("*")
	case "test_complete_multipart_upload_if_none_match_failed":
		input.IfNoneMatch = aws.String("*")
		status = 412
	case "test_complete_multipart_upload_if_match_and_if_none_match":
		input.IfMatch, input.IfNoneMatch = existing.ETag, existing.ETag
		status = 501
	case "test_complete_multipart_upload_if_match_and_if_none_match_any":
		input.IfMatch, input.IfNoneMatch = existing.ETag, aws.String("*")
		status = 501
	}
	_, err := f.s.client.CompleteMultipartUpload(context.Background(), input)
	if status != 0 {
		assertHTTPError(t, err, status)
	} else if err != nil {
		t.Fatal(err)
	}
}

func testMultipartChecksumCase(t *testing.T, name string) {
	s := newSuite(t)
	b := s.bucket(t)
	if name == "test_create_multipart_upload_empty_checksum_algorithm" {
		_, err := s.client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: aws.String(b), Key: aws.String(name), ChecksumType: types.ChecksumTypeFullObject})
		assertHTTPError(t, err, 400)
		return
	}
	if name == "test_multipart_upload_checksum_failure" {
		// Java: FULL_OBJECT rejects non-CRC algorithms (e.g. SHA256) with InvalidRequest.
		_, err := s.client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{
			Bucket: aws.String(b), Key: aws.String(name),
			ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
			ChecksumType:      types.ChecksumTypeFullObject,
		})
		assertS3Error(t, err, 400, "InvalidRequest")
		return
	}
	algorithm := types.ChecksumAlgorithmCrc32
	createInput := &s3.CreateMultipartUploadInput{Bucket: aws.String(b), Key: aws.String(name), ChecksumAlgorithm: algorithm, ChecksumType: types.ChecksumTypeComposite}
	if name == "test_create_multipart_upload_empty_checksum_type" {
		createInput.ChecksumType = ""
	}
	created, err := s.client.CreateMultipartUpload(context.Background(), createInput)
	if err != nil {
		t.Fatal(err)
	}
	body := bytes.Repeat([]byte("c"), 5*1024*1024)
	value := checksumValue(algorithm, body)
	part, err := s.client.UploadPart(context.Background(), &s3.UploadPartInput{Bucket: aws.String(b), Key: aws.String(name), UploadId: created.UploadId, PartNumber: aws.Int32(1), Body: bytes.NewReader(body), ChecksumAlgorithm: algorithm, ChecksumCRC32: aws.String(value)})
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{Bucket: aws.String(b), Key: aws.String(name), UploadId: created.UploadId, ChecksumType: types.ChecksumTypeComposite, MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{{ETag: part.ETag, PartNumber: aws.Int32(1), ChecksumCRC32: part.ChecksumCRC32}}}})
	if err != nil {
		t.Fatal(err)
	}
	assertObjectBytes(t, s.client, b, name, body)
}

func testMultipartListParts(t *testing.T) {
	f := newMultipartFixture(t, "parts")
	for i := int32(1); i <= 20; i++ {
		uploadMultipartPart(t, f, i, bytes.Repeat([]byte{byte(i)}, 1024))
	}
	var marker *string
	seen := 0
	for {
		out, err := f.s.client.ListParts(context.Background(), &s3.ListPartsInput{Bucket: aws.String(f.bucket), Key: aws.String(f.key), UploadId: f.created.UploadId, MaxParts: aws.Int32(10), PartNumberMarker: marker})
		if err != nil {
			t.Fatal(err)
		}
		seen += len(out.Parts)
		if !aws.ToBool(out.IsTruncated) {
			break
		}
		marker = out.NextPartNumberMarker
	}
	if seen != 20 {
		t.Fatalf("parts=%d", seen)
	}
}

func testMultipartCopyChecksum(t *testing.T) {
	s := newSuite(t)
	b := s.bucket(t)
	body := bytes.Repeat([]byte("q"), 5*1024*1024)
	putInput := &s3.PutObjectInput{Bucket: aws.String(b), Key: aws.String("source"), Body: bytes.NewReader(body), ChecksumAlgorithm: types.ChecksumAlgorithmCrc32}
	setPutChecksum(putInput, types.ChecksumAlgorithmCrc32, body, false)
	if _, err := s.client.PutObject(context.Background(), putInput); err != nil {
		t.Fatal(err)
	}
	created, err := s.client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: aws.String(b), Key: aws.String("target"), ChecksumAlgorithm: types.ChecksumAlgorithmCrc32, ChecksumType: types.ChecksumTypeComposite})
	if err != nil {
		t.Fatal(err)
	}
	copied, err := s.client.UploadPartCopy(context.Background(), &s3.UploadPartCopyInput{Bucket: aws.String(b), Key: aws.String("target"), UploadId: created.UploadId, PartNumber: aws.Int32(1), CopySource: copySource(b, "source", ""), CopySourceRange: aws.String(fmt.Sprintf("bytes=0-%d", len(body)-1))})
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{Bucket: aws.String(b), Key: aws.String("target"), UploadId: created.UploadId, ChecksumType: types.ChecksumTypeComposite, MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{{ETag: copied.CopyPartResult.ETag, PartNumber: aws.Int32(1), ChecksumCRC32: copied.CopyPartResult.ChecksumCRC32}}}})
	if err != nil {
		t.Fatal(err)
	}
	assertObjectBytes(t, s.client, b, "target", body)
}
