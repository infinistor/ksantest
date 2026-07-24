package s3tests

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const ssePartSize = 5 * 1024 * 1024

func TestSSES3(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		run  func(*testing.T)
	}{
		// 1Byte 오브젝트를 SSE-S3 설정하여 업/다운로드가 올바르게 동작하는지 확인
		{"test_sse_s3_encrypted_transfer_1b", func(t *testing.T) { testSSES3Write(t, 1) }},
		// 1KB 오브젝트를 SSE-S3 설정하여 업/다운로드가 올바르게 동작하는지 확인
		{"test_sse_s3_encrypted_transfer_1kb", func(t *testing.T) { testSSES3Write(t, 1024) }},
		// 1MB 오브젝트를 SSE-S3 설정하여 업/다운로드가 올바르게 동작하는지 확인
		{"test_sse_s3_encrypted_transfer_1mb", func(t *testing.T) { testSSES3Write(t, 1024*1024) }},
		// 13Byte 오브젝트를 SSE-S3 설정하여 업/다운로드가 올바르게 동작하는지 확인
		{"test_sse_s3_encrypted_transfer_13b", func(t *testing.T) { testSSES3Write(t, 13) }},
		// SSE-S3 설정하여 업로드한 오브젝트의 헤더정보읽기가 가능한지 확인
		{"test_sse_s3_encryption_method_head", testSSES3Head},
		// 멀티파트업로드를 SSE-S3 설정하여 업로드 가능 확인
		{"test_sse_s3_encryption_multipart_upload", testSSES3MultipartUpload},
		// 버킷의 SSE-S3 설정 확인
		{"test_get_bucket_encryption", testSSES3GetBucketEncryption},
		// 버킷의 SSE-S3 설정이 가능한지 확인
		{"test_put_bucket_encryption", testSSES3PutBucketEncryption},
		// 버킷의 SSE-S3 설정 삭제가 가능한지 확인
		{"test_delete_bucket_encryption", testSSES3DeleteBucketEncryption},
		// 버킷의 SSE-S3 설정이 오브젝트에 반영되는지 확인
		{"test_put_bucket_encryption_and_object_set_check", testSSES3DefaultObjects},
		// 버킷에 SSE-S3 설정하여 업로드한 1kb 오브젝트를 복사 가능한지 확인
		{"test_copy_object_encryption_1kb", func(t *testing.T) { testSSES3Copy(t, 1024) }},
		// 버킷에 SSE-S3 설정하여 업로드한 256kb 오브젝트를 복사 가능한지 확인
		{"test_copy_object_encryption_256kb", func(t *testing.T) { testSSES3Copy(t, 256*1024) }},
		// 버킷에 SSE-S3 설정하여 업로드한 1mb 오브젝트를 복사 가능한지 확인
		{"test_copy_object_encryption_1mb", func(t *testing.T) { testSSES3Copy(t, 1024*1024) }},
		// [버킷에 SSE-S3 설정] 업로드, 다운로드 성공 확인
		{"test_sse_s3_bucket_put_get", func(t *testing.T) { testSSES3DefaultPutGet(t, false) }},
		// [버킷에 SSE-S3 설정, SignatureVersion4, UseChunkEncoding = true] 업로드, 다운로드 성공 확인
		{"test_sse_s3_bucket_put_get_use_chunk_encoding", func(t *testing.T) { testSSES3DefaultPutGet(t, true) }},
		// [버킷에 SSE-S3 설정, SignatureVersion4, UseChunkEncoding = false] 업로드, 다운로드 성공 확인
		{"test_sse_s3_bucket_put_get_not_chunk_encoding", func(t *testing.T) { testSSES3DefaultPutGet(t, false) }},
		// [버킷에 SSE-S3 설정]PresignedURL로 오브젝트 업로드, 다운로드 성공 확인
		{"test_sse_s3_bucket_presigned_url_put_get", testSSES3Presigned},
		// [버킷에 SSE-S3 설정, SignatureVersion4]PresignedURL로 오브젝트 업로드, 다운로드 성공 확인
		{"test_sse_s3_bucket_presigned_url_put_get_v4", testSSES3Presigned},
		// SSE-S3설정한 오브젝트를 여러번 반복하여 다운로드 성공 확인
		{"test_sse_s3_get_object_many", testSSES3GetMany},
		// SSE-S3설정한 오브젝트를 여러번 반복하여 Range 다운로드 성공 확인
		{"test_sse_s3_range_object_many", testSSES3RangeMany},
		// SSE-S3 설정하여 멀티파트로 업로드한 오브젝트를 multi copy 로 복사 가능한지 확인
		{"test_sse_s3_encryption_multipart_copy_part_upload", testSSES3MultipartCopy},
		// SSE-S3 설정하여 Multipart와 Copy Part를 모두 사용하여 오브젝트가 업로드 가능한지 확인
		{"test_sse_s3_encryption_multipart_copy_many", testSSES3MultipartCopyMany},
		// sse-s3설정은 소급적용 되지 않음을 확인
		{"test_sse_s3_not_retroactive", testSSES3NotRetroactive},
		// SSE-S3 버킷에서 업로드한 오브젝트를 멀티파트 업로드로 덮어쓰기 성공 확인
		{"test_sse_s3_multipart_upload_overwrite_existing_object", testSSES3MultipartUploadOverwriteExistingObject},
		// SSE-S3 버킷에서 멀티파트 업로드한 오브젝트를 PutObject로 덮어쓰기 성공 확인
		{"test_sse_s3_put_object_overwrite_multipart_upload", testSSES3PutObjectOverwriteMultipartUpload},
	}
	for _, tc := range tests {
		t.Run(tc.name, tc.run)
	}
}

func testSSES3Write(t *testing.T, size int) {
	t.Helper()
	s := newSuite(t)
	bucket := s.bucket(t)
	body := deterministicBody(size)
	if _, err := s.client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String("test"), Body: bytes.NewReader(body), ServerSideEncryption: types.ServerSideEncryptionAes256}); err != nil {
		t.Fatal(err)
	}
	// Java testEncryptionSseS3Write asserts SSE on GetObject, not PutObject response.
	assertSSEObject(t, s, bucket, "test", body, true)
}

func testSSES3Head(t *testing.T) {
	s := newSuite(t)
	bucket := s.bucket(t)
	body := deterministicBody(1000)
	_, err := s.client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String("obj"), Body: bytes.NewReader(body), Metadata: map[string]string{"foo": "bar"}, ServerSideEncryption: types.ServerSideEncryptionAes256})
	if err != nil {
		t.Fatal(err)
	}
	head, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String("obj")})
	if err != nil {
		t.Fatal(err)
	}
	if head.Metadata["foo"] != "bar" || head.ServerSideEncryption != types.ServerSideEncryptionAes256 {
		t.Fatalf("HeadObject metadata=%v encryption=%q", head.Metadata, head.ServerSideEncryption)
	}
}

func testSSES3MultipartUpload(t *testing.T) {
	s := newSuite(t)
	bucket := s.bucket(t)
	body := deterministicBody(50 * 1024 * 1024)
	completeMultipart(t, s.client, bucket, "multipartEnc", body, true, map[string]string{"foo": "bar"})
	head, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String("multipartEnc")})
	if err != nil {
		t.Fatal(err)
	}
	if head.ContentLength == nil || *head.ContentLength != int64(len(body)) || head.ContentType == nil || *head.ContentType != "text/plain" || head.Metadata["foo"] != "bar" || head.ServerSideEncryption != types.ServerSideEncryptionAes256 {
		t.Fatalf("multipart head=%#v", head)
	}
	assertObjectRanges(t, s.client, bucket, "multipartEnc", body, []int{1024 * 1024, 10 * 1024 * 1024})
}

func testSSES3GetBucketEncryption(t *testing.T) {
	s := newSuite(t)
	bucket := s.bucket(t)
	out, err := s.client.GetBucketEncryption(context.Background(), &s3.GetBucketEncryptionInput{Bucket: aws.String(bucket)})
	if s.cfg.Endpoint() == "" {
		if err != nil {
			t.Fatal(err)
		}
		assertAESRule(t, out.ServerSideEncryptionConfiguration)
		return
	}
	if err == nil {
		t.Fatalf("GetBucketEncryption unexpectedly succeeded: %#v", out)
	}
}

func testSSES3PutBucketEncryption(t *testing.T) {
	s := newSuite(t)
	bucket := s.bucket(t)
	putAESBucketEncryption(t, s.client, bucket)
}

func testSSES3DeleteBucketEncryption(t *testing.T) {
	s := newSuite(t)
	bucket := s.bucket(t)
	putAESBucketEncryption(t, s.client, bucket)
	getAndAssertAESBucketEncryption(t, s.client, bucket)
	if _, err := s.client.DeleteBucketEncryption(context.Background(), &s3.DeleteBucketEncryptionInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatal(err)
	}
	out, err := s.client.GetBucketEncryption(context.Background(), &s3.GetBucketEncryptionInput{Bucket: aws.String(bucket)})
	if s.cfg.Endpoint() == "" {
		if err != nil {
			t.Fatal(err)
		}
		assertAESRule(t, out.ServerSideEncryptionConfiguration)
	} else if err == nil {
		t.Fatalf("bucket encryption remains after delete: %#v", out)
	}
}

func testSSES3DefaultObjects(t *testing.T) {
	s := newSuite(t)
	bucket := s.bucket(t)
	putAESBucketEncryption(t, s.client, bucket)
	getAndAssertAESBucketEncryption(t, s.client, bucket)
	for _, key := range []string{"for/bar", "test/"} {
		put(t, s, bucket, key, key, nil)
		head, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
		if err != nil || head.ServerSideEncryption != types.ServerSideEncryptionAes256 {
			t.Fatalf("HeadObject(%q) encryption=%q err=%v", key, head.ServerSideEncryption, err)
		}
	}
}

func testSSES3Copy(t *testing.T, size int) {
	t.Helper()
	s := newSuite(t)
	bucket := s.bucket(t)
	putAESBucketEncryption(t, s.client, bucket)
	body := deterministicBody(size)
	_, err := s.client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String("bar"), Body: bytes.NewReader(body)})
	if err != nil {
		t.Fatal(err)
	}
	assertSSEObject(t, s, bucket, "bar", body, true)
	if _, err := s.client.CopyObject(context.Background(), &s3.CopyObjectInput{Bucket: aws.String(bucket), Key: aws.String("foo"), CopySource: aws.String(bucket + "/bar")}); err != nil {
		t.Fatal(err)
	}
	assertSSEObject(t, s, bucket, "foo", body, true)
}

func testSSES3DefaultPutGet(t *testing.T, chunked bool) {
	t.Helper()
	s := newSuite(t)
	bucket := s.bucket(t)
	putAESBucketEncryption(t, s.client, bucket)
	getAndAssertAESBucketEncryption(t, s.client, bucket)
	body := deterministicBody(1000)
	putObjectMaybeChunked(t, s.client, bucket, "bar", body, chunked)
	assertSSEObject(t, s, bucket, "bar", body, true)
}

func testSSES3Presigned(t *testing.T) {
	s := newSuite(t)
	bucket := s.bucket(t)
	putAESBucketEncryption(t, s.client, bucket)
	getAndAssertAESBucketEncryption(t, s.client, bucket)
	presign := s3.NewPresignClient(s.client)
	body := []byte("foo")
	putURL, err := presign.PresignPutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String("foo")})
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(http.MethodPut, putURL.URL, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	for name, values := range putURL.SignedHeader {
		if strings.EqualFold(name, "Host") {
			request.Host = values[0]
		} else {
			request.Header[name] = append([]string(nil), values...)
		}
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("presigned PUT status=%d", response.StatusCode)
	}
	getURL, err := presign.PresignGetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String("foo")})
	if err != nil {
		t.Fatal(err)
	}
	response, err = http.Get(getURL.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	got, readErr := io.ReadAll(response.Body)
	if response.StatusCode != http.StatusOK || readErr != nil || !bytes.Equal(got, body) {
		t.Fatalf("presigned GET status=%d body=%q err=%v", response.StatusCode, got, readErr)
	}
}

func testSSES3GetMany(t *testing.T) {
	s := newSuite(t)
	bucket := s.bucket(t)
	putAESBucketEncryption(t, s.client, bucket)
	body := deterministicBody(15 * 1024 * 1024)
	putBytes(t, s.client, bucket, "foo", body)
	for i := 0; i < 50; i++ {
		assertSSEObject(t, s, bucket, "foo", body, true)
	}
}

func testSSES3RangeMany(t *testing.T) {
	s := newSuite(t)
	bucket := s.bucket(t)
	putAESBucketEncryption(t, s.client, bucket)
	body := deterministicBody(15 * 1024 * 1024)
	putBytes(t, s.client, bucket, "foo", body)
	for i := 0; i < 100; i++ {
		length := 1 + (i*7919)%65536
		start := (i * 104729) % (len(body) - length)
		assertRange(t, s.client, bucket, "foo", body, start, start+length-1)
	}
}

func testSSES3MultipartCopy(t *testing.T) {
	s := newSuite(t)
	bucket := s.bucket(t)
	body := deterministicBody(50 * 1024 * 1024)
	completeMultipart(t, s.client, bucket, "multipartEnc", body, true, map[string]string{"foo": "bar"})
	copyMultipart(t, s.client, bucket, "multipartEnc", "multipartEncCopy", len(body), map[string]string{"foo": "bar"})
	assertObjectRanges(t, s.client, bucket, "multipartEncCopy", body, []int{1024 * 1024})
}

func testSSES3MultipartCopyMany(t *testing.T) {
	s := newSuite(t)
	bucket := s.bucket(t)
	body := deterministicBody(10 * 1024 * 1024)
	completeMultipart(t, s.client, bucket, "multipartEnc", body, true, nil)
	first := append(append([]byte(nil), body...), body...)
	copyThenUpload(t, s.client, bucket, "multipartEnc", "my_multipart1", len(body), body)
	assertObjectRanges(t, s.client, bucket, "my_multipart1", first, []int{1024 * 1024})
	second := append(append([]byte(nil), first...), body...)
	copyThenUpload(t, s.client, bucket, "my_multipart1", "my_multipart2", len(first), body)
	assertObjectRanges(t, s.client, bucket, "my_multipart2", second, []int{1024 * 1024})
}

func testSSES3NotRetroactive(t *testing.T) {
	s := newSuite(t)
	bucket := s.bucket(t)
	plain := deterministicBody(1000)
	putBytes(t, s.client, bucket, "put", plain)
	if _, err := s.client.CopyObject(context.Background(), &s3.CopyObjectInput{Bucket: aws.String(bucket), Key: aws.String("copy"), CopySource: aws.String(bucket + "/put")}); err != nil {
		t.Fatal(err)
	}
	completeMultipart(t, s.client, bucket, "multi", deterministicBody(ssePartSize), false, nil)
	putAESBucketEncryption(t, s.client, bucket)
	assertObjectBytes(t, s.client, bucket, "put", plain)
	assertObjectBytes(t, s.client, bucket, "copy", plain)
	assertObjectBytes(t, s.client, bucket, "multi", deterministicBody(ssePartSize))
	encrypted := deterministicBody(1000)
	putBytes(t, s.client, bucket, "put2", encrypted)
	if _, err := s.client.CopyObject(context.Background(), &s3.CopyObjectInput{Bucket: aws.String(bucket), Key: aws.String("copy2"), CopySource: aws.String(bucket + "/put2")}); err != nil {
		t.Fatal(err)
	}
	multipartBody := deterministicBody(ssePartSize)
	completeMultipart(t, s.client, bucket, "multi2", multipartBody, false, nil)
	if _, err := s.client.DeleteBucketEncryption(context.Background(), &s3.DeleteBucketEncryptionInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatal(err)
	}
	assertSSEObject(t, s, bucket, "put2", encrypted, true)
	assertSSEObject(t, s, bucket, "copy2", encrypted, true)
	assertSSEObject(t, s, bucket, "multi2", multipartBody, true)
}

func testSSES3MultipartUploadOverwriteExistingObject(t *testing.T) {
	s := newSuite(t)
	bucket := s.bucket(t)
	key := "test_sse_s3_multipart_upload_overwrite_existing_object"
	putAESBucketEncryption(t, s.client, bucket)
	partBody := deterministicBody(5 * 1024 * 1024)
	putBytes(t, s.client, bucket, key, partBody)
	created, err := s.client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}
	parts := make([]types.CompletedPart, 0, 2)
	var want []byte
	for number := int32(1); number <= 2; number++ {
		out, err := s.client.UploadPart(context.Background(), &s3.UploadPartInput{Bucket: aws.String(bucket), Key: aws.String(key), UploadId: created.UploadId, PartNumber: aws.Int32(number), Body: bytes.NewReader(partBody)})
		if err != nil {
			t.Fatal(err)
		}
		parts = append(parts, types.CompletedPart{ETag: out.ETag, PartNumber: aws.Int32(number)})
		want = append(want, partBody...)
	}
	if _, err := s.client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(key), UploadId: created.UploadId, MultipartUpload: &types.CompletedMultipartUpload{Parts: parts}}); err != nil {
		t.Fatal(err)
	}
	assertSSEObject(t, s, bucket, key, want, true)
}

func testSSES3PutObjectOverwriteMultipartUpload(t *testing.T) {
	s := newSuite(t)
	bucket := s.bucket(t)
	key := "test_sse_s3_put_object_overwrite_multipart_upload"
	putAESBucketEncryption(t, s.client, bucket)
	completeMultipart(t, s.client, bucket, key, deterministicBody(10*1024*1024), false, nil)
	content := deterministicBody(1 * 1024 * 1024)
	putBytes(t, s.client, bucket, key, content)
	head, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil || aws.ToInt64(head.ContentLength) != int64(len(content)) || head.ServerSideEncryption != types.ServerSideEncryptionAes256 {
		t.Fatalf("HeadObject length=%d encryption=%q err=%v", aws.ToInt64(head.ContentLength), head.ServerSideEncryption, err)
	}
	assertSSEObject(t, s, bucket, key, content, true)
	assertObjectRanges(t, s.client, bucket, key, content, []int{1024})
}

func deterministicBody(size int) []byte {
	pattern := []byte("0123456789abcdefghijklmnopqrstuvwxyz")
	body := make([]byte, size)
	for index := range body {
		body[index] = pattern[index%len(pattern)]
	}
	return body
}

func aesBucketConfiguration() *types.ServerSideEncryptionConfiguration {
	return &types.ServerSideEncryptionConfiguration{Rules: []types.ServerSideEncryptionRule{{ApplyServerSideEncryptionByDefault: &types.ServerSideEncryptionByDefault{SSEAlgorithm: types.ServerSideEncryptionAes256}, BucketKeyEnabled: aws.Bool(false)}}}
}

func putAESBucketEncryption(t *testing.T, client *s3.Client, bucket string) {
	t.Helper()
	_, err := client.PutBucketEncryption(context.Background(), &s3.PutBucketEncryptionInput{Bucket: aws.String(bucket), ServerSideEncryptionConfiguration: aesBucketConfiguration()})
	if err != nil {
		t.Fatal(err)
	}
}

func getAndAssertAESBucketEncryption(t *testing.T, client *s3.Client, bucket string) {
	t.Helper()
	out, err := client.GetBucketEncryption(context.Background(), &s3.GetBucketEncryptionInput{Bucket: aws.String(bucket)})
	if err != nil {
		t.Fatal(err)
	}
	assertAESRule(t, out.ServerSideEncryptionConfiguration)
}

func assertAESRule(t *testing.T, configuration *types.ServerSideEncryptionConfiguration) {
	t.Helper()
	if configuration == nil || len(configuration.Rules) != 1 || configuration.Rules[0].ApplyServerSideEncryptionByDefault == nil || configuration.Rules[0].ApplyServerSideEncryptionByDefault.SSEAlgorithm != types.ServerSideEncryptionAes256 {
		t.Fatalf("encryption configuration=%#v", configuration)
	}
}

func putBytes(t *testing.T, client *s3.Client, bucket, key string, body []byte) {
	t.Helper()
	if _, err := client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader(body)}); err != nil {
		t.Fatal(err)
	}
}

func assertSSEObject(t *testing.T, s *suite, bucket, key string, want []byte, encrypted bool) {
	t.Helper()
	out, err := s.client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}
	defer out.Body.Close()
	got, err := io.ReadAll(out.Body)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("GetObject(%q) size=%d err=%v, want size=%d", key, len(got), err, len(want))
	}
	if encrypted != (out.ServerSideEncryption == types.ServerSideEncryptionAes256) {
		t.Fatalf("GetObject(%q) encryption=%q, want encrypted=%v", key, out.ServerSideEncryption, encrypted)
	}
}

func assertObjectBytes(t *testing.T, client *s3.Client, bucket, key string, want []byte) {
	t.Helper()
	out, err := client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}
	defer out.Body.Close()
	got, err := io.ReadAll(out.Body)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("GetObject(%q) size=%d err=%v, want size=%d", key, len(got), err, len(want))
	}
}

func completeMultipart(t *testing.T, client *s3.Client, bucket, key string, body []byte, explicitSSE bool, metadata map[string]string) {
	t.Helper()
	input := &s3.CreateMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(key), ContentType: aws.String("text/plain"), Metadata: metadata}
	if explicitSSE {
		input.ServerSideEncryption = types.ServerSideEncryptionAes256
	}
	created, err := client.CreateMultipartUpload(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	parts := uploadByteParts(t, client, bucket, key, aws.ToString(created.UploadId), body, 1)
	_, err = client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(key), UploadId: created.UploadId, MultipartUpload: &types.CompletedMultipartUpload{Parts: parts}})
	if err != nil {
		t.Fatal(err)
	}
}

func uploadByteParts(t *testing.T, client *s3.Client, bucket, key, uploadID string, body []byte, firstPart int32) []types.CompletedPart {
	t.Helper()
	parts := make([]types.CompletedPart, 0, (len(body)+ssePartSize-1)/ssePartSize)
	partNumber := firstPart
	for start := 0; start < len(body); start += ssePartSize {
		end := min(start+ssePartSize, len(body))
		out, err := client.UploadPart(context.Background(), &s3.UploadPartInput{Bucket: aws.String(bucket), Key: aws.String(key), UploadId: aws.String(uploadID), PartNumber: aws.Int32(partNumber), Body: bytes.NewReader(body[start:end])})
		if err != nil {
			t.Fatal(err)
		}
		parts = append(parts, types.CompletedPart{ETag: out.ETag, PartNumber: aws.Int32(partNumber)})
		partNumber++
	}
	return parts
}

func copyMultipart(t *testing.T, client *s3.Client, bucket, sourceKey, targetKey string, size int, metadata map[string]string) {
	t.Helper()
	created, err := client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(targetKey), Metadata: metadata})
	if err != nil {
		t.Fatal(err)
	}
	parts := copyByteParts(t, client, bucket, sourceKey, targetKey, aws.ToString(created.UploadId), size)
	_, err = client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(targetKey), UploadId: created.UploadId, MultipartUpload: &types.CompletedMultipartUpload{Parts: parts}})
	if err != nil {
		t.Fatal(err)
	}
}

func copyThenUpload(t *testing.T, client *s3.Client, bucket, sourceKey, targetKey string, sourceSize int, appended []byte) {
	t.Helper()
	created, err := client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(targetKey)})
	if err != nil {
		t.Fatal(err)
	}
	uploadID := aws.ToString(created.UploadId)
	parts := copyByteParts(t, client, bucket, sourceKey, targetKey, uploadID, sourceSize)
	parts = append(parts, uploadByteParts(t, client, bucket, targetKey, uploadID, appended, int32(len(parts)+1))...)
	_, err = client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(targetKey), UploadId: created.UploadId, MultipartUpload: &types.CompletedMultipartUpload{Parts: parts}})
	if err != nil {
		t.Fatal(err)
	}
}

func copyByteParts(t *testing.T, client *s3.Client, bucket, sourceKey, targetKey, uploadID string, size int) []types.CompletedPart {
	t.Helper()
	parts := make([]types.CompletedPart, 0, (size+ssePartSize-1)/ssePartSize)
	partNumber := int32(1)
	for start := 0; start < size; start += ssePartSize {
		end := min(start+ssePartSize, size) - 1
		out, err := client.UploadPartCopy(context.Background(), &s3.UploadPartCopyInput{Bucket: aws.String(bucket), Key: aws.String(targetKey), UploadId: aws.String(uploadID), PartNumber: aws.Int32(partNumber), CopySource: aws.String(bucket + "/" + sourceKey), CopySourceRange: aws.String(fmt.Sprintf("bytes=%d-%d", start, end))})
		if err != nil {
			t.Fatal(err)
		}
		if out.CopyPartResult == nil || out.CopyPartResult.ETag == nil {
			t.Fatalf("UploadPartCopy part %d returned no ETag", partNumber)
		}
		parts = append(parts, types.CompletedPart{ETag: out.CopyPartResult.ETag, PartNumber: aws.Int32(partNumber)})
		partNumber++
	}
	return parts
}

func assertObjectRanges(t *testing.T, client *s3.Client, bucket, key string, body []byte, steps []int) {
	t.Helper()
	for _, step := range steps {
		for start := 0; start < len(body); start += step {
			end := min(start+step, len(body)) - 1
			assertRange(t, client, bucket, key, body, start, end)
		}
	}
}

func assertRange(t *testing.T, client *s3.Client, bucket, key string, body []byte, start, end int) {
	t.Helper()
	out, err := client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Range: aws.String(fmt.Sprintf("bytes=%d-%d", start, end))})
	if err != nil {
		t.Fatal(err)
	}
	defer out.Body.Close()
	got, err := io.ReadAll(out.Body)
	if err != nil || !bytes.Equal(got, body[start:end+1]) {
		t.Fatalf("range %d-%d size=%d err=%v", start, end, len(got), err)
	}
}
