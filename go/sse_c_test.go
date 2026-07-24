package s3tests

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

const (
	sseCAlgorithm = "AES256"
	sseCKey       = "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs="
	sseCKeyMD5    = "DWygnHRtgiJ77HCm+1rvHw=="
	sseCOtherKey  = "6b+WOZ1T3cqZMxgThRcXAQBrS5mXKdDUphvpxptl9/4="
	sseCOtherMD5  = "arxBvwY2V4SiOne6yppVPQ=="
)

func TestSSEC(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		run  func(*testing.T)
	}{
		// 1Byte 오브젝트를 SSE-C 설정하여 업/다운로드가 올바르게 동작하는지 확인
		{"test_encrypted_transfer_1b", func(t *testing.T) { testSSECWrite(t, 1) }},
		// 1KB 오브젝트를 SSE-C 설정하여 업/다운로드가 올바르게 동작하는지 확인
		{"test_encrypted_transfer_1kb", func(t *testing.T) { testSSECWrite(t, 1024) }},
		// 1MB 오브젝트를 SSE-C 설정하여 업/다운로드가 올바르게 동작하는지 확인
		{"test_encrypted_transfer_1mb", func(t *testing.T) { testSSECWrite(t, 1024*1024) }},
		// 13Byte 오브젝트를 SSE-C 설정하여 업/다운로드가 올바르게 동작하는지 확인
		{"test_encrypted_transfer_13b", func(t *testing.T) { testSSECWrite(t, 13) }},
		// SSE-C 설정하여 업로드한 오브젝트를 SSE-C 설정하여 헤더정보읽기가 가능한지 확인
		{"test_encryption_sse_c_method_head", testSSECHead},
		// SSE-C 설정하여 업로드한 오브젝트를 SSE-C 설정없이 다운로드 실패 확인
		{"test_encryption_sse_c_present", testSSECMissingOnGet},
		// SSE-C 설정하여 업로드한 오브젝트와 다른 SSE-C 설정으로 다운로드 실패 확인
		{"test_encryption_sse_c_other_key", testSSECOtherKey},
		// SSE-C 설정값중 key-md5값이 올바르지 않을 경우 업로드 실패 확인
		{"test_encryption_sse_c_invalid_md5", testSSECInvalidMD5},
		// SSE-C 설정값중 key-md5값을 누락했을 경우 업로드 확인
		{"test_encryption_sse_c_no_md5", testSSECNoMD5},
		// SSE-C 설정값중 key값을 누락했을 경우 업로드 실패 확인
		{"test_encryption_sse_c_no_key", testSSECNoKey},
		// SSE-C 설정값중 algorithm값을 누락했을 경우 업로드 실패 확인
		{"test_encryption_key_no_sse_c", testSSECNoAlgorithm},
		// 멀티파트업로드를 SSE-C 설정하여 업로드 가능 확인
		{"test_encryption_sse_c_multipart_upload", testSSECMultipart},
		// SSE-C 설정하여 멀티파트 업로드한 오브젝트와 다른 SSE-C 설정으로 다운로드 실패 확인
		{"test_encryption_sse_c_multipart_bad_download", testSSECMultipartBadDownload},
		// Post 방식으로 SSE-C 설정하여 오브젝트 업로드가 올바르게 동작하는지 확인
		{"test_encryption_sse_c_post_object_authenticated_request", testSSECPost},
		// SSE-C설정한 오브젝트를 여러번 반복하여 다운로드 성공 확인
		{"test_encryption_sse_c_get_object_many", testSSECGetMany},
		// SSE-C설정한 오브젝트를 여러번 반복하여 Range 다운로드 성공 확인
		{"test_encryption_sse_c_range_object_many", testSSECRangeMany},
		// SSE-C 설정하여 멀티파트로 업로드한 오브젝트를 multi copy 로 복사 가능한지 확인
		{"test_sse_c_encryption_multipart_copy_part_upload", testSSECMultipartCopy},
		// 멀티파트 오브젝트를 여러 번 복사하여 정상적으로 동작하는지 확인
		{"test_sse_c_encryption_multipart_copy_many", testSSECMultipartCopyMany},
		// SSE-C로 업로드한 오브젝트를 멀티파트 업로드로 덮어쓰기 성공 확인
		{"test_encryption_sse_c_multipart_upload_overwrite_existing_object", testSSECMultipartUploadOverwriteExistingObject},
		// SSE-C 멀티파트 업로드한 오브젝트를 PutObject로 덮어쓰기 성공 확인
		{"test_encryption_sse_c_put_object_overwrite_multipart_upload", testSSECPutObjectOverwriteMultipartUpload},
	}
	for _, tc := range tests {
		t.Run(tc.name, tc.run)
	}
}

func sseCPutInput(bucket, key string, body []byte) *s3.PutObjectInput {
	return &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader(body), SSECustomerAlgorithm: aws.String(sseCAlgorithm), SSECustomerKey: aws.String(sseCKey), SSECustomerKeyMD5: aws.String(sseCKeyMD5)}
}

func sseCGetInput(bucket, key string) *s3.GetObjectInput {
	return &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), SSECustomerAlgorithm: aws.String(sseCAlgorithm), SSECustomerKey: aws.String(sseCKey), SSECustomerKeyMD5: aws.String(sseCKeyMD5)}
}

func testSSECWrite(t *testing.T, size int) {
	t.Helper()
	s := newSuite(t)
	bucket := s.bucket(t)
	body := deterministicBody(size)
	if _, err := s.client.PutObject(context.Background(), sseCPutInput(bucket, "test", body)); err != nil {
		t.Fatal(err)
	}
	assertSSECObject(t, s.client, bucket, "test", body)
}

func testSSECHead(t *testing.T) {
	s := newSuite(t)
	bucket := s.bucket(t)
	if _, err := s.client.PutObject(context.Background(), sseCPutInput(bucket, "obj", deterministicBody(1000))); err != nil {
		t.Fatal(err)
	}
	_, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String("obj")})
	assertHTTPError(t, err, 400)
	out, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String("obj"), SSECustomerAlgorithm: aws.String(sseCAlgorithm), SSECustomerKey: aws.String(sseCKey), SSECustomerKeyMD5: aws.String(sseCKeyMD5)})
	if err != nil || aws.ToString(out.SSECustomerAlgorithm) != sseCAlgorithm {
		t.Fatalf("HeadObject SSE-C algorithm=%q err=%v", aws.ToString(out.SSECustomerAlgorithm), err)
	}
}

func testSSECMissingOnGet(t *testing.T) {
	s, bucket := newSSECObject(t, 1000)
	_, err := s.client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String("obj")})
	assertHTTPError(t, err, 400)
}

func testSSECOtherKey(t *testing.T) {
	s, bucket := newSSECObject(t, 100)
	_, err := s.client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String("obj"), SSECustomerAlgorithm: aws.String(sseCAlgorithm), SSECustomerKey: aws.String(sseCOtherKey), SSECustomerKeyMD5: aws.String(sseCOtherMD5)})
	assertHTTPError(t, err, 403)
}

func testSSECInvalidMD5(t *testing.T) {
	s := newSuite(t)
	bucket := s.bucket(t)
	input := sseCPutInput(bucket, "obj", deterministicBody(100))
	input.SSECustomerKeyMD5 = aws.String("AAAAAAAAAAAAAAAAAAAAAA==")
	_, err := s.client.PutObject(context.Background(), input)
	assertHTTPError(t, err, 400)
}

func testSSECNoMD5(t *testing.T) {
	s := newSuite(t)
	bucket := s.bucket(t)
	input := sseCPutInput(bucket, "obj", deterministicBody(100))
	input.SSECustomerKeyMD5 = nil
	_, err := s.client.PutObject(context.Background(), input, func(options *s3.Options) {
		options.APIOptions = append(options.APIOptions, func(stack *middleware.Stack) error {
			return stack.Build.Add(middleware.BuildMiddlewareFunc("remove-sse-c-md5", func(ctx context.Context, in middleware.BuildInput, next middleware.BuildHandler) (middleware.BuildOutput, middleware.Metadata, error) {
				request, ok := in.Request.(*smithyhttp.Request)
				if ok {
					request.Header.Del("X-Amz-Server-Side-Encryption-Customer-Key-Md5")
				}
				return next.HandleBuild(ctx, in)
			}), middleware.After)
		})
	})
	assertHTTPError(t, err, 400)
}

func testSSECNoKey(t *testing.T) {
	s := newSuite(t)
	bucket := s.bucket(t)
	_, err := s.client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String("obj"), Body: bytes.NewReader(deterministicBody(100)), SSECustomerKeyMD5: aws.String(sseCKeyMD5)})
	assertHTTPError(t, err, 400)
}

func testSSECNoAlgorithm(t *testing.T) {
	s := newSuite(t)
	bucket := s.bucket(t)
	_, err := s.client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String("obj"), Body: bytes.NewReader(deterministicBody(100)), SSECustomerKey: aws.String(sseCKey), SSECustomerKeyMD5: aws.String(sseCKeyMD5)})
	assertHTTPError(t, err, 400)
}

func newSSECObject(t *testing.T, size int) (*suite, string) {
	t.Helper()
	s := newSuite(t)
	bucket := s.bucket(t)
	if _, err := s.client.PutObject(context.Background(), sseCPutInput(bucket, "obj", deterministicBody(size))); err != nil {
		t.Fatal(err)
	}
	return s, bucket
}

func testSSECMultipart(t *testing.T) {
	s := newSuite(t)
	bucket := s.bucket(t)
	body := deterministicBody(50 * 1024 * 1024)
	completeSSECMultipart(t, s.client, bucket, "multipartEnc", body, map[string]string{"foo": "bar"})
	headSSEC(t, s.client, bucket, "multipartEnc", map[string]string{"foo": "bar"})
	assertSSECRanges(t, s.client, bucket, "multipartEnc", body, []int{1024 * 1024, 10 * 1024 * 1024})
}

func testSSECMultipartBadDownload(t *testing.T) {
	s := newSuite(t)
	bucket := s.bucket(t)
	body := deterministicBody(50 * 1024 * 1024)
	completeSSECMultipart(t, s.client, bucket, "multipartEnc", body, map[string]string{"foo": "bar"})
	headSSEC(t, s.client, bucket, "multipartEnc", map[string]string{"foo": "bar"})
	_, err := s.client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String("multipartEnc"), SSECustomerAlgorithm: aws.String(sseCAlgorithm), SSECustomerKey: aws.String(sseCOtherKey), SSECustomerKeyMD5: aws.String(sseCOtherMD5)})
	assertHTTPError(t, err, 403)
}

func testSSECPost(t *testing.T) {
	s := newSuite(t)
	if s.cfg.Endpoint() == "" {
		t.Skip("SSE-C POST object test is not supported on AWS")
	}
	bucket := s.bucket(t)
	key := "foo.txt"
	expiration := time.Now().UTC().Add(100 * time.Minute).Format(time.RFC3339)
	document := map[string]any{"expiration": expiration, "conditions": []any{map[string]string{"bucket": bucket}, []string{"starts-with", "$key", "foo"}, map[string]string{"acl": "private"}, []string{"starts-with", "$Content-Type", "text/plain"}, []string{"starts-with", "$x-amz-server-side-encryption-customer-algorithm", sseCAlgorithm}, []string{"starts-with", "$x-amz-server-side-encryption-customer-key", sseCKey}, []string{"starts-with", "$x-amz-server-side-encryption-customer-key-md5", sseCKeyMD5}, []any{"content-length-range", 0, 1024}}}
	encoded, _ := json.Marshal(document)
	policy := base64.StdEncoding.EncodeToString(encoded)
	mac := hmac.New(sha1.New, []byte(s.cfg.Main.SecretKey))
	_, _ = mac.Write([]byte(policy))
	signature := base64.StdEncoding.EncodeToString(mac.Sum(nil))
	var payload bytes.Buffer
	writer := multipart.NewWriter(&payload)
	fields := map[string]string{"key": key, "AWSAccessKeyId": s.cfg.Main.AccessKey, "acl": "private", "signature": signature, "policy": policy, "Content-Type": "text/plain", "x-amz-server-side-encryption-customer-algorithm": sseCAlgorithm, "x-amz-server-side-encryption-customer-key": sseCKey, "x-amz-server-side-encryption-customer-key-md5": sseCKeyMD5}
	for name, value := range fields {
		if err := writer.WriteField(name, value); err != nil {
			t.Fatal(err)
		}
	}
	header := make(textproto.MIMEHeader)
	header.Set("Content-Disposition", `form-data; name="file"; filename="foo.txt"`)
	header.Set("Content-Type", "text/plain")
	part, err := writer.CreatePart(header)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = part.Write([]byte("bar"))
	_ = writer.Close()
	url := strings.TrimRight(s.cfg.Endpoint(), "/") + "/" + bucket
	request, _ := http.NewRequest(http.MethodPost, url, &payload)
	request.Header.Set("Content-Type", writer.FormDataContentType())
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		t.Fatalf("POST status=%d", response.StatusCode)
	}
	assertSSECObject(t, s.client, bucket, key, []byte("bar"))
}

func testSSECGetMany(t *testing.T) {
	s, bucket := newSSECObject(t, 15*1024*1024)
	body := deterministicBody(15 * 1024 * 1024)
	for i := 0; i < 50; i++ {
		assertSSECObject(t, s.client, bucket, "obj", body)
	}
}

func testSSECRangeMany(t *testing.T) {
	s, bucket := newSSECObject(t, 15*1024*1024)
	body := deterministicBody(15 * 1024 * 1024)
	assertSSECObject(t, s.client, bucket, "obj", body)
	for i := 0; i < 50; i++ {
		length := 1 + (i*7919)%65536
		start := (i * 104729) % (len(body) - length)
		assertSSECRange(t, s.client, bucket, "obj", body, start, start+length-1)
	}
}

func testSSECMultipartCopy(t *testing.T) {
	s := newSuite(t)
	bucket := s.bucket(t)
	body := deterministicBody(50 * 1024 * 1024)
	completeSSECMultipart(t, s.client, bucket, "multipartEnc", body, nil)
	copySSECMultipart(t, s.client, bucket, "multipartEnc", "multipartEncCopy", len(body))
	assertSSECObject(t, s.client, bucket, "multipartEncCopy", body)
}

func testSSECMultipartCopyMany(t *testing.T) {
	s := newSuite(t)
	bucket := s.bucket(t)
	body := deterministicBody(10 * 1024 * 1024)
	completeMultipart(t, s.client, bucket, "multipartEnc", body, false, nil)
	copyThenUpload(t, s.client, bucket, "multipartEnc", "my_multipart1", len(body), body)
	first := append(append([]byte(nil), body...), body...)
	assertObjectBytes(t, s.client, bucket, "my_multipart1", first)
	copyThenUpload(t, s.client, bucket, "my_multipart1", "my_multipart2", len(first), body)
	second := append(append([]byte(nil), first...), body...)
	assertObjectBytes(t, s.client, bucket, "my_multipart2", second)
}

func testSSECMultipartUploadOverwriteExistingObject(t *testing.T) {
	s := newSuite(t)
	bucket := s.bucket(t)
	key := "test_encryption_sse_c_multipart_upload_overwrite_existing_object"
	partBody := deterministicBody(5 * 1024 * 1024)
	if _, err := s.client.PutObject(context.Background(), sseCPutInput(bucket, key, partBody)); err != nil {
		t.Fatal(err)
	}
	created, err := s.client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(key), SSECustomerAlgorithm: aws.String(sseCAlgorithm), SSECustomerKey: aws.String(sseCKey), SSECustomerKeyMD5: aws.String(sseCKeyMD5)})
	if err != nil {
		t.Fatal(err)
	}
	parts := make([]types.CompletedPart, 0, 2)
	var want []byte
	for number := int32(1); number <= 2; number++ {
		out, err := s.client.UploadPart(context.Background(), &s3.UploadPartInput{Bucket: aws.String(bucket), Key: aws.String(key), UploadId: created.UploadId, PartNumber: aws.Int32(number), Body: bytes.NewReader(partBody), SSECustomerAlgorithm: aws.String(sseCAlgorithm), SSECustomerKey: aws.String(sseCKey), SSECustomerKeyMD5: aws.String(sseCKeyMD5)})
		if err != nil {
			t.Fatal(err)
		}
		parts = append(parts, types.CompletedPart{ETag: out.ETag, PartNumber: aws.Int32(number)})
		want = append(want, partBody...)
	}
	if _, err := s.client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(key), UploadId: created.UploadId, MultipartUpload: &types.CompletedMultipartUpload{Parts: parts}}); err != nil {
		t.Fatal(err)
	}
	assertSSECObject(t, s.client, bucket, key, want)
}

func testSSECPutObjectOverwriteMultipartUpload(t *testing.T) {
	s := newSuite(t)
	bucket := s.bucket(t)
	key := "test_encryption_sse_c_put_object_overwrite_multipart_upload"
	completeSSECMultipart(t, s.client, bucket, key, deterministicBody(10*1024*1024), nil)
	content := deterministicBody(1 * 1024 * 1024)
	if _, err := s.client.PutObject(context.Background(), sseCPutInput(bucket, key, content)); err != nil {
		t.Fatal(err)
	}
	head, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), SSECustomerAlgorithm: aws.String(sseCAlgorithm), SSECustomerKey: aws.String(sseCKey), SSECustomerKeyMD5: aws.String(sseCKeyMD5)})
	if err != nil || aws.ToInt64(head.ContentLength) != int64(len(content)) || aws.ToString(head.SSECustomerAlgorithm) != sseCAlgorithm {
		t.Fatalf("HeadObject length=%d algorithm=%q err=%v", aws.ToInt64(head.ContentLength), aws.ToString(head.SSECustomerAlgorithm), err)
	}
	assertSSECObject(t, s.client, bucket, key, content)
	assertSSECRanges(t, s.client, bucket, key, content, []int{1024})
}

func completeSSECMultipart(t *testing.T, client *s3.Client, bucket, key string, body []byte, metadata map[string]string) {
	t.Helper()
	created, err := client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(key), Metadata: metadata, SSECustomerAlgorithm: aws.String(sseCAlgorithm), SSECustomerKey: aws.String(sseCKey), SSECustomerKeyMD5: aws.String(sseCKeyMD5)})
	if err != nil {
		t.Fatal(err)
	}
	parts := make([]types.CompletedPart, 0)
	for start, number := 0, int32(1); start < len(body); start, number = start+ssePartSize, number+1 {
		end := min(start+ssePartSize, len(body))
		out, err := client.UploadPart(context.Background(), &s3.UploadPartInput{Bucket: aws.String(bucket), Key: aws.String(key), UploadId: created.UploadId, PartNumber: aws.Int32(number), Body: bytes.NewReader(body[start:end]), SSECustomerAlgorithm: aws.String(sseCAlgorithm), SSECustomerKey: aws.String(sseCKey), SSECustomerKeyMD5: aws.String(sseCKeyMD5)})
		if err != nil {
			t.Fatal(err)
		}
		parts = append(parts, types.CompletedPart{ETag: out.ETag, PartNumber: aws.Int32(number)})
	}
	_, err = client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(key), UploadId: created.UploadId, MultipartUpload: &types.CompletedMultipartUpload{Parts: parts}})
	if err != nil {
		t.Fatal(err)
	}
}

func copySSECMultipart(t *testing.T, client *s3.Client, bucket, sourceKey, targetKey string, size int) {
	t.Helper()
	created, err := client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(targetKey), SSECustomerAlgorithm: aws.String(sseCAlgorithm), SSECustomerKey: aws.String(sseCKey), SSECustomerKeyMD5: aws.String(sseCKeyMD5)})
	if err != nil {
		t.Fatal(err)
	}
	parts := make([]types.CompletedPart, 0)
	for start, number := 0, int32(1); start < size; start, number = start+ssePartSize, number+1 {
		end := min(start+ssePartSize, size) - 1
		out, err := client.UploadPartCopy(context.Background(), &s3.UploadPartCopyInput{Bucket: aws.String(bucket), Key: aws.String(targetKey), UploadId: created.UploadId, PartNumber: aws.Int32(number), CopySource: aws.String(bucket + "/" + sourceKey), CopySourceRange: aws.String(fmt.Sprintf("bytes=%d-%d", start, end)), CopySourceSSECustomerAlgorithm: aws.String(sseCAlgorithm), CopySourceSSECustomerKey: aws.String(sseCKey), CopySourceSSECustomerKeyMD5: aws.String(sseCKeyMD5), SSECustomerAlgorithm: aws.String(sseCAlgorithm), SSECustomerKey: aws.String(sseCKey), SSECustomerKeyMD5: aws.String(sseCKeyMD5)})
		if err != nil || out.CopyPartResult == nil {
			t.Fatalf("UploadPartCopy part=%d err=%v", number, err)
		}
		parts = append(parts, types.CompletedPart{ETag: out.CopyPartResult.ETag, PartNumber: aws.Int32(number)})
	}
	_, err = client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(targetKey), UploadId: created.UploadId, MultipartUpload: &types.CompletedMultipartUpload{Parts: parts}})
	if err != nil {
		t.Fatal(err)
	}
}

func headSSEC(t *testing.T, client *s3.Client, bucket, key string, metadata map[string]string) {
	t.Helper()
	out, err := client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), SSECustomerAlgorithm: aws.String(sseCAlgorithm), SSECustomerKey: aws.String(sseCKey), SSECustomerKeyMD5: aws.String(sseCKeyMD5)})
	if err != nil || aws.ToString(out.SSECustomerAlgorithm) != sseCAlgorithm || (metadata != nil && out.Metadata["foo"] != metadata["foo"]) {
		t.Fatalf("HeadObject SSE-C metadata=%v algorithm=%q err=%v", out.Metadata, aws.ToString(out.SSECustomerAlgorithm), err)
	}
}

func assertSSECObject(t *testing.T, client *s3.Client, bucket, key string, want []byte) {
	t.Helper()
	out, err := client.GetObject(context.Background(), sseCGetInput(bucket, key))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Body.Close()
	got, err := io.ReadAll(out.Body)
	if err != nil || !bytes.Equal(got, want) || aws.ToString(out.SSECustomerAlgorithm) != sseCAlgorithm {
		t.Fatalf("GetObject size=%d algorithm=%q err=%v", len(got), aws.ToString(out.SSECustomerAlgorithm), err)
	}
}

func assertSSECRanges(t *testing.T, client *s3.Client, bucket, key string, body []byte, steps []int) {
	t.Helper()
	for _, step := range steps {
		for start := 0; start < len(body); start += step {
			end := min(start+step, len(body)) - 1
			assertSSECRange(t, client, bucket, key, body, start, end)
		}
	}
}

func assertSSECRange(t *testing.T, client *s3.Client, bucket, key string, body []byte, start, end int) {
	t.Helper()
	input := sseCGetInput(bucket, key)
	input.Range = aws.String(fmt.Sprintf("bytes=%d-%d", start, end))
	out, err := client.GetObject(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Body.Close()
	got, err := io.ReadAll(out.Body)
	if err != nil || !bytes.Equal(got, body[start:end+1]) {
		t.Fatalf("range %d-%d size=%d err=%v", start, end, len(got), err)
	}
}

func assertHTTPError(t *testing.T, err error, status int) {
	t.Helper()
	assertAPIError(t, err)
	var responseErr *smithyhttp.ResponseError
	if !errors.As(err, &responseErr) {
		t.Fatalf("error type = %T, want HTTP response error: %v", err, err)
	}
	if got := responseErr.HTTPStatusCode(); got != status {
		t.Fatalf("HTTP status = %d, want %d", got, status)
	}
}
