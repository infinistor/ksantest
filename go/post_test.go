package s3tests

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
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
)

type postResult struct {
	status int
	body   []byte
	url    string
}

// post 방식으로 권한없는 사용자가 파일 업로드할 경우 성공 확인
func TestPostObjectAnonymousRequest(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := postPublicBucket(t, s)
	result := sendPostForm(t, postBucketURL(s, bucket), map[string]string{"key": "foo.txt", "acl": "public-read", "Content-Type": "text/plain"}, "foo.txt", "text/plain", []byte("bar"))
	if result.status != 204 {
		t.Fatalf("POST status=%d body=%s", result.status, result.body)
	}
	if got := read(t, s, bucket, "foo.txt"); got != "bar" {
		t.Fatalf("body=%q", got)
	}
}

// post 방식으로 로그인 정보를 포함한 파일 업로드할 경우 성공 확인
func TestPostObjectAuthenticatedRequest(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 2)
	fields, policy := postV4Fields(s, bucket, "text/plain", "foo", 0, 1024, nil)
	fields["key"], fields["acl"], fields["Content-Type"] = "foo.txt", "private", "text/plain"
	fields["policy"] = policy
	result := sendPostForm(t, postBucketURL(s, bucket), fields, "foo.txt", "text/plain", []byte("bar"))
	if result.status != 204 {
		t.Fatalf("POST status=%d body=%s", result.status, result.body)
	}
	if got := read(t, s, bucket, "foo.txt"); got != "bar" {
		t.Fatalf("body=%q", got)
	}
}

// [성공시 반환상태값을 201로 설정] post 방식으로 권한없는 사용자가 파일 업로드시 에러체크가 올바른지 확인
func TestPostObjectSetSuccessCode(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := postPublicBucket(t, s)
	fields := map[string]string{"key": "foo.txt", "acl": "public-read", "Content-Type": "text/plain", "success_action_status": "201"}
	result := sendPostForm(t, postBucketURL(s, bucket), fields, "foo.txt", "text/plain", []byte("bar"))
	if result.status != 201 {
		t.Fatalf("POST status=%d want=201 body=%s", result.status, result.body)
	}
	if got := read(t, s, bucket, "foo.txt"); got != "bar" {
		t.Fatalf("body=%q", got)
	}
}

// [성공시 반환상태값을 에러코드인 404로 설정] post 방식으로 권한없는 사용자가 파일 업로드시 에러체크가 올바른지 확인
func TestPostObjectSetInvalidSuccessCode(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := postPublicBucket(t, s)
	fields := map[string]string{"key": "foo.txt", "acl": "public-read", "Content-Type": "text/plain", "success_action_status": "404"}
	result := sendPostForm(t, postBucketURL(s, bucket), fields, "foo.txt", "text/plain", []byte("bar"))
	if result.status != 204 {
		t.Fatalf("POST status=%d want=204 body=%s", result.status, result.body)
	}
	if got := read(t, s, bucket, "foo.txt"); got != "bar" {
		t.Fatalf("body=%q", got)
	}
}

// content-type 헤더 정보 없이 post 방식으로 로그인정보를 포함한 파일 업로드시 올바르게 업로드 되는지 확인
func TestPostObjectAuthenticatedNoContentType(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	if s.cfg.Endpoint() == "" {
		t.Skip("source scenario uses SigV2 or compatibility-specific POST behavior")
	}
	bucket := s.bucket(t, 3)
	key, contentType, fileBody := "foo.txt", "text/plain", []byte("bar")
	keyPrefix := "foo"
	document := map[string]any{
		"expiration": time.Now().UTC().Add(100 * time.Minute).Format(time.RFC3339),
		"conditions": []any{map[string]string{"bucket": bucket}, []string{"starts-with", "$key", keyPrefix}, map[string]string{"acl": "private"}, []any{"content-length-range", 0, 1024}},
	}
	fields := map[string]string{"key": key, "acl": "private"}
	policy := encodePostPolicy(document)
	fields["AWSAccessKeyId"] = s.cfg.Main.AccessKey
	fields["signature"] = postV2Signature(policy, s.cfg.Main.SecretKey)
	fields["policy"] = policy
	result := sendPostForm(t, postBucketURL(s, bucket), fields, key, contentType, fileBody)
	if result.status != 204 {
		t.Fatalf("POST status=%d want=204 body=%s", result.status, result.body)
	}
	if got := read(t, s, bucket, key); !bytes.Equal([]byte(got), fileBody) {
		t.Fatalf("stored body size=%d want=%d", len(got), len(fileBody))
	}
}

// [PostKey 값이 틀린 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
func TestPostObjectAuthenticatedRequestBadAccessKey(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	if s.cfg.Endpoint() == "" {
		t.Skip("source scenario uses SigV2 or compatibility-specific POST behavior")
	}
	bucket := s.bucket(t, 4)
	key, contentType, fileBody := `\$foo.txt`, "text/plain", []byte("bar")
	keyPrefix := "foo"
	conditions := postV2Conditions(bucket, contentType, keyPrefix, 0, 1024)
	document := map[string]any{"expiration": time.Now().UTC().Add(100 * time.Minute).Format(time.RFC3339), "conditions": conditions}
	fields := map[string]string{"key": key, "acl": "private", "Content-Type": contentType}
	policy := encodePostPolicy(document)
	fields["AWSAccessKeyId"] = "foo"
	fields["signature"] = postV2Signature(policy, s.cfg.Main.SecretKey)
	fields["policy"] = policy
	result := sendPostForm(t, postBucketURL(s, bucket), fields, key, contentType, fileBody)
	if result.status != 403 {
		t.Fatalf("POST status=%d want=403 body=%s", result.status, result.body)
	}
}

// post 방식으로 로그인정보를 포함한 대용량 파일 업로드시 올바르게 업로드 되는지 확인
func TestPostObjectUploadLargerThanChunk(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	if s.cfg.Endpoint() == "" {
		t.Skip("source scenario uses SigV2 or compatibility-specific POST behavior")
	}
	bucket := s.bucket(t, 7)
	key, contentType := "foo.txt", "text/plain"
	keyPrefix := "foo"
	fileBody := randomTextToLong(5 * 1024 * 1024)
	conditions := postV2Conditions(bucket, contentType, keyPrefix, 0, len(fileBody))
	document := map[string]any{"expiration": time.Now().UTC().Add(100 * time.Minute).Format(time.RFC3339), "conditions": conditions}
	fields := map[string]string{"key": key, "acl": "private", "Content-Type": contentType}
	policy := encodePostPolicy(document)
	fields["AWSAccessKeyId"] = s.cfg.Main.AccessKey
	fields["signature"] = postV2Signature(policy, s.cfg.Main.SecretKey)
	fields["policy"] = policy
	result := sendPostForm(t, postBucketURL(s, bucket), fields, key, contentType, fileBody)
	if result.status != 204 {
		t.Fatalf("POST status=%d want=204 body=%s", result.status, result.body)
	}
	if got := read(t, s, bucket, key); !bytes.Equal([]byte(got), fileBody) {
		t.Fatalf("stored body size=%d want=%d", len(got), len(fileBody))
	}
}

// [오브젝트 이름을 로그인정보에 포함되어 있는 key값으로 대체할 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 올바르게 업로드 되는지 확인
func TestPostObjectSetKeyFromFilename(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	if s.cfg.Endpoint() == "" {
		t.Skip("source scenario uses SigV2 or compatibility-specific POST behavior")
	}
	bucket := s.bucket(t, 8)
	key, contentType, fileBody := "foo.txt", "text/plain", []byte("bar")
	keyPrefix := "foo"
	conditions := postV2Conditions(bucket, contentType, keyPrefix, 0, 1024)
	document := map[string]any{"expiration": time.Now().UTC().Add(100 * time.Minute).Format(time.RFC3339), "conditions": conditions}
	fields := map[string]string{"key": key, "acl": "private", "Content-Type": contentType}
	policy := encodePostPolicy(document)
	fields["AWSAccessKeyId"] = s.cfg.Main.AccessKey
	fields["signature"] = postV2Signature(policy, s.cfg.Main.SecretKey)
	fields["policy"] = policy
	result := sendPostForm(t, postBucketURL(s, bucket), fields, key, contentType, fileBody)
	if result.status != 204 {
		t.Fatalf("POST status=%d want=204 body=%s", result.status, result.body)
	}
	if got := read(t, s, bucket, key); !bytes.Equal([]byte(got), fileBody) {
		t.Fatalf("stored body size=%d want=%d", len(got), len(fileBody))
	}
}

// post 방식으로 로그인, 헤더 정보를 포함한 파일 업로드시 올바르게 업로드 되는지 확인
func TestPostObjectIgnoredHeader(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	if s.cfg.Endpoint() == "" {
		t.Skip("source scenario uses SigV2 or compatibility-specific POST behavior")
	}
	bucket := s.bucket(t, 9)
	key, contentType, fileBody := "foo.txt", "text/plain", []byte("bar")
	keyPrefix := "foo"
	conditions := postV2Conditions(bucket, contentType, keyPrefix, 0, 1024)
	document := map[string]any{"expiration": time.Now().UTC().Add(100 * time.Minute).Format(time.RFC3339), "conditions": conditions}
	fields := map[string]string{"key": key, "acl": "private", "Content-Type": contentType, "x-ignore-foo": "bar"}
	policy := encodePostPolicy(document)
	fields["AWSAccessKeyId"] = s.cfg.Main.AccessKey
	fields["signature"] = postV2Signature(policy, s.cfg.Main.SecretKey)
	fields["policy"] = policy
	result := sendPostForm(t, postBucketURL(s, bucket), fields, key, contentType, fileBody)
	if result.status != 204 {
		t.Fatalf("POST status=%d want=204 body=%s", result.status, result.body)
	}
	if got := read(t, s, bucket, key); !bytes.Equal([]byte(got), fileBody) {
		t.Fatalf("stored body size=%d want=%d", len(got), len(fileBody))
	}
}

// [헤더정보에 대소문자를 섞어서 사용할 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 올바르게 업로드 되는지 확인
func TestPostObjectCaseInsensitiveConditionFields(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	if s.cfg.Endpoint() == "" {
		t.Skip("source scenario uses SigV2 or compatibility-specific POST behavior")
	}
	bucket := s.bucket(t, 10)
	key, contentType, fileBody := "foo.txt", "text/plain", []byte("bar")
	document := map[string]any{
		"expiration": time.Now().UTC().Add(100 * time.Minute).Format(time.RFC3339),
		"conditions": []any{map[string]string{"bUcKeT": bucket}, []string{"StArTs-WiTh", "$KeY", "foo"}, map[string]string{"AcL": "private"}, []string{"StArTs-WiTh", "$CoNtEnT-TyPe", contentType}, []any{"content-length-range", 0, 1024}},
	}
	fields := map[string]string{"kEy": key, "aCl": "private", "Content-Type": contentType}
	policy := encodePostPolicy(document)
	fields["AWSAccessKeyId"] = s.cfg.Main.AccessKey
	fields["signature"] = postV2Signature(policy, s.cfg.Main.SecretKey)
	fields["pOLICy"] = policy
	result := sendPostForm(t, postBucketURL(s, bucket), fields, key, contentType, fileBody)
	if result.status != 204 {
		t.Fatalf("POST status=%d want=204 body=%s", result.status, result.body)
	}
	if got := read(t, s, bucket, "foo.txt"); !bytes.Equal([]byte(got), fileBody) {
		t.Fatalf("stored body size=%d want=%d", len(got), len(fileBody))
	}
}

// [오브젝트 이름에 '\'를 사용할 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 올바르게 업로드 되는지 확인
func TestPostObjectEscapedFieldValues(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	if s.cfg.Endpoint() == "" {
		t.Skip("source scenario uses SigV2 or compatibility-specific POST behavior")
	}
	bucket := s.bucket(t, 11)
	key, contentType, fileBody := `\$foo.txt`, "text/plain", []byte("bar")
	keyPrefix := `\$foo`
	conditions := postV2Conditions(bucket, contentType, keyPrefix, 0, 1024)
	document := map[string]any{"expiration": time.Now().UTC().Add(100 * time.Minute).Format(time.RFC3339), "conditions": conditions}
	fields := map[string]string{"key": key, "acl": "private", "Content-Type": contentType}
	policy := encodePostPolicy(document)
	fields["AWSAccessKeyId"] = s.cfg.Main.AccessKey
	fields["signature"] = postV2Signature(policy, s.cfg.Main.SecretKey)
	fields["policy"] = policy
	result := sendPostForm(t, postBucketURL(s, bucket), fields, key, contentType, fileBody)
	if result.status != 204 {
		t.Fatalf("POST status=%d want=204 body=%s", result.status, result.body)
	}
	if got := read(t, s, bucket, key); !bytes.Equal([]byte(got), fileBody) {
		t.Fatalf("stored body size=%d want=%d", len(got), len(fileBody))
	}
}

// [redirect url설정하여 체크] post 방식으로 로그인정보를 포함한 파일 업로드시 올바르게 업로드 되는지 확인
func TestPostObjectSuccessRedirectAction(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	if s.cfg.Endpoint() == "" {
		t.Skip("source scenario uses SigV2 or compatibility-specific POST behavior")
	}
	bucket := postPublicBucket(t, s)
	key, contentType, fileBody := "foo.txt", "text/plain", []byte("bar")
	keyPrefix := "foo"
	redirect := postBucketURL(s, bucket)
	conditions := append(postV2Conditions(bucket, contentType, keyPrefix, 0, 1024), []string{"eq", "$successActionRedirect", redirect})
	document := map[string]any{"expiration": time.Now().UTC().Add(100 * time.Minute).Format(time.RFC3339), "conditions": conditions}
	fields := map[string]string{"key": key, "acl": "private", "Content-Type": contentType, "successActionRedirect": redirect}
	policy := encodePostPolicy(document)
	fields["AWSAccessKeyId"] = s.cfg.Main.AccessKey
	fields["signature"] = postV2Signature(policy, s.cfg.Main.SecretKey)
	fields["policy"] = policy
	result := sendPostForm(t, postBucketURL(s, bucket), fields, key, contentType, fileBody)
	if result.status != 200 {
		t.Fatalf("POST status=%d want=200 body=%s", result.status, result.body)
	}
	if !strings.Contains(result.url, "bucket="+bucket) || !strings.Contains(result.url, "key="+key) || !strings.Contains(result.url, "etag=") {
		t.Fatalf("redirect URL=%q", result.url)
	}
}

// [SecretKey Hash 값이 틀린경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
func TestPostObjectInvalidSignature(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	if s.cfg.Endpoint() == "" {
		t.Skip("source scenario uses SigV2 or compatibility-specific POST behavior")
	}
	bucket := s.bucket(t, 13)
	key, contentType, fileBody := `\$foo.txt`, "text/plain", []byte("bar")
	keyPrefix := `\$foo`
	conditions := postV2Conditions(bucket, contentType, keyPrefix, 0, 1024)
	document := map[string]any{"expiration": time.Now().UTC().Add(100 * time.Minute).Format(time.RFC3339), "conditions": conditions}
	fields := map[string]string{"key": key, "acl": "private", "Content-Type": contentType}
	policy := encodePostPolicy(document)
	fields["AWSAccessKeyId"] = s.cfg.Main.AccessKey
	fields["signature"] = strings.TrimSuffix(postV2Signature(policy, s.cfg.Main.SecretKey), "=")
	fields["policy"] = policy
	result := sendPostForm(t, postBucketURL(s, bucket), fields, key, contentType, fileBody)
	if result.status != 403 {
		t.Fatalf("POST status=%d want=403 body=%s", result.status, result.body)
	}
}

// [PostKey 값이 틀린경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
func TestPostObjectInvalidAccessKey(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	if s.cfg.Endpoint() == "" {
		t.Skip("source scenario uses SigV2 or compatibility-specific POST behavior")
	}
	bucket := s.bucket(t, 14)
	key, contentType, fileBody := `\$foo.txt`, "text/plain", []byte("bar")
	keyPrefix := `\$foo`
	conditions := postV2Conditions(bucket, contentType, keyPrefix, 0, 1024)
	document := map[string]any{"expiration": time.Now().UTC().Add(100 * time.Minute).Format(time.RFC3339), "conditions": conditions}
	fields := map[string]string{"key": key, "acl": "private", "Content-Type": contentType}
	policy := encodePostPolicy(document)
	fields["AWSAccessKeyId"] = strings.TrimSuffix(s.cfg.Main.AccessKey, s.cfg.Main.AccessKey[len(s.cfg.Main.AccessKey)-1:])
	fields["signature"] = postV2Signature(policy, s.cfg.Main.SecretKey)
	fields["policy"] = policy
	result := sendPostForm(t, postBucketURL(s, bucket), fields, key, contentType, fileBody)
	if result.status != 403 {
		t.Fatalf("POST status=%d want=403 body=%s", result.status, result.body)
	}
}

// [로그인 정보의 날짜포맷이 다를경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
func TestPostObjectInvalidDateFormat(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	if s.cfg.Endpoint() == "" {
		t.Skip("source scenario uses SigV2 or compatibility-specific POST behavior")
	}
	bucket := s.bucket(t, 15)
	key, contentType, fileBody := `\$foo.txt`, "text/plain", []byte("bar")
	keyPrefix := `\$foo`
	conditions := postV2Conditions(bucket, contentType, keyPrefix, 0, 1024)
	expiration := strings.ReplaceAll(time.Now().UTC().Add(100*time.Minute).Format(time.RFC3339), "T", " ")
	document := map[string]any{"expiration": expiration, "conditions": conditions}
	fields := map[string]string{"key": key, "acl": "private", "Content-Type": contentType}
	policy := encodePostPolicy(document)
	fields["AWSAccessKeyId"] = s.cfg.Main.AccessKey
	fields["signature"] = postV2Signature(policy, s.cfg.Main.SecretKey)
	fields["policy"] = policy
	result := sendPostForm(t, postBucketURL(s, bucket), fields, key, contentType, fileBody)
	if result.status != 400 {
		t.Fatalf("POST status=%d want=400 body=%s", result.status, result.body)
	}
}

// [오브젝트 이름을 입력하지 않을 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
func TestPostObjectNoKeySpecified(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	if s.cfg.Endpoint() == "" {
		t.Skip("source scenario uses SigV2 or compatibility-specific POST behavior")
	}
	bucket := s.bucket(t, 16)
	contentType, fileBody := "text/plain", []byte("bar")
	document := map[string]any{
		"expiration": time.Now().UTC().Add(100 * time.Minute).Format(time.RFC3339),
		"conditions": []any{map[string]string{"bucket": bucket}, map[string]string{"acl": "private"}, []string{"starts-with", "$Content-Type", contentType}, []any{"content-length-range", 0, 1024}},
	}
	fields := map[string]string{"acl": "private", "Content-Type": contentType}
	policy := encodePostPolicy(document)
	fields["AWSAccessKeyId"] = s.cfg.Main.AccessKey
	fields["signature"] = postV2Signature(policy, s.cfg.Main.SecretKey)
	fields["policy"] = policy
	result := sendPostForm(t, postBucketURL(s, bucket), fields, `\$foo.txt`, contentType, fileBody)
	if result.status != 400 {
		t.Fatalf("POST status=%d want=400 body=%s", result.status, result.body)
	}
}

// [signature 정보를 누락하고 업로드할 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
func TestPostObjectMissingSignature(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	if s.cfg.Endpoint() == "" {
		t.Skip("source scenario uses SigV2 or compatibility-specific POST behavior")
	}
	bucket := s.bucket(t, 17)
	key, contentType, fileBody := `\$foo.txt`, "text/plain", []byte("bar")
	keyPrefix := `\$foo`
	conditions := postV2Conditions(bucket, contentType, keyPrefix, 0, 1024)
	document := map[string]any{"expiration": time.Now().UTC().Add(100 * time.Minute).Format(time.RFC3339), "conditions": conditions}
	fields := map[string]string{"key": key, "acl": "private", "Content-Type": contentType}
	policy := encodePostPolicy(document)
	fields["AWSAccessKeyId"] = s.cfg.Main.AccessKey
	fields["policy"] = policy
	result := sendPostForm(t, postBucketURL(s, bucket), fields, key, contentType, fileBody)
	if result.status != 400 {
		t.Fatalf("POST status=%d want=400 body=%s", result.status, result.body)
	}
}

// [policy에 버킷 이름을 누락하고 업로드할 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
func TestPostObjectMissingPolicyCondition(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	if s.cfg.Endpoint() == "" {
		t.Skip("source scenario uses SigV2 or compatibility-specific POST behavior")
	}
	bucket := s.bucket(t, 18)
	key, contentType, fileBody := `\$foo.txt`, "text/plain", []byte("bar")
	keyPrefix := `\$foo`
	conditions := postV2Conditions(bucket, contentType, keyPrefix, 0, 1024)
	document := map[string]any{"expiration": time.Now().UTC().Add(100 * time.Minute).Format(time.RFC3339), "conditions": conditions[1:]}
	fields := map[string]string{"key": key, "acl": "private", "Content-Type": contentType}
	policy := encodePostPolicy(document)
	fields["AWSAccessKeyId"] = s.cfg.Main.AccessKey
	fields["signature"] = postV2Signature(policy, s.cfg.Main.SecretKey)
	fields["policy"] = policy
	result := sendPostForm(t, postBucketURL(s, bucket), fields, key, contentType, fileBody)
	if result.status != 403 {
		t.Fatalf("POST status=%d want=403 body=%s", result.status, result.body)
	}
}

// [사용자가 추가 메타데이터를 입력한 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 올바르게 업로드 되는지 확인
func TestPostObjectUserSpecifiedHeader(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	if s.cfg.Endpoint() == "" {
		t.Skip("source scenario uses SigV2 or compatibility-specific POST behavior")
	}
	bucket := s.bucket(t, 19)
	key, contentType, fileBody := "foo.txt", "text/plain", []byte("bar")
	keyPrefix := "foo"
	conditions := append(postV2Conditions(bucket, contentType, keyPrefix, 0, 1024), []string{"starts-with", "$x-amz-meta-foo", "bar"})
	document := map[string]any{"expiration": time.Now().UTC().Add(100 * time.Minute).Format(time.RFC3339), "conditions": conditions}
	fields := map[string]string{"key": key, "acl": "private", "Content-Type": contentType, "x-amz-meta-foo": "bar-clamp"}
	policy := encodePostPolicy(document)
	fields["AWSAccessKeyId"] = s.cfg.Main.AccessKey
	fields["signature"] = postV2Signature(policy, s.cfg.Main.SecretKey)
	fields["policy"] = policy
	result := sendPostForm(t, postBucketURL(s, bucket), fields, key, contentType, fileBody)
	if result.status != 204 {
		t.Fatalf("POST status=%d want=204 body=%s", result.status, result.body)
	}
	if got := read(t, s, bucket, key); !bytes.Equal([]byte(got), fileBody) {
		t.Fatalf("stored body size=%d want=%d", len(got), len(fileBody))
	}
	head, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil || head.Metadata["foo"] != "bar-clamp" {
		t.Fatalf("metadata=%v err=%v", head.Metadata, err)
	}
}

// [사용자가 추가 메타데이터를 policy에 설정하였으나 오브젝트에 해당 정보가 누락된 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
func TestPostObjectRequestMissingPolicySpecifiedField(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	if s.cfg.Endpoint() == "" {
		t.Skip("source scenario uses SigV2 or compatibility-specific POST behavior")
	}
	bucket := s.bucket(t, 20)
	key, contentType, fileBody := `\$foo.txt`, "text/plain", []byte("bar")
	keyPrefix := `\$foo`
	conditions := postV2Conditions(bucket, contentType, keyPrefix, 0, 1024)
	document := map[string]any{"expiration": time.Now().UTC().Add(100 * time.Minute).Format(time.RFC3339), "conditions": append(conditions, []string{"starts-with", "$x-amz-meta-foo", "bar"})}
	fields := map[string]string{"key": key, "acl": "private", "Content-Type": contentType}
	policy := encodePostPolicy(document)
	fields["AWSAccessKeyId"] = s.cfg.Main.AccessKey
	fields["signature"] = postV2Signature(policy, s.cfg.Main.SecretKey)
	fields["policy"] = policy
	result := sendPostForm(t, postBucketURL(s, bucket), fields, key, contentType, fileBody)
	if result.status != 403 {
		t.Fatalf("POST status=%d want=403 body=%s", result.status, result.body)
	}
}

// [policy의 condition을 대문자(CONDITIONS)로 입력할 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
func TestPostObjectConditionIsCaseSensitive(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	if s.cfg.Endpoint() == "" {
		t.Skip("source scenario uses SigV2 or compatibility-specific POST behavior")
	}
	bucket := s.bucket(t, 21)
	key, contentType, fileBody := `\$foo.txt`, "text/plain", []byte("bar")
	keyPrefix := `\$foo`
	conditions := postV2Conditions(bucket, contentType, keyPrefix, 0, 1024)
	document := map[string]any{"expiration": time.Now().UTC().Add(100 * time.Minute).Format(time.RFC3339), "CONDITIONS": conditions}
	fields := map[string]string{"key": key, "acl": "private", "Content-Type": contentType}
	policy := encodePostPolicy(document)
	fields["AWSAccessKeyId"] = s.cfg.Main.AccessKey
	fields["signature"] = postV2Signature(policy, s.cfg.Main.SecretKey)
	fields["policy"] = policy
	result := sendPostForm(t, postBucketURL(s, bucket), fields, key, contentType, fileBody)
	if result.status != 400 {
		t.Fatalf("POST status=%d want=400 body=%s", result.status, result.body)
	}
}

// [policy의 expiration을 대문자(EXPIRATION)로 입력할 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
func TestPostObjectExpiresIsCaseSensitive(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	if s.cfg.Endpoint() == "" {
		t.Skip("source scenario uses SigV2 or compatibility-specific POST behavior")
	}
	bucket := s.bucket(t, 22)
	key, contentType, fileBody := `\$foo.txt`, "text/plain", []byte("bar")
	keyPrefix := `\$foo`
	conditions := postV2Conditions(bucket, contentType, keyPrefix, 0, 1024)
	document := map[string]any{"EXPIRATION": time.Now().UTC().Add(100 * time.Minute).Format(time.RFC3339), "conditions": conditions}
	fields := map[string]string{"key": key, "acl": "private", "Content-Type": contentType}
	policy := encodePostPolicy(document)
	fields["AWSAccessKeyId"] = s.cfg.Main.AccessKey
	fields["signature"] = postV2Signature(policy, s.cfg.Main.SecretKey)
	fields["policy"] = policy
	result := sendPostForm(t, postBucketURL(s, bucket), fields, key, contentType, fileBody)
	if result.status != 400 {
		t.Fatalf("POST status=%d want=400 body=%s", result.status, result.body)
	}
}

// [policy의 expiration을 만료된 값으로 입력할 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
func TestPostObjectExpiredPolicy(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	if s.cfg.Endpoint() == "" {
		t.Skip("source scenario uses SigV2 or compatibility-specific POST behavior")
	}
	bucket := s.bucket(t, 23)
	key, contentType, fileBody := `\$foo.txt`, "text/plain", []byte("bar")
	keyPrefix := `\$foo`
	conditions := postV2Conditions(bucket, contentType, keyPrefix, 0, 1024)
	document := map[string]any{"expiration": time.Now().UTC().Add(-100 * time.Minute).Format(time.RFC3339), "conditions": conditions}
	fields := map[string]string{"key": key, "acl": "private", "Content-Type": contentType}
	policy := encodePostPolicy(document)
	fields["AWSAccessKeyId"] = s.cfg.Main.AccessKey
	fields["signature"] = postV2Signature(policy, s.cfg.Main.SecretKey)
	fields["policy"] = policy
	result := sendPostForm(t, postBucketURL(s, bucket), fields, key, contentType, fileBody)
	if result.status != 403 {
		t.Fatalf("POST status=%d want=403 body=%s", result.status, result.body)
	}
}

// [사용자가 추가 메타데이터를 policy에 설정하였으나 설정정보가 올바르지 않을 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
func TestPostObjectInvalidRequestFieldValue(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	if s.cfg.Endpoint() == "" {
		t.Skip("source scenario uses SigV2 or compatibility-specific POST behavior")
	}
	bucket := s.bucket(t, 24)
	key, contentType, fileBody := `\$foo.txt`, "text/plain", []byte("bar")
	keyPrefix := `\$foo`
	conditions := postV2Conditions(bucket, contentType, keyPrefix, 0, 1024)
	document := map[string]any{"expiration": time.Now().UTC().Add(100 * time.Minute).Format(time.RFC3339), "conditions": append(conditions, []string{"eq", "$x-amz-meta-foo", ""})}
	fields := map[string]string{"key": key, "acl": "private", "Content-Type": contentType, "x-amz-meta-foo": "bar-clamp"}
	policy := encodePostPolicy(document)
	fields["AWSAccessKeyId"] = s.cfg.Main.AccessKey
	fields["signature"] = postV2Signature(policy, s.cfg.Main.SecretKey)
	fields["policy"] = policy
	result := sendPostForm(t, postBucketURL(s, bucket), fields, key, contentType, fileBody)
	if result.status != 403 {
		t.Fatalf("POST status=%d want=403 body=%s", result.status, result.body)
	}
}

// [policy의 expiration값을 누락했을 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
func TestPostObjectMissingExpiresCondition(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	if s.cfg.Endpoint() == "" {
		t.Skip("source scenario uses SigV2 or compatibility-specific POST behavior")
	}
	bucket := s.bucket(t, 25)
	key, contentType, fileBody := `\$foo.txt`, "text/plain", []byte("bar")
	keyPrefix := `\$foo`
	conditions := postV2Conditions(bucket, contentType, keyPrefix, 0, 1024)
	document := map[string]any{"conditions": conditions}
	fields := map[string]string{"key": key, "acl": "private", "Content-Type": contentType}
	policy := encodePostPolicy(document)
	fields["AWSAccessKeyId"] = s.cfg.Main.AccessKey
	fields["signature"] = postV2Signature(policy, s.cfg.Main.SecretKey)
	fields["policy"] = policy
	result := sendPostForm(t, postBucketURL(s, bucket), fields, key, contentType, fileBody)
	if result.status != 400 {
		t.Fatalf("POST status=%d want=400 body=%s", result.status, result.body)
	}
}

// [policy의 conditions값을 누락했을 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
func TestPostObjectMissingConditionsList(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	if s.cfg.Endpoint() == "" {
		t.Skip("source scenario uses SigV2 or compatibility-specific POST behavior")
	}
	bucket := s.bucket(t, 26)
	key, contentType, fileBody := `\$foo.txt`, "text/plain", []byte("bar")
	document := map[string]any{"expiration": time.Now().UTC().Add(100 * time.Minute).Format(time.RFC3339)}
	fields := map[string]string{"key": key, "acl": "private", "Content-Type": contentType}
	policy := encodePostPolicy(document)
	fields["AWSAccessKeyId"] = s.cfg.Main.AccessKey
	fields["signature"] = postV2Signature(policy, s.cfg.Main.SecretKey)
	fields["policy"] = policy
	result := sendPostForm(t, postBucketURL(s, bucket), fields, key, contentType, fileBody)
	if result.status != 400 {
		t.Fatalf("POST status=%d want=400 body=%s", result.status, result.body)
	}
}

// [policy에 설정한 용량보다 큰 오브젝트를 업로드 할 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
func TestPostObjectUploadSizeLimitExceeded(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	if s.cfg.Endpoint() == "" {
		t.Skip("source scenario uses SigV2 or compatibility-specific POST behavior")
	}
	bucket := s.bucket(t, 27)
	key, contentType, fileBody := `\$foo.txt`, "text/plain", []byte("bar")
	keyPrefix := `\$foo`
	document := map[string]any{"expiration": time.Now().UTC().Add(100 * time.Minute).Format(time.RFC3339), "conditions": postV2Conditions(bucket, contentType, keyPrefix, 0, 0)}
	fields := map[string]string{"key": key, "acl": "private", "Content-Type": contentType}
	policy := encodePostPolicy(document)
	fields["AWSAccessKeyId"] = s.cfg.Main.AccessKey
	fields["signature"] = postV2Signature(policy, s.cfg.Main.SecretKey)
	fields["policy"] = policy
	result := sendPostForm(t, postBucketURL(s, bucket), fields, key, contentType, fileBody)
	if result.status != 400 {
		t.Fatalf("POST status=%d want=400 body=%s", result.status, result.body)
	}
}

// [policy에 용량정보 설정을 누락할 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
func TestPostObjectMissingContentLengthArgument(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	if s.cfg.Endpoint() == "" {
		t.Skip("source scenario uses SigV2 or compatibility-specific POST behavior")
	}
	bucket := s.bucket(t, 28)
	key, contentType, fileBody := `\$foo.txt`, "text/plain", []byte("bar")
	keyPrefix := `\$foo`
	conditions := postV2Conditions(bucket, contentType, keyPrefix, 0, 1024)
	document := map[string]any{"expiration": time.Now().UTC().Add(100 * time.Minute).Format(time.RFC3339), "conditions": append(conditions[:len(conditions)-1], []any{"content-length-range", 0})}
	fields := map[string]string{"key": key, "acl": "private", "Content-Type": contentType}
	policy := encodePostPolicy(document)
	fields["AWSAccessKeyId"] = s.cfg.Main.AccessKey
	fields["signature"] = postV2Signature(policy, s.cfg.Main.SecretKey)
	fields["policy"] = policy
	result := sendPostForm(t, postBucketURL(s, bucket), fields, key, contentType, fileBody)
	if result.status != 400 {
		t.Fatalf("POST status=%d want=400 body=%s", result.status, result.body)
	}
}

// [policy에 용량정보 설정값이 틀렸을 경우(용량값을 음수로 입력) post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
func TestPostObjectInvalidContentLengthArgument(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	if s.cfg.Endpoint() == "" {
		t.Skip("source scenario uses SigV2 or compatibility-specific POST behavior")
	}
	bucket := s.bucket(t, 29)
	key, contentType, fileBody := `\$foo.txt`, "text/plain", []byte("bar")
	keyPrefix := `\$foo`
	document := map[string]any{"expiration": time.Now().UTC().Add(100 * time.Minute).Format(time.RFC3339), "conditions": postV2Conditions(bucket, contentType, keyPrefix, -1, 0)}
	fields := map[string]string{"key": key, "acl": "private", "Content-Type": contentType}
	policy := encodePostPolicy(document)
	fields["AWSAccessKeyId"] = s.cfg.Main.AccessKey
	fields["signature"] = postV2Signature(policy, s.cfg.Main.SecretKey)
	fields["policy"] = policy
	result := sendPostForm(t, postBucketURL(s, bucket), fields, key, contentType, fileBody)
	if result.status != 400 {
		t.Fatalf("POST status=%d want=400 body=%s", result.status, result.body)
	}
}

// [policy에 설정한 용량보다 작은 오브젝트를 업로드 할 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
func TestPostObjectUploadSizeBelowMinimum(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	if s.cfg.Endpoint() == "" {
		t.Skip("source scenario uses SigV2 or compatibility-specific POST behavior")
	}
	bucket := s.bucket(t, 30)
	key, contentType, fileBody := `\$foo.txt`, "text/plain", []byte("bar")
	keyPrefix := `\$foo`
	document := map[string]any{"expiration": time.Now().UTC().Add(100 * time.Minute).Format(time.RFC3339), "conditions": postV2Conditions(bucket, contentType, keyPrefix, 512, 1024)}
	fields := map[string]string{"key": key, "acl": "private", "Content-Type": contentType}
	policy := encodePostPolicy(document)
	fields["AWSAccessKeyId"] = s.cfg.Main.AccessKey
	fields["signature"] = postV2Signature(policy, s.cfg.Main.SecretKey)
	fields["policy"] = policy
	result := sendPostForm(t, postBucketURL(s, bucket), fields, key, contentType, fileBody)
	if result.status != 400 {
		t.Fatalf("POST status=%d want=400 body=%s", result.status, result.body)
	}
}

// [policy의 conditions값이 비어있을 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
func TestPostObjectEmptyConditions(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	if s.cfg.Endpoint() == "" {
		t.Skip("source scenario uses SigV2 or compatibility-specific POST behavior")
	}
	bucket := s.bucket(t, 31)
	key, contentType, fileBody := "foo.txt", "text/plain", []byte("bar")
	document := map[string]any{"expiration": time.Now().UTC().Add(100 * time.Minute).Format(time.RFC3339), "conditions": []any{}}
	fields := map[string]string{"key": key, "acl": "private", "Content-Type": contentType}
	policy := encodePostPolicy(document)
	fields["AWSAccessKeyId"] = s.cfg.Main.AccessKey
	fields["signature"] = postV2Signature(policy, s.cfg.Main.SecretKey)
	fields["policy"] = policy
	result := sendPostForm(t, postBucketURL(s, bucket), fields, key, contentType, fileBody)
	if result.status != 400 {
		t.Fatalf("POST status=%d want=400 body=%s", result.status, result.body)
	}
}

// PresignedURL로 오브젝트 업로드, 다운로드 성공 확인
func TestPresignedUrlPutGet(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 32)
	presign := s3.NewPresignClient(s.client)
	body := []byte("foo")
	putURL, err := presign.PresignPutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String("foo")})
	if err != nil {
		t.Fatal(err)
	}
	request, _ := http.NewRequest(http.MethodPut, putURL.URL, bytes.NewReader(body))
	copySignedHeaders(request, putURL.SignedHeader)
	response, err := insecureHTTPClient().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != 200 {
		t.Fatalf("PUT status=%d", response.StatusCode)
	}
	getURL, err := presign.PresignGetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String("foo")})
	if err != nil {
		t.Fatal(err)
	}
	response, err = insecureHTTPClient().Get(getURL.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	got, readErr := io.ReadAll(response.Body)
	if response.StatusCode != 200 || readErr != nil || !bytes.Equal(got, body) {
		t.Fatalf("GET status=%d body=%q err=%v", response.StatusCode, got, readErr)
	}
}

// SignatureVersion4로 오브젝트 업로드 성공 확인
func TestPutObjectV4(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 33)
	body := randomTextToLong(100)
	putBytes(t, s.client, bucket, "foo", body)
	assertObjectBytes(t, s.client, bucket, "foo", body)
}

// [SignatureVersion4] SDK 기본 전송 방식으로 오브젝트 업로드 성공 확인
func TestPutObjectChunkedV4(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 34)
	body := randomTextToLong(100)
	putBytes(t, s.client, bucket, "chunked", body)
	assertObjectBytes(t, s.client, bucket, "chunked", body)
}

// [SignatureVersion4] 오브젝트 다운로드 성공 확인
func TestGetObjectV4(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 35)
	body := randomTextToLong(100)
	putBytes(t, s.client, bucket, "foo", body)
	out, err := s.client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String("foo")})
	if err != nil {
		t.Fatal(err)
	}
	defer out.Body.Close()
	got, err := io.ReadAll(out.Body)
	if err != nil || !bytes.Equal(got, body) {
		t.Fatalf("body mismatch err=%v", err)
	}
}

// [policy에 설정된 버킷과 다른 버킷으로 업로드할 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
func TestPostObjectWrongBucket(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := "missing-" + uniqueBucketSuffix(t)
	wrong := "wrong-" + uniqueBucketSuffix(t)
	fields, policy := postV4Fields(s, bucket, "text/plain", `\$foo`, 512, 1024, nil)
	fields["key"], fields["bucket"], fields["acl"], fields["Content-Type"], fields["policy"] = `\$foo.txt`, bucket, "private", "text/plain", policy
	result := sendPostForm(t, postBucketURL(s, wrong), fields, `\$foo.txt`, "text/plain", []byte("bar"))
	if result.status != 404 {
		t.Fatalf("POST status=%d want=404 body=%s", result.status, result.body)
	}
}

func postPublicBucket(t *testing.T, s *suite) string {
	t.Helper()
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter)
	setBucketCannedACL(t, s, bucket, types.BucketCannedACLPublicReadWrite)
	return bucket
}

func postV2Conditions(bucket, contentType, keyPrefix string, minSize, maxSize int) []any {
	return []any{map[string]string{"bucket": bucket}, []string{"starts-with", "$key", keyPrefix}, map[string]string{"acl": "private"}, []string{"starts-with", "$Content-Type", contentType}, []any{"content-length-range", minSize, maxSize}}
}

func encodePostPolicy(document map[string]any) string {
	data, _ := json.Marshal(document)
	return base64.StdEncoding.EncodeToString(data)
}

func postV2Signature(policy, secret string) string {
	mac := hmac.New(sha1.New, []byte(secret))
	_, _ = mac.Write([]byte(policy))
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

func postV4Fields(s *suite, bucket, contentType, keyPrefix string, minSize, maxSize int, extra []any) (map[string]string, string) {
	now := time.Now().UTC()
	amzDate, date := now.Format("20060102T150405Z"), now.Format("20060102")
	region := s.cfg.Region
	if region == "" {
		region = "us-east-1"
	}
	credential := s.cfg.Main.AccessKey + "/" + date + "/" + region + "/s3/aws4_request"
	conditions := append(postV2Conditions(bucket, contentType, keyPrefix, minSize, maxSize), extra...)
	conditions = append(conditions, map[string]string{"x-amz-algorithm": "AWS4-HMAC-SHA256"}, map[string]string{"x-amz-credential": credential}, map[string]string{"x-amz-date": amzDate})
	policy := encodePostPolicy(map[string]any{"expiration": now.Add(100 * time.Minute).Format(time.RFC3339), "conditions": conditions})
	dateKey := hmacSHA256([]byte("AWS4"+s.cfg.Main.SecretKey), date)
	regionKey := hmacSHA256(dateKey, region)
	serviceKey := hmacSHA256(regionKey, "s3")
	signingKey := hmacSHA256(serviceKey, "aws4_request")
	signature := hex.EncodeToString(hmacSHA256(signingKey, policy))
	return map[string]string{"x-amz-algorithm": "AWS4-HMAC-SHA256", "x-amz-credential": credential, "x-amz-date": amzDate, "x-amz-signature": signature}, policy
}

func hmacSHA256(key []byte, value string) []byte {
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte(value))
	return mac.Sum(nil)
}

func postBucketURL(s *suite, bucket string) string {
	if endpoint := s.cfg.Endpoint(); endpoint != "" {
		return strings.TrimRight(endpoint, "/") + "/" + bucket
	}
	region := s.cfg.Region
	if region == "" || region == "us-east-1" {
		return "https://" + bucket + ".s3.amazonaws.com/"
	}
	return "https://" + bucket + ".s3." + region + ".amazonaws.com/"
}

// postBucketURLSecure는 Java createURL(bucket, true)와 같이 SSE-C POST용 HTTPS URL을 만든다.
func postBucketURLSecure(s *suite, bucket string) string {
	if s.cfg.URL == "" {
		return postBucketURL(s, bucket)
	}
	host := strings.TrimRight(strings.TrimPrefix(strings.TrimPrefix(s.cfg.URL, "http://"), "https://"), "/")
	if slash := strings.IndexByte(host, '/'); slash >= 0 {
		host = host[:slash]
	}
	if s.cfg.SSLPort <= 0 || s.cfg.SSLPort == 443 {
		return "https://" + host + "/" + bucket
	}
	return fmt.Sprintf("https://%s:%d/%s", host, s.cfg.SSLPort, bucket)
}

func sendPostForm(t *testing.T, url string, fields map[string]string, filename, contentType string, file []byte) postResult {
	t.Helper()
	var payload bytes.Buffer
	writer := multipart.NewWriter(&payload)
	for name, value := range fields {
		if err := writer.WriteField(name, value); err != nil {
			t.Fatal(err)
		}
	}
	header := make(textproto.MIMEHeader)
	header.Set("Content-Disposition", `form-data; name="file"; filename="`+filename+`"`)
	header.Set("Content-Type", contentType)
	part, err := writer.CreatePart(header)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := part.Write(file); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(http.MethodPost, url, &payload)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", writer.FormDataContentType())
	response, err := insecureHTTPClient().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return postResult{status: response.StatusCode, body: body, url: response.Request.URL.String()}
}

func copySignedHeaders(request *http.Request, headers http.Header) {
	for name, values := range headers {
		if strings.EqualFold(name, "Host") {
			request.Host = values[0]
		} else {
			request.Header[name] = append([]string(nil), values...)
		}
	}
}
