package s3tests

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/md5"
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
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"ksantest/go-s3tests/internal/testconfig"

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

// PresignedURL에 서명된 x-amz-acl 헤더를 그대로 보내면 업로드 성공 확인
func TestPresignedUrlPutSignedAcl(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 37)
	key := "TestPresignedUrlPutSignedAcl"
	input := &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), ACL: types.ObjectCannedACLPublicRead}
	status, body := presignedPut(t, s, input, nil, []byte(key))
	if status != 200 {
		t.Fatalf("PUT status=%d body=%s", status, body)
	}
}

// PresignedURL에 서명되지 않은 x-amz-acl 헤더를 붙이면 AccessDenied로 거부 확인
func TestPresignedUrlPutUnsignedAcl(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 38)
	key := "TestPresignedUrlPutUnsignedAcl"
	input := &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}
	status, body := presignedPut(t, s, input, map[string]string{"x-amz-acl": "public-read"}, []byte(key))
	if status != 403 || !bytes.Contains(body, []byte("AccessDenied")) {
		t.Fatalf("PUT status=%d body=%s want=403 AccessDenied", status, body)
	}
	_, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	assertS3Status(t, err, 404)
}

// PresignedURL(PUT)에 서명되지 않은 x-amz-copy-source 헤더를 붙이면 AccessDenied로 거부 확인
func TestPresignedUrlPutUnsignedCopySource(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 39)
	source, target := "source", "target"
	putBytes(t, s.client, bucket, source, []byte(source))
	input := &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(target)}
	// 본문이 없으면 서명 검사 전에 MissingContentLength로 막는 구현이 있어 빈 본문을 보낸다.
	status, body := presignedPut(t, s, input, map[string]string{"x-amz-copy-source": bucket + "/" + source}, []byte{})
	if status != 403 || !bytes.Contains(body, []byte("AccessDenied")) {
		t.Fatalf("PUT status=%d body=%s want=403 AccessDenied", status, body)
	}
	_, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(target)})
	assertS3Status(t, err, 404)
}

// presigned PUT URL에 서명된 헤더와 extra 헤더(서명에 넣지 않음)를 붙여 보낸다.
func presignedPut(t *testing.T, s *suite, input *s3.PutObjectInput, extra map[string]string, body []byte) (int, []byte) {
	t.Helper()
	putURL, err := s3.NewPresignClient(s.client).PresignPutObject(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	request, _ := http.NewRequest(http.MethodPut, putURL.URL, bytes.NewReader(body))
	copySignedHeaders(request, putURL.SignedHeader)
	for name, value := range extra {
		request.Header.Set(name, value)
	}
	response, err := insecureHTTPClient().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	got, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return response.StatusCode, got
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
	ordered := make(postFields, 0, len(fields))
	for name, value := range fields {
		ordered = append(ordered, postField{name, value})
	}
	return sendPostFields(t, url, ordered, filename, contentType, file)
}

// sendPostFields는 폼 필드를 주어진 순서대로 보내고 file 을 마지막에 보낸다.
func sendPostFields(t *testing.T, url string, fields postFields, filename, contentType string, file []byte) postResult {
	t.Helper()
	var payload bytes.Buffer
	writer := multipart.NewWriter(&payload)
	for _, field := range fields {
		if err := writer.WriteField(field.name, field.value); err != nil {
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

// region SigV4 POST (AWS 실측 기준)

type postField struct{ name, value string }

// postFields는 순서를 지키는 폼 필드 목록이다.
type postFields []postField

func (f postFields) get(name string) string {
	for _, field := range f {
		if field.name == name {
			return field.value
		}
	}
	return ""
}

func (f *postFields) set(name, value string) {
	for i := range *f {
		if (*f)[i].name == name {
			(*f)[i].value = value
			return
		}
	}
	*f = append(*f, postField{name, value})
}

func (f *postFields) remove(name string) {
	kept := make(postFields, 0, len(*f))
	for _, field := range *f {
		if field.name != name {
			kept = append(kept, field)
		}
	}
	*f = kept
}

// 주어진 필드와 값이 같아야 한다는 조건 목록을 만든다. key 는 뺀다.
func (f postFields) conditions() []any {
	conditions := []any{}
	for _, field := range f {
		if field.name != "key" {
			conditions = append(conditions, map[string]string{field.name: field.value})
		}
	}
	return conditions
}

// postV4Options는 postV4Form 의 선택 인자다.
type postV4Options struct {
	conditions []any                           // 기본 조건 뒤에 붙일 조건
	body       *string                         // 파일 본문. nil 이면 "bar"
	user       *testconfig.User                // 서명할 사용자. nil 이면 Main User
	policy     func(base []any) map[string]any // 기본 조건 목록을 받아 정책 문서를 만든다
	rawPolicy  string                          // 비어 있지 않으면 policy 필드에 그대로 넣는다
	mutate     func(fields *postFields)        // 서명까지 끝난 폼 필드를 고친다
}

// postV4Form은 SigV4 로 서명한 폼 업로드를 보낸다. 날짜·자격 증명은 정책과 폼에 같은 값을 쓴다.
// 정책을 따로 주지 않으면 bucket, key(아무 값), x-amz-* 조건에 conditions 를 더한 정책을 만든다.
func postV4Form(t *testing.T, s *suite, bucket string, fields postFields, opt postV4Options) postResult {
	t.Helper()
	user := s.cfg.Main
	if opt.user != nil {
		user = *opt.user
	}
	now := time.Now().UTC()
	amzDate, date := now.Format("20060102T150405Z"), now.Format("20060102")
	region := s.cfg.Region
	if region == "" {
		region = "us-east-1"
	}
	credential := user.AccessKey + "/" + date + "/" + region + "/s3/aws4_request"
	base := []any{
		map[string]string{"bucket": bucket},
		[]any{"starts-with", "$key", ""},
		map[string]string{"x-amz-algorithm": "AWS4-HMAC-SHA256"},
		map[string]string{"x-amz-credential": credential},
		map[string]string{"x-amz-date": amzDate},
	}
	encoded := opt.rawPolicy
	if encoded == "" {
		var document map[string]any
		if opt.policy == nil {
			document = map[string]any{"expiration": postExpiration(100), "conditions": append(base, opt.conditions...)}
		} else {
			document = opt.policy(base)
		}
		encoded = encodePostPolicy(document)
	}
	signingKey := hmacSHA256(hmacSHA256(hmacSHA256(hmacSHA256([]byte("AWS4"+user.SecretKey), date), region), "s3"), "aws4_request")
	payload := append(postFields{}, fields...)
	payload.set("policy", encoded)
	payload.set("x-amz-algorithm", "AWS4-HMAC-SHA256")
	payload.set("x-amz-credential", credential)
	payload.set("x-amz-date", amzDate)
	payload.set("x-amz-signature", hex.EncodeToString(hmacSHA256(signingKey, encoded)))
	if opt.mutate != nil {
		opt.mutate(&payload)
	}
	body := "bar"
	if opt.body != nil {
		body = *opt.body
	}
	return sendPostFields(t, postBucketURL(s, bucket), payload, "foo.txt", "text/plain", []byte(body))
}

// 지금부터 minutes 분 뒤의 정책 만료 시각
func postExpiration(minutes int) string {
	return time.Now().UTC().Add(time.Duration(minutes) * time.Minute).Format("2006-01-02T15:04:05Z")
}

// 에러 응답 본문에서 Code 를 꺼낸다.
func postErrorCode(result postResult) string {
	body := string(result.body)
	start, end := strings.Index(body, "<Code>"), strings.Index(body, "</Code>")
	if start == -1 || end == -1 {
		return ""
	}
	return body[start+6 : end]
}

func assertPostError(t *testing.T, result postResult, status int, code string) {
	t.Helper()
	if result.status != status {
		t.Fatalf("POST status=%d want=%d body=%s", result.status, status, result.body)
	}
	if got := postErrorCode(result); got != code {
		t.Fatalf("POST code=%q want=%q body=%s", got, code, result.body)
	}
}

func assertPostSuccess(t *testing.T, result postResult) {
	t.Helper()
	if result.status != 204 {
		t.Fatalf("POST status=%d want=204 body=%s", result.status, result.body)
	}
}

// 폼 필드로 준 헤더·메타데이터·암호화·태그가 오브젝트에 저장되는지 확인
func TestPostObjectV4StoredFields(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	key := "test_post_object_v4_stored_fields"
	bucket := s.bucket(t, 37)
	fields := postFields{
		{"key", key},
		{"Content-Type", "image/png"},
		{"Cache-Control", "max-age=60"},
		{"Content-Disposition", "attachment; filename=a.txt"},
		{"Content-Encoding", "identity"},
		{"x-amz-meta-foo", "bar"},
		{"x-amz-server-side-encryption", "AES256"},
		{"x-amz-website-redirect-location", "/other.txt"},
		{"tagging", "<Tagging><TagSet><Tag><Key>k1</Key><Value>v1</Value></Tag></TagSet></Tagging>"},
	}
	assertPostSuccess(t, postV4Form(t, s, bucket, fields, postV4Options{conditions: fields.conditions()}))

	head, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}
	if got := aws.ToInt64(head.ContentLength); got != 3 {
		t.Fatalf("ContentLength = %d, want 3", got)
	}
	if got := aws.ToString(head.ContentType); got != "image/png" {
		t.Fatalf("ContentType = %q", got)
	}
	if got := aws.ToString(head.CacheControl); got != "max-age=60" {
		t.Fatalf("CacheControl = %q", got)
	}
	if got := aws.ToString(head.ContentDisposition); got != "attachment; filename=a.txt" {
		t.Fatalf("ContentDisposition = %q", got)
	}
	if got := aws.ToString(head.ContentEncoding); got != "identity" {
		t.Fatalf("ContentEncoding = %q", got)
	}
	if len(head.Metadata) != 1 || head.Metadata["foo"] != "bar" {
		t.Fatalf("Metadata = %v", head.Metadata)
	}
	if head.ServerSideEncryption != types.ServerSideEncryptionAes256 {
		t.Fatalf("ServerSideEncryption = %q", head.ServerSideEncryption)
	}
	if got := aws.ToString(head.WebsiteRedirectLocation); got != "/other.txt" {
		t.Fatalf("WebsiteRedirectLocation = %q", got)
	}
	tagging, err := s.client.GetObjectTagging(context.Background(), &s3.GetObjectTaggingInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}
	if len(tagging.TagSet) != 1 || aws.ToString(tagging.TagSet[0].Key) != "k1" || aws.ToString(tagging.TagSet[0].Value) != "v1" {
		t.Fatalf("TagSet = %v", tagging.TagSet)
	}
}

// 같은 키로 다시 올리면 덮어쓰고, 빈 파일도 올릴 수 있는지 확인
func TestPostObjectV4Overwrite(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	key := "test_post_object_v4_overwrite"
	bucket := s.bucket(t, 38)
	assertPostSuccess(t, postV4Form(t, s, bucket, postFields{{"key", key}}, postV4Options{body: aws.String("first")}))
	assertPostSuccess(t, postV4Form(t, s, bucket, postFields{{"key", key}}, postV4Options{body: aws.String("second")}))
	assertObjectBody(t, s, bucket, key, "second")
	// 빈 파일도 올릴 수 있다
	assertPostSuccess(t, postV4Form(t, s, bucket, postFields{{"key", "empty"}}, postV4Options{body: aws.String("")}))
	head, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String("empty")})
	if err != nil {
		t.Fatal(err)
	}
	if got := aws.ToInt64(head.ContentLength); got != 0 {
		t.Fatalf("ContentLength = %d, want 0", got)
	}
}

// 버전 관리 버킷에 같은 키로 올리면 버전이 쌓이는지 확인
func TestPostObjectV4Versioning(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	key := "test_post_object_v4_versioning"
	bucket := s.bucket(t, 39)
	checkConfigureVersioningRetry(t, s, bucket, types.BucketVersioningStatusEnabled)
	assertPostSuccess(t, postV4Form(t, s, bucket, postFields{{"key", key}}, postV4Options{body: aws.String("first")}))
	assertPostSuccess(t, postV4Form(t, s, bucket, postFields{{"key", key}}, postV4Options{body: aws.String("second")}))
	listed, err := s.client.ListObjectVersions(context.Background(), &s3.ListObjectVersionsInput{Bucket: aws.String(bucket)})
	if err != nil {
		t.Fatal(err)
	}
	if len(listed.Versions) != 2 {
		t.Fatalf("versions = %d, want 2", len(listed.Versions))
	}
	assertObjectBody(t, s, bucket, key, "second")
}

// 정책 조건에 맞는 폼은 올라가는지 확인
func TestPostObjectV4ConditionMatched(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 40)
	// 접두어가 맞는 키
	assertPostSuccess(t, postV4Form(t, s, bucket, postFields{{"key", "user/a.txt"}}, postV4Options{conditions: []any{[]any{"starts-with", "$key", "user/"}}}))
	// 빈 접두어는 아무 값이나 받는다
	assertPostSuccess(t, postV4Form(t, s, bucket, postFields{{"key", "any"}, {"x-amz-meta-foo", "anything"}}, postV4Options{conditions: []any{[]any{"starts-with", "$x-amz-meta-foo", ""}}}))
	// 조건의 필드 이름은 대소문자를 구분하지 않는다
	assertPostSuccess(t, postV4Form(t, s, bucket, postFields{{"key", "case"}, {"x-amz-meta-foo", "value"}}, postV4Options{conditions: []any{[]any{"starts-with", "$X-Amz-Meta-Foo", ""}}}))
	// x-ignore- 로 시작하는 필드는 조건이 없어도 된다
	assertPostSuccess(t, postV4Form(t, s, bucket, postFields{{"key", "ignore"}, {"x-ignore-foo", "bar"}}, postV4Options{}))
	assertBucketKeyCount(t, s, bucket, 4)
}

// 정책 조건에 맞지 않는 폼은 거부되는지 확인
func TestPostObjectV4ConditionFailed(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	key := "test_post_object_v4_condition_failed"
	bucket := s.bucket(t, 41)
	cases := []struct {
		fields     postFields
		conditions []any
	}{
		{postFields{{"key", "other/a.txt"}}, []any{[]any{"starts-with", "$key", "user/"}}},
		{postFields{{"key", key}, {"Content-Type", "text/html"}}, []any{[]any{"eq", "$Content-Type", "text/plain"}}},
		{postFields{{"key", key}, {"x-amz-meta-foo", "2"}}, []any{map[string]string{"x-amz-meta-foo": "1"}}},
		// eq 는 값의 대소문자를 구분한다
		{postFields{{"key", key}, {"x-amz-meta-foo", "ABC"}}, []any{[]any{"eq", "$x-amz-meta-foo", "abc"}}},
		// 정책에 있는 필드가 폼에 없다
		{postFields{{"key", key}}, []any{map[string]string{"x-amz-meta-foo": "bar"}}},
		// 폼에 있는 필드가 정책에 없다
		{postFields{{"key", key}, {"x-amz-meta-foo", "bar"}}, nil},
	}
	for _, tc := range cases {
		assertPostError(t, postV4Form(t, s, bucket, tc.fields, postV4Options{conditions: tc.conditions}), 403, "AccessDenied")
	}
	assertBucketKeyCount(t, s, bucket, 0)
}

// 필수 조건이 빠지거나 버킷 이름이 다른 정책은 거부되는지 확인
func TestPostObjectV4RequiredConditions(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	key := "test_post_object_v4_required_conditions"
	bucket := s.bucket(t, 42)
	expiration := postExpiration(100)
	// bucket, key, x-amz-algorithm, x-amz-credential, x-amz-date 조건은 모두 있어야 한다
	for index := range 5 {
		without := func(base []any) map[string]any {
			conditions := append(append([]any{}, base[:index]...), base[index+1:]...)
			return map[string]any{"expiration": expiration, "conditions": conditions}
		}
		assertPostError(t, postV4Form(t, s, bucket, postFields{{"key", key}}, postV4Options{policy: without}), 403, "AccessDenied")
	}
	// 다른 버킷 이름을 적은 정책
	other := func(base []any) map[string]any {
		conditions := append([]any{map[string]string{"bucket": "other-bucket-name"}}, base[1:]...)
		return map[string]any{"expiration": expiration, "conditions": conditions}
	}
	assertPostError(t, postV4Form(t, s, bucket, postFields{{"key", key}}, postV4Options{policy: other}), 403, "AccessDenied")
	assertBucketKeyCount(t, s, bucket, 0)
}

// 만료된 정책은 거부되는지 확인
func TestPostObjectV4ExpiredPolicy(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	key := "test_post_object_v4_expired_policy"
	bucket := s.bucket(t, 43)
	expired := func(base []any) map[string]any {
		return map[string]any{"expiration": postExpiration(-10), "conditions": base}
	}
	assertPostError(t, postV4Form(t, s, bucket, postFields{{"key", key}}, postV4Options{policy: expired}), 403, "AccessDenied")
	assertBucketKeyCount(t, s, bucket, 0)
}

// 형식이 잘못된 정책 문서는 InvalidPolicyDocument 로 거부되는지 확인
func TestPostObjectV4InvalidPolicyDocument(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	key := "test_post_object_v4_invalid_policy_document"
	bucket := s.bucket(t, 44)
	expiration := postExpiration(100)
	document := func(build func(base []any) map[string]any) postV4Options {
		return postV4Options{policy: build}
	}
	policies := []postV4Options{
		{rawPolicy: "!!!not-base64!!!"},
		{rawPolicy: base64.StdEncoding.EncodeToString([]byte("not json"))},
		// expiration 누락
		document(func(base []any) map[string]any { return map[string]any{"conditions": base} }),
		// conditions 누락
		document(func(base []any) map[string]any { return map[string]any{"expiration": expiration} }),
		document(func(base []any) map[string]any {
			return map[string]any{"expiration": expiration, "conditions": []any{}}
		}),
		// ISO8601 이 아니다
		document(func(base []any) map[string]any {
			return map[string]any{"expiration": "2030-01-01 00:00:00", "conditions": base}
		}),
		document(func(base []any) map[string]any {
			return map[string]any{"expiration": expiration, "conditions": base, "unknown": "value"}
		}),
		// 이름은 소문자여야 한다
		document(func(base []any) map[string]any {
			return map[string]any{"EXPIRATION": expiration, "CONDITIONS": base}
		}),
		// 조건 형식이 잘못됐다
		document(func(base []any) map[string]any {
			return map[string]any{"expiration": expiration, "conditions": append(base, []any{"ends-with", "$key", "txt"})}
		}),
		document(func(base []any) map[string]any {
			return map[string]any{"expiration": expiration, "conditions": append(base, []any{"eq", "$key"})}
		}),
		document(func(base []any) map[string]any {
			return map[string]any{"expiration": expiration, "conditions": append(base, []any{"content-length-range", 0})}
		}),
	}
	for _, policy := range policies {
		assertPostError(t, postV4Form(t, s, bucket, postFields{{"key", key}}, policy), 400, "InvalidPolicyDocument")
	}
	assertBucketKeyCount(t, s, bucket, 0)
}

// content-length-range 조건이 양 끝을 포함해 적용되는지 확인
func TestPostObjectV4ContentLengthRange(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 45)
	post := func(key string, minSize, maxSize int) postResult {
		return postV4Form(t, s, bucket, postFields{{"key", key}}, postV4Options{conditions: []any{[]any{"content-length-range", minSize, maxSize}}})
	}
	// 본문은 3바이트. 범위는 양 끝을 포함한다
	assertPostSuccess(t, post("max", 0, 3))
	assertPostSuccess(t, post("min", 3, 10))
	assertPostError(t, post("large", 0, 2), 400, "EntityTooLarge")
	assertPostError(t, post("small", 4, 10), 400, "EntityTooSmall")
	listed, err := s.client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String(bucket)})
	if err != nil {
		t.Fatal(err)
	}
	if len(listed.Contents) != 2 || aws.ToString(listed.Contents[0].Key) != "max" || aws.ToString(listed.Contents[1].Key) != "min" {
		t.Fatalf("contents = %#v, want [max min]", listed.Contents)
	}
}

// 서명이나 액세스 키가 틀린 폼은 거부되는지 확인
func TestPostObjectV4InvalidSignature(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	key := "test_post_object_v4_invalid_signature"
	bucket := s.bucket(t, 46)
	badSignature := func(fields *postFields) { fields.set("x-amz-signature", strings.Repeat("0", 64)) }
	badAccessKey := func(fields *postFields) {
		_, scope, _ := strings.Cut(fields.get("x-amz-credential"), "/")
		fields.set("x-amz-credential", "AKIAXXXXXXXXXXXXXXXX/"+scope)
	}
	assertPostError(t, postV4Form(t, s, bucket, postFields{{"key", key}}, postV4Options{mutate: badSignature}), 403, "SignatureDoesNotMatch")
	assertPostError(t, postV4Form(t, s, bucket, postFields{{"key", key}}, postV4Options{mutate: badAccessKey}), 403, "InvalidAccessKeyId")
	assertBucketKeyCount(t, s, bucket, 0)
}

// 서명 필드가 없거나 값이 잘못된 폼은 InvalidArgument 로 거부되는지 확인
func TestPostObjectV4InvalidAuthField(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	key := "test_post_object_v4_invalid_auth_field"
	bucket := s.bucket(t, 47)
	remove := func(name string) func(*postFields) {
		return func(fields *postFields) { fields.remove(name) }
	}
	setField := func(name, value string) func(*postFields) {
		return func(fields *postFields) { fields.set(name, value) }
	}
	wrongService := func(fields *postFields) {
		fields.set("x-amz-credential", strings.Replace(fields.get("x-amz-credential"), "/s3/aws4_request", "/ec2/aws4_request", 1))
	}
	mutations := []func(*postFields){
		// 서명 필드가 하나라도 없다
		remove("x-amz-signature"),
		remove("x-amz-date"),
		remove("x-amz-algorithm"),
		remove("x-amz-credential"),
		// 값이 잘못됐다
		setField("x-amz-algorithm", "AWS4-HMAC-SHA1"),
		setField("x-amz-date", "2030-01-01"),
		setField("x-amz-credential", "malformed"),
		wrongService,
	}
	for _, mutate := range mutations {
		assertPostError(t, postV4Form(t, s, bucket, postFields{{"key", key}}, postV4Options{mutate: mutate}), 400, "InvalidArgument")
	}
	assertBucketKeyCount(t, s, bucket, 0)
}

// 쓰기 권한이 없는 사용자의 폼과 서명 없는 폼은 거부되는지 확인
func TestPostObjectV4AccessDenied(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	altClientOrSkip(t, s)
	key := "test_post_object_v4_access_denied"
	bucket := s.bucket(t, 48)
	// 쓰기 권한이 없는 사용자가 서명한 폼
	assertPostError(t, postV4Form(t, s, bucket, postFields{{"key", key}}, postV4Options{user: &s.cfg.Alt}), 403, "AccessDenied")
	// 서명 없는 폼
	result := sendPostFields(t, postBucketURL(s, bucket), postFields{{"key", key}}, "foo.txt", "text/plain", []byte("bar"))
	assertPostError(t, result, 403, "AccessDenied")
	assertBucketKeyCount(t, s, bucket, 0)
}

// 키가 없거나 비었거나 너무 긴 폼은 거부되는지 확인
func TestPostObjectV4InvalidKey(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 49)
	assertPostError(t, postV4Form(t, s, bucket, postFields{}, postV4Options{}), 400, "InvalidArgument")
	assertPostError(t, postV4Form(t, s, bucket, postFields{{"key", ""}}, postV4Options{}), 400, "InvalidArgument")
	// 키는 1024바이트까지
	assertPostError(t, postV4Form(t, s, bucket, postFields{{"key", strings.Repeat("k", 1025)}}, postV4Options{}), 400, "KeyTooLongError")
	assertPostSuccess(t, postV4Form(t, s, bucket, postFields{{"key", strings.Repeat("k", 1024)}}, postV4Options{}))
	assertBucketKeyCount(t, s, bucket, 1)
}

// 값이 잘못된 폼 필드는 필드마다 정해진 에러로 거부되는지 확인
func TestPostObjectV4InvalidFieldValue(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	key := "test_post_object_v4_invalid_field_value"
	bucket := s.bucket(t, 50)
	duplicatedTag := "<Tagging><TagSet><Tag><Key>a</Key><Value>1</Value></Tag>" +
		"<Tag><Key>a</Key><Value>2</Value></Tag></TagSet></Tagging>"
	cases := []struct{ name, value, code string }{
		{"acl", "invalid", "InvalidArgument"},
		{"x-amz-server-side-encryption", "AES128", "InvalidArgument"},
		{"tagging", "<Tagging>", "MalformedXML"},
		{"tagging", duplicatedTag, "InvalidTag"},
		{"x-amz-website-redirect-location", "invalid", "InvalidRedirectLocation"},
		{"x-amz-meta-big", strings.Repeat("v", 2100), "MetadataTooLarge"},
	}
	for _, tc := range cases {
		result := postV4Form(t, s, bucket, postFields{{"key", key}, {tc.name, tc.value}}, postV4Options{conditions: []any{[]any{"starts-with", "$" + tc.name, ""}}})
		assertPostError(t, result, 400, tc.code)
	}
	assertBucketKeyCount(t, s, bucket, 0)
}

// Content-MD5 필드가 본문과 맞을 때만 올라가는지 확인
func TestPostObjectV4ContentMd5(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	key := "test_post_object_v4_content_md5"
	bucket := s.bucket(t, 51)
	post := func(digest string) postResult {
		return postV4Form(t, s, bucket, postFields{{"key", key}, {"Content-MD5", digest}}, postV4Options{conditions: []any{map[string]string{"Content-MD5": digest}}})
	}
	md5Of := func(value string) string {
		sum := md5.Sum([]byte(value))
		return base64.StdEncoding.EncodeToString(sum[:])
	}
	assertPostError(t, post(md5Of("other")), 400, "BadDigest")
	assertPostError(t, post("invalid"), 400, "InvalidDigest")
	assertBucketKeyCount(t, s, bucket, 0)
	assertPostSuccess(t, post(md5Of("bar")))
	assertObjectBody(t, s, bucket, key, "bar")
}

// x-amz-checksum-sha256 필드가 본문과 맞을 때만 올라가고 체크섬이 저장되는지 확인
func TestPostObjectV4Checksum(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	key := "test_post_object_v4_checksum"
	bucket := s.bucket(t, 52)
	post := func(checksum string) postResult {
		fields := postFields{{"key", key}, {"x-amz-checksum-algorithm", "SHA256"}, {"x-amz-checksum-sha256", checksum}}
		return postV4Form(t, s, bucket, fields, postV4Options{conditions: fields.conditions()})
	}
	sha256Of := func(value string) string {
		sum := sha256.Sum256([]byte(value))
		return base64.StdEncoding.EncodeToString(sum[:])
	}
	assertPostError(t, post(sha256Of("other")), 400, "BadDigest")
	assertBucketKeyCount(t, s, bucket, 0)
	assertPostSuccess(t, post(sha256Of("bar")))
	head, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), ChecksumMode: types.ChecksumModeEnabled})
	if err != nil {
		t.Fatal(err)
	}
	if got := aws.ToString(head.ChecksumSHA256); got != sha256Of("bar") {
		t.Fatalf("ChecksumSHA256 = %q, want %q", got, sha256Of("bar"))
	}
}

// acl 필드가 버킷의 ACL 사용 여부에 따라 적용되거나 거부되는지 확인
func TestPostObjectV4Acl(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	post := func(bucket, key, acl string) postResult {
		return postV4Form(t, s, bucket, postFields{{"key", key}, {"acl", acl}}, postV4Options{conditions: []any{map[string]string{"acl": acl}}})
	}
	// ACL 을 쓰는 버킷: acl 필드가 오브젝트 ACL 이 된다
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 53)
	assertPostSuccess(t, post(bucket, "public", "public-read"))
	out, err := s.client.GetObjectAcl(context.Background(), &s3.GetObjectAclInput{Bucket: aws.String(bucket), Key: aws.String("public")})
	if err != nil {
		t.Fatal(err)
	}
	grants := make([]string, 0, len(out.Grants))
	for _, grant := range out.Grants {
		grants = append(grants, aws.ToString(grant.Grantee.URI)+" "+string(grant.Permission))
	}
	sort.Strings(grants)
	if want := []string{" FULL_CONTROL", "http://acs.amazonaws.com/groups/global/AllUsers READ"}; !slices.Equal(grants, want) {
		t.Fatalf("grants = %q, want %q", grants, want)
	}

	// ACL 을 끈 버킷: 소유자 전권을 뜻하는 값만 받는다
	enforced := ownershipBucket(t, s, types.ObjectOwnershipBucketOwnerEnforced, 53)
	for _, acl := range []string{"private", "bucket-owner-full-control"} {
		assertPostSuccess(t, post(enforced, acl, acl))
	}
	assertPostError(t, post(enforced, "public", "public-read"), 400, "AccessControlListNotSupported")
	assertBucketKeyCount(t, s, enforced, 2)
}

// endregion
