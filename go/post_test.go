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

func TestPost(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		run  func(*testing.T)
	}{
		// post 방식으로 권한없는 사용자가 파일 업로드할 경우 성공 확인
		{"test_post_object_anonymous_request", testPostAnonymous},
		// post 방식으로 로그인 정보를 포함한 파일 업로드할 경우 성공 확인
		{"test_post_object_authenticated_request", testPostAuthenticated},
		// [성공시 반환상태값을 201로 설정] post 방식으로 권한없는 사용자가 파일 업로드시 에러체크가 올바른지 확인
		{"test_post_object_set_success_code", func(t *testing.T) { testPostAnonymousStatus(t, "201", 201) }},
		// [성공시 반환상태값을 에러코드인 404로 설정] post 방식으로 권한없는 사용자가 파일 업로드시 에러체크가 올바른지 확인
		{"test_post_object_set_invalid_success_code", func(t *testing.T) { testPostAnonymousStatus(t, "404", 204) }},
		// content-type 헤더 정보 없이 post 방식으로 로그인정보를 포함한 파일 업로드시 올바르게 업로드 되는지 확인
		{"test_post_object_authenticated_no_content_type", func(t *testing.T) {
			testPostCompatibilityCase(t, "test_post_object_authenticated_no_content_type", 204)
		}},
		// [PostKey 값이 틀린 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
		{"test_post_object_authenticated_request_bad_access_key", func(t *testing.T) {
			testPostCompatibilityCase(t, "test_post_object_authenticated_request_bad_access_key", 403)
		}},
		// post 방식으로 로그인정보를 포함한 대용량 파일 업로드시 올바르게 업로드 되는지 확인
		{"test_post_object_upload_larger_than_chunk", func(t *testing.T) {
			testPostCompatibilityCase(t, "test_post_object_upload_larger_than_chunk", 204)
		}},
		// [오브젝트 이름을 로그인정보에 포함되어 있는 key값으로 대체할 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 올바르게 업로드 되는지 확인
		{"test_post_object_set_key_from_filename", func(t *testing.T) {
			testPostCompatibilityCase(t, "test_post_object_set_key_from_filename", 204)
		}},
		// post 방식으로 로그인, 헤더 정보를 포함한 파일 업로드시 올바르게 업로드 되는지 확인
		{"test_post_object_ignored_header", func(t *testing.T) {
			testPostCompatibilityCase(t, "test_post_object_ignored_header", 204)
		}},
		// [헤더정보에 대소문자를 섞어서 사용할 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 올바르게 업로드 되는지 확인
		{"test_post_object_case_insensitive_condition_fields", func(t *testing.T) {
			testPostCompatibilityCase(t, "test_post_object_case_insensitive_condition_fields", 204)
		}},
		// [오브젝트 이름에 '\'를 사용할 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 올바르게 업로드 되는지 확인
		{"test_post_object_escaped_field_values", func(t *testing.T) {
			testPostCompatibilityCase(t, "test_post_object_escaped_field_values", 204)
		}},
		// [redirect url설정하여 체크] post 방식으로 로그인정보를 포함한 파일 업로드시 올바르게 업로드 되는지 확인
		{"test_post_object_success_redirect_action", func(t *testing.T) {
			testPostCompatibilityCase(t, "test_post_object_success_redirect_action", 200)
		}},
		// [SecretKey Hash 값이 틀린경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
		{"test_post_object_invalid_signature", func(t *testing.T) {
			testPostCompatibilityCase(t, "test_post_object_invalid_signature", 403)
		}},
		// [PostKey 값이 틀린경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
		{"test_post_object_invalid_access_key", func(t *testing.T) {
			testPostCompatibilityCase(t, "test_post_object_invalid_access_key", 403)
		}},
		// [로그인 정보의 날짜포맷이 다를경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
		{"test_post_object_invalid_date_format", func(t *testing.T) {
			testPostCompatibilityCase(t, "test_post_object_invalid_date_format", 400)
		}},
		// [오브젝트 이름을 입력하지 않을 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
		{"test_post_object_no_key_specified", func(t *testing.T) {
			testPostCompatibilityCase(t, "test_post_object_no_key_specified", 400)
		}},
		// [signature 정보를 누락하고 업로드할 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
		{"test_post_object_missing_signature", func(t *testing.T) {
			testPostCompatibilityCase(t, "test_post_object_missing_signature", 400)
		}},
		// [policy에 버킷 이름을 누락하고 업로드할 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
		{"test_post_object_missing_policy_condition", func(t *testing.T) {
			testPostCompatibilityCase(t, "test_post_object_missing_policy_condition", 403)
		}},
		// [사용자가 추가 메타데이터를 입력한 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 올바르게 업로드 되는지 확인
		{"test_post_object_user_specified_header", func(t *testing.T) {
			testPostCompatibilityCase(t, "test_post_object_user_specified_header", 204)
		}},
		// [사용자가 추가 메타데이터를 policy에 설정하였으나 오브젝트에 해당 정보가 누락된 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
		{"test_post_object_request_missing_policy_specified_field", func(t *testing.T) {
			testPostCompatibilityCase(t, "test_post_object_request_missing_policy_specified_field", 403)
		}},
		// [policy의 condition을 대문자(CONDITIONS)로 입력할 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
		{"test_post_object_condition_is_case_sensitive", func(t *testing.T) {
			testPostCompatibilityCase(t, "test_post_object_condition_is_case_sensitive", 400)
		}},
		// [policy의 expiration을 대문자(EXPIRATION)로 입력할 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
		{"test_post_object_expires_is_case_sensitive", func(t *testing.T) {
			testPostCompatibilityCase(t, "test_post_object_expires_is_case_sensitive", 400)
		}},
		// [policy의 expiration을 만료된 값으로 입력할 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
		{"test_post_object_expired_policy", func(t *testing.T) {
			testPostCompatibilityCase(t, "test_post_object_expired_policy", 403)
		}},
		// [사용자가 추가 메타데이터를 policy에 설정하였으나 설정정보가 올바르지 않을 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
		{"test_post_object_invalid_request_field_value", func(t *testing.T) {
			testPostCompatibilityCase(t, "test_post_object_invalid_request_field_value", 403)
		}},
		// [policy의 expiration값을 누락했을 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
		{"test_post_object_missing_expires_condition", func(t *testing.T) {
			testPostCompatibilityCase(t, "test_post_object_missing_expires_condition", 400)
		}},
		// [policy의 conditions값을 누락했을 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
		{"test_post_object_missing_conditions_list", func(t *testing.T) {
			testPostCompatibilityCase(t, "test_post_object_missing_conditions_list", 400)
		}},
		// [policy에 설정한 용량보다 큰 오브젝트를 업로드 할 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
		{"test_post_object_upload_size_limit_exceeded", func(t *testing.T) {
			testPostCompatibilityCase(t, "test_post_object_upload_size_limit_exceeded", 400)
		}},
		// [policy에 용량정보 설정을 누락할 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
		{"test_post_object_missing_content_length_argument", func(t *testing.T) {
			testPostCompatibilityCase(t, "test_post_object_missing_content_length_argument", 400)
		}},
		// [policy에 용량정보 설정값이 틀렸을 경우(용량값을 음수로 입력) post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
		{"test_post_object_invalid_content_length_argument", func(t *testing.T) {
			testPostCompatibilityCase(t, "test_post_object_invalid_content_length_argument", 400)
		}},
		// [policy에 설정한 용량보다 작은 오브젝트를 업로드 할 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
		{"test_post_object_upload_size_below_minimum", func(t *testing.T) {
			testPostCompatibilityCase(t, "test_post_object_upload_size_below_minimum", 400)
		}},
		// [policy의 conditions값이 비어있을 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
		{"test_post_object_empty_conditions", func(t *testing.T) {
			testPostCompatibilityCase(t, "test_post_object_empty_conditions", 400)
		}},
		// PresignedURL로 오브젝트 업로드, 다운로드 성공 확인
		{"test_presigned_url_put_get", testPostPresigned},
		// SignatureVersion4로 오브젝트 업로드 성공 확인
		{"test_put_object_v4", func(t *testing.T) { testPostSDKPut(t, false) }},
		// [SignatureVersion4] post 방식으로 내용을 암호화 하여 오브젝트 업로드 성공 확인
		{"test_put_object_chunked_v4", func(t *testing.T) { testPostSDKPut(t, true) }},
		// [SignatureVersion4] post 방식으로 오브젝트 다운로드 성공 확인
		{"test_get_object_v4", testPostSDKGet},
		// [policy에 설정된 버킷과 다른 버킷으로 업로드할 경우] post 방식으로 로그인정보를 포함한 파일 업로드시 실패하는지 확인
		{"test_post_object_wrong_bucket", testPostWrongBucket},
	}
	for _, tc := range tests {
		t.Run(tc.name, tc.run)
	}
}

func testPostAnonymous(t *testing.T) {
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

func testPostAuthenticated(t *testing.T) {
	s := newSuite(t)
	bucket := s.bucket(t)
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

func testPostAnonymousStatus(t *testing.T, requested string, want int) {
	t.Helper()
	s := newSuite(t)
	bucket := postPublicBucket(t, s)
	fields := map[string]string{"key": "foo.txt", "acl": "public-read", "Content-Type": "text/plain", "success_action_status": requested}
	result := sendPostForm(t, postBucketURL(s, bucket), fields, "foo.txt", "text/plain", []byte("bar"))
	if result.status != want {
		t.Fatalf("POST status=%d want=%d body=%s", result.status, want, result.body)
	}
	if got := read(t, s, bucket, "foo.txt"); got != "bar" {
		t.Fatalf("body=%q", got)
	}
}

func testPostCompatibilityCase(t *testing.T, name string, want int) {
	t.Helper()
	s := newSuite(t)
	if s.cfg.Endpoint() == "" {
		t.Skip("source scenario uses SigV2 or compatibility-specific POST behavior")
	}
	bucket := s.bucket(t)
	if name == "test_post_object_success_redirect_action" {
		bucket = postPublicBucket(t, s)
	}
	key, contentType, fileBody := `\$foo.txt`, "text/plain", []byte("bar")
	keyPrefix, minSize, maxSize := `\$foo`, 0, 1024
	conditions := postV2Conditions(bucket, contentType, keyPrefix, minSize, maxSize)
	document := map[string]any{"expiration": time.Now().UTC().Add(100 * time.Minute).Format(time.RFC3339), "conditions": conditions}
	fields := map[string]string{"key": key, "acl": "private", "Content-Type": contentType}

	switch name {
	case "test_post_object_authenticated_no_content_type":
		key, keyPrefix = "foo.txt", "foo"
		fields["key"] = key
		delete(fields, "Content-Type")
		document["conditions"] = []any{map[string]string{"bucket": bucket}, []string{"starts-with", "$key", keyPrefix}, map[string]string{"acl": "private"}, []any{"content-length-range", 0, 1024}}
	case "test_post_object_authenticated_request_bad_access_key":
		key, keyPrefix = "foo.txt", "foo"
		fields["key"] = key
		document["conditions"] = postV2Conditions(bucket, contentType, keyPrefix, 0, 1024)
	case "test_post_object_upload_larger_than_chunk":
		key, keyPrefix = "foo.txt", "foo"
		fields["key"] = key
		fileBody = deterministicBody(5 * 1024 * 1024)
		document["conditions"] = postV2Conditions(bucket, contentType, keyPrefix, 0, len(fileBody))
	case "test_post_object_set_key_from_filename":
		key, keyPrefix = "foo.txt", "foo"
		fields["key"] = key
		document["conditions"] = postV2Conditions(bucket, contentType, keyPrefix, 0, 1024)
	case "test_post_object_ignored_header":
		key, keyPrefix = "foo.txt", "foo"
		fields["key"], fields["x-ignore-foo"] = key, "bar"
		document["conditions"] = postV2Conditions(bucket, contentType, keyPrefix, 0, 1024)
	case "test_post_object_case_insensitive_condition_fields":
		key = "foo.txt"
		fields = map[string]string{"kEy": key, "aCl": "private", "Content-Type": contentType}
		document["conditions"] = []any{map[string]string{"bUcKeT": bucket}, []string{"StArTs-WiTh", "$KeY", "foo"}, map[string]string{"AcL": "private"}, []string{"StArTs-WiTh", "$CoNtEnT-TyPe", contentType}, []any{"content-length-range", 0, 1024}}
	case "test_post_object_success_redirect_action":
		key, keyPrefix = "foo.txt", "foo"
		fields["key"] = key
		redirect := postBucketURL(s, bucket)
		fields["successActionRedirect"] = redirect
		document["conditions"] = append(postV2Conditions(bucket, contentType, keyPrefix, 0, 1024), []string{"eq", "$successActionRedirect", redirect})
	case "test_post_object_invalid_date_format":
		document["expiration"] = strings.ReplaceAll(document["expiration"].(string), "T", " ")
	case "test_post_object_no_key_specified":
		delete(fields, "key")
		document["conditions"] = []any{map[string]string{"bucket": bucket}, map[string]string{"acl": "private"}, []string{"starts-with", "$Content-Type", contentType}, []any{"content-length-range", 0, 1024}}
	case "test_post_object_missing_policy_condition":
		document["conditions"] = conditions[1:]
	case "test_post_object_user_specified_header":
		key, keyPrefix = "foo.txt", "foo"
		fields["key"], fields["x-amz-meta-foo"] = key, "bar-clamp"
		document["conditions"] = append(postV2Conditions(bucket, contentType, keyPrefix, 0, 1024), []string{"starts-with", "$x-amz-meta-foo", "bar"})
	case "test_post_object_request_missing_policy_specified_field":
		document["conditions"] = append(conditions, []string{"starts-with", "$x-amz-meta-foo", "bar"})
	case "test_post_object_condition_is_case_sensitive":
		delete(document, "conditions")
		document["CONDITIONS"] = conditions
	case "test_post_object_expires_is_case_sensitive":
		delete(document, "expiration")
		document["EXPIRATION"] = time.Now().UTC().Add(100 * time.Minute).Format(time.RFC3339)
	case "test_post_object_expired_policy":
		document["expiration"] = time.Now().UTC().Add(-100 * time.Minute).Format(time.RFC3339)
	case "test_post_object_invalid_request_field_value":
		fields["x-amz-meta-foo"] = "bar-clamp"
		document["conditions"] = append(conditions, []string{"eq", "$x-amz-meta-foo", ""})
	case "test_post_object_missing_expires_condition":
		delete(document, "expiration")
	case "test_post_object_missing_conditions_list":
		delete(document, "conditions")
	case "test_post_object_upload_size_limit_exceeded":
		document["conditions"] = postV2Conditions(bucket, contentType, keyPrefix, 0, 0)
	case "test_post_object_missing_content_length_argument":
		document["conditions"] = append(conditions[:len(conditions)-1], []any{"content-length-range", 0})
	case "test_post_object_invalid_content_length_argument":
		document["conditions"] = postV2Conditions(bucket, contentType, keyPrefix, -1, 0)
	case "test_post_object_upload_size_below_minimum":
		document["conditions"] = postV2Conditions(bucket, contentType, keyPrefix, 512, 1024)
	case "test_post_object_empty_conditions":
		key = "foo.txt"
		fields["key"] = key
		document["conditions"] = []any{}
	}

	policy := encodePostPolicy(document)
	fields["AWSAccessKeyId"] = s.cfg.Main.AccessKey
	fields["signature"] = postV2Signature(policy, s.cfg.Main.SecretKey)
	fields["policy"] = policy
	if name == "test_post_object_authenticated_request_bad_access_key" {
		fields["AWSAccessKeyId"] = "foo"
	}
	if name == "test_post_object_invalid_signature" {
		fields["signature"] = strings.TrimSuffix(fields["signature"], "=")
	}
	if name == "test_post_object_invalid_access_key" {
		fields["AWSAccessKeyId"] = strings.TrimSuffix(s.cfg.Main.AccessKey, s.cfg.Main.AccessKey[len(s.cfg.Main.AccessKey)-1:])
	}
	if name == "test_post_object_missing_signature" {
		delete(fields, "signature")
	}
	if name == "test_post_object_case_insensitive_condition_fields" {
		fields["AWSAccessKeyId"], fields["signature"], fields["pOLICy"] = s.cfg.Main.AccessKey, postV2Signature(policy, s.cfg.Main.SecretKey), policy
		delete(fields, "policy")
	}
	result := sendPostForm(t, postBucketURL(s, bucket), fields, key, contentType, fileBody)
	if result.status != want {
		t.Fatalf("POST status=%d want=%d body=%s", result.status, want, result.body)
	}
	if name == "test_post_object_success_redirect_action" && (!strings.Contains(result.url, "bucket="+bucket) || !strings.Contains(result.url, "key="+key) || !strings.Contains(result.url, "etag=")) {
		t.Fatalf("redirect URL=%q", result.url)
	}
	if want == 204 {
		objectKey := key
		if name == "test_post_object_case_insensitive_condition_fields" {
			objectKey = "foo.txt"
		}
		if got := read(t, s, bucket, objectKey); !bytes.Equal([]byte(got), fileBody) {
			t.Fatalf("stored body size=%d want=%d", len(got), len(fileBody))
		}
		if name == "test_post_object_user_specified_header" {
			head, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(objectKey)})
			if err != nil || head.Metadata["foo"] != "bar-clamp" {
				t.Fatalf("metadata=%v err=%v", head.Metadata, err)
			}
		}
	}
}

func testPostPresigned(t *testing.T) {
	s := newSuite(t)
	bucket := s.bucket(t)
	presign := s3.NewPresignClient(s.client)
	body := []byte("foo")
	putURL, err := presign.PresignPutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String("foo")})
	if err != nil {
		t.Fatal(err)
	}
	request, _ := http.NewRequest(http.MethodPut, putURL.URL, bytes.NewReader(body))
	copySignedHeaders(request, putURL.SignedHeader)
	response, err := http.DefaultClient.Do(request)
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
	response, err = http.Get(getURL.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	got, readErr := io.ReadAll(response.Body)
	if response.StatusCode != 200 || readErr != nil || !bytes.Equal(got, body) {
		t.Fatalf("GET status=%d body=%q err=%v", response.StatusCode, got, readErr)
	}
}

func testPostSDKPut(t *testing.T, chunked bool) {
	t.Helper()
	s := newSuite(t)
	bucket := s.bucket(t)
	body := deterministicBody(100)
	putObjectMaybeChunked(t, s.client, bucket, "foo", body, chunked)
	assertObjectBytes(t, s.client, bucket, "foo", body)
}

func testPostSDKGet(t *testing.T) {
	s := newSuite(t)
	bucket := s.bucket(t)
	body := deterministicBody(100)
	putBytes(t, s.client, bucket, "foo", body)
	assertObjectBytes(t, s.client, bucket, "foo", body)
}

func testPostWrongBucket(t *testing.T) {
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
	response, err := http.DefaultClient.Do(request)
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
