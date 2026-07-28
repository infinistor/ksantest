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

func TestPostObjectAnonymousRequest(t *testing.T) {
	t.Parallel()

	testPostAnonymous(t)
}
func TestPostObjectAuthenticatedRequest(t *testing.T) {
	t.Parallel()

	testPostAuthenticated(t)
}
func TestPostObjectSetSuccessCode(t *testing.T) {
	t.Parallel()

	testPostAnonymousStatus(t, "201", 201)
}
func TestPostObjectSetInvalidSuccessCode(t *testing.T) {
	t.Parallel()

	testPostAnonymousStatus(t, "404", 204)
}
func TestPostObjectAuthenticatedNoContentType(t *testing.T) {
	t.Parallel()

	testPostCompatibilityCase(t, "test_post_object_authenticated_no_content_type", 204)
}
func TestPostObjectAuthenticatedRequestBadAccessKey(t *testing.T) {
	t.Parallel()

	testPostCompatibilityCase(t, "test_post_object_authenticated_request_bad_access_key", 403)
}
func TestPostObjectUploadLargerThanChunk(t *testing.T) {
	t.Parallel()

	testPostCompatibilityCase(t, "test_post_object_upload_larger_than_chunk", 204)
}
func TestPostObjectSetKeyFromFilename(t *testing.T) {
	t.Parallel()

	testPostCompatibilityCase(t, "test_post_object_set_key_from_filename", 204)
}
func TestPostObjectIgnoredHeader(t *testing.T) {
	t.Parallel()

	testPostCompatibilityCase(t, "test_post_object_ignored_header", 204)
}
func TestPostObjectCaseInsensitiveConditionFields(t *testing.T) {
	t.Parallel()

	testPostCompatibilityCase(t, "test_post_object_case_insensitive_condition_fields", 204)
}
func TestPostObjectEscapedFieldValues(t *testing.T) {
	t.Parallel()

	testPostCompatibilityCase(t, "test_post_object_escaped_field_values", 204)
}
func TestPostObjectSuccessRedirectAction(t *testing.T) {
	t.Parallel()

	testPostCompatibilityCase(t, "test_post_object_success_redirect_action", 200)
}
func TestPostObjectInvalidSignature(t *testing.T) {
	t.Parallel()

	testPostCompatibilityCase(t, "test_post_object_invalid_signature", 403)
}
func TestPostObjectInvalidAccessKey(t *testing.T) {
	t.Parallel()

	testPostCompatibilityCase(t, "test_post_object_invalid_access_key", 403)
}
func TestPostObjectInvalidDateFormat(t *testing.T) {
	t.Parallel()

	testPostCompatibilityCase(t, "test_post_object_invalid_date_format", 400)
}
func TestPostObjectNoKeySpecified(t *testing.T) {
	t.Parallel()

	testPostCompatibilityCase(t, "test_post_object_no_key_specified", 400)
}
func TestPostObjectMissingSignature(t *testing.T) {
	t.Parallel()

	testPostCompatibilityCase(t, "test_post_object_missing_signature", 400)
}
func TestPostObjectMissingPolicyCondition(t *testing.T) {
	t.Parallel()

	testPostCompatibilityCase(t, "test_post_object_missing_policy_condition", 403)
}
func TestPostObjectUserSpecifiedHeader(t *testing.T) {
	t.Parallel()

	testPostCompatibilityCase(t, "test_post_object_user_specified_header", 204)
}
func TestPostObjectRequestMissingPolicySpecifiedField(t *testing.T) {
	t.Parallel()

	testPostCompatibilityCase(t, "test_post_object_request_missing_policy_specified_field", 403)
}
func TestPostObjectConditionIsCaseSensitive(t *testing.T) {
	t.Parallel()

	testPostCompatibilityCase(t, "test_post_object_condition_is_case_sensitive", 400)
}
func TestPostObjectExpiresIsCaseSensitive(t *testing.T) {
	t.Parallel()

	testPostCompatibilityCase(t, "test_post_object_expires_is_case_sensitive", 400)
}
func TestPostObjectExpiredPolicy(t *testing.T) {
	t.Parallel()

	testPostCompatibilityCase(t, "test_post_object_expired_policy", 403)
}
func TestPostObjectInvalidRequestFieldValue(t *testing.T) {
	t.Parallel()

	testPostCompatibilityCase(t, "test_post_object_invalid_request_field_value", 403)
}
func TestPostObjectMissingExpiresCondition(t *testing.T) {
	t.Parallel()

	testPostCompatibilityCase(t, "test_post_object_missing_expires_condition", 400)
}
func TestPostObjectMissingConditionsList(t *testing.T) {
	t.Parallel()

	testPostCompatibilityCase(t, "test_post_object_missing_conditions_list", 400)
}
func TestPostObjectUploadSizeLimitExceeded(t *testing.T) {
	t.Parallel()

	testPostCompatibilityCase(t, "test_post_object_upload_size_limit_exceeded", 400)
}
func TestPostObjectMissingContentLengthArgument(t *testing.T) {
	t.Parallel()

	testPostCompatibilityCase(t, "test_post_object_missing_content_length_argument", 400)
}
func TestPostObjectInvalidContentLengthArgument(t *testing.T) {
	t.Parallel()

	testPostCompatibilityCase(t, "test_post_object_invalid_content_length_argument", 400)
}
func TestPostObjectUploadSizeBelowMinimum(t *testing.T) {
	t.Parallel()

	testPostCompatibilityCase(t, "test_post_object_upload_size_below_minimum", 400)
}
func TestPostObjectEmptyConditions(t *testing.T) {
	t.Parallel()

	testPostCompatibilityCase(t, "test_post_object_empty_conditions", 400)
}
func TestPresignedUrlPutGet(t *testing.T) {
	t.Parallel()

	testPostPresigned(t)
}
func TestPutObjectV4(t *testing.T) {
	t.Parallel()

	testPostSDKPut(t, false)
}
func TestPutObjectChunkedV4(t *testing.T) {
	t.Parallel()

	testPostSDKPut(t, true)
}
func TestGetObjectV4(t *testing.T) {
	t.Parallel()

	testPostSDKGet(t)
}
func TestPostObjectWrongBucket(t *testing.T) {
	t.Parallel()

	testPostWrongBucket(t)
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
