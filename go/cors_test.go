package s3tests

import (
	"context"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// 버킷의 cors정보 세팅 성공 확인
func TestSetCors(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 1)
	assertNoCors(t, s, bucket)
	putCors(t, s, bucket, []types.CORSRule{{AllowedMethods: []string{"GET", "PUT"}, AllowedOrigins: []string{"*.get", "*.put"}}})
	out, err := s.client.GetBucketCors(context.Background(), &s3.GetBucketCorsInput{Bucket: aws.String(bucket)})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.CORSRules) != 1 || !equalStrings(out.CORSRules[0].AllowedMethods, []string{"GET", "PUT"}) || !equalStrings(out.CORSRules[0].AllowedOrigins, []string{"*.get", "*.put"}) {
		t.Fatalf("CORS rules = %#v", out.CORSRules)
	}
	if _, err := s.client.DeleteBucketCors(context.Background(), &s3.DeleteBucketCorsInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatal(err)
	}
	assertNoCors(t, s, bucket)
}

// 와일드카드 문자만 입력하여 cors설정을 하였을때 정상적으로 동작하는지 확인
func TestCorsOriginWildcard(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket := publicBucket(t, s, 3)
	assertNoCors(t, s, bucket)
	putCors(t, s, bucket, []types.CORSRule{{AllowedMethods: []string{"GET"}, AllowedOrigins: []string{"*"}}})

	checkCors(t, s, bucket, corsCase{method: "GET", status: 200})
	checkCors(t, s, bucket, corsCase{method: "GET", headers: map[string]string{"Origin": "example.origin"}, status: 200, allowOrigin: "*", allowMethods: "GET"})
}

// 모든 항목을 채운 cors 규칙이 그대로 조회되는지 확인
func TestCorsAllFields(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket := s.bucket(t, 5)
	putCors(t, s, bucket, []types.CORSRule{
		{
			ID:             aws.String("rule-1"),
			AllowedMethods: []string{"GET", "PUT"},
			AllowedOrigins: []string{"http://a.example.com"},
			AllowedHeaders: []string{"x-amz-*", "content-type"},
			ExposeHeaders:  []string{"ETag", "x-amz-meta-foo"},
			MaxAgeSeconds:  aws.Int32(600),
		},
		{ID: aws.String("rule-2"), AllowedMethods: []string{"GET"}, AllowedOrigins: []string{"*"}, MaxAgeSeconds: aws.Int32(20)},
	})

	rules := getCorsRules(t, s, bucket)
	if len(rules) != 2 {
		t.Fatalf("CORS rule count = %d, want 2", len(rules))
	}
	first, second := rules[0], rules[1]
	if aws.ToString(first.ID) != "rule-1" || !equalStrings(first.AllowedMethods, []string{"GET", "PUT"}) ||
		!equalStrings(first.AllowedOrigins, []string{"http://a.example.com"}) ||
		!equalStrings(first.AllowedHeaders, []string{"x-amz-*", "content-type"}) ||
		!equalStrings(first.ExposeHeaders, []string{"ETag", "x-amz-meta-foo"}) ||
		first.MaxAgeSeconds == nil || aws.ToInt32(first.MaxAgeSeconds) != 600 {
		t.Fatalf("first CORS rule = %#v", first)
	}
	if aws.ToString(second.ID) != "rule-2" || !equalStrings(second.AllowedMethods, []string{"GET"}) ||
		!equalStrings(second.AllowedOrigins, []string{"*"}) || second.MaxAgeSeconds == nil || aws.ToInt32(second.MaxAgeSeconds) != 20 {
		t.Fatalf("second CORS rule = %#v", second)
	}
}

// preflight 응답에 허용 헤더/노출 헤더/최대 유효 시간이 담기는지 확인
func TestCorsPreflightHeaders(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket := s.bucket(t, 6)
	putCors(t, s, bucket, []types.CORSRule{{
		AllowedMethods: []string{"GET", "PUT"},
		AllowedOrigins: []string{"http://a.example.com"},
		AllowedHeaders: []string{"x-amz-*", "content-type"},
		ExposeHeaders:  []string{"ETag", "x-amz-meta-foo"},
		MaxAgeSeconds:  aws.Int32(600),
	}})

	// 허용된 헤더(와일드카드, 대소문자 무시)로 preflight 요청
	status, headers := corsRequest(t, s, "OPTIONS", bucket, "", map[string]string{
		"Origin":                         "http://a.example.com",
		"Access-Control-Request-Method":  "PUT",
		"Access-Control-Request-Headers": "X-Amz-Meta-A, Content-Type",
	})
	assertCorsResponse(t, status, headers, 200, map[string]string{
		"Access-Control-Allow-Origin":      "http://a.example.com",
		"Access-Control-Allow-Methods":     "GET, PUT",
		"Access-Control-Allow-Headers":     "x-amz-meta-a, content-type",
		"Access-Control-Expose-Headers":    "ETag, x-amz-meta-foo",
		"Access-Control-Max-Age":           "600",
		"Access-Control-Allow-Credentials": "true",
	})

	// 허용되지 않은 헤더를 요청하면 거부된다
	status, headers = corsRequest(t, s, "OPTIONS", bucket, "", map[string]string{
		"Origin":                         "http://a.example.com",
		"Access-Control-Request-Method":  "PUT",
		"Access-Control-Request-Headers": "authorization",
	})
	assertCorsResponse(t, status, headers, 403, map[string]string{"Access-Control-Allow-Origin": ""})
}

// 여러 cors 규칙 중 먼저 선언된 규칙이 적용되는지 확인
func TestCorsRuleOrder(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket := s.bucket(t, 7)
	putCors(t, s, bucket, []types.CORSRule{
		{AllowedMethods: []string{"GET", "PUT"}, AllowedOrigins: []string{"http://a.example.com"}, MaxAgeSeconds: aws.Int32(600)},
		{AllowedMethods: []string{"GET"}, AllowedOrigins: []string{"*"}, MaxAgeSeconds: aws.Int32(20)},
	})

	// 두 규칙 모두에 맞으면 먼저 선언된 규칙이 적용된다
	status, headers := corsRequest(t, s, "OPTIONS", bucket, "", map[string]string{"Origin": "http://a.example.com", "Access-Control-Request-Method": "GET"})
	assertCorsResponse(t, status, headers, 200, map[string]string{
		"Access-Control-Allow-Origin":  "http://a.example.com",
		"Access-Control-Allow-Methods": "GET, PUT",
		"Access-Control-Max-Age":       "600",
	})

	// 첫 규칙에 맞지 않으면 다음 규칙이 적용된다
	status, headers = corsRequest(t, s, "OPTIONS", bucket, "", map[string]string{"Origin": "http://b.example.com", "Access-Control-Request-Method": "GET"})
	assertCorsResponse(t, status, headers, 200, map[string]string{
		"Access-Control-Allow-Origin":      "*",
		"Access-Control-Allow-Methods":     "GET",
		"Access-Control-Max-Age":           "20",
		"Access-Control-Allow-Credentials": "",
	})

	// 어느 규칙에도 맞지 않으면 거부된다
	status, headers = corsRequest(t, s, "OPTIONS", bucket, "", map[string]string{"Origin": "http://b.example.com", "Access-Control-Request-Method": "PUT"})
	assertCorsResponse(t, status, headers, 403, map[string]string{"Access-Control-Allow-Origin": ""})
}

// 오브젝트에 대한 GET/HEAD/preflight 요청에 cors 헤더가 담기는지 확인
func TestCorsObjectRequest(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket := publicBucket(t, s, 8)
	key := "test_cors_object_request"
	_, err := s.client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key), Body: strings.NewReader("cors"), ACL: types.ObjectCannedACLPublicRead,
	})
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	putCors(t, s, bucket, []types.CORSRule{{
		AllowedMethods: []string{"GET", "HEAD"}, AllowedOrigins: []string{"http://a.example.com"}, ExposeHeaders: []string{"ETag"},
	}})

	for _, method := range []string{"GET", "HEAD"} {
		status, headers := corsRequest(t, s, method, bucket, key, map[string]string{"Origin": "http://a.example.com"})
		assertCorsResponse(t, status, headers, 200, map[string]string{
			"Access-Control-Allow-Origin":   "http://a.example.com",
			"Access-Control-Allow-Methods":  "GET, HEAD",
			"Access-Control-Expose-Headers": "ETag",
		})

		// 규칙에 맞지 않는 Origin 이면 요청은 성공하지만 CORS 헤더가 없다
		status, headers = corsRequest(t, s, method, bucket, key, map[string]string{"Origin": "http://b.example.com"})
		assertCorsResponse(t, status, headers, 200, map[string]string{"Access-Control-Allow-Origin": ""})
	}

	// 오브젝트 키에 대한 preflight
	status, headers := corsRequest(t, s, "OPTIONS", bucket, key, map[string]string{"Origin": "http://a.example.com", "Access-Control-Request-Method": "GET"})
	assertCorsResponse(t, status, headers, 200, map[string]string{"Access-Control-Allow-Origin": "http://a.example.com"})
}

// 지원하지 않는 메서드로 cors 설정 시 실패하는지 확인
func TestCorsInvalidMethod(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket := s.bucket(t, 9)
	assertInvalidCors(t, s, bucket, []types.CORSRule{{AllowedMethods: []string{"PATCH"}, AllowedOrigins: []string{"*"}}})
	// 메서드 이름은 대문자만 허용된다
	assertInvalidCors(t, s, bucket, []types.CORSRule{{AllowedMethods: []string{"get"}, AllowedOrigins: []string{"*"}}})
}

// Origin 에 와일드카드를 둘 이상 넣으면 cors 설정이 실패하는지 확인
func TestCorsInvalidOriginWildcard(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket := s.bucket(t, 10)
	assertInvalidCors(t, s, bucket, []types.CORSRule{{AllowedMethods: []string{"GET"}, AllowedOrigins: []string{"*a*"}}})
}

// 허용 헤더에 와일드카드를 둘 이상 넣으면 cors 설정이 실패하는지 확인
func TestCorsInvalidHeaderWildcard(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket := s.bucket(t, 11)
	assertInvalidCors(t, s, bucket, []types.CORSRule{{AllowedMethods: []string{"GET"}, AllowedOrigins: []string{"*"}, AllowedHeaders: []string{"*a*"}}})
}

// 노출 헤더에 와일드카드를 넣으면 cors 설정이 실패하는지 확인
func TestCorsExposeHeaderWildcard(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket := s.bucket(t, 12)
	assertInvalidCors(t, s, bucket, []types.CORSRule{{AllowedMethods: []string{"GET"}, AllowedOrigins: []string{"*"}, ExposeHeaders: []string{"x-amz-*"}}})
}

// cors 규칙은 100개까지만 설정할 수 있는지 확인
func TestCorsRuleLimit(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket := s.bucket(t, 13)
	rules := make([]types.CORSRule, 0, 101)
	for i := 0; i < 100; i++ {
		rules = append(rules, types.CORSRule{AllowedMethods: []string{"GET"}, AllowedOrigins: []string{"http://" + strconv.Itoa(i) + ".example.com"}})
	}
	putCors(t, s, bucket, rules)
	if got := len(getCorsRules(t, s, bucket)); got != 100 {
		t.Fatalf("CORS rule count = %d, want 100", got)
	}

	rules = append(rules, types.CORSRule{AllowedMethods: []string{"GET"}, AllowedOrigins: []string{"http://100.example.com"}})
	assertInvalidCors(t, s, bucket, rules)
}

// cors 설정을 덮어쓰면 이전 규칙이 남지 않는지 확인
func TestCorsOverwrite(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket := s.bucket(t, 14)
	putCors(t, s, bucket, []types.CORSRule{
		{AllowedMethods: []string{"GET"}, AllowedOrigins: []string{"http://a.example.com"}},
		{AllowedMethods: []string{"PUT"}, AllowedOrigins: []string{"http://b.example.com"}},
	})
	putCors(t, s, bucket, []types.CORSRule{{AllowedMethods: []string{"HEAD"}, AllowedOrigins: []string{"http://z.example.com"}}})
	rules := getCorsRules(t, s, bucket)
	if len(rules) != 1 || !equalStrings(rules[0].AllowedMethods, []string{"HEAD"}) || !equalStrings(rules[0].AllowedOrigins, []string{"http://z.example.com"}) {
		t.Fatalf("CORS rules = %#v", rules)
	}
}

// cors 설정이 없는 버킷의 cors 삭제가 성공하는지 확인
func TestCorsDeleteNotExist(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket := s.bucket(t, 15)
	// 설정이 없어도 삭제는 성공한다
	if _, err := s.client.DeleteBucketCors(context.Background(), &s3.DeleteBucketCorsInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatal(err)
	}
	assertNoCors(t, s, bucket)
}

// 존재하지 않는 버킷에 cors 설정/조회/삭제 시 NoSuchBucket 에러가 발생하는지 확인
func TestCorsBucketNotExist(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket := buildBucketName(s.cfg.BucketPrefix, s.suiteID, 16)
	rules := []types.CORSRule{{AllowedMethods: []string{"GET"}, AllowedOrigins: []string{"*"}}}
	_, err := s.client.GetBucketCors(context.Background(), &s3.GetBucketCorsInput{Bucket: aws.String(bucket)})
	assertS3Error(t, err, 404, "NoSuchBucket")
	assertS3Error(t, putCorsError(s.client, bucket, rules), 404, "NoSuchBucket")
	_, err = s.client.DeleteBucketCors(context.Background(), &s3.DeleteBucketCorsInput{Bucket: aws.String(bucket)})
	assertS3Error(t, err, 404, "NoSuchBucket")
}

// 버킷 소유자가 아닌 사용자의 cors 설정/조회/삭제가 실패하는지 확인
func TestCorsAltUser(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	requireAltUser(t, s)
	altClient := s3Client(s.cfg, s.cfg.Alt)
	bucket := s.bucket(t, 17)
	rules := []types.CORSRule{{AllowedMethods: []string{"GET"}, AllowedOrigins: []string{"*"}}}
	putCors(t, s, bucket, rules)
	_, err := altClient.GetBucketCors(context.Background(), &s3.GetBucketCorsInput{Bucket: aws.String(bucket)})
	assertS3Error(t, err, 403, "AccessDenied")
	assertS3Error(t, putCorsError(altClient, bucket, rules), 403, "AccessDenied")
	_, err = altClient.DeleteBucketCors(context.Background(), &s3.DeleteBucketCorsInput{Bucket: aws.String(bucket)})
	assertS3Error(t, err, 403, "AccessDenied")
}

func putCors(t *testing.T, s *suite, bucket string, rules []types.CORSRule) {
	t.Helper()
	_, err := s.client.PutBucketCors(context.Background(), &s3.PutBucketCorsInput{
		Bucket:            aws.String(bucket),
		CORSConfiguration: &types.CORSConfiguration{CORSRules: rules},
	})
	if err != nil {
		t.Fatal(err)
	}
}

func assertNoCors(t *testing.T, s *suite, bucket string) {
	t.Helper()
	_, err := s.client.GetBucketCors(context.Background(), &s3.GetBucketCorsInput{Bucket: aws.String(bucket)})
	assertS3Error(t, err, 404, "NoSuchCORSConfiguration")
}

func publicBucket(t *testing.T, s *suite, id int) string {
	t.Helper()
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, id)
	_, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{
		Bucket: aws.String(bucket),
		ACL:    types.BucketCannedACLPublicRead,
	})
	if err != nil {
		t.Fatal(err)
	}
	return bucket
}

type corsCase struct {
	method       string
	headers      map[string]string
	status       int
	allowOrigin  string
	allowMethods string
	key          string
}

func checkCors(t *testing.T, s *suite, bucket string, c corsCase) {
	t.Helper()
	status, headers := corsRequest(t, s, c.method, bucket, c.key, c.headers)
	gotOrigin := headers.Get("Access-Control-Allow-Origin")
	gotMethods := headers.Get("Access-Control-Allow-Methods")
	if status != c.status || gotOrigin != c.allowOrigin || gotMethods != c.allowMethods {
		t.Fatalf("%s %s: status/origin/methods = %d/%q/%q, want %d/%q/%q",
			c.method, c.key, status, gotOrigin, gotMethods, c.status, c.allowOrigin, c.allowMethods)
	}
}

// corsRequest는 서명 없는 요청을 보내고 상태 코드와 응답 헤더를 돌려준다.
func corsRequest(t *testing.T, s *suite, method, bucket, key string, headers map[string]string) (int, http.Header) {
	t.Helper()
	url := strings.TrimRight(postBucketURL(s, bucket), "/")
	if key != "" {
		url += "/" + key
	}
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	return resp.StatusCode, resp.Header
}

// assertCorsResponse는 상태 코드와 응답 헤더를 확인한다. want 값이 빈 문자열이면 해당 헤더가 없어야 한다.
func assertCorsResponse(t *testing.T, status int, headers http.Header, wantStatus int, want map[string]string) {
	t.Helper()
	if status != wantStatus {
		t.Fatalf("status = %d, want %d", status, wantStatus)
	}
	for name, value := range want {
		if got := headers.Values(name); value == "" && len(got) != 0 || value != "" && headers.Get(name) != value {
			t.Fatalf("%s = %q, want %q", name, got, value)
		}
	}
}

func putCorsError(client *s3.Client, bucket string, rules []types.CORSRule) error {
	_, err := client.PutBucketCors(context.Background(), &s3.PutBucketCorsInput{
		Bucket:            aws.String(bucket),
		CORSConfiguration: &types.CORSConfiguration{CORSRules: rules},
	})
	return err
}

func assertInvalidCors(t *testing.T, s *suite, bucket string, rules []types.CORSRule) {
	t.Helper()
	assertS3Error(t, putCorsError(s.client, bucket, rules), 400, "InvalidRequest")
}

func getCorsRules(t *testing.T, s *suite, bucket string) []types.CORSRule {
	t.Helper()
	out, err := s.client.GetBucketCors(context.Background(), &s3.GetBucketCorsInput{Bucket: aws.String(bucket)})
	if err != nil {
		t.Fatalf("GetBucketCors: %v", err)
	}
	return out.CORSRules
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
