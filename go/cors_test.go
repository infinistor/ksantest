package s3tests

import (
	"context"
	"io"
	"net/http"
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

// 버킷의 cors정보를 URL로 읽고 쓰기 성공/실패 확인
func TestCorsOriginResponse(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket := publicBucket(t, s, 2)
	assertNoCors(t, s, bucket)
	putCors(t, s, bucket, []types.CORSRule{
		{AllowedMethods: []string{"GET"}, AllowedOrigins: []string{"*suffix"}},
		{AllowedMethods: []string{"GET"}, AllowedOrigins: []string{"start*end"}},
		{AllowedMethods: []string{"GET"}, AllowedOrigins: []string{"prefix*"}},
		{AllowedMethods: []string{"PUT"}, AllowedOrigins: []string{"*.put"}},
	})

	checkCors(t, s, bucket, corsCase{method: "GET", status: 200})
	checkCors(t, s, bucket, corsCase{method: "GET", headers: map[string]string{"Origin": "foo.suffix"}, status: 200, allowOrigin: "foo.suffix", allowMethods: "GET"})
	checkCors(t, s, bucket, corsCase{method: "GET", headers: map[string]string{"Origin": "foo.bar"}, status: 200})
	checkCors(t, s, bucket, corsCase{method: "GET", headers: map[string]string{"Origin": "foo.suffix.get"}, status: 200})
	checkCors(t, s, bucket, corsCase{method: "GET", headers: map[string]string{"Origin": "start_end"}, status: 200, allowOrigin: "start_end", allowMethods: "GET"})
	checkCors(t, s, bucket, corsCase{method: "GET", headers: map[string]string{"Origin": "start1end"}, status: 200, allowOrigin: "start1end", allowMethods: "GET"})
	checkCors(t, s, bucket, corsCase{method: "GET", headers: map[string]string{"Origin": "start12end"}, status: 200, allowOrigin: "start12end", allowMethods: "GET"})
	checkCors(t, s, bucket, corsCase{method: "GET", headers: map[string]string{"Origin": "0start12end"}, status: 200})
	checkCors(t, s, bucket, corsCase{method: "GET", headers: map[string]string{"Origin": "prefix"}, status: 200, allowOrigin: "prefix", allowMethods: "GET"})
	checkCors(t, s, bucket, corsCase{method: "GET", headers: map[string]string{"Origin": "prefix.suffix"}, status: 200, allowOrigin: "prefix.suffix", allowMethods: "GET"})
	checkCors(t, s, bucket, corsCase{method: "GET", headers: map[string]string{"Origin": "bla.prefix"}, status: 200})

	checkCors(t, s, bucket, corsCase{method: "GET", headers: map[string]string{"Origin": "foo.suffix"}, status: 404, allowOrigin: "foo.suffix", allowMethods: "GET", key: "bar"})
	checkCors(t, s, bucket, corsCase{
		method: "PUT",
		headers: map[string]string{
			"Origin":                        "foo.suffix",
			"Access-Control-Request-Method": "GET",
			"content-length":                "0",
		},
		status: 403, allowOrigin: "foo.suffix", allowMethods: "GET", key: "bar",
	})
	checkCors(t, s, bucket, corsCase{
		method: "PUT",
		headers: map[string]string{
			"Origin":                        "foo.suffix",
			"Access-Control-Request-Method": "PUT",
			"content-length":                "0",
		},
		status: 403, key: "bar",
	})
	checkCors(t, s, bucket, corsCase{
		method: "PUT",
		headers: map[string]string{
			"Origin":                        "foo.suffix",
			"Access-Control-Request-Method": "DELETE",
			"content-length":                "0",
		},
		status: 403, key: "bar",
	})
	checkCors(t, s, bucket, corsCase{
		method: "PUT",
		headers: map[string]string{
			"Origin":         "foo.suffix",
			"content-length": "0",
		},
		status: 403, key: "bar",
	})
	checkCors(t, s, bucket, corsCase{
		method: "PUT",
		headers: map[string]string{
			"Origin":         "foo.put",
			"content-length": "0",
		},
		status: 403, allowOrigin: "foo.put", allowMethods: "PUT", key: "bar",
	})
	checkCors(t, s, bucket, corsCase{method: "GET", headers: map[string]string{"Origin": "foo.suffix"}, status: 404, allowOrigin: "foo.suffix", allowMethods: "GET", key: "bar"})

	checkCors(t, s, bucket, corsCase{method: "OPTIONS", status: 400})
	checkCors(t, s, bucket, corsCase{method: "OPTIONS", headers: map[string]string{"Origin": "foo.suffix"}, status: 403})
	checkCors(t, s, bucket, corsCase{method: "OPTIONS", headers: map[string]string{"Origin": "foo.bla"}, status: 403})
	checkCors(t, s, bucket, corsCase{
		method: "OPTIONS",
		headers: map[string]string{
			"Origin":                        "foo.suffix",
			"Access-Control-Request-Method": "GET",
			"content-length":                "0",
		},
		status: 200, allowOrigin: "foo.suffix", allowMethods: "GET", key: "bar",
	})
	checkCors(t, s, bucket, corsCase{
		method: "OPTIONS",
		headers: map[string]string{
			"Origin":                        "foo.bar",
			"Access-Control-Request-Method": "GET",
		},
		status: 403,
	})
	checkCors(t, s, bucket, corsCase{
		method: "OPTIONS",
		headers: map[string]string{
			"Origin":                        "foo.suffix.get",
			"Access-Control-Request-Method": "GET",
		},
		status: 403,
	})
	checkCors(t, s, bucket, corsCase{
		method: "OPTIONS",
		headers: map[string]string{
			"Origin":                        "start_end",
			"Access-Control-Request-Method": "GET",
		},
		status: 200, allowOrigin: "start_end", allowMethods: "GET",
	})
	checkCors(t, s, bucket, corsCase{
		method: "OPTIONS",
		headers: map[string]string{
			"Origin":                        "start1end",
			"Access-Control-Request-Method": "GET",
		},
		status: 200, allowOrigin: "start1end", allowMethods: "GET",
	})
	checkCors(t, s, bucket, corsCase{
		method: "OPTIONS",
		headers: map[string]string{
			"Origin":                        "start12end",
			"Access-Control-Request-Method": "GET",
		},
		status: 200, allowOrigin: "start12end", allowMethods: "GET",
	})
	checkCors(t, s, bucket, corsCase{
		method: "OPTIONS",
		headers: map[string]string{
			"Origin":                        "0start12end",
			"Access-Control-Request-Method": "GET",
		},
		status: 403,
	})
	checkCors(t, s, bucket, corsCase{
		method: "OPTIONS",
		headers: map[string]string{
			"Origin":                        "prefix",
			"Access-Control-Request-Method": "GET",
		},
		status: 200, allowOrigin: "prefix", allowMethods: "GET",
	})
	checkCors(t, s, bucket, corsCase{
		method: "OPTIONS",
		headers: map[string]string{
			"Origin":                        "prefix.suffix",
			"Access-Control-Request-Method": "GET",
		},
		status: 200, allowOrigin: "prefix.suffix", allowMethods: "GET",
	})
	checkCors(t, s, bucket, corsCase{
		method: "OPTIONS",
		headers: map[string]string{
			"Origin":                        "bla.prefix",
			"Access-Control-Request-Method": "GET",
		},
		status: 403,
	})
	checkCors(t, s, bucket, corsCase{
		method: "OPTIONS",
		headers: map[string]string{
			"Origin":                        "foo.put",
			"Access-Control-Request-Method": "GET",
		},
		status: 403,
	})
	checkCors(t, s, bucket, corsCase{
		method: "OPTIONS",
		headers: map[string]string{
			"Origin":                        "foo.put",
			"Access-Control-Request-Method": "PUT",
		},
		status: 200, allowOrigin: "foo.put", allowMethods: "PUT",
	})
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

// cors옵션에서 사용자 추가 헤더를 설정하고 존재하지 않는 헤더를 request 설정한 채로 cors호출하면 실패하는지 확인
func TestCorsHeaderOption(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket := publicBucket(t, s, 4)
	assertNoCors(t, s, bucket)
	putCors(t, s, bucket, []types.CORSRule{{
		AllowedMethods: []string{"GET"},
		AllowedOrigins: []string{"*"},
		ExposeHeaders:  []string{"x-amz-meta-header1"},
	}})

	checkCors(t, s, bucket, corsCase{
		method: "OPTIONS",
		headers: map[string]string{
			"Origin":                         "example.origin",
			"Access-Control-Request-headers": "x-amz-meta-header2",
			"Access-Control-Request-Method":  "GET",
		},
		status: 403,
		key:    "bar",
	})
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
	url := strings.TrimRight(postBucketURL(s, bucket), "/")
	if c.key != "" {
		url += "/" + c.key
	}
	req, err := http.NewRequest(c.method, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range c.headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	gotOrigin := resp.Header.Get("Access-Control-Allow-Origin")
	gotMethods := resp.Header.Get("Access-Control-Allow-Methods")
	if resp.StatusCode != c.status || gotOrigin != c.allowOrigin || gotMethods != c.allowMethods {
		t.Fatalf("%s %s: status/origin/methods = %d/%q/%q, want %d/%q/%q",
			c.method, c.key, resp.StatusCode, gotOrigin, gotMethods, c.status, c.allowOrigin, c.allowMethods)
	}
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
