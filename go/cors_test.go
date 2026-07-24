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

func TestCors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		run  func(*testing.T)
	}{
		// 버킷의 cors정보 세팅 성공 확인
		{"test_set_cors", func(t *testing.T) {
			s := newSuite(t)
			bucket := s.bucket(t)
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
		}},
		// 버킷의 cors정보를 URL로 읽고 쓰기 성공/실패 확인
		{"test_cors_origin_response", func(t *testing.T) {
			// Java @Disabled — KSAN OPTIONS status expectations diverge from Ceph/AWS suite.
			t.Skip("Java Cors.testCorsOriginResponse is @Disabled")
		}},
		// 와일드카드 문자만 입력하여 cors설정을 하였을때 정상적으로 동작하는지 확인
		{"test_cors_origin_wildcard", func(t *testing.T) {
			t.Skip("Java Cors.testCorsOriginWildcard is @Disabled")
		}},
		// cors옵션에서 사용자 추가 헤더를 설정하고 존재하지 않는 헤더를 request 설정한 채로 cors호출하면 실패하는지 확인
		{"test_cors_header_option", func(t *testing.T) {
			t.Skip("Java Cors.testCorsHeaderOption is @Disabled")
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, tc.run)
	}
}

type corsCheck struct {
	method, key, origin, requestMethod        string
	status                                    int
	allowOrigin, allowMethods, requestHeaders string
}

func putCors(t *testing.T, s *suite, bucket string, rules []types.CORSRule) {
	t.Helper()
	_, err := s.client.PutBucketCors(context.Background(), &s3.PutBucketCorsInput{Bucket: aws.String(bucket), CORSConfiguration: &types.CORSConfiguration{CORSRules: rules}})
	if err != nil {
		t.Fatal(err)
	}
}
func assertNoCors(t *testing.T, s *suite, bucket string) {
	t.Helper()
	_, err := s.client.GetBucketCors(context.Background(), &s3.GetBucketCorsInput{Bucket: aws.String(bucket)})
	assertS3Error(t, err, 404, "NoSuchCORSConfiguration")
}
func publicBucket(t *testing.T, s *suite) string {
	t.Helper()
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter)
	_, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String(bucket), ACL: types.BucketCannedACLPublicRead})
	if err != nil {
		t.Fatal(err)
	}
	return bucket
}
func checkCors(t *testing.T, s *suite, bucket string, c corsCheck) {
	t.Helper()
	url := strings.TrimRight(s.cfg.Endpoint(), "/") + "/" + bucket
	if s.cfg.Endpoint() == "" {
		url = "https://" + bucket + ".s3." + s.cfg.Region + ".amazonaws.com"
	}
	if c.key != "" {
		url += "/" + c.key
	}
	req, _ := http.NewRequest(c.method, url, nil)
	if c.origin != "" {
		req.Header.Set("Origin", c.origin)
	}
	if c.requestMethod != "" {
		req.Header.Set("Access-Control-Request-Method", c.requestMethod)
	}
	if c.requestHeaders != "" {
		req.Header.Set("Access-Control-Request-Headers", c.requestHeaders)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != c.status || resp.Header.Get("Access-Control-Allow-Origin") != c.allowOrigin || resp.Header.Get("Access-Control-Allow-Methods") != c.allowMethods {
		t.Fatalf("%s %s: status/origin/methods = %d/%q/%q, want %d/%q/%q", c.method, c.key, resp.StatusCode, resp.Header.Get("Access-Control-Allow-Origin"), resp.Header.Get("Access-Control-Allow-Methods"), c.status, c.allowOrigin, c.allowMethods)
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
