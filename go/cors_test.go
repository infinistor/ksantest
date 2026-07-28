package s3tests

// import (
// 	"context"
// 	"io"
// 	"net/http"
// 	"strings"
// 	"testing"

// 	"github.com/aws/aws-sdk-go-v2/aws"
// 	"github.com/aws/aws-sdk-go-v2/service/s3"
// 	"github.com/aws/aws-sdk-go-v2/service/s3/types"
// )

// func TestSetCors(t *testing.T) {
// 	t.Parallel()

// 	s := newSuite(t)
// 	bucket := s.bucket(t)
// 	assertNoCors(t, s, bucket)
// 	putCors(t, s, bucket, []types.CORSRule{{AllowedMethods: []string{"GET", "PUT"}, AllowedOrigins: []string{"*.get", "*.put"}}})
// 	out, err := s.client.GetBucketCors(context.Background(), &s3.GetBucketCorsInput{Bucket: aws.String(bucket)})
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	if len(out.CORSRules) != 1 || !equalStrings(out.CORSRules[0].AllowedMethods, []string{"GET", "PUT"}) || !equalStrings(out.CORSRules[0].AllowedOrigins, []string{"*.get", "*.put"}) {
// 		t.Fatalf("CORS rules = %#v", out.CORSRules)
// 	}
// 	if _, err := s.client.DeleteBucketCors(context.Background(), &s3.DeleteBucketCorsInput{Bucket: aws.String(bucket)}); err != nil {
// 		t.Fatal(err)
// 	}
// 	assertNoCors(t, s, bucket)
// }

// func TestCorsOriginResponse(t *testing.T) {
// 	t.Parallel()
// 	s := newSuite(t)
// 	bucket := publicBucket(t, s)
// 	assertNoCors(t, s, bucket)
// 	putCors(t, s, bucket, []types.CORSRule{
// 		{AllowedMethods: []string{"GET"}, AllowedOrigins: []string{"*suffix"}},
// 		{AllowedMethods: []string{"GET"}, AllowedOrigins: []string{"start*end"}},
// 		{AllowedMethods: []string{"GET"}, AllowedOrigins: []string{"prefix*"}},
// 		{AllowedMethods: []string{"PUT"}, AllowedOrigins: []string{"*.put"}},
// 	})

// 	checkCors(t, s, "GET", bucket, "", nil, 200, "", "")
// 	checkCors(t, s, "GET", bucket, "", map[string]string{"Origin": "foo.suffix"}, 200, "foo.suffix", "GET")
// 	checkCors(t, s, "GET", bucket, "", map[string]string{"Origin": "foo.bar"}, 200, "", "")
// 	checkCors(t, s, "GET", bucket, "", map[string]string{"Origin": "foo.suffix.get"}, 200, "", "")
// 	checkCors(t, s, "GET", bucket, "", map[string]string{"Origin": "start_end"}, 200, "start_end", "GET")
// 	checkCors(t, s, "GET", bucket, "", map[string]string{"Origin": "start1end"}, 200, "start1end", "GET")
// 	checkCors(t, s, "GET", bucket, "", map[string]string{"Origin": "start12end"}, 200, "start12end", "GET")
// 	checkCors(t, s, "GET", bucket, "", map[string]string{"Origin": "0start12end"}, 200, "", "")
// 	checkCors(t, s, "GET", bucket, "", map[string]string{"Origin": "prefix"}, 200, "prefix", "GET")
// 	checkCors(t, s, "GET", bucket, "", map[string]string{"Origin": "prefix.suffix"}, 200, "prefix.suffix", "GET")
// 	checkCors(t, s, "GET", bucket, "", map[string]string{"Origin": "bla.prefix"}, 200, "", "")

// 	checkCors(t, s, "GET", bucket, "bar", map[string]string{"Origin": "foo.suffix"}, 404, "foo.suffix", "GET")
// 	checkCors(t, s, "PUT", bucket, "bar", map[string]string{
// 		"Origin":                        "foo.suffix",
// 		"Access-Control-Request-Method": "GET",
// 		"content-length":                "0",
// 	}, 403, "foo.suffix", "GET")
// 	checkCors(t, s, "PUT", bucket, "bar", map[string]string{
// 		"Origin":                        "foo.suffix",
// 		"Access-Control-Request-Method": "PUT",
// 		"content-length":                "0",
// 	}, 403, "", "")
// 	checkCors(t, s, "PUT", bucket, "bar", map[string]string{
// 		"Origin":                        "foo.suffix",
// 		"Access-Control-Request-Method": "DELETE",
// 		"content-length":                "0",
// 	}, 403, "", "")
// 	checkCors(t, s, "PUT", bucket, "bar", map[string]string{
// 		"Origin":         "foo.suffix",
// 		"content-length": "0",
// 	}, 403, "", "")
// 	checkCors(t, s, "PUT", bucket, "bar", map[string]string{
// 		"Origin":         "foo.put",
// 		"content-length": "0",
// 	}, 403, "foo.put", "PUT")
// 	checkCors(t, s, "GET", bucket, "bar", map[string]string{"Origin": "foo.suffix"}, 404, "foo.suffix", "GET")

// 	checkCors(t, s, "OPTIONS", bucket, "", nil, 400, "", "")
// 	checkCors(t, s, "OPTIONS", bucket, "", map[string]string{"Origin": "foo.suffix"}, 403, "", "")
// 	checkCors(t, s, "OPTIONS", bucket, "", map[string]string{"Origin": "foo.bla"}, 403, "", "")
// 	checkCors(t, s, "OPTIONS", bucket, "bar", map[string]string{
// 		"Origin":                        "foo.suffix",
// 		"Access-Control-Request-Method": "GET",
// 		"content-length":                "0",
// 	}, 200, "foo.suffix", "GET")
// 	checkCors(t, s, "OPTIONS", bucket, "", map[string]string{
// 		"Origin":                        "foo.bar",
// 		"Access-Control-Request-Method": "GET",
// 	}, 403, "", "")
// 	checkCors(t, s, "OPTIONS", bucket, "", map[string]string{
// 		"Origin":                        "foo.suffix.get",
// 		"Access-Control-Request-Method": "GET",
// 	}, 403, "", "")
// 	checkCors(t, s, "OPTIONS", bucket, "", map[string]string{
// 		"Origin":                        "start_end",
// 		"Access-Control-Request-Method": "GET",
// 	}, 200, "start_end", "GET")
// 	checkCors(t, s, "OPTIONS", bucket, "", map[string]string{
// 		"Origin":                        "start1end",
// 		"Access-Control-Request-Method": "GET",
// 	}, 200, "start1end", "GET")
// 	checkCors(t, s, "OPTIONS", bucket, "", map[string]string{
// 		"Origin":                        "start12end",
// 		"Access-Control-Request-Method": "GET",
// 	}, 200, "start12end", "GET")
// 	checkCors(t, s, "OPTIONS", bucket, "", map[string]string{
// 		"Origin":                        "0start12end",
// 		"Access-Control-Request-Method": "GET",
// 	}, 403, "", "")
// 	checkCors(t, s, "OPTIONS", bucket, "", map[string]string{
// 		"Origin":                        "prefix",
// 		"Access-Control-Request-Method": "GET",
// 	}, 200, "prefix", "GET")
// 	checkCors(t, s, "OPTIONS", bucket, "", map[string]string{
// 		"Origin":                        "prefix.suffix",
// 		"Access-Control-Request-Method": "GET",
// 	}, 200, "prefix.suffix", "GET")
// 	checkCors(t, s, "OPTIONS", bucket, "", map[string]string{
// 		"Origin":                        "bla.prefix",
// 		"Access-Control-Request-Method": "GET",
// 	}, 403, "", "")
// 	checkCors(t, s, "OPTIONS", bucket, "", map[string]string{
// 		"Origin":                        "foo.put",
// 		"Access-Control-Request-Method": "GET",
// 	}, 403, "", "")
// 	checkCors(t, s, "OPTIONS", bucket, "", map[string]string{
// 		"Origin":                        "foo.put",
// 		"Access-Control-Request-Method": "PUT",
// 	}, 200, "foo.put", "PUT")
// }

// func TestCorsOriginWildcard(t *testing.T) {
// 	t.Parallel()
// 	s := newSuite(t)
// 	bucket := publicBucket(t, s)
// 	assertNoCors(t, s, bucket)
// 	putCors(t, s, bucket, []types.CORSRule{{AllowedMethods: []string{"GET"}, AllowedOrigins: []string{"*"}}})

// 	checkCors(t, s, "GET", bucket, "", nil, 200, "", "")
// 	checkCors(t, s, "GET", bucket, "", map[string]string{"Origin": "example.origin"}, 200, "*", "GET")
// }

// func TestCorsHeaderOption(t *testing.T) {
// 	t.Parallel()
// 	s := newSuite(t)
// 	bucket := publicBucket(t, s)
// 	assertNoCors(t, s, bucket)
// 	putCors(t, s, bucket, []types.CORSRule{{
// 		AllowedMethods: []string{"GET"},
// 		AllowedOrigins: []string{"*"},
// 		ExposeHeaders:  []string{"x-amz-meta-header1"},
// 	}})

// 	checkCors(t, s, "OPTIONS", bucket, "bar", map[string]string{
// 		"Origin":                         "example.origin",
// 		"Access-Control-Request-Headers": "x-amz-meta-header2",
// 		"Access-Control-Request-Method":  "GET",
// 	}, 403, "", "")
// }

// func putCors(t *testing.T, s *suite, bucket string, rules []types.CORSRule) {
// 	t.Helper()
// 	_, err := s.client.PutBucketCors(context.Background(), &s3.PutBucketCorsInput{
// 		Bucket:            aws.String(bucket),
// 		CORSConfiguration: &types.CORSConfiguration{CORSRules: rules},
// 	})
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// }

// func assertNoCors(t *testing.T, s *suite, bucket string) {
// 	t.Helper()
// 	_, err := s.client.GetBucketCors(context.Background(), &s3.GetBucketCorsInput{Bucket: aws.String(bucket)})
// 	assertS3Error(t, err, 404, "NoSuchCORSConfiguration")
// }

// func publicBucket(t *testing.T, s *suite) string {
// 	t.Helper()
// 	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter)
// 	_, err := s.client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{
// 		Bucket: aws.String(bucket),
// 		ACL:    types.BucketCannedACLPublicRead,
// 	})
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	return bucket
// }

// func checkCors(t *testing.T, s *suite, method, bucket, key string, headers map[string]string, status int, allowOrigin, allowMethods string) {
// 	t.Helper()
// 	url := strings.TrimRight(s.cfg.Endpoint(), "/") + "/" + bucket
// 	if s.cfg.Endpoint() == "" {
// 		url = "https://" + bucket + ".s3." + s.cfg.Region + ".amazonaws.com"
// 	}
// 	if key != "" {
// 		url += "/" + key
// 	}
// 	req, err := http.NewRequest(method, url, nil)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	for k, v := range headers {
// 		req.Header.Set(k, v)
// 	}
// 	resp, err := http.DefaultClient.Do(req)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	defer resp.Body.Close()
// 	io.Copy(io.Discard, resp.Body)
// 	gotOrigin := resp.Header.Get("Access-Control-Allow-Origin")
// 	gotMethods := resp.Header.Get("Access-Control-Allow-Methods")
// 	if resp.StatusCode != status || gotOrigin != allowOrigin || gotMethods != allowMethods {
// 		t.Fatalf("%s %s: status/origin/methods = %d/%q/%q, want %d/%q/%q",
// 			method, key, resp.StatusCode, gotOrigin, gotMethods, status, allowOrigin, allowMethods)
// 	}
// }

// func equalStrings(a, b []string) bool {
// 	if len(a) != len(b) {
// 		return false
// 	}
// 	for i := range a {
// 		if a[i] != b[i] {
// 			return false
// 		}
// 	}
// 	return true
// }
