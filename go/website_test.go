package s3tests

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// 버킷의 Website 설정 조회 확인
func TestWebsiteGetBuckets(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 1)
	_, err := s.client.GetBucketWebsite(context.Background(), &s3.GetBucketWebsiteInput{Bucket: aws.String(bucket)})
	assertS3Error(t, err, 404, "NoSuchWebsiteConfiguration")
}

// 버킷의 Website 설정이 가능한지 확인
func TestWebsitePutBuckets(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 2)
	want := websiteConfiguration()
	putWebsite(t, s, bucket, want)
	out, err := s.client.GetBucketWebsite(context.Background(), &s3.GetBucketWebsiteInput{Bucket: aws.String(bucket)})
	if err != nil {
		t.Fatalf("GetBucketWebsite: %v", err)
	}
	if out.ErrorDocument == nil || aws.ToString(out.ErrorDocument.Key) != aws.ToString(want.ErrorDocument.Key) || out.IndexDocument == nil || aws.ToString(out.IndexDocument.Suffix) != aws.ToString(want.IndexDocument.Suffix) {
		t.Fatalf("website configuration = %#v, want %#v", out, want)
	}
}

// 버킷의 Website 설정이 삭제가능한지 확인
func TestWebsiteDeleteBuckets(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 3)
	putWebsite(t, s, bucket, websiteConfiguration())
	if _, err := s.client.DeleteBucketWebsite(context.Background(), &s3.DeleteBucketWebsiteInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("DeleteBucketWebsite: %v", err)
	}
	_, err := s.client.GetBucketWebsite(context.Background(), &s3.GetBucketWebsiteInput{Bucket: aws.String(bucket)})
	assertS3Error(t, err, 404, "NoSuchWebsiteConfiguration")
}

// Website 설정 삭제 후 조회하면 NoSuchWebsiteConfiguration 에러가 발생하는지 확인
func TestWebsiteGetAfterDelete(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 4)
	putWebsite(t, s, bucket, &types.WebsiteConfiguration{
		ErrorDocument: &types.ErrorDocument{Key: aws.String("error.html")},
		IndexDocument: &types.IndexDocument{Suffix: aws.String("index.html")},
	})
	getWebsite(t, s, bucket)
	if _, err := s.client.DeleteBucketWebsite(context.Background(), &s3.DeleteBucketWebsiteInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("DeleteBucketWebsite: %v", err)
	}
	_, err := s.client.GetBucketWebsite(context.Background(), &s3.GetBucketWebsiteInput{Bucket: aws.String(bucket)})
	assertS3Error(t, err, 404, "NoSuchWebsiteConfiguration")
}

// Website 설정이 없는 버킷에서 삭제해도 성공하는지 확인
func TestWebsiteDeleteNotConfigured(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 5)
	if _, err := s.client.DeleteBucketWebsite(context.Background(), &s3.DeleteBucketWebsiteInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("DeleteBucketWebsite: %v", err)
	}
}

// IndexDocument만으로 Website 설정이 가능한지 확인
func TestWebsiteIndexDocumentOnly(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 6)
	putWebsite(t, s, bucket, &types.WebsiteConfiguration{IndexDocument: &types.IndexDocument{Suffix: aws.String("index.html")}})

	out := getWebsite(t, s, bucket)
	if out.IndexDocument == nil || aws.ToString(out.IndexDocument.Suffix) != "index.html" {
		t.Fatalf("IndexDocument = %#v, want index.html", out.IndexDocument)
	}
	if out.ErrorDocument != nil || out.RedirectAllRequestsTo != nil || len(out.RoutingRules) != 0 {
		t.Fatalf("ErrorDocument=%#v RedirectAllRequestsTo=%#v RoutingRules=%#v, want empty", out.ErrorDocument, out.RedirectAllRequestsTo, out.RoutingRules)
	}
}

// Website 설정을 덮어쓰면 기존 설정이 완전히 대체되는지 확인
func TestWebsiteOverwrite(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 7)
	putWebsite(t, s, bucket, &types.WebsiteConfiguration{
		IndexDocument: &types.IndexDocument{Suffix: aws.String("index.html")},
		ErrorDocument: &types.ErrorDocument{Key: aws.String("error.html")},
	})
	putWebsite(t, s, bucket, &types.WebsiteConfiguration{IndexDocument: &types.IndexDocument{Suffix: aws.String("main.html")}})

	out := getWebsite(t, s, bucket)
	if out.IndexDocument == nil || aws.ToString(out.IndexDocument.Suffix) != "main.html" {
		t.Fatalf("IndexDocument = %#v, want main.html", out.IndexDocument)
	}
	if out.ErrorDocument != nil {
		t.Fatalf("ErrorDocument = %#v, want nil", out.ErrorDocument)
	}
}

// RedirectAllRequestsTo(호스트, 프로토콜) 설정이 가능한지 확인
func TestWebsiteRedirectAllRequests(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 8)
	putWebsite(t, s, bucket, &types.WebsiteConfiguration{
		RedirectAllRequestsTo: &types.RedirectAllRequestsTo{HostName: aws.String("example.com"), Protocol: types.ProtocolHttps},
	})

	out := getWebsite(t, s, bucket)
	if out.RedirectAllRequestsTo == nil || aws.ToString(out.RedirectAllRequestsTo.HostName) != "example.com" || out.RedirectAllRequestsTo.Protocol != types.ProtocolHttps {
		t.Fatalf("RedirectAllRequestsTo = %#v, want example.com/https", out.RedirectAllRequestsTo)
	}
	if out.IndexDocument != nil || out.ErrorDocument != nil {
		t.Fatalf("IndexDocument=%#v ErrorDocument=%#v, want nil", out.IndexDocument, out.ErrorDocument)
	}
}

// RedirectAllRequestsTo에 호스트만 설정이 가능한지 확인
func TestWebsiteRedirectAllRequestsHostOnly(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 9)
	putWebsite(t, s, bucket, &types.WebsiteConfiguration{
		RedirectAllRequestsTo: &types.RedirectAllRequestsTo{HostName: aws.String("example.com")},
	})

	out := getWebsite(t, s, bucket)
	if out.RedirectAllRequestsTo == nil || aws.ToString(out.RedirectAllRequestsTo.HostName) != "example.com" || out.RedirectAllRequestsTo.Protocol != "" {
		t.Fatalf("RedirectAllRequestsTo = %#v, want example.com without protocol", out.RedirectAllRequestsTo)
	}
}

// RoutingRules 설정이 가능하고 조회 결과가 일치하는지 확인
func TestWebsiteRoutingRules(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 10)
	rules := []types.RoutingRule{
		{
			Condition: &types.Condition{KeyPrefixEquals: aws.String("docs/")},
			Redirect:  &types.Redirect{ReplaceKeyPrefixWith: aws.String("documents/")},
		},
		{
			Condition: &types.Condition{HttpErrorCodeReturnedEquals: aws.String("404")},
			Redirect: &types.Redirect{HostName: aws.String("example.com"), Protocol: types.ProtocolHttps,
				HttpRedirectCode: aws.String("301"), ReplaceKeyWith: aws.String("error.html")},
		},
		{
			Redirect: &types.Redirect{HostName: aws.String("example.com")},
		},
	}
	putWebsite(t, s, bucket, &types.WebsiteConfiguration{
		IndexDocument: &types.IndexDocument{Suffix: aws.String("index.html")},
		ErrorDocument: &types.ErrorDocument{Key: aws.String("error.html")},
		RoutingRules:  rules,
	})

	out := getWebsite(t, s, bucket)
	if out.IndexDocument == nil || aws.ToString(out.IndexDocument.Suffix) != "index.html" || out.ErrorDocument == nil || aws.ToString(out.ErrorDocument.Key) != "error.html" {
		t.Fatalf("IndexDocument=%#v ErrorDocument=%#v", out.IndexDocument, out.ErrorDocument)
	}
	assertRoutingRules(t, rules, out.RoutingRules)
}

// RoutingRules를 최대 개수(50개)까지 설정 가능한지 확인
func TestWebsiteRoutingRulesMax(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 11)
	rules := createRoutingRules(50)
	putWebsite(t, s, bucket, &types.WebsiteConfiguration{
		IndexDocument: &types.IndexDocument{Suffix: aws.String("index.html")},
		RoutingRules:  rules,
	})

	assertRoutingRules(t, rules, getWebsite(t, s, bucket).RoutingRules)
}

// RoutingRules가 최대 개수(50개)를 넘으면 실패하는지 확인
func TestWebsiteRoutingRulesOverMax(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 12)
	checkPutWebsiteFailed(t, s, bucket, &types.WebsiteConfiguration{
		IndexDocument: &types.IndexDocument{Suffix: aws.String("index.html")},
		RoutingRules:  createRoutingRules(51),
	}, "InvalidRequest")
}

// RedirectAllRequestsTo 없이 IndexDocument를 빠뜨리면 실패하는지 확인
func TestWebsiteNoIndexDocument(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 13)
	checkPutWebsiteFailed(t, s, bucket, &types.WebsiteConfiguration{
		ErrorDocument: &types.ErrorDocument{Key: aws.String("error.html")},
	}, "InvalidArgument")
}

// IndexDocument Suffix가 비어있으면 실패하는지 확인
func TestWebsiteEmptyIndexSuffix(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 14)
	checkPutWebsiteFailed(t, s, bucket, &types.WebsiteConfiguration{
		IndexDocument: &types.IndexDocument{Suffix: aws.String("")},
	}, "InvalidArgument")
}

// IndexDocument Suffix에 '/'가 포함되면 실패하는지 확인
func TestWebsiteIndexSuffixWithSlash(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 15)
	checkPutWebsiteFailed(t, s, bucket, &types.WebsiteConfiguration{
		IndexDocument: &types.IndexDocument{Suffix: aws.String("dir/index.html")},
	}, "InvalidArgument")
}

// RedirectAllRequestsTo와 IndexDocument를 함께 설정하면 실패하는지 확인
func TestWebsiteRedirectAllWithIndexDocument(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 16)
	checkPutWebsiteFailed(t, s, bucket, &types.WebsiteConfiguration{
		RedirectAllRequestsTo: &types.RedirectAllRequestsTo{HostName: aws.String("example.com")},
		IndexDocument:         &types.IndexDocument{Suffix: aws.String("index.html")},
	}, "InvalidArgument")
}

// RedirectAllRequestsTo와 RoutingRules를 함께 설정하면 실패하는지 확인
func TestWebsiteRedirectAllWithRoutingRules(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 17)
	checkPutWebsiteFailed(t, s, bucket, &types.WebsiteConfiguration{
		RedirectAllRequestsTo: &types.RedirectAllRequestsTo{HostName: aws.String("example.com")},
		RoutingRules:          createRoutingRules(1),
	}, "InvalidArgument")
}

// RedirectAllRequestsTo의 Protocol이 http/https가 아니면 실패하는지 확인
func TestWebsiteRedirectAllInvalidProtocol(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 18)
	checkPutWebsiteFailed(t, s, bucket, &types.WebsiteConfiguration{
		RedirectAllRequestsTo: &types.RedirectAllRequestsTo{HostName: aws.String("example.com"), Protocol: types.Protocol("ftp")},
	}, "InvalidRequest")
}

// RoutingRule에 ReplaceKeyWith와 ReplaceKeyPrefixWith를 함께 설정하면 실패하는지 확인
func TestWebsiteRoutingRuleReplaceKeyAndPrefix(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 19)
	checkPutWebsiteFailed(t, s, bucket, &types.WebsiteConfiguration{
		IndexDocument: &types.IndexDocument{Suffix: aws.String("index.html")},
		RoutingRules: []types.RoutingRule{{
			Condition: &types.Condition{KeyPrefixEquals: aws.String("docs/")},
			Redirect:  &types.Redirect{ReplaceKeyWith: aws.String("index.html"), ReplaceKeyPrefixWith: aws.String("documents/")},
		}},
	}, "InvalidRequest")
}

// RoutingRule의 HttpRedirectCode가 3XX가 아니면 실패하는지 확인
func TestWebsiteRoutingRuleInvalidRedirectCode(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 20)
	checkPutWebsiteFailed(t, s, bucket, &types.WebsiteConfiguration{
		IndexDocument: &types.IndexDocument{Suffix: aws.String("index.html")},
		RoutingRules: []types.RoutingRule{{
			Redirect: &types.Redirect{HostName: aws.String("example.com"), HttpRedirectCode: aws.String("200")},
		}},
	}, "InvalidRequest")
}

// RoutingRule 조건의 HttpErrorCodeReturnedEquals가 4XX/5XX가 아니면 실패하는지 확인
func TestWebsiteRoutingRuleInvalidErrorCode(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 21)
	checkPutWebsiteFailed(t, s, bucket, &types.WebsiteConfiguration{
		IndexDocument: &types.IndexDocument{Suffix: aws.String("index.html")},
		RoutingRules: []types.RoutingRule{{
			Condition: &types.Condition{HttpErrorCodeReturnedEquals: aws.String("200")},
			Redirect:  &types.Redirect{HostName: aws.String("example.com")},
		}},
	}, "InvalidRequest")
}

// RoutingRule의 Redirect가 비어있어도 설정이 가능한지 확인
func TestWebsiteRoutingRuleEmptyRedirect(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 22)
	rules := []types.RoutingRule{{
		Condition: &types.Condition{KeyPrefixEquals: aws.String("docs/")},
		Redirect:  &types.Redirect{},
	}}
	putWebsite(t, s, bucket, &types.WebsiteConfiguration{
		IndexDocument: &types.IndexDocument{Suffix: aws.String("index.html")},
		RoutingRules:  rules,
	})

	assertRoutingRules(t, rules, getWebsite(t, s, bucket).RoutingRules)
}

// 존재하지 않는 버킷에 Website 설정/조회/삭제 시 NoSuchBucket 에러가 발생하는지 확인
func TestWebsiteBucketNotFound(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := buildBucketName(s.cfg.BucketPrefix, s.suiteID, 23)
	ctx := context.Background()

	_, err := s.client.PutBucketWebsite(ctx, &s3.PutBucketWebsiteInput{Bucket: aws.String(bucket),
		WebsiteConfiguration: &types.WebsiteConfiguration{IndexDocument: &types.IndexDocument{Suffix: aws.String("index.html")}}})
	assertS3Error(t, err, 404, "NoSuchBucket")

	_, err = s.client.GetBucketWebsite(ctx, &s3.GetBucketWebsiteInput{Bucket: aws.String(bucket)})
	assertS3Error(t, err, 404, "NoSuchBucket")

	_, err = s.client.DeleteBucketWebsite(ctx, &s3.DeleteBucketWebsiteInput{Bucket: aws.String(bucket)})
	assertS3Error(t, err, 404, "NoSuchBucket")
}

// 다른 사용자가 Website 설정/조회/삭제할 수 없는지 확인
func TestWebsiteAccessDenied(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket := s.bucket(t, 24)
	alt := s3Client(s.cfg, s.cfg.Alt)
	ctx := context.Background()
	configuration := &types.WebsiteConfiguration{IndexDocument: &types.IndexDocument{Suffix: aws.String("index.html")}}

	_, err := alt.PutBucketWebsite(ctx, &s3.PutBucketWebsiteInput{Bucket: aws.String(bucket), WebsiteConfiguration: configuration})
	assertS3Error(t, err, 403, "AccessDenied")

	putWebsite(t, s, bucket, configuration)

	_, err = alt.GetBucketWebsite(ctx, &s3.GetBucketWebsiteInput{Bucket: aws.String(bucket)})
	assertS3Error(t, err, 403, "AccessDenied")

	_, err = alt.DeleteBucketWebsite(ctx, &s3.DeleteBucketWebsiteInput{Bucket: aws.String(bucket)})
	assertS3Error(t, err, 403, "AccessDenied")

	out := getWebsite(t, s, bucket)
	if out.IndexDocument == nil || aws.ToString(out.IndexDocument.Suffix) != "index.html" {
		t.Fatalf("IndexDocument = %#v, want index.html", out.IndexDocument)
	}
}

// 웹사이트 엔드포인트에서 루트와 디렉터리 요청에 IndexDocument를 돌려주는지 확인
func TestWebsiteAccessIndexDocument(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	skipWithoutWebsiteURL(t, s)
	bucket := createPublicWebsiteBucket(t, s, 25, &types.WebsiteConfiguration{IndexDocument: &types.IndexDocument{Suffix: aws.String("index.html")}})
	putHTML(t, s, bucket, "index.html", "root index")
	putHTML(t, s, bucket, "dir/index.html", "dir index")

	assertWebsiteBody(t, websiteGet(t, s, bucket, ""), http.StatusOK, "root index")
	assertWebsiteBody(t, websiteGet(t, s, bucket, "dir/"), http.StatusOK, "dir index")
	assertWebsiteBody(t, websiteGet(t, s, bucket, "index.html"), http.StatusOK, "root index")

	// 슬래시 없이 디렉터리를 요청하면 슬래시를 붙인 주소로 보낸다
	assertWebsiteRedirect(t, websiteGet(t, s, bucket, "dir"), http.StatusFound, "/dir/")
}

// 없는 객체를 요청하면 ErrorDocument 내용을 404로 돌려주는지 확인
func TestWebsiteAccessErrorDocument(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	skipWithoutWebsiteURL(t, s)
	bucket := createPublicWebsiteBucket(t, s, 26, &types.WebsiteConfiguration{
		IndexDocument: &types.IndexDocument{Suffix: aws.String("index.html")},
		ErrorDocument: &types.ErrorDocument{Key: aws.String("error.html")},
	})
	putHTML(t, s, bucket, "error.html", "custom error")

	assertWebsiteBody(t, websiteGet(t, s, bucket, "not-exist.html"), http.StatusNotFound, "custom error")
}

// ErrorDocument가 없을 때 없는 객체를 요청하면 기본 404 NoSuchKey 페이지를 돌려주는지 확인
func TestWebsiteAccessNoErrorDocument(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	skipWithoutWebsiteURL(t, s)
	bucket := createPublicWebsiteBucket(t, s, 27, &types.WebsiteConfiguration{IndexDocument: &types.IndexDocument{Suffix: aws.String("index.html")}})

	assertWebsiteBodyContains(t, websiteGet(t, s, bucket, "not-exist.html"), http.StatusNotFound, "NoSuchKey")
}

// RedirectAllRequestsTo 설정 시 모든 요청을 지정한 호스트로 301 리다이렉트하는지 확인
func TestWebsiteAccessRedirectAllRequests(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	skipWithoutWebsiteURL(t, s)
	bucket := createPublicWebsiteBucket(t, s, 28, &types.WebsiteConfiguration{
		RedirectAllRequestsTo: &types.RedirectAllRequestsTo{HostName: aws.String("example.com"), Protocol: types.ProtocolHttps},
	})

	assertWebsiteRedirect(t, websiteGet(t, s, bucket, "docs/a.html"), http.StatusMovedPermanently, "https://example.com/docs/a.html")
}

// KeyPrefixEquals 라우팅 규칙으로 접두어를 바꿔 리다이렉트하는지 확인
func TestWebsiteAccessRoutingRuleKeyPrefix(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	skipWithoutWebsiteURL(t, s)
	bucket := createPublicWebsiteBucket(t, s, 29, &types.WebsiteConfiguration{
		IndexDocument: &types.IndexDocument{Suffix: aws.String("index.html")},
		RoutingRules: []types.RoutingRule{{
			Condition: &types.Condition{KeyPrefixEquals: aws.String("docs/")},
			Redirect:  &types.Redirect{ReplaceKeyPrefixWith: aws.String("documents/")},
		}},
	})

	response := websiteGet(t, s, bucket, "docs/a.html")
	if response.statusCode != http.StatusMovedPermanently {
		t.Fatalf("status = %d, want %d (body=%q)", response.statusCode, http.StatusMovedPermanently, response.body)
	}
	// 호스트는 엔드포인트마다 달라지므로 경로만 확인한다
	if !strings.HasSuffix(response.location, "/documents/a.html") {
		t.Fatalf("Location = %q, want suffix /documents/a.html", response.location)
	}
}

// HttpErrorCodeReturnedEquals 라우팅 규칙으로 404일 때만 리다이렉트하는지 확인
func TestWebsiteAccessRoutingRuleErrorCode(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	skipWithoutWebsiteURL(t, s)
	bucket := createPublicWebsiteBucket(t, s, 30, &types.WebsiteConfiguration{
		IndexDocument: &types.IndexDocument{Suffix: aws.String("index.html")},
		RoutingRules: []types.RoutingRule{{
			Condition: &types.Condition{HttpErrorCodeReturnedEquals: aws.String("404")},
			Redirect: &types.Redirect{HostName: aws.String("example.com"), Protocol: types.ProtocolHttps,
				HttpRedirectCode: aws.String("302"), ReplaceKeyWith: aws.String("error.html")},
		}},
	})
	putHTML(t, s, bucket, "exist.html", "exist")

	assertWebsiteBody(t, websiteGet(t, s, bucket, "exist.html"), http.StatusOK, "exist")
	assertWebsiteRedirect(t, websiteGet(t, s, bucket, "not-exist.html"), http.StatusFound, "https://example.com/error.html")
}

// 객체의 x-amz-website-redirect-location 메타데이터로 리다이렉트하는지 확인
func TestWebsiteAccessObjectRedirectLocation(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	skipWithoutWebsiteURL(t, s)
	bucket := createPublicWebsiteBucket(t, s, 31, &types.WebsiteConfiguration{IndexDocument: &types.IndexDocument{Suffix: aws.String("index.html")}})
	_, err := s.client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String("old.html"),
		WebsiteRedirectLocation: aws.String("/new.html"), Body: strings.NewReader("")})
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}

	assertWebsiteRedirect(t, websiteGet(t, s, bucket, "old.html"), http.StatusMovedPermanently, "/new.html")
}

// 공개되지 않은 버킷을 웹사이트로 접근하면 403 AccessDenied가 반환되는지 확인
func TestWebsiteAccessNotPublic(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	skipWithoutWebsiteURL(t, s)
	bucket := s.bucket(t, 32)
	putWebsite(t, s, bucket, &types.WebsiteConfiguration{IndexDocument: &types.IndexDocument{Suffix: aws.String("index.html")}})
	putHTML(t, s, bucket, "index.html", "root index")

	assertWebsiteBodyContains(t, websiteGet(t, s, bucket, "index.html"), http.StatusForbidden, "AccessDenied")
}

// 웹사이트 설정이 없는 버킷에 접근하면 404 NoSuchWebsiteConfiguration이 반환되는지 확인
func TestWebsiteAccessNoConfiguration(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	skipWithoutWebsiteURL(t, s)
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, 33)
	setPublicReadPolicy(t, s, bucket)
	putHTML(t, s, bucket, "index.html", "root index")

	assertWebsiteBodyContains(t, websiteGet(t, s, bucket, "index.html"), http.StatusNotFound, "NoSuchWebsiteConfiguration")
}

// 웹사이트 엔드포인트 응답
type websiteResponse struct {
	statusCode int
	location   string
	body       string
}

// 설정에 WebsiteURL이 없으면 웹사이트 접속 테스트를 건너뛴다
func skipWithoutWebsiteURL(t *testing.T, s *suite) {
	t.Helper()
	if !s.cfg.HasWebsiteURL() {
		t.Skip("설정의 [S3] WebsiteURL을 지정하세요")
	}
}

// 설정의 WebsiteURL 템플릿에 버킷명을 넣어 웹사이트 주소를 만든다
func websiteURL(s *suite, bucket, path string) string {
	baseURL := strings.ReplaceAll(s.cfg.WebsiteURL, "{bucket}", bucket)
	return strings.TrimSuffix(baseURL, "/") + "/" + path
}

// 서명 없이 웹사이트 엔드포인트에 GET 요청을 보낸다. 리다이렉트를 검증하기 위해 따라가지 않는다
func websiteGet(t *testing.T, s *suite, bucket, path string) websiteResponse {
	t.Helper()
	client := insecureHTTPClient()
	client.Timeout = 15 * time.Second
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

	response, err := client.Get(websiteURL(s, bucket, path))
	if err != nil {
		t.Fatalf("website GET %q: %v", path, err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("website GET %q 본문 읽기: %v", path, err)
	}
	return websiteResponse{statusCode: response.StatusCode, location: response.Header.Get("Location"), body: string(body)}
}

func assertWebsiteBody(t *testing.T, response websiteResponse, status int, body string) {
	t.Helper()
	if response.statusCode != status || response.body != body {
		t.Fatalf("status/body = %d/%q, want %d/%q", response.statusCode, response.body, status, body)
	}
}

func assertWebsiteBodyContains(t *testing.T, response websiteResponse, status int, text string) {
	t.Helper()
	if response.statusCode != status || !strings.Contains(response.body, text) {
		t.Fatalf("status/body = %d/%q, want %d containing %q", response.statusCode, response.body, status, text)
	}
}

func assertWebsiteRedirect(t *testing.T, response websiteResponse, status int, location string) {
	t.Helper()
	if response.statusCode != status || response.location != location {
		t.Fatalf("status/Location = %d/%q, want %d/%q", response.statusCode, response.location, status, location)
	}
}

// 익명 사용자가 버킷의 모든 객체를 읽을 수 있도록 정책을 건다
func setPublicReadPolicy(t *testing.T, s *suite, bucket string) {
	t.Helper()
	statement := map[string]any{"Effect": "Allow", "Principal": map[string]any{"AWS": "*"}, "Action": "s3:GetObject", "Resource": bucketARN(bucket) + "/*"}
	policy, _ := json.Marshal(map[string]any{"Version": "2012-10-17", "Statement": []any{statement}})
	if _, err := s.client.PutBucketPolicy(context.Background(), &s3.PutBucketPolicyInput{Bucket: aws.String(bucket), Policy: aws.String(string(policy))}); err != nil {
		t.Fatalf("PutBucketPolicy: %v", err)
	}
}

// 익명 읽기를 허용하고 웹사이트 설정을 건 버킷을 만든다
func createPublicWebsiteBucket(t *testing.T, s *suite, testID int, configuration *types.WebsiteConfiguration) string {
	t.Helper()
	bucket := ownershipBucket(t, s, types.ObjectOwnershipObjectWriter, testID)
	setPublicReadPolicy(t, s, bucket)
	putWebsite(t, s, bucket, configuration)
	return bucket
}

func putHTML(t *testing.T, s *suite, bucket, key, body string) {
	t.Helper()
	_, err := s.client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key),
		ContentType: aws.String("text/html"), Body: strings.NewReader(body)})
	if err != nil {
		t.Fatalf("PutObject %s: %v", key, err)
	}
}

func websiteConfiguration() *types.WebsiteConfiguration {
	return &types.WebsiteConfiguration{ErrorDocument: &types.ErrorDocument{Key: aws.String("HttpStatus.SC_BAD_REQUEST")}, IndexDocument: &types.IndexDocument{Suffix: aws.String("a")}}
}

func putWebsite(t *testing.T, s *suite, bucket string, configuration *types.WebsiteConfiguration) {
	t.Helper()
	_, err := s.client.PutBucketWebsite(context.Background(), &s3.PutBucketWebsiteInput{Bucket: aws.String(bucket), WebsiteConfiguration: configuration})
	if err != nil {
		t.Fatalf("PutBucketWebsite: %v", err)
	}
}

func getWebsite(t *testing.T, s *suite, bucket string) *s3.GetBucketWebsiteOutput {
	t.Helper()
	out, err := s.client.GetBucketWebsite(context.Background(), &s3.GetBucketWebsiteInput{Bucket: aws.String(bucket)})
	if err != nil {
		t.Fatalf("GetBucketWebsite: %v", err)
	}
	return out
}

func createRoutingRules(count int) []types.RoutingRule {
	rules := make([]types.RoutingRule, 0, count)
	for i := 0; i < count; i++ {
		rules = append(rules, types.RoutingRule{
			Condition: &types.Condition{KeyPrefixEquals: aws.String(fmt.Sprintf("prefix%d/", i))},
			Redirect:  &types.Redirect{ReplaceKeyPrefixWith: aws.String(fmt.Sprintf("target%d/", i))},
		})
	}
	return rules
}

func assertRoutingRules(t *testing.T, want, got []types.RoutingRule) {
	t.Helper()
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("RoutingRules = %#v, want %#v", got, want)
	}
}

// PutBucketWebsite가 400으로 실패하고 설정이 남지 않았는지 확인
func checkPutWebsiteFailed(t *testing.T, s *suite, bucket string, configuration *types.WebsiteConfiguration, code string) {
	t.Helper()
	ctx := context.Background()
	_, err := s.client.PutBucketWebsite(ctx, &s3.PutBucketWebsiteInput{Bucket: aws.String(bucket), WebsiteConfiguration: configuration})
	assertS3Error(t, err, 400, code)

	_, err = s.client.GetBucketWebsite(ctx, &s3.GetBucketWebsiteInput{Bucket: aws.String(bucket)})
	assertS3Error(t, err, 404, "NoSuchWebsiteConfiguration")
}
