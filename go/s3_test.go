package s3tests

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"ksantest/go-s3tests/internal/testconfig"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	smithyauth "github.com/aws/smithy-go/auth"
	smithyendpoints "github.com/aws/smithy-go/endpoints"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

type suite struct {
	client *s3.Client
	cfg    testconfig.Config
}

func newSuite(t *testing.T) *suite {
	t.Helper()
	path := os.Getenv("S3TESTS_INI")
	if path == "" {
		path = "awstests.ini"
		// path = "11.151.ini"
		// path = "config.ini"
	}
	if !filepath.IsAbs(path) {
		path = filepath.Clean(path)
	}
	cfg, err := testconfig.Load(path)
	if err != nil {
		t.Fatalf("설정 읽기: %v", err)
	}
	if cfg.Main.AccessKey == "" || cfg.Main.SecretKey == "" {
		t.Skip("config.ini의 Main User 자격 증명을 설정하세요")
	}
	if cfg.SignatureVersion == 2 {
		t.Skip("AWS SDK for Go v2는 SigV4만 지원합니다")
	}
	opt := s3.Options{Region: cfg.Region, Credentials: aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(cfg.Main.AccessKey, cfg.Main.SecretKey, "")), UsePathStyle: cfg.Endpoint() != ""}
	if ep := cfg.Endpoint(); ep != "" {
		opt.BaseEndpoint = aws.String(ep)
	}
	applyCompatibleS3Options(&opt)
	return &suite{s3.New(opt), cfg}
}

// applyCompatibleS3Options는 Java TestBase.getClient 기본값
// (RequestChecksumCalculation/ResponseChecksumValidation WHEN_REQUIRED)을 따르고
// KSAN 호환 요청 와이어 재작성을 적용합니다.
func applyCompatibleS3Options(o *s3.Options) {
	o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
	o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
	ensureDisableDoubleEncoding(o)
	fixGetObjectAttributesWireFormat(o)
	lowercaseMetadataHeaders(o)
	// 인접 슬래시 축약은 KSAN SigV4 호환용입니다. AWS에서는
	// "/" 같은 객체 키가 깨집니다(경로가 CreateBucket 형태가 됨).
	if o.BaseEndpoint != nil && aws.ToString(o.BaseEndpoint) != "" {
		preserveOrCollapseAdjacentSlashes(o)
	}
}

// preserveOrCollapseAdjacentSlashes는 "//"가 포함된 요청 경로를 재작성합니다.
// KSAN의 SigV4 검증은 Java HTTP 클라이언트의 경로 정규화
// (인접 슬래시 축약)와 일치합니다. aws-sdk-go-v2는 Path/RawPath에 "//"를 유지하고
// 그 형태로 서명하므로, KSAN은 SignatureDoesNotMatch로 거부합니다.
func preserveOrCollapseAdjacentSlashes(o *s3.Options) {
	o.APIOptions = append(o.APIOptions, func(stack *middleware.Stack) error {
		return stack.Finalize.Add(middleware.FinalizeMiddlewareFunc("collapseAdjacentSlashes",
			func(ctx context.Context, in middleware.FinalizeInput, next middleware.FinalizeHandler) (
				out middleware.FinalizeOutput, metadata middleware.Metadata, err error,
			) {
				if req, ok := in.Request.(*smithyhttp.Request); ok {
					if strings.Contains(req.URL.Path, "//") {
						req.URL.Path = collapseAdjacentSlashes(req.URL.Path)
					}
					if req.URL.RawPath != "" && strings.Contains(req.URL.RawPath, "//") {
						req.URL.RawPath = collapseAdjacentSlashes(req.URL.RawPath)
					}
				}
				return next.HandleFinalize(ctx, in)
			}), middleware.Before)
	})
}

func collapseAdjacentSlashes(path string) string {
	for strings.Contains(path, "//") {
		path = strings.ReplaceAll(path, "//", "/")
	}
	return path
}

// ensureDisableDoubleEncoding은 커스텀 BaseEndpoint 클라이언트에 대해
// SigV4 DisableURIPathEscaping을 강제합니다(aws-sdk-go-v2#3349). 없으면
// 괄호 등 문자가 포함된 키가 이중 percent-encoding되어 KSAN이
// SignatureDoesNotMatch를 반환합니다.
func ensureDisableDoubleEncoding(o *s3.Options) {
	inner := o.EndpointResolverV2
	if inner == nil {
		inner = s3.NewDefaultEndpointResolverV2()
	}
	o.EndpointResolverV2 = disableDoubleEncodingResolver{inner: inner}
}

type disableDoubleEncodingResolver struct {
	inner s3.EndpointResolverV2
}

func (r disableDoubleEncodingResolver) ResolveEndpoint(ctx context.Context, params s3.EndpointParameters) (smithyendpoints.Endpoint, error) {
	out, err := r.inner.ResolveEndpoint(ctx, params)
	if err != nil {
		return out, err
	}
	opts, _ := smithyauth.GetAuthOptions(&out.Properties)
	if len(opts) == 0 {
		var sp smithy.Properties
		smithyhttp.SetDisableDoubleEncoding(&sp, true)
		smithyauth.SetAuthOptions(&out.Properties, []*smithyauth.Option{{
			SchemeID:         "aws.auth#sigv4",
			SignerProperties: sp,
		}})
		return out, nil
	}
	for _, opt := range opts {
		smithyhttp.SetDisableDoubleEncoding(&opt.SignerProperties, true)
	}
	smithyauth.SetAuthOptions(&out.Properties, opts)
	return out, nil
}

// fixGetObjectAttributesWireFormat는 Java AWS SDK v2 와이어 형식에 맞춥니다:
//
//  1. 헤더(AWS 필수): 다중 값 X-Amz-Object-Attributes를
//     하나의 쉼표 구분 헤더로 병합(aws-sdk-go-v2#1620). 이 헤더는 절대 제거하지 않음.
//
//  2. 쿼리 서브리소스: smithy Encode()는 "?attributes"를 "attributes="로 인코딩.
//     Java는 null 값을 저장하고 "="를 생략(bare "attributes"). SDK의 attributes= 형태로
//     서명한 뒤, HTTP 클라이언트(마지막 hop)에서 송신 URL을 bare "attributes"로
//     재작성하여 AWS/KSAN이 Java 형태를 보게 함.
func fixGetObjectAttributesWireFormat(o *s3.Options) {
	o.APIOptions = append(o.APIOptions, func(stack *middleware.Stack) error {
		return stack.Finalize.Add(middleware.FinalizeMiddlewareFunc("mergeObjectAttributesHeader",
			func(ctx context.Context, in middleware.FinalizeInput, next middleware.FinalizeHandler) (
				out middleware.FinalizeOutput, metadata middleware.Metadata, err error,
			) {
				if req, ok := in.Request.(*smithyhttp.Request); ok {
					mergeObjectAttributesHeader(req.Header)
				}
				return next.HandleFinalize(ctx, in)
			}), middleware.Before)
	})
	wrapBareAttributesHTTPClient(o)
}

const objectAttributesHeader = "X-Amz-Object-Attributes"

func wrapBareAttributesHTTPClient(o *s3.Options) {
	inner := o.HTTPClient
	o.HTTPClient = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL != nil {
			if rewritten := ensureBareAttributesQueryFlag(r.URL.RawQuery); rewritten != r.URL.RawQuery {
				r = r.Clone(r.Context())
				r.URL.RawQuery = rewritten
			}
		}
		if inner != nil {
			return inner.Do(r)
		}
		return http.DefaultClient.Do(r)
	})
}

func mergeObjectAttributesHeader(h http.Header) {
	values := h.Values(objectAttributesHeader)
	if len(values) == 0 {
		for k, vs := range h {
			if strings.EqualFold(k, objectAttributesHeader) {
				values = append(values, vs...)
			}
		}
	}
	if len(values) == 0 {
		return
	}
	for k := range h {
		if strings.EqualFold(k, objectAttributesHeader) {
			delete(h, k)
		}
	}
	h.Set(objectAttributesHeader, strings.Join(values, ","))
}

// ensureBareAttributesQueryFlag는 빈 attributes=를 bare 서브리소스 플래그
// "attributes"로 재작성합니다(Java SdkHttpUtils null-value 형태).
// attributes=<비어 있지 않음>은 그대로 둡니다. 키가 없으면 no-op입니다.
func ensureBareAttributesQueryFlag(raw string) string {
	if raw == "" {
		return raw
	}
	hasEmpty := false
	parts := make([]string, 0)
	for _, p := range strings.Split(raw, "&") {
		if p == "attributes" || p == "attributes=" {
			hasEmpty = true
			continue
		}
		if p != "" {
			parts = append(parts, p)
		}
	}
	if !hasEmpty {
		return raw
	}
	if len(parts) == 0 {
		return "attributes"
	}
	return "attributes&" + strings.Join(parts, "&")
}

// lowercaseMetadataHeaders는 SigV4 서명 전에 user-metadata 헤더를 재작성합니다.
//
// aws-sdk-go-v2는 Metadata를 http.CanonicalHeaderKey로 직렬화합니다
// (예: X-Amz-Meta-Meta1). Java AWS SDK v2는 locationName+key를 그대로 연결합니다
// (x-amz-meta-meta1). KSAN은 user-metadata 접두사를 대소문자 구분으로 매칭하므로
// Canonical 헤더는 무시되고 Content-Type 등 다른 필드는 정상 동작합니다.
//
// 근거: TestPost/test_post_object_user_specified_header (폼 필드
// x-amz-meta-foo)는 HeadObject 메타데이터 검사를 통과; PutObject/CopyObject/
// CreateMultipartUpload Metadata는 metadata=map[] / Metadata:nil로 실패.
//
// 또한 호출자가 이미 "x-amz-meta-"를 포함한 키를 넣을 때 이중 접두사를 축약합니다
// (Java MAP marshaller는 재접두사를 건너뛰고, Go는 항상 접두사를 붙임).
func lowercaseMetadataHeaders(o *s3.Options) {
	o.APIOptions = append(o.APIOptions, func(stack *middleware.Stack) error {
		return stack.Finalize.Add(middleware.FinalizeMiddlewareFunc("lowercaseMetadataHeaders",
			func(ctx context.Context, in middleware.FinalizeInput, next middleware.FinalizeHandler) (
				out middleware.FinalizeOutput, metadata middleware.Metadata, err error,
			) {
				if req, ok := in.Request.(*smithyhttp.Request); ok {
					normalizeOutgoingMetadataHeaders(req.Header)
				}
				return next.HandleFinalize(ctx, in)
			}), middleware.Before)
	})
}

const amzMetaPrefix = "x-amz-meta-"

func normalizeOutgoingMetadataHeaders(header http.Header) {
	type rewrite struct {
		oldKey string
		newKey string
		values []string
	}
	var pending []rewrite
	for key, values := range header {
		if len(key) < len(amzMetaPrefix) || !strings.EqualFold(key[:len(amzMetaPrefix)], amzMetaPrefix) {
			continue
		}
		metaKey := key[len(amzMetaPrefix):]
		for len(metaKey) >= len(amzMetaPrefix) && strings.EqualFold(metaKey[:len(amzMetaPrefix)], amzMetaPrefix) {
			metaKey = metaKey[len(amzMetaPrefix):]
		}
		newKey := amzMetaPrefix + strings.ToLower(metaKey)
		if key == newKey {
			continue
		}
		pending = append(pending, rewrite{oldKey: key, newKey: newKey, values: append([]string(nil), values...)})
	}
	for _, item := range pending {
		delete(header, item.oldKey)
		header[item.newKey] = item.values
	}
}

func (s *suite) bucket(t *testing.T) string {
	t.Helper()
	name := newBucketName(s.cfg.BucketPrefix)
	_, err := s.client.CreateBucket(context.Background(), createBucketInput(s.cfg, name))
	if err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	t.Cleanup(func() {
		if s.cfg.NotDelete {
			return
		}
		ctx := context.Background()
		var keyMarker, versionMarker *string
		for {
			listed, err := s.client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{Bucket: aws.String(name), KeyMarker: keyMarker, VersionIdMarker: versionMarker})
			if err != nil || listed == nil {
				break
			}
			ids := make([]types.ObjectIdentifier, 0, len(listed.Versions)+len(listed.DeleteMarkers))
			for _, v := range listed.Versions {
				ids = append(ids, types.ObjectIdentifier{Key: v.Key, VersionId: v.VersionId})
			}
			for _, v := range listed.DeleteMarkers {
				ids = append(ids, types.ObjectIdentifier{Key: v.Key, VersionId: v.VersionId})
			}
			if len(ids) > 0 {
				_, _ = s.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: aws.String(name), Delete: &types.Delete{Objects: ids, Quiet: aws.Bool(true)}})
			}
			if !aws.ToBool(listed.IsTruncated) {
				break
			}
			keyMarker, versionMarker = listed.NextKeyMarker, listed.NextVersionIdMarker
		}
		_, _ = s.client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(name)})
	})
	return name
}

func newBucketName(prefix string) string {
	return randomBucketName(prefix)
}

func randomText(length int) string {
	const letters = "abcdefghijklmnopqrstuvwxyz0123456789"
	if length <= 0 {
		return ""
	}
	b := make([]byte, length)
	for i := range b {
		b[i] = letters[rand.IntN(len(letters))]
	}
	return string(b)
}

func randomBucketName(prefix string) string {
	const bucketMaxLength = 63
	name := prefix + randomText(bucketMaxLength)
	if len(name) > bucketMaxLength-1 {
		return name[:bucketMaxLength-1]
	}
	return name
}

func createBucketInput(cfg testconfig.Config, name string) *s3.CreateBucketInput {
	input := &s3.CreateBucketInput{Bucket: aws.String(name)}
	if cfg.Endpoint() == "" && cfg.Region != "" && cfg.Region != "us-east-1" {
		input.CreateBucketConfiguration = &types.CreateBucketConfiguration{
			LocationConstraint: types.BucketLocationConstraint(cfg.Region),
		}
	}
	return input
}

func TestCreateBucketInputLocation(t *testing.T) {
	tests := []struct {
		name       string
		cfg        testconfig.Config
		wantRegion string
	}{
		{name: "aws regional endpoint", cfg: testconfig.Config{Region: "ap-northeast-2"}, wantRegion: "ap-northeast-2"},
		{name: "aws us-east-1", cfg: testconfig.Config{Region: "us-east-1"}},
		{name: "compatible endpoint", cfg: testconfig.Config{URL: "s3.example.test", Region: "ap-northeast-2", Port: 80}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			input := createBucketInput(tc.cfg, "bucket")
			if tc.wantRegion == "" {
				if input.CreateBucketConfiguration != nil {
					t.Fatalf("CreateBucketConfiguration = %#v, want nil", input.CreateBucketConfiguration)
				}
				return
			}
			if input.CreateBucketConfiguration == nil || string(input.CreateBucketConfiguration.LocationConstraint) != tc.wantRegion {
				t.Fatalf("location = %#v, want %q", input.CreateBucketConfiguration, tc.wantRegion)
			}
		})
	}
}

func TestNewBucketName(t *testing.T) {
	name := newBucketName("go-")
	if len(name) != 62 {
		t.Fatalf("bucket length = %d, want 62: %q", len(name), name)
	}
	if !strings.HasPrefix(name, "go-") {
		t.Fatalf("bucket prefix = %q, want go-", name)
	}
	for i, r := range name {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '-' {
			continue
		}
		t.Fatalf("bucket[%d] = %q, want lowercase letter, digit, or hyphen", i, r)
	}
	if name[0] == '-' || name[len(name)-1] == '-' {
		t.Fatalf("bucket must start and end with alphanumeric: %q", name)
	}
}

func TestEnsureBareAttributesQueryFlag(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{"", ""},
		{"attributes=", "attributes"},
		{"attributes", "attributes"},
		{"attributes=&versionId=abc", "attributes&versionId=abc"},
		{"versionId=abc&attributes=", "attributes&versionId=abc"},
		{"foo=bar", "foo=bar"},
		{"attributes=ObjectSize", "attributes=ObjectSize"},
	}
	for _, tc := range tests {
		if got := ensureBareAttributesQueryFlag(tc.in); got != tc.want {
			t.Fatalf("ensureBareAttributesQueryFlag(%q)=%q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestMergeObjectAttributesHeader(t *testing.T) {
	h := http.Header{}
	h["X-Amz-Object-Attributes"] = []string{"ObjectSize", "ETag", "StorageClass"}
	mergeObjectAttributesHeader(h)
	if got := h.Get(objectAttributesHeader); got != "ObjectSize,ETag,StorageClass" {
		t.Fatalf("merged header=%q map=%v", got, h)
	}
	if len(h.Values(objectAttributesHeader)) != 1 {
		t.Fatalf("want single header value, got %v", h.Values(objectAttributesHeader))
	}
}

func TestNormalizeOutgoingMetadataHeaders(t *testing.T) {
	h := http.Header{}
	h["X-Amz-Meta-Meta1"] = []string{"my"}
	h["X-Amz-Meta-Foo"] = []string{"bar"}
	h["X-Amz-Meta-X-Amz-Meta-Key"] = []string{"secret"}
	h["Content-Type"] = []string{"text/plain"}
	normalizeOutgoingMetadataHeaders(h)
	if _, ok := h["X-Amz-Meta-Meta1"]; ok {
		t.Fatalf("canonical meta header still present: %v", h)
	}
	if got := h["x-amz-meta-meta1"]; len(got) != 1 || got[0] != "my" {
		t.Fatalf("x-amz-meta-meta1=%v", got)
	}
	if got := h["x-amz-meta-foo"]; len(got) != 1 || got[0] != "bar" {
		t.Fatalf("x-amz-meta-foo=%v", got)
	}
	if got := h["x-amz-meta-key"]; len(got) != 1 || got[0] != "secret" {
		t.Fatalf("collapsed x-amz-meta-key=%v", got)
	}
	if _, ok := h["X-Amz-Meta-X-Amz-Meta-Key"]; ok {
		t.Fatalf("double-prefixed header still present: %v", h)
	}
	if got := h.Get("Content-Type"); got != "text/plain" {
		t.Fatalf("Content-Type=%q", got)
	}
}

func TestPutObjectMetadataWireFormat(t *testing.T) {
	var captured *http.Request
	opt := s3.Options{
		Region:       "us-east-1",
		Credentials:  aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider("AKID", "SECRET", "")),
		BaseEndpoint: aws.String("http://127.0.0.1:9"),
		UsePathStyle: true,
		HTTPClient: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			captured = r.Clone(r.Context())
			return &http.Response{
				StatusCode: 200,
				Header:     http.Header{"ETag": []string{`"etag"`}},
				Body:       io.NopCloser(strings.NewReader("")),
				Request:    r,
			}, nil
		}),
	}
	applyCompatibleS3Options(&opt)
	client := s3.New(opt)
	_, err := client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket:   aws.String("bucket"),
		Key:      aws.String("key"),
		Body:     bytes.NewReader([]byte("body")),
		Metadata: map[string]string{"meta1": "my", "x-amz-meta-key": "secret"},
	})
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	if captured == nil {
		t.Fatal("request was not captured")
	}
	if got := captured.Header["x-amz-meta-meta1"]; len(got) != 1 || got[0] != "my" {
		t.Fatalf("x-amz-meta-meta1=%v header=%v", got, captured.Header)
	}
	if got := captured.Header["x-amz-meta-key"]; len(got) != 1 || got[0] != "secret" {
		t.Fatalf("x-amz-meta-key=%v (want collapsed double-prefix) header=%v", got, captured.Header)
	}
	for k := range captured.Header {
		if len(k) >= len(amzMetaPrefix) && strings.EqualFold(k[:len(amzMetaPrefix)], amzMetaPrefix) && k != strings.ToLower(k) {
			t.Fatalf("metadata header %q is not lowercase", k)
		}
	}
}

func TestGetObjectAttributesWireFormat(t *testing.T) {
	var captured *http.Request
	opt := s3.Options{
		Region:       "us-east-1",
		Credentials:  aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider("AKID", "SECRET", "")),
		BaseEndpoint: aws.String("http://127.0.0.1:9"),
		UsePathStyle: true,
		HTTPClient: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			captured = r.Clone(r.Context())
			return &http.Response{
				StatusCode: 404,
				Header:     make(http.Header),
				Body: io.NopCloser(strings.NewReader(
					`<?xml version="1.0"?><Error><Code>NoSuchKey</Code><Message>missing</Message></Error>`)),
				Request: r,
			}, nil
		}),
	}
	applyCompatibleS3Options(&opt)
	client := s3.New(opt)
	_, _ = client.GetObjectAttributes(context.Background(), &s3.GetObjectAttributesInput{
		Bucket: aws.String("bucket"),
		Key:    aws.String("key"),
		ObjectAttributes: []types.ObjectAttributes{
			types.ObjectAttributesObjectSize,
			types.ObjectAttributesEtag,
			types.ObjectAttributesStorageClass,
		},
	})
	if captured == nil {
		t.Fatal("request was not captured")
	}
	if captured.URL.RawQuery != "attributes" {
		t.Fatalf("RawQuery=%q, want bare attributes subresource", captured.URL.RawQuery)
	}
	if got := captured.Header.Get(objectAttributesHeader); got != "ObjectSize,ETag,StorageClass" {
		t.Fatalf("X-Amz-Object-Attributes=%q header=%v", got, captured.Header)
	}
}

func TestCollapseAdjacentSlashes(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"/bucket//", "/bucket/"},
		{"/bucket/folder//double", "/bucket/folder/double"},
		{"/bucket///triple", "/bucket/triple"},
		{"/bucket/no-double", "/bucket/no-double"},
		{"", ""},
	}
	for _, tc := range cases {
		if got := collapseAdjacentSlashes(tc.in); got != tc.want {
			t.Fatalf("collapseAdjacentSlashes(%q)=%q, want %q", tc.in, got, tc.want)
		}
	}

	var captured *http.Request
	opt := s3.Options{
		Region:       "us-east-1",
		Credentials:  aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider("AKID", "SECRET", "")),
		BaseEndpoint: aws.String("http://127.0.0.1:9"),
		UsePathStyle: true,
		HTTPClient: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			captured = r.Clone(r.Context())
			return &http.Response{
				StatusCode: 404,
				Header:     make(http.Header),
				Body: io.NopCloser(strings.NewReader(
					`<?xml version="1.0"?><Error><Code>NoSuchKey</Code><Message>missing</Message></Error>`)),
				Request: r,
			}, nil
		}),
	}
	applyCompatibleS3Options(&opt)
	client := s3.New(opt)
	_, _ = client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String("bucket"),
		Key:    aws.String("folder//double"),
		Body:   bytes.NewReader([]byte("body")),
	})
	if captured == nil {
		t.Fatal("request was not captured")
	}
	if strings.Contains(captured.URL.Path, "//") {
		t.Fatalf("Path still has adjacent slashes: %q", captured.URL.Path)
	}
	if !strings.Contains(captured.URL.Path, "/folder/double") {
		t.Fatalf("Path=%q, want collapsed /folder/double", captured.URL.Path)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) Do(r *http.Request) (*http.Response, error) { return f(r) }

func put(t *testing.T, s *suite, bucket, key, body string, metadata map[string]string) *s3.PutObjectOutput {
	t.Helper()
	out, err := s.client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte(body)), Metadata: metadata})
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	return out
}

// putObjectMaybeChunked는 Java getClientHttps(useChunkEncoding)를 따릅니다:
// UNSIGNED-PAYLOAD와 함께 non-seekable body. ContentLength를 설정하여 KSAN이 411을 반환하지 않게 함.
func putObjectMaybeChunked(t *testing.T, client *s3.Client, bucket, key string, body []byte, chunked bool) {
	t.Helper()
	input := &s3.PutObjectInput{
		Bucket:        aws.String(bucket),
		Key:           aws.String(key),
		Body:          bytes.NewReader(body),
		ContentLength: aws.Int64(int64(len(body))),
	}
	var opts []func(*s3.Options)
	if chunked {
		input.Body = opaqueReader{Reader: bytes.NewReader(body)}
		opts = append(opts, func(o *s3.Options) {
			o.APIOptions = append(o.APIOptions, v4.SwapComputePayloadSHA256ForUnsignedPayloadMiddleware)
		})
	}
	if _, err := client.PutObject(context.Background(), input, opts...); err != nil {
		t.Fatal(err)
	}
}

// opaqueReader는 Seek를 숨겨 SDK가 body를 streaming/chunked 업로드로 취급하게 합니다.
type opaqueReader struct{ io.Reader }

func read(t *testing.T, s *suite, bucket, key string) string {
	t.Helper()
	out, err := s.client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	defer out.Body.Close()
	b, err := io.ReadAll(out.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestSmokeListBuckets(t *testing.T) {
	s := newSuite(t)
	b := s.bucket(t)
	out, err := s.client.ListBuckets(context.Background(), &s3.ListBucketsInput{})
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range out.Buckets {
		if aws.ToString(v.Name) == b {
			return
		}
	}
	t.Fatalf("created bucket %q not listed", b)
}
func TestObjectWriteReadUpdateDelete(t *testing.T) {
	s := newSuite(t)
	b := s.bucket(t)
	put(t, s, b, "object", "first", nil)
	if got := read(t, s, b, "object"); got != "first" {
		t.Fatalf("got %q", got)
	}
	put(t, s, b, "object", "second", nil)
	if got := read(t, s, b, "object"); got != "second" {
		t.Fatalf("got %q", got)
	}
	if _, err := s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(b), Key: aws.String("object")}); err != nil {
		t.Fatal(err)
	}
}
func TestObjectETagAndMetadata(t *testing.T) {
	s := newSuite(t)
	b := s.bucket(t)
	body := "hello s3"
	out := put(t, s, b, "metadata", body, map[string]string{"purpose": "compatibility"})
	sum := md5.Sum([]byte(body))
	if strings.Trim(aws.ToString(out.ETag), "\"") != hex.EncodeToString(sum[:]) {
		t.Fatalf("unexpected ETag %s", aws.ToString(out.ETag))
	}
	head, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String("metadata")})
	if err != nil {
		t.Fatal(err)
	}
	if head.Metadata["purpose"] != "compatibility" {
		t.Fatalf("metadata=%v", head.Metadata)
	}
}
func TestListObjectsPrefixDelimiter(t *testing.T) {
	s := newSuite(t)
	b := s.bucket(t)
	for _, k := range []string{"photos/2025/a.jpg", "photos/2026/b.jpg", "readme.txt"} {
		put(t, s, b, k, k, nil)
	}
	out, err := s.client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String(b), Prefix: aws.String("photos/"), Delimiter: aws.String("/")})
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, p := range out.CommonPrefixes {
		got = append(got, aws.ToString(p.Prefix))
	}
	sort.Strings(got)
	want := "photos/2025/,photos/2026/"
	if strings.Join(got, ",") != want {
		t.Fatalf("got %v, want %s", got, want)
	}
}
func TestSmokeCopyObject(t *testing.T) {
	s := newSuite(t)
	b := s.bucket(t)
	put(t, s, b, "source", "copy me", nil)
	_, err := s.client.CopyObject(context.Background(), &s3.CopyObjectInput{Bucket: aws.String(b), Key: aws.String("dest"), CopySource: aws.String(b + "/source")})
	if err != nil {
		t.Fatal(err)
	}
	if got := read(t, s, b, "dest"); got != "copy me" {
		t.Fatalf("got %q", got)
	}
}
func TestSmokeDeleteObjects(t *testing.T) {
	s := newSuite(t)
	b := s.bucket(t)
	put(t, s, b, "a", "a", nil)
	put(t, s, b, "b", "b", nil)
	_, err := s.client.DeleteObjects(context.Background(), &s3.DeleteObjectsInput{Bucket: aws.String(b), Delete: &types.Delete{Objects: []types.ObjectIdentifier{{Key: aws.String("a")}, {Key: aws.String("b")}}}})
	if err != nil {
		t.Fatal(err)
	}
	out, err := s.client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String(b)})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Contents) != 0 {
		t.Fatalf("objects remain: %v", out.Contents)
	}
}
func TestSmokeMultipartUpload(t *testing.T) {
	s := newSuite(t)
	b := s.bucket(t)
	key := "multipart"
	ctx := context.Background()
	create, err := s.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: aws.String(b), Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}
	data := bytes.Repeat([]byte("x"), 5*1024*1024)
	part, err := s.client.UploadPart(ctx, &s3.UploadPartInput{Bucket: aws.String(b), Key: aws.String(key), UploadId: create.UploadId, PartNumber: aws.Int32(1), Body: bytes.NewReader(data)})
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{Bucket: aws.String(b), Key: aws.String(key), UploadId: create.UploadId, MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{{ETag: part.ETag, PartNumber: aws.Int32(1)}}}})
	if err != nil {
		t.Fatal(err)
	}
	head, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}
	if head.ContentLength == nil || *head.ContentLength != int64(len(data)) {
		t.Fatalf("size=%v", head.ContentLength)
	}
}
func TestObjectTagging(t *testing.T) {
	s := newSuite(t)
	b := s.bucket(t)
	put(t, s, b, "tagged", "data", nil)
	_, err := s.client.PutObjectTagging(context.Background(), &s3.PutObjectTaggingInput{Bucket: aws.String(b), Key: aws.String("tagged"), Tagging: &types.Tagging{TagSet: []types.Tag{{Key: aws.String("env"), Value: aws.String("test")}}}})
	if err != nil {
		t.Fatal(err)
	}
	out, err := s.client.GetObjectTagging(context.Background(), &s3.GetObjectTaggingInput{Bucket: aws.String(b), Key: aws.String("tagged")})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.TagSet) != 1 || aws.ToString(out.TagSet[0].Value) != "test" {
		t.Fatalf("tags=%v", out.TagSet)
	}
}
func TestSmokeVersioning(t *testing.T) {
	s := newSuite(t)
	b := s.bucket(t)
	ctx := context.Background()
	_, err := s.client.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{Bucket: aws.String(b), VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled}})
	if err != nil {
		t.Fatal(err)
	}
	a := put(t, s, b, "versioned", "one", nil)
	c := put(t, s, b, "versioned", "two", nil)
	if aws.ToString(a.VersionId) == "" || aws.ToString(c.VersionId) == "" || aws.ToString(a.VersionId) == aws.ToString(c.VersionId) {
		t.Fatalf("invalid version IDs %q %q", aws.ToString(a.VersionId), aws.ToString(c.VersionId))
	}
}
