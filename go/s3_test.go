package s3tests

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"

	"ksantest/go-s3tests/internal/testconfig"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type suite struct {
	client  *s3.Client
	cfg     testconfig.Config
	suiteID string
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
	return &suite{client: s3.New(opt), cfg: cfg, suiteID: callerSuiteID()}
}

// callerSuiteID는 newSuite를 호출한 *_test.go 파일에서 java식 suite id를 파생한다.
// 버킷 생성 헬퍼도 자신의 도메인 파일에 있으므로 파일명이 곧 java 클래스(suite)와 일치한다.
// java Utils.toSuiteId(클래스 simpleName 소문자·영숫자)와 동일한 결과를 낸다.
func callerSuiteID() string {
	for skip := 2; skip < 10; skip++ {
		_, file, _, ok := runtime.Caller(skip)
		if !ok {
			break
		}
		base := filepath.Base(file)
		if !strings.HasSuffix(base, "_test.go") {
			continue
		}
		if id := suiteIDFromFile(base); id != "" {
			return id
		}
	}
	return "x"
}

// suiteIDFromFile: "put_object_test.go" -> "putobject" (java toSuiteId와 동일 규칙)
func suiteIDFromFile(base string) string {
	name := strings.TrimSuffix(base, "_test.go")
	var b strings.Builder
	for _, r := range strings.ToLower(name) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// buildBucketName은 java Utils.getNewBucketName과 동일한 형식을 만든다:
// id가 있으면 {prefix}{suite}-{id}-{random}, 없으면 {prefix}{suite}-{random}. 62자 캡.
func buildBucketName(prefix, suite string, id ...int) string {
	const maxLen = 62 // BUCKET_MAX_LENGTH - 1
	if suite == "" {
		suite = "x"
	}
	head := prefix + suite + "-"
	if len(id) > 0 {
		head += strconv.Itoa(id[0]) + "-"
	}
	randomLen := maxLen - len(head)
	if randomLen < 6 {
		return randomBucketName(prefix)
	}
	name := head + randomText(randomLen)
	if len(name) > maxLen {
		name = name[:maxLen]
	}
	return name
}

func (s *suite) bucket(t *testing.T, id ...int) string {
	t.Helper()
	name := buildBucketName(s.cfg.BucketPrefix, s.suiteID, id...)
	_, err := s.client.CreateBucket(context.Background(), createBucketInput(s.cfg, name))
	if err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	t.Cleanup(func() {
		if s.cfg.NotDelete {
			return
		}
		ctx := context.Background()
		for {
			listed, err := s.client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{Bucket: aws.String(name)})
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
				if _, err := s.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: aws.String(name), Delete: &types.Delete{Objects: ids, Quiet: aws.Bool(true)}}); err != nil {
					t.Logf("DeleteObjects cleanup %s: %v", name, err)
					break
				}
			}
			if !aws.ToBool(listed.IsTruncated) {
				break
			}
		}
		if _, err := s.client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(name)}); err != nil {
			t.Logf("DeleteBucket cleanup %s: %v", name, err)
		}
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

func put(t *testing.T, s *suite, bucket, key, body string, metadata map[string]string) *s3.PutObjectOutput {
	t.Helper()
	out, err := s.client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte(body)), Metadata: metadata})
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	return out
}

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
