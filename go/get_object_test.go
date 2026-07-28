package s3tests

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func TestObjectReadNotExist(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	_, err := s.client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String(s.bucket(t)), Key: aws.String("foo")})
	assertS3Error(t, err, 404, "NoSuchKey")
}
func TestGetObjectIfMatchGood(t *testing.T) {
	t.Parallel()

	testConditionalGet(t, "test_get_object_if_match_good")
}
func TestGetObjectIfMatchFailed(t *testing.T) {
	t.Parallel()

	testConditionalGet(t, "test_get_object_if_match_failed")
}
func TestGetObjectIfNoneMatchGood(t *testing.T) {
	t.Parallel()

	testConditionalGet(t, "test_get_object_if_none_match_good")
}
func TestGetObjectIfNoneMatchFailed(t *testing.T) {
	t.Parallel()

	testConditionalGet(t, "test_get_object_if_none_match_failed")
}
func TestGetObjectIfModifiedSinceGood(t *testing.T) {
	t.Parallel()

	testConditionalGet(t, "test_get_object_if_modified_since_good")
}
func TestGetObjectIfModifiedSinceFailed(t *testing.T) {
	t.Parallel()

	testConditionalGet(t, "test_get_object_if_modified_since_failed")
}
func TestGetObjectIfUnmodifiedSinceGood(t *testing.T) {
	t.Parallel()

	testConditionalGet(t, "test_get_object_if_unmodified_since_good")
}
func TestGetObjectIfUnmodifiedSinceFailed(t *testing.T) {
	t.Parallel()

	testConditionalGet(t, "test_get_object_if_unmodified_since_failed")
}
func TestGetObjectIfMatchWithIfUnmodifiedSince(t *testing.T) {
	t.Parallel()

	testConditionalGet(t, "test_get_object_if_match_with_if_unmodified_since")
}
func TestGetObjectIfNoneMatchWithIfModifiedSince(t *testing.T) {
	t.Parallel()

	testConditionalGet(t, "test_get_object_if_none_match_with_if_modified_since")
}
func TestGetObjectIfMatchAndIfNoneMatch(t *testing.T) {
	t.Parallel()

	testConditionalGet(t, "test_get_object_if_match_and_if_none_match")
}
func TestGetObjectIfMatchAndIfNoneMatchAny(t *testing.T) {
	t.Parallel()

	testConditionalGet(t, "test_get_object_if_match_and_if_none_match_any")
}
func TestHeadObjectIfMatchGood(t *testing.T) {
	t.Parallel()

	testConditionalHead(t, "test_head_object_if_match_good")
}
func TestHeadObjectIfMatchFailed(t *testing.T) {
	t.Parallel()

	testConditionalHead(t, "test_head_object_if_match_failed")
}
func TestHeadObjectIfNoneMatchGood(t *testing.T) {
	t.Parallel()

	testConditionalHead(t, "test_head_object_if_none_match_good")
}
func TestHeadObjectIfNoneMatchFailed(t *testing.T) {
	t.Parallel()

	testConditionalHead(t, "test_head_object_if_none_match_failed")
}
func TestHeadObjectIfModifiedSinceGood(t *testing.T) {
	t.Parallel()

	testConditionalHead(t, "test_head_object_if_modified_since_good")
}
func TestHeadObjectIfModifiedSinceFailed(t *testing.T) {
	t.Parallel()

	testConditionalHead(t, "test_head_object_if_modified_since_failed")
}
func TestHeadObjectIfUnmodifiedSinceGood(t *testing.T) {
	t.Parallel()

	testConditionalHead(t, "test_head_object_if_unmodified_since_good")
}
func TestHeadObjectIfUnmodifiedSinceFailed(t *testing.T) {
	t.Parallel()

	testConditionalHead(t, "test_head_object_if_unmodified_since_failed")
}
func TestRangedRequestResponseCode(t *testing.T) {
	t.Parallel()

	testGetRange(t, "test_ranged_request_response_code")
}
func TestRangedBigRequestResponseCode(t *testing.T) {
	t.Parallel()

	testGetRange(t, "test_ranged_big_request_response_code")
}
func TestRangedRequestSkipLeadingBytesResponseCode(t *testing.T) {
	t.Parallel()

	testGetRange(t, "test_ranged_request_skip_leading_bytes_response_code")
}
func TestRangedRequestReturnTrailingBytesResponseCode(t *testing.T) {
	t.Parallel()

	testGetRange(t, "test_ranged_request_return_trailing_bytes_response_code")
}
func TestRangedRequestInvalidRange(t *testing.T) {
	t.Parallel()

	testGetRange(t, "test_ranged_request_invalid_range")
}
func TestRangedRequestEmptyObject(t *testing.T) {
	t.Parallel()

	testGetRange(t, "test_ranged_request_empty_object")
}
func TestGetObjectMany(t *testing.T) {
	t.Parallel()

	testGetMany(t, "test_get_object_many")
}
func TestRangeObjectMany(t *testing.T) {
	t.Parallel()

	testGetMany(t, "test_range_object_many")
}
func TestObjectResponseHeaders(t *testing.T) {
	t.Parallel()

	testGetResponseHeaders(t)
}
func TestMultipartObjectRange(t *testing.T) {
	t.Parallel()

	testGetMultipartPart(t)
}
func TestGetObjectIgnore(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket, key := s.bucket(t), "testObjectIgnore"
	put(t, s, bucket, key, key, nil)
	out := getObject(t, s.client, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if aws.ToInt64(out.ContentLength) != int64(len(key)) {
		t.Fatalf("length=%v", out.ContentLength)
	}
	out.Body.Close()
}
func TestGetObjectAfterDelete(t *testing.T) {
	t.Parallel()

	testGetAfterDelete(t, false)
}
func TestGetObjectAfterDeleteVersioning(t *testing.T) {
	t.Parallel()

	testGetAfterDelete(t, true)
}
func TestGetObjectDeleteMarker(t *testing.T) {
	t.Parallel()

	testGetDeleteMarker(t)
}

func testConditionalGet(t *testing.T, name string) {
	t.Helper()
	s := newSuite(t)
	bucket, key := s.bucket(t), name
	created := put(t, s, bucket, key, "bar", nil)
	past, future := time.Date(1994, 9, 29, 19, 43, 31, 0, time.UTC), time.Date(2100, 9, 29, 19, 43, 31, 0, time.UTC)
	input := &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}
	status, code := 0, ""
	switch name {
	case "test_get_object_if_match_good":
		input.IfMatch = created.ETag
	case "test_get_object_if_match_failed":
		input.IfMatch = aws.String("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
		status, code = 412, "PreconditionFailed"
	case "test_get_object_if_none_match_good":
		input.IfNoneMatch = created.ETag
		status = 304
	case "test_get_object_if_none_match_failed":
		input.IfNoneMatch = aws.String("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
	case "test_get_object_if_modified_since_good":
		input.IfModifiedSince = &past
	case "test_get_object_if_modified_since_failed":

		head := headObject(t, s.client, bucket, key)
		after := head.LastModified.Add(time.Second)
		input.IfModifiedSince = &after
		time.Sleep(time.Second)
		status = 304
	case "test_get_object_if_unmodified_since_good":
		input.IfUnmodifiedSince = &past
		status, code = 412, "PreconditionFailed"
	case "test_get_object_if_unmodified_since_failed":
		input.IfUnmodifiedSince = &future
	case "test_get_object_if_match_with_if_unmodified_since":
		input.IfMatch, input.IfUnmodifiedSince = created.ETag, &past
	case "test_get_object_if_none_match_with_if_modified_since":
		input.IfNoneMatch, input.IfModifiedSince = created.ETag, &past
		status = 304
	case "test_get_object_if_match_and_if_none_match":
		input.IfMatch, input.IfNoneMatch = created.ETag, created.ETag
		status = 304
	case "test_get_object_if_match_and_if_none_match_any":
		input.IfMatch, input.IfNoneMatch = created.ETag, aws.String("*")
		status = 304
	default:
		t.Fatalf("unknown conditional GET %q", name)
	}
	out, err := s.client.GetObject(context.Background(), input)
	if status != 0 {
		if code == "" {
			assertHTTPError(t, err, status)
		} else {
			assertS3Error(t, err, status, code)
		}
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	defer out.Body.Close()
	body, err := io.ReadAll(out.Body)
	if err != nil || string(body) != "bar" {
		t.Fatalf("body=%q err=%v", body, err)
	}
}

func testConditionalHead(t *testing.T, name string) {
	t.Helper()
	s := newSuite(t)
	bucket, key := s.bucket(t), name
	created := put(t, s, bucket, key, "bar", nil)
	past, future := time.Date(1994, 9, 29, 19, 43, 31, 0, time.UTC), time.Date(2100, 9, 29, 19, 43, 31, 0, time.UTC)
	input := &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}
	status := 0
	switch name {
	case "test_head_object_if_match_good":
		input.IfMatch = created.ETag
	case "test_head_object_if_match_failed":
		input.IfMatch = aws.String("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
		status = 412
	case "test_head_object_if_none_match_good":
		input.IfNoneMatch = created.ETag
		status = 304
	case "test_head_object_if_none_match_failed":
		input.IfNoneMatch = aws.String("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
	case "test_head_object_if_modified_since_good":
		input.IfModifiedSince = &past
	case "test_head_object_if_modified_since_failed":

		head := headObject(t, s.client, bucket, key)
		after := head.LastModified.Add(time.Second)
		input.IfModifiedSince = &after
		time.Sleep(time.Second)
		status = 304
	case "test_head_object_if_unmodified_since_good":
		input.IfUnmodifiedSince = &past
		status = 412
	case "test_head_object_if_unmodified_since_failed":
		input.IfUnmodifiedSince = &future
	default:
		t.Fatalf("unknown conditional HEAD %q", name)
	}
	out, err := s.client.HeadObject(context.Background(), input)
	if status != 0 {
		assertHTTPError(t, err, status)
		return
	}
	if err != nil || aws.ToInt64(out.ContentLength) != 3 || (name == "test_head_object_if_match_good" && aws.ToString(out.ETag) != aws.ToString(created.ETag)) {
		t.Fatalf("head=%#v err=%v", out, err)
	}
}

func testGetRange(t *testing.T, name string) {
	t.Helper()
	s := newSuite(t)
	bucket, key := s.bucket(t), "obj"
	content, requestRange, want, wantRange := "contentData", "bytes=4-7", "entD", "bytes 4-7/11"
	switch name {
	case "test_ranged_big_request_response_code":
		content = stringsRepeatPattern(8 * 1024 * 1024)
		requestRange = "bytes=3145728-5242880"
		want = content[3145728:5242881]
		wantRange = "bytes 3145728-5242880/8388608"
	case "test_ranged_request_skip_leading_bytes_response_code":
		requestRange, want, wantRange = "bytes=4-", content[4:], "bytes 4-10/11"
	case "test_ranged_request_return_trailing_bytes_response_code":
		requestRange, want, wantRange = "bytes=-7", content[len(content)-7:], "bytes 4-10/11"
	case "test_ranged_request_invalid_range":
		requestRange = "bytes=40-50"
	case "test_ranged_request_empty_object":
		content, requestRange = "", "bytes=40-50"
	}
	put(t, s, bucket, key, content, nil)
	out, err := s.client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Range: aws.String(requestRange)})
	if name == "test_ranged_request_invalid_range" || name == "test_ranged_request_empty_object" {
		assertS3Error(t, err, 416, "InvalidRange")
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	defer out.Body.Close()
	body, err := io.ReadAll(out.Body)
	if err != nil || string(body) != want || aws.ToString(out.ContentRange) != wantRange {
		t.Fatalf("length=%d range=%q want length=%d range=%q err=%v", len(body), aws.ToString(out.ContentRange), len(want), wantRange, err)
	}
}

func testGetMany(t *testing.T, name string) {
	t.Helper()
	s := newSuite(t)
	bucket, key := s.bucket(t), "foo"
	data := stringsRepeatPattern(15 * 1024 * 1024)
	put(t, s, bucket, key, data, nil)
	for i := 0; i < 50; i++ {
		input := &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}
		start, end := 0, len(data)-1
		if name == "test_range_object_many" {
			start = (i * 7919) % (len(data) - 65536)
			end = start + 65535
			input.Range = aws.String(fmt.Sprintf("bytes=%d-%d", start, end))
		}
		out := getObject(t, s.client, input)
		body, err := io.ReadAll(out.Body)
		out.Body.Close()
		if err != nil || !bytes.Equal(body, []byte(data[start:end+1])) {
			t.Fatalf("iteration=%d range=%d-%d length=%d err=%v", i, start, end, len(body), err)
		}
	}
}

func testGetResponseHeaders(t *testing.T) {
	t.Helper()
	s := newSuite(t)
	bucket, key := s.bucket(t), "testObjectResponseHeaders"
	put(t, s, bucket, key, key, nil)
	expires := time.Now().UTC().Truncate(time.Second)
	out := getObject(t, s.client, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), ResponseCacheControl: aws.String("no-cache"), ResponseContentDisposition: aws.String("bla"), ResponseContentEncoding: aws.String("aaa"), ResponseContentLanguage: aws.String("esperanto"), ResponseContentType: aws.String("foo/bar"), ResponseExpires: &expires})
	defer out.Body.Close()
	if aws.ToString(out.CacheControl) != "no-cache" || aws.ToString(out.ContentDisposition) != "bla" || aws.ToString(out.ContentEncoding) != "aaa" || aws.ToString(out.ContentLanguage) != "esperanto" || aws.ToString(out.ContentType) != "foo/bar" {
		t.Fatalf("response headers cache=%q disposition=%q encoding=%q language=%q type=%q", aws.ToString(out.CacheControl), aws.ToString(out.ContentDisposition), aws.ToString(out.ContentEncoding), aws.ToString(out.ContentLanguage), aws.ToString(out.ContentType))
	}
}

func testGetMultipartPart(t *testing.T) {
	t.Helper()
	s := newSuite(t)
	bucket, key := s.bucket(t), "testMultipartObjectRange"
	ctx := context.Background()
	data := bytes.Repeat([]byte("m"), 5*1024*1024)
	created, err := s.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}
	part, err := s.client.UploadPart(ctx, &s3.UploadPartInput{Bucket: aws.String(bucket), Key: aws.String(key), UploadId: created.UploadId, PartNumber: aws.Int32(1), Body: bytes.NewReader(data)})
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(key), UploadId: created.UploadId, MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{{ETag: part.ETag, PartNumber: aws.Int32(1)}}}})
	if err != nil {
		t.Fatal(err)
	}
	out := getObject(t, s.client, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), PartNumber: aws.Int32(1)})
	body, err := io.ReadAll(out.Body)
	out.Body.Close()
	if err != nil || !bytes.Equal(body, data) {
		t.Fatalf("length=%d want=%d err=%v", len(body), len(data), err)
	}
}

func testGetAfterDelete(t *testing.T, versioned bool) {
	t.Helper()
	s := newSuite(t)
	bucket, key, body := s.bucket(t), "deleted", "testContent"
	if versioned {
		enableVersioning(t, s, bucket)
	}
	put(t, s, bucket, key, body, nil)
	if read(t, s, bucket, key) != body {
		t.Fatal("initial body mismatch")
	}
	_, err := s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	assertS3Error(t, err, 404, "NoSuchKey")
}

func testGetDeleteMarker(t *testing.T) {
	t.Helper()
	s := newSuite(t)
	bucket, key, body := s.bucket(t), "marker", "testContent"
	enableVersioning(t, s, bucket)
	put(t, s, bucket, key, body, nil)
	_, err := s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}
	listed, err := s.client.ListObjectVersions(context.Background(), &s3.ListObjectVersionsInput{Bucket: aws.String(bucket)})
	if err != nil || len(listed.DeleteMarkers) != 1 || len(listed.Versions) != 1 {
		t.Fatalf("markers=%v versions=%v err=%v", listed.DeleteMarkers, listed.Versions, err)
	}
	_, err = s.client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), VersionId: listed.DeleteMarkers[0].VersionId})
	assertS3Error(t, err, 405, "MethodNotAllowed")
	out := getObject(t, s.client, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), VersionId: listed.Versions[0].VersionId})
	defer out.Body.Close()
	bytesBody, err := io.ReadAll(out.Body)
	if err != nil || string(bytesBody) != body {
		t.Fatalf("body=%q err=%v", bytesBody, err)
	}
}

func getObject(t *testing.T, client *s3.Client, input *s3.GetObjectInput) *s3.GetObjectOutput {
	t.Helper()
	out, err := client.GetObject(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	return out
}
func headObject(t *testing.T, client *s3.Client, bucket, key string) *s3.HeadObjectOutput {
	t.Helper()
	out, err := client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}
	return out
}
func stringsRepeatPattern(size int) string {
	pattern := []byte("0123456789abcdef")
	data := make([]byte, size)
	for i := range data {
		data[i] = pattern[i%len(pattern)]
	}
	return string(data)
}
