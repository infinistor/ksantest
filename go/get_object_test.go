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

// 버킷에 존재하지 않는 오브젝트 다운로드를 할 경우 실패 확인
func TestObjectReadNotExist(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	_, err := s.client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String(s.bucket(t, 1)), Key: aws.String("foo")})
	assertS3Error(t, err, 404, "NoSuchKey")
}

// 존재하는 오브젝트 이름과 ETag 값으로 오브젝트를 가져오는지 확인
func TestGetObjectIfMatchGood(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket, key := s.bucket(t, 2), "test_get_object_if_match_good"
	created := put(t, s, bucket, key, "bar", nil)
	input := &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}
	input.IfMatch = created.ETag
	out, err := s.client.GetObject(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Body.Close()
	body, err := io.ReadAll(out.Body)
	if err != nil || string(body) != "bar" {
		t.Fatalf("body=%q err=%v", body, err)
	}
}

// 오브젝트와 일치하지 않는 ETag 값을 설정하여 오브젝트 조회 실패 확인
func TestGetObjectIfMatchFailed(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket, key := s.bucket(t, 3), "test_get_object_if_match_failed"
	put(t, s, bucket, key, "bar", nil)
	input := &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}
	input.IfMatch = aws.String("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
	_, err := s.client.GetObject(context.Background(), input)
	assertS3Error(t, err, 412, "PreconditionFailed")
}

// 오브젝트와 일치하는 ETag 값을 IfsNoneMatch에 설정하여 오브젝트 조회 실패
func TestGetObjectIfNoneMatchGood(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket, key := s.bucket(t, 4), "test_get_object_if_none_match_good"
	created := put(t, s, bucket, key, "bar", nil)
	input := &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}
	input.IfNoneMatch = created.ETag
	_, err := s.client.GetObject(context.Background(), input)
	assertHTTPError(t, err, 304)
}

// 오브젝트와 일치하지 않는 ETag 값을 IfsNoneMatch에 설정하여 오브젝트 조회 성공
func TestGetObjectIfNoneMatchFailed(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket, key := s.bucket(t, 5), "test_get_object_if_none_match_failed"
	put(t, s, bucket, key, "bar", nil)
	input := &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}
	input.IfNoneMatch = aws.String("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
	out, err := s.client.GetObject(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Body.Close()
	body, err := io.ReadAll(out.Body)
	if err != nil || string(body) != "bar" {
		t.Fatalf("body=%q err=%v", body, err)
	}
}

// [지정일을 오브젝트 업로드 시간 이전으로 설정] 지정일(ifModifiedSince)보다 이후에 수정된 오브젝트를 조회 성공
func TestGetObjectIfModifiedSinceGood(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket, key := s.bucket(t, 6), "test_get_object_if_modified_since_good"
	put(t, s, bucket, key, "bar", nil)
	past := time.Date(1994, 9, 29, 19, 43, 31, 0, time.UTC)
	input := &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}
	input.IfModifiedSince = &past
	out, err := s.client.GetObject(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Body.Close()
	body, err := io.ReadAll(out.Body)
	if err != nil || string(body) != "bar" {
		t.Fatalf("body=%q err=%v", body, err)
	}
}

// [지정일을 오브젝트 업로드 시간 이후로 설정] 지정일(ifModifiedSince)보다 이전에 수정된 오브젝트 조회 실패
func TestGetObjectIfModifiedSinceFailed(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket, key := s.bucket(t, 7), "test_get_object_if_modified_since_failed"
	put(t, s, bucket, key, "bar", nil)
	head := headObject(t, s.client, bucket, key)
	after := head.LastModified.Add(time.Second)
	input := &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}
	input.IfModifiedSince = &after
	time.Sleep(time.Second)
	_, err := s.client.GetObject(context.Background(), input)
	assertHTTPError(t, err, 304)
}

// [지정일을 오브젝트 업로드 시간 이전으로 설정] 지정일(ifUnmodifiedSince) 이후 수정되지 않은 오브젝트 조회 실패
func TestGetObjectIfUnmodifiedSinceGood(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket, key := s.bucket(t, 8), "test_get_object_if_unmodified_since_good"
	put(t, s, bucket, key, "bar", nil)
	past := time.Date(1994, 9, 29, 19, 43, 31, 0, time.UTC)
	input := &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}
	input.IfUnmodifiedSince = &past
	_, err := s.client.GetObject(context.Background(), input)
	assertS3Error(t, err, 412, "PreconditionFailed")
}

// [지정일을 오브젝트 업로드 시간 이후으로 설정] 지정일(ifUnmodifiedSince) 이후 수정되지 않은 오브젝트 조회 성공
func TestGetObjectIfUnmodifiedSinceFailed(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket, key := s.bucket(t, 9), "test_get_object_if_unmodified_since_failed"
	put(t, s, bucket, key, "bar", nil)
	future := time.Date(2100, 9, 29, 19, 43, 31, 0, time.UTC)
	input := &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}
	input.IfUnmodifiedSince = &future
	out, err := s.client.GetObject(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Body.Close()
	body, err := io.ReadAll(out.Body)
	if err != nil || string(body) != "bar" {
		t.Fatalf("body=%q err=%v", body, err)
	}
}

// If-Match(일치)와 If-Unmodified-Since(불일치)를 함께 사용할 경우 ETag 조건이 우선되어 성공하는지 확인
func TestGetObjectIfMatchWithIfUnmodifiedSince(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket, key := s.bucket(t, 10), "test_get_object_if_match_with_if_unmodified_since"
	created := put(t, s, bucket, key, "bar", nil)
	past := time.Date(1994, 9, 29, 19, 43, 31, 0, time.UTC)
	input := &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}
	input.IfMatch, input.IfUnmodifiedSince = created.ETag, &past
	out, err := s.client.GetObject(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Body.Close()
	body, err := io.ReadAll(out.Body)
	if err != nil || string(body) != "bar" {
		t.Fatalf("body=%q err=%v", body, err)
	}
}

// If-None-Match(불일치)와 If-Modified-Since(일치)를 함께 사용할 경우 ETag 조건이 우선되어 304가 반환되는지 확인
func TestGetObjectIfNoneMatchWithIfModifiedSince(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket, key := s.bucket(t, 11), "test_get_object_if_none_match_with_if_modified_since"
	created := put(t, s, bucket, key, "bar", nil)
	past := time.Date(1994, 9, 29, 19, 43, 31, 0, time.UTC)
	input := &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}
	input.IfNoneMatch, input.IfModifiedSince = created.ETag, &past
	_, err := s.client.GetObject(context.Background(), input)
	assertHTTPError(t, err, 304)
}

// If-Match와 If-None-Match에 동일한 ETag를 지정하면 304가 반환되는지 확인
func TestGetObjectIfMatchAndIfNoneMatch(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket, key := s.bucket(t, 12), "test_get_object_if_match_and_if_none_match"
	created := put(t, s, bucket, key, "bar", nil)
	input := &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}
	input.IfMatch, input.IfNoneMatch = created.ETag, created.ETag
	_, err := s.client.GetObject(context.Background(), input)
	assertHTTPError(t, err, 304)
}

// If-Match와 If-None-Match: * 를 함께 지정하면 304가 반환되는지 확인
func TestGetObjectIfMatchAndIfNoneMatchAny(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket, key := s.bucket(t, 13), "test_get_object_if_match_and_if_none_match_any"
	created := put(t, s, bucket, key, "bar", nil)
	input := &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}
	input.IfMatch, input.IfNoneMatch = created.ETag, aws.String("*")
	_, err := s.client.GetObject(context.Background(), input)
	assertHTTPError(t, err, 304)
}

// HeadObject에서 일치하는 If-Match 조건으로 성공 확인
func TestHeadObjectIfMatchGood(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket, key := s.bucket(t, 14), "test_head_object_if_match_good"
	created := put(t, s, bucket, key, "bar", nil)
	input := &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}
	input.IfMatch = created.ETag
	out, err := s.client.HeadObject(context.Background(), input)
	if err != nil || aws.ToInt64(out.ContentLength) != 3 || aws.ToString(out.ETag) != aws.ToString(created.ETag) {
		t.Fatalf("head=%#v err=%v", out, err)
	}
}

// HeadObject에서 일치하지 않는 If-Match 조건으로 412 실패 확인
func TestHeadObjectIfMatchFailed(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket, key := s.bucket(t, 15), "test_head_object_if_match_failed"
	put(t, s, bucket, key, "bar", nil)
	input := &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}
	input.IfMatch = aws.String("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
	_, err := s.client.HeadObject(context.Background(), input)
	assertHTTPError(t, err, 412)
}

// HeadObject에서 일치하는 If-None-Match 조건으로 304 반환 확인
func TestHeadObjectIfNoneMatchGood(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket, key := s.bucket(t, 16), "test_head_object_if_none_match_good"
	created := put(t, s, bucket, key, "bar", nil)
	input := &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}
	input.IfNoneMatch = created.ETag
	_, err := s.client.HeadObject(context.Background(), input)
	assertHTTPError(t, err, 304)
}

// HeadObject에서 일치하지 않는 If-None-Match 조건으로 성공 확인
func TestHeadObjectIfNoneMatchFailed(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket, key := s.bucket(t, 17), "test_head_object_if_none_match_failed"
	put(t, s, bucket, key, "bar", nil)
	input := &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}
	input.IfNoneMatch = aws.String("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
	out, err := s.client.HeadObject(context.Background(), input)
	if err != nil || aws.ToInt64(out.ContentLength) != 3 {
		t.Fatalf("head=%#v err=%v", out, err)
	}
}

// HeadObject에서 오브젝트 업로드 이전 시간의 If-Modified-Since 조건으로 성공 확인
func TestHeadObjectIfModifiedSinceGood(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket, key := s.bucket(t, 18), "test_head_object_if_modified_since_good"
	put(t, s, bucket, key, "bar", nil)
	past := time.Date(1994, 9, 29, 19, 43, 31, 0, time.UTC)
	input := &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}
	input.IfModifiedSince = &past
	out, err := s.client.HeadObject(context.Background(), input)
	if err != nil || aws.ToInt64(out.ContentLength) != 3 {
		t.Fatalf("head=%#v err=%v", out, err)
	}
}

// HeadObject에서 오브젝트 업로드 이후 시간의 If-Modified-Since 조건으로 304 반환 확인
func TestHeadObjectIfModifiedSinceFailed(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket, key := s.bucket(t, 19), "test_head_object_if_modified_since_failed"
	put(t, s, bucket, key, "bar", nil)
	head := headObject(t, s.client, bucket, key)
	after := head.LastModified.Add(time.Second)
	input := &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}
	input.IfModifiedSince = &after
	time.Sleep(time.Second)
	_, err := s.client.HeadObject(context.Background(), input)
	assertHTTPError(t, err, 304)
}

// HeadObject에서 오브젝트 업로드 이전 시간의 If-Unmodified-Since 조건으로 412 실패 확인
func TestHeadObjectIfUnmodifiedSinceGood(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket, key := s.bucket(t, 20), "test_head_object_if_unmodified_since_good"
	put(t, s, bucket, key, "bar", nil)
	past := time.Date(1994, 9, 29, 19, 43, 31, 0, time.UTC)
	input := &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}
	input.IfUnmodifiedSince = &past
	_, err := s.client.HeadObject(context.Background(), input)
	assertHTTPError(t, err, 412)
}

// HeadObject에서 오브젝트 업로드 이후 시간의 If-Unmodified-Since 조건으로 성공 확인
func TestHeadObjectIfUnmodifiedSinceFailed(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket, key := s.bucket(t, 21), "test_head_object_if_unmodified_since_failed"
	put(t, s, bucket, key, "bar", nil)
	future := time.Date(2100, 9, 29, 19, 43, 31, 0, time.UTC)
	input := &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}
	input.IfUnmodifiedSince = &future
	out, err := s.client.HeadObject(context.Background(), input)
	if err != nil || aws.ToInt64(out.ContentLength) != 3 {
		t.Fatalf("head=%#v err=%v", out, err)
	}
}

// 지정한 범위로 오브젝트 다운로드가 가능한지 확인
func TestRangedRequestResponseCode(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket, key := s.bucket(t, 22), "obj"
	content, requestRange, want, wantRange := "contentData", "bytes=4-7", "entD", "bytes 4-7/11"
	put(t, s, bucket, key, content, nil)
	out, err := s.client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Range: aws.String(requestRange)})
	if err != nil {
		t.Fatal(err)
	}
	defer out.Body.Close()
	body, err := io.ReadAll(out.Body)
	if err != nil || string(body) != want || aws.ToString(out.ContentRange) != wantRange {
		t.Fatalf("length=%d range=%q want length=%d range=%q err=%v", len(body), aws.ToString(out.ContentRange), len(want), wantRange, err)
	}
}

// 지정한 범위로 대용량인 오브젝트 다운로드가 가능한지 확인
func TestRangedBigRequestResponseCode(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket, key := s.bucket(t, 23), "obj"
	content := stringsRepeatPattern(8 * 1024 * 1024)
	requestRange := "bytes=3145728-5242880"
	want := content[3145728:5242881]
	wantRange := "bytes 3145728-5242880/8388608"
	put(t, s, bucket, key, content, nil)
	out, err := s.client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Range: aws.String(requestRange)})
	if err != nil {
		t.Fatal(err)
	}
	defer out.Body.Close()
	body, err := io.ReadAll(out.Body)
	if err != nil || string(body) != want || aws.ToString(out.ContentRange) != wantRange {
		t.Fatalf("length=%d range=%q want length=%d range=%q err=%v", len(body), aws.ToString(out.ContentRange), len(want), wantRange, err)
	}
}

// 특정지점부터 끝까지 오브젝트 다운로드 가능한지 확인
func TestRangedRequestSkipLeadingBytesResponseCode(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket, key := s.bucket(t, 24), "obj"
	content := "contentData"
	requestRange, want, wantRange := "bytes=4-", content[4:], "bytes 4-10/11"
	put(t, s, bucket, key, content, nil)
	out, err := s.client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Range: aws.String(requestRange)})
	if err != nil {
		t.Fatal(err)
	}
	defer out.Body.Close()
	body, err := io.ReadAll(out.Body)
	if err != nil || string(body) != want || aws.ToString(out.ContentRange) != wantRange {
		t.Fatalf("length=%d range=%q want length=%d range=%q err=%v", len(body), aws.ToString(out.ContentRange), len(want), wantRange, err)
	}
}

// 끝에서 부터 특정 길이까지 오브젝트 다운로드 가능한지 확인
func TestRangedRequestReturnTrailingBytesResponseCode(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket, key := s.bucket(t, 25), "obj"
	content := "contentData"
	requestRange, want, wantRange := "bytes=-7", content[len(content)-7:], "bytes 4-10/11"
	put(t, s, bucket, key, content, nil)
	out, err := s.client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Range: aws.String(requestRange)})
	if err != nil {
		t.Fatal(err)
	}
	defer out.Body.Close()
	body, err := io.ReadAll(out.Body)
	if err != nil || string(body) != want || aws.ToString(out.ContentRange) != wantRange {
		t.Fatalf("length=%d range=%q want length=%d range=%q err=%v", len(body), aws.ToString(out.ContentRange), len(want), wantRange, err)
	}
}

// 오브젝트의 크기를 초과한 범위를 설정하여 다운로드 할경우 실패 확인
func TestRangedRequestInvalidRange(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket, key := s.bucket(t, 26), "obj"
	put(t, s, bucket, key, "contentData", nil)
	_, err := s.client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Range: aws.String("bytes=40-50")})
	assertS3Error(t, err, 416, "InvalidRange")
}

// 비어있는 오브젝트를 범위를 지정하여 다운로드 실패 확인
func TestRangedRequestEmptyObject(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket, key := s.bucket(t, 27), "obj"
	put(t, s, bucket, key, "", nil)
	_, err := s.client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Range: aws.String("bytes=40-50")})
	assertS3Error(t, err, 416, "InvalidRange")
}

// 같은 오브젝트를 여러번 반복하여 다운로드 성공 확인
func TestGetObjectMany(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket, key := s.bucket(t, 28), "foo"
	data := stringsRepeatPattern(15 * 1024 * 1024)
	put(t, s, bucket, key, data, nil)
	for i := 0; i < 50; i++ {
		input := &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}
		start, end := 0, len(data)-1
		out := getObject(t, s.client, input)
		body, err := io.ReadAll(out.Body)
		out.Body.Close()
		if err != nil || !bytes.Equal(body, []byte(data[start:end+1])) {
			t.Fatalf("iteration=%d range=%d-%d length=%d err=%v", i, start, end, len(body), err)
		}
	}
}

// 같은 오브젝트를 여러번 반복하여 Range 다운로드 성공 확인
func TestRangeObjectMany(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket, key := s.bucket(t, 29), "foo"
	data := stringsRepeatPattern(15 * 1024 * 1024)
	put(t, s, bucket, key, data, nil)
	for i := 0; i < 50; i++ {
		start := (i * 7919) % (len(data) - 65536)
		end := start + 65535
		input := &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Range: aws.String(fmt.Sprintf("bytes=%d-%d", start, end))}
		out := getObject(t, s.client, input)
		body, err := io.ReadAll(out.Body)
		out.Body.Close()
		if err != nil || !bytes.Equal(body, []byte(data[start:end+1])) {
			t.Fatalf("iteration=%d range=%d-%d length=%d err=%v", i, start, end, len(body), err)
		}
	}
}

// GetObject의 반환헤더값을 설정하여 업로드 할 경우 적용되었는지 확인
func TestObjectResponseHeaders(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket, key := s.bucket(t, 30), "testObjectResponseHeaders"
	put(t, s, bucket, key, key, nil)
	expires := time.Now().UTC().Truncate(time.Second)
	out := getObject(t, s.client, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), ResponseCacheControl: aws.String("no-cache"), ResponseContentDisposition: aws.String("bla"), ResponseContentEncoding: aws.String("aaa"), ResponseContentLanguage: aws.String("esperanto"), ResponseContentType: aws.String("foo/bar"), ResponseExpires: &expires})
	defer out.Body.Close()
	if aws.ToString(out.CacheControl) != "no-cache" || aws.ToString(out.ContentDisposition) != "bla" || aws.ToString(out.ContentEncoding) != "aaa" || aws.ToString(out.ContentLanguage) != "esperanto" || aws.ToString(out.ContentType) != "foo/bar" {
		t.Fatalf("response headers cache=%q disposition=%q encoding=%q language=%q type=%q", aws.ToString(out.CacheControl), aws.ToString(out.ContentDisposition), aws.ToString(out.ContentEncoding), aws.ToString(out.ContentLanguage), aws.ToString(out.ContentType))
	}
}

// 멀티파트로 업로드 된 오브젝트를 다운로드 할때 파트 번호를 지정하여 다운로드 가능한지 확인
func TestMultipartObjectRange(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket, key := s.bucket(t, 31), "testMultipartObjectRange"
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

// GetObject에서 파일을 읽지 않고 버려도 무시되는지 확인
func TestGetObjectIgnore(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	bucket, key := s.bucket(t, 32), "testObjectIgnore"
	put(t, s, bucket, key, key, nil)
	out := getObject(t, s.client, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if aws.ToInt64(out.ContentLength) != int64(len(key)) {
		t.Fatalf("length=%v", out.ContentLength)
	}
	out.Body.Close()
}

// 삭제한 파일 GetObject 실패 확인
func TestGetObjectAfterDelete(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket, key, body := s.bucket(t, 33), "deleted", "testContent"
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

// 버저닝한 버킷에서 삭제한 파일 GetObject 실패 확인
func TestGetObjectAfterDeleteVersioning(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket, key, body := s.bucket(t, 34), "deleted", "testContent"
	enableVersioning(t, s, bucket)
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

// 버저닝한 버킷에서 DeleteMarker로 GetObject 실패 확인
func TestGetObjectDeleteMarker(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket, key, body := s.bucket(t, 35), "marker", "testContent"
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
