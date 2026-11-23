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
	_, err := s.client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String(s.bucket(t, 1)), Key: aws.String("TestObjectReadNotExist")})
	assertS3Error(t, err, 404, "NoSuchKey")
}

// 존재하는 오브젝트 이름과 ETag 값으로 오브젝트를 가져오는지 확인
func TestGetObjectIfMatchGood(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket, key := s.bucket(t, 2), "TestGetObjectIfMatchGood"
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
	bucket, key := s.bucket(t, 3), "TestGetObjectIfMatchFailed"
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
	bucket, key := s.bucket(t, 4), "TestGetObjectIfNoneMatchGood"
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
	bucket, key := s.bucket(t, 5), "TestGetObjectIfNoneMatchFailed"
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
	bucket, key := s.bucket(t, 6), "TestGetObjectIfModifiedSinceGood"
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
	bucket, key := s.bucket(t, 7), "TestGetObjectIfModifiedSinceFailed"
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
	bucket, key := s.bucket(t, 8), "TestGetObjectIfUnmodifiedSinceGood"
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
	bucket, key := s.bucket(t, 9), "TestGetObjectIfUnmodifiedSinceFailed"
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
	bucket, key := s.bucket(t, 10), "TestGetObjectIfMatchWithIfUnmodifiedSince"
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
	bucket, key := s.bucket(t, 11), "TestGetObjectIfNoneMatchWithIfModifiedSince"
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
	bucket, key := s.bucket(t, 12), "TestGetObjectIfMatchAndIfNoneMatch"
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
	bucket, key := s.bucket(t, 13), "TestGetObjectIfMatchAndIfNoneMatchAny"
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
	bucket, key := s.bucket(t, 14), "TestHeadObjectIfMatchGood"
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
	bucket, key := s.bucket(t, 15), "TestHeadObjectIfMatchFailed"
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
	bucket, key := s.bucket(t, 16), "TestHeadObjectIfNoneMatchGood"
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
	bucket, key := s.bucket(t, 17), "TestHeadObjectIfNoneMatchFailed"
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
	bucket, key := s.bucket(t, 18), "TestHeadObjectIfModifiedSinceGood"
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
	bucket, key := s.bucket(t, 19), "TestHeadObjectIfModifiedSinceFailed"
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
	bucket, key := s.bucket(t, 20), "TestHeadObjectIfUnmodifiedSinceGood"
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
	bucket, key := s.bucket(t, 21), "TestHeadObjectIfUnmodifiedSinceFailed"
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

// 모든 체크섬 알고리즘으로 업로드한 오브젝트의 Range 다운로드 내용 확인
func TestRangeGetChecksum(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket := s.bucket(t, 36)
	data := []byte(stringsRepeatPattern(5 * 1024 * 1024))
	algorithms := []types.ChecksumAlgorithm{
		types.ChecksumAlgorithmCrc32,
		types.ChecksumAlgorithmCrc32c,
		types.ChecksumAlgorithmSha1,
		types.ChecksumAlgorithmSha256,
	}

	for _, algorithm := range algorithms {
		algorithm := algorithm
		t.Run(string(algorithm), func(t *testing.T) {
			key := "TestRangeGetChecksum/" + string(algorithm)
			out, err := s.client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket:            aws.String(bucket),
				Key:               aws.String(key),
				Body:              bytes.NewReader(data),
				ChecksumAlgorithm: algorithm,
			})
			if err != nil {
				t.Fatal(err)
			}

			want := checksumValue(algorithm, data)
			got := map[types.ChecksumAlgorithm]string{
				types.ChecksumAlgorithmCrc32:  aws.ToString(out.ChecksumCRC32),
				types.ChecksumAlgorithmCrc32c: aws.ToString(out.ChecksumCRC32C),
				types.ChecksumAlgorithmSha1:   aws.ToString(out.ChecksumSHA1),
				types.ChecksumAlgorithmSha256: aws.ToString(out.ChecksumSHA256),
			}[algorithm]
			if got != want {
				t.Fatalf("checksum=%q want=%q", got, want)
			}

			for i := 0; i < 50; i++ {
				start := (i * 7919) % (len(data) - 65536)
				end := start + 65535
				input := &s3.GetObjectInput{
					Bucket: aws.String(bucket),
					Key:    aws.String(key),
					Range:  aws.String(fmt.Sprintf("bytes=%d-%d", start, end)),
				}
				response := getObject(t, s.client, input)
				body, readErr := io.ReadAll(response.Body)
				response.Body.Close()
				if readErr != nil || !bytes.Equal(body, data[start:end+1]) {
					t.Fatalf("iteration=%d range=%d-%d length=%d err=%v", i, start, end, len(body), readErr)
				}
			}
		})
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

// putDigits는 10바이트(0123456789) 오브젝트를 하나 넣은 버킷을 만든다.
func putDigits(t *testing.T, s *suite, id int, key string) string {
	t.Helper()
	bucket := s.bucket(t, id)
	put(t, s, bucket, key, "0123456789", nil)
	return bucket
}

// getObjectBody는 GetObject 응답과 본문을 함께 돌려준다.
func getObjectBody(t *testing.T, client *s3.Client, input *s3.GetObjectInput) (*s3.GetObjectOutput, string) {
	t.Helper()
	out, err := client.GetObject(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Body.Close()
	body, err := io.ReadAll(out.Body)
	if err != nil {
		t.Fatal(err)
	}
	return out, string(body)
}

// 없는 버킷의 오브젝트를 조회하면 NoSuchBucket
func TestGetObjectBucketNotExist(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	b := s.bucketNameOnly(37)
	_, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(b), Key: aws.String("key")})
	assertS3Error(t, err, 404, "NoSuchBucket")
	// HEAD 는 본문이 없어 상태 코드만 온다
	_, err = s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String("key")})
	assertS3Status(t, err, 404)
}

// 없는 오브젝트의 HEAD 는 404
func TestHeadObjectNotExist(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	_, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(s.bucket(t, 38)), Key: aws.String("key")})
	assertS3Status(t, err, 404)
}

// 권한이 없는 사용자와 익명 사용자는 조회할 수 없다
func TestGetObjectAccessDenied(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	alt := altClientOrSkip(t, s)
	key := "TestGetObjectAccessDenied"
	b := putDigits(t, s, 39, key)
	for _, other := range []*s3.Client{alt, anonymousClient(s)} {
		_, err := other.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(b), Key: aws.String(key)})
		assertS3Error(t, err, 403, "AccessDenied")
		// 목록 권한이 없으면 없는 키도 404 가 아니라 403 이다
		_, err = other.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(b), Key: aws.String("not_exist")})
		assertS3Error(t, err, 403, "AccessDenied")
		_, err = other.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String(key)})
		assertS3Status(t, err, 403)
	}
}

// 여러 구간, 잘못된 형식, 다른 단위, 뒤집힌 구간은 무시하고 전체를 200 으로 돌려준다
func TestRangedRequestIgnored(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	key := "TestRangedRequestIgnored"
	b := putDigits(t, s, 40, key)
	for _, value := range []string{"bytes=0-1,3-4", "bytes=abc", "items=0-1", "bytes=5-2"} {
		out, body := getObjectBody(t, s.client, &s3.GetObjectInput{Bucket: aws.String(b), Key: aws.String(key), Range: aws.String(value)})
		if status := responseStatus(t, out.ResultMetadata); status != 200 || out.ContentRange != nil || body != "0123456789" {
			t.Fatalf("range=%q status=%d ContentRange=%q body=%q", value, status, aws.ToString(out.ContentRange), body)
		}
	}
}

// 크기를 넘는 구간은 잘라서 돌려주고, 시작이 크기 이상이면 416
func TestRangedRequestBeyondEnd(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	key := "TestRangedRequestBeyondEnd"
	b := putDigits(t, s, 41, key)
	// 끝이 크기를 넘으면 마지막 바이트까지로 잘린다
	out, body := getObjectBody(t, s.client, &s3.GetObjectInput{Bucket: aws.String(b), Key: aws.String(key), Range: aws.String("bytes=5-100")})
	if status := responseStatus(t, out.ResultMetadata); status != 206 || aws.ToString(out.ContentRange) != "bytes 5-9/10" || body != "56789" {
		t.Fatalf("status=%d ContentRange=%q body=%q", status, aws.ToString(out.ContentRange), body)
	}

	// 뒤에서부터의 길이가 크기를 넘으면 전체를 206 으로 돌려준다
	out, body = getObjectBody(t, s.client, &s3.GetObjectInput{Bucket: aws.String(b), Key: aws.String(key), Range: aws.String("bytes=-100")})
	if status := responseStatus(t, out.ResultMetadata); status != 206 || aws.ToString(out.ContentRange) != "bytes 0-9/10" || body != "0123456789" {
		t.Fatalf("status=%d ContentRange=%q body=%q", status, aws.ToString(out.ContentRange), body)
	}

	// 시작이 크기와 같거나, 뒤에서 0바이트를 요청하면 416
	for _, value := range []string{"bytes=10-20", "bytes=-0"} {
		_, err := s.client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String(b), Key: aws.String(key), Range: aws.String(value)})
		assertS3Error(t, err, 416, "InvalidRange")
	}
}

// HEAD 에도 Range 를 줄 수 있다
func TestHeadObjectRange(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	key := "TestHeadObjectRange"
	b := putDigits(t, s, 42, key)
	out, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String(key), Range: aws.String("bytes=0-4")})
	if err != nil {
		t.Fatal(err)
	}
	if status := responseStatus(t, out.ResultMetadata); status != 206 || aws.ToInt64(out.ContentLength) != 5 || aws.ToString(out.ContentRange) != "bytes 0-4/10" {
		t.Fatalf("status=%d ContentLength=%d ContentRange=%q", status, aws.ToInt64(out.ContentLength), aws.ToString(out.ContentRange))
	}
}

// 멀티파트가 아닌 오브젝트는 1번 파트가 전체다
func TestGetObjectPartNumber(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	key := "TestGetObjectPartNumber"
	b := putDigits(t, s, 43, key)
	out, body := getObjectBody(t, s.client, &s3.GetObjectInput{Bucket: aws.String(b), Key: aws.String(key), PartNumber: aws.Int32(1)})
	if status := responseStatus(t, out.ResultMetadata); status != 206 || aws.ToString(out.ContentRange) != "bytes 0-9/10" || out.PartsCount != nil || body != "0123456789" {
		t.Fatalf("status=%d ContentRange=%q PartsCount=%v body=%q", status, aws.ToString(out.ContentRange), out.PartsCount, body)
	}

	head, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String(key), PartNumber: aws.Int32(1)})
	if err != nil {
		t.Fatal(err)
	}
	if status := responseStatus(t, head.ResultMetadata); status != 206 || aws.ToInt64(head.ContentLength) != 10 {
		t.Fatalf("HeadObject status=%d ContentLength=%d", status, aws.ToInt64(head.ContentLength))
	}

	_, err = s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(b), Key: aws.String(key), PartNumber: aws.Int32(2)})
	assertS3Error(t, err, 416, "InvalidPartNumber")
}

// 파트 번호는 1~10000 이고 Range 와 함께 쓸 수 없다
func TestGetObjectPartNumberInvalid(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	key := "TestGetObjectPartNumberInvalid"
	b := putDigits(t, s, 44, key)
	for _, partNumber := range []int32{0, 10001} {
		_, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(b), Key: aws.String(key), PartNumber: aws.Int32(partNumber)})
		assertS3Error(t, err, 400, "InvalidArgument")
	}
	_, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(b), Key: aws.String(key), PartNumber: aws.Int32(1), Range: aws.String("bytes=0-1")})
	assertS3Error(t, err, 400, "InvalidRequest")
}

// 버저닝을 쓰지 않는 버킷의 오브젝트는 null 버전이다
func TestGetObjectVersionIdUnversioned(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	key := "TestGetObjectVersionIdUnversioned"
	b := putDigits(t, s, 45, key)
	if _, body := getObjectBody(t, s.client, &s3.GetObjectInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: aws.String("null")}); body != "0123456789" {
		t.Fatalf("body=%q", body)
	}
	_, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: aws.String("invalid")})
	assertS3Error(t, err, 400, "InvalidArgument")
	_, err = s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: aws.String("invalid")})
	assertS3Status(t, err, 400)
}

// 버저닝을 켠 뒤에 만든 오브젝트에는 null 버전이 없다
func TestGetObjectNullVersionNotExist(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	b, key := s.bucket(t, 46), "TestGetObjectNullVersionNotExist"
	enableVersioning(t, s, b)
	put(t, s, b, key, "content", nil)
	_, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: aws.String("null")})
	assertS3Error(t, err, 404, "NoSuchVersion")
	_, err = s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String(key), VersionId: aws.String("null")})
	assertS3Status(t, err, 404)
}

// 익명 요청에는 response-* 재정의를 쓸 수 없다
func TestGetObjectResponseHeadersAnonymous(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	key := "TestGetObjectResponseHeadersAnonymous"
	b := putDigits(t, s, 47, key)
	_, err := anonymousClient(s).GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String(b), Key: aws.String(key), ResponseContentType: aws.String("text/html")})
	assertS3Error(t, err, 400, "InvalidRequest")
}

// HEAD 에도 response-* 재정의가 적용된다
func TestHeadObjectResponseHeaders(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	key := "TestHeadObjectResponseHeaders"
	b := putDigits(t, s, 48, key)
	out, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String(b), Key: aws.String(key),
		ResponseContentType:        aws.String("text/html"),
		ResponseContentDisposition: aws.String("inline"),
		ResponseCacheControl:       aws.String("max-age=1"),
		ResponseContentEncoding:    aws.String("identity"),
		ResponseContentLanguage:    aws.String("en"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if aws.ToString(out.ContentType) != "text/html" || aws.ToString(out.ContentDisposition) != "inline" || aws.ToString(out.CacheControl) != "max-age=1" || aws.ToString(out.ContentEncoding) != "identity" || aws.ToString(out.ContentLanguage) != "en" {
		t.Fatalf("type=%q disposition=%q cache=%q encoding=%q language=%q", aws.ToString(out.ContentType), aws.ToString(out.ContentDisposition), aws.ToString(out.CacheControl), aws.ToString(out.ContentEncoding), aws.ToString(out.ContentLanguage))
	}
}

// 아무것도 지정하지 않고 올린 오브젝트의 기본 응답 헤더
// (Content-Type 기본값 binary/octet-stream 은 확인하지 않는다: go SDK 가 PutObject 에 항상 application/octet-stream 을 붙인다)
func TestGetObjectDefaultHeaders(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	key := "TestGetObjectDefaultHeaders"
	b := putDigits(t, s, 49, key)
	check := func(name string, acceptRanges, etag *string, contentLength *int64, lastModified *time.Time) {
		t.Helper()
		if aws.ToString(acceptRanges) != "bytes" || aws.ToInt64(contentLength) != 10 || aws.ToString(etag) != `"781e5e245d69b566979b86e28d23f2c7"` || lastModified == nil {
			t.Fatalf("%s AcceptRanges=%q ContentLength=%d ETag=%q LastModified=%v", name, aws.ToString(acceptRanges), aws.ToInt64(contentLength), aws.ToString(etag), lastModified)
		}
	}
	got, _ := getObjectBody(t, s.client, &s3.GetObjectInput{Bucket: aws.String(b), Key: aws.String(key)})
	check("GetObject", got.AcceptRanges, got.ETag, got.ContentLength, got.LastModified)
	head := headObject(t, s.client, b, key)
	check("HeadObject", head.AcceptRanges, head.ETag, head.ContentLength, head.LastModified)
}

// If-Match: * 는 오브젝트가 있으면 통과, If-None-Match: * 는 304
func TestGetObjectIfMatchWildcard(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	key := "TestGetObjectIfMatchWildcard"
	b := putDigits(t, s, 50, key)
	if _, body := getObjectBody(t, s.client, &s3.GetObjectInput{Bucket: aws.String(b), Key: aws.String(key), IfMatch: aws.String("*")}); body != "0123456789" {
		t.Fatalf("body=%q", body)
	}
	_, err := s.client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String(b), Key: aws.String(key), IfNoneMatch: aws.String("*")})
	assertS3Status(t, err, 304)
}
