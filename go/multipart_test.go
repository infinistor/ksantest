package s3tests

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// 비어있는 오브젝트를 멀티파트로 업로드 실패 확인
func TestMultipartUploadEmpty(t *testing.T) {
	t.Parallel()
	f := newMultipartFixture(t, "TestMultipartUploadEmpty", 1)
	_, err := f.s.client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{Bucket: aws.String(f.bucket), Key: aws.String(f.key), UploadId: f.created.UploadId, MultipartUpload: &types.CompletedMultipartUpload{}})
	assertHTTPError(t, err, 400)
}

// 파트 크기보다 작은 오브젝트를 멀티파트 업로드시 성공확인
func TestMultipartUploadSmall(t *testing.T) {
	t.Parallel()
	f := newMultipartFixture(t, "TestMultipartUploadSmall", 2)
	body := bytes.Repeat([]byte{'a'}, 1024)
	part := uploadMultipartPart(t, f, 1, body)
	completeMultipartParts(t, f, []types.CompletedPart{{ETag: part.ETag, PartNumber: aws.Int32(1)}})
	assertObjectBytes(t, f.s.client, f.bucket, f.key, body)
}

// 버킷a에서 버킷b로 멀티파트 복사 성공확인
func TestMultipartCopySmall(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t, 3)
	source, target := "TestMultipartCopySmall?Source", "TestMultipartCopySmall&Target"
	body := []byte("small")
	put(t, s, b, source, string(body), nil)
	created, err := s.client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: aws.String(b), Key: aws.String(target)})
	if err != nil {
		t.Fatal(err)
	}
	input := &s3.UploadPartCopyInput{Bucket: aws.String(b), Key: aws.String(target), UploadId: created.UploadId, PartNumber: aws.Int32(1), CopySource: copySource(b, source, ""), CopySourceRange: aws.String(fmt.Sprintf("bytes=0-%d", len(body)-1))}
	copied, err := s.client.UploadPartCopy(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{Bucket: aws.String(b), Key: aws.String(target), UploadId: created.UploadId, MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{{ETag: copied.CopyPartResult.ETag, PartNumber: aws.Int32(1)}}}}); err != nil {
		t.Fatal(err)
	}
	assertObjectBytes(t, s.client, b, target, body)
}

// 범위설정을 잘못한 멀티파트 복사 실패 확인
func TestMultipartCopyInvalidRange(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t, 4)
	source, target := "TestMultipartCopyInvalidRange?Source", "TestMultipartCopyInvalidRange&Target"
	body := randomTextToLong(6 * 1024 * 1024)
	put(t, s, b, source, string(body), nil)
	created, err := s.client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: aws.String(b), Key: aws.String(target)})
	if err != nil {
		t.Fatal(err)
	}
	input := &s3.UploadPartCopyInput{Bucket: aws.String(b), Key: aws.String(target), UploadId: created.UploadId, PartNumber: aws.Int32(1), CopySource: copySource(b, source, ""), CopySourceRange: aws.String("bytes=99999999-100000000")}
	_, err = s.client.UploadPartCopy(context.Background(), input)
	if err == nil {
		t.Fatal("invalid range accepted")
	}
}

// 범위를 지정한 멀티파트 복사 성공확인
func TestMultipartCopyWithoutRange(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t, 5)
	source, target := "TestMultipartCopyWithoutRange?Source", "TestMultipartCopyWithoutRange&Target"
	body := randomTextToLong(6 * 1024 * 1024)
	put(t, s, b, source, string(body), nil)
	created, err := s.client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: aws.String(b), Key: aws.String(target)})
	if err != nil {
		t.Fatal(err)
	}
	input := &s3.UploadPartCopyInput{Bucket: aws.String(b), Key: aws.String(target), UploadId: created.UploadId, PartNumber: aws.Int32(1), CopySource: copySource(b, source, "")}
	copied, err := s.client.UploadPartCopy(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{Bucket: aws.String(b), Key: aws.String(target), UploadId: created.UploadId, MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{{ETag: copied.CopyPartResult.ETag, PartNumber: aws.Int32(1)}}}}); err != nil {
		t.Fatal(err)
	}
	assertObjectBytes(t, s.client, b, target, body)
}

// 특수문자로 오브젝트 이름을 만들어 업로드한 오브젝트를 멀티파트 복사 성공 확인
func TestMultipartCopySpecialNames(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t, 6)
	source, target := "TestMultipartCopySpecialNames?Source", "TestMultipartCopySpecialNames&Target"
	body := randomTextToLong(6 * 1024 * 1024)
	put(t, s, b, source, string(body), nil)
	created, err := s.client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: aws.String(b), Key: aws.String(target)})
	if err != nil {
		t.Fatal(err)
	}
	input := &s3.UploadPartCopyInput{Bucket: aws.String(b), Key: aws.String(target), UploadId: created.UploadId, PartNumber: aws.Int32(1), CopySource: copySource(b, source, ""), CopySourceRange: aws.String(fmt.Sprintf("bytes=0-%d", len(body)-1))}
	copied, err := s.client.UploadPartCopy(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{Bucket: aws.String(b), Key: aws.String(target), UploadId: created.UploadId, MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{{ETag: copied.CopyPartResult.ETag, PartNumber: aws.Int32(1)}}}}); err != nil {
		t.Fatal(err)
	}
	assertObjectBytes(t, s.client, b, target, body)
}

// 멀티파트 업로드 확인
func TestMultipartUpload(t *testing.T) {
	t.Parallel()
	f := newMultipartFixture(t, "TestMultipartUpload", 7)
	body := bytes.Repeat([]byte{'a'}, 5*1024*1024)
	part := uploadMultipartPart(t, f, 1, body)
	completeMultipartParts(t, f, []types.CompletedPart{{ETag: part.ETag, PartNumber: aws.Int32(1)}})
	assertObjectBytes(t, f.s.client, f.bucket, f.key, body)
}

// 버저닝되어있는 버킷에서 오브젝트를 멀티파트로 복사 성공 확인
func TestMultipartCopyVersioned(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t, 8)
	source, target := "TestMultipartCopyVersioned?Source", "TestMultipartCopyVersioned&Target"
	body := randomTextToLong(6 * 1024 * 1024)
	put(t, s, b, source, string(body), nil)
	enableVersioning(t, s, b)
	put(t, s, b, source, string(body), nil)
	created, err := s.client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: aws.String(b), Key: aws.String(target)})
	if err != nil {
		t.Fatal(err)
	}
	input := &s3.UploadPartCopyInput{Bucket: aws.String(b), Key: aws.String(target), UploadId: created.UploadId, PartNumber: aws.Int32(1), CopySource: copySource(b, source, ""), CopySourceRange: aws.String(fmt.Sprintf("bytes=0-%d", len(body)-1))}
	copied, err := s.client.UploadPartCopy(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{Bucket: aws.String(b), Key: aws.String(target), UploadId: created.UploadId, MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{{ETag: copied.CopyPartResult.ETag, PartNumber: aws.Int32(1)}}}}); err != nil {
		t.Fatal(err)
	}
	assertObjectBytes(t, s.client, b, target, body)
}

// 멀티파트 업로드중 같은 파츠를 여러번 업로드시 성공 확인
func TestMultipartUploadResendPart(t *testing.T) {
	t.Parallel()
	f := newMultipartFixture(t, "TestMultipartUploadResendPart", 9)
	size := 5 * 1024 * 1024
	original := bytes.Repeat([]byte{'a'}, size)
	uploadMultipartPart(t, f, 1, original)
	replacement := bytes.Repeat([]byte("z"), size)
	part := uploadMultipartPart(t, f, 1, replacement)
	completeMultipartParts(t, f, []types.CompletedPart{{ETag: part.ETag, PartNumber: aws.Int32(1)}})
	assertObjectBytes(t, f.s.client, f.bucket, f.key, replacement)
}

// 한 오브젝트에 대해 다양한 크기의 멀티파트 업로드 성공 확인
func TestMultipartUploadMultipleSizes(t *testing.T) {
	t.Parallel()
	f := newMultipartFixture(t, "TestMultipartUploadMultipleSizes", 10)
	sizes := []int{5 * 1024 * 1024, 6 * 1024 * 1024, 1024}
	parts := make([]types.CompletedPart, 0, len(sizes))
	var want []byte
	for i, size := range sizes {
		body := bytes.Repeat([]byte{byte('a' + i)}, size)
		part := uploadMultipartPart(t, f, int32(i+1), body)
		parts = append(parts, types.CompletedPart{ETag: part.ETag, PartNumber: aws.Int32(int32(i + 1))})
		want = append(want, body...)
	}
	completeMultipartParts(t, f, parts)
	assertObjectBytes(t, f.s.client, f.bucket, f.key, want)
}

// 한 오브젝트에 대해 다양한 크기의 오브젝트 멀티파트 복사 성공 확인
func TestMultipartCopyMultipleSizes(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t, 11)
	source, target := "TestMultipartCopyMultipleSizes?Source", "TestMultipartCopyMultipleSizes&Target"
	body := randomTextToLong(6 * 1024 * 1024)
	put(t, s, b, source, string(body), nil)
	created, err := s.client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: aws.String(b), Key: aws.String(target)})
	if err != nil {
		t.Fatal(err)
	}
	input := &s3.UploadPartCopyInput{Bucket: aws.String(b), Key: aws.String(target), UploadId: created.UploadId, PartNumber: aws.Int32(1), CopySource: copySource(b, source, ""), CopySourceRange: aws.String(fmt.Sprintf("bytes=0-%d", len(body)-1))}
	copied, err := s.client.UploadPartCopy(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{Bucket: aws.String(b), Key: aws.String(target), UploadId: created.UploadId, MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{{ETag: copied.CopyPartResult.ETag, PartNumber: aws.Int32(1)}}}}); err != nil {
		t.Fatal(err)
	}
	assertObjectBytes(t, s.client, b, target, body)
}

// 멀티파트 업로드시에 파츠의 크기가 너무 작을 경우 업로드 실패 확인
func TestMultipartUploadSizeTooSmall(t *testing.T) {
	t.Parallel()
	f := newMultipartFixture(t, "TestMultipartUploadSizeTooSmall", 12)
	one := uploadMultipartPart(t, f, 1, bytes.Repeat([]byte("a"), 1024))
	two := uploadMultipartPart(t, f, 2, bytes.Repeat([]byte("b"), 1024))
	_, err := f.s.client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{Bucket: aws.String(f.bucket), Key: aws.String(f.key), UploadId: f.created.UploadId, MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{{ETag: one.ETag, PartNumber: aws.Int32(1)}, {ETag: two.ETag, PartNumber: aws.Int32(2)}}}})
	assertS3Error(t, err, 400, "EntityTooSmall")
}

// 내용물을 채운 멀티파트 업로드 성공 확인
func TestMultipartUploadContents(t *testing.T) {
	t.Parallel()
	f := newMultipartFixture(t, "TestMultipartUploadContents", 13)
	body := bytes.Repeat([]byte{'a'}, 5*1024*1024)
	part := uploadMultipartPart(t, f, 1, body)
	completeMultipartParts(t, f, []types.CompletedPart{{ETag: part.ETag, PartNumber: aws.Int32(1)}})
	assertObjectBytes(t, f.s.client, f.bucket, f.key, body)
}

// 업로드한 오브젝트를 멀티파트 업로드로 덮어쓰기 성공 확인
func TestMultipartUploadOverwriteExistingObject(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket := s.bucket(t, 14)
	key := "TestMultipartUploadOverwriteExistingObject"
	partBody := bytes.Repeat([]byte("a"), 5*1024*1024)
	put(t, s, bucket, key, string(partBody), nil)

	created, err := s.client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = s.client.AbortMultipartUpload(context.Background(), &s3.AbortMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(key), UploadId: created.UploadId})
	})

	parts := make([]types.CompletedPart, 0, 2)
	var want []byte
	for number := int32(1); number <= 2; number++ {
		out, err := s.client.UploadPart(context.Background(), &s3.UploadPartInput{Bucket: aws.String(bucket), Key: aws.String(key), UploadId: created.UploadId, PartNumber: aws.Int32(number), Body: bytes.NewReader(partBody)})
		if err != nil {
			t.Fatal(err)
		}
		parts = append(parts, types.CompletedPart{ETag: out.ETag, PartNumber: aws.Int32(number)})
		want = append(want, partBody...)
	}
	if _, err := s.client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(key), UploadId: created.UploadId, MultipartUpload: &types.CompletedMultipartUpload{Parts: parts}}); err != nil {
		t.Fatal(err)
	}
	assertObjectBytes(t, s.client, bucket, key, want)
}

// 멀티파트로 업로드된 오브젝트를 putObject로 덮어쓴 뒤 파일이 정상인지 확인
func TestPutObjectOverwriteMultipartUpload(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket := s.bucket(t, 48)
	key := "TestPutObjectOverwriteMultipartUpload"
	completeMultipart(t, s.client, bucket, key, randomTextToLong(10*1024*1024), false, nil)
	content := randomTextToLong(1 * 1024 * 1024)
	putBytes(t, s.client, bucket, key, content)
	head, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil || aws.ToInt64(head.ContentLength) != int64(len(content)) {
		t.Fatalf("HeadObject length=%d err=%v want=%d", aws.ToInt64(head.ContentLength), err, len(content))
	}
	assertObjectBytes(t, s.client, bucket, key, content)
	assertObjectRanges(t, s.client, bucket, key, content, []int{1024})
}

// 멀티파트 업로드하는 도중 중단 성공 확인
func TestAbortMultipartUpload(t *testing.T) {
	t.Parallel()
	f := newMultipartFixture(t, "TestAbortMultipartUpload", 15)
	uploadMultipartPart(t, f, 1, bytes.Repeat([]byte("x"), 5*1024*1024))
	listed, err := f.s.client.ListMultipartUploads(context.Background(), &s3.ListMultipartUploadsInput{Bucket: aws.String(f.bucket)})
	if err != nil || len(listed.Uploads) == 0 {
		t.Fatalf("uploads=%v err=%v", listed.Uploads, err)
	}
	if _, err = f.s.client.AbortMultipartUpload(context.Background(), &s3.AbortMultipartUploadInput{Bucket: aws.String(f.bucket), Key: aws.String(f.key), UploadId: f.created.UploadId}); err != nil {
		t.Fatal(err)
	}
	listed, _ = f.s.client.ListMultipartUploads(context.Background(), &s3.ListMultipartUploadsInput{Bucket: aws.String(f.bucket)})
	if len(listed.Uploads) != 0 {
		t.Fatalf("uploads=%v", listed.Uploads)
	}
}

// 존재하지 않은 멀티파트 업로드 중단 실패 확인
func TestAbortMultipartUploadNotFound(t *testing.T) {
	t.Parallel()
	f := newMultipartFixture(t, "TestAbortMultipartUploadNotFound", 16)
	uploadMultipartPart(t, f, 1, bytes.Repeat([]byte("x"), 5*1024*1024))
	_, err := f.s.client.AbortMultipartUpload(context.Background(), &s3.AbortMultipartUploadInput{Bucket: aws.String(f.bucket), Key: aws.String(f.key), UploadId: aws.String("nonexistent")})
	assertS3Error(t, err, 404, "NoSuchUpload")
}

// 멀티파트 업로드 중인 목록 확인
func TestListMultipartUpload(t *testing.T) {
	t.Parallel()
	f := newMultipartFixture(t, "TestListMultipartUpload", 17)
	uploadMultipartPart(t, f, 1, bytes.Repeat([]byte("x"), 5*1024*1024))
	listed, err := f.s.client.ListMultipartUploads(context.Background(), &s3.ListMultipartUploadsInput{Bucket: aws.String(f.bucket)})
	if err != nil || len(listed.Uploads) == 0 {
		t.Fatalf("uploads=%v err=%v", listed.Uploads, err)
	}
}

// 업로드 하지 않은 파츠가 있는 상태에서 멀티파트 완료 함수 실패 확인
func TestMultipartUploadMissingPart(t *testing.T) {
	t.Parallel()
	f := newMultipartFixture(t, "TestMultipartUploadMissingPart", 18)
	one := uploadMultipartPart(t, f, 1, bytes.Repeat([]byte("a"), 1024))
	parts := []types.CompletedPart{
		{ETag: one.ETag, PartNumber: aws.Int32(1)},
		{ETag: aws.String(`"missing"`), PartNumber: aws.Int32(2)},
	}
	_, err := f.s.client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{Bucket: aws.String(f.bucket), Key: aws.String(f.key), UploadId: f.created.UploadId, MultipartUpload: &types.CompletedMultipartUpload{Parts: parts}})
	if err == nil {
		t.Fatal("invalid completion accepted")
	}
}

// 잘못된 eTag값을 입력한 멀티파트 완료 함수 실패 확인
func TestMultipartUploadIncorrectEtag(t *testing.T) {
	t.Parallel()
	f := newMultipartFixture(t, "TestMultipartUploadIncorrectEtag", 19)
	uploadMultipartPart(t, f, 1, bytes.Repeat([]byte("a"), 1024))
	parts := []types.CompletedPart{{ETag: aws.String(`"incorrect"`), PartNumber: aws.Int32(1)}}
	_, err := f.s.client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{Bucket: aws.String(f.bucket), Key: aws.String(f.key), UploadId: f.created.UploadId, MultipartUpload: &types.CompletedMultipartUpload{Parts: parts}})
	if err == nil {
		t.Fatal("invalid completion accepted")
	}
}

// 버킷에 존재하는 오브젝트와 동일한 이름으로 멀티파트 업로드를 시작 또는 중단했을때 오브젝트에 영향이 없음을 확인
func TestAtomicMultipartUploadWrite(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket := s.bucket(t, 20)
	key := "TestAtomicMultipartUploadWrite"
	put(t, s, bucket, key, "bar", nil)
	created, err := s.client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}
	assertObjectBytes(t, s.client, bucket, key, []byte("bar"))
	if _, err := s.client.AbortMultipartUpload(context.Background(), &s3.AbortMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(key), UploadId: created.UploadId}); err != nil {
		t.Fatal(err)
	}
	assertObjectBytes(t, s.client, bucket, key, []byte("bar"))
}

// 멀티파트 업로드 목록 확인
func TestMultipartUploadList(t *testing.T) {
	t.Parallel()
	f := newMultipartFixture(t, "TestMultipartUploadList", 21)
	uploadMultipartPart(t, f, 1, bytes.Repeat([]byte("x"), 5*1024*1024))
	listed, err := f.s.client.ListMultipartUploads(context.Background(), &s3.ListMultipartUploadsInput{Bucket: aws.String(f.bucket)})
	if err != nil || len(listed.Uploads) == 0 {
		t.Fatalf("uploads=%v err=%v", listed.Uploads, err)
	}
}

// 멀티파트 업로드하는 도중 중단 성공 확인
func TestAbortMultipartUploadList(t *testing.T) {
	t.Parallel()
	f := newMultipartFixture(t, "TestAbortMultipartUploadList", 22)
	uploadMultipartPart(t, f, 1, bytes.Repeat([]byte("x"), 5*1024*1024))
	listed, err := f.s.client.ListMultipartUploads(context.Background(), &s3.ListMultipartUploadsInput{Bucket: aws.String(f.bucket)})
	if err != nil || len(listed.Uploads) == 0 {
		t.Fatalf("uploads=%v err=%v", listed.Uploads, err)
	}
	if _, err = f.s.client.AbortMultipartUpload(context.Background(), &s3.AbortMultipartUploadInput{Bucket: aws.String(f.bucket), Key: aws.String(f.key), UploadId: f.created.UploadId}); err != nil {
		t.Fatal(err)
	}
	listed, _ = f.s.client.ListMultipartUploads(context.Background(), &s3.ListMultipartUploadsInput{Bucket: aws.String(f.bucket)})
	if len(listed.Uploads) != 0 {
		t.Fatalf("uploads=%v", listed.Uploads)
	}
}

// 멀티파트업로드와 멀티파티 카피로 오브젝트가 업로드 가능한지 확인
func TestMultipartCopyMany(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t, 23)
	source, target := "TestMultipartCopyMany?Source", "TestMultipartCopyMany&Target"
	body := randomTextToLong(6 * 1024 * 1024)
	put(t, s, b, source, string(body), nil)
	created, err := s.client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: aws.String(b), Key: aws.String(target)})
	if err != nil {
		t.Fatal(err)
	}
	input := &s3.UploadPartCopyInput{Bucket: aws.String(b), Key: aws.String(target), UploadId: created.UploadId, PartNumber: aws.Int32(1), CopySource: copySource(b, source, ""), CopySourceRange: aws.String(fmt.Sprintf("bytes=0-%d", len(body)-1))}
	copied, err := s.client.UploadPartCopy(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{Bucket: aws.String(b), Key: aws.String(target), UploadId: created.UploadId, MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{{ETag: copied.CopyPartResult.ETag, PartNumber: aws.Int32(1)}}}}); err != nil {
		t.Fatal(err)
	}
	assertObjectBytes(t, s.client, b, target, body)
}

// 멀티파트 목록 확인
func TestMultipartListParts(t *testing.T) {
	t.Parallel()
	f := newMultipartFixture(t, "parts", 24)
	for i := int32(1); i <= 20; i++ {
		uploadMultipartPart(t, f, i, bytes.Repeat([]byte{byte(i)}, 1024))
	}
	var marker *string
	seen := 0
	for {
		out, err := f.s.client.ListParts(context.Background(), &s3.ListPartsInput{Bucket: aws.String(f.bucket), Key: aws.String(f.key), UploadId: f.created.UploadId, MaxParts: aws.Int32(10), PartNumberMarker: marker})
		if err != nil {
			t.Fatal(err)
		}
		seen += len(out.Parts)
		if !aws.ToBool(out.IsTruncated) {
			break
		}
		marker = out.NextPartNumberMarker
	}
	if seen != 20 {
		t.Fatalf("parts=%d", seen)
	}
}

// UseChunkEncoding을 사용하는 멀티파트 업로드 시 체크섬 계산 및 검증 확인
func TestMultipartUploadChecksumUseChunkEncoding(t *testing.T) {
	t.Parallel()
	testMultipartUploadChecksum(t, "TestMultipartUploadChecksumUseChunkEncoding", 25)
}

// UseChunkEncoding을 사용하지 않는 멀티파트 업로드 시 체크섬 계산 및 검증 확인
func TestMultipartUploadChecksum(t *testing.T) {
	t.Parallel()
	testMultipartUploadChecksum(t, "TestMultipartUploadChecksum", 26)
}

func testMultipartUploadChecksum(t *testing.T, name string, id ...int) {
	t.Helper()
	s := newSuite(t)
	b := s.bucket(t, id...)
	algorithm := types.ChecksumAlgorithmCrc32
	created, err := s.client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: aws.String(b), Key: aws.String(name), ChecksumAlgorithm: algorithm, ChecksumType: types.ChecksumTypeComposite})
	if err != nil {
		t.Fatal(err)
	}
	body := bytes.Repeat([]byte("c"), 5*1024*1024)
	value := checksumValue(algorithm, body)
	part, err := s.client.UploadPart(context.Background(), &s3.UploadPartInput{Bucket: aws.String(b), Key: aws.String(name), UploadId: created.UploadId, PartNumber: aws.Int32(1), Body: bytes.NewReader(body), ChecksumAlgorithm: algorithm, ChecksumCRC32: aws.String(value)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{Bucket: aws.String(b), Key: aws.String(name), UploadId: created.UploadId, ChecksumType: types.ChecksumTypeComposite, MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{{ETag: part.ETag, PartNumber: aws.Int32(1), ChecksumCRC32: part.ChecksumCRC32}}}}); err != nil {
		t.Fatal(err)
	}
	assertObjectBytes(t, s.client, b, name, body)
}

// 멀티파트 업로드 시 체크섬 계산 및 검증 실패 확인
func TestMultipartUploadChecksumFailure(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t, 27)
	name := "TestMultipartUploadChecksumFailure"
	_, err := s.client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{
		Bucket:            aws.String(b),
		Key:               aws.String(name),
		ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
		ChecksumType:      types.ChecksumTypeFullObject,
	})
	assertS3Error(t, err, 400, "InvalidRequest")
}

// 멀티파트 업로드 시 체크섬 계산 및 검증 확인
func TestMultipartCopyChecksum(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t, 28)
	body := bytes.Repeat([]byte("q"), 5*1024*1024)
	putInput := &s3.PutObjectInput{Bucket: aws.String(b), Key: aws.String("source"), Body: bytes.NewReader(body), ChecksumAlgorithm: types.ChecksumAlgorithmCrc32}
	setPutChecksum(putInput, types.ChecksumAlgorithmCrc32, body, false)
	if _, err := s.client.PutObject(context.Background(), putInput); err != nil {
		t.Fatal(err)
	}
	created, err := s.client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: aws.String(b), Key: aws.String("target"), ChecksumAlgorithm: types.ChecksumAlgorithmCrc32, ChecksumType: types.ChecksumTypeComposite})
	if err != nil {
		t.Fatal(err)
	}
	copied, err := s.client.UploadPartCopy(context.Background(), &s3.UploadPartCopyInput{Bucket: aws.String(b), Key: aws.String("target"), UploadId: created.UploadId, PartNumber: aws.Int32(1), CopySource: copySource(b, "source", ""), CopySourceRange: aws.String(fmt.Sprintf("bytes=0-%d", len(body)-1))})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{Bucket: aws.String(b), Key: aws.String("target"), UploadId: created.UploadId, ChecksumType: types.ChecksumTypeComposite, MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{{ETag: copied.CopyPartResult.ETag, PartNumber: aws.Int32(1), ChecksumCRC32: copied.CopyPartResult.ChecksumCRC32}}}}); err != nil {
		t.Fatal(err)
	}
	assertObjectBytes(t, s.client, b, "target", body)
}

// 멀티파트 업로드 시 체크섬 알고리즘이 누락될 경우 에러 확인
func TestCreateMultipartUploadEmptyChecksumAlgorithm(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t, 29)
	_, err := s.client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: aws.String(b), Key: aws.String("TestCreateMultipartUploadEmptyChecksumAlgorithm"), ChecksumType: types.ChecksumTypeFullObject})
	assertHTTPError(t, err, 400)
}

// 멀티파트 업로드 시 체크섬 타입이 누락될 경우 에러 확인
func TestCreateMultipartUploadEmptyChecksumType(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t, 30)
	name := "TestCreateMultipartUploadEmptyChecksumType"
	algorithm := types.ChecksumAlgorithmCrc32
	created, err := s.client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: aws.String(b), Key: aws.String(name), ChecksumAlgorithm: algorithm})
	if err != nil {
		t.Fatal(err)
	}
	body := bytes.Repeat([]byte("c"), 5*1024*1024)
	value := checksumValue(algorithm, body)
	part, err := s.client.UploadPart(context.Background(), &s3.UploadPartInput{Bucket: aws.String(b), Key: aws.String(name), UploadId: created.UploadId, PartNumber: aws.Int32(1), Body: bytes.NewReader(body), ChecksumAlgorithm: algorithm, ChecksumCRC32: aws.String(value)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{Bucket: aws.String(b), Key: aws.String(name), UploadId: created.UploadId, ChecksumType: types.ChecksumTypeComposite, MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{{ETag: part.ETag, PartNumber: aws.Int32(1), ChecksumCRC32: part.ChecksumCRC32}}}}); err != nil {
		t.Fatal(err)
	}
	assertObjectBytes(t, s.client, b, name, body)
}

// 소스 오브젝트와 일치하는 copy-source-if-match 조건으로 UploadPartCopy 성공 확인
func TestUploadPartCopyIfMatchGood(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t, 31)
	source, target := "TestUploadPartCopyIfMatchGoodSource", "TestUploadPartCopyIfMatchGoodTarget"
	createdSource := put(t, s, b, source, source, nil)
	created, err := s.client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: aws.String(b), Key: aws.String(target)})
	if err != nil {
		t.Fatal(err)
	}
	input := &s3.UploadPartCopyInput{Bucket: aws.String(b), Key: aws.String(target), UploadId: created.UploadId, PartNumber: aws.Int32(1), CopySource: copySource(b, source, ""), CopySourceIfMatch: createdSource.ETag}
	if _, err = s.client.UploadPartCopy(context.Background(), input); err != nil {
		t.Fatal(err)
	}
}

// 소스 오브젝트와 일치하지 않는 copy-source-if-match 조건으로 UploadPartCopy 시 412 실패 확인
func TestUploadPartCopyIfMatchFailed(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t, 32)
	source, target := "TestUploadPartCopyIfMatchFailedSource", "TestUploadPartCopyIfMatchFailedTarget"
	put(t, s, b, source, source, nil)
	created, err := s.client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: aws.String(b), Key: aws.String(target)})
	if err != nil {
		t.Fatal(err)
	}
	input := &s3.UploadPartCopyInput{Bucket: aws.String(b), Key: aws.String(target), UploadId: created.UploadId, PartNumber: aws.Int32(1), CopySource: copySource(b, source, ""), CopySourceIfMatch: aws.String("ABC")}
	_, err = s.client.UploadPartCopy(context.Background(), input)
	assertHTTPError(t, err, 412)
}

// 소스 오브젝트와 일치하지 않는 copy-source-if-none-match 조건으로 UploadPartCopy 성공 확인
func TestUploadPartCopyIfNoneMatchGood(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t, 33)
	source, target := "TestUploadPartCopyIfNoneMatchGoodSource", "TestUploadPartCopyIfNoneMatchGoodTarget"
	put(t, s, b, source, source, nil)
	created, err := s.client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: aws.String(b), Key: aws.String(target)})
	if err != nil {
		t.Fatal(err)
	}
	input := &s3.UploadPartCopyInput{Bucket: aws.String(b), Key: aws.String(target), UploadId: created.UploadId, PartNumber: aws.Int32(1), CopySource: copySource(b, source, ""), CopySourceIfNoneMatch: aws.String("ABC")}
	if _, err = s.client.UploadPartCopy(context.Background(), input); err != nil {
		t.Fatal(err)
	}
}

// 소스 오브젝트와 일치하는 copy-source-if-none-match 조건으로 UploadPartCopy 시 412 실패 확인
func TestUploadPartCopyIfNoneMatchFailed(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t, 34)
	source, target := "TestUploadPartCopyIfNoneMatchFailedSource", "TestUploadPartCopyIfNoneMatchFailedTarget"
	createdSource := put(t, s, b, source, source, nil)
	created, err := s.client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: aws.String(b), Key: aws.String(target)})
	if err != nil {
		t.Fatal(err)
	}
	input := &s3.UploadPartCopyInput{Bucket: aws.String(b), Key: aws.String(target), UploadId: created.UploadId, PartNumber: aws.Int32(1), CopySource: copySource(b, source, ""), CopySourceIfNoneMatch: createdSource.ETag}
	_, err = s.client.UploadPartCopy(context.Background(), input)
	assertHTTPError(t, err, 412)
}

// If-Match와 If-None-Match를 함께 지정하면 UploadPartCopy가 501로 거부되는지 확인
func TestUploadPartCopyIfMatchAndIfNoneMatch(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t, 35)
	source, target := "TestUploadPartCopyIfMatchAndIfNoneMatchSource", "TestUploadPartCopyIfMatchAndIfNoneMatchTarget"
	createdSource := put(t, s, b, source, source, nil)
	created, err := s.client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: aws.String(b), Key: aws.String(target)})
	if err != nil {
		t.Fatal(err)
	}
	input := &s3.UploadPartCopyInput{Bucket: aws.String(b), Key: aws.String(target), UploadId: created.UploadId, PartNumber: aws.Int32(1), CopySource: copySource(b, source, ""), CopySourceIfMatch: createdSource.ETag, CopySourceIfNoneMatch: createdSource.ETag}
	_, err = s.client.UploadPartCopy(context.Background(), input)
	assertHTTPError(t, err, 412)
}

// If-Match와 If-None-Match: * 를 함께 지정하면 UploadPartCopy가 501로 거부되는지 확인
func TestUploadPartCopyIfMatchAndIfNoneMatchAny(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t, 36)
	source, target := "TestUploadPartCopyIfMatchAndIfNoneMatchAnySource", "TestUploadPartCopyIfMatchAndIfNoneMatchAnyTarget"
	createdSource := put(t, s, b, source, source, nil)
	created, err := s.client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: aws.String(b), Key: aws.String(target)})
	if err != nil {
		t.Fatal(err)
	}
	input := &s3.UploadPartCopyInput{Bucket: aws.String(b), Key: aws.String(target), UploadId: created.UploadId, PartNumber: aws.Int32(1), CopySource: copySource(b, source, ""), CopySourceIfMatch: createdSource.ETag, CopySourceIfNoneMatch: aws.String("*")}
	_, err = s.client.UploadPartCopy(context.Background(), input)
	assertHTTPError(t, err, 412)
}

// 소스 오브젝트 업로드 이전 시간의 copy-source-if-modified-since 조건으로 UploadPartCopy 성공 확인
func TestUploadPartCopyIfModifiedSinceGood(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t, 37)
	source, target := "TestUploadPartCopyIfModifiedSinceGoodSource", "TestUploadPartCopyIfModifiedSinceGoodTarget"
	put(t, s, b, source, source, nil)
	created, err := s.client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: aws.String(b), Key: aws.String(target)})
	if err != nil {
		t.Fatal(err)
	}
	past := time.Date(1994, 1, 1, 0, 0, 0, 0, time.UTC)
	input := &s3.UploadPartCopyInput{Bucket: aws.String(b), Key: aws.String(target), UploadId: created.UploadId, PartNumber: aws.Int32(1), CopySource: copySource(b, source, ""), CopySourceIfModifiedSince: &past}
	if _, err = s.client.UploadPartCopy(context.Background(), input); err != nil {
		t.Fatal(err)
	}
}

// 소스 오브젝트 업로드 이후 시간의 copy-source-if-modified-since 조건으로 UploadPartCopy 시 412 실패 확인
func TestUploadPartCopyIfModifiedSinceFailed(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t, 38)
	source, target := "TestUploadPartCopyIfModifiedSinceFailedSource", "TestUploadPartCopyIfModifiedSinceFailedTarget"
	put(t, s, b, source, source, nil)
	created, err := s.client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: aws.String(b), Key: aws.String(target)})
	if err != nil {
		t.Fatal(err)
	}
	head := headObject(t, s.client, b, source)
	after := head.LastModified.Add(time.Second)
	input := &s3.UploadPartCopyInput{Bucket: aws.String(b), Key: aws.String(target), UploadId: created.UploadId, PartNumber: aws.Int32(1), CopySource: copySource(b, source, ""), CopySourceIfModifiedSince: &after}
	time.Sleep(time.Second)
	_, err = s.client.UploadPartCopy(context.Background(), input)
	assertHTTPError(t, err, 412)
}

// 소스 오브젝트 업로드 이후 시간의 copy-source-if-unmodified-since 조건으로 UploadPartCopy 성공 확인
func TestUploadPartCopyIfUnmodifiedSinceGood(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t, 39)
	source, target := "TestUploadPartCopyIfUnmodifiedSinceGoodSource", "TestUploadPartCopyIfUnmodifiedSinceGoodTarget"
	put(t, s, b, source, source, nil)
	created, err := s.client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: aws.String(b), Key: aws.String(target)})
	if err != nil {
		t.Fatal(err)
	}
	future := time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)
	input := &s3.UploadPartCopyInput{Bucket: aws.String(b), Key: aws.String(target), UploadId: created.UploadId, PartNumber: aws.Int32(1), CopySource: copySource(b, source, ""), CopySourceIfUnmodifiedSince: &future}
	if _, err = s.client.UploadPartCopy(context.Background(), input); err != nil {
		t.Fatal(err)
	}
}

// 소스 오브젝트 업로드 이전 시간의 copy-source-if-unmodified-since 조건으로 UploadPartCopy 시 412 실패 확인
func TestUploadPartCopyIfUnmodifiedSinceFailed(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t, 40)
	source, target := "TestUploadPartCopyIfUnmodifiedSinceFailedSource", "TestUploadPartCopyIfUnmodifiedSinceFailedTarget"
	put(t, s, b, source, source, nil)
	created, err := s.client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: aws.String(b), Key: aws.String(target)})
	if err != nil {
		t.Fatal(err)
	}
	past := time.Date(1994, 1, 1, 0, 0, 0, 0, time.UTC)
	input := &s3.UploadPartCopyInput{Bucket: aws.String(b), Key: aws.String(target), UploadId: created.UploadId, PartNumber: aws.Int32(1), CopySource: copySource(b, source, ""), CopySourceIfUnmodifiedSince: &past}
	_, err = s.client.UploadPartCopy(context.Background(), input)
	assertHTTPError(t, err, 412)
}

// 대상 오브젝트와 일치하는 If-Match 조건으로 CompleteMultipartUpload 덮어쓰기 성공 확인
func TestCompleteMultipartUploadIfMatchGood(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket := s.bucket(t, 41)
	key := "TestCompleteMultipartUploadIfMatchGood"
	existing := put(t, s, bucket, key, "old", nil)
	created, err := s.client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = s.client.AbortMultipartUpload(context.Background(), &s3.AbortMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(key), UploadId: created.UploadId})
	})
	part, err := s.client.UploadPart(context.Background(), &s3.UploadPartInput{Bucket: aws.String(bucket), Key: aws.String(key), UploadId: created.UploadId, PartNumber: aws.Int32(1), Body: bytes.NewReader(bytes.Repeat([]byte("n"), 5*1024*1024))})
	if err != nil {
		t.Fatal(err)
	}
	input := &s3.CompleteMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(key), UploadId: created.UploadId, MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{{ETag: part.ETag, PartNumber: aws.Int32(1)}}}, IfMatch: existing.ETag}
	if _, err = s.client.CompleteMultipartUpload(context.Background(), input); err != nil {
		t.Fatal(err)
	}
}

// 대상 오브젝트와 일치하지 않는 If-Match 조건으로 CompleteMultipartUpload 시 412 실패 확인
func TestCompleteMultipartUploadIfMatchFailed(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket := s.bucket(t, 42)
	key := "TestCompleteMultipartUploadIfMatchFailed"
	put(t, s, bucket, key, "old", nil)
	created, err := s.client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = s.client.AbortMultipartUpload(context.Background(), &s3.AbortMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(key), UploadId: created.UploadId})
	})
	part, err := s.client.UploadPart(context.Background(), &s3.UploadPartInput{Bucket: aws.String(bucket), Key: aws.String(key), UploadId: created.UploadId, PartNumber: aws.Int32(1), Body: bytes.NewReader(bytes.Repeat([]byte("n"), 5*1024*1024))})
	if err != nil {
		t.Fatal(err)
	}
	input := &s3.CompleteMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(key), UploadId: created.UploadId, MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{{ETag: part.ETag, PartNumber: aws.Int32(1)}}}, IfMatch: aws.String("bad")}
	_, err = s.client.CompleteMultipartUpload(context.Background(), input)
	assertHTTPError(t, err, 412)
}

// 존재하지 않는 키에 If-None-Match: * 조건으로 CompleteMultipartUpload 성공 확인
func TestCompleteMultipartUploadIfNoneMatchGood(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket := s.bucket(t, 43)
	key := "TestCompleteMultipartUploadIfNoneMatchGood"
	created, err := s.client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = s.client.AbortMultipartUpload(context.Background(), &s3.AbortMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(key), UploadId: created.UploadId})
	})
	part, err := s.client.UploadPart(context.Background(), &s3.UploadPartInput{Bucket: aws.String(bucket), Key: aws.String(key), UploadId: created.UploadId, PartNumber: aws.Int32(1), Body: bytes.NewReader(bytes.Repeat([]byte("n"), 5*1024*1024))})
	if err != nil {
		t.Fatal(err)
	}
	input := &s3.CompleteMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(key), UploadId: created.UploadId, MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{{ETag: part.ETag, PartNumber: aws.Int32(1)}}}, IfNoneMatch: aws.String("*")}
	if _, err = s.client.CompleteMultipartUpload(context.Background(), input); err != nil {
		t.Fatal(err)
	}
}

// 이미 존재하는 키에 If-None-Match: * 조건으로 CompleteMultipartUpload 시 412 실패 확인
func TestCompleteMultipartUploadIfNoneMatchFailed(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket := s.bucket(t, 44)
	key := "TestCompleteMultipartUploadIfNoneMatchFailed"
	put(t, s, bucket, key, "old", nil)
	created, err := s.client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = s.client.AbortMultipartUpload(context.Background(), &s3.AbortMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(key), UploadId: created.UploadId})
	})
	part, err := s.client.UploadPart(context.Background(), &s3.UploadPartInput{Bucket: aws.String(bucket), Key: aws.String(key), UploadId: created.UploadId, PartNumber: aws.Int32(1), Body: bytes.NewReader(bytes.Repeat([]byte("n"), 5*1024*1024))})
	if err != nil {
		t.Fatal(err)
	}
	input := &s3.CompleteMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(key), UploadId: created.UploadId, MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{{ETag: part.ETag, PartNumber: aws.Int32(1)}}}, IfNoneMatch: aws.String("*")}
	_, err = s.client.CompleteMultipartUpload(context.Background(), input)
	assertHTTPError(t, err, 412)
}

// If-Match와 If-None-Match를 함께 지정하면 501로 거부되는지 확인
func TestCompleteMultipartUploadIfMatchAndIfNoneMatch(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket := s.bucket(t, 45)
	key := "TestCompleteMultipartUploadIfMatchAndIfNoneMatch"
	existing := put(t, s, bucket, key, "old", nil)
	created, err := s.client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = s.client.AbortMultipartUpload(context.Background(), &s3.AbortMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(key), UploadId: created.UploadId})
	})
	part, err := s.client.UploadPart(context.Background(), &s3.UploadPartInput{Bucket: aws.String(bucket), Key: aws.String(key), UploadId: created.UploadId, PartNumber: aws.Int32(1), Body: bytes.NewReader(bytes.Repeat([]byte("n"), 5*1024*1024))})
	if err != nil {
		t.Fatal(err)
	}
	input := &s3.CompleteMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(key), UploadId: created.UploadId, MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{{ETag: part.ETag, PartNumber: aws.Int32(1)}}}, IfMatch: existing.ETag, IfNoneMatch: existing.ETag}
	_, err = s.client.CompleteMultipartUpload(context.Background(), input)
	assertHTTPError(t, err, 501)
}

// If-Match와 If-None-Match: * 를 함께 지정하면 501로 거부되는지 확인
func TestCompleteMultipartUploadIfMatchAndIfNoneMatchAny(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket := s.bucket(t, 46)
	key := "TestCompleteMultipartUploadIfMatchAndIfNoneMatchAny"
	existing := put(t, s, bucket, key, "old", nil)
	created, err := s.client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = s.client.AbortMultipartUpload(context.Background(), &s3.AbortMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(key), UploadId: created.UploadId})
	})
	part, err := s.client.UploadPart(context.Background(), &s3.UploadPartInput{Bucket: aws.String(bucket), Key: aws.String(key), UploadId: created.UploadId, PartNumber: aws.Int32(1), Body: bytes.NewReader(bytes.Repeat([]byte("n"), 5*1024*1024))})
	if err != nil {
		t.Fatal(err)
	}
	input := &s3.CompleteMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(key), UploadId: created.UploadId, MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{{ETag: part.ETag, PartNumber: aws.Int32(1)}}}, IfMatch: existing.ETag, IfNoneMatch: aws.String("*")}
	_, err = s.client.CompleteMultipartUpload(context.Background(), input)
	assertHTTPError(t, err, 501)
}

// 멀티파티 업로드 abort 이후 uploadPart가 실패하는지 확인
func TestMultipartUploadAbortDuringUpload(t *testing.T) {
	t.Parallel()
	f := newMultipartFixture(t, "TestMultipartUploadAbortDuringUpload", 47)
	if _, err := f.s.client.AbortMultipartUpload(context.Background(), &s3.AbortMultipartUploadInput{Bucket: aws.String(f.bucket), Key: aws.String(f.key), UploadId: f.created.UploadId}); err != nil {
		t.Fatal(err)
	}
	_, err := f.s.client.UploadPart(context.Background(), &s3.UploadPartInput{
		Bucket:     aws.String(f.bucket),
		Key:        aws.String(f.key),
		UploadId:   f.created.UploadId,
		PartNumber: aws.Int32(1),
		Body:       bytes.NewReader(bytes.Repeat([]byte("x"), 5*1024*1024)),
	})
	assertS3Error(t, err, 404, "NoSuchUpload")
}

type multipartFixture struct {
	s       *suite
	bucket  string
	key     string
	created *s3.CreateMultipartUploadOutput
}

func newMultipartFixture(t *testing.T, key string, id ...int) *multipartFixture {
	t.Helper()
	s := newSuite(t)
	b := s.bucket(t, id...)
	created, err := s.client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: aws.String(b), Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = s.client.AbortMultipartUpload(context.Background(), &s3.AbortMultipartUploadInput{Bucket: aws.String(b), Key: aws.String(key), UploadId: created.UploadId})
	})
	return &multipartFixture{s, b, key, created}
}

func uploadMultipartPart(t *testing.T, f *multipartFixture, number int32, body []byte) *s3.UploadPartOutput {
	t.Helper()
	out, err := f.s.client.UploadPart(context.Background(), &s3.UploadPartInput{Bucket: aws.String(f.bucket), Key: aws.String(f.key), UploadId: f.created.UploadId, PartNumber: aws.Int32(number), Body: bytes.NewReader(body)})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func completeMultipartParts(t *testing.T, f *multipartFixture, parts []types.CompletedPart, options ...func(*s3.Options)) *s3.CompleteMultipartUploadOutput {
	t.Helper()
	out, err := f.s.client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{Bucket: aws.String(f.bucket), Key: aws.String(f.key), UploadId: f.created.UploadId, MultipartUpload: &types.CompletedMultipartUpload{Parts: parts}}, options...)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// createUpload는 멀티파트 업로드를 시작하고 업로드 ID 를 돌려준다.
func createUpload(t *testing.T, client *s3.Client, bucket, key string) *string {
	t.Helper()
	created, err := client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}
	return created.UploadId
}

// uploadPartETag는 파트를 올리고 ETag 를 돌려준다.
func uploadPartETag(t *testing.T, client *s3.Client, bucket, key string, uploadID *string, number int32, body []byte) *string {
	t.Helper()
	out, err := uploadPartCall(client, bucket, key, uploadID, number, body)
	if err != nil {
		t.Fatal(err)
	}
	return out.ETag
}

func uploadPartCall(client *s3.Client, bucket, key string, uploadID *string, number int32, body []byte) (*s3.UploadPartOutput, error) {
	return client.UploadPart(context.Background(), &s3.UploadPartInput{Bucket: aws.String(bucket), Key: aws.String(key), UploadId: uploadID, PartNumber: aws.Int32(number), Body: bytes.NewReader(body)})
}

// uploadSmallParts는 10바이트짜리 파트를 count 개 올리고 업로드 ID 와 ETag 목록을 돌려준다.
func uploadSmallParts(t *testing.T, client *s3.Client, bucket, key string, count int) (*string, []*string) {
	t.Helper()
	uploadID := createUpload(t, client, bucket, key)
	etags := make([]*string, count)
	for index := range etags {
		etags[index] = uploadPartETag(t, client, bucket, key, uploadID, int32(index+1), []byte("0123456789"))
	}
	return uploadID, etags
}

func completedPart(number int32, etag *string) types.CompletedPart {
	return types.CompletedPart{PartNumber: aws.Int32(number), ETag: etag}
}

func completeUploadCall(client *s3.Client, bucket, key string, uploadID *string, parts ...types.CompletedPart) (*s3.CompleteMultipartUploadOutput, error) {
	return client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(key), UploadId: uploadID, MultipartUpload: &types.CompletedMultipartUpload{Parts: parts}})
}

func listPartNumbers(out *s3.ListPartsOutput) []int32 {
	if out == nil {
		return nil
	}
	numbers := make([]int32, 0, len(out.Parts))
	for _, part := range out.Parts {
		numbers = append(numbers, aws.ToInt32(part.PartNumber))
	}
	return numbers
}

// createTreeUploads는 a/1 에 2개, a/2, b/1, c 에 1개씩 업로드를 만든다.
func createTreeUploads(t *testing.T, client *s3.Client, bucket string) {
	t.Helper()
	for _, key := range []string{"a/1", "a/1", "a/2", "b/1", "c"} {
		createUpload(t, client, bucket, key)
	}
}

func uploadKeys(out *s3.ListMultipartUploadsOutput) []string {
	keys := make([]string, 0, len(out.Uploads))
	for _, upload := range out.Uploads {
		keys = append(keys, aws.ToString(upload.Key))
	}
	return keys
}

func listUploads(t *testing.T, client *s3.Client, input *s3.ListMultipartUploadsInput) *s3.ListMultipartUploadsOutput {
	t.Helper()
	out, err := client.ListMultipartUploads(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// 파트 번호는 1~10000
func TestUploadPartInvalidPartNumber(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b, key := s.bucket(t, 49), "TestUploadPartInvalidPartNumber"
	uploadID := createUpload(t, s.client, b, key)
	for _, number := range []int32{0, 10001} {
		_, err := uploadPartCall(s.client, b, key, uploadID, number, []byte("content"))
		assertS3Error(t, err, 400, "InvalidArgument")
	}
	uploadPartETag(t, s.client, b, key, uploadID, 10000, []byte("content"))
	listed, err := s.client.ListParts(context.Background(), &s3.ListPartsInput{Bucket: aws.String(b), Key: aws.String(key), UploadId: uploadID})
	if err != nil {
		t.Fatal(err)
	}
	if numbers := listPartNumbers(listed); !slices.Equal(numbers, []int32{10000}) {
		t.Fatalf("parts=%v", numbers)
	}
}

// 없는 업로드 ID 나 다른 키의 업로드 ID 로는 파트를 올리거나 볼 수 없다
func TestUploadPartNoSuchUpload(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b, key := s.bucket(t, 50), "TestUploadPartNoSuchUpload"
	uploadID := createUpload(t, s.client, b, key)
	for _, target := range []struct {
		key      string
		uploadID *string
	}{{key, aws.String("invalid")}, {key + "_other", uploadID}} {
		_, err := uploadPartCall(s.client, b, target.key, target.uploadID, 1, []byte("content"))
		assertS3Error(t, err, 404, "NoSuchUpload")
		_, err = s.client.ListParts(context.Background(), &s3.ListPartsInput{Bucket: aws.String(b), Key: aws.String(target.key), UploadId: target.uploadID})
		assertS3Error(t, err, 404, "NoSuchUpload")
	}
}

// 본문과 다른 MD5 를 준 파트는 저장되지 않는다
func TestUploadPartContentMd5Mismatch(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	b, key := s.bucket(t, 51), "TestUploadPartContentMd5Mismatch"
	uploadID := createUpload(t, s.client, b, key)
	_, err := s.client.UploadPart(ctx, &s3.UploadPartInput{Bucket: aws.String(b), Key: aws.String(key), UploadId: uploadID, PartNumber: aws.Int32(1), Body: bytes.NewReader([]byte("content")), ContentMD5: aws.String(md5Base64("other"))})
	assertS3Error(t, err, 400, "BadDigest")
	listed, err := s.client.ListParts(ctx, &s3.ListPartsInput{Bucket: aws.String(b), Key: aws.String(key), UploadId: uploadID})
	if err != nil || len(listed.Parts) != 0 {
		t.Fatalf("parts=%v err=%v", listPartNumbers(listed), err)
	}
}

// 권한이 없는 사용자는 멀티파트 업로드를 다룰 수 없다
func TestMultipartAccessDenied(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	alt := altClientOrSkip(t, s)
	b, key := s.bucket(t, 52), "TestMultipartAccessDenied"
	uploadID, etags := uploadSmallParts(t, s.client, b, key, 1)

	_, err := alt.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: aws.String(b), Key: aws.String(key)})
	assertS3Error(t, err, 403, "AccessDenied")
	_, err = uploadPartCall(alt, b, key, uploadID, 2, []byte("content"))
	assertS3Error(t, err, 403, "AccessDenied")
	_, err = alt.ListParts(ctx, &s3.ListPartsInput{Bucket: aws.String(b), Key: aws.String(key), UploadId: uploadID})
	assertS3Error(t, err, 403, "AccessDenied")
	_, err = alt.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{Bucket: aws.String(b)})
	assertS3Error(t, err, 403, "AccessDenied")
	_, err = completeUploadCall(alt, b, key, uploadID, completedPart(1, etags[0]))
	assertS3Error(t, err, 403, "AccessDenied")
	_, err = alt.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: aws.String(b), Key: aws.String(key), UploadId: uploadID})
	assertS3Error(t, err, 403, "AccessDenied")

	// 업로드는 그대로 남아 있다
	listed, err := s.client.ListParts(ctx, &s3.ListPartsInput{Bucket: aws.String(b), Key: aws.String(key), UploadId: uploadID})
	if err != nil {
		t.Fatal(err)
	}
	if numbers := listPartNumbers(listed); !slices.Equal(numbers, []int32{1}) {
		t.Fatalf("parts=%v", numbers)
	}
}

// 없는 버킷에는 멀티파트 업로드를 시작하거나 목록을 볼 수 없다
func TestMultipartBucketNotExist(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	b := s.bucketNameOnly(53)
	_, err := s.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: aws.String(b), Key: aws.String("key")})
	assertS3Error(t, err, 404, "NoSuchBucket")
	_, err = s.client.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{Bucket: aws.String(b)})
	assertS3Error(t, err, 404, "NoSuchBucket")
}

// 알 수 없는 스토리지 클래스로는 멀티파트 업로드를 시작할 수 없다
func TestMultipartInvalidStorageClass(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t, 54)
	_, err := s.client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: aws.String(b), Key: aws.String("key"), StorageClass: types.StorageClass("INVALID")})
	assertS3Error(t, err, 400, "InvalidStorageClass")
	if out := listUploads(t, s.client, &s3.ListMultipartUploadsInput{Bucket: aws.String(b)}); len(out.Uploads) != 0 {
		t.Fatalf("uploads=%v", uploadKeys(out))
	}
}

// ListParts 의 max-parts 와 part-number-marker
func TestListPartsMaxParts(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b, key := s.bucket(t, 55), "TestListPartsMaxParts"
	uploadID, _ := uploadSmallParts(t, s.client, b, key, 3)
	listParts := func(maxParts *int32, marker *string) (*s3.ListPartsOutput, error) {
		return s.client.ListParts(context.Background(), &s3.ListPartsInput{Bucket: aws.String(b), Key: aws.String(key), UploadId: uploadID, MaxParts: maxParts, PartNumberMarker: marker})
	}
	check := func(name string, maxParts *int32, marker *string, want []int32, truncated bool) *s3.ListPartsOutput {
		t.Helper()
		out, err := listParts(maxParts, marker)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if numbers := listPartNumbers(out); !slices.Equal(numbers, want) || aws.ToBool(out.IsTruncated) != truncated {
			t.Fatalf("%s parts=%v truncated=%v want=%v/%v", name, numbers, aws.ToBool(out.IsTruncated), want, truncated)
		}
		return out
	}

	if out := check("default", nil, nil, []int32{1, 2, 3}, false); aws.ToInt32(out.MaxParts) != 1000 {
		t.Fatalf("MaxParts=%d", aws.ToInt32(out.MaxParts))
	}
	if out := check("MaxParts=2", aws.Int32(2), nil, []int32{1, 2}, true); aws.ToString(out.NextPartNumberMarker) != "2" {
		t.Fatalf("NextPartNumberMarker=%q", aws.ToString(out.NextPartNumberMarker))
	}
	check("PartNumberMarker=2", nil, aws.String("2"), []int32{3}, false)
	check("MaxParts=0", aws.Int32(0), nil, []int32{}, false)

	// 1000 보다 큰 값은 1000 으로 낮춘다
	out, err := listParts(aws.Int32(1001), nil)
	if err != nil || aws.ToInt32(out.MaxParts) != 1000 {
		t.Fatalf("MaxParts=1001 response=%v err=%v", out, err)
	}
	_, err = listParts(aws.Int32(-1), nil)
	assertS3Error(t, err, 400, "InvalidArgument")
}

// 완료할 때 파트 번호는 오름차순이어야 하고 중복될 수 없다
func TestCompleteMultipartUploadInvalidPartOrder(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b, key := s.bucket(t, 56), "TestCompleteMultipartUploadInvalidPartOrder"
	uploadID, etags := uploadSmallParts(t, s.client, b, key, 2)
	for _, parts := range [][]types.CompletedPart{
		{completedPart(2, etags[1]), completedPart(1, etags[0])},
		{completedPart(1, etags[0]), completedPart(1, etags[0])},
	} {
		_, err := completeUploadCall(s.client, b, key, uploadID, parts...)
		assertS3Error(t, err, 400, "InvalidPartOrder")
	}
	_, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String(key)})
	assertS3Status(t, err, 404)
}

// 파트를 하나도 주지 않고 완료하면 거부된다
func TestCompleteMultipartUploadNoParts(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b, key := s.bucket(t, 57), "TestCompleteMultipartUploadNoParts"
	uploadID, _ := uploadSmallParts(t, s.client, b, key, 1)
	_, err := completeUploadCall(s.client, b, key, uploadID)
	assertS3Error(t, err, 400, "MalformedXML")
}

// 올린 파트 중 일부만 골라 완료할 수 있고, ETag 는 따옴표가 없어도 된다
func TestCompleteMultipartUploadSkipPart(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b, key := s.bucket(t, 58), "TestCompleteMultipartUploadSkipPart"
	uploadID := createUpload(t, s.client, b, key)
	bodies := [][]byte{bytes.Repeat([]byte("a"), 5*1024*1024), bytes.Repeat([]byte("b"), 5*1024*1024), bytes.Repeat([]byte("c"), 10)}
	etags := make([]string, len(bodies))
	for index, body := range bodies {
		etags[index] = strings.Trim(aws.ToString(uploadPartETag(t, s.client, b, key, uploadID, int32(index+1), body)), `"`)
	}

	out, err := completeUploadCall(s.client, b, key, uploadID, completedPart(1, aws.String(etags[0])), completedPart(3, aws.String(etags[2])))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(aws.ToString(out.ETag), `-2"`) || aws.ToString(out.Bucket) != b || aws.ToString(out.Key) != key {
		t.Fatalf("ETag=%q Bucket=%q Key=%q", aws.ToString(out.ETag), aws.ToString(out.Bucket), aws.ToString(out.Key))
	}

	want := append(append([]byte{}, bodies[0]...), bodies[2]...)
	got, body := getObjectBody(t, s.client, &s3.GetObjectInput{Bucket: aws.String(b), Key: aws.String(key)})
	if aws.ToInt64(got.ContentLength) != int64(len(want)) || body != string(want) {
		t.Fatalf("ContentLength=%d body length=%d want=%d", aws.ToInt64(got.ContentLength), len(body), len(want))
	}
}

// 완료한 업로드를 같은 내용으로 다시 완료하면 같은 결과로 성공한다
func TestCompleteMultipartUploadTwice(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	b, key := s.bucket(t, 59), "TestCompleteMultipartUploadTwice"
	uploadID, etags := uploadSmallParts(t, s.client, b, key, 1)
	first, err := completeUploadCall(s.client, b, key, uploadID, completedPart(1, etags[0]))
	if err != nil || !strings.HasSuffix(aws.ToString(first.ETag), `-1"`) {
		t.Fatalf("first=%v err=%v", first, err)
	}
	second, err := completeUploadCall(s.client, b, key, uploadID, completedPart(1, etags[0]))
	if err != nil || aws.ToString(second.ETag) != aws.ToString(first.ETag) {
		t.Fatalf("second=%v want ETag=%q err=%v", second, aws.ToString(first.ETag), err)
	}

	// 완료한 업로드에는 파트를 더 올리거나 목록을 볼 수 없고, 중단 요청은 성공한다
	_, err = uploadPartCall(s.client, b, key, uploadID, 2, []byte("content"))
	assertS3Error(t, err, 404, "NoSuchUpload")
	_, err = s.client.ListParts(ctx, &s3.ListPartsInput{Bucket: aws.String(b), Key: aws.String(key), UploadId: uploadID})
	assertS3Error(t, err, 404, "NoSuchUpload")
	aborted, err := s.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: aws.String(b), Key: aws.String(key), UploadId: uploadID})
	if err != nil {
		t.Fatal(err)
	}
	if status := responseStatus(t, aborted.ResultMetadata); status != 204 {
		t.Fatalf("AbortMultipartUpload status=%d", status)
	}
	if got := read(t, s, b, key); got != "0123456789" {
		t.Fatalf("body=%q", got)
	}
}

// 이미 중단한 업로드를 다시 중단해도 성공한다
func TestAbortMultipartUploadTwice(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	b, key := s.bucket(t, 60), "TestAbortMultipartUploadTwice"
	uploadID, etags := uploadSmallParts(t, s.client, b, key, 1)
	abort := &s3.AbortMultipartUploadInput{Bucket: aws.String(b), Key: aws.String(key), UploadId: uploadID}
	if _, err := s.client.AbortMultipartUpload(ctx, abort); err != nil {
		t.Fatal(err)
	}
	aborted, err := s.client.AbortMultipartUpload(ctx, abort)
	if err != nil {
		t.Fatal(err)
	}
	if status := responseStatus(t, aborted.ResultMetadata); status != 204 {
		t.Fatalf("AbortMultipartUpload status=%d", status)
	}

	_, err = uploadPartCall(s.client, b, key, uploadID, 1, []byte("content"))
	assertS3Error(t, err, 404, "NoSuchUpload")
	_, err = completeUploadCall(s.client, b, key, uploadID, completedPart(1, etags[0]))
	assertS3Error(t, err, 404, "NoSuchUpload")
}

// ListMultipartUploads 의 prefix 와 delimiter
func TestListMultipartUploadsPrefixDelimiter(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t, 61)
	createTreeUploads(t, s.client, b)

	out := listUploads(t, s.client, &s3.ListMultipartUploadsInput{Bucket: aws.String(b)})
	assertStringList(t, uploadKeys(out), []string{"a/1", "a/1", "a/2", "b/1", "c"})
	if aws.ToBool(out.IsTruncated) || aws.ToInt32(out.MaxUploads) != 1000 {
		t.Fatalf("IsTruncated=%v MaxUploads=%d", aws.ToBool(out.IsTruncated), aws.ToInt32(out.MaxUploads))
	}

	out = listUploads(t, s.client, &s3.ListMultipartUploadsInput{Bucket: aws.String(b), Prefix: aws.String("a/")})
	assertStringList(t, uploadKeys(out), []string{"a/1", "a/1", "a/2"})

	out = listUploads(t, s.client, &s3.ListMultipartUploadsInput{Bucket: aws.String(b), Delimiter: aws.String("/")})
	assertStringList(t, uploadKeys(out), []string{"c"})
	prefixes := make([]string, 0, len(out.CommonPrefixes))
	for _, prefix := range out.CommonPrefixes {
		prefixes = append(prefixes, aws.ToString(prefix.Prefix))
	}
	assertStringList(t, prefixes, []string{"a/", "b/"})
}

// ListMultipartUploads 를 마커로 이어서 조회한다
func TestListMultipartUploadsPagination(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t, 62)
	createTreeUploads(t, s.client, b)

	out := listUploads(t, s.client, &s3.ListMultipartUploadsInput{Bucket: aws.String(b), MaxUploads: aws.Int32(2)})
	assertStringList(t, uploadKeys(out), []string{"a/1", "a/1"})
	if !aws.ToBool(out.IsTruncated) || aws.ToString(out.NextKeyMarker) != "a/1" || aws.ToString(out.NextUploadIdMarker) != aws.ToString(out.Uploads[1].UploadId) {
		t.Fatalf("IsTruncated=%v NextKeyMarker=%q NextUploadIdMarker=%q", aws.ToBool(out.IsTruncated), aws.ToString(out.NextKeyMarker), aws.ToString(out.NextUploadIdMarker))
	}

	out = listUploads(t, s.client, &s3.ListMultipartUploadsInput{Bucket: aws.String(b), MaxUploads: aws.Int32(2), KeyMarker: out.NextKeyMarker, UploadIdMarker: out.NextUploadIdMarker})
	assertStringList(t, uploadKeys(out), []string{"a/2", "b/1"})
	if !aws.ToBool(out.IsTruncated) {
		t.Fatal("second page is not truncated")
	}

	// 키 마커만 주면 그 키의 업로드는 모두 건너뛴다
	out = listUploads(t, s.client, &s3.ListMultipartUploadsInput{Bucket: aws.String(b), KeyMarker: aws.String("a/1")})
	assertStringList(t, uploadKeys(out), []string{"a/2", "b/1", "c"})
	if aws.ToBool(out.IsTruncated) {
		t.Fatal("key marker page is truncated")
	}
}

// ListMultipartUploads 의 max-uploads 범위
func TestListMultipartUploadsMaxUploads(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b := s.bucket(t, 63)
	createTreeUploads(t, s.client, b)

	out := listUploads(t, s.client, &s3.ListMultipartUploadsInput{Bucket: aws.String(b), MaxUploads: aws.Int32(0)})
	if len(out.Uploads) != 0 || aws.ToBool(out.IsTruncated) {
		t.Fatalf("uploads=%v IsTruncated=%v", uploadKeys(out), aws.ToBool(out.IsTruncated))
	}

	// 1000 보다 큰 값은 1000 으로 낮춘다
	out = listUploads(t, s.client, &s3.ListMultipartUploadsInput{Bucket: aws.String(b), MaxUploads: aws.Int32(1001)})
	if aws.ToInt32(out.MaxUploads) != 1000 {
		t.Fatalf("MaxUploads=%d", aws.ToInt32(out.MaxUploads))
	}
	_, err := s.client.ListMultipartUploads(context.Background(), &s3.ListMultipartUploadsInput{Bucket: aws.String(b), MaxUploads: aws.Int32(-1)})
	assertS3Error(t, err, 400, "InvalidArgument")
}

// UploadPartCopy 의 원본 키·원본 버킷·업로드가 없는 경우
func TestUploadPartCopySourceNotExist(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	b, key := s.bucket(t, 64), "TestUploadPartCopySourceNotExist"
	put(t, s, b, "source", "0123456789", nil)
	uploadID := createUpload(t, s.client, b, key)
	partCopy := func(sourceBucket, sourceKey string, targetUploadID *string) error {
		_, err := s.client.UploadPartCopy(ctx, &s3.UploadPartCopyInput{Bucket: aws.String(b), Key: aws.String(key), UploadId: targetUploadID, PartNumber: aws.Int32(1), CopySource: copySource(sourceBucket, sourceKey, "")})
		return err
	}

	assertS3Error(t, partCopy(b, "not_exist", uploadID), 404, "NoSuchKey")
	assertS3Error(t, partCopy(s.bucketNameOnly(64), "source", uploadID), 404, "NoSuchBucket")
	assertS3Error(t, partCopy(b, "source", aws.String("invalid")), 404, "NoSuchUpload")
	listed, err := s.client.ListParts(ctx, &s3.ListPartsInput{Bucket: aws.String(b), Key: aws.String(key), UploadId: uploadID})
	if err != nil || len(listed.Parts) != 0 {
		t.Fatalf("parts=%v err=%v", listPartNumbers(listed), err)
	}
}

// UploadPartCopy 의 범위는 bytes=시작-끝 형식만 허용한다(뒤에서부터의 길이 지정 불가)
func TestUploadPartCopyInvalidRangeFormat(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	b, key := s.bucket(t, 65), "TestUploadPartCopyInvalidRangeFormat"
	put(t, s, b, "source", "0123456789", nil)
	uploadID := createUpload(t, s.client, b, key)
	for _, value := range []string{"bytes=abc", "bytes=-5"} {
		_, err := s.client.UploadPartCopy(context.Background(), &s3.UploadPartCopyInput{Bucket: aws.String(b), Key: aws.String(key), UploadId: uploadID, PartNumber: aws.Int32(1), CopySource: copySource(b, "source", ""), CopySourceRange: aws.String(value)})
		assertS3Error(t, err, 400, "InvalidArgument")
	}
}

// partNumber 를 주면 그 파트만 206 으로 돌려주고 전체 파트 수를 알려준다
func TestMultipartGetPartNumber(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	ctx := context.Background()
	b, key := s.bucket(t, 66), "TestMultipartGetPartNumber"
	uploadID := createUpload(t, s.client, b, key)
	bodies := [][]byte{bytes.Repeat([]byte("a"), 5*1024*1024), bytes.Repeat([]byte("b"), 10)}
	parts := make([]types.CompletedPart, len(bodies))
	for index, body := range bodies {
		parts[index] = completedPart(int32(index+1), uploadPartETag(t, s.client, b, key, uploadID, int32(index+1), body))
	}
	if _, err := completeUploadCall(s.client, b, key, uploadID, parts...); err != nil {
		t.Fatal(err)
	}
	first, total := len(bodies[0]), len(bodies[0])+len(bodies[1])

	head, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String(key), PartNumber: aws.Int32(1)})
	if err != nil {
		t.Fatal(err)
	}
	if status, wantRange := responseStatus(t, head.ResultMetadata), fmt.Sprintf("bytes 0-%d/%d", first-1, total); status != 206 || aws.ToInt64(head.ContentLength) != int64(first) || aws.ToInt32(head.PartsCount) != 2 || aws.ToString(head.ContentRange) != wantRange {
		t.Fatalf("HeadObject status=%d ContentLength=%d PartsCount=%d ContentRange=%q want=%q", status, aws.ToInt64(head.ContentLength), aws.ToInt32(head.PartsCount), aws.ToString(head.ContentRange), wantRange)
	}

	got, body := getObjectBody(t, s.client, &s3.GetObjectInput{Bucket: aws.String(b), Key: aws.String(key), PartNumber: aws.Int32(2)})
	if status, wantRange := responseStatus(t, got.ResultMetadata), fmt.Sprintf("bytes %d-%d/%d", first, total-1, total); status != 206 || aws.ToInt32(got.PartsCount) != 2 || aws.ToString(got.ContentRange) != wantRange || body != string(bodies[1]) {
		t.Fatalf("GetObject status=%d PartsCount=%d ContentRange=%q want=%q body=%q", status, aws.ToInt32(got.PartsCount), aws.ToString(got.ContentRange), wantRange, body)
	}

	_, err = s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(b), Key: aws.String(key), PartNumber: aws.Int32(3)})
	assertS3Error(t, err, 416, "InvalidPartNumber")
}
