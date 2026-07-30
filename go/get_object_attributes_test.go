package s3tests

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

var basicObjectAttributes = []types.ObjectAttributes{types.ObjectAttributesObjectSize, types.ObjectAttributesStorageClass, types.ObjectAttributesEtag}

// 기본 GetObjectAttributes 테스트 모든 속성을 요청하고 응답이 올바른지 확인
func TestGetObjectAttributesBasic(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	b, key := s.bucket(t), "test_get_object_attributes_basic"
	putOut, err := s.client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(b), Key: aws.String(key), Body: bytes.NewReader([]byte(key))})
	if err != nil {
		t.Fatal(err)
	}
	out := getAttributes(t, s.client, &s3.GetObjectAttributesInput{Bucket: aws.String(b), Key: aws.String(key), ObjectAttributes: basicObjectAttributes})
	if aws.ToInt64(out.ObjectSize) != int64(len(key)) || aws.ToString(out.ETag) == "" || out.StorageClass != types.StorageClassStandard {
		t.Fatalf("attributes=%#v", out)
	}
	if strings.Trim(aws.ToString(putOut.ETag), "\"") != strings.Trim(aws.ToString(out.ETag), "\"") {
		t.Fatalf("ETag=%q want=%q", aws.ToString(out.ETag), aws.ToString(putOut.ETag))
	}
}

// 특정 속성만 요청하는 테스트
func TestGetObjectAttributesSpecificAttributes(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	b, key := s.bucket(t), "test_get_object_attributes_specific_attributes"
	if _, err := s.client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(b), Key: aws.String(key), Body: bytes.NewReader([]byte(key))}); err != nil {
		t.Fatal(err)
	}
	size := getAttributes(t, s.client, &s3.GetObjectAttributesInput{Bucket: aws.String(b), Key: aws.String(key), ObjectAttributes: []types.ObjectAttributes{types.ObjectAttributesObjectSize}})
	if aws.ToInt64(size.ObjectSize) != int64(len(key)) || size.Checksum != nil {
		t.Fatalf("size=%v checksum=%#v", size.ObjectSize, size.Checksum)
	}
	etag := getAttributes(t, s.client, &s3.GetObjectAttributesInput{Bucket: aws.String(b), Key: aws.String(key), ObjectAttributes: []types.ObjectAttributes{types.ObjectAttributesEtag}})
	if aws.ToString(etag.ETag) == "" || etag.ObjectSize != nil {
		t.Fatalf("etag=%q size=%v", aws.ToString(etag.ETag), etag.ObjectSize)
	}
}

// 멀티파트 업로드된 객체에 대한 GetObjectAttributes 테스트
func TestGetObjectAttributesMultipart(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	b, key := s.bucket(t), "test_get_object_attributes_multipart"
	created, err := s.client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: aws.String(b), Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}
	partBody := bytes.Repeat([]byte("m"), 5*1024*1024)
	parts := make([]types.CompletedPart, 0, 2)
	for i := 1; i <= 2; i++ {
		part, partErr := s.client.UploadPart(context.Background(), &s3.UploadPartInput{Bucket: aws.String(b), Key: aws.String(key), UploadId: created.UploadId, PartNumber: aws.Int32(int32(i)), Body: bytes.NewReader(partBody)})
		if partErr != nil {
			t.Fatal(partErr)
		}
		parts = append(parts, types.CompletedPart{ETag: part.ETag, PartNumber: aws.Int32(int32(i))})
	}
	if _, err = s.client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{Bucket: aws.String(b), Key: aws.String(key), UploadId: created.UploadId, MultipartUpload: &types.CompletedMultipartUpload{Parts: parts}}); err != nil {
		t.Fatal(err)
	}
	out := getAttributes(t, s.client, &s3.GetObjectAttributesInput{Bucket: aws.String(b), Key: aws.String(key), ObjectAttributes: []types.ObjectAttributes{types.ObjectAttributesObjectSize, types.ObjectAttributesStorageClass, types.ObjectAttributesEtag, types.ObjectAttributesObjectParts}})
	wantSize := int64(2 * len(partBody))
	if aws.ToInt64(out.ObjectSize) != wantSize || out.ObjectParts == nil || aws.ToInt32(out.ObjectParts.TotalPartsCount) != 2 {
		t.Fatalf("size=%v parts=%#v", out.ObjectSize, out.ObjectParts)
	}
}

// 체크섬 알고리즘을 사용한 객체에 대한 GetObjectAttributes 테스트
func TestGetObjectAttributesWithChecksum(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	b, key := s.bucket(t), "test_get_object_attributes_with_checksum"
	if _, err := s.client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(b), Key: aws.String(key), Body: bytes.NewReader([]byte(key)), ChecksumAlgorithm: types.ChecksumAlgorithmSha256}); err != nil {
		t.Fatal(err)
	}
	out := getAttributes(t, s.client, &s3.GetObjectAttributesInput{Bucket: aws.String(b), Key: aws.String(key), ObjectAttributes: []types.ObjectAttributes{types.ObjectAttributesChecksum}})
	if out.Checksum == nil || aws.ToString(out.Checksum.ChecksumSHA256) == "" {
		t.Fatalf("checksum=%#v", out.Checksum)
	}
}

// 존재하지 않는 객체에 대한 GetObjectAttributes 테스트
func TestGetObjectAttributesNonExistentObject(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	b := s.bucket(t)
	_, err := s.client.GetObjectAttributes(context.Background(), &s3.GetObjectAttributesInput{Bucket: aws.String(b), Key: aws.String("missing"), ObjectAttributes: []types.ObjectAttributes{types.ObjectAttributesObjectSize}})
	assertS3Error(t, err, 404, "NoSuchKey")
}

// 존재하지 않는 버킷에 대한 GetObjectAttributes 테스트
func TestGetObjectAttributesNonExistentBucket(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	_, err := s.client.GetObjectAttributes(context.Background(), &s3.GetObjectAttributesInput{Bucket: aws.String("missing-" + uniqueBucketSuffix(t)), Key: aws.String("key"), ObjectAttributes: []types.ObjectAttributes{types.ObjectAttributesObjectSize}})
	assertS3Error(t, err, 404, "NoSuchBucket")
}

// 속성을 지정하지 않은 GetObjectAttributes 테스트
func TestGetObjectAttributesNoAttributes(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	b := s.bucket(t)
	put(t, s, b, "key", "body", nil)
	_, err := s.client.GetObjectAttributes(context.Background(), &s3.GetObjectAttributesInput{Bucket: aws.String(b), Key: aws.String("key")})
	if err == nil {
		t.Fatal("request without attributes was accepted")
	}
	// Java는 서비스까지 도달(HTTP 400). Go SDK는 요청 전송 전에 ObjectAttributes를
	// 클라이언트 측 필수로 검증(*smithy.OperationError).
	var responseErr *smithyhttp.ResponseError
	if errors.As(err, &responseErr) {
		assertHTTPError(t, err, 400)
		return
	}
	var opErr *smithy.OperationError
	if !errors.As(err, &opErr) {
		t.Fatalf("want client validation or HTTP 400, got %T: %v", err, err)
	}
}

// 버전 ID를 사용한 GetObjectAttributes 테스트
func TestGetObjectAttributesWithVersionId(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	b := s.bucket(t)
	enableVersioning(t, s, b)
	first := put(t, s, b, "key", "first", nil)
	second := put(t, s, b, "key", "second-version", nil)
	one := getAttributes(t, s.client, &s3.GetObjectAttributesInput{Bucket: aws.String(b), Key: aws.String("key"), VersionId: first.VersionId, ObjectAttributes: []types.ObjectAttributes{types.ObjectAttributesObjectSize}})
	two := getAttributes(t, s.client, &s3.GetObjectAttributesInput{Bucket: aws.String(b), Key: aws.String("key"), VersionId: second.VersionId, ObjectAttributes: []types.ObjectAttributes{types.ObjectAttributesObjectSize}})
	if aws.ToInt64(one.ObjectSize) != 5 || aws.ToInt64(two.ObjectSize) != 14 || aws.ToString(one.VersionId) != aws.ToString(first.VersionId) || aws.ToString(two.VersionId) != aws.ToString(second.VersionId) {
		t.Fatalf("first=%#v second=%#v", one, two)
	}
}

// 잘못된 버전 ID를 사용한 GetObjectAttributes 테스트
func TestGetObjectAttributesInvalidVersionId(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	b := s.bucket(t)
	enableVersioning(t, s, b)
	put(t, s, b, "key", "first", nil)
	put(t, s, b, "key", "second-version", nil)
	_, err := s.client.GetObjectAttributes(context.Background(), &s3.GetObjectAttributesInput{
		Bucket:           aws.String(b),
		Key:              aws.String("key"),
		VersionId:        aws.String("f0lPRNkF3bFOqnocdRx5wLUxaJoESQ59"),
		ObjectAttributes: []types.ObjectAttributes{types.ObjectAttributesObjectSize},
	})
	assertS3Error(t, err, 404, "NoSuchVersion")
}

// 대용량 멀티파트 업로드 객체에 대한 GetObjectAttributes 테스트
func TestGetObjectAttributesLargeMultipart(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	b, key := s.bucket(t), "test_get_object_attributes_large_multipart"
	created, err := s.client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: aws.String(b), Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}
	partBody := bytes.Repeat([]byte("m"), 5*1024*1024)
	parts := make([]types.CompletedPart, 0, 20)
	for i := 1; i <= 20; i++ {
		part, partErr := s.client.UploadPart(context.Background(), &s3.UploadPartInput{Bucket: aws.String(b), Key: aws.String(key), UploadId: created.UploadId, PartNumber: aws.Int32(int32(i)), Body: bytes.NewReader(partBody)})
		if partErr != nil {
			t.Fatal(partErr)
		}
		parts = append(parts, types.CompletedPart{ETag: part.ETag, PartNumber: aws.Int32(int32(i))})
	}
	if _, err = s.client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{Bucket: aws.String(b), Key: aws.String(key), UploadId: created.UploadId, MultipartUpload: &types.CompletedMultipartUpload{Parts: parts}}); err != nil {
		t.Fatal(err)
	}
	out := getAttributes(t, s.client, &s3.GetObjectAttributesInput{Bucket: aws.String(b), Key: aws.String(key), ObjectAttributes: []types.ObjectAttributes{types.ObjectAttributesObjectSize, types.ObjectAttributesStorageClass, types.ObjectAttributesEtag, types.ObjectAttributesObjectParts}})
	wantSize := int64(20 * len(partBody))
	if aws.ToInt64(out.ObjectSize) != wantSize || out.ObjectParts == nil || aws.ToInt32(out.ObjectParts.TotalPartsCount) != 20 {
		t.Fatalf("size=%v parts=%#v", out.ObjectSize, out.ObjectParts)
	}
}

// 메타데이터가 있는 객체에 대한 GetObjectAttributes 테스트
func TestGetObjectAttributesWithMetadata(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	b, key := s.bucket(t), "test_get_object_attributes_with_metadata"
	putOut, err := s.client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(b), Key: aws.String(key), Body: bytes.NewReader([]byte(key)), Metadata: map[string]string{"custom-key1": "custom-value1", "custom-key2": "custom-value2"}})
	if err != nil {
		t.Fatal(err)
	}
	out := getAttributes(t, s.client, &s3.GetObjectAttributesInput{Bucket: aws.String(b), Key: aws.String(key), ObjectAttributes: basicObjectAttributes})
	if aws.ToInt64(out.ObjectSize) != int64(len(key)) || aws.ToString(out.ETag) == "" || out.StorageClass != types.StorageClassStandard {
		t.Fatalf("attributes=%#v", out)
	}
	if strings.Trim(aws.ToString(putOut.ETag), "\"") != strings.Trim(aws.ToString(out.ETag), "\"") {
		t.Fatalf("ETag=%q want=%q", aws.ToString(out.ETag), aws.ToString(putOut.ETag))
	}
	head, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String(key)})
	if err != nil || head.Metadata["custom-key1"] != "custom-value1" {
		t.Fatalf("metadata=%v err=%v", head.Metadata, err)
	}
}

// SSE-S3 암호화된 객체에 대한 GetObjectAttributes 테스트
func TestGetObjectAttributesWithSSES3(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	b, key := s.bucket(t), "test_get_object_attributes_with_sse_s3"
	putOut, err := s.client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(b), Key: aws.String(key), Body: bytes.NewReader([]byte(key)), ServerSideEncryption: types.ServerSideEncryptionAes256})
	if err != nil {
		t.Fatal(err)
	}
	out := getAttributes(t, s.client, &s3.GetObjectAttributesInput{Bucket: aws.String(b), Key: aws.String(key), ObjectAttributes: basicObjectAttributes})
	if aws.ToInt64(out.ObjectSize) != int64(len(key)) || aws.ToString(out.ETag) == "" || out.StorageClass != types.StorageClassStandard {
		t.Fatalf("attributes=%#v", out)
	}
	if strings.Trim(aws.ToString(putOut.ETag), "\"") != strings.Trim(aws.ToString(out.ETag), "\"") {
		t.Fatalf("ETag=%q want=%q", aws.ToString(out.ETag), aws.ToString(putOut.ETag))
	}
	head, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String(key)})
	if err != nil || head.ServerSideEncryption != types.ServerSideEncryptionAes256 {
		t.Fatalf("encryption=%q err=%v", head.ServerSideEncryption, err)
	}
}

// 모든 가능한 속성을 요청하는 GetObjectAttributes 테스트
func TestGetObjectAttributesAllAttributes(t *testing.T) {
	t.Parallel()

	s := newSuite(t)
	b, key := s.bucket(t), "test_get_object_attributes_all_attributes"
	created, err := s.client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: aws.String(b), Key: aws.String(key), ChecksumAlgorithm: types.ChecksumAlgorithmCrc64nvme, ChecksumType: types.ChecksumTypeFullObject})
	if err != nil {
		t.Fatal(err)
	}
	partBody := bytes.Repeat([]byte("m"), 5*1024*1024)
	part, partErr := s.client.UploadPart(context.Background(), &s3.UploadPartInput{Bucket: aws.String(b), Key: aws.String(key), UploadId: created.UploadId, PartNumber: aws.Int32(1), Body: bytes.NewReader(partBody), ChecksumAlgorithm: types.ChecksumAlgorithmCrc64nvme})
	if partErr != nil {
		t.Fatal(partErr)
	}
	parts := []types.CompletedPart{{ETag: part.ETag, PartNumber: aws.Int32(1), ChecksumCRC64NVME: part.ChecksumCRC64NVME}}
	if _, err = s.client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{Bucket: aws.String(b), Key: aws.String(key), UploadId: created.UploadId, ChecksumType: types.ChecksumTypeFullObject, MultipartUpload: &types.CompletedMultipartUpload{Parts: parts}}); err != nil {
		t.Fatal(err)
	}
	out := getAttributes(t, s.client, &s3.GetObjectAttributesInput{Bucket: aws.String(b), Key: aws.String(key), ObjectAttributes: []types.ObjectAttributes{types.ObjectAttributesObjectSize, types.ObjectAttributesStorageClass, types.ObjectAttributesEtag, types.ObjectAttributesObjectParts, types.ObjectAttributesChecksum}})
	wantSize := int64(len(partBody))
	if aws.ToInt64(out.ObjectSize) != wantSize || out.ObjectParts == nil || aws.ToInt32(out.ObjectParts.TotalPartsCount) != 1 {
		t.Fatalf("size=%v parts=%#v", out.ObjectSize, out.ObjectParts)
	}
	if out.Checksum == nil || aws.ToString(out.Checksum.ChecksumCRC64NVME) == "" {
		t.Fatalf("checksum=%#v", out.Checksum)
	}
}

func getAttributes(t *testing.T, client *s3.Client, input *s3.GetObjectAttributesInput) *s3.GetObjectAttributesOutput {
	t.Helper()
	out, err := client.GetObjectAttributes(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	return out
}
